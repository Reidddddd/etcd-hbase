package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.concurrent.ContinuationExecutor;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseBindingIdentity;
import io.etcdhbase.core.lease.LeaseBindingVersion;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/** Reads a Lease's effective key bindings from one protected M2 barrier. */
public final class LeaseBindingReader {
  private static final ContinuationExecutor CONTINUE = new ContinuationExecutor();
  private static final long OBJECT_ALLOWANCE = 256;

  private final SystemRecordStore records;
  private final ManifestCatalog manifests;
  private final Function<ByteKey, LeaseBindingVersion> decode;
  private final int pageEntries;
  private final long pageBytes;
  private final int maximumExamined;
  private final long maximumScannedBytes;
  private final int maximumBindings;
  private final long maximumOutputBytes;
  private final int maximumOverlayRecords;
  private final long maximumOverlayBytes;

  private record StableBinding(LeaseBindingVersion value, BudgetManager.Lease memory) {}

  public LeaseBindingReader(SystemRecordStore records, ManifestCatalog manifests,
      Function<ByteKey, LeaseBindingVersion> decode, int pageEntries, long pageBytes,
      int maximumExamined, long maximumScannedBytes, int maximumBindings,
      long maximumOutputBytes, int maximumOverlayRecords, long maximumOverlayBytes) {
    this.records = Objects.requireNonNull(records, "records");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.decode = Objects.requireNonNull(decode, "decode");
    if (pageEntries < 1 || pageBytes < 1 || maximumExamined < 1 || maximumScannedBytes < 1
        || maximumBindings < 1 || maximumOutputBytes < 1 || maximumOverlayRecords < 1
        || maximumOverlayBytes < 1) {
      throw new IllegalArgumentException("invalid Lease binding read limits");
    }
    this.pageEntries = pageEntries;
    this.pageBytes = pageBytes;
    this.maximumExamined = maximumExamined;
    this.maximumScannedBytes = maximumScannedBytes;
    this.maximumBindings = maximumBindings;
    this.maximumOutputBytes = maximumOutputBytes;
    this.maximumOverlayRecords = maximumOverlayRecords;
    this.maximumOverlayBytes = maximumOverlayBytes;
  }

  /** Returns all currently bound keys or fails as a whole when the view exceeds its budget. */
  public CompletableFuture<List<ByteKey>> read(PinnedReadView pin, LeaseState state,
      RequestScope scope, BudgetManager.Account account,
      ManifestCatalog.ProofReadBudget metadataBudget) {
    return readBindings(pin, state, scope, account, metadataBudget)
        .thenApply(bindings -> bindings.stream().map(LeaseBindingVersion::key).toList()).copy();
  }

  /** Returns the effective binding versions for an atomic revoke plan at this pinned barrier. */
  public CompletableFuture<List<LeaseBindingVersion>> readBindings(PinnedReadView pin, LeaseState state,
      RequestScope scope, BudgetManager.Account account,
      ManifestCatalog.ProofReadBudget metadataBudget) {
    Objects.requireNonNull(pin, "pin");
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(metadataBudget, "metadataBudget");
    if (!pin.belongsTo(scope) || !pin.protectsAuxiliary() || !account.belongsTo(scope)
        || !metadataBudget.belongsTo(scope) || state.status() != LeaseState.Status.ACTIVE) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("invalid Lease binding read view"));
    }
    PinnedReadView.ReaderLease reader;
    try {
      reader = pin.retainReader();
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) {
      reader.close();
      return CompletableFuture.failedFuture(inactive(scope));
    }
    ScopedBudgetLeases reservations = null;
    CompletableFuture<List<LeaseBindingVersion>> operation;
    try {
      if (state.boundKeyCount() > maximumBindings || state.boundKeyBytes() > maximumOutputBytes) {
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
      reservations = ScopedBudgetLeases.register(scope, account);
      long outputBytes = Math.addExact(state.boundKeyBytes(),
          Math.multiplyExact(OBJECT_ALLOWANCE, state.boundKeyCount()));
      if (outputBytes > 0) reservations.retain(account, BudgetManager.Memory.OUTPUT, outputBytes);
      reservations.retain(account, BudgetManager.Memory.PAGE, pageMemoryBytes());
      reservations.acquire(account, BudgetManager.Work.scan());
      Cursor cursor = new Cursor(pin, state, scope, metadataBudget, account, reservations);
      operation = pin.renew(scope).thenCompose(ignored -> cursor.readOverlay())
          .thenCompose(ignored -> cursor.scan(Optional.empty()))
          .thenApply(ignored -> cursor.result());
    } catch (BackendFailure | RuntimeException failure) {
      closeReservations(reservations, failure);
      drained.complete(null);
      reader.close();
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      closeReservations(reservations, fatal);
      drained.complete(null);
      reader.close();
      throw fatal;
    }
    return operation.whenComplete((ignored, failure) -> {
      drained.complete(null);
      reader.close();
    }).copy();
  }

  private final class Cursor {
    private final PinnedReadView pin;
    private final LeaseState lease;
    private final RequestScope scope;
    private final ManifestCatalog.ProofReadBudget budget;
    private final BudgetManager.Account account;
    private final ScopedBudgetLeases reservations;
    private final Map<ByteKey, StableBinding> stable = new HashMap<>();
    private final Map<ByteKey, LeaseBindingVersion> selected = new HashMap<>();
    private int bucket = -1;
    private int examined;
    private long scannedBytes;
    private int overlayRecords;
    private long overlayBytes;

    private Cursor(PinnedReadView pin, LeaseState lease, RequestScope scope,
        ManifestCatalog.ProofReadBudget budget, BudgetManager.Account account,
        ScopedBudgetLeases reservations) {
      this.pin = pin;
      this.lease = lease;
      this.scope = scope;
      this.budget = budget;
      this.account = account;
      this.reservations = reservations;
    }

    private CompletableFuture<Void> readOverlay() {
      active();
      Optional<ManifestRoot> root = pin.selectedRoot();
      if (root.isEmpty() || root.orElseThrow().auxiliaryReference().isEmpty()) {
        return CompletableFuture.completedFuture(null);
      }
      AuxiliaryReference reference = root.orElseThrow().auxiliaryReference().orElseThrow();
      if (reference.itemCount() > maximumOverlayRecords || reference.totalBytes() > maximumOverlayBytes) {
        return CompletableFuture.failedFuture(resourceLimit());
      }
      long retainedBytes = Math.addExact(
          Math.multiplyExact(2L * OBJECT_ALLOWANCE, reference.itemCount()),
          Math.multiplyExact(2, reference.totalBytes()));
      try {
        if (retainedBytes > 0) {
          reservations.retain(account, BudgetManager.Memory.CANDIDATE, retainedBytes);
        }
      } catch (BackendFailure | RuntimeException failure) {
        return CompletableFuture.failedFuture(failure);
      }
      return manifests.forEachAuxiliary(root.orElseThrow(), scope, budget, record -> {
        active();
        overlayRecords = Math.addExact(overlayRecords, 1);
        overlayBytes = Math.addExact(overlayBytes, record.encodedBytes());
        if (overlayRecords > maximumOverlayRecords || overlayBytes > maximumOverlayBytes) {
          return CompletableFuture.failedFuture(resourceLimit());
        }
        if (record.targetType() == SystemRecordType.LEASE_BINDING) {
          LeaseBindingIdentity.Decoded identity = LeaseBindingIdentity.decode(record.targetIdentity());
          if (identity.leaseId() == lease.identity().leaseId()
              && identity.incarnation() == lease.identity().incarnation()) {
            LeaseBindingVersion binding = validate(record.targetIdentity(), record.payload(),
                pin.view().barrierSequence());
            if (selected.putIfAbsent(binding.key(), binding) != null) {
              throw new IllegalStateException("duplicate selected Lease binding for one key");
            }
          }
        }
        return CompletableFuture.completedFuture(null);
      }).thenApply(ignored -> null);
    }

    private CompletableFuture<Void> scan(Optional<ByteKey> after) {
      active();
      return pin.renew(scope).thenCompose(ignored -> budget.scanMeasured(
          pageBytes,
          SystemRecordPage::encodedBytes,
          () -> records.scanLeaseBindings(lease.identity().leaseId(),
              lease.identity().incarnation(), after, pageEntries, pageBytes, scope)))
          .thenComposeAsync(page -> {
            active();
            validatePage(page, after);
            try {
              account.chargeInternalScanPage(
                  page.physicalRows(), page.encodedBytes(), page.scanRpcCalls());
            } catch (BackendFailure failure) {
              return CompletableFuture.failedFuture(failure);
            }
            return visit(page.entries(), 0).thenComposeAsync(ignored -> {
              if (page.exhausted()) return CompletableFuture.completedFuture(null);
              return scan(page.lastIdentity());
            }, CONTINUE);
          }, CONTINUE);
    }

    private void validatePage(SystemRecordPage page, Optional<ByteKey> after) {
      long actualBytes = 0;
      ByteKey previous = after.orElse(null);
      for (SystemRecordEntry entry : page.entries()) {
        actualBytes = Math.addExact(actualBytes,
            Math.addExact(entry.identity().length(), entry.value().length()));
        if (previous != null && entry.identity().compareTo(previous) <= 0) {
          throw new IllegalStateException("Lease binding page is not strictly ordered");
        }
        previous = entry.identity();
      }
      if (page.entries().size() > pageEntries || page.encodedBytes() > pageBytes
          || page.encodedBytes() != actualBytes || page.entries().isEmpty() && !page.exhausted()
          || page.physicalRows() > pageEntries + 1L
          || page.lastIdentity().isPresent() != !page.entries().isEmpty()
          || !page.entries().isEmpty()
              && !page.lastIdentity().orElseThrow().equals(page.entries().getLast().identity())) {
        throw new IllegalStateException("Lease binding page violates its bounded cursor contract");
      }
    }

    private CompletableFuture<Void> visit(List<SystemRecordEntry> entries, int index) {
      active();
      if (index == entries.size()) return CompletableFuture.completedFuture(null);
      SystemRecordEntry entry = entries.get(index);
      LeaseBindingIdentity.Decoded identity = LeaseBindingIdentity.decode(entry.identity());
      if (identity.leaseId() != lease.identity().leaseId()
          || identity.incarnation() != lease.identity().incarnation()) {
        throw new IllegalStateException("Lease binding escaped its route prefix");
      }
      int actualBucket = records.bucket(SystemRecordType.LEASE_BINDING, entry.identity());
      if (bucket == -1) bucket = actualBucket;
      else if (bucket != actualBucket) {
        throw new IllegalStateException("Lease binding route escaped its salt bucket");
      }
      examined = Math.addExact(examined, 1);
      scannedBytes = Math.addExact(scannedBytes,
          Math.addExact(entry.identity().length(), entry.value().length()));
      if (examined > maximumExamined || scannedBytes > maximumScannedBytes) {
        return CompletableFuture.failedFuture(resourceLimit());
      }
      if (identity.sequence() <= pin.view().finalizedSequence()) {
        LeaseBindingVersion binding = validate(entry.identity(), entry.value(),
            pin.view().finalizedSequence());
        StableBinding previous = stable.get(binding.key());
        if (previous != null && previous.value().sequence() >= binding.sequence()) {
          throw new IllegalStateException("Lease binding history is not strictly versioned");
        }
        long retainedBytes = stableBindingMemoryBytes(entry, binding);
        BudgetManager.Lease memory;
        try {
          memory = reservations.retain(account, BudgetManager.Memory.CANDIDATE, retainedBytes);
        } catch (BackendFailure failure) {
          return CompletableFuture.failedFuture(failure);
        }
        try {
          StableBinding replaced = stable.put(binding.key(), new StableBinding(binding, memory));
          if (replaced != null) reservations.release(replaced.memory());
        } catch (RuntimeException | Error failure) {
          reservations.release(memory);
          throw failure;
        }
      }
      return visit(entries, index + 1);
    }

    private LeaseBindingVersion validate(ByteKey identity, ByteKey payload, long maximumSequence) {
      LeaseBindingIdentity.Decoded encoded = LeaseBindingIdentity.decode(identity);
      LeaseBindingVersion binding = Objects.requireNonNull(decode.apply(payload), "decoded Lease binding");
      if (encoded.leaseId() != lease.identity().leaseId()
          || encoded.incarnation() != lease.identity().incarnation()
          || encoded.sequence() > maximumSequence
          || !binding.identity().equals(lease.identity())
          || !binding.key().equals(encoded.key())
          || binding.sequence() != encoded.sequence()
          || !binding.recordIdentity().encode().equals(identity)) {
        throw new IllegalStateException("Lease binding differs from its identity or read barrier");
      }
      return binding;
    }

    private List<LeaseBindingVersion> result() {
      active();
      long bindingCount = 0;
      long keyBytes = 0;
      for (Map.Entry<ByteKey, StableBinding> entry : stable.entrySet()) {
        LeaseBindingVersion binding = selected.getOrDefault(entry.getKey(), entry.getValue().value());
        if (binding.bound()) {
          bindingCount = Math.addExact(bindingCount, 1);
          keyBytes = Math.addExact(keyBytes, binding.key().length());
        }
      }
      for (Map.Entry<ByteKey, LeaseBindingVersion> entry : selected.entrySet()) {
        if (!stable.containsKey(entry.getKey()) && entry.getValue().bound()) {
          bindingCount = Math.addExact(bindingCount, 1);
          keyBytes = Math.addExact(keyBytes, entry.getValue().key().length());
        }
      }
      if (bindingCount > maximumBindings || keyBytes > maximumOutputBytes) {
        throw new CompletionException(resourceLimit());
      }
      if (bindingCount != lease.boundKeyCount() || keyBytes != lease.boundKeyBytes()) {
        throw new IllegalStateException("Lease binding records disagree with authoritative counters");
      }
      var bindings = new ArrayList<LeaseBindingVersion>(Math.toIntExact(bindingCount));
      for (Map.Entry<ByteKey, StableBinding> entry : stable.entrySet()) {
        LeaseBindingVersion binding = selected.getOrDefault(entry.getKey(), entry.getValue().value());
        if (binding.bound()) bindings.add(binding);
      }
      for (Map.Entry<ByteKey, LeaseBindingVersion> entry : selected.entrySet()) {
        if (!stable.containsKey(entry.getKey()) && entry.getValue().bound()) {
          bindings.add(entry.getValue());
        }
      }
      bindings.sort(java.util.Comparator.comparing(LeaseBindingVersion::key));
      return List.copyOf(bindings);
    }

    private long stableBindingMemoryBytes(SystemRecordEntry entry, LeaseBindingVersion binding) {
      return Math.addExact(OBJECT_ALLOWANCE,
          Math.addExact(entry.identity().length(),
              Math.addExact(entry.value().length(), binding.key().length())));
    }

    private void active() {
      if (!pin.isOpen() || scope.isClosed() || scope.isDeadlineExceeded()) {
        throw new CompletionException(inactive(scope));
      }
    }
  }

  private static BackendFailure resourceLimit() {
    return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
  }

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }

  public static long requiredPageMemoryBytes(int pageEntries, long pageBytes) {
    if (pageEntries < 1 || pageBytes < 1) throw new IllegalArgumentException("invalid binding page bounds");
    return Math.addExact(Math.multiplyExact(3, pageBytes),
        Math.multiplyExact(OBJECT_ALLOWANCE, pageEntries));
  }

  private long pageMemoryBytes() {
    return requiredPageMemoryBytes(pageEntries, pageBytes);
  }

  private static void closeReservations(ScopedBudgetLeases reservations, Throwable failure) {
    if (reservations == null) return;
    try {
      reservations.close();
    } catch (RuntimeException | Error cleanupFailure) {
      if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
    }
  }
}
