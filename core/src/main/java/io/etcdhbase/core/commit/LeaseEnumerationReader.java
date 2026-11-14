package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.concurrent.ContinuationExecutor;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseEnumerationPointer;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/** Enumerates authoritative Lease states at one auxiliary-protected commit barrier. */
public final class LeaseEnumerationReader {
  private static final ContinuationExecutor CONTINUE = new ContinuationExecutor();
  private static final long OBJECT_ALLOWANCE = 256;
  private static final int OUTPUT_RESERVATION_BATCH = 64;
  private final SystemRecordStore records;
  private final ManifestCatalog manifests;
  private final Function<ByteKey, LeaseState> decode;
  private final int buckets;
  private final int pageEntries;
  private final long pageBytes;
  private final int maximumExamined;
  private final int maximumResults;
  private final int maximumOverlayRecords;
  private final long maximumOverlayBytes;

  public LeaseEnumerationReader(SystemRecordStore records, ManifestCatalog manifests,
      Function<ByteKey, LeaseState> decode, int buckets, int pageEntries, long pageBytes,
      int maximumExamined, int maximumResults, int maximumOverlayRecords, long maximumOverlayBytes) {
    this.records = Objects.requireNonNull(records, "records");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.decode = Objects.requireNonNull(decode, "decode");
    if (buckets < 1 || buckets > 4096 || pageEntries < 1 || pageBytes < 1
        || maximumExamined < 1 || maximumResults < 1 || maximumOverlayRecords < 1
        || maximumOverlayRecords > AuxiliaryReference.MAX_RECORDS || maximumOverlayBytes < 1
        || maximumOverlayBytes > AuxiliaryReference.MAX_TOTAL_BYTES) {
      throw new IllegalArgumentException("invalid Lease enumeration limits");
    }
    this.buckets = buckets;
    this.pageEntries = pageEntries;
    this.pageBytes = pageBytes;
    this.maximumExamined = maximumExamined;
    this.maximumResults = maximumResults;
    this.maximumOverlayRecords = maximumOverlayRecords;
    this.maximumOverlayBytes = maximumOverlayBytes;
  }

  /** Returns all visible states, including revoked states; no partial list is returned on failure. */
  public CompletableFuture<List<LeaseState>> read(PinnedReadView pin, RequestScope scope,
      BudgetManager.Account account, ManifestCatalog.ProofReadBudget metadataBudget) {
    Objects.requireNonNull(pin, "pin");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(metadataBudget, "metadataBudget");
    if (!pin.belongsTo(scope) || !pin.protectsAuxiliary() || !account.belongsTo(scope)
        || !metadataBudget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid Lease enumeration scope"));
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
    CompletableFuture<List<LeaseState>> operation;
    try {
      reservations = ScopedBudgetLeases.register(scope, account);
      reservations.retain(account, BudgetManager.Memory.PAGE, pageMemoryBytes());
      reservations.acquire(account, BudgetManager.Work.scan());
      var cursor = new Cursor(pin, scope, metadataBudget, account, reservations);
      operation = pin.renew(scope).thenCompose(ignored -> cursor.readOverlay())
          .thenCompose(ignored -> cursor.scanBucket(0, Optional.empty()))
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
    private final RequestScope scope;
    private final ManifestCatalog.ProofReadBudget budget;
    private final BudgetManager.Account account;
    private final ScopedBudgetLeases reservations;
    private final Map<Long, LeaseState> selectedStates = new HashMap<>();
    private final Set<Long> selectedPointers = new HashSet<>();
    private final Set<Long> visitedSelected = new HashSet<>();
    private final List<LeaseState> output = new ArrayList<>();
    private int examined;
    private int reservedOutputStates;

    private Cursor(PinnedReadView pin, RequestScope scope, ManifestCatalog.ProofReadBudget budget,
        BudgetManager.Account account, ScopedBudgetLeases reservations) {
      this.pin = pin;
      this.scope = scope;
      this.budget = budget;
      this.account = account;
      this.reservations = reservations;
    }

    private CompletableFuture<Void> readOverlay() {
      active();
      var root = pin.selectedRoot();
      if (root.isEmpty() || root.orElseThrow().auxiliaryReference().isEmpty()) {
        return CompletableFuture.completedFuture(null);
      }
      var reference = root.orElseThrow().auxiliaryReference().orElseThrow();
      if (reference.itemCount() > maximumOverlayRecords || reference.totalBytes() > maximumOverlayBytes) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      long retainedBytes = Math.addExact(
          Math.multiplyExact(3L * OBJECT_ALLOWANCE, reference.itemCount()),
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
        if (record.targetType() == SystemRecordType.LEASE_STATE) {
          long id = LeaseRecordIdentity.leaseId(record.targetIdentity());
          var state = validate(record.targetIdentity(), record.payload(), id, pin.view().barrierSequence());
          if (state.sequence() != pin.view().barrierSequence() || selectedStates.putIfAbsent(id, state) != null) {
            throw new IllegalStateException("invalid selected Lease state");
          }
        } else if (record.targetType() == SystemRecordType.LEASE_ENUMERATION) {
          long id = enumerationId(record.targetIdentity());
          if (LeaseEnumerationPointer.decode(record.payload()).sequence() != pin.view().barrierSequence()
              || !selectedPointers.add(id)) {
            throw new IllegalStateException("invalid selected Lease pointer");
          }
        }
        return CompletableFuture.completedFuture(null);
      }).thenApply(ignored -> null);
    }

    private CompletableFuture<Void> scanBucket(int bucket, Optional<ByteKey> after) {
      active();
      if (bucket == buckets) return CompletableFuture.completedFuture(null);
      return pin.renew(scope).thenCompose(ignored -> budget.scanMeasured(
          pageBytes,
          SystemRecordPage::encodedBytes,
          () -> records.scanBucket(SystemRecordType.LEASE_ENUMERATION, bucket, after,
              pageEntries, pageBytes, scope)))
          .thenComposeAsync(page -> {
            active();
            long actualBytes = 0;
            for (SystemRecordEntry entry : page.entries()) {
              actualBytes = Math.addExact(actualBytes,
                  Math.addExact(entry.identity().length(), entry.value().length()));
            }
            if (page.entries().size() > pageEntries || page.encodedBytes() > pageBytes
                || page.encodedBytes() != actualBytes
                || page.physicalRows() > pageEntries + 1L
                || page.entries().isEmpty() && !page.exhausted()
                || after.isPresent() && !page.entries().isEmpty()
                    && page.entries().getFirst().identity().compareTo(after.orElseThrow()) <= 0) {
              throw new IllegalStateException("invalid Lease enumeration page");
            }
            try {
              account.chargeInternalScanPage(
                  page.physicalRows(), page.encodedBytes(), page.scanRpcCalls());
            } catch (BackendFailure failure) {
              return CompletableFuture.failedFuture(failure);
            }
            return visitPage(page.entries(), 0, bucket).thenComposeAsync(ignored -> page.exhausted()
                ? scanBucket(bucket + 1, Optional.empty())
                : scanBucket(bucket, page.lastIdentity()), CONTINUE);
          }, CONTINUE);
    }

    private CompletableFuture<Void> visitPage(List<SystemRecordEntry> entries, int index, int bucket) {
      active();
      if (index == entries.size()) return CompletableFuture.completedFuture(null);
      SystemRecordEntry entry = entries.get(index);
      long id = enumerationId(entry.identity());
      if (records.bucket(SystemRecordType.LEASE_ENUMERATION, entry.identity()) != bucket) {
        throw new IllegalStateException("Lease pointer escaped its salt bucket");
      }
      if (++examined > maximumExamined) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      long pointerSequence = LeaseEnumerationPointer.decode(entry.value()).sequence();
      long finalized = pin.view().finalizedSequence();
      CompletableFuture<Optional<SystemRecordEntry>> stable = finalized == 0
          ? CompletableFuture.completedFuture(Optional.empty())
          : budget.readMeasured(ManifestCatalog.PROOF_SYSTEM_RECORD_ENVELOPE,
              value -> value.map(SystemRecordEntry::encodedBytes).orElse(0L),
              () -> records.latestAtOrBefore(SystemRecordType.LEASE_STATE,
                  LeaseRecordIdentity.routePrefix(id), finalized, scope));
      return stable.thenComposeAsync(found -> {
        active();
        var state = found.map(value -> validate(value.identity(), value.value(), id, finalized));
        if (pointerSequence <= finalized
            && (state.isEmpty() || state.orElseThrow().sequence() != pointerSequence)) {
          throw new IllegalStateException("Lease pointer has no matching durable state");
        }
        if (selectedStates.containsKey(id) || selectedPointers.contains(id)) visitedSelected.add(id);
        LeaseState selected = selectedStates.get(id);
        if (selectedPointers.contains(id) && selected == null) {
          throw new IllegalStateException("selected Lease pointer has no state");
        }
        if (selected != null) append(selected);
        else state.ifPresent(this::append);
        return visitPage(entries, index + 1, bucket);
      }, CONTINUE);
    }

    private List<LeaseState> result() {
      active();
      for (long id : selectedPointers) {
        if (!selectedStates.containsKey(id)) throw new IllegalStateException("selected Lease pointer has no state");
      }
      for (var entry : selectedStates.entrySet()) {
        if (!visitedSelected.contains(entry.getKey())) {
          if (!selectedPointers.contains(entry.getKey())) {
            throw new IllegalStateException("selected Lease state has no enumeration pointer");
          }
          append(entry.getValue());
        }
      }
      output.sort(java.util.Comparator.comparingLong(value -> value.identity().leaseId()));
      return List.copyOf(output);
    }

    private void append(LeaseState state) {
      if (output.size() >= maximumResults) throw resourceLimit();
      if (output.size() == reservedOutputStates) {
        int additionalStates = Math.min(OUTPUT_RESERVATION_BATCH, maximumResults - reservedOutputStates);
        long retainedBytes = Math.multiplyExact(OBJECT_ALLOWANCE, additionalStates);
        try {
          reservations.retain(account, BudgetManager.Memory.OUTPUT, retainedBytes);
        } catch (BackendFailure failure) {
          throw new CompletionException(failure);
        }
        reservedOutputStates += additionalStates;
      }
      output.add(state);
    }

    private void active() {
      if (!pin.isOpen() || scope.isClosed() || scope.isDeadlineExceeded()) {
        throw new CompletionException(inactive(scope));
      }
    }
  }

  private LeaseState validate(ByteKey identity, ByteKey payload, long id, long maximumSequence) {
    long sequence = LeaseRecordIdentity.sequence(identity);
    LeaseState state = Objects.requireNonNull(decode.apply(payload), "decoded Lease state");
    if (id == 0 || LeaseRecordIdentity.leaseId(identity) != id || sequence > maximumSequence
        || state.sequence() != sequence || state.identity().leaseId() != id) {
      throw new IllegalStateException("Lease state differs from its identity or barrier");
    }
    return state;
  }

  private static long enumerationId(ByteKey identity) {
    if (identity.length() != LeaseRecordIdentity.ROUTE_BYTES) {
      throw new IllegalStateException("invalid Lease enumeration identity");
    }
    long id = LeaseRecordIdentity.leaseId(identity);
    if (id == 0) throw new IllegalStateException("zero Lease ID in enumeration");
    return id;
  }

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }

  private long pageMemoryBytes() {
    return Math.addExact(Math.multiplyExact(3, pageBytes),
        Math.multiplyExact(OBJECT_ALLOWANCE, pageEntries));
  }

  private static CompletionException resourceLimit() {
    return new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
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
