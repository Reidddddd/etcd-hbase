package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseBindingIdentity;
import io.etcdhbase.core.lease.LeaseBindingVersion;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/** Reads one key's immutable Lease binding history at a protected M2 barrier. */
public final class LeaseBindingReadView {
  private final SystemRecordStore records;
  private final ManifestCatalog manifests;
  private final Function<ByteKey, LeaseBindingVersion> decode;
  private final long maximumOverlayRecords;
  private final long maximumOverlayBytes;

  public LeaseBindingReadView(SystemRecordStore records, ManifestCatalog manifests,
      Function<ByteKey, LeaseBindingVersion> decode, long maximumOverlayRecords,
      long maximumOverlayBytes) {
    this.records = Objects.requireNonNull(records, "records");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.decode = Objects.requireNonNull(decode, "decode");
    if (maximumOverlayRecords <= 0 || maximumOverlayBytes <= 0) {
      throw new IllegalArgumentException("Lease binding overlay limits must be positive");
    }
    this.maximumOverlayRecords = maximumOverlayRecords;
    this.maximumOverlayBytes = maximumOverlayBytes;
  }

  /** Returns the latest binding version for one key, including a selected, unfinalized update. */
  public CompletableFuture<Optional<LeaseBindingVersion>> find(PinnedReadView pinned,
      LeaseIdentity identity, ByteKey key, RequestScope scope) {
    return find(pinned, identity, key, scope, null);
  }

  /** Charges stable and selected auxiliary reads to the caller's proof budget. */
  public CompletableFuture<Optional<LeaseBindingVersion>> find(PinnedReadView pinned,
      LeaseIdentity identity, ByteKey key, RequestScope scope,
      ManifestCatalog.ProofReadBudget metadataBudget) {
    Objects.requireNonNull(pinned, "pinned");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(scope, "scope");
    if (key.length() == 0 || !pinned.belongsTo(scope) || !pinned.protectsAuxiliary()
        || metadataBudget != null && !metadataBudget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("invalid Lease binding read view"));
    }
    PinnedReadView.ReaderLease reader;
    try {
      reader = pinned.retainReader();
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    CompletableFuture<Optional<LeaseBindingVersion>> operation;
    try {
      operation = pinned.renew(scope).thenCompose(ignored ->
          findAtBarrier(pinned, identity, key, scope, metadataBudget));
    } catch (RuntimeException | Error failure) {
      reader.close();
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    return operation.thenApply(binding -> {
      requireActive(pinned, scope);
      return binding;
    }).whenComplete((ignored, failure) -> reader.close()).copy();
  }

  private CompletableFuture<Optional<LeaseBindingVersion>> findAtBarrier(PinnedReadView pinned,
      LeaseIdentity identity, ByteKey key, RequestScope scope,
      ManifestCatalog.ProofReadBudget metadataBudget) {
    requireActive(pinned, scope);
    ReadView view = pinned.view();
    CompletableFuture<Optional<LeaseBindingVersion>> stable = view.finalizedSequence() == 0
        ? CompletableFuture.completedFuture(Optional.empty())
        : readStable(identity, key, view.finalizedSequence(), scope, metadataBudget);
    return stable.thenCompose(binding -> {
      requireActive(pinned, scope);
      if (view.selectedRoot().isEmpty()
          || view.selectedRoot().orElseThrow().auxiliaryReference().isEmpty()) {
        return CompletableFuture.completedFuture(binding);
      }
      return overlay(view.selectedRoot().orElseThrow(), binding, identity, key,
          view.barrierSequence(), pinned, scope, metadataBudget);
    });
  }

  private CompletableFuture<Optional<LeaseBindingVersion>> readStable(LeaseIdentity identity,
      ByteKey key, long sequence, RequestScope scope,
      ManifestCatalog.ProofReadBudget metadataBudget) {
    ByteKey route = LeaseBindingIdentity.keyRoutePrefix(identity.leaseId(), identity.incarnation(), key);
    java.util.function.Supplier<CompletableFuture<Optional<SystemRecordEntry>>> read =
        () -> records.latestAtOrBefore(SystemRecordType.LEASE_BINDING, route, sequence, scope);
    CompletableFuture<Optional<SystemRecordEntry>> record = metadataBudget == null ? read.get()
        : metadataBudget.readMeasured(ManifestCatalog.PROOF_SYSTEM_RECORD_ENVELOPE,
            value -> value.map(SystemRecordEntry::encodedBytes).orElse(0L), read);
    return record.thenApply(found -> found.map(entry -> validate(entry.identity(), entry.value(),
        identity, key, sequence)));
  }

  private CompletableFuture<Optional<LeaseBindingVersion>> overlay(ManifestRoot root,
      Optional<LeaseBindingVersion> stable, LeaseIdentity identity, ByteKey key, long sequence,
      PinnedReadView pinned, RequestScope scope,
      ManifestCatalog.ProofReadBudget metadataBudget) {
    AuxiliaryReference reference = root.auxiliaryReference().orElseThrow();
    if (reference.itemCount() > maximumOverlayRecords || reference.totalBytes() > maximumOverlayBytes) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    }
    ByteKey target = new LeaseBindingIdentity(identity.leaseId(), identity.incarnation(), key, sequence).encode();
    var selected = new AtomicReference<LeaseBindingVersion>();
    return manifests.forEachAuxiliary(root, scope, metadataBudget, record -> {
      requireActive(pinned, scope);
      if (record.targetType() == SystemRecordType.LEASE_BINDING
          && record.targetIdentity().equals(target)) {
        LeaseBindingVersion binding = validate(record.targetIdentity(), record.payload(), identity, key, sequence);
        if (!selected.compareAndSet(null, binding)) {
          throw new IllegalStateException("duplicate selected Lease binding version");
        }
      }
      return CompletableFuture.completedFuture(null);
    }).thenApply(ignored -> {
      LeaseBindingVersion selectedBinding = selected.get();
      return selectedBinding == null ? stable : Optional.of(selectedBinding);
    });
  }

  private LeaseBindingVersion validate(ByteKey encodedIdentity, ByteKey payload,
      LeaseIdentity expectedLease, ByteKey expectedKey, long maximumSequence) {
    LeaseBindingIdentity.Decoded encoded = LeaseBindingIdentity.decode(encodedIdentity);
    LeaseBindingVersion binding = Objects.requireNonNull(decode.apply(payload), "decoded Lease binding");
    if (encoded.leaseId() != expectedLease.leaseId()
        || encoded.incarnation() != expectedLease.incarnation()
        || encoded.sequence() > maximumSequence
        || !encoded.key().equals(expectedKey)
        || !binding.identity().equals(expectedLease)
        || !binding.key().equals(expectedKey)
        || binding.sequence() != encoded.sequence()
        || !binding.recordIdentity().encode().equals(encodedIdentity)) {
      throw new IllegalStateException("Lease binding differs from its identity or read barrier");
    }
    return binding;
  }

  private static void requireActive(PinnedReadView pinned, RequestScope scope) {
    if (!pinned.isOpen() || scope.isClosed() || scope.isDeadlineExceeded()) {
      throw new CompletionException(new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
    }
  }
}
