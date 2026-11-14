package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/** Reads a lease at a pinned commit barrier, including a selected but unfinalized update. */
public final class LeaseReadView {
  private final SystemRecordStore records;
  private final ManifestCatalog manifests;
  private final Function<ByteKey, LeaseState> decode;
  private final long maximumOverlayRecords;
  private final long maximumOverlayBytes;

  public LeaseReadView(SystemRecordStore records, ManifestCatalog manifests,
      Function<ByteKey, LeaseState> decode, long maximumOverlayRecords, long maximumOverlayBytes) {
    this.records = Objects.requireNonNull(records, "records");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.decode = Objects.requireNonNull(decode, "decode");
    if (maximumOverlayRecords <= 0 || maximumOverlayBytes <= 0) {
      throw new IllegalArgumentException("lease overlay limits must be positive");
    }
    this.maximumOverlayRecords = maximumOverlayRecords;
    this.maximumOverlayBytes = maximumOverlayBytes;
  }

  /** Includes revoked versions so the owner can distinguish an unused ID from a reused ID. */
  public CompletableFuture<Optional<LeaseState>> find(
      PinnedReadView pinned, long leaseId, RequestScope scope) {
    return find(pinned, leaseId, scope, null);
  }

  /** Charges physical metadata reads to the supplied request budget. */
  public CompletableFuture<Optional<LeaseState>> find(PinnedReadView pinned, long leaseId,
      RequestScope scope, ManifestCatalog.ProofReadBudget metadataBudget) {
    Objects.requireNonNull(pinned, "pinned");
    Objects.requireNonNull(scope, "scope");
    if (leaseId == 0 || !pinned.belongsTo(scope) || !pinned.protectsAuxiliary()
        || metadataBudget != null && !metadataBudget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("lease ID or auxiliary-protected request scope is invalid"));
    }
    PinnedReadView.ReaderLease reader;
    try {
      reader = pinned.retainReader();
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    CompletableFuture<Optional<LeaseState>> work;
    try {
      work = pinned.renew(scope).thenCompose(ignored -> findAtBarrier(pinned, leaseId, scope, metadataBudget));
    } catch (RuntimeException | Error failure) {
      reader.close();
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    return work.thenApply(state -> {
      requireActive(pinned, scope);
      return state;
    }).whenComplete((ignored, failure) -> reader.close()).copy();
  }

  /** Validates the durable pin after the complete result is assembled and before responding. */
  public CompletableFuture<Void> finish(PinnedReadView pinned, RequestScope scope) {
    Objects.requireNonNull(pinned, "pinned");
    Objects.requireNonNull(scope, "scope");
    if (!pinned.belongsTo(scope) || !pinned.protectsAuxiliary()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid auxiliary-protected pin"));
    }
    PinnedReadView.ReaderLease reader;
    try {
      reader = pinned.retainReader();
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    CompletableFuture<Void> completion;
    try {
      requireActive(pinned, scope);
      completion = pinned.complete(scope);
    } catch (RuntimeException | Error failure) {
      reader.close();
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    return completion.whenComplete((ignored, failure) -> reader.close()).copy();
  }

  private CompletableFuture<Optional<LeaseState>> findAtBarrier(
      PinnedReadView pinned, long leaseId, RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    requireActive(pinned, scope);
    ReadView view = pinned.view();
    CompletableFuture<Optional<SystemRecordEntry>> stable = view.finalizedSequence() == 0
        ? CompletableFuture.completedFuture(Optional.empty())
        : readStable(leaseId, view.finalizedSequence(), scope, budget);
    return stable.thenCompose(entry -> {
      requireActive(pinned, scope);
      Optional<LeaseState> base = entry.map(value -> validate(value.identity(), value.value(),
          leaseId, view.finalizedSequence()));
      if (view.selectedRoot().isEmpty()
          || view.selectedRoot().orElseThrow().auxiliaryReference().isEmpty()) {
        return CompletableFuture.completedFuture(base);
      }
      return overlay(view.selectedRoot().orElseThrow(), base, leaseId, view.barrierSequence(),
          pinned, scope, budget);
    });
  }

  private CompletableFuture<Optional<SystemRecordEntry>> readStable(long leaseId, long sequence,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    java.util.function.Supplier<CompletableFuture<Optional<SystemRecordEntry>>> read =
        () -> records.latestAtOrBefore(SystemRecordType.LEASE_STATE,
            LeaseRecordIdentity.routePrefix(leaseId), sequence, scope);
    return budget == null ? read.get() : budget.readMeasured(
        ManifestCatalog.PROOF_SYSTEM_RECORD_ENVELOPE,
        value -> value.map(SystemRecordEntry::encodedBytes).orElse(0L), read);
  }

  private CompletableFuture<Optional<LeaseState>> overlay(ManifestRoot root,
      Optional<LeaseState> base, long leaseId, long sequence, PinnedReadView pinned,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    AuxiliaryReference reference = root.auxiliaryReference().orElseThrow();
    if (reference.itemCount() > maximumOverlayRecords || reference.totalBytes() > maximumOverlayBytes) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    }
    ByteKey target = LeaseRecordIdentity.stateVersion(leaseId, sequence);
    var selected = new java.util.concurrent.atomic.AtomicReference<LeaseState>();
    return manifests.forEachAuxiliary(root, scope, budget, record -> {
      requireActive(pinned, scope);
      if (record.targetType() == SystemRecordType.LEASE_STATE
          && record.targetIdentity().equals(target)) {
        LeaseState state = validate(record.targetIdentity(), record.payload(), leaseId, sequence);
        if (!selected.compareAndSet(null, state)) {
          throw new IllegalStateException("duplicate lease state in selected auxiliary manifest");
        }
      }
      return CompletableFuture.completedFuture(null);
    }).thenApply(ignored -> selected.get() == null ? base : Optional.of(selected.get()));
  }

  private LeaseState validate(ByteKey identity, ByteKey payload, long leaseId, long upperSequence) {
    long sequence = LeaseRecordIdentity.sequence(identity);
    LeaseState state = Objects.requireNonNull(decode.apply(payload), "decoded lease state");
    if (LeaseRecordIdentity.leaseId(identity) != leaseId || sequence > upperSequence
        || state.sequence() != sequence || state.identity().leaseId() != leaseId) {
      throw new IllegalStateException("lease state differs from its identity or read barrier");
    }
    return state;
  }

  private static void requireActive(PinnedReadView pinned, RequestScope scope) {
    if (!pinned.isOpen() || scope.isClosed() || scope.isDeadlineExceeded()) {
      throw new CompletionException(new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
    }
  }
}
