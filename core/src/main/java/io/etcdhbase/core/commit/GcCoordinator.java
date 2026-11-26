package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.bytes.ByteKey;
import java.util.ArrayList;
import io.etcdhbase.core.request.RequestScope;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Converts the durable KV pin registration floors into a narrowly scoped history permission. */
public final class GcCoordinator {
  private final CommitControlStore control;
  private final CompactionService compactions;
  private final SnapshotPinManager pins;
  private final Clock clock;

  public GcCoordinator(CommitControlStore control, CompactionService compactions,
      SnapshotPinManager pins, Clock clock) {
    this.control = Objects.requireNonNull(control, "control");
    this.compactions = Objects.requireNonNull(compactions, "compactions");
    this.pins = Objects.requireNonNull(pins, "pins");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  /**
   * All pin shards must confirm both floors before this method returns. Selected roots and all
   * system records remain retained; this permission only retires redundant materialized KV data.
   */
  public CompletableFuture<HistoryGcPermit> authorizeHistory(CommitOwnerService.OwnerToken owner,
      RequestScope scope) {
    return control.read(scope).thenCompose(found -> {
      CommitState observed = found.orElseThrow(() -> new IllegalStateException("missing control")).state();
      requireOwner(observed, owner);
      if (observed.publication().isPresent() || observed.compactOperationId().isEmpty()) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      return compactions.reconcile(observed, scope).thenCompose(operation ->
          pins.advanceRegistrationFloors(observed.compactRevision(), observed.finalizedSequence(), scope)
              .thenCompose(boundary -> confirmHistoryPermit(observed, operation.orElseThrow(),
                  boundary, owner, scope)));
    });
  }

  /** Confirms event/backfill retention before raising KV floors, then binds both durable registries. */
  public CompletableFuture<SystemRecordRetirement.Permit> authorizeSystemRecords(
      CommitOwnerService.OwnerToken owner, EventHistoryRetention events, ByteKey authorizationDigest,
      ManifestCatalog.ProofReadBudget budget, RequestScope scope) {
    Objects.requireNonNull(events, "events");
    if (!scope.hasDeadline()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("system-record GC requires a deadline"));
    }
    return control.read(scope).thenCompose(found -> {
      var observed = found.orElseThrow(() -> new IllegalStateException("missing control")).state();
      requireOwner(observed, owner);
      if (observed.publication().isPresent() || observed.compactOperationId().isEmpty()
          || !observed.supportsAuthorizationBarrier() || observed.eventRegistryNamespace().isEmpty()
          || !events.pins().namespace().equals(observed.eventRegistryNamespace())) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      return compactions.reconcile(observed, scope).thenCompose(operation ->
          events.advance(observed.compactRevision(), observed.finalizedSequence(), authorizationDigest, scope, budget)
              .thenCompose(eventBoundary -> pins.advanceRegistrationFloors(
                  observed.compactRevision(), observed.finalizedSequence(), scope)
                  .thenCompose(kvBoundary -> confirmSystemPermit(observed, operation.orElseThrow(),
                      kvBoundary, eventBoundary, events.pins(), owner, scope))));
    });
  }

  private CompletableFuture<SystemRecordRetirement.Permit> confirmSystemPermit(CommitState observed,
      CompactionOperation operation, SnapshotGcBoundary kvBoundary, SnapshotGcBoundary eventBoundary,
      EventHistoryPinManager eventPins, CommitOwnerService.OwnerToken owner, RequestScope scope) {
    return control.read(scope).thenApply(found -> {
      var current = found.orElseThrow(() -> new IllegalStateException("missing control")).state();
      requireOwner(current, owner);
      if (!current.profileFingerprint().equals(observed.profileFingerprint())
          || !current.compactOperationId().equals(observed.compactOperationId())
          || current.compactRevision() != observed.compactRevision() || current.publication().isPresent()
          || !current.eventRegistryNamespace().equals(observed.eventRegistryNamespace())
          || current.authorizationBarrierGeneration() != observed.authorizationBarrierGeneration()
          || current.finalizedSequence() < observed.finalizedSequence()
          || operation.stage() != CompactionOperation.Stage.LOGICAL
          || !observed.compactOperationId().equals(java.util.Optional.of(operation.id()))
          || scope.isClosed() || scope.isDeadlineExceeded()) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      var authority = new SystemRecordRetirement.Authority(owner, observed.authorizationBarrierGeneration(),
          pins.configuration(), eventPins.configuration(), eventPins.registryIdentity(),
          eventPins.namespace().orElseThrow());
      var protections = new ArrayList<>(kvBoundary.protections());
      protections.addAll(eventBoundary.protections());
      return new SystemRecordRetirement.Permit(observed.profileFingerprint(), pins.registryIdentity(),
          operation.id(), Math.addExact(operation.controlVersion(), 1),
          Math.min(observed.compactRevision(), Math.min(kvBoundary.safeRevision(), eventBoundary.safeRevision())),
          Math.min(observed.finalizedSequence(), Math.min(kvBoundary.safeSequence(), eventBoundary.safeSequence())),
          authority, protections, scope);
    });
  }

  private CompletableFuture<HistoryGcPermit> confirmHistoryPermit(CommitState observed,
      CompactionOperation operation, SnapshotGcBoundary boundary,
      CommitOwnerService.OwnerToken owner, RequestScope scope) {
    return control.read(scope).thenApply(found -> {
      CommitState current = found.orElseThrow(() -> new IllegalStateException("missing control")).state();
      requireOwner(current, owner);
      if (!current.profileFingerprint().equals(observed.profileFingerprint())
          || !current.compactOperationId().equals(observed.compactOperationId())
          || current.compactRevision() != observed.compactRevision()
          || current.finalizedSequence() < observed.finalizedSequence()
          || scope.isClosed() || scope.isDeadlineExceeded()) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      return new HistoryGcPermit(observed.profileFingerprint(), pins.registryIdentity(), operation.id(),
          Math.addExact(operation.controlVersion(), 1),
          Math.min(observed.compactRevision(), boundary.safeRevision()),
          Math.min(observed.finalizedSequence(), boundary.safeSequence()), scope);
    });
  }

  private void requireOwner(CommitState state, CommitOwnerService.OwnerToken owner) {
    if (state.ownerEpoch() != owner.epoch() || state.ownerInstance().filter(owner.instance()::equals).isEmpty()
        || state.ownerLeaseExpired(clock.millis())) {
      throw new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
  }
}
