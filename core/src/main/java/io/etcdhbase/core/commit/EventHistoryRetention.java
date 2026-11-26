package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Combines durable backfill progress with the event pin floors used by a collector. */
public final class EventHistoryRetention {
  private final EventHistoryPinManager pins;
  private final ReadViewService views;
  private final EventIndexBackfillReader backfill;

  public EventHistoryRetention(EventHistoryPinManager pins, ReadViewService views,
      EventIndexBackfillReader backfill) {
    this.pins = Objects.requireNonNull(pins, "pins");
    this.views = Objects.requireNonNull(views, "views");
    this.backfill = Objects.requireNonNull(backfill, "backfill");
  }

  EventHistoryPinManager pins() { return pins; }

  /**
   * Opens a fresh auxiliary barrier before raising registration floors. The caller owns the
   * scope until collection drains; Compact and exact root references still constrain deletion.
   * An absent checkpoint prevents collection because an initial capture may be in flight.
   */
  public CompletableFuture<SnapshotGcBoundary> advance(long revision, long sequence,
      ByteKey authorizationDigest, RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(authorizationDigest, "authorizationDigest");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(budget, "budget");
    if (revision < 0 || sequence < 0 || !budget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid event collection request"));
    }
    var requested = new SnapshotGcBoundary(revision, sequence, List.of());
    var output = new CompletableFuture<SnapshotGcBoundary>();
    output.whenComplete((ignored, failure) -> { if (output.isCancelled()) scope.cancel(); });
    views.openAuxiliary(authorizationDigest, scope).thenCompose(pinned ->
        backfill.retainPendingHistory(requested, pins.registryIdentity(), pinned, scope, budget)
            .thenCompose(limit -> pins.advanceRegistrationFloors(
                limit.safeRevision(), limit.safeSequence(), scope).thenApply(candidate ->
                    new SnapshotGcBoundary(Math.min(candidate.safeRevision(), limit.safeRevision()),
                        Math.min(candidate.safeSequence(), limit.safeSequence()), candidate.protections())))
            .whenComplete((ignored, failure) -> {
              if (failure != null) pinned.close();
            })).whenComplete((boundary, failure) -> {
              if (failure != null) output.completeExceptionally(failure);
              else if (!output.complete(boundary)) scope.cancel();
            });
    return output;
  }
}
