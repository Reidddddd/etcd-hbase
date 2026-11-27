package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.GcCoordinator;
import io.etcdhbase.core.commit.HistoryGcCheckpointStore;
import io.etcdhbase.core.commit.HistoryGcPermit;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.storage.HBaseHistoryCollector;
import io.etcdhbase.hbase.storage.HBaseHistoryKeyEnumerator;
import io.etcdhbase.server.commit.GcCheckpointWorker.Outcome;
import io.etcdhbase.server.observability.RequestMetrics;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/** Resumes at most one durably identified key/version page per maintenance tick. */
final class HistoryGcCycle {
  private static final int PAGE_VERSIONS = 64;
  private final String group;
  private final Supplier<Optional<CommitOwnerService.OwnerToken>> owner;
  private final CommitControlStore control;
  private final GcCoordinator coordinator;
  private final HistoryGcCheckpointStore checkpoints;
  private final HBaseHistoryKeyEnumerator enumerator;
  private final HBaseHistoryCollector collector;
  private final BudgetManager budgets;
  private final BudgetManager.Reservation reservation;
  private final RequestMetrics metrics;
  private final int buckets;
  private int bucket;

  HistoryGcCycle(String group, Supplier<Optional<CommitOwnerService.OwnerToken>> owner,
      CommitControlStore control, GcCoordinator coordinator, HistoryGcCheckpointStore checkpoints,
      HBaseHistoryKeyEnumerator enumerator, HBaseHistoryCollector collector,
      BudgetManager budgets, BudgetManager.Reservation reservation, RequestMetrics metrics, int buckets) {
    this.group = Objects.requireNonNull(group, "group");
    this.owner = Objects.requireNonNull(owner, "owner");
    this.control = Objects.requireNonNull(control, "control");
    this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
    this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
    this.enumerator = Objects.requireNonNull(enumerator, "enumerator");
    this.collector = Objects.requireNonNull(collector, "collector");
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.reservation = Objects.requireNonNull(reservation, "reservation");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    if (buckets < 1 || buckets > 65_536) throw new IllegalArgumentException("invalid history GC buckets");
    this.buckets = buckets;
  }

  CompletableFuture<Outcome> run(RequestScope scope) {
    var token = owner.get();
    if (token.isEmpty()) return CompletableFuture.completedFuture(Outcome.IDLE);
    try {
      GcPageAdmission.admit(budgets, group, scope, reservation,
          new BudgetManager.ReadLimits(65_536, 512L << 20, 16_384, 16_384, 16_384));
    } catch (BackendFailure rejected) {
      if (rejected.category() != BackendFailure.Category.RESOURCE_LIMIT) {
        return CompletableFuture.failedFuture(rejected);
      }
      metrics.recordGcBacklog();
      return CompletableFuture.completedFuture(Outcome.BACKLOG);
    }
    return control.read(scope).thenCompose(found -> {
      var state = found.orElseThrow(() -> new IllegalStateException("missing history GC control")).state();
      if (state.compactOperationId().isEmpty() || state.publication().isPresent()) {
        return CompletableFuture.completedFuture(Outcome.IDLE);
      }
      requireOwner(token.orElseThrow(), scope);
      return coordinator.authorizeHistory(token.orElseThrow(), scope)
          .thenCompose(permit -> collect(token.orElseThrow(), permit, scope));
    });
  }

  private CompletableFuture<Outcome> collect(CommitOwnerService.OwnerToken token,
      HistoryGcPermit permit, RequestScope scope) {
    requireOwner(token, scope);
    return checkpoints.claim(token.epoch(), bucket, permit, scope).thenCompose(found -> {
      if (found.isEmpty()) return CompletableFuture.completedFuture(nextBucket(false));
      var acquired = found.orElseThrow();
      var checkpoint = acquired.checkpoint();
      if (checkpoint.exhausted()) return CompletableFuture.completedFuture(nextBucket(false));
      // Revision zero has no materialized KV versions; bucket completion needs no business scan.
      if (checkpoint.safeRevision() == 0) {
        requireOwner(token, scope);
        return checkpoints.finishBucket(acquired, scope)
            .thenApply(updated -> updated ? nextBucket(true) : Outcome.IDLE);
      }
      var pagePermit = permit.restrictTo(checkpoint.safeRevision(), checkpoint.safeSequence());
      return checkpoint.activeKey().isPresent()
          ? collectActiveKey(token, acquired, pagePermit, scope)
          : enumerateKey(token, acquired, pagePermit, scope);
    });
  }

  private CompletableFuture<Outcome> collectActiveKey(CommitOwnerService.OwnerToken token,
      HistoryGcCheckpointStore.Held acquired, HistoryGcPermit permit, RequestScope scope) {
    var checkpoint = acquired.checkpoint();
    return collector.collectKey(permit, checkpoint.activeKey().orElseThrow(),
            checkpoint.afterRevision(), PAGE_VERSIONS, scope)
        .thenCompose(page -> {
          requireOwner(token, scope);
          return checkpoints.afterPage(acquired, page.lastRevision(), page.exhausted(),
              page.deletedVersions(), scope).thenApply(updated -> updated ? nextBucket(true) : Outcome.IDLE);
        });
  }

  private CompletableFuture<Outcome> enumerateKey(CommitOwnerService.OwnerToken token,
      HistoryGcCheckpointStore.Held acquired, HistoryGcPermit permit, RequestScope scope) {
    return enumerator.nextKey(permit, bucket, acquired.checkpoint().afterKey(), scope).thenCompose(key -> {
      requireOwner(token, scope);
      var persisted = key.isPresent()
          ? checkpoints.beginKey(acquired, key.orElseThrow(), scope)
          : checkpoints.finishBucket(acquired, scope);
      return persisted.thenApply(updated -> updated ? nextBucket(true) : Outcome.IDLE);
    });
  }

  private Outcome nextBucket(boolean progressed) {
    bucket = (bucket + 1) % buckets;
    return progressed || bucket != 0 ? Outcome.PROGRESSED : Outcome.IDLE;
  }

  private void requireOwner(CommitOwnerService.OwnerToken expected, RequestScope scope) {
    if (scope.isClosed() || scope.isDeadlineExceeded() || !owner.get().equals(Optional.of(expected))) {
      throw new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
  }
}
