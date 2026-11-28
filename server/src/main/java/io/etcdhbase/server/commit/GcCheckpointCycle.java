package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.EventHistoryRetention;
import io.etcdhbase.core.commit.GcCheckpoint;
import io.etcdhbase.core.commit.GcCheckpointStore;
import io.etcdhbase.core.commit.GcCoordinator;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.SystemRecordClosure;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.server.commit.GcCheckpointWorker.Outcome;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Performs at most one durable bucket page per invocation, outside the mutation owner lane. */
final class GcCheckpointCycle {
  static final BudgetManager.Reservation MEMORY = new BudgetManager.Reservation(64L << 10, 16L << 20,
      2 * ManifestCatalog.maximumProofReadPageBytes(), 16L << 20, 16L << 20);
  private static final BudgetManager.Reservation CHECKPOINT_MEMORY =
      new BudgetManager.Reservation(64L << 10, 1L << 20, 1L << 20, 1L << 20, 1L << 20);
  private static final SystemRecordClosure.Domain[] DOMAINS = SystemRecordClosure.Domain.values();
  private final String group;
  private final Supplier<Optional<CommitOwnerService.OwnerToken>> owner;
  private final CommitControlStore control;
  private final GcCoordinator coordinator;
  private final EventHistoryRetention retention;
  private final ByteKey authorization;
  private final SystemRecordClosure closure;
  private final GcCheckpointStore checkpoints;
  private final BudgetManager budgets;
  private final BudgetManager.Reservation reservation;
  private final int buckets;
  private final int pageRecords;
  private final RequestMetrics metrics;
  private int shard;

  private sealed interface PageWork permits Deferred, Collected {}
  private record Deferred(Outcome outcome) implements PageWork {}
  private record Collected(CommitOwnerService.OwnerToken owner, GcCheckpointStore.Held acquired,
      SystemRecordClosure.Page page) implements PageWork {}

  GcCheckpointCycle(String group, Supplier<Optional<CommitOwnerService.OwnerToken>> owner,
      CommitControlStore control, GcCoordinator coordinator, EventHistoryRetention retention,
      ByteKey authorization, SystemRecordClosure closure, GcCheckpointStore checkpoints,
      BudgetManager budgets, BudgetManager.Reservation reservation, int buckets, int pageRecords,
      RequestMetrics metrics) {
    this.group = Objects.requireNonNull(group, "group");
    this.owner = Objects.requireNonNull(owner, "owner");
    this.control = Objects.requireNonNull(control, "control");
    this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
    this.retention = Objects.requireNonNull(retention, "retention");
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.closure = Objects.requireNonNull(closure, "closure");
    this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.reservation = Objects.requireNonNull(reservation, "reservation");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    if (group.isBlank() || authorization.length() != 32 || buckets < 1 || buckets > 65_536
        || pageRecords < 1 || pageRecords > 1024) throw new IllegalArgumentException("invalid GC cycle");
    this.buckets = buckets;
    this.pageRecords = pageRecords;
  }

  CompletableFuture<Outcome> run(RequestScope scope) {
    var token = owner.get();
    if (token.isEmpty()) return CompletableFuture.completedFuture(Outcome.IDLE);
    var page = new DrainingRequestScope(scope);
    CompletableFuture<PageWork> work;
    try { work = runPage(token.orElseThrow(), page.scope()); }
    catch (RuntimeException | Error failure) { work = CompletableFuture.failedFuture(failure); }
    return page.finish(work).thenCompose(completed -> completed instanceof Deferred deferred
        ? CompletableFuture.completedFuture(deferred.outcome()) : persist((Collected) completed, scope));
  }

  private CompletableFuture<PageWork> runPage(CommitOwnerService.OwnerToken token, RequestScope scope) {
    BudgetManager.Account account;
    try {
      account = GcPageAdmission.admit(budgets, group, scope, reservation,
          new BudgetManager.ReadLimits(65_536, 512L << 20, 16_384, 16_384, 16_384));
    } catch (BackendFailure rejected) {
      if (rejected.category() != BackendFailure.Category.RESOURCE_LIMIT) {
        return CompletableFuture.failedFuture(rejected);
      }
      metrics.recordGcBacklog();
      return CompletableFuture.completedFuture(new Deferred(Outcome.BACKLOG));
    }
    var proof = new ManifestCatalog.ProofReadBudget(scope, account, 16_384, 256L << 20);
    return control.read(scope).thenCompose(found -> {
      var state = found.orElseThrow(() -> new IllegalStateException("missing GC control")).state();
      // No selected Compact and a publication in flight are healthy waiting states. Authorization
      // failures after this observation remain failures; the coordinator independently fences them.
      if (state.compactOperationId().isEmpty() || state.publication().isPresent()) {
        return CompletableFuture.completedFuture(new Deferred(Outcome.IDLE));
      }
      requireOwner(token, scope);
      return coordinator.authorizeSystemRecords(token, retention, authorization, proof, scope)
          .thenCompose(permit -> collect(token, permit, scope));
    });
  }

  private CompletableFuture<PageWork> collect(CommitOwnerService.OwnerToken token,
      SystemRecordRetirement.Permit permit, RequestScope scope) {
    requireOwner(token, scope);
    int bucket = shard % buckets;
    var domain = DOMAINS[shard / buckets];
    var proposed = GcCheckpoint.start(group, domain, token.epoch(), bucket, buckets, permit);
    return checkpoints.claim(proposed, scope).thenCompose(claim -> {
      requireOwner(token, scope);
      if (claim instanceof GcCheckpointStore.Superseded) return CompletableFuture.completedFuture(new Deferred(nextShard()));
      var acquired = (GcCheckpointStore.Held) claim;
      var sweep = acquired.checkpoint().plan();
      var pagePermit = sweep.equals(GcCheckpoint.Plan.from(permit)) ? permit
          : permit.restrictTo(sweep.safeRevision(), sweep.safeSequence());
      boolean fencing = acquired.checkpoint().phase() == GcCheckpoint.Phase.FENCING;
      var page = fencing
          ? closure.fence(domain, bucket, acquired.checkpoint().fencePosition().afterIdentity(), pageRecords, pagePermit, scope)
          : closure.collect(domain, bucket, acquired.checkpoint().position().afterIdentity(), pageRecords, pagePermit, scope);
      return page.thenApply(completed -> new Collected(token, acquired, completed));
    });
  }

  private CompletableFuture<Outcome> persist(Collected collected, RequestScope scope) {
    requireOwner(collected.owner(), scope);
    try {
      GcPageAdmission.admit(budgets, group, scope, CHECKPOINT_MEMORY,
          new BudgetManager.ReadLimits(16, 1L << 20, 16, 1, 16), new BudgetManager.Work(0, 1, 0, 1, 0));
    } catch (BackendFailure rejected) {
      if (rejected.category() != BackendFailure.Category.RESOURCE_LIMIT) return CompletableFuture.failedFuture(rejected);
      metrics.recordGcBacklog();
      return CompletableFuture.completedFuture(Outcome.BACKLOG);
    }
    return control.read(scope).thenCompose(found -> {
      var state = found.orElseThrow(() -> new IllegalStateException("missing GC control")).state();
      var plan = collected.acquired().checkpoint().plan();
      requireOwner(collected.owner(), scope);
      if (state.ownerEpoch() != collected.owner().epoch()
          || !state.ownerInstance().equals(Optional.of(collected.owner().instance()))
          || state.ownerLeaseExpired(System.currentTimeMillis())
          || !state.profileFingerprint().equals(plan.registries().profile())
          || !state.compactOperationId().equals(Optional.of(plan.operationId()))) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      return checkpoints.advance(collected.acquired(), collected.page(), scope).thenApply(updated -> {
        if (!updated) return Outcome.IDLE;
        metrics.recordGcPage(collected.page().examined(), collected.page().retired());
        var next = nextShard();
        return collected.acquired().checkpoint().phase() == GcCheckpoint.Phase.SCANNING
            && collected.page().exhausted() ? next : Outcome.PROGRESSED;
      });
    });
  }

  private Outcome nextShard() {
    shard = (shard + 1) % (DOMAINS.length * buckets);
    return shard == 0 ? Outcome.IDLE : Outcome.PROGRESSED;
  }

  private void requireOwner(CommitOwnerService.OwnerToken expected, RequestScope scope) {
    if (scope.isClosed() || scope.isDeadlineExceeded() || !owner.get().equals(Optional.of(expected))) {
      throw new java.util.concurrent.CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
  }

}
