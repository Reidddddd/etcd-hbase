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
    BudgetManager.Account account;
    try {
      account = GcPageAdmission.admit(budgets, group, scope, reservation,
          new BudgetManager.ReadLimits(65_536, 512L << 20, 16_384, 16_384, 16_384));
    } catch (BackendFailure rejected) {
      if (rejected.category() != BackendFailure.Category.RESOURCE_LIMIT) {
        return CompletableFuture.failedFuture(rejected);
      }
      metrics.recordGcBacklog();
      return CompletableFuture.completedFuture(Outcome.BACKLOG);
    }
    var proof = new ManifestCatalog.ProofReadBudget(scope, account, 16_384, 256L << 20);
    return control.read(scope).thenCompose(found -> {
      var state = found.orElseThrow(() -> new IllegalStateException("missing GC control")).state();
      // No selected Compact and a publication in flight are healthy waiting states. Authorization
      // failures after this observation remain failures; the coordinator independently fences them.
      if (state.compactOperationId().isEmpty() || state.publication().isPresent()) {
        return CompletableFuture.completedFuture(Outcome.IDLE);
      }
      requireOwner(token.orElseThrow(), scope);
      return coordinator.authorizeSystemRecords(token.orElseThrow(), retention, authorization, proof, scope)
          .thenCompose(permit -> collect(token.orElseThrow(), permit, scope));
    });
  }

  private CompletableFuture<Outcome> collect(CommitOwnerService.OwnerToken token,
      SystemRecordRetirement.Permit permit, RequestScope scope) {
    requireOwner(token, scope);
    int bucket = shard % buckets;
    var domain = DOMAINS[shard / buckets];
    var proposed = GcCheckpoint.start(group, domain, token.epoch(), bucket, buckets, permit);
    return checkpoints.claim(proposed, scope).thenCompose(claim -> {
      requireOwner(token, scope);
      if (claim instanceof GcCheckpointStore.Superseded) return CompletableFuture.completedFuture(nextShard());
      var acquired = (GcCheckpointStore.Held) claim;
      var sweep = acquired.checkpoint().plan();
      var pagePermit = sweep.equals(GcCheckpoint.Plan.from(permit)) ? permit
          : permit.restrictTo(sweep.safeRevision(), sweep.safeSequence());
      boolean fencing = acquired.checkpoint().phase() == GcCheckpoint.Phase.FENCING;
      var page = fencing
          ? closure.fence(domain, bucket, acquired.checkpoint().fencePosition().afterIdentity(), pageRecords, pagePermit, scope)
          : closure.collect(domain, bucket, acquired.checkpoint().position().afterIdentity(), pageRecords, pagePermit, scope);
      return page.thenCompose(completed -> {
            requireOwner(token, scope);
            return checkpoints.advance(acquired, completed, scope).thenApply(updated -> {
              if (!updated) return Outcome.IDLE;
              metrics.recordGcPage(completed.examined(), completed.retired());
              var next = nextShard();
              return !fencing && completed.exhausted() ? next : Outcome.PROGRESSED;
            });
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
