package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.GcCheckpoint;
import io.etcdhbase.core.commit.GcCheckpointStore;
import io.etcdhbase.core.commit.SystemRecordClosure;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.observability.RequestMetrics;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Drives background system-record GC from persisted shard checkpoints.
 *
 * <p>§6.3 of the M5 design fixes the shape: shard by {@code recordDomain + bucket}, resume from the
 * durable checkpoint instead of rescanning, keep every update bound to the maintenance epoch that
 * claimed it, fence before takeover, and yield rather than compete with foreground, recovery,
 * renewal or expiry admission. A cycle that cannot get capacity records backlog and retries instead
 * of consuming a reserved lane.
 *
 * <p>The worker never deletes on its own. It claims a shard, asks the closure authority what is
 * unreachable, and only then advances the checkpoint after the closure page actually retired.
 */
public final class GcCheckpointWorker {
  private static final long IDLE_POLL_MILLIS = 1_000;
  private static final long PROGRESS_DELAY_MILLIS = 20;
  private static final long RETRY_DELAY_MILLIS = 250;
  private static final Executor CLEANUP =
      task -> Thread.ofVirtual().name("j52-gc-cleanup").start(task);

  private final Object lock = new Object();
  private final String tableGroup;
  private final SystemRecordClosure closure;
  private final GcCheckpointStore checkpoints;
  private final M3OwnerLease owner;
  private final Executor worker;
  private final ScheduledExecutorService timer;
  private final Clock clock;
  private final RequestMetrics metrics;
  private final io.etcdhbase.core.bytes.ByteKey profile;
  private final io.etcdhbase.core.bytes.ByteKey registryIdentity;
  private final io.etcdhbase.core.scan.BudgetManager budgets;
  private final io.etcdhbase.core.scan.BudgetManager.Reservation reservation;
  private final int bucketCount;
  private final int pageRecords;
  private final long timeoutMillis;
  private final CompletableFuture<Void> initialReadiness = new CompletableFuture<>();
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private final AtomicBoolean checkpointHealthy = new AtomicBoolean(false);
  private final AtomicLong checkpointOwnerEpoch = new AtomicLong(-1);
  private final AtomicLong backlogRounds = new AtomicLong();
  private ScheduledFuture<?> scheduled;
  private RequestScope activeScope;
  private boolean cycleOutstanding;
  private boolean running;

  public GcCheckpointWorker(String tableGroup, SystemRecordClosure closure,
      GcCheckpointStore checkpoints, M3OwnerLease owner, int bucketCount, int pageRecords,
      long timeoutMillis, io.etcdhbase.core.bytes.ByteKey profile,
      io.etcdhbase.core.bytes.ByteKey registryIdentity,
      io.etcdhbase.core.scan.BudgetManager budgets,
      io.etcdhbase.core.scan.BudgetManager.Reservation reservation,
      Executor worker, ScheduledExecutorService timer, Clock clock, RequestMetrics metrics) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.closure = Objects.requireNonNull(closure, "closure");
    this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
    this.owner = Objects.requireNonNull(owner, "owner");
    this.worker = Objects.requireNonNull(worker, "worker");
    this.timer = Objects.requireNonNull(timer, "timer");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.profile = Objects.requireNonNull(profile, "profile");
    this.registryIdentity = Objects.requireNonNull(registryIdentity, "registryIdentity");
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.reservation = Objects.requireNonNull(reservation, "reservation");
    if (profile.length() != 32 || registryIdentity.length() == 0) {
      throw new IllegalArgumentException("GC worker requires the real table-group identities");
    }
    if (tableGroup.isBlank() || bucketCount < 1 || bucketCount > 65_536
        || pageRecords < 1 || pageRecords > 1024
        || timeoutMillis < 1 || timeoutMillis > 300_000) {
      throw new IllegalArgumentException("invalid GC worker configuration");
    }
    this.bucketCount = bucketCount;
    this.pageRecords = pageRecords;
    this.timeoutMillis = timeoutMillis;
  }

  public void start() {
    synchronized (lock) {
      if (running || drained.isDone()) throw new IllegalStateException("GC worker cannot start twice");
      running = true;
      scheduleLocked(0);
    }
  }

  /** Ready only while this process still owns the table group at the epoch its checkpoints use. */
  public boolean isReady() {
    synchronized (lock) {
      if (!running) return false;
      Optional<CommitOwnerService.OwnerToken> token = owner.currentToken();
      return token.isEmpty() || (checkpointHealthy.get()
          && checkpointOwnerEpoch.get() == token.orElseThrow().epoch());
    }
  }

  CompletableFuture<Void> initialReadiness() { return initialReadiness.copy(); }

  /** Rounds that could not run because GC yielded its admission; reported, never silently dropped. */
  public long backlogRounds() { return backlogRounds.get(); }

  public void stop() {
    RequestScope toCancel;
    synchronized (lock) {
      if (!running) {
        if (!cycleOutstanding) drained.complete(null);
        return;
      }
      running = false;
      if (scheduled != null) {
        scheduled.cancel(false);
        scheduled = null;
      }
      toCancel = activeScope;
      if (!cycleOutstanding) drained.complete(null);
    }
    initialReadiness.completeExceptionally(
        new IllegalStateException("GC worker stopped before becoming ready"));
    if (toCancel != null) toCancel.cancelAsync(CLEANUP);
  }

  public CompletableFuture<Void> drained() { return drained.copy(); }

  private void scheduleLocked(long delayMillis) {
    if (!running || scheduled != null || cycleOutstanding) return;
    scheduled = timer.schedule(this::dispatch, delayMillis, TimeUnit.MILLISECONDS);
  }

  private void dispatch() {
    synchronized (lock) {
      scheduled = null;
      if (!running || cycleOutstanding) return;
      cycleOutstanding = true;
    }
    try {
      worker.execute(this::runCycle);
    } catch (RuntimeException rejected) {
      finishCycle(RETRY_DELAY_MILLIS, null, null);
    }
  }

  private void runCycle() {
    Optional<CommitOwnerService.OwnerToken> current = owner.currentToken();
    if (current.isEmpty()) {
      // Standing by is not a failure: a non-owner must not touch shards at all.
      completeInitialReadinessIfReady();
      finishCycle(IDLE_POLL_MILLIS, null, null);
      return;
    }
    CommitOwnerService.OwnerToken token = current.orElseThrow();
    RequestScope scope = RequestScope.withTimeout(Duration.ofMillis(timeoutMillis));
    synchronized (lock) {
      activeScope = scope;
    }
    var cycleDone = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(cycleDone))) {
      finishCycle(IDLE_POLL_MILLIS, scope, null);
      return;
    }
    final io.etcdhbase.core.scan.BudgetManager.Account account;
    try {
      // GC is regular work: when it cannot get capacity it yields and records backlog instead of
      // competing with recovery, renewal or expiry for the reserved lane.
      account = budgets.admit(tableGroup, scope, reservation);
    } catch (BackendFailure | RuntimeException rejected) {
      backlogRounds.incrementAndGet();
      metrics.recordLeaseExpiryRetry();
      finishCycle(RETRY_DELAY_MILLIS, scope, null);
      return;
    }
    // The permit is bound to the real profile and pin registry so it cannot authorize another table.
    var permit = SystemRecordRetirement.Permit.issue(profile, registryIdentity,
        io.etcdhbase.core.commit.InvocationId.create(), token.epoch(), 0, 0, scope);
    runShards(token, permit, scope).whenComplete((outcome, failure) -> {
      cycleDone.complete(null);
      if (failure != null) {
        metrics.recordLeaseExpiryRetry();
        finishCycle(RETRY_DELAY_MILLIS, scope, failure);
      } else if (outcome == Outcome.BACKLOG) {
        backlogRounds.incrementAndGet();
        metrics.recordLeaseExpiryRetry();
        finishCycle(RETRY_DELAY_MILLIS, scope, null);
      } else {
        finishCycle(PROGRESS_DELAY_MILLIS, scope, null);
      }
      account.close();
    });
  }

  private enum Outcome { PROGRESSED, BACKLOG, IDLE }

  private CompletableFuture<Outcome> runShards(CommitOwnerService.OwnerToken token,
      SystemRecordRetirement.Permit permit, RequestScope scope) {
    var progressed = new AtomicBoolean();
    CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
    for (SystemRecordClosure.Domain domain : SystemRecordClosure.Domain.values()) {
      for (int bucket = 0; bucket < bucketCount; bucket++) {
        final int current = bucket;
        chain = chain.thenCompose(ignored -> {
          if (scope.isClosed() || scope.isDeadlineExceeded()) return CompletableFuture.completedFuture(null);
          return runShard(domain, current, token, permit, scope, progressed);
        });
      }
    }
    return chain.thenApply(ignored -> progressed.get() ? Outcome.PROGRESSED : Outcome.IDLE);
  }

  private CompletableFuture<Void> runShard(SystemRecordClosure.Domain domain, int bucket,
      CommitOwnerService.OwnerToken token, SystemRecordRetirement.Permit permit,
      RequestScope scope, AtomicBoolean progressed) {
    var proposed = new GcCheckpoint(tableGroup, domain, token.epoch(), bucket, bucketCount,
        permit.safeSequence(), permit.safeRevision(), GcCheckpoint.Position.start(), 0);
    return checkpoints.claim(proposed, scope).thenCompose(claim -> {
      if (claim instanceof GcCheckpointStore.Superseded || claim instanceof GcCheckpointStore.Absent) {
        return CompletableFuture.completedFuture(null);
      }
      var acquired = (GcCheckpointStore.Acquired) claim;
      synchronized (lock) {
        checkpointHealthy.set(true);
        checkpointOwnerEpoch.set(acquired.checkpoint().maintenanceEpoch());
      }
      if (acquired.checkpoint().maintenanceEpoch() != token.epoch()) {
        // A newer epoch owns this shard; this worker stands down without writing.
        return CompletableFuture.completedFuture(null);
      }
      return closure.collect(domain, acquired.checkpoint().position().afterIdentity(), pageRecords,
              permit, scope).thenCompose(page -> {
                if (page.retired() == 0 && page.lastIdentity().equals(
                    acquired.checkpoint().position().afterIdentity())) {
                  // Nothing moved: do not burn budget re-reading the same page this round.
                  return CompletableFuture.completedFuture(null);
                }
                var next = new GcCheckpoint(acquired.checkpoint().tableGroupId(),
                    acquired.checkpoint().domain(), acquired.checkpoint().maintenanceEpoch(),
                    acquired.checkpoint().bucket(), acquired.checkpoint().bucketCount(),
                    acquired.checkpoint().protectedSequence(),
                    acquired.checkpoint().protectedRevision(), new GcCheckpoint.Position(
                        page.lastIdentity()), acquired.checkpoint().completedPages() + 1);
                return checkpoints.advance(next, acquired.exactBytes(), scope).thenApply(updated -> {
                  if (updated) {
                    progressed.set(true);
                    completeInitialReadinessIfReady();
                  }
                  return null;
                });
              });
    });
  }

  private void completeInitialReadinessIfReady() {
    if (checkpointHealthy.get()) initialReadiness.complete(null);
  }

  private void finishCycle(long delayMillis, RequestScope scope, Throwable failure) {
    boolean completeDrain;
    synchronized (lock) {
      if (scope != null && activeScope == scope) activeScope = null;
      cycleOutstanding = false;
      completeDrain = !running;
      if (!completeDrain) scheduleLocked(delayMillis);
    }
    if (completeDrain) {
      if (failure == null) drained.complete(null);
      else drained.completeExceptionally(failure);
    }
  }
}
