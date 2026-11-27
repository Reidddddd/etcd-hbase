package io.etcdhbase.server.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.observability.RequestMetrics;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Schedules one GC page at a time and owns cancellation through physical and cleanup settlement. */
final class GcCheckpointWorker {
  private static final Logger LOG = LoggerFactory.getLogger(GcCheckpointWorker.class);
  private static final Executor CLEANUP = task -> Thread.ofVirtual().name("gc-cleanup").start(task);
  private final Object lock = new Object();
  private final Executor executor;
  private final ScheduledExecutorService timer;
  private final Duration timeout;
  private final Function<RequestScope, CompletableFuture<GcCheckpointCycle.Outcome>> cycle;
  private final RequestMetrics metrics;
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private ScheduledFuture<?> scheduled;
  private RequestScope active;
  private BackendFailure.Category lastFailure;
  private Throwable drainFailure;
  private boolean started;
  private boolean running;

  GcCheckpointWorker(Executor executor, ScheduledExecutorService timer, Duration timeout,
      Function<RequestScope, CompletableFuture<GcCheckpointCycle.Outcome>> cycle, RequestMetrics metrics) {
    this.executor = Objects.requireNonNull(executor, "executor");
    this.timer = Objects.requireNonNull(timer, "timer");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.cycle = Objects.requireNonNull(cycle, "cycle");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofMinutes(5)) > 0) {
      throw new IllegalArgumentException("invalid GC timeout");
    }
  }

  void start() {
    synchronized (lock) {
      if (started) throw new IllegalStateException("GC worker cannot restart");
      started = true;
      running = true;
      schedule(0);
    }
  }

  void stop() {
    RequestScope cancel;
    synchronized (lock) {
      started = true;
      running = false;
      if (scheduled != null) scheduled.cancel(false);
      scheduled = null;
      cancel = active;
      if (cancel == null) completeDrain();
    }
    if (cancel != null) cancel.cancelAsync(CLEANUP);
  }

  CompletableFuture<Void> drained() { return drained.copy(); }

  private void schedule(long delay) {
    try {
      scheduled = timer.schedule(this::dispatch, delay, TimeUnit.MILLISECONDS);
    } catch (RuntimeException rejection) {
      running = false;
      recordFailure(rejection);
      drainFailure = append(drainFailure, rejection);
      completeDrain();
    }
  }

  private void dispatch() {
    RequestScope scope;
    synchronized (lock) {
      scheduled = null;
      if (!running || active != null) return;
      scope = RequestScope.withTimeout(timeout);
      active = scope;
    }
    try {
      executor.execute(() -> run(scope));
    } catch (RuntimeException | Error rejection) {
      settle(scope, null, rejection);
      if (rejection instanceof Error fatal) throw fatal;
    }
  }

  private void run(RequestScope scope) {
    CompletableFuture<GcCheckpointCycle.Outcome> operation;
    try {
      synchronized (lock) {
        if (!running) {
          settle(scope, GcCheckpointCycle.Outcome.IDLE, null);
          return;
        }
      }
      // A concurrent stop closes this scope; every real cycle/adapter checks it before starting I/O.
      operation = Objects.requireNonNull(cycle.apply(scope), "GC cycle future");
    } catch (RuntimeException | Error failure) {
      settle(scope, null, failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }
    operation.whenComplete((outcome, failure) -> settle(scope, outcome, failure));
  }

  private void settle(RequestScope scope, GcCheckpointCycle.Outcome outcome, Throwable failure) {
    try { scope.closeAsync(CLEANUP); }
    catch (RuntimeException | Error cleanup) { failure = append(failure, cleanup); }
    Throwable primary = failure;
    var cleanup = scope.cleanupOutcomeCompletion().handle((ignored, problem) -> unwrap(problem)).toCompletableFuture();
    var producers = scope.physicalProducersCompletion().handle((ignored, problem) -> unwrap(problem)).toCompletableFuture();
    CompletableFuture.allOf(cleanup, producers).whenComplete((ignored, problem) -> {
      Throwable releaseFailure = append(cleanup.join(), producers.join());
      if (scope.cleanupFailureCount() != 0 && cleanup.join() == null) {
        releaseFailure = append(releaseFailure, new IllegalStateException("GC cleanup failed"));
      }
      finish(scope, outcome, append(primary, releaseFailure), releaseFailure);
    });
  }

  private void finish(RequestScope scope, GcCheckpointCycle.Outcome outcome, Throwable failure,
      Throwable releaseFailure) {
    synchronized (lock) {
      if (active != scope) throw new IllegalStateException("GC scope settled twice");
      active = null;
      // Cleanup failures must also survive a later stop after a retry succeeded.
      drainFailure = append(drainFailure, releaseFailure);
      if (!running) { completeDrain(); return; }
      if (failure != null) recordFailure(failure);
      else lastFailure = null;
      schedule(failure != null || outcome == GcCheckpointCycle.Outcome.BACKLOG ? 1000
          : outcome == GcCheckpointCycle.Outcome.PROGRESSED ? 20 : 1000);
    }
  }

  private void recordFailure(Throwable failure) {
    Throwable cause = unwrap(failure);
    var category = cause instanceof BackendFailure backend ? backend.category() : BackendFailure.Category.UNKNOWN;
    metrics.recordGcFailure();
    if (lastFailure != category) LOG.warn("System-record GC paused for retry: {}", category);
    lastFailure = category;
  }

  private void completeDrain() {
    if (drainFailure == null) drained.complete(null);
    else drained.completeExceptionally(drainFailure);
  }

  private static Throwable unwrap(Throwable failure) {
    return failure == null ? null : CompletionFailures.unwrapCompletionPreservingSuppressed(failure);
  }

  private static Throwable append(Throwable primary, Throwable additional) {
    if (primary == null) return additional;
    if (additional != null && primary != additional) primary.addSuppressed(additional);
    return primary;
  }
}
