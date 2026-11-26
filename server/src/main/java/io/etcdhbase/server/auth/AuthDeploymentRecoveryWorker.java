package io.etcdhbase.server.auth;

import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Retries one local deployment-recovery cycle at a time and drains its physical work on stop. */
public final class AuthDeploymentRecoveryWorker {
  private static final Executor CLEANUP = task -> Thread.ofVirtual().name("auth-recovery-cleanup").start(task);
  private final Object lock = new Object();
  private final Function<RequestScope, CompletionStage<Void>> cycle;
  private final Duration timeout;
  private final long intervalMillis;
  private final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1,
      Thread.ofPlatform().daemon().name("auth-deployment-recovery").factory());
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private final CompletableFuture<Void> initiallyReady = new CompletableFuture<>();
  private final AtomicBoolean drainStarted = new AtomicBoolean();
  private ScheduledFuture<?> scheduled;
  private RequestScope active;
  private Throwable lastFailure;
  private Throwable drainFailure;
  private boolean started;
  private boolean running;

  public AuthDeploymentRecoveryWorker(Function<RequestScope, CompletionStage<Void>> cycle,
      Duration timeout, Duration interval) {
    this.cycle = Objects.requireNonNull(cycle, "cycle");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    Objects.requireNonNull(interval, "interval");
    if (timeout.isZero() || timeout.isNegative() || interval.toMillis() < 1) {
      throw new IllegalArgumentException("recovery timeout and interval must be positive");
    }
    intervalMillis = interval.toMillis();
    timer.setRemoveOnCancelPolicy(true);
  }

  public void start() {
    synchronized (lock) {
      if (started) throw new IllegalStateException("recovery worker already started");
      started = true;
      running = true;
      schedule(0);
    }
  }

  public Optional<Throwable> lastFailure() {
    synchronized (lock) { return Optional.ofNullable(lastFailure); }
  }

  public CompletableFuture<Void> drained() { return drained.copy(); }

  public CompletableFuture<Void> initialReadiness() { return initiallyReady.copy(); }

  public boolean isReady() {
    synchronized (lock) {
      return running && initiallyReady.isDone() && !initiallyReady.isCompletedExceptionally()
          && lastFailure == null;
    }
  }

  public void stop() {
    RequestScope cancel;
    boolean idle;
    synchronized (lock) {
      if (started && !running) return;
      started = true;
      running = false;
      if (scheduled != null) scheduled.cancel(false);
      scheduled = null;
      cancel = active;
      idle = cancel == null;
      timer.shutdown();
    }
    initiallyReady.completeExceptionally(new CancellationException("recovery worker stopped"));
    if (cancel != null) cancel.cancelAsync(CLEANUP);
    if (idle) finishDrain();
  }

  private void schedule(long delayMillis) {
    try {
      scheduled = timer.schedule(this::dispatch, delayMillis, TimeUnit.MILLISECONDS);
    } catch (RuntimeException failure) {
      running = false;
      lastFailure = failure;
      drainFailure = failure;
      initiallyReady.completeExceptionally(failure);
      timer.shutdown();
      finishDrain();
    }
  }

  private void dispatch() {
    RequestScope request;
    synchronized (lock) {
      scheduled = null;
      if (!running) return;
      request = RequestScope.withTimeout(timeout);
      active = request;
    }
    try {
      if (!request.startIfOpen(() -> Objects.requireNonNull(cycle.apply(request), "recovery cycle stage")
          .whenComplete((ignored, failure) -> settle(request, failure)))) {
        settle(request, new CancellationException("recovery scope closed before dispatch"));
      }
    } catch (RuntimeException | Error failure) {
      settle(request, failure);
      if (failure instanceof Error fatal) throw fatal;
    }
  }

  private void settle(RequestScope request, Throwable failure) {
    request.closeAsync(CLEANUP);
    CompletableFuture.allOf(request.cleanupOutcomeCompletion().toCompletableFuture(),
        request.physicalProducersCompletion().toCompletableFuture()).whenComplete((ignored, cleanupFailure) -> {
      boolean stop;
      boolean ready = false;
      synchronized (lock) {
        if (active != request) return;
        active = null;
        stop = !running;
        if (cleanupFailure != null) {
          lastFailure = cleanupFailure;
          drainFailure = cleanupFailure;
          running = false;
          stop = true;
          timer.shutdown();
        } else if (stop) drainFailure = null;
        else {
          lastFailure = failure;
          ready = lastFailure == null;
          schedule(intervalMillis);
        }
      }
      if (ready) initiallyReady.complete(null);
      if (cleanupFailure != null) initiallyReady.completeExceptionally(cleanupFailure);
      if (stop) finishDrain();
    });
  }

  private void finishDrain() {
    if (!drainStarted.compareAndSet(false, true)) return;
    CLEANUP.execute(() -> {
      boolean interrupted = false;
      try {
        while (true) {
          try {
            if (timer.awaitTermination(30, TimeUnit.SECONDS)) break;
          } catch (InterruptedException failure) {
            interrupted = true;
          }
        }
      } finally {
        if (interrupted) Thread.currentThread().interrupt();
      }
      Throwable failure;
      synchronized (lock) { failure = drainFailure; }
      if (failure == null) drained.complete(null);
      else drained.completeExceptionally(failure);
    });
  }
}
