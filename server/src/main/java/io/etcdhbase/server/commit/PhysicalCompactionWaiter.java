package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CompactionOperation;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/** Owns RPC completion polling; cancellation detaches the waiter, never the durable GC task. */
final class PhysicalCompactionWaiter implements CompactGateway.PhysicalCompletion, AutoCloseable {
  enum Progress { ADVANCED, WAITING, COMPLETE }
  private static final Executor CLEANUP = task -> Thread.ofVirtual().name("compact-wait-cleanup").start(task);
  private static final BudgetManager.Reservation MEMORY =
      new BudgetManager.Reservation(64L << 10, 1L << 20, 1L << 20, 0, 0);
  private final Object lock = new Object();
  private final String group;
  private final BudgetManager budgets;
  private final ScheduledExecutorService timer;
  private final Executor executor;
  private final long maximumWaitNanos;
  private final Supplier<BiFunction<CompactionOperation, RequestScope, CompletableFuture<Progress>>> scans;
  private final Set<Wait> active = new HashSet<>();
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private boolean closed;
  private Throwable cleanupFailure;

  PhysicalCompactionWaiter(String group, BudgetManager budgets, ScheduledExecutorService timer,
      Executor executor, Duration maximumWait,
      Supplier<BiFunction<CompactionOperation, RequestScope, CompletableFuture<Progress>>> scans) {
    this.group = Objects.requireNonNull(group, "group");
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.timer = Objects.requireNonNull(timer, "timer");
    this.executor = Objects.requireNonNull(executor, "executor");
    this.scans = Objects.requireNonNull(scans, "scans");
    maximumWaitNanos = maximumWait.toNanos();
    if (maximumWaitNanos <= 0) throw new IllegalArgumentException("invalid Compact wait timeout");
  }

  @Override
  public CompletableFuture<Void> awaitCompletion(CompactionOperation target, RequestScope request) {
    Wait wait;
    synchronized (lock) {
      if (closed) return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      wait = new Wait(target, request);
      active.add(wait);
    }
    wait.start(request);
    return wait.response.copy();
  }

  @Override
  public void close() {
    java.util.List<Wait> stopping;
    synchronized (lock) {
      closed = true;
      stopping = java.util.List.copyOf(active);
      completeDrain();
    }
    stopping.forEach(wait -> wait.finish(new BackendFailure(BackendFailure.Category.UNAVAILABLE)));
  }

  CompletableFuture<Void> drained() { return drained.copy(); }

  private void completeDrain() {
    if (!closed || !active.isEmpty()) return;
    if (cleanupFailure == null) drained.complete(null);
    else drained.completeExceptionally(cleanupFailure);
  }

  private final class Wait {
    private final CompactionOperation target;
    private final RequestScope scope;
    private final BiFunction<CompactionOperation, RequestScope, CompletableFuture<Progress>> scan;
    private final CompletableFuture<Void> response = new CompletableFuture<>();
    private final CompletableFuture<Void> released = new CompletableFuture<>();
    private RequestScope.ScopedCleanup registration;
    private ScheduledFuture<?> scheduled;
    private ScheduledFuture<?> deadline;
    private boolean finished;

    private Wait(CompactionOperation target, RequestScope request) {
      this.target = Objects.requireNonNull(target, "target");
      scope = request.childWithTimeoutNanos(maximumWaitNanos);
      scan = Objects.requireNonNull(scans.get(), "Compact progress scan");
    }

    private void start(RequestScope request) {
      try {
        GcPageAdmission.admit(budgets, group, scope, MEMORY,
            new BudgetManager.ReadLimits(65_536, 512L << 20, 65_536, 1, 65_536),
            new BudgetManager.Work(0, 1, 0, 1, 0));
        registration = request.registerScopedCleanup(() -> finish(new BackendFailure(
            request.isDeadlineExceeded() ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED)), released);
        synchronized (lock) {
          if (!finished) {
            deadline = timer.schedule(() -> finish(new BackendFailure(BackendFailure.Category.TIMEOUT)),
                scope.remainingDeadlineNanos(), TimeUnit.NANOSECONDS);
            schedule(0);
          }
        }
        if (released.isDone()) registration.close();
      } catch (Exception failure) {
        // Admission, scoped cleanup and executor rejection all end this wait; none cancels GC.
        finish(failure);
      } catch (Error fatal) {
        finish(fatal);
        throw fatal;
      }
    }

    private void schedule(long delayMillis) {
      synchronized (lock) {
        if (!finished) scheduled = timer.schedule(this::dispatch, delayMillis, TimeUnit.MILLISECONDS);
      }
    }

    private void dispatch() {
      synchronized (lock) {
        scheduled = null;
        if (finished) return;
      }
      try { executor.execute(this::poll); }
      catch (RuntimeException rejection) { finish(new BackendFailure(BackendFailure.Category.UNAVAILABLE, rejection)); }
    }

    private void poll() {
      synchronized (lock) { if (finished) return; }
      try {
        scan.apply(target, scope).whenComplete((progress, failure) -> {
          if (failure != null) { finish(CompletionFailures.unwrapCompletion(failure)); return; }
          try {
            switch (progress) {
              case COMPLETE -> finish(null);
              case ADVANCED -> schedule(0);
              case WAITING -> schedule(100);
            }
          } catch (RuntimeException problem) { finish(problem); }
        });
      } catch (RuntimeException failure) { finish(failure); }
      catch (Error fatal) { finish(fatal); throw fatal; }
    }

    private void finish(Throwable failure) {
      if (failure == null && scope.isDeadlineExceeded()) failure = new BackendFailure(BackendFailure.Category.TIMEOUT);
      synchronized (lock) {
        if (finished) return;
        finished = true;
        if (scheduled != null) scheduled.cancel(false);
        if (deadline != null) deadline.cancel(false);
      }
      if (failure != null) response.completeExceptionally(failure);
      try { scope.closeAsync(CLEANUP); }
      catch (RuntimeException | Error problem) { failure = append(failure, problem); }
      Throwable primary = failure;
      var cleanup = scope.cleanupOutcomeCompletion().toCompletableFuture();
      var producers = scope.physicalProducersCompletion().toCompletableFuture();
      CompletableFuture.allOf(cleanup, producers).whenComplete((ignored, problem) -> release(primary, problem));
    }

    private void release(Throwable failure, Throwable problem) {
      Throwable cleanup = problem == null ? null : CompletionFailures.unwrapCompletion(problem);
      if (cleanup == null && scope.cleanupFailureCount() != 0) cleanup = new IllegalStateException("Compact wait cleanup failed");
      if (failure == null && cleanup == null) response.complete(null);
      else response.completeExceptionally(failure == null ? cleanup : failure);
      if (cleanup == null) released.complete(null);
      else released.completeExceptionally(cleanup);
      try { if (registration != null) registration.close(); }
      catch (Exception detach) { cleanup = append(cleanup, detach); }
      synchronized (lock) {
        active.remove(this);
        cleanupFailure = append(cleanupFailure, cleanup);
        completeDrain();
      }
    }
  }

  private static Throwable append(Throwable primary, Throwable additional) {
    if (primary == null) return additional;
    if (additional != null && primary != additional) primary.addSuppressed(additional);
    return primary;
  }
}
