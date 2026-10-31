package io.etcdhbase.core.request;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/** Owns request resources; stopping new work and completing cleanup are separate transitions. */
public final class RequestScope implements AutoCloseable {
  private final boolean hasDeadline;
  private final long deadlineNanos;
  private final LongSupplier nanoTime;
  private final Object lock = new Object();
  private final Deque<AutoCloseable> cleanupOrder = new ArrayDeque<>();
  private final Set<AutoCloseable> registrations =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private final AtomicInteger cleanupFailureCount = new AtomicInteger();
  private final CompletableFuture<Void> cleanupCompletion = new CompletableFuture<>();
  private boolean cancelled;
  private boolean closed;
  private boolean cleanupFinished;
  private int pendingCleanup;

  public static RequestScope withTimeout(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) throw new IllegalArgumentException("timeout must not be negative");
    long nanos;
    try {
      nanos = timeout.toNanos();
    } catch (ArithmeticException overflow) {
      nanos = Long.MAX_VALUE;
    }
    return new RequestScope(nanos, System::nanoTime);
  }

  public static RequestScope withoutDeadline() {
    return new RequestScope(false, 0, System::nanoTime);
  }

  RequestScope(long timeoutNanos, LongSupplier nanoTime) {
    this(true, deadline(timeoutNanos, nanoTime), nanoTime);
  }

  private RequestScope(boolean hasDeadline, long deadlineNanos, LongSupplier nanoTime) {
    this.hasDeadline = hasDeadline;
    this.deadlineNanos = deadlineNanos;
    this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
  }

  public long remainingDeadlineNanos() {
    return hasDeadline ? Math.max(0, deadlineNanos - nanoTime.getAsLong()) : Long.MAX_VALUE;
  }

  public boolean isDeadlineExceeded() {
    return hasDeadline && remainingDeadlineNanos() == 0;
  }

  public boolean hasDeadline() {
    return hasDeadline;
  }

  public boolean isCancelled() {
    synchronized (lock) {
      return cancelled;
    }
  }

  /** True once new work is stopped, including while another caller is still closing resources. */
  public boolean isClosed() {
    synchronized (lock) {
      return closed;
    }
  }

  public int cleanupFailureCount() {
    return cleanupFailureCount.get();
  }

  /** Read-only completion of all resources registered before or during the closing batch. */
  public CompletionStage<Void> cleanupCompletion() {
    return cleanupCompletion.minimalCompletionStage();
  }

  public boolean cancel() {
    return finish(true, Runnable::run);
  }

  @Override
  public void close() {
    finish(false, Runnable::run);
  }

  public boolean cancelAsync(Executor cleanupExecutor) {
    return finish(true, cleanupExecutor);
  }

  public boolean closeAsync(Executor cleanupExecutor) {
    return finish(false, cleanupExecutor);
  }

  /** Tracks request-owned work, never the producer future of a shared connection. */
  public <T> CompletableFuture<T> track(CompletableFuture<T> operation) {
    Objects.requireNonNull(operation, "operation");
    AutoCloseable cancellation = () -> operation.cancel(true);
    registerCleanup(cancellation);
    operation.whenComplete(
        (value, failure) -> {
          synchronized (lock) {
            cleanupOrder.remove(cancellation);
            registrations.remove(cancellation);
          }
        });
    return operation;
  }

  /** Identity-deduplicated, reverse-order cleanup; a late resource is closed synchronously. */
  public void registerCleanup(AutoCloseable resource) {
    Objects.requireNonNull(resource, "resource");
    boolean joinsClosingBatch;
    synchronized (lock) {
      if (!registrations.add(resource)) return;
      if (!closed) {
        cleanupOrder.push(resource);
        return;
      }
      joinsClosingBatch = !cleanupFinished;
      if (joinsClosingBatch) pendingCleanup++;
    }
    closeResource(resource, joinsClosingBatch);
  }

  /** Transfers one resource into a releasable owner; finished pages leave no retained registration. */
  public ScopedCleanup registerScopedCleanup(AutoCloseable resource) {
    return registerScopedCleanup(resource, CompletableFuture.completedFuture(null));
  }

  /** The resource initiates cleanup; actualCompletion must settle after all physical cleanup ends. */
  public ScopedCleanup registerScopedCleanup(AutoCloseable resource, CompletionStage<Void> actualCompletion) {
    var registration = new ScopedCleanup(Objects.requireNonNull(resource, "resource"),
        Objects.requireNonNull(actualCompletion, "actualCompletion"));
    registerCleanup(registration);
    return registration;
  }

  int registeredResourceCount() {
    synchronized (lock) {
      return registrations.size();
    }
  }

  public final class ScopedCleanup implements AutoCloseable {
    private final AutoCloseable resource;
    private final CompletionStage<Void> actualCompletion;
    private final AtomicBoolean closeStarted = new AtomicBoolean();
    private final CompletableFuture<Void> closeCompletion = new CompletableFuture<>();

    private ScopedCleanup(AutoCloseable resource, CompletionStage<Void> actualCompletion) {
      this.resource = resource;
      this.actualCompletion = actualCompletion;
    }

    @Override
    public void close() throws Exception {
      if (!closeStarted.compareAndSet(false, true)) return;
      Throwable failure = null;
      try {
        resource.close();
      } catch (Exception | Error problem) {
        failure = problem;
        throw problem;
      } finally {
        Throwable synchronousFailure = failure;
        actualCompletion.whenComplete((ignored, completionFailure) ->
            completeClose(synchronousFailure == null ? completionFailure : synchronousFailure));
      }
    }

    private void completeClose(Throwable failure) {
      synchronized (lock) {
        cleanupOrder.remove(this);
        registrations.remove(this);
      }
      // Duplicate closes never wait, including reentrant scope cleanup. Only the physical
      // completion detaches this registration, and callbacks run outside the state lock.
      if (failure == null) closeCompletion.complete(null);
      else closeCompletion.completeExceptionally(failure);
    }
  }

  /** Root-operation completion stops the request without replacing its original failure. */
  public <T> CompletionStage<T> own(CompletionStage<T> operation) {
    return Objects.requireNonNull(operation, "operation")
        .whenComplete((ignored, failure) -> close());
  }

  private boolean finish(boolean cancellation, Executor executor) {
    Objects.requireNonNull(executor, "cleanupExecutor");
    List<AutoCloseable> resources;
    synchronized (lock) {
      if (closed) return false;
      cancelled = cancellation;
      closed = true;
      resources = new ArrayList<>(cleanupOrder);
      cleanupOrder.clear();
      // The batch sentinel keeps completion pending until the cleanup task itself has run.
      pendingCleanup = resources.size() + 1;
    }
    Runnable cleanup = () -> closeBatch(resources);
    try {
      executor.execute(cleanup);
    } catch (java.util.concurrent.RejectedExecutionException rejected) {
      // Generic callers retain cleanup responsibility when their executor refuses ownership.
      cleanup.run();
      throw rejected;
    }
    return true;
  }

  private void closeBatch(List<AutoCloseable> resources) {
    Error fatal = null;
    try {
      for (var resource : resources) {
        try {
          closeResource(resource, true);
        } catch (Error failure) {
          if (fatal == null) fatal = failure;
        }
      }
    } finally {
      resourceClosed();
    }
    if (fatal != null) throw fatal;
  }

  private void closeResource(AutoCloseable resource, boolean joinsClosingBatch) {
    var actualCloseCompletion =
        resource instanceof ScopedCleanup registration ? registration.closeCompletion : null;
    try {
      resource.close();
    } catch (Exception failure) {
      if (actualCloseCompletion == null) cleanupFailureCount.incrementAndGet();
    } catch (Error failure) {
      if (actualCloseCompletion == null) cleanupFailureCount.incrementAndGet();
      throw failure;
    } finally {
      if (actualCloseCompletion == null) {
        if (joinsClosingBatch) resourceClosed();
      } else {
        // Another caller may own a still-running close. The closing batch observes that
        // attempt's outcome rather than treating the duplicate close's return as completion.
        actualCloseCompletion.whenComplete((ignored, failure) -> {
          if (failure != null) cleanupFailureCount.incrementAndGet();
          if (joinsClosingBatch) resourceClosed();
        });
      }
    }
  }

  private void resourceClosed() {
    boolean complete;
    synchronized (lock) {
      pendingCleanup--;
      complete = pendingCleanup == 0;
      if (complete) cleanupFinished = true;
    }
    // Completion callbacks can re-enter this scope; never run them under its state lock.
    if (complete) cleanupCompletion.complete(null);
  }

  private static long deadline(long timeoutNanos, LongSupplier nanoTime) {
    if (timeoutNanos < 0) throw new IllegalArgumentException("timeout must not be negative");
    return Objects.requireNonNull(nanoTime, "nanoTime").getAsLong() + timeoutNanos;
  }
}
