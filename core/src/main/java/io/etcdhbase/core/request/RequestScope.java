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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/** Owns the deadline, cancellation state, and request-scoped resources for one operation. */
public final class RequestScope implements AutoCloseable {
  private final boolean hasDeadline;
  private final long deadlineNanos;
  private final LongSupplier nanoTime;
  private final Object lock = new Object();
  private final Deque<AutoCloseable> cleanupOrder = new ArrayDeque<>();
  private final Set<AutoCloseable> registrations =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private final AtomicInteger cleanupFailureCount = new AtomicInteger();

  private boolean cancelled;
  private boolean closed;

  public static RequestScope withTimeout(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must not be negative");
    }
    long timeoutNanos;
    try {
      timeoutNanos = timeout.toNanos();
    } catch (ArithmeticException overflow) {
      return withoutDeadline();
    }
    if (timeoutNanos == Long.MAX_VALUE) {
      return withoutDeadline();
    }
    return new RequestScope(timeoutNanos, System::nanoTime);
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

  /** Returns the remaining monotonic deadline budget, or {@link Long#MAX_VALUE} if unbounded. */
  public long remainingDeadlineNanos() {
    if (!hasDeadline) {
      return Long.MAX_VALUE;
    }
    return Math.max(0, deadlineNanos - nanoTime.getAsLong());
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

  public boolean isClosed() {
    synchronized (lock) {
      return closed;
    }
  }

  public int cleanupFailureCount() {
    return cleanupFailureCount.get();
  }

  /** Cancels the scope and releases registered resources. Repeated cancellation is harmless. */
  public boolean cancel() {
    synchronized (lock) {
      if (cancelled || closed) {
        return false;
      }
      cancelled = true;
    }
    close();
    return true;
  }

  /** Registers a resource once by object identity; resources close in reverse registration order. */
  public void registerCleanup(AutoCloseable resource) {
    Objects.requireNonNull(resource, "resource");
    boolean closeImmediately;
    synchronized (lock) {
      if (!registrations.add(resource)) {
        return;
      }
      closeImmediately = closed;
      if (!closeImmediately) {
        cleanupOrder.push(resource);
      }
    }
    if (closeImmediately) {
      closeResource(resource);
    }
  }

  /**
   * Binds scope lifetime to the terminal completion of the owned asynchronous operation.
   * Cancellation releases registered resources; operation-specific cancellation should also be
   * registered as cleanup when the underlying client supports it.
   */
  public <T> CompletionStage<T> own(CompletionStage<T> operation) {
    Objects.requireNonNull(operation, "operation");
    return operation.whenComplete((ignored, failure) -> close());
  }

  @Override
  public void close() {
    List<AutoCloseable> resources;
    synchronized (lock) {
      if (closed) {
        return;
      }
      closed = true;
      resources = new ArrayList<>(cleanupOrder);
      cleanupOrder.clear();
    }

    Error fatalFailure = null;
    for (AutoCloseable resource : resources) {
      try {
        resource.close();
      } catch (Exception failure) {
        cleanupFailureCount.incrementAndGet();
      } catch (Error failure) {
        cleanupFailureCount.incrementAndGet();
        if (fatalFailure == null) {
          fatalFailure = failure;
        }
      }
    }
    if (fatalFailure != null) {
      throw fatalFailure;
    }
  }

  private void closeResource(AutoCloseable resource) {
    try {
      resource.close();
    } catch (Exception failure) {
      cleanupFailureCount.incrementAndGet();
    }
  }

  private static long requireNonNegative(long timeoutNanos) {
    if (timeoutNanos < 0) {
      throw new IllegalArgumentException("timeout must not be negative");
    }
    return timeoutNanos;
  }

  private static long deadline(long timeoutNanos, LongSupplier nanoTime) {
    long start = Objects.requireNonNull(nanoTime, "nanoTime").getAsLong();
    return start + requireNonNegative(timeoutNanos);
  }
}
