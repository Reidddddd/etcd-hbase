package io.etcdhbase.grpc;

import io.etcdhbase.core.request.RequestScope;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enforces bounded admission and propagates client/server deadlines into request-owned resources.
 */
public final class RequestScopeInterceptor implements ServerInterceptor, AutoCloseable {
  static final Context.Key<RequestScope> SCOPE = Context.key("etcd-hbase-request-scope");
  static final Context.Key<String> TRACE = Context.key("etcd-hbase-trace-id");
  static final Context.Key<String> AUTH_TOKEN = Context.key("etcd-hbase-auth-token");
  private final ScheduledExecutorService scheduler;
  private final Semaphore admission;
  private final Object gate = new Object();
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private final java.util.concurrent.ThreadPoolExecutor cleanupExecutor;
  private boolean closing;
  private final int maxConcurrentRequests;
  private final Duration timeout;
  private final int maxRequestBytes;
  private final RequestCompletionRecorder recorder;
  private final AtomicLong observationFailures = new AtomicLong();

  public RequestScopeInterceptor(
      ScheduledExecutorService scheduler,
      int maxConcurrentRequests,
      Duration timeout,
      int maxRequestBytes,
      RequestCompletionRecorder recorder) {
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.recorder = Objects.requireNonNull(recorder, "recorder");
    if (maxConcurrentRequests < 1
        || maxRequestBytes < 1
        || timeout.isNegative()
        || timeout.isZero()
        || timeout.compareTo(Duration.ofMinutes(10)) > 0) {
      throw new IllegalArgumentException("invalid request budget");
    }
    this.admission = new Semaphore(maxConcurrentRequests);
    int cleanupWorkers = Math.min(maxConcurrentRequests, 4);
    this.cleanupExecutor =
        new java.util.concurrent.ThreadPoolExecutor(
            cleanupWorkers,
            cleanupWorkers,
            0,
            TimeUnit.MILLISECONDS,
            new java.util.concurrent.ArrayBlockingQueue<>(maxConcurrentRequests),
            Thread.ofPlatform().daemon().name("grpc-request-cleanup-", 0).factory());
    this.maxConcurrentRequests = maxConcurrentRequests;
    this.maxRequestBytes = maxRequestBytes;
  }

  public static RequestScope currentScope() {
    return Objects.requireNonNull(SCOPE.get(), "request scope is available only inside an RPC");
  }

  public static String currentTraceId() {
    return Objects.requireNonNull(TRACE.get(), "trace ID is available only inside an RPC");
  }

  /** The raw token is request-scoped and must never be copied into diagnostics or durable state. */
  public static Optional<String> currentAuthToken() {
    return Optional.ofNullable(AUTH_TOKEN.get());
  }

  public int activeRequests() {
    return maxConcurrentRequests - admission.availablePermits();
  }

  /** Completes after admission is closed and every admitted request has released its resources. */
  public CompletionStage<Void> drained() { return drained.minimalCompletionStage(); }

  public long observationFailureCount() {
    return observationFailures.get();
  }

  @Override
  public <RequestT, ResponseT> ServerCall.Listener<RequestT> interceptCall(
      ServerCall<RequestT, ResponseT> call,
      Metadata headers,
      ServerCallHandler<RequestT, ResponseT> next) {
    var methodClass =
        RequestLifecycle.classifyMethod(call.getMethodDescriptor().getFullMethodName());
    if (!acquire()) {
      try (var scope = RequestScope.withTimeout(timeout)) {
        scope.close();
        record(
            methodClass,
            RequestCompletionRecorder.Outcome.RESOURCE_EXHAUSTED,
            UUID.randomUUID().toString(),
            scope,
            0);
      }
      call.close(
          Status.RESOURCE_EXHAUSTED.withDescription("request capacity exhausted"), new Metadata());
      return new ServerCall.Listener<>() {};
    }
    RequestLifecycle<RequestT, ResponseT> lifecycle = null;
    Context.CancellableContext context = null;
    try {
      context = call.getMethodDescriptor().getType()
              == io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING
          ? Context.current().withCancellation()
          : Context.current().withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS, scheduler);
      lifecycle = new RequestLifecycle<>(call, context, methodClass, maxRequestBytes, this);
      return lifecycle.start(headers, next);
    } catch (RuntimeException failure) {
      if (lifecycle != null) lifecycle.fail(failure);
      else {
        if (context != null) context.close();
        release();
        call.close(
            Status.INTERNAL.withDescription("request initialization failed"), new Metadata());
      }
      return new ServerCall.Listener<>() {};
    } catch (Error failure) {
      if (lifecycle == null) {
        if (context != null) context.close();
        release();
      } else lifecycle.finish(Status.INTERNAL);
      throw failure;
    }
  }

  private boolean acquire() {
    synchronized (gate) {
      return !closing && admission.tryAcquire();
    }
  }

  void executeCleanup(Runnable cleanup) {
    // At most maxConcurrentRequests calls hold permits, and each submits exactly one task.
    cleanupExecutor.execute(cleanup);
  }

  void release() {
    boolean allDrained;
    synchronized (gate) {
      admission.release();
      allDrained = closing && activeRequests() == 0;
      if (allDrained) cleanupExecutor.shutdown();
      gate.notifyAll();
    }
    if (allDrained) drained.complete(null);
  }

  @Override
  public void close() {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    boolean allDrained;
    synchronized (gate) {
      closing = true;
      if (activeRequests() == 0) cleanupExecutor.shutdown();
      try {
        while (activeRequests() != 0) {
          long remaining = deadline - System.nanoTime();
          if (remaining <= 0) throw new IllegalStateException("request cleanup did not drain");
          TimeUnit.NANOSECONDS.timedWait(gate, remaining);
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("request cleanup drain interrupted");
      }
      allDrained = activeRequests() == 0;
    }
    if (allDrained) drained.complete(null);
    try {
      if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS))
        throw new IllegalStateException("request cleanup workers did not terminate");
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("request cleanup shutdown interrupted");
    }
  }

  void record(
      RequestCompletionRecorder.MethodClass methodClass,
      RequestCompletionRecorder.Outcome outcome,
      String traceId,
      RequestScope scope,
      long elapsedNanos) {
    try {
      recorder.recordCompletion(methodClass, outcome, traceId, scope, elapsedNanos);
    } catch (RuntimeException failure) {
      observationFailures.incrementAndGet();
    }
  }
}
