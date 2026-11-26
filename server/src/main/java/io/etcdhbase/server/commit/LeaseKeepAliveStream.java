package io.etcdhbase.server.commit;

import etcdserverpb.Rpc;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.grpc.protocol.ErrorProjection;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/** One-at-a-time, bounded KeepAlive duplex flow controller with a single response writer. */
final class LeaseKeepAliveStream implements StreamObserver<Rpc.LeaseKeepAliveRequest> {
  private static final long MAX_OUTBOUND_READY_WAIT_NANOS = TimeUnit.SECONDS.toNanos(5);

  record Delivery(Rpc.LeaseKeepAliveResponse response, Runnable release,
      Supplier<CompletionStage<Void>> beforeSendValidation) {
    Delivery {
      Objects.requireNonNull(response, "response");
      Objects.requireNonNull(release, "release");
      Objects.requireNonNull(beforeSendValidation, "beforeSendValidation");
    }

    Delivery(Rpc.LeaseKeepAliveResponse response, Runnable release) {
      this(response, release, () -> CompletableFuture.completedFuture(null));
    }

    Delivery(Rpc.LeaseKeepAliveResponse response, Runnable release, Runnable beforeSendCheck) {
      this(response, release, adapt(beforeSendCheck));
    }

    CompletionStage<Void> validateBeforeSend() {
      return Objects.requireNonNull(beforeSendValidation.get(), "before-send validation");
    }

    private static Supplier<CompletionStage<Void>> adapt(Runnable beforeSendCheck) {
      Objects.requireNonNull(beforeSendCheck, "beforeSendCheck");
      return () -> {
        beforeSendCheck.run();
        return CompletableFuture.completedFuture(null);
      };
    }
  }

  private final Object lock = new Object();
  private final StreamObserver<Rpc.LeaseKeepAliveResponse> observer;
  private final ServerCallStreamObserver<Rpc.LeaseKeepAliveResponse> serverObserver;
  private final RequestScope scope;
  private final Executor worker;
  private final ScheduledExecutorService timers;
  private final RequestMetrics metrics;
  private final Function<Rpc.LeaseKeepAliveRequest, CompletionStage<Delivery>> handler;
  private final AtomicBoolean draining = new AtomicBoolean();
  private boolean demandOutstanding;
  private boolean processing;
  private boolean inboundClosed;
  private boolean terminal;
  private boolean cancelled;
  private Throwable failure;
  private Delivery pending;
  private boolean pendingValidationStarted;
  private boolean pendingValidated;
  private long readinessGeneration;
  private ScheduledFuture<?> pendingReadyTimeout;
  private long pendingReadyWaitStartedNanos;

  LeaseKeepAliveStream(StreamObserver<Rpc.LeaseKeepAliveResponse> observer,
      RequestScope scope, Executor worker, ScheduledExecutorService timers,
      Function<Rpc.LeaseKeepAliveRequest, CompletionStage<Delivery>> handler,
      RequestMetrics metrics) {
    this.observer = Objects.requireNonNull(observer, "observer");
    this.scope = Objects.requireNonNull(scope, "scope");
    this.worker = Objects.requireNonNull(worker, "worker");
    this.timers = Objects.requireNonNull(timers, "timers");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.handler = Objects.requireNonNull(handler, "handler");
    if (observer instanceof ServerCallStreamObserver<?> serverCall) {
      @SuppressWarnings("unchecked")
      ServerCallStreamObserver<Rpc.LeaseKeepAliveResponse> typed =
          (ServerCallStreamObserver<Rpc.LeaseKeepAliveResponse>) serverCall;
      serverObserver = typed;
      serverObserver.disableAutoInboundFlowControl();
      serverObserver.setOnReadyHandler(this::onTransportReady);
      serverObserver.setOnCancelHandler(this::cancelFromPeer);
      requestNext();
    } else {
      serverObserver = null;
    }
  }

  @Override
  public void onNext(Rpc.LeaseKeepAliveRequest request) {
    Objects.requireNonNull(request, "request");
    boolean accepted;
    synchronized (lock) {
      if (terminal || cancelled) return;
      if (inboundClosed || processing || pending != null
          || serverObserver != null && !demandOutstanding) {
        if (failure == null) failure = new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
        accepted = false;
      } else {
        demandOutstanding = false;
        processing = true;
        accepted = true;
      }
    }
    if (!accepted) {
      scheduleDrain();
      return;
    }
    CompletionStage<Delivery> operation;
    try {
      operation = Objects.requireNonNull(handler.apply(request), "KeepAlive operation");
    } catch (RuntimeException | Error rejected) {
      completeRequest(null, rejected);
      if (rejected instanceof Error fatal) throw fatal;
      return;
    }
    operation.whenComplete(this::completeRequest);
  }

  @Override
  public void onError(Throwable requestFailure) {
    Objects.requireNonNull(requestFailure, "requestFailure");
    cancelFromPeer();
  }

  @Override
  public void onCompleted() {
    synchronized (lock) {
      if (terminal || cancelled) return;
      inboundClosed = true;
      demandOutstanding = false;
    }
    scheduleDrain();
  }

  private void completeRequest(Delivery delivery, Throwable operationFailure) {
    long readyWaitNanos = 0;
    boolean scheduleTimeout = false;
    synchronized (lock) {
      processing = false;
      if (terminal || cancelled) {
        release(delivery);
        return;
      }
      if (operationFailure != null) {
        release(delivery);
        failure = operationFailure;
      } else if (delivery == null) {
        failure = new BackendFailure(BackendFailure.Category.UNKNOWN);
      } else if (pending != null) {
        release(delivery);
        failure = new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      } else {
        pending = delivery;
        pendingValidationStarted = false;
        pendingValidated = false;
        pendingReadyWaitStartedNanos = System.nanoTime();
        readyWaitNanos = maximumOutboundReadyWaitNanos(delivery.response().getTTL());
        scheduleTimeout = true;
      }
    }
    if (scheduleTimeout) schedulePendingTimeout(delivery, readyWaitNanos);
    scheduleDrain();
  }

  private static long maximumOutboundReadyWaitNanos(long ttlSeconds) {
    if (ttlSeconds <= 0) return MAX_OUTBOUND_READY_WAIT_NANOS;
    return Math.max(1, Math.min(MAX_OUTBOUND_READY_WAIT_NANOS,
        TimeUnit.SECONDS.toNanos(ttlSeconds) / 3));
  }

  private void schedulePendingTimeout(Delivery expected, long waitNanos) {
    ScheduledFuture<?> timeout;
    try {
      timeout = timers.schedule(() -> timeoutPending(expected), waitNanos, TimeUnit.NANOSECONDS);
    } catch (RejectedExecutionException rejected) {
      fail(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT, rejected));
      return;
    }
    synchronized (lock) {
      if (pending != expected || terminal || cancelled) timeout.cancel(false);
      else pendingReadyTimeout = timeout;
    }
  }

  private void timeoutPending(Delivery expected) {
    long waitedNanos = 0;
    synchronized (lock) {
      if (pending != expected || terminal || cancelled) return;
      waitedNanos = Math.max(0, System.nanoTime() - pendingReadyWaitStartedNanos);
    }
    // A pending frame has not been selected by the writer, so terminal delivery can bypass a
    // stalled authorization future. Its physical producer still drains through the request scope.
    if (failDirect(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT), expected)) {
      metrics.recordLeaseRenewalStage(
          RequestMetrics.LeaseRenewalStage.OUTBOUND_READY_WAIT, waitedNanos);
    }
  }

  private void requestNext() {
    if (serverObserver == null) return;
    synchronized (lock) {
      if (terminal || cancelled || inboundClosed || processing || pending != null
          || demandOutstanding) return;
      demandOutstanding = true;
    }
    try {
      serverObserver.request(1);
    } catch (RuntimeException requestFailure) {
      fail(requestFailure);
    }
  }

  private void scheduleDrain() {
    if (!draining.compareAndSet(false, true)) return;
    try {
      worker.execute(this::runOwnedDrain);
    } catch (RuntimeException rejected) {
      draining.set(false);
      failDirect(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT, rejected));
    } catch (Error fatal) {
      draining.set(false);
      failDirect(fatal);
      throw fatal;
    }
  }

  private void runOwnedDrain() {
    boolean handedOff = false;
    try {
      handedOff = drainLoop();
    } catch (RuntimeException | Error failure) {
      failDirect(failure);
      if (failure instanceof Error fatal) throw fatal;
    } finally {
      if (!handedOff) {
        draining.set(false);
        if (hasRunnableWork()) scheduleDrain();
      }
    }
  }

  /** Returns true only when an authorization completion now owns the writer. */
  private boolean drainLoop() {
    for (;;) {
      Delivery delivery = null;
      Throwable terminalFailure = null;
      boolean complete = false;
      boolean request = false;
      Delivery validate = null;
      long validateAtReadinessGeneration = -1;
      long outboundReadyWaitNanos = 0;
      ScheduledFuture<?> timeout = null;
      synchronized (lock) {
        if (terminal || cancelled) return false;
        if (failure != null) {
          terminal = true;
          terminalFailure = failure;
          failure = null;
          delivery = pending;
          pending = null;
          resetPendingValidation();
          timeout = pendingReadyTimeout;
          pendingReadyTimeout = null;
        } else if (pending != null && !isReady()) {
          if (!pendingValidationStarted) pendingValidated = false;
          return false;
        } else if (pending != null) {
          if (pendingValidated) {
            outboundReadyWaitNanos = Math.max(0,
                System.nanoTime() - pendingReadyWaitStartedNanos);
            delivery = pending;
            pending = null;
            resetPendingValidation();
            timeout = pendingReadyTimeout;
            pendingReadyTimeout = null;
          } else if (!pendingValidationStarted) {
            pendingValidationStarted = true;
            validate = pending;
            validateAtReadinessGeneration = readinessGeneration;
          } else {
            return false;
          }
        } else if (pending == null && !processing && inboundClosed) {
          terminal = true;
          complete = true;
        } else if (pending == null && !processing && !inboundClosed
            && serverObserver != null && !demandOutstanding) {
          request = true;
          demandOutstanding = true;
        } else {
          return false;
        }
      }
      cancel(timeout);
      if (terminalFailure != null) {
        release(delivery);
        cancelScope();
        try {
          observer.onError(ErrorProjection.project(terminalFailure));
        } catch (RuntimeException ignored) {
          // The peer may have cancelled while the terminal status was being sent.
        }
        return false;
      }
      if (complete) {
        observer.onCompleted();
        return false;
      }
      if (request) {
        try {
          serverObserver.request(1);
        } catch (RuntimeException requestFailure) {
          fail(requestFailure);
        }
        return false;
      }
      if (validate != null) {
        if (validateBeforeSend(validate, validateAtReadinessGeneration)) return true;
        continue;
      }
      try {
        metrics.recordLeaseRenewalStage(
            RequestMetrics.LeaseRenewalStage.OUTBOUND_READY_WAIT, outboundReadyWaitNanos);
        observer.onNext(delivery.response());
        metrics.recordLeaseRenewalResponse(delivery.response().getSerializedSize());
      } catch (RuntimeException | Error writeFailure) {
        release(delivery);
        cancelFromPeer();
        if (writeFailure instanceof Error fatal) throw fatal;
        return false;
      }
      release(delivery);
    }
  }

  private boolean validateBeforeSend(Delivery expected, long startedAtReadinessGeneration) {
    return OutboundAuthorizationHandoff.start(completed -> {
      CompletionStage<Void> validation;
      try {
        validation = expected.validateBeforeSend();
      } catch (RuntimeException | Error validationFailure) {
        completed.accept(validationFailure);
        if (validationFailure instanceof Error fatal) throw fatal;
        return;
      }
      validation.whenComplete((ignored, validationFailure) -> completed.accept(validationFailure));
    }, failure -> completePendingValidation(expected, startedAtReadinessGeneration, failure),
        this::runOwnedDrain);
  }

  private void completePendingValidation(Delivery expected, long startedAtReadinessGeneration,
      Throwable validationFailure) {
    synchronized (lock) {
      if (pending != expected || terminal || cancelled) return;
      if (validationFailure != null) {
        if (failure == null) failure = validationFailure;
      } else if (readinessGeneration == startedAtReadinessGeneration && isReady()) {
        pendingValidationStarted = false;
        pendingValidated = true;
      } else {
        pendingValidationStarted = false;
        pendingValidated = false;
      }
    }
  }

  private void onTransportReady() {
    synchronized (lock) {
      readinessGeneration++;
      if (pending != null && pendingValidated) {
        pendingValidationStarted = false;
        pendingValidated = false;
      }
    }
    scheduleDrain();
  }

  private boolean hasRunnableWork() {
    synchronized (lock) {
      return !terminal && !cancelled && (failure != null
          || pending != null && isReady() && (!pendingValidationStarted || pendingValidated)
          || pending == null && !processing && inboundClosed
          || serverObserver != null && pending == null && !processing
              && !inboundClosed && !demandOutstanding);
    }
  }

  private boolean isReady() {
    return serverObserver == null || serverObserver.isReady();
  }

  private void fail(Throwable cause) {
    synchronized (lock) {
      if (terminal || cancelled) return;
      failure = Objects.requireNonNull(cause, "cause");
    }
    scheduleDrain();
  }

  private void failDirect(Throwable cause) {
    failDirect(cause, null);
  }

  private boolean failDirect(Throwable cause, Delivery expected) {
    Delivery release;
    ScheduledFuture<?> timeout;
    synchronized (lock) {
      if (terminal || cancelled || expected != null && pending != expected) return false;
      terminal = true;
      release = pending;
      pending = null;
      resetPendingValidation();
      timeout = pendingReadyTimeout;
      pendingReadyTimeout = null;
    }
    cancel(timeout);
    release(release);
    cancelScope();
    try {
      observer.onError(ErrorProjection.project(cause));
    } catch (RuntimeException ignored) {
      // The peer may have cancelled while the terminal status was being sent.
    }
    return true;
  }

  private void cancelFromPeer() {
    Delivery release;
    ScheduledFuture<?> timeout;
    synchronized (lock) {
      if (terminal || cancelled) return;
      cancelled = true;
      demandOutstanding = false;
      release = pending;
      pending = null;
      resetPendingValidation();
      timeout = pendingReadyTimeout;
      pendingReadyTimeout = null;
    }
    cancel(timeout);
    release(release);
    cancelScope();
  }

  private void cancelScope() {
    try {
      scope.cancel();
    } catch (RuntimeException ignored) {
      // Cancellation is best effort; physical producers retain their own drain registration.
    }
  }

  private static void release(Delivery delivery) {
    if (delivery == null) return;
    try {
      delivery.release().run();
    } catch (RuntimeException ignored) {
      // The enclosing request scope records and drains the owned resource.
    }
  }

  private static void cancel(ScheduledFuture<?> timeout) {
    if (timeout != null) timeout.cancel(false);
  }

  private void resetPendingValidation() {
    pendingValidationStarted = false;
    pendingValidated = false;
    pendingReadyWaitStartedNanos = 0;
  }
}
