package io.etcdhbase.grpc;

import com.google.protobuf.MessageLite;
import io.etcdhbase.core.request.RequestScope;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** One call owns a scope, context listener, deadline timer and admission permit. */
final class RequestLifecycle<RequestT, ResponseT> {
  private final Context.CancellableContext context;
  private final RequestScope scope;
  private final String traceId = UUID.randomUUID().toString();
  private final long startedNanos = System.nanoTime();
  private final RequestCompletionRecorder.MethodClass methodClass;
  private final RequestScopeInterceptor owner;
  private final int maxRequestBytes;
  private final AtomicBoolean finished = new AtomicBoolean();
  private final AtomicBoolean transportClosed = new AtomicBoolean();
  private final ServerCall<RequestT, ResponseT> observedCall;
  private final Context.CancellationListener cancellation;

  RequestLifecycle(
      ServerCall<RequestT, ResponseT> call,
      Context.CancellableContext context,
      RequestCompletionRecorder.MethodClass methodClass,
      int maxRequestBytes,
      RequestScopeInterceptor owner) {
    this.context = context;
    this.owner = owner;
    this.methodClass = methodClass;
    this.maxRequestBytes = maxRequestBytes;
    this.scope =
        RequestScope.withTimeout(
            Duration.ofNanos(
                Math.max(0, context.getDeadline().timeRemaining(TimeUnit.NANOSECONDS))));
    this.observedCall =
        new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
          @Override
          public void close(Status status, Metadata trailers) {
            if (!transportClosed.compareAndSet(false, true)) return;
            try {
              super.close(status, trailers);
            } finally {
              finish(status);
            }
          }
        };
    this.cancellation = ignored -> observedCall.close(cancelledStatus(), new Metadata());
  }

  ServerCall.Listener<RequestT> start(
      Metadata headers, ServerCallHandler<RequestT, ResponseT> next) {
    context.addListener(cancellation, Runnable::run);
    if (finished.get()) return new ServerCall.Listener<>() {};
    var requestContext =
        context
            .withValue(RequestScopeInterceptor.SCOPE, scope)
            .withValue(RequestScopeInterceptor.TRACE, traceId);
    var listener = Contexts.interceptCall(requestContext, observedCall, headers, next);
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
      private long receivedBytes;

      @Override
      public void onMessage(RequestT message) {
        if (message instanceof MessageLite wire) {
          receivedBytes += wire.getSerializedSize();
          if (receivedBytes > maxRequestBytes) {
            observedCall.close(
                Status.RESOURCE_EXHAUSTED.withDescription("request byte budget exceeded"),
                new Metadata());
            return;
          }
        }
        run(() -> super.onMessage(message));
      }

      @Override
      public void onHalfClose() {
        run(super::onHalfClose);
      }

      @Override
      public void onReady() {
        run(super::onReady);
      }

      @Override
      public void onCancel() {
        try {
          super.onCancel();
        } catch (RuntimeException failure) {
          fail(failure);
        } finally {
          finish(cancelledStatus());
        }
      }

      @Override
      public void onComplete() {
        try {
          super.onComplete();
        } catch (RuntimeException failure) {
          fail(failure);
        } finally {
          finish(Status.OK);
        }
      }

      private void run(Runnable callback) {
        if (finished.get()) return;
        try {
          callback.run();
        } catch (RuntimeException failure) {
          fail(failure);
        } catch (Error failure) {
          finish(Status.INTERNAL);
          throw failure;
        }
      }
    };
  }

  void fail(RuntimeException failure) {
    observedCall.close(
        io.etcdhbase.grpc.protocol.ErrorProjection.project(failure).getStatus(), new Metadata());
  }

  void finish(Status status) {
    if (!finished.compareAndSet(false, true)) return;
    context.removeListener(cancellation);
    var contextClosed = new java.util.concurrent.CompletableFuture<Void>();
    scope
        .cleanupCompletion()
        .runAfterBoth(
            contextClosed,
            () -> {
              owner.release();
              owner.record(
                  methodClass,
                  classifyOutcome(status),
                  traceId,
                  scope,
                  Math.max(0, System.nanoTime() - startedNanos));
            });
    Runnable closeContext =
        () -> {
          try {
            context.close();
          } finally {
            contextClosed.complete(null);
          }
        };
    java.util.concurrent.Executor executor =
        cleanup ->
            owner.executeCleanup(
                () -> {
                  try {
                    cleanup.run();
                  } finally {
                    closeContext.run();
                  }
                });
    boolean started =
        status.getCode() == Status.Code.CANCELLED
                || status.getCode() == Status.Code.DEADLINE_EXCEEDED
            ? scope.cancelAsync(executor)
            : scope.closeAsync(executor);
    if (!started) owner.executeCleanup(closeContext);
  }

  private Status cancelledStatus() {
    return context.getDeadline().isExpired() ? Status.DEADLINE_EXCEEDED : Status.CANCELLED;
  }

  static RequestCompletionRecorder.MethodClass classifyMethod(String method) {
    int separator = method.indexOf('/');
    String service = separator < 0 ? method : method.substring(0, separator);
    return switch (service) {
      case "etcdserverpb.Auth" -> RequestCompletionRecorder.MethodClass.AUTHENTICATION;
      case "etcdserverpb.KV" -> RequestCompletionRecorder.MethodClass.KV_UNARY;
      case "etcdserverpb.Watch" -> RequestCompletionRecorder.MethodClass.WATCH_STREAM;
      case "etcdserverpb.Lease" -> RequestCompletionRecorder.MethodClass.LEASE;
      case "etcdserverpb.Maintenance", "etcdserverpb.Cluster" ->
          RequestCompletionRecorder.MethodClass.ADMINISTRATION;
      default -> RequestCompletionRecorder.MethodClass.OTHER;
    };
  }

  static RequestCompletionRecorder.Outcome classifyOutcome(Status status) {
    return switch (status.getCode()) {
      case OK -> RequestCompletionRecorder.Outcome.SUCCESS;
      case CANCELLED -> RequestCompletionRecorder.Outcome.CANCELLED;
      case DEADLINE_EXCEEDED -> RequestCompletionRecorder.Outcome.DEADLINE_EXCEEDED;
      case UNIMPLEMENTED -> RequestCompletionRecorder.Outcome.UNIMPLEMENTED;
      case RESOURCE_EXHAUSTED -> RequestCompletionRecorder.Outcome.RESOURCE_EXHAUSTED;
      case INVALID_ARGUMENT -> RequestCompletionRecorder.Outcome.INVALID_ARGUMENT;
      case PERMISSION_DENIED, UNAUTHENTICATED ->
          RequestCompletionRecorder.Outcome.PERMISSION_DENIED;
      case FAILED_PRECONDITION, NOT_FOUND, OUT_OF_RANGE, ALREADY_EXISTS ->
          RequestCompletionRecorder.Outcome.REQUEST_REJECTED;
      default -> RequestCompletionRecorder.Outcome.BACKEND_FAILURE;
    };
  }
}
