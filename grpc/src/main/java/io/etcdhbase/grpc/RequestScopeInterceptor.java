package io.etcdhbase.grpc;

import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Maps gRPC cancellation and deadlines to an internal request scope. */
public final class RequestScopeInterceptor implements ServerInterceptor {
  private static final Context.Key<RequestScope> REQUEST_SCOPE =
      Context.key("etcd-hbase-request-scope");
  private static final Context.Key<String> TRACE_ID = Context.key("etcd-hbase-trace-id");
  private static final Context.Key<AuthenticationScope> AUTHENTICATION_SCOPE =
      Context.key("etcd-hbase-listener-authentication-scope");

  private final AuthenticationScope listenerBoundScope;
  private final RequestCompletionRecorder completionRecorder;

  public RequestScopeInterceptor() {
    this(null, RequestCompletionRecorder.NOOP);
  }

  /** Binds every call on this listener to its configured backend scope. */
  public RequestScopeInterceptor(AuthenticationScope listenerBoundScope) {
    this(listenerBoundScope, RequestCompletionRecorder.NOOP);
  }

  public RequestScopeInterceptor(RequestCompletionRecorder completionRecorder) {
    this(null, completionRecorder);
  }

  /** Binds request completion observations without exposing server implementation types. */
  public RequestScopeInterceptor(
      AuthenticationScope listenerBoundScope, RequestCompletionRecorder completionRecorder) {
    this.listenerBoundScope = listenerBoundScope;
    this.completionRecorder = Objects.requireNonNull(completionRecorder, "completionRecorder");
  }

  public static RequestScope currentScope() {
    RequestScope scope = REQUEST_SCOPE.get();
    if (scope == null) {
      throw new IllegalStateException("request scope is available only inside an intercepted RPC");
    }
    return scope;
  }

  public static AuthenticationScope currentAuthenticationScope() {
    AuthenticationScope scope = AUTHENTICATION_SCOPE.get();
    if (scope == null) {
      throw new IllegalStateException(
          "authentication scope is available only on a listener-bound server");
    }
    return scope;
  }

  /** Returns the generated trace ID while handling an intercepted RPC. */
  public static String currentTraceId() {
    String traceId = TRACE_ID.get();
    if (traceId == null) {
      throw new IllegalStateException("trace ID is available only inside an intercepted RPC");
    }
    return traceId;
  }

  @Override
  public <RequestT, ResponseT> ServerCall.Listener<RequestT> interceptCall(
      ServerCall<RequestT, ResponseT> call,
      Metadata headers,
      ServerCallHandler<RequestT, ResponseT> next) {
    Context parent = Context.current();
    RequestScope scope = createScope(parent.getDeadline());
    String traceId = UUID.randomUUID().toString();
    long startedNanos = System.nanoTime();
    RequestCompletionRecorder.MethodClass methodClass =
        classifyMethod(
            call.getMethodDescriptor() == null
                ? null
                : call.getMethodDescriptor().getFullMethodName());
    Context requestScopeContext = parent.withValue(REQUEST_SCOPE, scope).withValue(TRACE_ID, traceId);
    Context requestContext =
        listenerBoundScope == null
            ? requestScopeContext
            : requestScopeContext.withValue(AUTHENTICATION_SCOPE, listenerBoundScope);
    RequestObservation observation =
        new RequestObservation(requestContext, traceId, methodClass, scope, startedNanos);
    Context.CancellationListener cancellationListener = ignored -> scope.cancel();
    requestContext.addListener(cancellationListener, Runnable::run);

    AtomicReference<ServerCall.Listener<RequestT>> delegate = new AtomicReference<>();
    ServerCall<RequestT, ResponseT> observedCall =
        new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
          @Override
          public void close(Status status, Metadata trailers) {
            try {
              super.close(status, trailers);
            } finally {
              observation.complete(status);
            }
          }
        };
    try {
      requestContext.run(() -> delegate.set(next.startCall(observedCall, headers)));
      Objects.requireNonNull(delegate.get(), "server call handler returned no listener");
    } catch (RuntimeException | Error failure) {
      requestContext.removeListener(cancellationListener);
      try {
        observation.complete(Status.UNKNOWN);
      } finally {
        scope.close();
      }
      throw failure;
    }

    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate.get()) {
      @Override
      public void onMessage(RequestT message) {
        runScoped(() -> super.onMessage(message));
      }

      @Override
      public void onHalfClose() {
        runScoped(super::onHalfClose);
      }

      @Override
      public void onReady() {
        runScoped(super::onReady);
      }

      @Override
      public void onCancel() {
        try {
          observation.complete(cancellationStatus(scope));
          scope.cancel();
          requestContext.run(super::onCancel);
        } finally {
          requestContext.removeListener(cancellationListener);
        }
      }

      @Override
      public void onComplete() {
        try {
          requestContext.run(super::onComplete);
        } finally {
          try {
            observation.complete(Status.OK);
          } finally {
            scope.close();
          }
          requestContext.removeListener(cancellationListener);
        }
      }

      private void runScoped(Runnable callback) {
        try {
          requestContext.run(callback);
        } catch (RuntimeException | Error failure) {
          try {
            observation.complete(Status.UNKNOWN);
            scope.cancel();
          } finally {
            requestContext.removeListener(cancellationListener);
          }
          throw failure;
        }
      }
    };
  }

  private static RequestScope createScope(Deadline deadline) {
    if (deadline == null) {
      return RequestScope.withoutDeadline();
    }
    long remainingNanos = Math.max(0, deadline.timeRemaining(TimeUnit.NANOSECONDS));
    return RequestScope.withTimeout(Duration.ofNanos(remainingNanos));
  }

  private final class RequestObservation {
    private final Context requestContext;
    private final String traceId;
    private final RequestCompletionRecorder.MethodClass methodClass;
    private final RequestScope scope;
    private final long startedNanos;
    private final AtomicBoolean completionRecorded = new AtomicBoolean();

    private RequestObservation(
        Context requestContext,
        String traceId,
        RequestCompletionRecorder.MethodClass methodClass,
        RequestScope scope,
        long startedNanos) {
      this.requestContext = requestContext;
      this.traceId = traceId;
      this.methodClass = methodClass;
      this.scope = scope;
      this.startedNanos = startedNanos;
    }

    private void complete(Status status) {
      if (!completionRecorded.compareAndSet(false, true)) {
        return;
      }
      long elapsedNanos = Math.max(0, System.nanoTime() - startedNanos);
      RequestCompletionRecorder.Outcome outcome = classifyOutcome(status);
      requestContext.run(
          () ->
              completionRecorder.recordCompletion(
                  methodClass, outcome, traceId, scope, elapsedNanos));
    }
  }

  private static RequestCompletionRecorder.MethodClass classifyMethod(String fullMethodName) {
    if (fullMethodName == null) {
      return RequestCompletionRecorder.MethodClass.OTHER;
    }
    int methodSeparator = fullMethodName.indexOf('/');
    String serviceName =
        methodSeparator < 0 ? fullMethodName : fullMethodName.substring(0, methodSeparator);
    int packageSeparator = serviceName.lastIndexOf('.');
    String simpleServiceName =
        packageSeparator < 0 ? serviceName : serviceName.substring(packageSeparator + 1);
    return switch (simpleServiceName) {
      case "Auth" -> RequestCompletionRecorder.MethodClass.AUTHENTICATION;
      case "KV" -> RequestCompletionRecorder.MethodClass.KV_UNARY;
      case "Watch" -> RequestCompletionRecorder.MethodClass.WATCH_STREAM;
      case "Lease" -> RequestCompletionRecorder.MethodClass.LEASE;
      case "Maintenance", "Cluster" -> RequestCompletionRecorder.MethodClass.ADMINISTRATION;
      default -> RequestCompletionRecorder.MethodClass.OTHER;
    };
  }

  private static RequestCompletionRecorder.Outcome classifyOutcome(Status status) {
    if (status == null) {
      return RequestCompletionRecorder.Outcome.BACKEND_FAILURE;
    }
    return switch (status.getCode()) {
      case OK -> RequestCompletionRecorder.Outcome.SUCCESS;
      case CANCELLED -> RequestCompletionRecorder.Outcome.CANCELLED;
      case DEADLINE_EXCEEDED -> RequestCompletionRecorder.Outcome.DEADLINE_EXCEEDED;
      case UNIMPLEMENTED -> RequestCompletionRecorder.Outcome.UNIMPLEMENTED;
      default -> RequestCompletionRecorder.Outcome.BACKEND_FAILURE;
    };
  }

  private static Status cancellationStatus(RequestScope scope) {
    return scope.isDeadlineExceeded() ? Status.DEADLINE_EXCEEDED : Status.CANCELLED;
  }
}
