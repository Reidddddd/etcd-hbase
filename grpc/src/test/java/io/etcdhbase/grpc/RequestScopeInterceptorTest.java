package io.etcdhbase.grpc;

import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.grpc.Context;
import io.grpc.MethodDescriptor;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RequestScopeInterceptorTest {
  private final RequestScopeInterceptor interceptor = new RequestScopeInterceptor();

  @Test
  void installsScopeForTheHandlerAndClosesItOnCompletion() {
    AtomicReference<RequestScope> captured = new AtomicReference<>();
    AtomicInteger cleanupCount = new AtomicInteger();
    ServerCall.Listener<String> listener =
        intercept(
            (call, headers) -> {
              captured.set(RequestScopeInterceptor.currentScope());
              return new ServerCall.Listener<>() {};
            });
    captured.get().registerCleanup(cleanupCount::incrementAndGet);

    listener.onComplete();

    assertTrue(captured.get().isClosed());
    assertFalse(captured.get().isCancelled());
    assertEquals(1, cleanupCount.get());
  }

  @Test
  void parentContextCancellationClosesScopeAndRunsCleanupOnce() {
    AtomicReference<RequestScope> captured = new AtomicReference<>();
    AtomicInteger cleanupCount = new AtomicInteger();
    Context.CancellableContext cancellable = Context.current().withCancellation();
    Context previous = cancellable.attach();
    try {
      ServerCall.Listener<String> listener =
          intercept(
              (call, headers) -> {
                captured.set(RequestScopeInterceptor.currentScope());
                return new ServerCall.Listener<>() {};
              });
      captured.get().registerCleanup(cleanupCount::incrementAndGet);

      cancellable.cancel(new CancellationException("test cancellation"));
      listener.onCancel();

      assertTrue(captured.get().isCancelled());
      assertTrue(captured.get().isClosed());
      assertEquals(1, cleanupCount.get());
    } finally {
      cancellable.detach(previous);
      cancellable.close();
    }
  }

  @Test
  void closesScopeWhenStartingTheHandlerFails() {
    AtomicReference<RequestScope> captured = new AtomicReference<>();
    IllegalStateException expected = new IllegalStateException("handler failed");
    ServerCallHandler<String, String> handler =
        (call, headers) -> {
          captured.set(RequestScopeInterceptor.currentScope());
          throw expected;
        };

    IllegalStateException observed =
        assertThrows(IllegalStateException.class, () -> intercept(handler));

    assertTrue(captured.get().isClosed());
    assertTrue(observed == expected);
  }

  @Test
  void currentScopeIsUnavailableOutsideAnInterceptedCall() {
    assertThrows(IllegalStateException.class, RequestScopeInterceptor::currentScope);
    assertThrows(IllegalStateException.class, RequestScopeInterceptor::currentTraceId);
  }

  @Test
  void recordsBoundedMethodOutcomeAndGeneratedTraceIdOnce() {
    AtomicReference<Observation> observation = new AtomicReference<>();
    AtomicInteger observationCount = new AtomicInteger();
    RequestCompletionRecorder recorder =
        (methodClass, outcome, traceId, requestScope, elapsedNanos) -> {
          observationCount.incrementAndGet();
          observation.set(new Observation(methodClass, outcome, traceId, elapsedNanos));
        };
    AtomicReference<ServerCall<String, String>> observedCall = new AtomicReference<>();
    AtomicReference<String> contextTraceId = new AtomicReference<>();
    @SuppressWarnings("unchecked")
    MethodDescriptor<String, String> descriptor = mock(MethodDescriptor.class);
    when(descriptor.getFullMethodName()).thenReturn("etcdserverpb.KV/Range");
    @SuppressWarnings("unchecked")
    ServerCall<String, String> call = mock(ServerCall.class);
    when(call.getMethodDescriptor()).thenReturn(descriptor);
    RequestScopeInterceptor observedInterceptor = new RequestScopeInterceptor(recorder);

    ServerCall.Listener<String> listener =
        observedInterceptor.interceptCall(
            call,
            new Metadata(),
            (wrappedCall, headers) -> {
              observedCall.set(wrappedCall);
              contextTraceId.set(RequestScopeInterceptor.currentTraceId());
              assertTrue(contextTraceId.get().matches("[0-9a-f-]{36}"));
              return new ServerCall.Listener<>() {};
            });

    observedCall.get().close(Status.UNIMPLEMENTED, new Metadata());
    listener.onComplete();

    assertEquals(RequestCompletionRecorder.MethodClass.KV_UNARY, observation.get().methodClass());
    assertEquals(RequestCompletionRecorder.Outcome.UNIMPLEMENTED, observation.get().outcome());
    assertEquals(1, observationCount.get());
    assertEquals(contextTraceId.get(), observation.get().traceId());
    assertTrue(observation.get().elapsedNanos() >= 0);
  }

  @Test
  void retainsTheScopeBoundToTheListener() {
    AuthenticationScope listenerScope = new AuthenticationScope("local", "local-hbase");
    AtomicReference<AuthenticationScope> captured = new AtomicReference<>();

    intercept(
        new RequestScopeInterceptor(listenerScope),
        (call, headers) -> {
          captured.set(RequestScopeInterceptor.currentAuthenticationScope());
          return new ServerCall.Listener<>() {};
        });

    assertSame(listenerScope, captured.get());
  }

  private ServerCall.Listener<String> intercept(ServerCallHandler<String, String> handler) {
    return intercept(interceptor, handler);
  }

  private static ServerCall.Listener<String> intercept(
      RequestScopeInterceptor interceptor, ServerCallHandler<String, String> handler) {
    @SuppressWarnings("unchecked")
    ServerCall<String, String> call = mock(ServerCall.class);
    ServerCall.Listener<String> listener = interceptor.interceptCall(call, new Metadata(), handler);
    assertNotNull(listener);
    return listener;
  }

  private record Observation(
      RequestCompletionRecorder.MethodClass methodClass,
      RequestCompletionRecorder.Outcome outcome,
      String traceId,
      long elapsedNanos) {}
}
