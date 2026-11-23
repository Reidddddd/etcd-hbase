package io.etcdhbase.server.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import etcdserverpb.Rpc;
import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class AuthenticateRpcHandlerTest {
  @Test
  void authenticatesAndReturnsOnlyTheIssuedOpaqueToken() {
    AuthSessionService sessions = mock(AuthSessionService.class);
    AuthSessionService.IssuedToken issued = mock(AuthSessionService.IssuedToken.class);
    when(issued.tokenForResponse()).thenReturn("opaque-session-token");
    when(sessions.authenticate(any(BackendCredentials.class), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(issued));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(3))) {
      var handler = new AuthenticateRpcHandler(sessions, () -> scope);
      var request = Rpc.AuthenticateRequest.newBuilder()
          .setName("milvus-service").setPassword("test-password").build();
      var response = new AtomicReference<Rpc.AuthenticateResponse>();
      var failure = new AtomicReference<Throwable>();
      var completed = new AtomicReference<Boolean>(false);
      StreamObserver<Rpc.AuthenticateResponse> observer = observer(response, failure, completed);

      handler.accept(request, observer);

      assertEquals("opaque-session-token", response.get().getToken());
      assertTrue(completed.get());
      assertNull(failure.get());
      ArgumentCaptor<BackendCredentials> captured = ArgumentCaptor.forClass(BackendCredentials.class);
      verify(sessions).authenticate(captured.capture(), same(scope));
      assertEquals("milvus-service", captured.getValue().principal());
      assertTrue(captured.getValue().isClosed(), "the request credential copy must be cleared");
    }
  }

  @Test
  void mapsCredentialRejectionWithoutReturningATokenOrSecret() {
    AuthSessionService sessions = mock(AuthSessionService.class);
    when(sessions.authenticate(any(BackendCredentials.class), any(RequestScope.class)))
        .thenReturn(CompletableFuture.failedFuture(
            new AuthSessionException(AuthSessionException.Reason.INVALID_CREDENTIALS)));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(3))) {
      var handler = new AuthenticateRpcHandler(sessions, () -> scope);
      var request = Rpc.AuthenticateRequest.newBuilder()
          .setName("milvus-service").setPassword("never-leak-this").build();
      var response = new AtomicReference<Rpc.AuthenticateResponse>();
      var failure = new AtomicReference<Throwable>();
      var completed = new AtomicReference<Boolean>(false);

      handler.accept(request, observer(response, failure, completed));

      assertNull(response.get());
      assertFalse(completed.get());
      assertNotNull(failure.get());
      assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(failure.get()).getCode());
      assertFalse(Status.fromThrowable(failure.get()).getDescription().contains("never-leak-this"));
    }
  }

  @Test
  void cancellationBeforeAuthenticateTokenSendRetiresTheUndeliveredSession() throws Exception {
    AuthSessionService sessions = mock(AuthSessionService.class);
    AuthSessionService.IssuedToken issued = mock(AuthSessionService.IssuedToken.class);
    var issue = new CompletableFuture<AuthSessionService.IssuedToken>();
    var cleanup = new CompletableFuture<Void>();
    when(sessions.authenticate(any(BackendCredentials.class), any(RequestScope.class)))
        .thenReturn(issue);
    when(sessions.discardUndelivered(same(issued))).thenReturn(cleanup);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(3))) {
      var handler = new AuthenticateRpcHandler(sessions, () -> scope);
      var request = Rpc.AuthenticateRequest.newBuilder()
          .setName("milvus-service").setPassword("test-password").build();
      var response = new AtomicReference<Rpc.AuthenticateResponse>();
      var failure = new AtomicReference<Throwable>();
      var completed = new AtomicReference<Boolean>(false);

      handler.accept(request, observer(response, failure, completed));
      assertTrue(scope.cancel(), "cancellation must win before token delivery starts");
      issue.complete(issued);

      verify(sessions).discardUndelivered(same(issued));
      assertNull(response.get(), "a token must not be sent after the request closes");
      assertNull(failure.get());
      assertFalse(completed.get());
      assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone(),
          "request cleanup must retain ownership until the slot cleanup settles");
      assertTrue(scope.physicalProducersCompletion().toCompletableFuture().isDone());

      cleanup.complete(null);
      scope.cleanupCompletion().toCompletableFuture().get(1, java.util.concurrent.TimeUnit.SECONDS);
    }
  }

  private static StreamObserver<Rpc.AuthenticateResponse> observer(
      AtomicReference<Rpc.AuthenticateResponse> response,
      AtomicReference<Throwable> failure,
      AtomicReference<Boolean> completed) {
    return new StreamObserver<>() {
      @Override public void onNext(Rpc.AuthenticateResponse value) { response.set(value); }
      @Override public void onError(Throwable value) { failure.set(value); }
      @Override public void onCompleted() { completed.set(true); }
    };
  }
}
