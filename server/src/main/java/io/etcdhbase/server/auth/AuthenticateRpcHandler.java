package io.etcdhbase.server.auth;

import etcdserverpb.Rpc;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.grpc.protocol.ErrorProjection;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/** Adapts the etcd Authenticate RPC to the persistent session service. */
public final class AuthenticateRpcHandler
    implements BiConsumer<Rpc.AuthenticateRequest, StreamObserver<Rpc.AuthenticateResponse>> {
  private final AuthSessionService sessions;
  private final Supplier<RequestScope> requestScopes;

  public AuthenticateRpcHandler(AuthSessionService sessions) {
    this(sessions, RequestScopeInterceptor::currentScope);
  }

  AuthenticateRpcHandler(AuthSessionService sessions, Supplier<RequestScope> requestScopes) {
    this.sessions = Objects.requireNonNull(sessions, "sessions");
    this.requestScopes = Objects.requireNonNull(requestScopes, "requestScopes");
  }

  @Override
  public void accept(Rpc.AuthenticateRequest request,
      StreamObserver<Rpc.AuthenticateResponse> observer) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(observer, "observer");
    RequestScope scope = Objects.requireNonNull(requestScopes.get(), "request scope");
    var drained = new CompletableFuture<Void>();
    var delivery = new UndeliveredTokenGuard(sessions);
    var deliveryRegistration = new AtomicReference<RequestScope.ScopedCleanup>();
    if (!scope.startIfOpen(() -> {
      scope.trackPhysicalProducer(drained);
      deliveryRegistration.set(scope.registerScopedCleanup(delivery, delivery.cleanupCompletion()));
    })) return;

    char[] password = request.getPassword().toCharArray();
    BackendCredentials credentials;
    try {
      credentials = new BackendCredentials(request.getName(), password);
    } catch (RuntimeException invalid) {
      delivery.issuanceFailed();
      try {
        scope.startIfOpen(() -> observer.onError(ErrorProjection.project(invalid)));
      } finally {
        closeRegistration(deliveryRegistration.get());
        drained.complete(null);
      }
      return;
    } catch (Error fatal) {
      delivery.issuanceFailed();
      closeRegistration(deliveryRegistration.get());
      drained.complete(null);
      throw fatal;
    } finally {
      Arrays.fill(password, '\0');
    }

    final java.util.concurrent.CompletionStage<AuthSessionService.IssuedToken> issued;
    try {
      issued = Objects.requireNonNull(sessions.authenticate(credentials, scope),
          "authentication session stage");
    } catch (RuntimeException failure) {
      credentials.close();
      delivery.issuanceFailed();
      drained.complete(null);
      try {
        scope.startIfOpen(() -> observer.onError(ErrorProjection.project(failure)));
      } finally {
        closeRegistration(deliveryRegistration.get());
      }
      return;
    } catch (Error fatal) {
      credentials.close();
      delivery.issuanceFailed();
      closeRegistration(deliveryRegistration.get());
      drained.complete(null);
      throw fatal;
    }
    issued.whenComplete((token, failure) -> {
      try {
        if (failure != null) {
          delivery.issuanceFailed();
          scope.startIfOpen(() -> observer.onError(ErrorProjection.project(failure)));
          return;
        }
        AuthSessionService.IssuedToken issuedToken = token;
        if (issuedToken == null) {
          delivery.issuanceFailed();
          scope.startIfOpen(() -> observer.onError(ErrorProjection.project(
              new NullPointerException("issued token"))));
          return;
        }
        delivery.issued(issuedToken);
        scope.startIfOpen(() -> {
          if (!delivery.startDelivery()) return;
          observer.onNext(Rpc.AuthenticateResponse.newBuilder()
              .setToken(issuedToken.tokenForResponse())
              .build());
          observer.onCompleted();
        });
      } finally {
        closeRegistration(deliveryRegistration.get());
        credentials.close();
        drained.complete(null);
      }
    });
  }

  private static void closeRegistration(RequestScope.ScopedCleanup registration) {
    if (registration == null) return;
    try {
      registration.close();
    } catch (Exception failure) {
      throw new IllegalStateException("unable to close token-delivery guard", failure);
    }
  }

  /** Owns a successfully issued session until token delivery or durable retirement is settled. */
  private static final class UndeliveredTokenGuard implements AutoCloseable {
    private final AuthSessionService sessions;
    private final CompletableFuture<Void> cleanupCompletion = new CompletableFuture<>();
    private AuthSessionService.IssuedToken issued;
    private boolean issuanceComplete;
    private boolean closeRequested;
    private boolean deliveryStarted;
    private boolean cleanupStarted;

    private UndeliveredTokenGuard(AuthSessionService sessions) {
      this.sessions = sessions;
    }

    private CompletionStage<Void> cleanupCompletion() {
      return cleanupCompletion.minimalCompletionStage();
    }

    private void issued(AuthSessionService.IssuedToken token) {
      AuthSessionService.IssuedToken cleanup;
      synchronized (this) {
        if (issuanceComplete) throw new IllegalStateException("authentication already settled");
        issued = Objects.requireNonNull(token, "token");
        issuanceComplete = true;
        cleanup = beginCleanupIfNeeded();
      }
      if (cleanup != null) discard(cleanup);
    }

    private void issuanceFailed() {
      boolean complete;
      synchronized (this) {
        if (issuanceComplete) return;
        issuanceComplete = true;
        complete = closeRequested;
        if (complete) cleanupStarted = true;
      }
      if (complete) cleanupCompletion.complete(null);
    }

    /** Called inside RequestScope.startIfOpen, which linearizes cancellation against delivery. */
    private boolean startDelivery() {
      synchronized (this) {
        if (!issuanceComplete || issued == null || closeRequested || cleanupStarted) return false;
        deliveryStarted = true;
      }
      cleanupCompletion.complete(null);
      return true;
    }

    @Override
    public void close() {
      AuthSessionService.IssuedToken cleanup;
      boolean complete;
      synchronized (this) {
        closeRequested = true;
        if (!issuanceComplete || deliveryStarted || cleanupStarted) {
          cleanup = null;
          complete = false;
        } else if (issued == null) {
          cleanupStarted = true;
          cleanup = null;
          complete = true;
        } else {
          cleanupStarted = true;
          cleanup = issued;
          complete = false;
        }
      }
      if (cleanup != null) discard(cleanup);
      else if (complete) cleanupCompletion.complete(null);
    }

    private AuthSessionService.IssuedToken beginCleanupIfNeeded() {
      if (!closeRequested || !issuanceComplete || deliveryStarted || cleanupStarted
          || issued == null) return null;
      cleanupStarted = true;
      return issued;
    }

    private void discard(AuthSessionService.IssuedToken token) {
      CompletionStage<Void> cleanup;
      try {
        cleanup = Objects.requireNonNull(sessions.discardUndelivered(token),
            "undelivered session cleanup stage");
      } catch (RuntimeException failure) {
        cleanupCompletion.completeExceptionally(failure);
        return;
      } catch (Error fatal) {
        cleanupCompletion.completeExceptionally(fatal);
        throw fatal;
      }
      cleanup.whenComplete((ignored, failure) -> {
        if (failure == null) cleanupCompletion.complete(null);
        else cleanupCompletion.completeExceptionally(failure);
      });
    }
  }
}
