package io.etcdhbase.server.commit;

import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/** Resolves one RPC's backend identity and safe owner-forwarding reference. */
final class AuthenticatedRequestIdentityResolver {
  private final String tableGroup;
  private final AuthorizedBackendIdentity serviceIdentity;
  private final ByteKey serviceAuthorizationDigest;
  private final AuthSessionService sessions;
  private final Consumer<ByteKey> invalidateReadAuthorization;

  AuthenticatedRequestIdentityResolver(String tableGroup,
      AuthorizedBackendIdentity serviceIdentity, ByteKey serviceAuthorizationDigest,
      AuthSessionService sessions) {
    this(tableGroup, serviceIdentity, serviceAuthorizationDigest, sessions, null);
  }

  AuthenticatedRequestIdentityResolver(String tableGroup,
      AuthorizedBackendIdentity serviceIdentity, ByteKey serviceAuthorizationDigest,
      AuthSessionService sessions, Consumer<ByteKey> invalidateReadAuthorization) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.serviceIdentity = Objects.requireNonNull(serviceIdentity, "serviceIdentity");
    this.serviceAuthorizationDigest = Objects.requireNonNull(
        serviceAuthorizationDigest, "serviceAuthorizationDigest");
    this.sessions = sessions;
    this.invalidateReadAuthorization = invalidateReadAuthorization;
  }

  CompletableFuture<ResolvedRequestIdentity> resolve(
      Optional<String> token, RequestScope scope) {
    Objects.requireNonNull(token, "token");
    Objects.requireNonNull(scope, "scope");
    if (token.isEmpty()) {
      if (sessions != null) {
        return CompletableFuture.failedFuture(
            new AuthSessionException(AuthSessionException.Reason.INVALID_TOKEN));
      }
      return CompletableFuture.completedFuture(new ResolvedRequestIdentity(serviceIdentity,
          serviceAuthorizationDigest,
          ServiceAuthorizationContextReference.create(serviceIdentity, serviceAuthorizationDigest),
          null));
    }
    if (sessions == null) return CompletableFuture.failedFuture(
        new AuthSessionException(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
    return sessions.resolve(token.orElseThrow(), scope).thenCompose(this::bindToTableGroup)
        .toCompletableFuture();
  }

  /** Rechecks the durable session and obtains a fresh borrow of its backend identity. */
  CompletableFuture<Void> revalidate(ResolvedRequestIdentity expected, RequestScope scope) {
    Objects.requireNonNull(expected, "expected");
    Objects.requireNonNull(scope, "scope");
    AuthSessionService.ResolvedIdentity held = expected.sessionIdentity();
    if (held == null) return CompletableFuture.completedFuture(null);
    if (sessions == null) return CompletableFuture.failedFuture(
        new AuthSessionException(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));

    AuthSession expectedSession = held.session();
    return sessions.resolveByDigest(expectedSession.tokenDigest(), scope).thenCompose(current -> {
      boolean sameAuthorization = expectedSession.equals(current.session())
          && expected.authorizationDigest().equals(
              AuthorizationScopeDigest.fromIdentity(current.identity()))
          && tableGroup.equals(current.identity().scope().tableGroupId())
          && current.identity().allowedTableGroupIds().contains(tableGroup);
      return current.release().handle((ignored, releaseFailure) -> {
        if (releaseFailure != null) {
          throw new java.util.concurrent.CompletionException(
              new AuthSessionException(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
        }
        if (!sameAuthorization) {
          throw new AuthSessionException(AuthSessionException.Reason.PERMISSION_DENIED);
        }
        return (Void) null;
      });
    }).whenComplete((ignored, failure) -> {
      if (failure != null
          && CompletionFailures.unwrapCompletion(failure) instanceof AuthSessionException) {
        if (expected.retireOnAuthorizationFailure(failure) && invalidateReadAuthorization != null) {
          invalidateReadAuthorization.accept(expected.readAuthorizationDigest());
        }
      }
    }).toCompletableFuture();
  }

  private CompletionStage<ResolvedRequestIdentity> bindToTableGroup(
      AuthSessionService.ResolvedIdentity resolved) {
    AuthorizedBackendIdentity identity = resolved.identity();
    if (!identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)) {
      return resolved.release().handle((ignored, failure) -> {
        if (failure != null) throw new java.util.concurrent.CompletionException(
            new AuthSessionException(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
        throw new AuthSessionException(AuthSessionException.Reason.PERMISSION_DENIED);
      });
    }
    ByteKey authorization = AuthorizationScopeDigest.fromIdentity(identity);
    ByteKey reference = SessionAuthorizationContextReference.fromSession(resolved.session()).encode();
    return CompletableFuture.completedFuture(
        new ResolvedRequestIdentity(identity, authorization, reference, resolved));
  }

  record ResolvedRequestIdentity(AuthorizedBackendIdentity identity,
      ByteKey authorizationDigest, ByteKey authorizationReference,
      AuthSessionService.ResolvedIdentity sessionIdentity) {
    ResolvedRequestIdentity {
      Objects.requireNonNull(identity, "identity");
      Objects.requireNonNull(authorizationDigest, "authorizationDigest");
      Objects.requireNonNull(authorizationReference, "authorizationReference");
    }

    ByteKey readAuthorizationDigest() {
      return sessionIdentity == null ? authorizationDigest
          : AuthorizationScopeDigest.fromSessionRead(identity, sessionIdentity.session());
    }

    /** Signals retirement without waiting for this request's own physical borrow to drain. */
    boolean retireOnAuthorizationFailure(Throwable failure) {
      if (sessionIdentity == null || failure == null) return false;
      Throwable cause = CompletionFailures.unwrapCompletion(failure);
      boolean rejected = cause instanceof BackendFailure backend
          && (backend.category() == BackendFailure.Category.AUTHENTICATION
              || backend.category() == BackendFailure.Category.PERMISSION
              || backend.category() == BackendFailure.Category.INVALID_AUTH_TOKEN)
          || cause instanceof AuthSessionException auth
          && (auth.reason() == AuthSessionException.Reason.INVALID_CREDENTIALS
              || auth.reason() == AuthSessionException.Reason.PERMISSION_DENIED
              || auth.reason() == AuthSessionException.Reason.INVALID_TOKEN);
      if (!rejected) return false;
      return retireUserBackendIdentity();
    }

    /** Called only for a rejection attributed to this identity's physical backend operations. */
    boolean retireUserBackendIdentity() {
      if (sessionIdentity == null) return false;
      sessionIdentity.retire();
      return true;
    }

    void releaseAfterPhysicalDrain(RequestScope scope) {
      if (sessionIdentity == null) return;
      var releaseCompletion = new CompletableFuture<Void>();
      AutoCloseable releaseAfterDrain = () -> scope.physicalProducersCompletion()
          .whenComplete((ignored, producerFailure) -> sessionIdentity.release()
              .whenComplete((released, releaseFailure) -> {
                if (releaseFailure == null) releaseCompletion.complete(null);
                else releaseCompletion.completeExceptionally(releaseFailure);
              }));
      scope.registerScopedCleanup(releaseAfterDrain, releaseCompletion);
    }
  }
}
