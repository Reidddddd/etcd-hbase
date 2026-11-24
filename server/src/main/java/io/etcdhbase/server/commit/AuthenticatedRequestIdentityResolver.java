package io.etcdhbase.server.commit;

import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Resolves one RPC's backend identity and safe owner-forwarding reference. */
final class AuthenticatedRequestIdentityResolver {
  private final String tableGroup;
  private final AuthorizedBackendIdentity serviceIdentity;
  private final ByteKey serviceAuthorizationDigest;
  private final AuthSessionService sessions;

  AuthenticatedRequestIdentityResolver(String tableGroup,
      AuthorizedBackendIdentity serviceIdentity, ByteKey serviceAuthorizationDigest,
      AuthSessionService sessions) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.serviceIdentity = Objects.requireNonNull(serviceIdentity, "serviceIdentity");
    this.serviceAuthorizationDigest = Objects.requireNonNull(
        serviceAuthorizationDigest, "serviceAuthorizationDigest");
    this.sessions = sessions;
  }

  CompletableFuture<ResolvedRequestIdentity> resolve(
      Optional<String> token, RequestScope scope) {
    Objects.requireNonNull(token, "token");
    Objects.requireNonNull(scope, "scope");
    if (token.isEmpty()) {
      if (!BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID.equals(
          serviceIdentity.providerId())) {
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
