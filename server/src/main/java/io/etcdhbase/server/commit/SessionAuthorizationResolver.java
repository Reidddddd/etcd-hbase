package io.etcdhbase.server.commit;

import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.auth.AuthSessionService;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

/** Revalidates a forwarded session reference against durable auth-control state at the owner. */
final class SessionAuthorizationResolver implements MutationForwardReceiver.AuthorizationResolver {
  private final String tableGroup;
  private final AuthSessionService sessions;
  private final Predicate<String> trustedPeer;

  SessionAuthorizationResolver(
      String tableGroup, AuthSessionService sessions, Predicate<String> trustedPeer) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.sessions = Objects.requireNonNull(sessions, "sessions");
    this.trustedPeer = Objects.requireNonNull(trustedPeer, "trustedPeer");
  }

  @Override
  public CompletableFuture<MutationForwardReceiver.VerifiedAuthorization> verifyAndResolve(
      String authenticatedPeerIdentity, MutationForwardRequest request, RequestScope scope) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(scope, "scope");
    if (!trustedPeer.test(authenticatedPeerIdentity)
        || !tableGroup.equals(request.tableGroupId())) {
      return rejected(BackendFailure.Category.PERMISSION);
    }

    final SessionAuthorizationContextReference reference;
    try {
      reference = SessionAuthorizationContextReference.parse(
          request.authorizationContextReference());
    } catch (IllegalArgumentException malformed) {
      return rejected(BackendFailure.Category.PERMISSION);
    }
    return sessions.resolveByDigest(reference.tokenDigest(), scope).thenCompose(resolved -> {
      ByteKey authorization = AuthorizationScopeDigest.fromIdentity(resolved.identity());
      if (!reference.matches(resolved.session())
          || !tableGroup.equals(resolved.identity().scope().tableGroupId())
          || !resolved.identity().allowedTableGroupIds().contains(tableGroup)
          || !authorization.equals(request.authorizationScopeDigest())) {
        return releaseAndReject(resolved, BackendFailure.Category.PERMISSION);
      }
      registerReleaseAfterDrain(scope, resolved);
      return CompletableFuture.completedFuture(
          new MutationForwardReceiver.VerifiedAuthorization(resolved.identity(), authorization));
    }).exceptionallyCompose(failure -> {
      Throwable cause = unwrap(failure);
      if (cause instanceof BackendFailure backend) return rejected(backend.category());
      if (cause instanceof AuthSessionException auth) {
        return rejected(switch (auth.reason()) {
          case INVALID_CREDENTIALS, PERMISSION_DENIED -> BackendFailure.Category.PERMISSION;
          case INVALID_TOKEN -> BackendFailure.Category.INVALID_AUTH_TOKEN;
          case RESOURCE_LIMIT -> BackendFailure.Category.RESOURCE_LIMIT;
          case DEPENDENCY_UNAVAILABLE -> BackendFailure.Category.UNAVAILABLE;
        });
      }
      return rejected(BackendFailure.Category.UNAVAILABLE);
    }).toCompletableFuture();
  }

  private static void registerReleaseAfterDrain(
      RequestScope scope, AuthSessionService.ResolvedIdentity resolved) {
    var completion = new CompletableFuture<Void>();
    scope.registerScopedCleanup(() -> scope.physicalProducersCompletion()
        .whenComplete((ignored, producerFailure) -> resolved.release()
            .whenComplete((released, releaseFailure) -> {
              Throwable failure = releaseFailure != null ? releaseFailure : producerFailure;
              if (failure == null) completion.complete(null);
              else completion.completeExceptionally(failure);
            })), completion);
  }

  private static CompletableFuture<MutationForwardReceiver.VerifiedAuthorization> releaseAndReject(
      AuthSessionService.ResolvedIdentity resolved, BackendFailure.Category category) {
    return resolved.release().handle((ignored, failure) -> {
      if (failure != null) throw new java.util.concurrent.CompletionException(
          new BackendFailure(BackendFailure.Category.UNAVAILABLE, failure));
      return null;
    }).thenCompose(ignored -> CompletableFuture
        .<MutationForwardReceiver.VerifiedAuthorization>failedFuture(
            new BackendFailure(category)))
        .toCompletableFuture();
  }

  private static CompletableFuture<MutationForwardReceiver.VerifiedAuthorization> rejected(
      BackendFailure.Category category) {
    return CompletableFuture.failedFuture(new BackendFailure(category));
  }

  private static Throwable unwrap(Throwable failure) {
    while ((failure instanceof java.util.concurrent.CompletionException
        || failure instanceof java.util.concurrent.ExecutionException) && failure.getCause() != null) {
      failure = failure.getCause();
    }
    return failure;
  }
}
