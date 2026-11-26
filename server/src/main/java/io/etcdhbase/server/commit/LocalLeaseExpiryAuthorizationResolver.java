package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/** Resolves the service identity only on the private, owner-local Lease expiry lane. */
final class LocalLeaseExpiryAuthorizationResolver implements MutationForwardReceiver.AuthorizationResolver {
  private final String tableGroup;
  private final AuthorizedBackendIdentity identity;
  private final ByteKey authorization;
  private final Predicate<String> currentOwner;

  LocalLeaseExpiryAuthorizationResolver(String tableGroup, AuthorizedBackendIdentity identity,
      ByteKey authorization, Predicate<String> currentOwner) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.identity = Objects.requireNonNull(identity, "identity");
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.currentOwner = Objects.requireNonNull(currentOwner, "currentOwner");
    if (tableGroup.isBlank() || !tableGroup.equals(identity.scope().tableGroupId())
        || !identity.allowedTableGroupIds().contains(tableGroup) || authorization.length() != 32) {
      throw new IllegalArgumentException("invalid local Lease expiry authorization");
    }
  }

  @Override
  public CompletableFuture<MutationForwardReceiver.VerifiedAuthorization> verifyAndResolve(
      String peer, MutationForwardRequest request, long barrierGeneration, RequestScope scope) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(scope, "scope");
    try {
      if (!currentOwner.test(peer) || !tableGroup.equals(request.tableGroupId())
          || MutationOperationEnvelope.kind(request.operationBytes())
              != MutationOperationEnvelope.Kind.LEASE_EXPIRY_STEP
          || !authorization.equals(request.authorizationScopeDigest())
          || !ServiceAuthorizationContextReference.matches(identity, authorization,
              request.authorizationContextReference())) {
        return rejected();
      }
    } catch (IllegalArgumentException malformed) {
      return rejected();
    }
    return CompletableFuture.completedFuture(new MutationForwardReceiver.VerifiedAuthorization(
        identity, authorization, ServiceAuthorizationContextReference.publicationProof(
            identity, authorization, barrierGeneration)));
  }

  private static CompletableFuture<MutationForwardReceiver.VerifiedAuthorization> rejected() {
    return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
  }
}
