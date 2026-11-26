package io.etcdhbase.server.auth;

import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.core.auth.AuthorizationRevocation;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.Objects;

/** Freezes every active scope of an authenticated identity's explicitly configured family. */
final class AuthorizationRevocationSnapshot {
  private AuthorizationRevocationSnapshot() {}

  static AuthorizationRevocation create(AuthDeploymentRegistry registry, String identityDomain,
      AuthorizedBackendIdentity verified, ByteKey operationId) {
    Objects.requireNonNull(registry, "registry");
    Objects.requireNonNull(verified, "verified");
    if (!verified.hasKnownCredentialGeneration() || verified.credentialGeneration() < 1) {
      throw new IllegalArgumentException("credential generation must be attested");
    }
    var source = registry.groups().stream()
        .filter(group -> group.tableGroupId().equals(verified.scope().tableGroupId())).findFirst()
        .orElseThrow(() -> new IllegalArgumentException("identity source is not registered"));
    if (!source.enabled() || !source.providerId().equals(verified.providerId())
        || !source.backendIdentityDomain().equals(identityDomain)
        || !source.connectionProfileId().equals(verified.scope().connectionProfileId())
        || !verified.allowedTableGroupIds().contains(source.tableGroupId())) {
      throw new IllegalArgumentException("verified identity does not match deployment binding");
    }
    var targets = registry.groups().stream()
        .filter(group -> group.enabled() && group.providerId().equals(verified.providerId())
            && group.backendIdentityDomain().equals(identityDomain))
        .map(group -> new AuthorizationRevocation.TargetIdentity(group.tableGroupId(), group.datasetDigest(),
            SubjectIdentityDigests.scoped(registry.trustDomain(), group.providerId(),
                group.backendIdentityDomain(),
                new AuthenticationScope(group.tableGroupId(), group.connectionProfileId()), verified.subject())))
        .toList();
    return AuthorizationRevocation.start(operationId,
        SubjectIdentityDigests.family(registry.trustDomain(), identityDomain, verified),
        verified.providerId(), identityDomain, verified.credentialGeneration(),
        Math.addExact(registry.revision(), 1), registry.configurationGeneration(), targets);
  }
}
