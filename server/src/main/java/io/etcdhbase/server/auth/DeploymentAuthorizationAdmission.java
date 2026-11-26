package io.etcdhbase.server.auth;

import io.etcdhbase.core.auth.AuthDeploymentRegistryStore;
import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.auth.AuthorizationRevocation;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** Rejects stale deployment bindings and affected subjects while a revocation is installing. */
final class DeploymentAuthorizationAdmission {
  private final AuthDeploymentRegistryStore registry;
  private final String trustDomain;
  private final String providerId;
  private final String identityDomain;
  private final AuthenticationScope scope;
  private final ByteKey datasetDigest;
  private final ByteKey configurationDigest;

  DeploymentAuthorizationAdmission(AuthDeploymentRegistryStore registry, String trustDomain,
      String providerId, String identityDomain, AuthenticationScope scope, ByteKey datasetDigest,
      ByteKey configurationDigest) {
    this.registry = Objects.requireNonNull(registry, "registry");
    this.trustDomain = Objects.requireNonNull(trustDomain, "trustDomain");
    this.providerId = Objects.requireNonNull(providerId, "providerId");
    this.identityDomain = Objects.requireNonNull(identityDomain, "identityDomain");
    this.scope = Objects.requireNonNull(scope, "scope");
    this.datasetDigest = Objects.requireNonNull(datasetDigest, "datasetDigest");
    this.configurationDigest = Objects.requireNonNull(configurationDigest, "configurationDigest");
  }

  CompletionStage<Void> check(ByteKey scopedSubject, long authorityConfigurationGeneration,
      RequestScope requestScope) {
    Objects.requireNonNull(scopedSubject, "scopedSubject");
    return registry.readRegistry(requestScope).thenApply(found -> {
      var record = found.orElseThrow(DeploymentAuthorizationAdmission::unavailable);
      if (record.configurationUpdate() != null) throw unavailable();
      var group = record.groups().stream()
          .filter(candidate -> candidate.tableGroupId().equals(scope.tableGroupId())).findFirst()
          .orElseThrow(DeploymentAuthorizationAdmission::unavailable);
      if (!record.trustDomain().equals(trustDomain) || !group.enabled()
          || record.configurationGeneration() != authorityConfigurationGeneration
          || !group.providerId().equals(providerId) || !group.backendIdentityDomain().equals(identityDomain)
          || !group.connectionProfileId().equals(scope.connectionProfileId())
          || !group.datasetDigest().equals(datasetDigest)
          || !group.configurationDigest().equals(configurationDigest)) throw unavailable();
      var operation = record.revocation();
      if (operation != null && operation.state() == AuthorizationRevocation.State.INSTALLING
          && operation.targets().stream().anyMatch(target ->
              target.identity().tableGroupId().equals(scope.tableGroupId())
                  && target.identity().authorityDigest().equals(scopedSubject))) {
        throw new AuthSessionException(AuthSessionException.Reason.INVALID_TOKEN);
      }
      return null;
    });
  }

  private static AuthSessionException unavailable() {
    return new AuthSessionException(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE);
  }
}
