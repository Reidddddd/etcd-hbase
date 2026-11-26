package io.etcdhbase.server.auth;

import io.etcdhbase.core.auth.AuthDeploymentRegistryStore;
import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.core.auth.AuthConfigurationUpdateExecutor;
import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.auth.AuthSessionStore;
import io.etcdhbase.core.auth.AuthorizationRevocation;
import io.etcdhbase.core.auth.AuthorizationRevocationExecutor;
import io.etcdhbase.core.auth.RegistryRevocationStore;
import io.etcdhbase.core.auth.SubjectAuthority;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Coordinates rotation using a pre-provisioned deployment inventory and resumable operation. */
final class DeploymentRevocationCoordinator {
  private static final int MAX_ATTEMPTS = 16;
  private final AuthDeploymentRegistryStore registry;
  private final AuthSessionStore authorities;
  private final RegistryRevocationStore operations;
  private final AuthorizationRevocationExecutor executor;
  private final AuthConfigurationUpdateExecutor configurationUpdates;
  private final String identityDomain;
  private final String trustDomain;
  private final ByteKey datasetDigest;
  private final ByteKey configurationDigest;
  private final SecureRandom random;
  private final Set<String> availableGroups;

  DeploymentRevocationCoordinator(AuthDeploymentRegistryStore registry, AuthSessionStore authorities,
      AuthorizationRevocationExecutor.Barriers barriers, String trustDomain, String identityDomain,
      ByteKey datasetDigest, ByteKey configurationDigest, SecureRandom random) {
    this(registry, authorities, barriers, trustDomain, identityDomain, datasetDigest,
        configurationDigest, random, null, null);
  }

  DeploymentRevocationCoordinator(AuthDeploymentRegistryStore registry, AuthSessionStore authorities,
      AuthorizationRevocationExecutor.Barriers barriers, String trustDomain, String identityDomain,
      ByteKey datasetDigest, ByteKey configurationDigest, SecureRandom random, Set<String> availableGroups,
      AuthConfigurationUpdateExecutor.Barriers configurationBarriers) {
    this.registry = Objects.requireNonNull(registry, "registry");
    this.authorities = Objects.requireNonNull(authorities, "authorities");
    this.operations = new RegistryRevocationStore(registry);
    this.executor = new AuthorizationRevocationExecutor(operations, barriers, operations);
    this.configurationUpdates = configurationBarriers == null ? null
        : new AuthConfigurationUpdateExecutor(registry, configurationBarriers);
    this.identityDomain = Objects.requireNonNull(identityDomain, "identityDomain");
    this.trustDomain = Objects.requireNonNull(trustDomain, "trustDomain");
    this.datasetDigest = Objects.requireNonNull(datasetDigest, "datasetDigest");
    this.configurationDigest = Objects.requireNonNull(configurationDigest, "configurationDigest");
    this.random = Objects.requireNonNull(random, "random");
    this.availableGroups = availableGroups == null ? null : Set.copyOf(availableGroups);
  }

  CompletionStage<Void> resumePending(String providerId, AuthenticationScope localScope, RequestScope scope) {
    Objects.requireNonNull(providerId, "providerId");
    Objects.requireNonNull(localScope, "localScope");
    return registry.readRegistry(scope).thenCompose(found -> {
      if (found.isEmpty()) return unavailable();
      var record = found.orElseThrow();
      if (!matchesLocal(record, providerId, localScope)
          || availableGroups == null || !availableGroups.equals(Set.of(localScope.tableGroupId()))) {
        return unavailable();
      }
      if (record.configurationUpdate() != null) {
        if (configurationUpdates == null) return unavailable();
        return configurationUpdates.resume(record.configurationUpdate().operationSequence(), availableGroups, scope)
            .thenCompose(current -> current.configurationUpdate() == null
                && matchesLocal(current, providerId, localScope)
                ? CompletableFuture.completedFuture(null) : unavailable());
      }
      var pending = record.revocation();
      if (pending == null || pending.state() == AuthorizationRevocation.State.COMPLETED) {
        return CompletableFuture.completedFuture(null);
      }
      return resume(pending.operationId(), scope).thenCompose(current ->
          current.state() == AuthorizationRevocation.State.COMPLETED
              ? CompletableFuture.completedFuture(null) : unavailable());
    });
  }

  private boolean matchesLocal(AuthDeploymentRegistry record, String providerId,
      AuthenticationScope localScope) {
    // Disabled groups still own configuration barriers; admission separately rejects users.
    return record.trustDomain().equals(trustDomain) && record.groups().stream().anyMatch(binding ->
        binding.tableGroupId().equals(localScope.tableGroupId())
            && binding.providerId().equals(providerId) && binding.backendIdentityDomain().equals(identityDomain)
            && binding.connectionProfileId().equals(localScope.connectionProfileId())
            && binding.datasetDigest().equals(datasetDigest)
            && binding.configurationDigest().equals(configurationDigest));
  }

  private CompletionStage<AuthorizationRevocation> resume(ByteKey operationId, RequestScope scope) {
    return availableGroups == null ? executor.resume(operationId, scope)
        : executor.resumeAvailable(operationId, availableGroups, scope);
  }

  private CompletionStage<SubjectAuthority> continueAfterRecovery(ByteKey operationId,
      AuthorizedBackendIdentity identity, RequestScope scope, int attempts) {
    return resume(operationId, scope).thenCompose(operation ->
        operation.state() == AuthorizationRevocation.State.COMPLETED
            ? ensureCurrent(identity, scope, attempts + 1) : unavailable());
  }

  CompletionStage<SubjectAuthority> ensureCurrent(AuthorizedBackendIdentity identity, RequestScope scope) {
    return ensureCurrent(identity, scope, 0);
  }

  private CompletionStage<SubjectAuthority> ensureCurrent(AuthorizedBackendIdentity identity,
      RequestScope scope, int attempts) {
    if (attempts >= MAX_ATTEMPTS || scope.isClosed() || scope.isDeadlineExceeded()) return unavailable();
    return registry.readRegistry(scope).thenCompose(found -> {
      if (found.isEmpty()) return unavailable();
      var record = found.orElseThrow();
      if (record.configurationUpdate() != null) return unavailable();
      var source = record.groups().stream()
          .filter(group -> group.tableGroupId().equals(identity.scope().tableGroupId())).findFirst();
      if (!record.trustDomain().equals(trustDomain) || source.isEmpty()
          || !source.orElseThrow().enabled() || !source.orElseThrow().datasetDigest().equals(datasetDigest)
          || !source.orElseThrow().configurationDigest().equals(configurationDigest)
          || !source.orElseThrow().providerId().equals(identity.providerId())
          || !source.orElseThrow().backendIdentityDomain().equals(identityDomain)
          || !source.orElseThrow().connectionProfileId().equals(identity.scope().connectionProfileId())) {
        return unavailable();
      }
      var pending = record.revocation();
      if (pending != null && pending.state() == AuthorizationRevocation.State.INSTALLING) {
        return continueAfterRecovery(pending.operationId(), identity, scope, attempts);
      }
      byte[] id = new byte[16];
      random.nextBytes(id);
      AuthorizationRevocation snapshot;
      try { snapshot = AuthorizationRevocationSnapshot.create(record, identityDomain, identity, ByteKey.copyOf(id)); }
      catch (IllegalArgumentException | ArithmeticException invalid) { return unavailable(); }
      var subject = snapshot.targets().stream()
          .filter(target -> target.identity().tableGroupId().equals(identity.scope().tableGroupId()))
          .findFirst().orElseThrow().identity().authorityDigest();
      return authorities.readAuthority(subject, scope).thenCompose(observed -> {
        if (observed.isPresent()) {
          var current = observed.orElseThrow();
          if (!current.trustDomain().equals(record.trustDomain())
              || !current.providerId().equals(identity.providerId()) || !current.scope().equals(identity.scope())
              || current.credentialGeneration() > identity.credentialGeneration()) return unavailable();
          if (current.state() == SubjectAuthority.State.ACTIVE
              && current.credentialGeneration() == identity.credentialGeneration()
              && current.revocationConfigurationGeneration() == record.configurationGeneration()) {
            return CompletableFuture.completedFuture(current);
          }
        }
        return operations.createRevocationIfAbsent(snapshot, scope).thenCompose(created ->
            created ? continueAfterRecovery(snapshot.operationId(), identity, scope, attempts)
                : ensureCurrent(identity, scope, attempts + 1));
      });
    });
  }

  private static <T> CompletionStage<T> unavailable() {
    return CompletableFuture.failedFuture(new AuthSessionException(
        AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
  }
}
