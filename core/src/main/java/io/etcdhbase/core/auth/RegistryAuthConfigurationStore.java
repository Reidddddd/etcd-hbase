package io.etcdhbase.core.auth;

import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.error.BackendFailure;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Projects a group's status from the authoritative deployment configuration generation. */
public final class RegistryAuthConfigurationStore implements AuthConfigurationStore {
  private final AuthDeploymentRegistryStore registry;

  public RegistryAuthConfigurationStore(AuthDeploymentRegistryStore registry) {
    this.registry = Objects.requireNonNull(registry, "registry");
  }

  @Override
  public CompletionStage<Optional<AuthConfigurationRecord>> readConfiguration(String tableGroupId,
      RequestScope scope) {
    Objects.requireNonNull(tableGroupId, "tableGroupId");
    return registry.readRegistry(scope).thenApply(found -> found.flatMap(record -> {
      if (record.configurationUpdate() != null) {
        throw new java.util.concurrent.CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      return record.groups().stream().filter(group -> group.tableGroupId().equals(tableGroupId)).findFirst()
          .map(group -> new AuthConfigurationRecord(record.trustDomain(), group.tableGroupId(),
              group.configurationDigest(), group.enabled(), record.configurationGeneration()));
    }));
  }
}
