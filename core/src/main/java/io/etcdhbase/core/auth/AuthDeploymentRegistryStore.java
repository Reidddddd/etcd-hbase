package io.etcdhbase.core.auth;

import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Strong read and exact CAS of the deployment inventory and its current operation. */
public interface AuthDeploymentRegistryStore {
  CompletionStage<Optional<AuthDeploymentRegistry>> readRegistry(RequestScope scope);

  CompletionStage<Boolean> createRegistryIfAbsent(AuthDeploymentRegistry initial, RequestScope scope);

  CompletionStage<Boolean> compareAndSetRegistry(AuthDeploymentRegistry observed,
      AuthDeploymentRegistry replacement, RequestScope scope);
}
