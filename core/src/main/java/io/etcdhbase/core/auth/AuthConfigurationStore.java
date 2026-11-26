package io.etcdhbase.core.auth;

import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Strong-read projection of one group's configuration from the deployment registry. */
public interface AuthConfigurationStore {
  CompletionStage<Optional<AuthConfigurationRecord>> readConfiguration(
      String tableGroupId, RequestScope scope);
}
