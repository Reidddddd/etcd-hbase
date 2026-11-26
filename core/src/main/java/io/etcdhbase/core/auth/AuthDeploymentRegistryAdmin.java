package io.etcdhbase.core.auth;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Explicit administrator operations; ordinary session paths never provision the registry. */
public final class AuthDeploymentRegistryAdmin {
  private final AuthDeploymentRegistryStore store;

  public AuthDeploymentRegistryAdmin(AuthDeploymentRegistryStore store) {
    this.store = Objects.requireNonNull(store, "store");
  }

  public CompletionStage<Boolean> initialize(String trustDomain,
      List<AuthDeploymentRegistry.Group> groups, RequestScope scope) {
    return store.createRegistryIfAbsent(AuthDeploymentRegistry.initial(trustDomain, groups), scope);
  }

  /** Reserves an update; success is not completion and requires recovery of its barriers. */
  public CompletionStage<Boolean> reserveUpdate(long expectedGeneration,
      List<AuthDeploymentRegistry.Group> groups, RequestScope scope) {
    if (expectedGeneration < 1) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid configuration generation"));
    }
    return store.readRegistry(scope).thenCompose(found -> {
      if (found.isEmpty()) return unavailable();
      var current = found.orElseThrow();
      if (current.configurationGeneration() != expectedGeneration
          || current.configurationUpdate() != null
          || current.revocation() != null
              && current.revocation().state() == AuthorizationRevocation.State.INSTALLING) {
        return CompletableFuture.completedFuture(false);
      }
      return store.compareAndSetRegistry(current, current.beginConfigurationUpdate(groups), scope);
    });
  }

  /** Configuration updates remain unavailable until their old-group barriers can be recovered. */
  public CompletionStage<Boolean> update(long expectedGeneration,
      List<AuthDeploymentRegistry.Group> groups, RequestScope scope) {
    if (expectedGeneration < 1) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid configuration generation"));
    }
    return store.readRegistry(scope).thenCompose(found -> {
      if (found.isEmpty()) return unavailable();
      var current = found.orElseThrow();
      if (current.configurationGeneration() != expectedGeneration) return CompletableFuture.completedFuture(false);
      return unavailable();
    });
  }

  private static <T> CompletionStage<T> unavailable() {
    return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
  }
}
