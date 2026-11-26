package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Stores revocation progress in the same CAS row that protects deployment configuration. */
public final class RegistryRevocationStore implements AuthorizationRevocationStore,
    AuthorizationRevocationExecutor.ConfigurationFence {
  private final AuthDeploymentRegistryStore registry;

  public RegistryRevocationStore(AuthDeploymentRegistryStore registry) {
    this.registry = Objects.requireNonNull(registry, "registry");
  }

  @Override
  public CompletionStage<Optional<AuthorizationRevocation>> readRevocation(ByteKey operationId,
      RequestScope scope) {
    Objects.requireNonNull(operationId, "operationId");
    return registry.readRegistry(scope).thenApply(found -> found
        .map(AuthDeploymentRegistry::revocation).filter(operation -> operation.operationId().equals(operationId)));
  }

  @Override
  public CompletionStage<Boolean> createRevocationIfAbsent(AuthorizationRevocation initial,
      RequestScope scope) {
    Objects.requireNonNull(initial, "initial");
    if (!initial.isInitial()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("revocation must be initial"));
    }
    return registry.readRegistry(scope).thenCompose(found -> {
      if (found.isEmpty()) return unavailable();
      var current = found.orElseThrow();
      if (current.configurationUpdate() != null) return CompletableFuture.completedFuture(false);
      var operation = current.revocation();
      if (operation != null && (operation.state() == AuthorizationRevocation.State.INSTALLING
          || operation.operationId().equals(initial.operationId()))) {
        return CompletableFuture.completedFuture(false);
      }
      if (current.revision() == Long.MAX_VALUE) return unavailable();
      if (initial.configurationGeneration() != current.configurationGeneration()
          || initial.operationSequence() != current.revision() + 1) {
        return CompletableFuture.completedFuture(false);
      }
      return registry.compareAndSetRegistry(current, current.beginRevocation(initial), scope);
    });
  }

  @Override
  public CompletionStage<Boolean> compareAndSetRevocation(AuthorizationRevocation observed,
      AuthorizationRevocation replacement, RequestScope scope) {
    Objects.requireNonNull(observed, "observed");
    return registry.readRegistry(scope).thenCompose(found -> {
      if (found.isEmpty()) return unavailable();
      var current = found.orElseThrow();
      if (!observed.equals(current.revocation())) return CompletableFuture.completedFuture(false);
      return registry.compareAndSetRegistry(current, current.advanceRevocation(replacement), scope);
    });
  }

  @Override
  public CompletionStage<Void> validate(AuthorizationRevocation operation, RequestScope scope) {
    return registry.readRegistry(scope).thenCompose(found -> found.isPresent()
        && operation.equals(found.orElseThrow().revocation())
        ? CompletableFuture.completedFuture(null) : unavailable());
  }

  private static <T> CompletionStage<T> unavailable() {
    return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
  }
}
