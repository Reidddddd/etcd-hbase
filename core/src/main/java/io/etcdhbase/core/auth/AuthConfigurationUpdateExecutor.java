package io.etcdhbase.core.auth;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Advances a reserved update only for old groups available to this worker. */
public final class AuthConfigurationUpdateExecutor {
  private static final int MAX_STEPS = AuthorizationRevocation.MAX_TARGETS * 4 + 16;

  /** Both methods must verify the old group's dataset incarnation. */
  public interface Barriers {
    CompletionStage<Long> allocate(AuthConfigurationUpdate.Target target, RequestScope scope);
    CompletionStage<Void> install(AuthConfigurationUpdate.Target target, RequestScope scope);
  }

  private final AuthDeploymentRegistryStore store;
  private final Barriers barriers;

  public AuthConfigurationUpdateExecutor(AuthDeploymentRegistryStore store, Barriers barriers) {
    this.store = Objects.requireNonNull(store, "store");
    this.barriers = Objects.requireNonNull(barriers, "barriers");
  }

  public CompletionStage<AuthDeploymentRegistry> resume(long operationSequence,
      Set<String> availableGroups, RequestScope scope) {
    if (operationSequence < 2) throw new IllegalArgumentException("invalid operation sequence");
    return resume(operationSequence, Set.copyOf(availableGroups), Objects.requireNonNull(scope, "scope"), 0);
  }

  private CompletionStage<AuthDeploymentRegistry> resume(long operationSequence,
      Set<String> availableGroups, RequestScope scope, int steps) {
    if (steps >= MAX_STEPS) return failed(BackendFailure.Category.RESOURCE_LIMIT);
    if (scope.isDeadlineExceeded()) return failed(BackendFailure.Category.TIMEOUT);
    if (scope.isClosed()) return failed(BackendFailure.Category.CANCELLED);
    return store.readRegistry(scope).thenCompose(found -> {
      if (found.isEmpty()) return failed(BackendFailure.Category.UNAVAILABLE);
      var current = found.orElseThrow();
      var update = current.configurationUpdate();
      if (update == null || update.operationSequence() != operationSequence) {
        return failed(BackendFailure.Category.UNAVAILABLE);
      }
      if (update.isInstalled()) {
        return store.compareAndSetRegistry(current, current.finishConfigurationUpdate(), scope)
            .thenCompose(applied -> applied ? CompletableFuture.completedFuture(current.finishConfigurationUpdate())
                : resume(operationSequence, availableGroups, scope, steps + 1));
      }
      var eligible = update.targets().stream().filter(target -> !target.installed()
          && availableGroups.contains(target.tableGroupId())).findFirst();
      if (eligible.isEmpty()) return CompletableFuture.completedFuture(current);
      var target = eligible.orElseThrow();
      if (target.barrierGeneration() == 0) {
        return barriers.allocate(target, scope).thenCompose(generation -> save(current,
            update.assign(target.tableGroupId(), generation), operationSequence, availableGroups, scope, steps));
      }
      return barriers.install(target, scope).thenCompose(ignored -> save(current,
          update.acknowledge(target.tableGroupId(), target.barrierGeneration()), operationSequence,
          availableGroups, scope, steps));
    });
  }

  private CompletionStage<AuthDeploymentRegistry> save(AuthDeploymentRegistry current,
      AuthConfigurationUpdate update, long sequence, Set<String> groups, RequestScope scope, int steps) {
    return store.compareAndSetRegistry(current, current.advanceConfigurationUpdate(update), scope)
        .thenCompose(ignored -> resume(sequence, groups, scope, steps + 1));
  }

  private static <T> CompletionStage<T> failed(BackendFailure.Category category) {
    return CompletableFuture.failedFuture(new BackendFailure(category));
  }
}
