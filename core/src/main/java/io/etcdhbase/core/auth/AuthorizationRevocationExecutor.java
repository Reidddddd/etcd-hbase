package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

/** Resumes one durable revocation without replacing its target snapshot. */
public final class AuthorizationRevocationExecutor {
  private static final int MAX_STEPS = AuthorizationRevocation.MAX_TARGETS * 4 + 16;

  /**
   * Verifies a durable reservation that prevents the deployment configuration from changing
   * while this operation is installing. A check of a local configuration alone is insufficient.
   */
  public interface ConfigurationFence {
    CompletionStage<Void> validate(AuthorizationRevocation operation, RequestScope scope);
  }

  /** Each adapter must verify the target incarnation; installation is monotone and idempotent. */
  public interface Barriers {
    CompletionStage<Long> allocate(AuthorizationRevocation.TargetIdentity target,
        ByteKey operationId, long operationSequence, long configurationGeneration,
        long credentialGeneration, RequestScope scope);
    CompletionStage<Void> install(AuthorizationRevocation.TargetIdentity target, ByteKey operationId,
        long generation, RequestScope scope);
    CompletionStage<Void> release(AuthorizationRevocation.TargetIdentity target,
        ByteKey operationId, long credentialGeneration, RequestScope scope);
  }

  private final AuthorizationRevocationStore store;
  private final Barriers barriers;
  private final ConfigurationFence configurationFence;

  public AuthorizationRevocationExecutor(AuthorizationRevocationStore store, Barriers barriers,
      ConfigurationFence configurationFence) {
    this.store = Objects.requireNonNull(store, "store");
    this.barriers = Objects.requireNonNull(barriers, "barriers");
    this.configurationFence = Objects.requireNonNull(configurationFence, "configurationFence");
  }

  public CompletionStage<AuthorizationRevocation> resume(ByteKey operationId, RequestScope scope) {
    Objects.requireNonNull(operationId, "operationId");
    Objects.requireNonNull(scope, "scope");
    return resume(operationId, scope, group -> true, 0);
  }

  /** Advances only groups available to this worker and returns an installing snapshot if others remain. */
  public CompletionStage<AuthorizationRevocation> resumeAvailable(ByteKey operationId,
      Set<String> availableGroups, RequestScope scope) {
    Objects.requireNonNull(operationId, "operationId");
    Objects.requireNonNull(scope, "scope");
    var groups = Set.copyOf(Objects.requireNonNull(availableGroups, "availableGroups"));
    return resume(operationId, scope, groups::contains, 0);
  }

  private CompletionStage<AuthorizationRevocation> resume(ByteKey operationId, RequestScope scope,
      Predicate<String> available, int steps) {
    if (steps >= MAX_STEPS) return failed(BackendFailure.Category.RESOURCE_LIMIT);
    if (scope.isDeadlineExceeded()) return failed(BackendFailure.Category.TIMEOUT);
    if (scope.isClosed()) return failed(BackendFailure.Category.CANCELLED);
    return store.readRevocation(operationId, scope).thenCompose(found -> {
      if (found.isEmpty()) return failed(BackendFailure.Category.UNAVAILABLE);
      AuthorizationRevocation operation = found.orElseThrow();
      if (!operation.operationId().equals(operationId)) return failed(BackendFailure.Category.UNAVAILABLE);
      if (operation.state() == AuthorizationRevocation.State.COMPLETED) {
        return CompletableFuture.completedFuture(operation);
      }
      return configurationFence.validate(operation, scope)
          .thenCompose(ignored -> advance(operation, scope, available, steps));
    });
  }

  private CompletionStage<AuthorizationRevocation> advance(AuthorizationRevocation operation,
      RequestScope scope, Predicate<String> available, int steps) {
    var pending = operation.targets().stream().filter(target -> !target.installed()).findFirst();
    if (pending.isEmpty()) {
      var unreleased = operation.targets().stream().filter(target -> !target.released()).findFirst();
      if (unreleased.isEmpty()) return save(operation, operation.finish(), scope, available, steps);
      var eligible = operation.targets().stream().filter(target -> !target.released()
          && available.test(target.identity().tableGroupId())).findFirst();
      if (eligible.isEmpty()) return CompletableFuture.completedFuture(operation);
      var target = eligible.orElseThrow();
      return barriers.release(target.identity(), operation.operationId(), operation.credentialGeneration(), scope)
          .thenCompose(ignored -> save(operation, operation.release(target.identity().tableGroupId()),
              scope, available, steps));
    }
    var eligible = operation.targets().stream().filter(target -> !target.installed()
        && available.test(target.identity().tableGroupId())).findFirst();
    if (eligible.isEmpty()) return CompletableFuture.completedFuture(operation);
    var target = eligible.orElseThrow();
    String group = target.identity().tableGroupId();
    if (target.barrierGeneration() == 0) {
      return barriers.allocate(target.identity(), operation.operationId(), operation.operationSequence(),
          operation.configurationGeneration(), operation.credentialGeneration(), scope)
          .thenCompose(generation ->
          save(operation, operation.assignBarrier(group, generation), scope, available, steps));
    }
    return barriers.install(target.identity(), operation.operationId(), target.barrierGeneration(), scope)
        .thenCompose(ignored ->
        save(operation, operation.acknowledge(group, target.barrierGeneration()), scope, available, steps));
  }

  private CompletionStage<AuthorizationRevocation> save(AuthorizationRevocation observed,
      AuthorizationRevocation replacement, RequestScope scope, Predicate<String> available, int steps) {
    return store.compareAndSetRevocation(observed, replacement, scope)
        .thenCompose(ignored -> resume(observed.operationId(), scope, available, steps + 1));
  }

  private static <T> CompletionStage<T> failed(BackendFailure.Category category) {
    return CompletableFuture.failedFuture(new BackendFailure(category));
  }
}
