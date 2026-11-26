package io.etcdhbase.core.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class AuthConfigurationUpdateExecutorTest {
  @Test
  void workersResumePartialFailureAndSwitchOnlyAfterAllOldGroupsAreFenced() {
    var a = group("a");
    var b = group("b");
    var c = group("c");
    var initial = AuthDeploymentRegistry.initial("trust", List.of(a, b));
    var store = new MemoryStore(initial.beginConfigurationUpdate(List.of(b, c)));
    var barriers = new TestBarriers();
    try (var scope = RequestScope.withoutDeadline()) {
      var first = new AuthConfigurationUpdateExecutor(store, barriers).resume(2, Set.of("a"), scope)
          .toCompletableFuture().join();
      assertEquals(1, first.configurationGeneration());
      assertTrue(first.configurationUpdate().targets().getFirst().installed());
      assertFalse(first.configurationUpdate().targets().getLast().installed());
      assertThrows(CompletionException.class, () -> new AuthConfigurationUpdateExecutor(store, barriers)
          .resume(2, Set.of("b"), scope).toCompletableFuture().join());
      assertEquals(1, store.value.configurationGeneration());
      assertEquals(9, store.value.configurationUpdate().targets().getLast().barrierGeneration());
      var completed = new AuthConfigurationUpdateExecutor(store, barriers).resume(2, Set.of("b"), scope)
          .toCompletableFuture().join();
      assertEquals(2, completed.configurationGeneration());
      assertEquals(List.of(b, c), completed.groups());
      assertEquals(null, completed.configurationUpdate());
      assertEquals(1, barriers.aInstalls);
      assertEquals(2, barriers.bInstalls);
    }
  }

  private static final class TestBarriers implements AuthConfigurationUpdateExecutor.Barriers {
    private int aInstalls;
    private int bInstalls;
    @Override public CompletionStage<Long> allocate(AuthConfigurationUpdate.Target target, RequestScope scope) {
      return CompletableFuture.completedFuture(9L);
    }
    @Override public CompletionStage<Void> install(AuthConfigurationUpdate.Target target, RequestScope scope) {
      if (target.tableGroupId().equals("a")) aInstalls++;
      else if (++bInstalls == 1) return CompletableFuture.failedFuture(new IllegalStateException("injected"));
      return CompletableFuture.completedFuture(null);
    }
  }

  private static final class MemoryStore implements AuthDeploymentRegistryStore {
    private AuthDeploymentRegistry value;
    private MemoryStore(AuthDeploymentRegistry value) { this.value = value; }
    @Override public CompletionStage<Optional<AuthDeploymentRegistry>> readRegistry(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(value));
    }
    @Override public CompletionStage<Boolean> createRegistryIfAbsent(AuthDeploymentRegistry initial, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletionStage<Boolean> compareAndSetRegistry(AuthDeploymentRegistry observed,
        AuthDeploymentRegistry replacement, RequestScope scope) {
      if (!observed.equals(value)) return CompletableFuture.completedFuture(false);
      if (!observed.permitsReplacement(replacement)) throw new IllegalArgumentException();
      value = replacement;
      return CompletableFuture.completedFuture(true);
    }
  }

  private static AuthDeploymentRegistry.Group group(String id) {
    return new AuthDeploymentRegistry.Group(id, "provider", "realm", "profile",
        ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]), true);
  }
}
