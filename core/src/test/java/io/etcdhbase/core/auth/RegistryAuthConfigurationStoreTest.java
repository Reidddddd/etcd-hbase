package io.etcdhbase.core.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class RegistryAuthConfigurationStoreTest {
  @Test
  void statusDoesNotReportAnActiveConfigurationDuringAnUpdate() {
    var group = new AuthDeploymentRegistry.Group("group", "provider", "realm", "profile",
        bytes(1), bytes(2), true);
    var next = new AuthDeploymentRegistry.Group("group", "provider", "realm", "profile",
        bytes(1), bytes(3), true);
    var registry = AuthDeploymentRegistry.initial("trust", List.of(group)).beginConfigurationUpdate(List.of(next));
    try (var scope = RequestScope.withoutDeadline()) {
      assertThrows(CompletionException.class, () -> new RegistryAuthConfigurationStore(new ReadOnlyStore(registry))
          .readConfiguration("group", scope).toCompletableFuture().join());
    }
  }

  @Test
  void projectsDeploymentGenerationFromTheRegistry() {
    var group = new AuthDeploymentRegistry.Group("group", "provider", "realm", "profile",
        bytes(1), bytes(2), true);
    var registry = AuthDeploymentRegistry.initial("trust", List.of(group));
    var replacement = new AuthDeploymentRegistry.Group("group", "provider", "realm", "profile",
        bytes(1), bytes(3), false);
    var changed = registry.updateConfiguration(List.of(replacement));
    var adapter = new RegistryAuthConfigurationStore(new ReadOnlyStore(changed));
    try (var scope = RequestScope.withoutDeadline()) {
      var record = adapter.readConfiguration("group", scope).toCompletableFuture().join().orElseThrow();
      assertEquals(2, record.revision());
      assertEquals(bytes(3), record.configurationDigest());
      assertEquals(false, record.enabled());
      assertTrue(adapter.readConfiguration("missing", scope).toCompletableFuture().join().isEmpty());
    }
  }

  private record ReadOnlyStore(AuthDeploymentRegistry value) implements AuthDeploymentRegistryStore {
    @Override public CompletionStage<Optional<AuthDeploymentRegistry>> readRegistry(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(value));
    }
    @Override public CompletionStage<Boolean> createRegistryIfAbsent(AuthDeploymentRegistry initial,
        RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletionStage<Boolean> compareAndSetRegistry(AuthDeploymentRegistry observed,
        AuthDeploymentRegistry replacement, RequestScope scope) { throw new UnsupportedOperationException(); }
  }

  private static ByteKey bytes(int value) {
    byte[] data = new byte[32];
    java.util.Arrays.fill(data, (byte) value);
    return ByteKey.copyOf(data);
  }
}
