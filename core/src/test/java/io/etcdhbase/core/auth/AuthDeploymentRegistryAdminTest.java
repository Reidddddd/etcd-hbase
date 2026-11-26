package io.etcdhbase.core.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

class AuthDeploymentRegistryAdminTest {
  @Test
  void reserveDoesNotPublishTheReplacementOrPermitAnotherOperation() {
    var group = group("a", 1);
    var store = new MemoryStore();
    var admin = new AuthDeploymentRegistryAdmin(store);
    try (var scope = RequestScope.withoutDeadline()) {
      assertTrue(admin.initialize("trust", List.of(group), scope).toCompletableFuture().join());
      assertTrue(admin.reserveUpdate(1, List.of(group("b", 2)), scope).toCompletableFuture().join());
      assertEquals(1, store.current.configurationGeneration());
      assertEquals(List.of(group), store.current.groups());
      assertEquals(2, store.current.configurationUpdate().operationSequence());
      assertFalse(admin.reserveUpdate(1, List.of(group("c", 3)), scope).toCompletableFuture().join());
    }
  }

  @Test
  void explicitInitializationAndUpdateUseFullInventoryAndRejectInFlightRevocation() {
    var group = group("a", 1);
    var store = new MemoryStore();
    var admin = new AuthDeploymentRegistryAdmin(store);
    try (var scope = RequestScope.withoutDeadline()) {
      assertTrue(admin.initialize("trust", List.of(group), scope).toCompletableFuture().join());
      assertFalse(admin.initialize("trust", List.of(group), scope).toCompletableFuture().join());
      assertFalse(admin.update(2, List.of(group("b", 2)), scope).toCompletableFuture().join());
      var op = AuthorizationRevocation.start(bytes(3, 16), bytes(4, 32), "provider", "realm", 2, 2, 1,
          List.of(new AuthorizationRevocation.TargetIdentity("a", group.datasetDigest(), bytes(5, 32))));
      store.current = store.current.beginRevocation(op);
      assertThrows(CompletionException.class,
          () -> admin.update(1, List.of(group("b", 2)), scope).toCompletableFuture().join());
      var done = op.assignBarrier("a", 1).acknowledge("a", 1).release("a").finish();
      store.current = new AuthDeploymentRegistry("trust", 1, 7, List.of(group), done);
      assertThrows(CompletionException.class,
          () -> admin.update(1, List.of(group("b", 2)), scope).toCompletableFuture().join());
      assertEquals(1, store.current.configurationGeneration());
      assertEquals(List.of(group), store.current.groups());
    }
  }

  private static final class MemoryStore implements AuthDeploymentRegistryStore {
    private AuthDeploymentRegistry current;
    @Override public CompletionStage<Optional<AuthDeploymentRegistry>> readRegistry(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(current));
    }
    @Override public CompletionStage<Boolean> createRegistryIfAbsent(AuthDeploymentRegistry initial, RequestScope scope) {
      if (current != null) return CompletableFuture.completedFuture(false);
      current = initial;
      return CompletableFuture.completedFuture(true);
    }
    @Override public CompletionStage<Boolean> compareAndSetRegistry(AuthDeploymentRegistry observed,
        AuthDeploymentRegistry replacement, RequestScope scope) {
      if (!observed.equals(current) || !observed.permitsReplacement(replacement)) {
        return CompletableFuture.completedFuture(false);
      }
      current = replacement;
      return CompletableFuture.completedFuture(true);
    }
  }

  private static AuthDeploymentRegistry.Group group(String id, int value) {
    return new AuthDeploymentRegistry.Group(id, "provider", "realm", "profile", bytes(value, 32),
        bytes(value + 10, 32), true);
  }
  private static ByteKey bytes(int value, int length) {
    byte[] data = new byte[length];
    java.util.Arrays.fill(data, (byte) value);
    return ByteKey.copyOf(data);
  }
}
