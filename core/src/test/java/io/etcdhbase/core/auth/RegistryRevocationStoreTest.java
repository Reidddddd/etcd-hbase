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

class RegistryRevocationStoreTest {
  @Test
  void staleReservationRetriesAfterAnotherOperationOrConfigurationUpdate() {
    var group = group("a", 1);
    var initial = AuthDeploymentRegistry.initial("trust", List.of(group));
    var op = AuthorizationRevocation.start(bytes(3, 16), bytes(4, 32), "provider", "realm", 2, 2, 1,
        List.of(new AuthorizationRevocation.TargetIdentity("a", group.datasetDigest(), bytes(5, 32))));
    var registry = new MemoryRegistry(initial);
    var store = new RegistryRevocationStore(registry);
    try (var scope = RequestScope.withoutDeadline()) {
      registry.current = initial.updateConfiguration(List.of(group("a", 2)));
      assertFalse(store.createRevocationIfAbsent(op, scope).toCompletableFuture().join());
      assertEquals(2, registry.current.configurationGeneration());

      var other = AuthorizationRevocation.start(bytes(6, 16), bytes(7, 32), "provider", "realm", 2, 2, 1,
          List.of(new AuthorizationRevocation.TargetIdentity("a", group.datasetDigest(), bytes(8, 32))));
      registry.current = initial.beginRevocation(other);
      var done = other.assignBarrier("a", 1).acknowledge("a", 1).release("a").finish();
      registry.current = new AuthDeploymentRegistry("trust", 1, 7, List.of(group), done);
      assertFalse(store.createRevocationIfAbsent(op, scope).toCompletableFuture().join());
      assertEquals(done, registry.current.revocation());

      var invalid = AuthorizationRevocation.start(bytes(9, 16), bytes(4, 32), "provider", "realm", 2, 8, 1,
          List.of(new AuthorizationRevocation.TargetIdentity("a", bytes(99, 32), bytes(5, 32))));
      assertThrows(CompletionException.class,
          () -> store.createRevocationIfAbsent(invalid, scope).toCompletableFuture().join());
      assertEquals(done, registry.current.revocation());
    }
  }

  @Test
  void reservationAndConfigurationUpdateCompeteOnTheSameRow() {
    var group = group("a", 1);
    var initial = AuthDeploymentRegistry.initial("trust", List.of(group));
    var registry = new MemoryRegistry(initial);
    var operations = new RegistryRevocationStore(registry);
    var op = AuthorizationRevocation.start(bytes(3, 16), bytes(4, 32), "provider", "realm", 2, 2, 1,
        List.of(new AuthorizationRevocation.TargetIdentity("a", group.datasetDigest(), bytes(5, 32))));
    try (var scope = RequestScope.withoutDeadline()) {
      var staleUpdate = initial.updateConfiguration(List.of(group("b", 2)));
      assertTrue(operations.createRevocationIfAbsent(op, scope).toCompletableFuture().join());
      assertFalse(registry.compareAndSetRegistry(initial, staleUpdate, scope).toCompletableFuture().join());
      assertEquals(op, operations.readRevocation(op.operationId(), scope).toCompletableFuture().join().orElseThrow());
      operations.validate(op, scope).toCompletableFuture().join();
      var assigned = op.assignBarrier("a", 7);
      assertTrue(operations.compareAndSetRevocation(op, assigned, scope).toCompletableFuture().join());
      assertFalse(operations.compareAndSetRevocation(op, assigned, scope).toCompletableFuture().join());
      assertThrows(CompletionException.class, () -> operations.validate(op, scope).toCompletableFuture().join());
      assertEquals(assigned, new RegistryRevocationStore(registry)
          .readRevocation(op.operationId(), scope).toCompletableFuture().join().orElseThrow());
    }
  }

  @Test
  void restartedExecutorResumesAReservedInventoryAfterPartialInstallation() {
    var a = group("a", 1);
    var b = group("b", 2);
    var registry = new MemoryRegistry(AuthDeploymentRegistry.initial("trust", List.of(a, b)));
    var store = new RegistryRevocationStore(registry);
    var op = AuthorizationRevocation.start(bytes(3, 16), bytes(4, 32), "provider", "realm", 2, 2, 1,
        List.of(new AuthorizationRevocation.TargetIdentity("a", a.datasetDigest(), bytes(5, 32)),
            new AuthorizationRevocation.TargetIdentity("b", b.datasetDigest(), bytes(6, 32))));
    var attempts = new java.util.HashMap<String, Integer>();
    var barriers = new AuthorizationRevocationExecutor.Barriers() {
      @Override public CompletionStage<Long> allocate(AuthorizationRevocation.TargetIdentity target,
          ByteKey operationId, long operationSequence, long configurationGeneration,
          long credentialGeneration, RequestScope scope) {
        return CompletableFuture.completedFuture(7L);
      }
      @Override public CompletionStage<Void> install(AuthorizationRevocation.TargetIdentity target,
          ByteKey operationId, long generation, RequestScope scope) {
        int count = attempts.merge(target.tableGroupId(), 1, Integer::sum);
        if (target.tableGroupId().equals("b") && count == 1) {
          return CompletableFuture.failedFuture(new IllegalStateException("injected failure"));
        }
        return CompletableFuture.completedFuture(null);
      }
      @Override public CompletionStage<Void> release(AuthorizationRevocation.TargetIdentity target,
          ByteKey operationId, long credentialGeneration, RequestScope scope) {
        return CompletableFuture.completedFuture(null);
      }
    };
    try (var scope = RequestScope.withoutDeadline()) {
      assertTrue(store.createRevocationIfAbsent(op, scope).toCompletableFuture().join());
      assertThrows(CompletionException.class, () -> new AuthorizationRevocationExecutor(store, barriers, store)
          .resume(op.operationId(), scope).toCompletableFuture().join());
      assertTrue(registry.current.revocation().targets().getFirst().installed());
      assertThrows(IllegalStateException.class, () -> registry.current.updateConfiguration(List.of(a)));
      var restartedStore = new RegistryRevocationStore(registry);
      var completed = new AuthorizationRevocationExecutor(restartedStore, barriers, restartedStore)
          .resume(op.operationId(), scope).toCompletableFuture().join();
      assertEquals(AuthorizationRevocation.State.COMPLETED, completed.state());
      assertEquals(1, attempts.get("a"));
      assertEquals(2, attempts.get("b"));
      assertFalse(registry.current.permitsReplacement(registry.current.updateConfiguration(List.of(a))));
    }
  }

  private static final class MemoryRegistry implements AuthDeploymentRegistryStore {
    private AuthDeploymentRegistry current;
    private MemoryRegistry(AuthDeploymentRegistry current) { this.current = current; }
    @Override public CompletionStage<Optional<AuthDeploymentRegistry>> readRegistry(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(current));
    }
    @Override public CompletionStage<Boolean> createRegistryIfAbsent(AuthDeploymentRegistry initial,
        RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletionStage<Boolean> compareAndSetRegistry(AuthDeploymentRegistry observed,
        AuthDeploymentRegistry replacement, RequestScope scope) {
      if (!current.equals(observed)) return CompletableFuture.completedFuture(false);
      if (!observed.permitsReplacement(replacement)) throw new IllegalArgumentException();
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
