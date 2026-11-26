package io.etcdhbase.core.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class AuthorizationRevocationExecutorTest {
  @Test
  void workersForDifferentGroupsCooperateWithoutReleasingBeforeAllBarriers() {
    var initial = operation();
    var store = new MemoryStore(initial);
    var barriers = new Barriers();
    var executor = new AuthorizationRevocationExecutor(store, barriers, new Fence(initial));
    try (var scope = RequestScope.withoutDeadline()) {
      var afterB = executor.resumeAvailable(initial.operationId(), Set.of("group-b"), scope)
          .toCompletableFuture().join();
      assertFalse(afterB.targets().get(0).installed());
      assertTrue(afterB.targets().get(1).installed());
      assertTrue(barriers.releases.isEmpty());

      var afterA = executor.resumeAvailable(initial.operationId(), Set.of("group-a"), scope)
          .toCompletableFuture().join();
      assertTrue(afterA.targets().stream().allMatch(AuthorizationRevocation.Target::installed));
      assertTrue(afterA.targets().get(0).released());
      assertFalse(afterA.targets().get(1).released());

      var done = executor.resumeAvailable(initial.operationId(), Set.of("group-b"), scope)
          .toCompletableFuture().join();
      assertEquals(AuthorizationRevocation.State.COMPLETED, done.state());
      assertEquals(Map.of("group-a", 1, "group-b", 1), barriers.installations);
      assertEquals(Map.of("group-a", 1, "group-b", 1), barriers.releases);
    }
  }

  @Test
  void partialFailureKeepsTheSnapshotAndRestartResumesTheSameBarriers() {
    var initial = operation();
    var store = new MemoryStore(initial);
    var barriers = new Barriers();
    barriers.failGroup = "group-b";
    var fence = new Fence(initial);
    try (var scope = RequestScope.withoutDeadline()) {
      var first = new AuthorizationRevocationExecutor(store, barriers, fence);
      assertThrows(CompletionException.class, () -> first.resume(initial.operationId(), scope)
          .toCompletableFuture().join());
      var pending = store.current;
      assertEquals(AuthorizationRevocation.State.INSTALLING, pending.state());
      assertTrue(pending.targets().get(0).installed());
      assertFalse(pending.targets().get(1).installed());
      assertEquals(11, pending.targets().get(1).barrierGeneration());

      var restarted = new AuthorizationRevocationExecutor(store, barriers, fence);
      var completed = restarted.resume(initial.operationId(), scope).toCompletableFuture().join();
      assertEquals(AuthorizationRevocation.State.COMPLETED, completed.state());
      assertEquals(1, barriers.allocations.get("group-a"));
      assertEquals(1, barriers.allocations.get("group-b"));
      assertEquals(1, barriers.installations.get("group-a"));
      assertEquals(1, barriers.installations.get("group-b"));
      assertEquals(initial.configurationGeneration(), completed.configurationGeneration());
      assertEquals(initial.targets().stream().map(AuthorizationRevocation.Target::identity).toList(),
          completed.targets().stream().map(AuthorizationRevocation.Target::identity).toList());
    }
  }

  @Test
  void unknownCasOutcomeIsRecoveredFromTheSameDurableOperation() {
    var initial = operation();
    var store = new MemoryStore(initial);
    store.failAfterWrite = true;
    var barriers = new Barriers();
    var fence = new Fence(initial);
    try (var scope = RequestScope.withoutDeadline()) {
      var first = new AuthorizationRevocationExecutor(store, barriers, fence);
      assertThrows(CompletionException.class, () -> first.resume(initial.operationId(), scope)
          .toCompletableFuture().join());
      assertEquals(11, store.current.targets().get(0).barrierGeneration());
      var recovered = new AuthorizationRevocationExecutor(store, barriers, fence)
          .resume(initial.operationId(), scope).toCompletableFuture().join();
      assertEquals(AuthorizationRevocation.State.COMPLETED, recovered.state());
      assertEquals(1, barriers.allocations.get("group-a"));
    }
  }

  @Test
  void changedConfigurationKeepsTheOperationInstalling() {
    var initial = operation();
    var store = new MemoryStore(initial);
    var fence = new Fence(initial);
    fence.generation++;
    var barriers = new Barriers();
    try (var scope = RequestScope.withoutDeadline()) {
      assertThrows(CompletionException.class,
          () -> new AuthorizationRevocationExecutor(store, barriers, fence)
              .resume(initial.operationId(), scope).toCompletableFuture().join());
      assertEquals(initial, store.current);
      assertTrue(barriers.allocations.isEmpty());
    }
  }

  @Test
  void unknownAcknowledgementAfterBarrierInstallationDoesNotInstallItTwice() {
    var initial = operation();
    var store = new MemoryStore(initial);
    store.failAfterRevision = 3;
    var barriers = new Barriers();
    var fence = new Fence(initial);
    try (var scope = RequestScope.withoutDeadline()) {
      assertThrows(CompletionException.class,
          () -> new AuthorizationRevocationExecutor(store, barriers, fence)
              .resume(initial.operationId(), scope).toCompletableFuture().join());
      assertTrue(store.current.targets().getFirst().installed());
      var completed = new AuthorizationRevocationExecutor(store, barriers, fence)
          .resume(initial.operationId(), scope).toCompletableFuture().join();
      assertEquals(AuthorizationRevocation.State.COMPLETED, completed.state());
      assertEquals(1, barriers.installations.get("group-a"));
    }
  }

  private static AuthorizationRevocation operation() {
    return AuthorizationRevocation.start(bytes(1, 16), bytes(2, 32), "provider", "realm", 2, 2, 3,
        List.of(new AuthorizationRevocation.TargetIdentity("group-a", bytes(3, 32), bytes(4, 32)),
            new AuthorizationRevocation.TargetIdentity("group-b", bytes(5, 32), bytes(6, 32))));
  }

  private static final class MemoryStore implements AuthorizationRevocationStore {
    private AuthorizationRevocation current;
    private boolean failAfterWrite;
    private long failAfterRevision;
    private MemoryStore(AuthorizationRevocation current) { this.current = current; }
    @Override public CompletionStage<Optional<AuthorizationRevocation>> readRevocation(
        ByteKey operationId, RequestScope scope) {
      return CompletableFuture.completedFuture(current.operationId().equals(operationId)
          ? Optional.of(current) : Optional.empty());
    }
    @Override public CompletionStage<Boolean> createRevocationIfAbsent(
        AuthorizationRevocation initial, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletionStage<Boolean> compareAndSetRevocation(
        AuthorizationRevocation observed, AuthorizationRevocation replacement, RequestScope scope) {
      if (!current.equals(observed)) return CompletableFuture.completedFuture(false);
      current = replacement;
      if (failAfterWrite || replacement.revision() == failAfterRevision) {
        failAfterWrite = false;
        failAfterRevision = 0;
        return CompletableFuture.failedFuture(new IllegalStateException("unknown write outcome"));
      }
      return CompletableFuture.completedFuture(true);
    }
  }

  private static final class Fence implements AuthorizationRevocationExecutor.ConfigurationFence {
    private final ByteKey operationId;
    private long generation;
    private Fence(AuthorizationRevocation operation) {
      operationId = operation.operationId();
      generation = operation.configurationGeneration();
    }
    @Override public CompletionStage<Void> validate(AuthorizationRevocation operation, RequestScope scope) {
      return operationId.equals(operation.operationId()) && generation == operation.configurationGeneration()
          ? CompletableFuture.completedFuture(null)
          : CompletableFuture.failedFuture(new IllegalStateException("configuration changed"));
    }
  }

  private static final class Barriers implements AuthorizationRevocationExecutor.Barriers {
    private final Map<String, Integer> allocations = new HashMap<>();
    private final Map<String, Integer> installations = new HashMap<>();
    private final Map<String, Integer> releases = new HashMap<>();
    private String failGroup;
    @Override public CompletionStage<Long> allocate(AuthorizationRevocation.TargetIdentity target,
        ByteKey operationId, long operationSequence, long configurationGeneration,
        long credentialGeneration, RequestScope scope) {
      allocations.merge(target.tableGroupId(), 1, Integer::sum);
      return CompletableFuture.completedFuture(11L);
    }
    @Override public CompletionStage<Void> install(AuthorizationRevocation.TargetIdentity target,
        ByteKey operationId, long generation, RequestScope scope) {
      if (target.tableGroupId().equals(failGroup)) {
        failGroup = null;
        return CompletableFuture.failedFuture(new IllegalStateException("injected group failure"));
      }
      installations.merge(target.tableGroupId(), 1, Integer::sum);
      return CompletableFuture.completedFuture(null);
    }
    @Override public CompletionStage<Void> release(AuthorizationRevocation.TargetIdentity target,
        ByteKey operationId, long credentialGeneration, RequestScope scope) {
      releases.merge(target.tableGroupId(), 1, Integer::sum);
      return CompletableFuture.completedFuture(null);
    }
  }

  private static ByteKey bytes(int value, int length) {
    byte[] raw = new byte[length];
    java.util.Arrays.fill(raw, (byte) value);
    return ByteKey.copyOf(raw);
  }
}
