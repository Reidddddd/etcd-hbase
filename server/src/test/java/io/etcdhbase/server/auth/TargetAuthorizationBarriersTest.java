package io.etcdhbase.server.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.core.auth.AuthDeploymentRegistryStore;
import io.etcdhbase.core.auth.AuthSessionStore;
import io.etcdhbase.core.auth.AuthorizationRevocation;
import io.etcdhbase.core.auth.SubjectAuthority;
import io.etcdhbase.core.auth.TargetAuthorizationBarriers;
import io.etcdhbase.core.auth.TargetConfigurationBarriers;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.AuthorizationBarrierInstaller;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.security.SecureRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class TargetAuthorizationBarriersTest {
  @Test
  void partialFailureAndRestartFenceAndReleaseEveryRegisteredGroup() {
    var groupA = group("a", 1);
    var groupB = group("b", 2);
    var initial = AuthDeploymentRegistry.initial("trust", List.of(groupA, groupB));
    var registry = new MemoryRegistry(initial);
    var identity = new AuthorizedBackendIdentity("provider", "alice", "context",
        new AuthenticationScope("a", "profile"), Set.of("a"), 4);
    var snapshot = AuthorizationRevocationSnapshot.create(initial, "realm", identity, bytes(9, 16));
    var targetA = snapshot.targets().get(0).identity();
    var targetB = snapshot.targets().get(1).identity();
    var authorities = new HashMap<ByteKey, SubjectAuthority>();
    var oldA = authority(targetA, "a");
    var oldB = authority(targetB, "b");
    authorities.put(targetA.authorityDigest(), oldA);
    authorities.put(targetB.authorityDigest(), oldB);
    var authStore = mock(AuthSessionStore.class);
    when(authStore.readAuthority(any(), any())).thenAnswer(call ->
        CompletableFuture.completedFuture(Optional.ofNullable(authorities.get(call.getArgument(0)))));
    when(authStore.createAuthorityIfAbsent(any(), any())).thenAnswer(call -> {
      SubjectAuthority value = call.getArgument(0);
      return CompletableFuture.completedFuture(authorities.putIfAbsent(value.subjectDigest(), value) == null);
    });
    when(authStore.compareAndSetAuthority(any(), any(), any())).thenAnswer(call -> {
      SubjectAuthority observed = call.getArgument(0);
      SubjectAuthority replacement = call.getArgument(1);
      if (!observed.equals(authorities.get(observed.subjectDigest()))) return CompletableFuture.completedFuture(false);
      if (!observed.allowsTransitionTo(replacement)) throw new IllegalArgumentException();
      authorities.put(observed.subjectDigest(), replacement);
      return CompletableFuture.completedFuture(true);
    });
    var controlA = new MemoryControl();
    var controlB = new MemoryControl();
    controlB.failRead = 2;
    var barriers = new TargetAuthorizationBarriers(authStore, "trust", Map.of(
        "a", binding(groupA, controlA), "b", binding(groupB, controlB)));
    try (var scope = RequestScope.withoutDeadline()) {
      var stale = new DeploymentRevocationCoordinator(registry, authStore, barriers, "trust", "realm",
          bytes(99, 32), groupA.configurationDigest(), new SecureRandom());
      assertThrows(CompletionException.class,
          () -> stale.ensureCurrent(identity, scope).toCompletableFuture().join());
      assertEquals(initial, registry.current);
      var coordinator = new DeploymentRevocationCoordinator(registry, authStore, barriers, "trust", "realm",
          groupA.datasetDigest(), groupA.configurationDigest(), new SecureRandom());
      assertThrows(CompletionException.class,
          () -> coordinator.ensureCurrent(identity, scope).toCompletableFuture().join());
      var op = registry.current.revocation();
      assertEquals(SubjectAuthority.State.REVOKING, authorities.get(targetA.authorityDigest()).state());
      assertTrue(registry.current.revocation().targets().getFirst().installed());
      assertFalse(registry.current.revocation().targets().getFirst().released());
      assertThrows(IllegalStateException.class, () -> registry.current.updateConfiguration(List.of(groupA)));
      var barriersB = new TargetAuthorizationBarriers(authStore, "trust", Map.of("b", binding(groupB, controlB)));
      var restartedB = new DeploymentRevocationCoordinator(registry, authStore, barriersB, "trust", "realm",
          groupB.datasetDigest(), groupB.configurationDigest(), new SecureRandom(), Set.of("b"),
          new TargetConfigurationBarriers(Map.of("b", new TargetConfigurationBarriers.Group(
              groupB.datasetDigest(), new AuthorizationBarrierInstaller(controlB, Clock.fixed(
                  Instant.ofEpochMilli(10), ZoneOffset.UTC))))));
      var staleB = new DeploymentRevocationCoordinator(registry, authStore, barriersB, "trust", "realm",
          groupB.datasetDigest(), bytes(99, 32), new SecureRandom(), Set.of("b"),
          new TargetConfigurationBarriers(Map.of("b", new TargetConfigurationBarriers.Group(
              groupB.datasetDigest(), new AuthorizationBarrierInstaller(controlB, Clock.fixed(
                  Instant.ofEpochMilli(10), ZoneOffset.UTC))))));
      var beforeRecovery = registry.current;
      assertThrows(CompletionException.class, () -> staleB.resumePending("provider",
          new AuthenticationScope("b", "profile"), scope).toCompletableFuture().join());
      assertEquals(beforeRecovery, registry.current);
      assertThrows(CompletionException.class, () -> restartedB.resumePending("provider",
          new AuthenticationScope("b", "profile"), scope).toCompletableFuture().join());
      assertEquals(AuthorizationRevocation.State.INSTALLING, registry.current.revocation().state());
      assertTrue(registry.current.revocation().targets().get(1).released());
      var barriersA = new TargetAuthorizationBarriers(authStore, "trust", Map.of("a", binding(groupA, controlA)));
      var restartedA = new DeploymentRevocationCoordinator(registry, authStore, barriersA, "trust", "realm",
          groupA.datasetDigest(), groupA.configurationDigest(), new SecureRandom(), Set.of("a"),
          new TargetConfigurationBarriers(Map.of("a", new TargetConfigurationBarriers.Group(
              groupA.datasetDigest(), new AuthorizationBarrierInstaller(controlA, Clock.fixed(
                  Instant.ofEpochMilli(10), ZoneOffset.UTC))))));
      restartedA.resumePending("provider", new AuthenticationScope("a", "profile"), scope)
          .toCompletableFuture().join();
      assertEquals(AuthorizationRevocation.State.COMPLETED, registry.current.revocation().state());
      for (var target : List.of(targetA, targetB)) {
        var authority = authorities.get(target.authorityDigest());
        assertEquals(SubjectAuthority.State.ACTIVE, authority.state());
        assertEquals(4, authority.credentialGeneration());
        assertEquals(op.operationId(), authority.revocationOperationId());
      }
      assertEquals(1, controlA.current.state().authorizationBarrierGeneration());
      assertEquals(1, controlB.current.state().authorizationBarrierGeneration());
      assertThrows(CompletionException.class,
          () -> barriers.allocate(targetA, op.operationId(), op.operationSequence(),
              op.configurationGeneration(), 4, scope).toCompletableFuture().join());
    }
  }

  private static TargetAuthorizationBarriers.Group binding(AuthDeploymentRegistry.Group group, MemoryControl control) {
    var clock = Clock.fixed(Instant.ofEpochMilli(20), ZoneOffset.UTC);
    return new TargetAuthorizationBarriers.Group("provider", new AuthenticationScope(group.tableGroupId(), "profile"),
        group.datasetDigest(), new AuthorizationBarrierInstaller(control, clock));
  }

  private static SubjectAuthority authority(AuthorizationRevocation.TargetIdentity target, String group) {
    return new SubjectAuthority(target.authorityDigest(), "trust", "provider",
        new AuthenticationScope(group, "profile"), 3, 1, SubjectAuthority.State.ACTIVE);
  }

  private static final class MemoryControl implements CommitControlStore {
    private ControlSnapshot current;
    private int reads;
    private int failRead;
    private MemoryControl() {
      var owned = CommitState.initial(bytes(0, 32), 1).claimOwner("owner", 1, 100);
      current = snapshot(owned.enableAuthorizationBarrier("owner", owned.ownerEpoch(), 19));
    }
    @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      if (++reads == failRead) return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      return CompletableFuture.completedFuture(Optional.of(current));
    }
    @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState state, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<Boolean> compareAndSet(ControlSnapshot observed, CommitState next, RequestScope scope) {
      if (current != observed) return CompletableFuture.completedFuture(false);
      current = snapshot(next);
      return CompletableFuture.completedFuture(true);
    }
    private static ControlSnapshot snapshot(CommitState state) {
      return new ControlSnapshot(state, ByteBuffer.allocate(8).putLong(state.stateVersion()).array());
    }
  }

  private static final class MemoryRegistry implements AuthDeploymentRegistryStore {
    private AuthDeploymentRegistry current;
    private MemoryRegistry(AuthDeploymentRegistry value) { current = value; }
    @Override public CompletionStage<Optional<AuthDeploymentRegistry>> readRegistry(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(current));
    }
    @Override public CompletionStage<Boolean> createRegistryIfAbsent(AuthDeploymentRegistry initial, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletionStage<Boolean> compareAndSetRegistry(AuthDeploymentRegistry observed,
        AuthDeploymentRegistry next, RequestScope scope) {
      if (!current.equals(observed)) return CompletableFuture.completedFuture(false);
      if (!observed.permitsReplacement(next)) throw new IllegalArgumentException();
      current = next;
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
