package io.etcdhbase.core.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.List;
import org.junit.jupiter.api.Test;

class AuthDeploymentRegistryTest {
  private final AuthDeploymentRegistry.Group a = group("a", 1);
  private final AuthDeploymentRegistry.Group b = group("b", 2);

  @Test
  void configurationReservationExcludesRevocationAndRequiresEveryBarrierBeforeSwitch() {
    var initial = AuthDeploymentRegistry.initial("trust", List.of(a, b));
    var reserved = initial.beginConfigurationUpdate(List.of(a));
    assertTrue(initial.permitsReplacement(reserved));
    assertThrows(IllegalStateException.class, () -> reserved.beginRevocation(operation(initial)));
    assertThrows(IllegalStateException.class, reserved::finishConfigurationUpdate);
    var assignedA = reserved.advanceConfigurationUpdate(reserved.configurationUpdate().assign("a", 5));
    assertTrue(reserved.permitsReplacement(assignedA));
    var installedA = assignedA.advanceConfigurationUpdate(assignedA.configurationUpdate().acknowledge("a", 5));
    assertThrows(IllegalStateException.class, installedA::finishConfigurationUpdate);
    var assignedB = installedA.advanceConfigurationUpdate(installedA.configurationUpdate().assign("b", 6));
    var installedB = assignedB.advanceConfigurationUpdate(assignedB.configurationUpdate().acknowledge("b", 6));
    var finished = installedB.finishConfigurationUpdate();
    assertTrue(installedB.permitsReplacement(finished));
    assertEquals(2, finished.configurationGeneration());
    assertEquals(List.of(a), finished.groups());
    assertEquals(null, finished.configurationUpdate());
    assertFalse(initial.permitsReplacement(initial.updateConfiguration(List.of(a))));
  }

  @Test
  void reservesTheExactInventoryAndBlocksConfigurationUpdatesUntilCompletion() {
    var registry = AuthDeploymentRegistry.initial("trust", List.of(a, b));
    var operation = operation(registry);
    var reserved = registry.beginRevocation(operation);
    assertTrue(registry.permitsReplacement(reserved));
    assertThrows(IllegalStateException.class, () -> reserved.updateConfiguration(List.of(a)));
    assertThrows(IllegalStateException.class, () -> reserved.beginRevocation(operation));
    var progress = operation.assignBarrier("a", 1);
    var advanced = reserved.advanceRevocation(progress);
    assertTrue(reserved.permitsReplacement(advanced));
    assertFalse(registry.permitsReplacement(advanced));
    var done = progress.acknowledge("a", 1).assignBarrier("b", 2)
        .acknowledge("b", 2).release("a").release("b").finish();
    assertThrows(IllegalArgumentException.class, () -> advanced.advanceRevocation(done));
    var completed = advanced.advanceRevocation(progress.acknowledge("a", 1));
    completed = completed.advanceRevocation(completed.revocation().assignBarrier("b", 2));
    completed = completed.advanceRevocation(completed.revocation().acknowledge("b", 2));
    completed = completed.advanceRevocation(completed.revocation().release("a"));
    completed = completed.advanceRevocation(completed.revocation().release("b"));
    completed = completed.advanceRevocation(completed.revocation().finish());
    var updated = completed.updateConfiguration(List.of(a));
    assertFalse(completed.permitsReplacement(updated));
    assertEquals(2, updated.configurationGeneration());
    assertEquals(List.of(a), updated.groups());
    assertEquals(null, updated.revocation());
  }

  @Test
  void rejectsMissingChangedOrDuplicateIncarnations() {
    var registry = AuthDeploymentRegistry.initial("trust", List.of(a, b));
    var missing = AuthorizationRevocation.start(bytes(1, 16), bytes(2, 32), "provider", "realm", 2, 2, 1,
        List.of(target(a, 3)));
    assertThrows(IllegalArgumentException.class, () -> registry.beginRevocation(missing));
    var changed = AuthorizationRevocation.start(bytes(1, 16), bytes(2, 32), "provider", "realm", 2, 2, 1,
        List.of(target(a, 3), target(group("b", 9), 4)));
    assertThrows(IllegalArgumentException.class, () -> registry.beginRevocation(changed));
    assertThrows(IllegalArgumentException.class,
        () -> AuthDeploymentRegistry.initial("trust", List.of(a, a)));
  }

  @Test
  void snapshotIncludesOnlyActiveGroupsInTheSameProviderAndIdentityDomain() {
    var otherProvider = new AuthDeploymentRegistry.Group("c", "other", "realm", "profile",
        bytes(3, 32), bytes(13, 32), true);
    var otherDomain = new AuthDeploymentRegistry.Group("d", "provider", "other", "profile",
        bytes(4, 32), bytes(14, 32), true);
    var disabled = new AuthDeploymentRegistry.Group("e", "provider", "realm", "profile",
        bytes(5, 32), bytes(15, 32), false);
    var registry = AuthDeploymentRegistry.initial("trust", List.of(a, b, otherProvider, otherDomain, disabled));
    assertTrue(registry.permitsReplacement(registry.beginRevocation(operation(registry))));
    var includesOtherDomain = AuthorizationRevocation.start(bytes(1, 16), bytes(2, 32),
        "provider", "realm", 2, 2, 1, List.of(target(a, 3), target(b, 4), target(otherDomain, 5)));
    assertThrows(IllegalArgumentException.class, () -> registry.beginRevocation(includesOtherDomain));
  }

  private AuthorizationRevocation operation(AuthDeploymentRegistry registry) {
    return AuthorizationRevocation.start(bytes(1, 16), bytes(2, 32), "provider", "realm", 2, 2,
        registry.configurationGeneration(), List.of(target(a, 3), target(b, 4)));
  }

  private static AuthDeploymentRegistry.Group group(String id, int dataset) {
    return new AuthDeploymentRegistry.Group(id, "provider", "realm", "profile", bytes(dataset, 32),
        bytes(dataset + 10, 32), true);
  }

  private static AuthorizationRevocation.TargetIdentity target(AuthDeploymentRegistry.Group group, int authority) {
    return new AuthorizationRevocation.TargetIdentity(group.tableGroupId(), group.datasetDigest(),
        bytes(authority, 32));
  }

  private static ByteKey bytes(int value, int length) {
    byte[] data = new byte[length];
    java.util.Arrays.fill(data, (byte) value);
    return ByteKey.copyOf(data);
  }
}
