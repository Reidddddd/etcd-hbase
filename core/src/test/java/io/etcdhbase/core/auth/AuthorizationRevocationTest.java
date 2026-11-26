package io.etcdhbase.core.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.List;
import org.junit.jupiter.api.Test;

class AuthorizationRevocationTest {
  private static final AuthorizationRevocation.TargetIdentity GROUP_A =
      new AuthorizationRevocation.TargetIdentity("group-a", bytes(1, 32), bytes(11, 32));
  private static final AuthorizationRevocation.TargetIdentity GROUP_B =
      new AuthorizationRevocation.TargetIdentity("group-b", bytes(2, 32), bytes(12, 32));

  @Test
  void freezesTargetsAndCompletesOnlyAfterEveryBarrierIsAcknowledged() {
    var input = new java.util.ArrayList<>(List.of(GROUP_B, GROUP_A));
    var initial = AuthorizationRevocation.start(bytes(7, 16), bytes(8, 32), "provider", "realm", 2, 2, 3, input);
    assertTrue(initial.isInitial());
    input.clear();
    assertEquals(List.of(GROUP_A, GROUP_B), initial.targets().stream()
        .map(AuthorizationRevocation.Target::identity).toList());
    assertThrows(IllegalStateException.class, initial::finish);
    var allocated = initial.assignBarrier("group-a", 5);
    assertTrue(initial.allowsTransitionTo(allocated));
    var installed = allocated.acknowledge("group-a", 5);
    assertFalse(installed.isInitial());
    assertTrue(allocated.allowsTransitionTo(installed));
    assertThrows(IllegalStateException.class, installed::finish);
    var secondAllocated = installed.assignBarrier("group-b", 8);
    var secondInstalled = secondAllocated.acknowledge("group-b", 8);
    assertThrows(IllegalStateException.class, secondInstalled::finish);
    var firstReleased = secondInstalled.release("group-a");
    var allReleased = firstReleased.release("group-b");
    var completed = allReleased.finish();
    assertTrue(secondInstalled.allowsTransitionTo(firstReleased));
    assertTrue(allReleased.allowsTransitionTo(completed));
    assertEquals(AuthorizationRevocation.State.COMPLETED, completed.state());
    assertFalse(completed.allowsTransitionTo(initial));
  }

  @Test
  void retryIsIdempotentButCannotChangeTheSnapshotOrAcknowledgedBarrier() {
    var initial = AuthorizationRevocation.start(bytes(7, 16), bytes(8, 32), "provider", "realm", 2, 2, 3,
        List.of(GROUP_A, GROUP_B));
    var assigned = initial.assignBarrier("group-a", 5);
    assertEquals(assigned, assigned.assignBarrier("group-a", 5));
    assertThrows(IllegalStateException.class, () -> assigned.assignBarrier("group-a", 6));
    assertThrows(IllegalArgumentException.class, () -> assigned.acknowledge("group-a", 6));
    var acknowledged = assigned.acknowledge("group-a", 5);
    assertEquals(acknowledged, acknowledged.acknowledge("group-a", 5));
    assertThrows(IllegalArgumentException.class, () -> acknowledged.assignBarrier("unknown", 1));
    var changedConfiguration = AuthorizationRevocation.start(bytes(7, 16), bytes(8, 32), "provider", "realm", 2, 2, 4,
        List.of(GROUP_A, GROUP_B)).assignBarrier("group-a", 5);
    assertFalse(initial.allowsTransitionTo(changedConfiguration));
    var changedIncarnation = new AuthorizationRevocation.TargetIdentity("group-a", bytes(3, 32), bytes(11, 32));
    var replacedTarget = AuthorizationRevocation.start(bytes(7, 16), bytes(8, 32), "provider", "realm", 2, 2, 3,
        List.of(changedIncarnation, GROUP_B)).assignBarrier("group-a", 5);
    assertFalse(initial.allowsTransitionTo(replacedTarget));
  }

  @Test
  void rejectsEmptyOrDuplicateTargetSets() {
    assertThrows(IllegalArgumentException.class,
        () -> AuthorizationRevocation.start(bytes(7, 16), bytes(8, 32), "provider", "realm", 2, 2, 3, List.of()));
    assertThrows(IllegalArgumentException.class,
        () -> AuthorizationRevocation.start(bytes(7, 16), bytes(8, 32), "provider", "realm", 2, 2, 3, List.of(GROUP_A, GROUP_A)));
  }

  @Test
  void rejectsForgedTransitionsThatSkipOrRewriteProgress() {
    var initial = AuthorizationRevocation.start(bytes(7, 16), bytes(8, 32), "provider", "realm", 2, 2, 3,
        List.of(GROUP_A, GROUP_B));
    var first = new AuthorizationRevocation.Target(GROUP_A, 5, true, true);
    var second = new AuthorizationRevocation.Target(GROUP_B, 8, true, true);
    var skipped = new AuthorizationRevocation(initial.operationId(), initial.subjectFamilyDigest(),
        initial.providerId(), initial.backendIdentityDomain(), initial.credentialGeneration(), initial.operationSequence(),
        3, 2, AuthorizationRevocation.State.INSTALLING, List.of(first, second));
    assertFalse(initial.allowsTransitionTo(skipped));
    assertFalse(skipped.isInitial());
    var premature = new AuthorizationRevocation(initial.operationId(), initial.subjectFamilyDigest(),
        initial.providerId(), initial.backendIdentityDomain(), initial.credentialGeneration(), initial.operationSequence(),
        3, 1, AuthorizationRevocation.State.COMPLETED, List.of(first, second));
    assertFalse(premature.isInitial());
  }

  private static ByteKey bytes(int value, int length) {
    byte[] raw = new byte[length];
    java.util.Arrays.fill(raw, (byte) value);
    return ByteKey.copyOf(raw);
  }
}
