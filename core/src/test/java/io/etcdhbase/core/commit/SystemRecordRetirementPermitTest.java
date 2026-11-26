package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * §6.2 of the M5 design makes a collection permit conservative: it binds one table group, one
 * registry, one Compact operation and the caller's scope, and it is never reusable elsewhere.
 */
class SystemRecordRetirementPermitTest {
  private static final ByteKey PROFILE = ByteKey.copyOf(new byte[32]);
  private static final ByteKey REGISTRY = ByteKey.copyOf(new byte[16]);

  @Test
  void permitBindsProfileRegistryOperationGenerationAndBoundary() {
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var operation = InvocationId.create();
      var permit = SystemRecordRetirement.Permit.issue(PROFILE, REGISTRY, operation, 3, 7, 5, scope);
      assertEquals(PROFILE, permit.profile());
      assertEquals(REGISTRY, permit.registryIdentity());
      assertEquals(operation, permit.operationId());
      assertEquals(3, permit.generation());
      assertEquals(7, permit.safeRevision());
      assertEquals(5, permit.safeSequence());
      permit.requireUsable(PROFILE, scope);
    }
  }

  @Test
  void permitFailsClosedForAnotherProfileScopeOrAfterItsScopeCloses() {
    RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
    var permit = SystemRecordRetirement.Permit.issue(
        PROFILE, REGISTRY, InvocationId.create(), 1, 4, 4, scope);
    try (var other = RequestScope.withTimeout(Duration.ofSeconds(1))) {
      assertThrows(IllegalStateException.class, () -> permit.requireUsable(PROFILE, other),
          "a permit cannot be replayed under another scope");
    }
    var otherProfile = new byte[32];
    otherProfile[0] = 1;
    assertThrows(IllegalStateException.class,
        () -> permit.requireUsable(ByteKey.copyOf(otherProfile), scope),
        "a permit is bound to its exact profile");
    scope.close();
    assertThrows(IllegalStateException.class, () -> permit.requireUsable(PROFILE, scope),
        "a permit cannot outlive its collection scope");
  }

  @Test
  void permitRejectsAnUnaccountedGenerationOrNegativeBoundary() {
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      assertThrows(IllegalArgumentException.class, () -> SystemRecordRetirement.Permit.issue(
          PROFILE, REGISTRY, InvocationId.create(), 0, 4, 4, scope));
      assertThrows(IllegalArgumentException.class, () -> SystemRecordRetirement.Permit.issue(
          PROFILE, REGISTRY, InvocationId.create(), 1, -1, 4, scope));
      assertThrows(IllegalArgumentException.class, () -> SystemRecordRetirement.Permit.issue(
          PROFILE, REGISTRY, InvocationId.create(), 1, 4, -1, scope));
    }
  }

  @Test
  void tombstoneRequiresAnExactDigestAndAccountedBoundary() {
    var digest = ByteKey.copyOf(new byte[32]);
    var identity = ByteKey.copyOf(new byte[] {1, 2});
    var tombstone = new RetirementTombstone(SystemRecordType.DECISION, identity, digest, 2,
        InvocationId.create(), SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, 9, 4);
    assertEquals(SystemRecordType.DECISION, tombstone.type());
    assertEquals(2, tombstone.maintenanceGeneration());
    assertEquals(digest, tombstone.retiredValueDigest());
    assertEquals(9, tombstone.retiredRevision());
    assertEquals(4, tombstone.retiredSequence());

    assertThrows(IllegalArgumentException.class, () -> new RetirementTombstone(
        SystemRecordType.DECISION, identity, ByteKey.copyOf(new byte[31]), 2, InvocationId.create(),
        SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, 9, 4));
    assertThrows(IllegalArgumentException.class, () -> new RetirementTombstone(
        SystemRecordType.DECISION, ByteKey.copyOf(new byte[0]), digest, 2, InvocationId.create(),
        SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, 9, 4));
    assertThrows(IllegalArgumentException.class, () -> new RetirementTombstone(
        SystemRecordType.DECISION, identity, digest, 0, InvocationId.create(),
        SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, 9, 4));
    assertEquals(SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, tombstone.reason());
  }
}
