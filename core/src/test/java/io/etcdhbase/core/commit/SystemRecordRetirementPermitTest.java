package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.lang.reflect.Modifier;
import java.util.Arrays;

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
  void derivedPermitOnlyNarrowsBoundsAndKeepsAuthorityProtectionsAndScope() {
    var scope = RequestScope.withTimeout(Duration.ofSeconds(5));
    var seed = permit(PROFILE, REGISTRY, InvocationId.create(), 3, 7, 5, scope);
    var protection = new SnapshotGcBoundary.Protection(0, 0, 1, REGISTRY, 1, 1,
        java.util.Optional.of(TransactionId.create()), java.util.Optional.of(ByteKey.copyOf(new byte[] {1})),
        java.util.Optional.of(PROFILE));
    var original = new SystemRecordRetirement.Permit(PROFILE, REGISTRY, seed.operationId(), 3, 7, 5,
        seed.authority(), java.util.List.of(protection), scope);
    var narrowed = original.restrictTo(4, 3);
    assertEquals(4, narrowed.safeRevision());
    assertEquals(3, narrowed.safeSequence());
    assertEquals(original.operationId(), narrowed.operationId());
    assertEquals(original.generation(), narrowed.generation());
    assertEquals(original.profile(), narrowed.profile());
    assertEquals(original.registryIdentity(), narrowed.registryIdentity());
    assertEquals(original.authority(), narrowed.authority());
    assertEquals(original.protections(), narrowed.protections());
    narrowed.requireUsable(PROFILE, scope);
    assertThrows(IllegalArgumentException.class, () -> original.restrictTo(8, 5));
    assertThrows(IllegalArgumentException.class, () -> original.restrictTo(7, 6));
    assertThrows(IllegalArgumentException.class, () -> original.restrictTo(-1, 0));
    scope.close();
    assertThrows(IllegalStateException.class, () -> narrowed.requireUsable(PROFILE, scope));
    assertThrows(IllegalStateException.class, () -> original.restrictTo(4, 3));
  }

  @Test
  void ordinaryCallersCannotMintRetirementPermits() {
    assertEquals(0, SystemRecordRetirement.Permit.class.getConstructors().length);
    assertTrue(Arrays.stream(SystemRecordRetirement.Permit.class.getMethods()).noneMatch(method ->
        Modifier.isStatic(method.getModifiers()) && method.getReturnType() == SystemRecordRetirement.Permit.class),
        "GC authority must be the only permit issuer");
  }

  @Test
  void permitBindsProfileRegistryOperationGenerationAndBoundary() {
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var operation = InvocationId.create();
      var permit = permit(PROFILE, REGISTRY, operation, 3, 7, 5, scope);
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
    var permit = permit(
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
      assertThrows(IllegalArgumentException.class, () -> permit(
          PROFILE, REGISTRY, InvocationId.create(), 0, 4, 4, scope));
      assertThrows(IllegalArgumentException.class, () -> permit(
          PROFILE, REGISTRY, InvocationId.create(), 1, -1, 4, scope));
      assertThrows(IllegalArgumentException.class, () -> permit(
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
  private static SystemRecordRetirement.Permit permit(ByteKey profile, ByteKey registry,
      InvocationId operation, long generation, long revision, long sequence, RequestScope scope) {
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1));
    var authority = new SystemRecordRetirement.Authority(new CommitOwnerService.OwnerToken("owner", 1), 0,
        configuration, configuration, REGISTRY, REGISTRY);
    return new SystemRecordRetirement.Permit(profile, registry, operation, generation, revision, sequence,
        authority, java.util.List.of(), scope);
  }

}
