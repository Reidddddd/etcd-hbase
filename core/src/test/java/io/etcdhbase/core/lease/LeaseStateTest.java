package io.etcdhbase.core.lease;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class LeaseStateTest {
  @Test
  void preservesSignedWireIdAndSeparatesIncarnations() {
    var first = new LeaseIdentity(Long.MIN_VALUE, 1);
    var second = new LeaseIdentity(Long.MIN_VALUE, 2);

    assertEquals(Long.MIN_VALUE, first.leaseId());
    assertFalse(first.equals(second));
    assertThrows(IllegalArgumentException.class, () -> new LeaseIdentity(0, 1));
  }

  @Test
  void grantsMinimumTtlAndRejectsOnlyValuesAboveMaximum() {
    assertEquals(5, LeaseTimeRules.grantedTtlSeconds(0, 5, 60));
    assertEquals(5, LeaseTimeRules.grantedTtlSeconds(-3, 5, 60));
    assertEquals(60, LeaseTimeRules.grantedTtlSeconds(60, 5, 60));
    assertEquals(LeaseFailure.Reason.TTL_TOO_LARGE,
        assertThrows(LeaseFailure.class, () -> LeaseTimeRules.grantedTtlSeconds(61, 5, 60))
            .reason());
  }

  @Test
  void deadlineUsesUpperBoundAndExpiryUsesLowerBound() {
    var time = new LeaseTimeObservation(10_000, 10_250, true);
    long deadline = LeaseTimeRules.deadlineEpochMillis(time, 5);
    var state = LeaseState.grant(new LeaseIdentity(-7, 1), 5, deadline, 4);

    assertEquals(15_250, deadline);
    assertFalse(LeaseTimeRules.isExpired(state, new LeaseTimeObservation(15_249, 15_500, true)));
    assertTrue(LeaseTimeRules.isExpired(state, new LeaseTimeObservation(15_250, 15_500, true)));
    assertEquals(0, LeaseTimeRules.remainingTtlSeconds(
        state, new LeaseTimeObservation(14_000, 15_250, true)));
    assertEquals(5, LeaseTimeRules.remainingTtlSeconds(
        state, new LeaseTimeObservation(1_000, 1_250, true)));
  }

  @Test
  void overflowAndUnhealthyClockNeverCreateADeadline() {
    var overflowTime = new LeaseTimeObservation(Long.MAX_VALUE - 10, Long.MAX_VALUE - 10, true);
    assertEquals(LeaseFailure.Reason.TTL_TOO_LARGE,
        assertThrows(LeaseFailure.class, () -> LeaseTimeRules.deadlineEpochMillis(overflowTime, 1))
            .reason());
    assertEquals(LeaseFailure.Reason.CLOCK_UNHEALTHY,
        assertThrows(LeaseFailure.class, () -> LeaseTimeRules.deadlineEpochMillis(
            new LeaseTimeObservation(1, 1, false), 1)).reason());
  }

  @Test
  void renewalAdvancesSequenceAndGenerationWithoutChangingGrantedTtl() {
    var lease = LeaseState.grant(new LeaseIdentity(7, 1), 30, 40_000, 2);
    var renewed = lease.renew(70_000, 3);

    assertEquals(30, renewed.grantedTtlSeconds());
    assertEquals(1, renewed.renewalGeneration());
    assertEquals(3, renewed.sequence());
    assertThrows(IllegalArgumentException.class, () -> renewed.renew(80_000, 3));
    assertThrows(IllegalArgumentException.class, () -> renewed.revoke(2));
    assertThrows(IllegalStateException.class, () -> renewed.revoke(4).renew(80_000, 5));
    assertThrows(ArithmeticException.class, () -> new LeaseState(
        renewed.identity(), renewed.grantedTtlSeconds(), renewed.deadlineEpochMillis(),
        Long.MAX_VALUE, renewed.status(), renewed.sequence(), 0, 0, 0).renew(80_000, 4));
  }

  @Test
  void refusesToRevokeWithoutAnAtomicDeletionPlanForExistingBindings() {
    var lease = new LeaseState(new LeaseIdentity(7, 1), 30, 40_000, 0,
        LeaseState.Status.ACTIVE, 2, 1, 10, 20);
    assertThrows(IllegalStateException.class, () -> lease.revoke(3));
    assertThrows(IllegalStateException.class, () -> new LeaseState(lease.identity(), 30, 40_000,
        0, LeaseState.Status.ACTIVE, 2, 0, 10, 0).revoke(3));
    assertThrows(IllegalStateException.class, () -> new LeaseState(lease.identity(), 30, 40_000,
        0, LeaseState.Status.ACTIVE, 2, 0, 0, 20).revoke(3));
    assertThrows(IllegalArgumentException.class, () -> new LeaseState(lease.identity(), 30, 40_000,
        0, LeaseState.Status.REVOKED, 3, 1, 10, 20));
  }
}
