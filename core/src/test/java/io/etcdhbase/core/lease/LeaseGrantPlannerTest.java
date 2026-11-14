package io.etcdhbase.core.lease;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class LeaseGrantPlannerTest {
  private static final LeaseTimeObservation TIME = new LeaseTimeObservation(9_900, 10_000, true);

  @Test
  void grantsSignedIdWithMinimumTtlAndConservativeDeadline() {
    LeaseState state = LeaseGrantPlanner.grant(Long.MIN_VALUE, 0, Optional.empty(), 4, TIME, 5, 60);
    assertEquals(new LeaseIdentity(Long.MIN_VALUE, 1), state.identity());
    assertEquals(5, state.grantedTtlSeconds());
    assertEquals(15_000, state.deadlineEpochMillis());
    assertEquals(4, state.sequence());
  }

  @Test
  void requiresRevocationBeforeReusingIdAndAdvancesIncarnation() {
    var old = LeaseState.grant(new LeaseIdentity(-7, 3), 10, 11_000, 4);
    assertEquals(LeaseFailure.Reason.ALREADY_EXISTS,
        assertThrows(LeaseFailure.class,
            () -> LeaseGrantPlanner.grant(-7, 10, Optional.of(old), 5, TIME, 5, 60)).reason());
    var replacement = LeaseGrantPlanner.grant(-7, 10, Optional.of(old.revoke(5)), 6, TIME, 5, 60);
    assertEquals(new LeaseIdentity(-7, 4), replacement.identity());
    assertThrows(IllegalArgumentException.class,
        () -> LeaseGrantPlanner.grant(8, 10, Optional.of(old.revoke(5)), 6, TIME, 5, 60));
    assertThrows(IllegalArgumentException.class,
        () -> LeaseGrantPlanner.grant(-7, 10, Optional.of(old.revoke(5)), 5, TIME, 5, 60));
  }

  @Test
  void rejectsInvalidIdentityOverflowAndUnhealthyTime() {
    assertThrows(IllegalArgumentException.class,
        () -> LeaseGrantPlanner.grant(0, 10, Optional.empty(), 1, TIME, 5, 60));
    var retired = LeaseState.grant(new LeaseIdentity(8, Long.MAX_VALUE), 10, 20_000, 1).revoke(2);
    assertEquals(LeaseFailure.Reason.ID_SPACE_EXHAUSTED,
        assertThrows(LeaseFailure.class,
            () -> LeaseGrantPlanner.grant(8, 10, Optional.of(retired), 3, TIME, 5, 60)).reason());
    assertEquals(LeaseFailure.Reason.CLOCK_UNHEALTHY,
        assertThrows(LeaseFailure.class, () -> LeaseGrantPlanner.grant(8, 10, Optional.empty(), 1,
            new LeaseTimeObservation(9_900, 10_000, false), 5, 60)).reason());
  }
}
