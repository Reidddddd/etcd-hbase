package io.etcdhbase.core.lease;

import java.util.Objects;
import java.util.Optional;

/** Computes a new lease lifetime from an authoritative version at one fixed commit barrier. */
public final class LeaseGrantPlanner {
  private LeaseGrantPlanner() {}

  /** An expired ACTIVE version must first be revoked by the owner; it is not silently reused. */
  public static LeaseState grant(long leaseId, long requestedTtlSeconds,
      Optional<LeaseState> previous, long sequence, LeaseTimeObservation time,
      long minimumTtlSeconds, long maximumTtlSeconds) {
    Objects.requireNonNull(previous, "previous");
    Objects.requireNonNull(time, "time");
    if (leaseId == 0 || sequence <= 0) {
      throw new IllegalArgumentException("grant needs a nonzero allocated ID and positive sequence");
    }
    long incarnation = 1;
    if (previous.isPresent()) {
      LeaseState old = previous.orElseThrow();
      if (old.identity().leaseId() != leaseId || sequence <= old.sequence()) {
        throw new IllegalArgumentException("previous lease differs from the grant base");
      }
      if (old.status() == LeaseState.Status.ACTIVE) {
        throw new LeaseFailure(LeaseFailure.Reason.ALREADY_EXISTS);
      }
      try {
        incarnation = Math.addExact(old.identity().incarnation(), 1);
      } catch (ArithmeticException overflow) {
        throw new LeaseFailure(LeaseFailure.Reason.ID_SPACE_EXHAUSTED, overflow);
      }
    }
    long grantedTtl = LeaseTimeRules.grantedTtlSeconds(
        requestedTtlSeconds, minimumTtlSeconds, maximumTtlSeconds);
    long deadline = LeaseTimeRules.deadlineEpochMillis(time, grantedTtl);
    return LeaseState.grant(new LeaseIdentity(leaseId, incarnation), grantedTtl, deadline, sequence);
  }
}
