package io.etcdhbase.core.lease;

import java.util.Objects;

/** Checked TTL arithmetic shared by Grant, reads, renewal, and expiry decisions. */
public final class LeaseTimeRules {
  private static final long MILLIS_PER_SECOND = 1_000;

  /** Maximum accepted by etcd v3.5.23 lessor. */
  public static final long MAX_TTL_SECONDS = 9_000_000_000L;

  private LeaseTimeRules() {}

  /** Applies the configured etcd-compatible minimum after rejecting only an oversized TTL. */
  public static long grantedTtlSeconds(long requested, long minimum, long maximum) {
    if (minimum <= 0 || maximum < minimum || maximum > MAX_TTL_SECONDS) {
      throw new IllegalArgumentException("invalid lease TTL limits");
    }
    if (requested > maximum) {
      throw new LeaseFailure(LeaseFailure.Reason.TTL_TOO_LARGE);
    }
    return Math.max(requested, minimum);
  }

  /** Calculates a persisted deadline from the upper time bound to avoid premature expiration. */
  public static long deadlineEpochMillis(LeaseTimeObservation time, long ttlSeconds) {
    requireHealthy(time);
    if (ttlSeconds <= 0) throw new IllegalArgumentException("lease TTL must be positive");
    try {
      return Math.addExact(time.nowUpperEpochMillis(),
          Math.multiplyExact(ttlSeconds, MILLIS_PER_SECOND));
    } catch (ArithmeticException overflow) {
      throw new LeaseFailure(LeaseFailure.Reason.TTL_TOO_LARGE, overflow);
    }
  }

  /** Returns true only after the lower time bound reaches the persisted deadline. */
  public static boolean isExpired(LeaseState state, LeaseTimeObservation time) {
    Objects.requireNonNull(state, "state");
    requireHealthy(time);
    return state.status() == LeaseState.Status.ACTIVE
        && time.nowLowerEpochMillis() >= state.deadlineEpochMillis();
  }

  /** Returns a conservative, non-negative whole-second estimate without extending the lease. */
  public static long remainingTtlSeconds(LeaseState state, LeaseTimeObservation time) {
    Objects.requireNonNull(state, "state");
    requireHealthy(time);
    if (state.status() != LeaseState.Status.ACTIVE) return -1;
    long remainingMillis = state.deadlineEpochMillis() - time.nowUpperEpochMillis();
    return remainingMillis <= 0 ? 0
        : Math.min(state.grantedTtlSeconds(), remainingMillis / MILLIS_PER_SECOND);
  }

  private static void requireHealthy(LeaseTimeObservation time) {
    if (!Objects.requireNonNull(time, "time").healthy()) {
      throw new LeaseFailure(LeaseFailure.Reason.CLOCK_UNHEALTHY);
    }
  }
}
