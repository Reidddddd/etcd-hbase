package io.etcdhbase.core.lease;

import java.util.Objects;

/** Immutable authoritative lease version selected by one M2 commit sequence. */
public record LeaseState(
    LeaseIdentity identity,
    long grantedTtlSeconds,
    long deadlineEpochMillis,
    long renewalGeneration,
    Status status,
    long sequence,
    long boundKeyCount,
    long boundKeyBytes,
    long revokeCostBytes) {

  public enum Status { ACTIVE, REVOKED }

  public LeaseState {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(status, "status");
    if (grantedTtlSeconds <= 0 || deadlineEpochMillis <= 0 || renewalGeneration < 0
        || sequence <= 0 || boundKeyCount < 0 || boundKeyBytes < 0 || revokeCostBytes < 0
        || status == Status.REVOKED && (boundKeyCount != 0 || boundKeyBytes != 0 || revokeCostBytes != 0)) {
      throw new IllegalArgumentException("invalid lease state");
    }
  }

  public static LeaseState grant(
      LeaseIdentity identity, long grantedTtlSeconds, long deadlineEpochMillis, long sequence) {
    return new LeaseState(identity, grantedTtlSeconds, deadlineEpochMillis, 0,
        Status.ACTIVE, sequence, 0, 0, 0);
  }

  public LeaseState renew(long deadlineEpochMillis, long sequence) {
    requireActiveAndLaterSequence(sequence);
    return new LeaseState(identity, grantedTtlSeconds, deadlineEpochMillis,
        Math.addExact(renewalGeneration, 1), status, sequence,
        boundKeyCount, boundKeyBytes, revokeCostBytes);
  }

  public LeaseState revoke(long sequence) {
    requireActiveAndLaterSequence(sequence);
    if (boundKeyCount != 0 || boundKeyBytes != 0 || revokeCostBytes != 0) {
      throw new IllegalStateException("nonempty lease requires an atomic binding deletion plan");
    }
    return new LeaseState(identity, grantedTtlSeconds, deadlineEpochMillis,
        renewalGeneration, Status.REVOKED, sequence, 0, 0, 0);
  }

  private void requireActiveAndLaterSequence(long nextSequence) {
    if (status != Status.ACTIVE) {
      throw new IllegalStateException("lease is not active");
    }
    if (nextSequence <= sequence) {
      throw new IllegalArgumentException("lease sequence must advance");
    }
  }
}
