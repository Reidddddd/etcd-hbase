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
    long expiryCandidateSequence,
    long boundKeyCount,
    long boundKeyBytes,
    long revokeCostBytes,
    long bindingHistoryBytes) {

  public enum Status { ACTIVE, REVOKED }

  public LeaseState {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(status, "status");
    if (grantedTtlSeconds <= 0 || deadlineEpochMillis <= 0 || renewalGeneration < 0
        || sequence <= 0 || expiryCandidateSequence <= 0 || expiryCandidateSequence > sequence
        || boundKeyCount < 0 || boundKeyBytes < 0 || revokeCostBytes < 0 || bindingHistoryBytes < 0
        || status == Status.REVOKED && (boundKeyCount != 0 || boundKeyBytes != 0 || revokeCostBytes != 0)) {
      throw new IllegalArgumentException("invalid lease state");
    }
  }

  /** Reads the J40 state shape before J41 began tracking binding-history capacity. */
  public LeaseState(LeaseIdentity identity, long grantedTtlSeconds, long deadlineEpochMillis,
      long renewalGeneration, Status status, long sequence, long expiryCandidateSequence,
      long boundKeyCount, long boundKeyBytes, long revokeCostBytes) {
    this(identity, grantedTtlSeconds, deadlineEpochMillis, renewalGeneration, status, sequence,
        expiryCandidateSequence, boundKeyCount, boundKeyBytes, revokeCostBytes, 0);
  }

  /** Reads the original J40 shape; its expiry candidate was created with the state version. */
  public LeaseState(LeaseIdentity identity, long grantedTtlSeconds, long deadlineEpochMillis,
      long renewalGeneration, Status status, long sequence,
      long boundKeyCount, long boundKeyBytes, long revokeCostBytes) {
    this(identity, grantedTtlSeconds, deadlineEpochMillis, renewalGeneration, status,
        sequence, sequence, boundKeyCount, boundKeyBytes, revokeCostBytes, 0);
  }

  public static LeaseState grant(
      LeaseIdentity identity, long grantedTtlSeconds, long deadlineEpochMillis, long sequence) {
    return new LeaseState(identity, grantedTtlSeconds, deadlineEpochMillis, 0,
        Status.ACTIVE, sequence, sequence, 0, 0, 0, 0);
  }

  public LeaseState renew(long deadlineEpochMillis, long sequence) {
    requireActiveAndLaterSequence(sequence);
    return new LeaseState(identity, grantedTtlSeconds, deadlineEpochMillis,
        Math.addExact(renewalGeneration, 1), status, sequence, sequence,
        boundKeyCount, boundKeyBytes, revokeCostBytes, bindingHistoryBytes);
  }

  public LeaseState revoke(long sequence) {
    return revoke(sequence, bindingHistoryBytes);
  }

  /** Retains the bounded immutable-history cost after the active binding set is revoked. */
  public LeaseState revoke(long sequence, long retainedBindingHistoryBytes) {
    requireActiveAndLaterSequence(sequence);
    if (boundKeyCount != 0 || boundKeyBytes != 0 || revokeCostBytes != 0
        || retainedBindingHistoryBytes < bindingHistoryBytes) {
      throw new IllegalStateException("nonempty lease requires an atomic binding deletion plan");
    }
    return new LeaseState(identity, grantedTtlSeconds, deadlineEpochMillis,
        renewalGeneration, Status.REVOKED, sequence, expiryCandidateSequence, 0, 0, 0,
        retainedBindingHistoryBytes);
  }

  /** Advances binding accounting without changing the expiry identity or its immutable row. */
  public LeaseState withBindingAccounting(long nextSequence, long keyCount,
      long keyBytes, long nextRevokeCostBytes) {
    return withBindingAccounting(nextSequence, keyCount, keyBytes,
        nextRevokeCostBytes, bindingHistoryBytes);
  }

  /** Advances live Lease costs and the bounded immutable binding-history cost together. */
  public LeaseState withBindingAccounting(long nextSequence, long keyCount,
      long keyBytes, long nextRevokeCostBytes, long nextBindingHistoryBytes) {
    requireActiveAndLaterSequence(nextSequence);
    return new LeaseState(identity, grantedTtlSeconds, deadlineEpochMillis,
        renewalGeneration, status, nextSequence, expiryCandidateSequence,
        keyCount, keyBytes, nextRevokeCostBytes, nextBindingHistoryBytes);
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
