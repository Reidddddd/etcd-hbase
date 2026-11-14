package io.etcdhbase.core.lease;

/** Identifies one lifetime of an etcd lease ID. */
public record LeaseIdentity(long leaseId, long incarnation) {
  public LeaseIdentity {
    if (leaseId == 0 || incarnation <= 0) {
      throw new IllegalArgumentException("lease ID must be nonzero and incarnation must be positive");
    }
  }
}
