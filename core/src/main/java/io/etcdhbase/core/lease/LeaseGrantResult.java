package io.etcdhbase.core.lease;

/** The original durable response values for one selected Grant invocation. */
public record LeaseGrantResult(long leaseId, long grantedTtlSeconds, long revision) {
  public LeaseGrantResult {
    if (leaseId == 0 || grantedTtlSeconds <= 0 || revision <= 0) {
      throw new IllegalArgumentException("invalid durable lease grant result");
    }
  }
}
