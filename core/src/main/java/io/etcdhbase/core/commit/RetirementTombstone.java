package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/**
 * Durable proof that one system-record identity was retired, and by which maintenance generation.
 *
 * <p>The tombstone is intentionally smaller than the payload it replaces but it is never absent:
 * an absent tombstone is what would let a late writer recreate a record GC already retired.
 */
public record RetirementTombstone(
    SystemRecordType type,
    ByteKey identity,
    ByteKey retiredValueDigest,
    long maintenanceGeneration,
    InvocationId operationId,
    SystemRecordRetirement.Reason reason,
    long retiredRevision,
    long retiredSequence) {
  public RetirementTombstone {
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(retiredValueDigest, "retiredValueDigest");
    Objects.requireNonNull(operationId, "operationId");
    Objects.requireNonNull(reason, "reason");
    if (identity.length() == 0 || retiredValueDigest.length() != 32
        || maintenanceGeneration <= 0 || retiredRevision < 0 || retiredSequence < 0) {
      throw new IllegalArgumentException("invalid retirement tombstone");
    }
  }
}
