package io.etcdhbase.core.lease;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Canonical system-row identity for an immutable lease state version. */
public final class LeaseRecordIdentity {
  public static final int ROUTE_BYTES = Long.BYTES;
  public static final int IDENTITY_BYTES = Long.BYTES * 2;

  private LeaseRecordIdentity() {}

  /** Encodes signed wire IDs in sortable order, followed by the positive global sequence. */
  public static ByteKey stateVersion(long leaseId, long sequence) {
    if (sequence <= 0) throw new IllegalArgumentException("lease state sequence must be positive");
    return ByteKey.copyOf(ByteBuffer.allocate(IDENTITY_BYTES)
        .putLong(leaseId ^ Long.MIN_VALUE).putLong(sequence).array());
  }

  /** Stable enumeration row; the value is append-only and the identity is the wire ID. */
  public static ByteKey enumeration(long leaseId) {
    return ByteKey.copyOf(ByteBuffer.allocate(Long.BYTES)
        .putLong(leaseId ^ Long.MIN_VALUE).array());
  }

  public static long leaseId(ByteKey identity) {
    Objects.requireNonNull(identity, "identity");
    if (identity.length() < ROUTE_BYTES) {
      throw new IllegalArgumentException("lease identity is shorter than its route key");
    }
    return ByteBuffer.wrap(identity.bytes(), 0, ROUTE_BYTES).getLong() ^ Long.MIN_VALUE;
  }

  public static long sequence(ByteKey identity) {
    Objects.requireNonNull(identity, "identity");
    if (identity.length() != IDENTITY_BYTES) {
      throw new IllegalArgumentException("lease state identity must contain 16 bytes");
    }
    long sequence = ByteBuffer.wrap(identity.bytes(), ROUTE_BYTES, Long.BYTES).getLong();
    if (sequence <= 0) throw new IllegalArgumentException("lease state sequence must be positive");
    return sequence;
  }

  public static ByteKey routePrefix(long leaseId) {
    return ByteKey.copyOf(ByteBuffer.allocate(ROUTE_BYTES)
        .putLong(leaseId ^ Long.MIN_VALUE).array());
  }
}
