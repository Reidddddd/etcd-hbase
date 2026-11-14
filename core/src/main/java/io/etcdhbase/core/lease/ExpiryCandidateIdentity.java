package io.etcdhbase.core.lease;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Sortable identity for one lease-generation expiration candidate. */
public record ExpiryCandidateIdentity(
    long deadlineEpochMillis, long leaseId, long incarnation, long renewalGeneration) {
  public static final int ENCODED_BYTES = Long.BYTES * 4;

  public ExpiryCandidateIdentity {
    if (deadlineEpochMillis < 0 || incarnation <= 0 || renewalGeneration < 0) {
      throw new IllegalArgumentException("invalid expiry candidate identity");
    }
  }

  public ByteKey encode() {
    return ByteKey.copyOf(ByteBuffer.allocate(ENCODED_BYTES)
        .putLong(deadlineEpochMillis ^ Long.MIN_VALUE)
        .putLong(leaseId ^ Long.MIN_VALUE)
        .putLong(incarnation)
        .putLong(renewalGeneration)
        .array());
  }

  public static ExpiryCandidateIdentity decode(ByteKey encoded) {
    Objects.requireNonNull(encoded, "encoded");
    if (encoded.length() != ENCODED_BYTES) {
      throw new IllegalArgumentException("expiry candidate identity must contain 32 bytes");
    }
    ByteBuffer bytes = ByteBuffer.wrap(encoded.bytes());
    return new ExpiryCandidateIdentity(bytes.getLong() ^ Long.MIN_VALUE,
        bytes.getLong() ^ Long.MIN_VALUE, bytes.getLong(), bytes.getLong());
  }

  /** Returns the sortable lease-ID bytes used to choose the salt bucket. */
  public static ByteKey leaseRoute(ByteKey encoded) {
    decode(encoded);
    byte[] bytes = encoded.bytes();
    return ByteKey.copyOf(java.util.Arrays.copyOfRange(bytes, Long.BYTES, Long.BYTES * 2));
  }
}
