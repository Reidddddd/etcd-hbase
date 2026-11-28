package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;

/** Same-row, immutable-until-extended retention witness. Missing evidence never means expired. */
record InvocationWindowEvidence(long minimumMillis, long notBeforeEpochMillis) {
  static final byte[] COLUMN = {'w'};
  static final long TIMESTAMP = Long.MAX_VALUE - 3;
  private static final int VERSION = 1;
  private static final int BYTES = Integer.BYTES + 2 * Long.BYTES;

  InvocationWindowEvidence {
    if (minimumMillis <= 0 || minimumMillis > java.time.Duration.ofDays(30).toMillis()
        || notBeforeEpochMillis < minimumMillis) {
      throw new IllegalArgumentException("invalid invocation window evidence");
    }
  }

  ByteKey encode() {
    return ByteKey.copyOf(ByteBuffer.allocate(BYTES).putInt(VERSION)
        .putLong(minimumMillis).putLong(notBeforeEpochMillis).array());
  }

  static InvocationWindowEvidence decode(ByteKey encoded) {
    if (encoded.length() != BYTES) throw new IllegalArgumentException("invalid invocation window evidence size");
    var input = ByteBuffer.wrap(encoded.bytes());
    if (input.getInt() != VERSION) throw new IllegalArgumentException("unsupported invocation window evidence");
    return new InvocationWindowEvidence(input.getLong(), input.getLong());
  }
}
