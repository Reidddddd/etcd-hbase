package io.etcdhbase.core.lease;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Payload for an expiry index row, identifying the Lease version that created the candidate. */
public record ExpiryCandidateRecord(long stateSequence) {
  private static final byte[] MAGIC = {'E', 'C', 'P', 1};
  private static final int ENCODED_BYTES = MAGIC.length + Long.BYTES;

  public ExpiryCandidateRecord {
    if (stateSequence <= 0) throw new IllegalArgumentException("expiry state sequence must be positive");
  }

  public ByteKey encode() {
    return ByteKey.copyOf(ByteBuffer.allocate(ENCODED_BYTES)
        .put(MAGIC).putLong(stateSequence).array());
  }

  public static ExpiryCandidateRecord decode(ByteKey encoded) {
    Objects.requireNonNull(encoded, "encoded");
    byte[] bytes = encoded.bytes();
    if (bytes.length != ENCODED_BYTES
        || !java.util.Arrays.equals(bytes, 0, MAGIC.length, MAGIC, 0, MAGIC.length)) {
      throw new IllegalArgumentException("invalid expiry candidate payload");
    }
    return new ExpiryCandidateRecord(ByteBuffer.wrap(bytes, MAGIC.length, Long.BYTES).getLong());
  }
}
