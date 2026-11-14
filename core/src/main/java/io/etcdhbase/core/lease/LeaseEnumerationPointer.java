package io.etcdhbase.core.lease;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Monotonic locator for the latest persisted state version of one lease ID. */
public record LeaseEnumerationPointer(long sequence) {
  private static final byte[] MAGIC = {'L', 'E', 'P', 1};
  private static final int ENCODED_BYTES = MAGIC.length + Long.BYTES;

  public LeaseEnumerationPointer {
    if (sequence <= 0) throw new IllegalArgumentException("lease pointer sequence must be positive");
  }

  public ByteKey encode() {
    return ByteKey.copyOf(ByteBuffer.allocate(ENCODED_BYTES)
        .put(MAGIC).putLong(sequence).array());
  }

  public static LeaseEnumerationPointer decode(ByteKey encoded) {
    Objects.requireNonNull(encoded, "encoded");
    byte[] bytes = encoded.bytes();
    if (bytes.length != ENCODED_BYTES
        || !java.util.Arrays.equals(bytes, 0, MAGIC.length, MAGIC, 0, MAGIC.length)) {
      throw new IllegalArgumentException("invalid lease enumeration pointer");
    }
    return new LeaseEnumerationPointer(ByteBuffer.wrap(bytes, MAGIC.length, Long.BYTES).getLong());
  }
}
