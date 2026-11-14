package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/** One immutable system-record row returned by a bounded range read. */
public record SystemRecordEntry(ByteKey identity, ByteKey value) {
  public SystemRecordEntry {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(value, "value");
    if (identity.length() == 0 || value.length() == 0) {
      throw new IllegalArgumentException("system record entry must not be empty");
    }
  }

  /** Returns the encoded row identity and cell payload bytes used by read-work accounting. */
  public long encodedBytes() {
    return Math.addExact(identity.length(), value.length());
  }
}
