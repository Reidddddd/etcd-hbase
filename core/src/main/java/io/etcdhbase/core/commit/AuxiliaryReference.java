package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/** Authenticated pointer to a bounded, contiguous auxiliary directory. */
public record AuxiliaryReference(ByteKey identity, ByteKey digest, long itemCount,
    long totalBytes, int pageCount) {
  public static final long MAX_RECORDS = 8_192;
  public static final long MAX_TOTAL_BYTES = 64L * 1024 * 1024;
  public static final int MAX_PAGES = 128;

  public AuxiliaryReference {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(digest, "digest");
    if (identity.length() == 0 || digest.length() != 32 || itemCount <= 0
        || itemCount > MAX_RECORDS || totalBytes <= 0 || totalBytes > MAX_TOTAL_BYTES
        || pageCount <= 0 || pageCount > MAX_PAGES || pageCount > itemCount) {
      throw new IllegalArgumentException("invalid auxiliary reference");
    }
  }
}
