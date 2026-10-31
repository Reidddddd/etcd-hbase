package io.etcdhbase.core.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import java.util.Objects;
import java.util.Optional;

/** Frozen-dataset page request. Resume excludes the last completely consumed logical key. */
public record ScanRequest(int bucket, KeyRange range, Optional<ByteKey> after,
    long revisionUpperBound, ReadProjection projection, int maximumRows, long maximumBytes,
    int maximumPhysicalRows) {
  public ScanRequest {
    Objects.requireNonNull(range, "range");
    Objects.requireNonNull(after, "after");
    Objects.requireNonNull(projection, "projection");
    if (bucket < 0 || revisionUpperBound <= 0 || maximumRows <= 0 || maximumRows > 1024
        || maximumBytes <= 0 || maximumBytes > 64L * 1024 * 1024
        || maximumPhysicalRows <= 0 || maximumPhysicalRows > 1_000_000) {
      throw new IllegalArgumentException("invalid bounded scan request");
    }
  }
}
