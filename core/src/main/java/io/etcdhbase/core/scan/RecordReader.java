package io.etcdhbase.core.scan;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.storage.ReadProjection;
import java.util.concurrent.CompletableFuture;

/** Internal, request-bound read boundary; any future optimization must preserve this ownership. */
public interface RecordReader {
  CompletableFuture<PointResult> read(ByteKey key, long revisionUpperBound, ReadProjection projection);
}
