package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Conservative independent revision and decision-sequence limits for a future GC worker. */
public record SnapshotGcBoundary(long safeRevision, long safeSequence,
    List<Protection> protections) {
  public SnapshotGcBoundary {
    protections = List.copyOf(Objects.requireNonNull(protections, "protections"));
    if (safeRevision < 0 || safeSequence < 0) {
      throw new IllegalArgumentException("negative snapshot GC boundary");
    }
  }

  /** Exact active references are retained in addition to the two independent watermarks. */
  public record Protection(int shard, int slot, long generation, ByteKey pinId,
      long requiredRevision, long requiredSequence, Optional<TransactionId> transactionId,
      Optional<ByteKey> rootIdentity, Optional<ByteKey> rootDigest) {
    public Protection {
      Objects.requireNonNull(pinId, "pinId");
      Objects.requireNonNull(transactionId, "transactionId");
      Objects.requireNonNull(rootIdentity, "rootIdentity");
      Objects.requireNonNull(rootDigest, "rootDigest");
      if (shard < 0 || slot < 0 || generation <= 0 || pinId.length() != 16
          || requiredRevision <= 0 || requiredSequence < 0
          || transactionId.isPresent() && requiredSequence == 0
          || transactionId.isPresent() != rootIdentity.isPresent()
          || rootIdentity.isPresent() != rootDigest.isPresent()
          || rootDigest.filter(digest -> digest.length() != 32).isPresent()) {
        throw new IllegalArgumentException("invalid snapshot pin protection");
      }
    }
  }
}
