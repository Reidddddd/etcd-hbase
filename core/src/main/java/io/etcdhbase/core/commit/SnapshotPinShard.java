package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Immutable bounded CAS value for one pin shard. */
record SnapshotPinShard(int shard, long generation, long revisionFloor, long sequenceFloor,
    List<Slot> slots) {
  SnapshotPinShard {
    slots = List.copyOf(Objects.requireNonNull(slots, "slots"));
    if (shard < 0 || generation < 0 || revisionFloor < 0 || sequenceFloor < 0 || slots.isEmpty()) {
      throw new IllegalArgumentException("invalid snapshot pin shard");
    }
  }

  SnapshotPinShard update(long nextRevisionFloor, long nextSequenceFloor, List<Slot> nextSlots) {
    return new SnapshotPinShard(shard, Math.addExact(generation, 1), nextRevisionFloor,
        nextSequenceFloor, nextSlots);
  }

  enum Status { EMPTY, ACTIVE, COMPLETED, REVOKED }

  record Slot(Status status, long generation, ByteKey pinId, long requiredRevision,
      long requiredSequence, ByteKey viewDigest, long expiresAtEpochMillis,
      Optional<TransactionId> transactionId, Optional<ByteKey> rootIdentity,
      Optional<ByteKey> rootDigest) {
    Slot {
      Objects.requireNonNull(status, "status");
      Objects.requireNonNull(pinId, "pinId");
      Objects.requireNonNull(viewDigest, "viewDigest");
      Objects.requireNonNull(transactionId, "transactionId");
      Objects.requireNonNull(rootIdentity, "rootIdentity");
      Objects.requireNonNull(rootDigest, "rootDigest");
      if (generation < 0 || requiredRevision < 0 || requiredSequence < 0
          || expiresAtEpochMillis < 0) {
        throw new IllegalArgumentException("negative snapshot pin slot field");
      }
      if (status == Status.EMPTY) {
        if (generation != 0 || pinId.length() != 0 || requiredRevision != 0
            || requiredSequence != 0 || viewDigest.length() != 0 || expiresAtEpochMillis != 0
            || transactionId.isPresent() || rootIdentity.isPresent() || rootDigest.isPresent()) {
          throw new IllegalArgumentException("empty snapshot pin slot contains state");
        }
      } else if (status == Status.ACTIVE) {
        if (generation == 0 || pinId.length() != 16 || requiredRevision <= 0
            || viewDigest.length() != 32 || expiresAtEpochMillis <= 0
            || transactionId.isPresent() && requiredSequence == 0
            || transactionId.isPresent() != rootIdentity.isPresent()
            || rootIdentity.isPresent() != rootDigest.isPresent()
            || rootDigest.filter(digest -> digest.length() != 32).isPresent()) {
          throw new IllegalArgumentException("invalid active snapshot pin slot");
        }
      } else if (generation == 0 || pinId.length() != 16 || requiredRevision != 0
          || requiredSequence != 0 || viewDigest.length() != 0 || expiresAtEpochMillis != 0
          || transactionId.isPresent() || rootIdentity.isPresent() || rootDigest.isPresent()) {
        throw new IllegalArgumentException("terminal snapshot pin slot contains active state");
      }
    }

    static Slot empty() {
      return new Slot(Status.EMPTY, 0, ByteKey.empty(), 0, 0, ByteKey.empty(), 0,
          Optional.empty(), Optional.empty(), Optional.empty());
    }

    static Slot active(long generation, ByteKey pinId, long requiredRevision,
        long requiredSequence, ByteKey viewDigest, long expiresAtEpochMillis,
        Optional<TransactionId> transactionId, Optional<ByteKey> rootIdentity,
        Optional<ByteKey> rootDigest) {
      return new Slot(Status.ACTIVE, generation, pinId, requiredRevision, requiredSequence,
          viewDigest, expiresAtEpochMillis, transactionId, rootIdentity, rootDigest);
    }

    Slot renewed(long expiresAt) {
      return active(generation, pinId, requiredRevision, requiredSequence, viewDigest,
          expiresAt, transactionId, rootIdentity, rootDigest);
    }

    Slot terminal(Status terminalStatus) {
      if (terminalStatus != Status.COMPLETED && terminalStatus != Status.REVOKED) {
        throw new IllegalArgumentException("snapshot pin terminal status is invalid");
      }
      return new Slot(terminalStatus, generation, pinId, 0, 0, ByteKey.empty(), 0,
          Optional.empty(), Optional.empty(), Optional.empty());
    }

    boolean matches(ByteKey expectedPinId, long expectedGeneration) {
      return generation == expectedGeneration && pinId.equals(expectedPinId);
    }

    SnapshotGcBoundary.Protection protection(int shard, int slot) {
      if (status != Status.ACTIVE) throw new IllegalStateException("snapshot pin slot is not active");
      return new SnapshotGcBoundary.Protection(shard, slot, generation, pinId,
          requiredRevision, requiredSequence, transactionId, rootIdentity, rootDigest);
    }
  }
}
