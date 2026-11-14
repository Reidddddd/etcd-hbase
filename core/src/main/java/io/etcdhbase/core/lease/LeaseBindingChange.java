package io.etcdhbase.core.lease;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Objects;
import java.util.Optional;

/** The net Lease binding transition for one key in one unpublished KV transaction. */
public record LeaseBindingChange(
    ByteKey key,
    long previousLeaseId,
    long currentLeaseId,
    long previousCreateRevision,
    long previousModRevision,
    long previousValueBytes,
    long currentCreateRevision,
    long currentModRevision,
    long currentValueBytes) {

  public LeaseBindingChange {
    Objects.requireNonNull(key, "key");
    if (key.length() == 0 || previousLeaseId == 0 && previousValueBytes != 0
        || currentLeaseId == 0 && currentValueBytes != 0
        || previousValueBytes < 0 || currentValueBytes < 0
        || previousLeaseId != 0 && (previousCreateRevision <= 0 || previousModRevision < previousCreateRevision)
        || currentLeaseId != 0 && (currentCreateRevision <= 0 || currentModRevision < currentCreateRevision)) {
      throw new IllegalArgumentException("invalid Lease binding change");
    }
  }

  /** Captures the first-to-current transition, omitting keys that remain unleased. */
  public static Optional<LeaseBindingChange> between(
      Optional<StoredRecord> previous, StoredRecord current) {
    Objects.requireNonNull(previous, "previous");
    Objects.requireNonNull(current, "current");
    return Optional.ofNullable(change(previous, current));
  }

  /** Extends a previously captured transition with the next private operation for this key. */
  public LeaseBindingChange through(Optional<StoredRecord> previous, StoredRecord current) {
    Objects.requireNonNull(previous, "previous");
    Objects.requireNonNull(current, "current");
    StoredRecord observed = previous.orElse(null);
    if (observed == null || !observed.key().equals(key)
        || observed.metadata().createRevision() != currentCreateRevision
        || observed.metadata().modificationRevision() != currentModRevision
        || liveLeaseId(observed) != currentLeaseId
        || currentLeaseId != 0 && observed.value().orElseThrow().length != currentValueBytes) {
      throw new IllegalStateException("Lease binding transition is out of order");
    }
    LeaseBindingChange next = change(previous, current);
    if (next == null) {
      return new LeaseBindingChange(key, previousLeaseId, 0, previousCreateRevision,
          previousModRevision, previousValueBytes, current.metadata().createRevision(),
          current.metadata().modificationRevision(), 0);
    }
    return new LeaseBindingChange(key, previousLeaseId, next.currentLeaseId(),
        previousCreateRevision, previousModRevision, previousValueBytes,
        next.currentCreateRevision(), next.currentModRevision(), next.currentValueBytes());
  }

  /**
   * A new immutable binding version is needed only when association ownership or key incarnation
   * changes. Ordinary value updates retain the current binding row and update live-value cost.
   */
  public boolean changesBindingIdentity() {
    return previousLeaseId != currentLeaseId
        || previousLeaseId != 0 && previousCreateRevision != currentCreateRevision;
  }

  /** Number of new immutable binding versions required for this net transition. */
  public int newBindingVersionCount() {
    if (!changesBindingIdentity()) return 0;
    if (previousLeaseId != 0 && currentLeaseId != 0 && previousLeaseId != currentLeaseId) return 2;
    return 1;
  }

  private static LeaseBindingChange change(Optional<StoredRecord> previous, StoredRecord current) {
    if (previous.filter(record -> !record.key().equals(current.key())).isPresent()) {
      throw new IllegalArgumentException("Lease binding records refer to different keys");
    }
    StoredRecord oldRecord = previous.filter(record -> record.metadata().state() == StorageMetadata.State.LIVE)
        .orElse(null);
    long oldLeaseId = oldRecord == null ? 0 : oldRecord.metadata().leaseId();
    boolean currentLive = current.metadata().state() == StorageMetadata.State.LIVE;
    long newLeaseId = currentLive ? current.metadata().leaseId() : 0;
    if (oldLeaseId == 0 && newLeaseId == 0) return null;
    return new LeaseBindingChange(current.key(), oldLeaseId, newLeaseId,
        oldLeaseId == 0 ? 0 : oldRecord.metadata().createRevision(),
        oldLeaseId == 0 ? 0 : oldRecord.metadata().modificationRevision(),
        oldLeaseId == 0 ? 0 : oldRecord.value().orElseThrow().length,
        current.metadata().createRevision(), current.metadata().modificationRevision(),
        newLeaseId == 0 ? 0 : current.value().orElseThrow().length);
  }

  private static long liveLeaseId(StoredRecord record) {
    return record.metadata().state() == StorageMetadata.State.LIVE ? record.metadata().leaseId() : 0;
  }
}
