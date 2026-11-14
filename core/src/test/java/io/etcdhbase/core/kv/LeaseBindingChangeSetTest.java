package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.LeaseBindingChange;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class LeaseBindingChangeSetTest {
  private static final ByteKey KEY = ByteKey.copyOf(new byte[] {'r', 0, (byte) 0xff});

  @Test
  void keepsTheBaseToFinalTransitionAcrossSeveralPrivatePuts() throws Exception {
    var changes = new LeaseBindingChangeSet(8L << 10);
    StoredRecord original = live(4, 5, 2, 11, "old");
    StoredRecord rebound = live(4, 6, 3, 12, "middle");
    StoredRecord updated = live(4, 7, 4, 11, "final");

    changes.record(Optional.of(original), rebound);
    changes.record(Optional.of(rebound), updated);

    LeaseBindingChange change = changes.snapshot().getFirst();
    assertEquals(KEY, change.key());
    assertEquals(11, change.previousLeaseId());
    assertEquals(11, change.currentLeaseId());
    assertEquals(4, change.previousCreateRevision());
    assertEquals(5, change.previousModRevision());
    assertEquals(3, change.previousValueBytes());
    assertEquals(4, change.currentCreateRevision());
    assertEquals(7, change.currentModRevision());
    assertEquals(5, change.currentValueBytes());
  }

  @Test
  void createThenDeleteWithinOneTxnLeavesNoBindingDelta() throws Exception {
    var changes = new LeaseBindingChangeSet(8L << 10);
    StoredRecord created = live(9, 9, 1, 11, "temporary");
    StoredRecord deleted = tombstone(9, 10, 2);

    changes.record(Optional.empty(), created);
    changes.record(Optional.of(created), deleted);

    assertTrue(changes.snapshot().isEmpty());
  }

  @Test
  void deleteAndUnleasedRecreationRetainTheOriginalUnbind() throws Exception {
    var changes = new LeaseBindingChangeSet(8L << 10);
    StoredRecord original = live(4, 5, 2, 11, "old");
    StoredRecord deleted = tombstone(4, 6, 3);
    StoredRecord recreated = live(7, 7, 1, 0, "new");

    changes.record(Optional.of(original), deleted);
    changes.record(Optional.of(deleted), recreated);

    LeaseBindingChange change = changes.snapshot().getFirst();
    assertEquals(11, change.previousLeaseId());
    assertEquals(0, change.currentLeaseId());
    assertEquals(4, change.previousCreateRevision());
    assertEquals(5, change.previousModRevision());
    assertEquals(7, change.currentCreateRevision());
    assertEquals(7, change.currentModRevision());
  }

  private static StoredRecord live(long create, long modified, long version,
      long leaseId, String value) {
    byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    return StoredRecord.live(KEY,
        new StorageMetadata(create, modified, version, leaseId,
            StorageMetadata.State.LIVE, bytes.length), bytes);
  }

  private static StoredRecord tombstone(long create, long modified, long version) {
    return StoredRecord.metadataOnly(KEY,
        new StorageMetadata(create, modified, version, 0,
            StorageMetadata.State.TOMBSTONE, 0));
  }
}
