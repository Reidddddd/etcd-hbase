package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseBindingChange;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Bounded net binding changes for keys touched by one private transaction. */
final class LeaseBindingChangeSet {
  private static final int MAX_CHANGES = 8_192;
  private static final long OBJECT_ALLOWANCE = 512;

  private final long maximumRetainedBytes;
  private final Map<ByteKey, LeaseBindingChange> changes = new HashMap<>();
  private long retainedBytes;

  LeaseBindingChangeSet(long maximumRetainedBytes) {
    if (maximumRetainedBytes < 1) throw new IllegalArgumentException("binding change budget must be positive");
    this.maximumRetainedBytes = maximumRetainedBytes;
  }

  synchronized void record(Optional<StoredRecord> previous, StoredRecord current) throws BackendFailure {
    ByteKey key = current.key();
    LeaseBindingChange prior = changes.get(key);
    if (prior == null) {
      Optional<LeaseBindingChange> change = LeaseBindingChange.between(previous, current);
      if (change.isEmpty()) return;
      long bytes = retainedBytes(change.orElseThrow());
      if (changes.size() == MAX_CHANGES || bytes > maximumRetainedBytes / 2 - retainedBytes) {
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
      changes.put(key, change.orElseThrow());
      retainedBytes = Math.addExact(retainedBytes, bytes);
      return;
    }

    LeaseBindingChange updated = prior.through(previous, current);
    if (updated.previousLeaseId() == 0 && updated.currentLeaseId() == 0) {
      changes.remove(key);
      retainedBytes = Math.subtractExact(retainedBytes, retainedBytes(prior));
      return;
    }
    changes.put(key, updated);
  }

  synchronized List<LeaseBindingChange> snapshot() {
    var result = new ArrayList<>(changes.values());
    result.sort(java.util.Comparator.comparing(LeaseBindingChange::key));
    return List.copyOf(result);
  }

  private static long retainedBytes(LeaseBindingChange change) {
    return Math.addExact(OBJECT_ALLOWANCE, change.key().length());
  }
}
