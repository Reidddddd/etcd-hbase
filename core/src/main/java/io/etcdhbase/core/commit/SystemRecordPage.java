package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Bounded scan result in storage order. The exclusive cursor is the last inspected identity, including
 * retired rows that have no live entry. Encoded bytes include those inspected retirement records.
 */
public record SystemRecordPage(List<SystemRecordEntry> entries,
    Optional<ByteKey> lastIdentity, boolean exhausted, long encodedBytes,
    long physicalRows, long scanRpcCalls) {
  public SystemRecordPage(List<SystemRecordEntry> entries, Optional<ByteKey> lastIdentity,
      boolean exhausted, long encodedBytes) {
    this(entries, lastIdentity, exhausted, encodedBytes, entries.size(), 1);
  }

  public SystemRecordPage {
    entries = List.copyOf(entries);
    Objects.requireNonNull(lastIdentity, "lastIdentity");
    if (encodedBytes < 0 || physicalRows < entries.size() || scanRpcCalls < 1
        || lastIdentity.isPresent() && physicalRows == 0
        || !exhausted && lastIdentity.isEmpty()
        || !entries.isEmpty() && lastIdentity.isEmpty()) {
      throw new IllegalArgumentException("invalid system record page");
    }
    var identities = new HashSet<ByteKey>();
    for (var entry : entries) {
      if (!identities.add(entry.identity())) {
        throw new IllegalArgumentException("system record page contains duplicate identities");
      }
    }
  }
}
