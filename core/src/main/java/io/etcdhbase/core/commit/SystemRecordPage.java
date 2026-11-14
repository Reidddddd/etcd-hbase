package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Bounded ordered scan result; the last identity is the exclusive resume point. */
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
        || entries.isEmpty() != lastIdentity.isEmpty()
        || !entries.isEmpty() && !lastIdentity.orElseThrow().equals(entries.getLast().identity())) {
      throw new IllegalArgumentException("invalid system record page");
    }
    for (int index = 1; index < entries.size(); index++) {
      if (entries.get(index - 1).identity().compareTo(entries.get(index).identity()) >= 0) {
        throw new IllegalArgumentException("system record identities must be strictly ordered");
      }
    }
  }
}
