package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable domain and progress models for bounded system-record closure collection.
 *
 * <p>§6.2 of the M5 design lists the retention rule for every domain. A domain becomes collectable
 * only after Compact, the protection floors, the exact root references and the remaining domains'
 * dependencies have all cleared. The storage adapter owns proof and retirement execution.
 */
public final class SystemRecordClosure {
  private SystemRecordClosure() {}

  /** Record domains; each page independently checks the surviving reference graph. */
  public enum Domain {
    INVOCATION(SystemRecordType.INVOCATION),
    EVENT_INDEX(SystemRecordType.EVENT_REVISION_INDEX),
    RESULT(SystemRecordType.RESULT),
    CANDIDATE(SystemRecordType.CANDIDATE),
    MANIFEST(SystemRecordType.MANIFEST),
    DECISION(SystemRecordType.DECISION),
    EVENT(SystemRecordType.EVENT);

    private final SystemRecordType recordType;

    Domain(SystemRecordType recordType) { this.recordType = recordType; }

    public SystemRecordType recordType() { return recordType; }
  }

  /** What one bounded closure page did, so a caller can resume without guessing. */
  public record Page(Domain domain, int examined, int retired, Optional<ByteKey> lastIdentity,
      boolean exhausted) {
    public Page {
      Objects.requireNonNull(domain, "domain");
      Objects.requireNonNull(lastIdentity, "lastIdentity");
      if (examined < 0 || retired < 0 || retired > examined) {
        throw new IllegalArgumentException("invalid closure page counters");
      }
      if (!exhausted && lastIdentity.isEmpty()) {
        throw new IllegalArgumentException("an unfinished closure page must publish a resume point");
      }
    }
  }

}
