package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Decides whether one record domain is unreachable yet, and only then retires it.
 *
 * <p>§6.2 of the M5 design lists the retention rule for every domain. A domain becomes collectable
 * only after Compact, the protection floors, the exact root references and the remaining domains'
 * dependencies have all cleared. This type deliberately keeps that decision separate from the
 * physical delete so neither can be mistaken for the other.
 */
public interface SystemRecordClosure {
  /** Ordered domains: a later domain is never collected before every domain it depends on. */
  enum Domain {
    INVOCATION(SystemRecordType.INVOCATION),
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
  record Page(Domain domain, int examined, int retired, Optional<ByteKey> lastIdentity,
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

  /**
   * Collects at most {@code maximumRecords} records of one domain that are provably unreachable.
   * Records whose references or protection floors still hold are examined but never retired, so a
   * caller cannot mistake "not yet collectable" for "does not exist".
   */
  CompletableFuture<Page> collect(Domain domain, Optional<ByteKey> afterIdentity, int maximumRecords,
      SystemRecordRetirement.Permit permit, RequestScope scope);
}
