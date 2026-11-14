package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Point-addressed durable M2 record port; persisted values are immutable byte strings. */
public interface SystemRecordStore {
  enum CreateResult { CREATED, ALREADY_PRESENT, CONFLICT }

  CompletableFuture<Optional<ByteKey>> read(
      SystemRecordType type, ByteKey identity, RequestScope scope);

  /** Validates the physical row identity and returns its configured salt bucket without I/O. */
  int bucket(SystemRecordType type, ByteKey identity);

  /** Returns CONFLICT for a durable value under the same identity with different bytes. */
  CompletableFuture<CreateResult> createIfAbsent(
      SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope);

  /** Exact-value CAS; empty expected means the row must not exist yet. */
  CompletableFuture<Boolean> compareAndSet(
      SystemRecordType type,
      ByteKey identity,
      Optional<ByteKey> expected,
      ByteKey replacement,
      RequestScope scope);

  /** Reads at most {@code maximumEntries} identities in one fixed salt bucket. */
  default CompletableFuture<SystemRecordPage> scanBucket(SystemRecordType type, int bucket,
      Optional<ByteKey> afterIdentity, int maximumEntries, long maximumBytes,
      RequestScope scope) {
    return CompletableFuture.failedFuture(new UnsupportedOperationException(
        "system-record range scans are not available"));
  }

  /** Reverse-seeks a versioned identity by stable route prefix and an inclusive sequence fence. */
  default CompletableFuture<Optional<SystemRecordEntry>> latestAtOrBefore(
      SystemRecordType type, ByteKey routePrefix, long sequence, RequestScope scope) {
    return CompletableFuture.failedFuture(new UnsupportedOperationException(
        "system-record version seeks are not available"));
  }

  /** Reads a bounded deadline interval from one expiry-candidate bucket. */
  default CompletableFuture<SystemRecordPage> scanExpiryWindow(int bucket,
      long startDeadlineInclusive, long stopDeadlineExclusive, Optional<ByteKey> afterIdentity,
      int maximumEntries, long maximumBytes, RequestScope scope) {
    return CompletableFuture.failedFuture(new UnsupportedOperationException(
        "expiry-window scans are not available"));
  }

  /** Reads only the versioned binding range for one Lease incarnation. */
  default CompletableFuture<SystemRecordPage> scanLeaseBindings(long leaseId, long incarnation,
      Optional<ByteKey> afterIdentity, int maximumEntries, long maximumBytes, RequestScope scope) {
    return CompletableFuture.failedFuture(new UnsupportedOperationException(
        "Lease binding range scans are not available"));
  }
}
