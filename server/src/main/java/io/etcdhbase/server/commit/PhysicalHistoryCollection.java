package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.HistoryGcPermit;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.storage.HBaseHistoryCollector;
import io.etcdhbase.hbase.storage.HBaseHistoryKeyEnumerator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Drains the bounded history pages a {@code physical} Compact promises to finish.
 *
 * <p>§6.1 requires that {@code physical=true} waits for the deletions this call is responsible for
 * and that a logical CAS is never reported as physical completion. This collector therefore walks
 * key pages under the authorized permit, deletes each key's redundant versions, and only advances
 * its cursor after the delete for that key was confirmed. It still makes no claim about HFile space,
 * which HBase compaction releases asynchronously.
 */
final class PhysicalHistoryCollection implements CompactGateway.HistoryCollection {
  private static final int MAX_KEYS_PER_ROUND = 64;

  private final HBaseHistoryKeyEnumerator enumerator;
  private final HBaseHistoryCollector collector;

  PhysicalHistoryCollection(HBaseHistoryKeyEnumerator enumerator, HBaseHistoryCollector collector) {
    this.enumerator = Objects.requireNonNull(enumerator, "enumerator");
    this.collector = Objects.requireNonNull(collector, "collector");
  }

  @Override
  public CompletableFuture<Integer> collect(HistoryGcPermit permit, RequestScope scope) {
    Objects.requireNonNull(permit, "permit");
    Objects.requireNonNull(scope, "scope");
    return drain(permit, Optional.empty(), 0, scope);
  }

  private CompletableFuture<Integer> drain(HistoryGcPermit permit, Optional<ByteKey> after,
      int deletedSoFar, RequestScope scope) {
    if (scope.isClosed() || scope.isDeadlineExceeded()) {
      return CompletableFuture.failedFuture(new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
    }
    return enumerator.nextKeys(permit, after, MAX_KEYS_PER_ROUND, scope).thenCompose(page -> {
      if (page.keys().isEmpty()) return CompletableFuture.completedFuture(deletedSoFar);
      return deleteKeys(permit, page.keys(), 0, deletedSoFar, scope).thenCompose(deleted -> {
        if (page.exhausted()) return CompletableFuture.completedFuture(deleted);
        // The cursor only advances to a fully processed key page.
        return drain(permit, page.lastKey(), deleted, scope);
      });
    });
  }

  private CompletableFuture<Integer> deleteKeys(HistoryGcPermit permit, java.util.List<ByteKey> keys,
      int index, int deletedSoFar, RequestScope scope) {
    if (index >= keys.size()) return CompletableFuture.completedFuture(deletedSoFar);
    return deleteKey(permit, keys.get(index), scope)
        .thenCompose(deleted -> deleteKeys(permit, keys, index + 1, deletedSoFar + deleted, scope));
  }

  private CompletableFuture<Integer> deleteKey(HistoryGcPermit permit, ByteKey key,
      RequestScope scope) {
    return collector.collectKey(permit, key, 0, MAX_KEYS_PER_ROUND, scope)
        .thenCompose(page -> page.deletedVersions() > 0 || page.exhausted()
            ? CompletableFuture.completedFuture(page.deletedVersions())
            // One bounded page may not finish a key with many versions; continue from its cursor.
            : collector.collectKey(permit, key, page.lastRevision(), MAX_KEYS_PER_ROUND, scope)
                .thenApply(next -> page.deletedVersions() + next.deletedVersions()));
  }

  static CompactGateway.HistoryCollection unavailable() {
    return (permit, scope) -> CompletableFuture.failedFuture(
        new BackendFailure(BackendFailure.Category.UNAVAILABLE));
  }
}
