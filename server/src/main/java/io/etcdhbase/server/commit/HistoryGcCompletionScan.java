package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CompactionOperation;
import io.etcdhbase.core.commit.HistoryGcCheckpointStore;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** One waiter-owned, bounded walk of monotonic per-bucket KV completion receipts. */
final class HistoryGcCompletionScan {
  private final HistoryGcCheckpointStore checkpoints;
  private final int buckets;
  private int bucket;

  HistoryGcCompletionScan(HistoryGcCheckpointStore checkpoints, int buckets) {
    this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
    if (buckets < 1 || buckets > 65_536) throw new IllegalArgumentException("invalid completion buckets");
    this.buckets = buckets;
  }

  CompletableFuture<PhysicalCompactionWaiter.Progress> readNext(
      CompactionOperation target, RequestScope scope) {
    return checkpoints.read(bucket, scope).thenApply(found -> {
      boolean completed = found.filter(checkpoint -> checkpoint.completedGeneration() > 0
          && checkpoint.completedRevision() >= target.revision()
          && checkpoint.completedSequence() >= target.baseSequence()).isPresent();
      if (!completed) {
        bucket = 0;
        return PhysicalCompactionWaiter.Progress.WAITING;
      }
      bucket++;
      return bucket == buckets ? PhysicalCompactionWaiter.Progress.COMPLETE
          : PhysicalCompactionWaiter.Progress.ADVANCED;
    });
  }
}
