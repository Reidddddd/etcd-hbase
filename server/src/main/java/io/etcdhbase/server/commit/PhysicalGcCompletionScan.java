package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CompactionOperation;
import io.etcdhbase.core.commit.GcCheckpoint;
import io.etcdhbase.core.commit.GcCheckpointStore;
import io.etcdhbase.core.commit.HistoryGcCheckpointStore;
import io.etcdhbase.core.commit.SystemRecordClosure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** A waiter-owned walk; each poll reads at most one durable receipt, never any payload. */
final class PhysicalGcCompletionScan {
  private static final SystemRecordClosure.Domain[] DOMAINS = SystemRecordClosure.Domain.values();
  private final String group;
  private final HistoryGcCompletionScan history;
  private final GcCheckpointStore system;
  private final GcCheckpoint.RegistryBinding registries;
  private final int buckets;
  private boolean historyComplete;
  private int shard;

  PhysicalGcCompletionScan(String group, HistoryGcCheckpointStore history, GcCheckpointStore system,
      GcCheckpoint.RegistryBinding registries, int buckets) {
    this.group = Objects.requireNonNull(group, "group");
    this.history = new HistoryGcCompletionScan(history, buckets);
    this.system = Objects.requireNonNull(system, "system");
    this.registries = Objects.requireNonNull(registries, "registries");
    this.buckets = buckets;
  }

  CompletableFuture<PhysicalCompactionWaiter.Progress> readNext(
      CompactionOperation target, RequestScope scope) {
    if (!target.profile().equals(registries.profile())) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("Compact profile differs"));
    }
    if (!historyComplete) {
      return history.readNext(target, scope).thenApply(progress -> {
        historyComplete = progress == PhysicalCompactionWaiter.Progress.COMPLETE;
        return historyComplete ? PhysicalCompactionWaiter.Progress.ADVANCED : progress;
      });
    }
    return readSystem(target, scope);
  }

  private CompletableFuture<PhysicalCompactionWaiter.Progress> readSystem(
      CompactionOperation target, RequestScope scope) {
    return system.read(group, DOMAINS[shard / buckets], shard % buckets, scope).thenApply(found -> {
      found.ifPresent(checkpoint -> {
        if (checkpoint.bucketCount() != buckets || !checkpoint.plan().registries().equals(registries)) {
          throw new IllegalArgumentException("system GC completion binding differs");
        }
      });
      if (found.filter(checkpoint -> checkpoint.completion().covers(
          target.revision(), target.baseSequence())).isEmpty()) {
        shard = 0;
        return PhysicalCompactionWaiter.Progress.WAITING;
      }
      shard++;
      return shard == buckets * DOMAINS.length ? PhysicalCompactionWaiter.Progress.COMPLETE
          : PhysicalCompactionWaiter.Progress.ADVANCED;
    });
  }
}
