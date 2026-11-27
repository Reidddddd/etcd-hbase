package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.*;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.GcCheckpoint;
import io.etcdhbase.core.commit.GcCheckpointStore;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.SystemRecordClosure;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.codec.ManifestMessages;
import io.etcdhbase.hbase.storage.HBaseSystemRecordClosure;
import io.etcdhbase.hbase.storage.SystemRecordGcTestFixture;
import io.etcdhbase.server.observability.RequestMetrics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Real selected Compact, sealed pin registries, per-bucket retirement and durable restart. */
class GcCheckpointCycleIT {
  @Test
  void realWorkerProtectsPinnedEventsThenRestartCollectsBothBucketsAndFencesOldCheckpoint() throws Exception {
    try (var fixtureScope = RequestScope.withTimeout(Duration.ofMinutes(3));
        var environment = SystemRecordGcTestFixture.open(2, fixtureScope);
        var timer = new ScheduledThreadPoolExecutor(1)) {
      var records = environment.records();
      var authority = environment.authority();
      var events = new ArrayList<PreparedEvent>();
      int[] counts = new int[2];
      while (counts[0] < 2 || counts[1] < 2) {
        var event = event();
        int bucket = records.bucket(SystemRecordType.EVENT, event.systemIdentity());
        if (counts[bucket] == 2) continue;
        counts[bucket]++;
        events.add(event);
        assertEquals(SystemRecordStore.CreateResult.CREATED, records.createIfAbsent(SystemRecordType.EVENT,
            event.systemIdentity(), ByteKey.copyOf(ManifestMessages.encode(event)), fixtureScope).join());
      }
      var pin = authority.eventPins().register(new EventHistoryPinManager.Requirement(
          4, 4, SystemRecordGcTestFixture.DIGEST, Optional.empty(), Optional.empty()), fixtureScope).join();
      var budgets = budgets(environment.group());
      var metrics = new RequestMetrics();
      var checkpoints = new GcCheckpointStore(records);
      var delegate = new HBaseSystemRecordClosure(records);
      var released = new java.util.concurrent.atomic.AtomicBoolean();
      var releasing = new SystemRecordClosure() {
        @Override public java.util.concurrent.CompletableFuture<Page> fence(Domain domain, int bucket,
            Optional<ByteKey> after, int maximumRecords, io.etcdhbase.core.commit.SystemRecordRetirement.Permit permit,
            RequestScope scope) {
          return delegate.fence(domain, bucket, after, maximumRecords, permit, scope);
        }
        @Override public java.util.concurrent.CompletableFuture<Page> collect(Domain domain, int bucket,
            Optional<ByteKey> after, int maximumRecords, io.etcdhbase.core.commit.SystemRecordRetirement.Permit permit,
            RequestScope scope) {
          return delegate.collect(domain, bucket, after, maximumRecords, permit, scope).thenCompose(page -> {
            if (domain == Domain.EVENT && bucket == 0 && after.isEmpty() && released.compareAndSet(false, true)) {
              pin.close();
              return pin.releaseCompletion().toCompletableFuture().thenApply(ignored -> page);
            }
            return java.util.concurrent.CompletableFuture.completedFuture(page);
          });
        }
      };
      var first = cycle(environment, checkpoints, budgets, metrics, releasing);
      var boundary = new CountDownLatch(1);
      var completed = new AtomicReference<GcCheckpointWorker.Outcome>();
      var worker = new GcCheckpointWorker(Runnable::run, timer, Duration.ofSeconds(20), scope ->
          first.run(scope).thenApply(outcome -> {
            completed.set(outcome);
            if (outcome == GcCheckpointWorker.Outcome.IDLE) boundary.countDown();
            return outcome;
          }), metrics);
      try {
        worker.start();
        assertTrue(boundary.await(60, TimeUnit.SECONDS), "worker must finish an actual bounded sweep");
      } finally {
        worker.stop();
        worker.drained().get(30, TimeUnit.SECONDS);
      }
      assertEquals(GcCheckpointWorker.Outcome.IDLE, completed.get());
      assertEquals(0, metrics.gc().failures());
      for (var event : events) {
        assertTrue(records.read(SystemRecordType.EVENT, event.systemIdentity(), fixtureScope).join().isPresent());
      }
      assertTrue(released.get());
      assertEquals(0, metrics.gc().retired(), "the original fenced sweep keeps its pin-constrained bounds");
      for (int bucket = 0; bucket < 2; bucket++) {
        var checkpoint = checkpoints.read(environment.group(), SystemRecordClosure.Domain.EVENT, bucket, fixtureScope)
            .join().orElseThrow();
        assertTrue(checkpoint.exhausted());
        assertEquals(4, checkpoint.plan().safeSequence());
        assertEquals(4, checkpoint.plan().safeRevision());
        assertEquals(bucket, records.bucket(SystemRecordType.EVENT,
            checkpoint.position().afterIdentity().orElseThrow()));
        assertEquals(GcCheckpoint.Phase.SCANNING, checkpoint.phase());
        assertEquals(4, checkpoint.completedPages(), "two fence pages precede the two collection pages");
      }
      pin.close();
      pin.releaseCompletion().toCompletableFuture().get(10, TimeUnit.SECONDS);
      var restarted = cycle(environment, checkpoints, budgets, metrics);
      GcCheckpointWorker.Outcome outcome;
      int pages = 0;
      do {
        outcome = run(restarted);
        assertTrue(++pages < 30, "restart must make bounded progress from durable shards");
      } while (outcome != GcCheckpointWorker.Outcome.IDLE);
      for (var event : events) {
        assertTrue(records.read(SystemRecordType.EVENT, event.systemIdentity(), fixtureScope).join().isEmpty());
        var lateCreate = assertThrows(java.util.concurrent.CompletionException.class,
            () -> records.createIfAbsent(SystemRecordType.EVENT, event.systemIdentity(),
                ByteKey.copyOf(ManifestMessages.encode(event)), fixtureScope).join());
        assertEquals(io.etcdhbase.core.error.BackendFailure.Category.UNKNOWN,
            assertInstanceOf(io.etcdhbase.core.error.BackendFailure.class, lateCreate.getCause()).category());
        assertTrue(records.read(SystemRecordType.EVENT, event.systemIdentity(), fixtureScope).join().isEmpty());
      }
      assertEquals(4, metrics.gc().retired());
      assertEquals(0, budgets.snapshot().requests());
      var current = checkpoints.read(environment.group(), SystemRecordClosure.Domain.EVENT, 0, fixtureScope).join().orElseThrow();
      var proposal = new GcCheckpoint(current.tableGroupId(), current.domain(), current.maintenanceEpoch(),
          0, 2, current.plan(), GcCheckpoint.Position.start(), GcCheckpoint.Position.start(), GcCheckpoint.Phase.FENCING, 0, 0, false);
      var held = assertInstanceOf(GcCheckpointStore.Held.class, checkpoints.claim(proposal, fixtureScope).join());
      var newer = new GcCheckpoint(proposal.tableGroupId(), proposal.domain(), proposal.maintenanceEpoch() + 1,
          0, 2, proposal.plan(), GcCheckpoint.Position.start(), GcCheckpoint.Position.start(), GcCheckpoint.Phase.FENCING, 0, 0, false);
      assertInstanceOf(GcCheckpointStore.Held.class, checkpoints.claim(newer, fixtureScope).join());
      assertFalse(checkpoints.advance(held, new SystemRecordClosure.Page(current.domain(), 0, 0,
          held.checkpoint().position().afterIdentity(), true), fixtureScope).join());
    }
  }

  private static GcCheckpointCycle cycle(SystemRecordGcTestFixture.Environment environment,
      GcCheckpointStore checkpoints, BudgetManager budgets, RequestMetrics metrics) {
    return cycle(environment, checkpoints, budgets, metrics, new HBaseSystemRecordClosure(environment.records()));
  }

  private static GcCheckpointCycle cycle(SystemRecordGcTestFixture.Environment environment,
      GcCheckpointStore checkpoints, BudgetManager budgets, RequestMetrics metrics, SystemRecordClosure closure) {
    var authority = environment.authority();
    return new GcCheckpointCycle(environment.group(), () -> Optional.of(authority.owner()), authority.control(),
        authority.coordinator(), authority.retention(), SystemRecordGcTestFixture.DIGEST,
        closure, checkpoints, budgets,
        GcCheckpointCycle.MEMORY, 2, 1, metrics);
  }

  private static GcCheckpointWorker.Outcome run(GcCheckpointCycle cycle) throws Exception {
    var scope = RequestScope.withTimeout(Duration.ofSeconds(20));
    try { return cycle.run(scope).get(20, TimeUnit.SECONDS); }
    finally {
      scope.close();
      scope.physicalProducersCompletion().toCompletableFuture().get(20, TimeUnit.SECONDS);
      scope.cleanupOutcomeCompletion().toCompletableFuture().get(20, TimeUnit.SECONDS);
    }
  }

  private static BudgetManager budgets(String group) {
    var limits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1_000_000, 256L << 20);
    return new BudgetManager(limits, Map.of(group, limits), limits);
  }

  private static PreparedEvent event() {
    var value = StoredRecord.live(ByteKey.copyOf(new byte[] {1}),
        new StorageMetadata(6, 6, 1, 0, StorageMetadata.State.LIVE, 1), new byte[] {2});
    return new PreparedEvent(TransactionId.create(), 1, 4, 5, 0, PreparedEvent.Kind.PUT, value, Optional.empty());
  }
}
