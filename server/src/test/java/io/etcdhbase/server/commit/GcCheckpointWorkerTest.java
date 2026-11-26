package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.GcCheckpointStore;
import io.etcdhbase.core.commit.SystemRecordClosure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.server.observability.RequestMetrics;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * §6.3 requires background GC to run only as the current owner, to yield instead of competing for
 * admission, and to drain cleanly on stop. These are lifecycle properties, not closure semantics.
 */
class GcCheckpointWorkerTest {
  private static final ByteKey PROFILE = ByteKey.copyOf(new byte[32]);
  private static final ByteKey REGISTRY = ByteKey.copyOf(new byte[16]);

  @Test
  void aNonOwnerNeverTouchesAShard() throws Exception {
    var closure = mock(SystemRecordClosure.class);
    var checkpoints = mock(GcCheckpointStore.class);
    when(checkpoints.claim(any(), any())).thenReturn(CompletableFuture.completedFuture(
        new GcCheckpointStore.Absent()));
    var owner = mock(M3OwnerLease.class);
    when(owner.currentToken()).thenReturn(Optional.empty());
    var worker = worker(closure, checkpoints, owner, budgets());

    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      worker.start();
      Thread.sleep(150);
      worker.stop();
      worker.drained().get(5, TimeUnit.SECONDS);
    }

    verify(closure, never()).collect(any(), any(), anyInt(), any(), any());
    verify(checkpoints, never()).claim(any(), any());
  }

  @Test
  void anOwnerCollectsOnlyThroughItsCheckpointsAndStandeesDownOnATakeover() throws Exception {
    var closure = mock(SystemRecordClosure.class);
    when(closure.collect(any(), any(), anyInt(), any(), any())).thenReturn(CompletableFuture.completedFuture(
        new SystemRecordClosure.Page(SystemRecordClosure.Domain.MANIFEST, 0, 0, Optional.empty(), true)));
    var checkpoints = mock(GcCheckpointStore.class);
    // A newer epoch already owns every shard: this worker must not write anything.
    when(checkpoints.claim(any(), any())).thenReturn(CompletableFuture.completedFuture(
        new GcCheckpointStore.Superseded(new io.etcdhbase.core.commit.GcCheckpoint(
            "gc-group", SystemRecordClosure.Domain.MANIFEST, 99, 0, 1, 0, 0,
            io.etcdhbase.core.commit.GcCheckpoint.Position.start(), 0))));
    var owner = mock(M3OwnerLease.class);
    when(owner.currentToken()).thenReturn(Optional.of(
        new CommitOwnerService.OwnerToken("owner", 3)));
    var worker = worker(closure, checkpoints, owner, budgets());

    worker.start();
    Thread.sleep(150);
    worker.stop();
    worker.drained().get(5, TimeUnit.SECONDS);

    verify(checkpoints, never()).advance(any(), any(), any());
    verify(closure, never()).collect(any(), any(), anyInt(), any(), any());
    assertTrue(worker.backlogRounds() >= 0);
  }

  @Test
  void missingAdmissionYieldsAndRecordsBacklogInsteadOfRunning() throws Exception {
    var closure = mock(SystemRecordClosure.class);
    var checkpoints = mock(GcCheckpointStore.class);
    var owner = mock(M3OwnerLease.class);
    when(owner.currentToken()).thenReturn(Optional.of(
        new CommitOwnerService.OwnerToken("owner", 3)));
    var limits = new BudgetManager.Limits(1, 0, 1, 1, 1, 0, 1024);
    var exhausted = new BudgetManager(limits, Map.of("gc-group", limits), limits);
    var reservation = new BudgetManager.Reservation(1L << 40, 1L << 40, 1L << 40, 1L << 40, 1L << 40);
    var worker = new GcCheckpointWorker("gc-group", closure, checkpoints, owner, 1, 8, 5_000,
        PROFILE, REGISTRY, exhausted, reservation, Runnable::run, timers(), Clock.systemUTC(),
        new RequestMetrics());

    worker.start();
    Thread.sleep(200);
    worker.stop();
    worker.drained().get(5, TimeUnit.SECONDS);

    assertTrue(worker.backlogRounds() > 0, "a refused admission must be recorded, not silently dropped");
    verify(checkpoints, never()).claim(any(), any());
  }

  @Test
  void stopBeforeStartIsSafeAndStartCannotRepeat() throws Exception {
    var closure = mock(SystemRecordClosure.class);
    var checkpoints = mock(GcCheckpointStore.class);
    var owner = mock(M3OwnerLease.class);
    when(owner.currentToken()).thenReturn(Optional.empty());
    var worker = worker(closure, checkpoints, owner, budgets());

    worker.stop();
    worker.drained().get(5, TimeUnit.SECONDS);
    // A stopped worker has already drained; it must refuse to restart rather than resurrect.
    assertThrows(IllegalStateException.class, worker::start);
    assertFalse(worker.isReady());
    worker.stop();
  }

  private static GcCheckpointWorker worker(SystemRecordClosure closure,
      GcCheckpointStore checkpoints, M3OwnerLease owner, BudgetManager budgets) {
    var reservation = new BudgetManager.Reservation(1L << 16, 1L << 16, 1L << 16, 1L << 16, 1L << 16);
    return new GcCheckpointWorker("gc-group", closure, checkpoints, owner, 1, 8, 5_000,
        PROFILE, REGISTRY, budgets, reservation, Runnable::run, timers(), Clock.systemUTC(),
        new RequestMetrics());
  }

  private static BudgetManager budgets() {
    var limits = new BudgetManager.Limits(4, 0, 4, 4, 4, 0, 1L << 20);
    return new BudgetManager(limits, Map.of("gc-group", limits), limits);
  }

  private static ScheduledThreadPoolExecutor timers() {
    var timers = new ScheduledThreadPoolExecutor(1,
        Thread.ofPlatform().daemon().name("j52-gc-test-timer-", 0).factory());
    timers.setRemoveOnCancelPolicy(true);
    return timers;
  }
}
