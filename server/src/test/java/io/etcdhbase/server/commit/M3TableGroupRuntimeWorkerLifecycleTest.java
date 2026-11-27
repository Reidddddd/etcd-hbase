package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import io.etcdhbase.core.commit.SharedEventTailer;
import io.etcdhbase.core.request.RequestScope;
import org.junit.jupiter.api.Test;

class M3TableGroupRuntimeWorkerLifecycleTest {
  @Test
  void historyMaintenanceRunsWithoutLeaseAndOwnerLossRejectsLateInstallation() {
    try (var scope = RequestScope.withoutDeadline()) {
      var pending = new M3TableGroupRuntime.Pending(scope, null, false);
      var history = mock(GcCheckpointWorker.class);
      var completion = mock(PhysicalCompactionWaiter.class);
      pending.installHistoryMaintenance(history, completion);
      verify(history).start();
      pending.ownerLost();
      verify(history).stop();
      verify(completion).close();
      var late = mock(GcCheckpointWorker.class);
      assertThrows(IllegalStateException.class,
          () -> pending.installHistoryMaintenance(late, mock(PhysicalCompactionWaiter.class)));
      verify(late, never()).start();
    }
  }

  @Test
  void failedHistoryStopStillClosesWaitersSystemGcAndTailerOnOwnerLoss() {
    try (var scope = RequestScope.withoutDeadline()) {
      var pending = new M3TableGroupRuntime.Pending(scope, null, true);
      var history = mock(GcCheckpointWorker.class);
      var completion = mock(PhysicalCompactionWaiter.class);
      var system = mock(GcCheckpointWorker.class);
      var tailer = mock(SharedEventTailer.class);
      pending.installHistoryMaintenance(history, completion);
      pending.installGcWorker(system);
      pending.installEventTailer(tailer);
      org.mockito.Mockito.doThrow(new IllegalStateException("stop failed")).when(history).stop();

      assertThrows(java.util.concurrent.CompletionException.class, pending::ownerLost);

      verify(completion).close();
      verify(system).stop();
      verify(tailer).close();
    }
  }

  @Test
  void ownerLossStopsInstalledGcAndRejectsReplacement() {
    try (var scope = RequestScope.withoutDeadline()) {
      var pending = new M3TableGroupRuntime.Pending(scope, null, true);
      var gc = mock(GcCheckpointWorker.class);
      pending.installGcWorker(gc);
      verify(gc).start();
      assertThrows(IllegalStateException.class, () -> pending.installGcWorker(mock(GcCheckpointWorker.class)));
      pending.ownerLost();
      verify(gc).stop();
      var late = mock(GcCheckpointWorker.class);
      assertThrows(IllegalStateException.class, () -> pending.installGcWorker(late));
      verify(late, never()).start();
    }
  }

  @Test
  void closedTailerCanBeDetachedAndRecreatedWithoutRemovingTheNewGeneration() {
    try (var scope = RequestScope.withoutDeadline()) {
      var pending = new M3TableGroupRuntime.Pending(scope, null, true);
      var first = mock(SharedEventTailer.class);
      when(first.isClosed()).thenReturn(false, true);
      assertSame(first, pending.installEventTailer(first));
      var firstReady = pending.eventTailerReady();
      assertSame(first, firstReady.join());

      assertFalse(pending.detachClosedEventTailer(first));
      assertSame(first, pending.eventTailer().orElseThrow());
      assertTrue(pending.detachClosedEventTailer(first));
      assertTrue(pending.eventTailer().isEmpty());

      var nextReady = pending.eventTailerReady();
      assertFalse(nextReady.isDone());
      var replacement = mock(SharedEventTailer.class);
      assertSame(replacement, pending.installEventTailer(replacement));
      assertSame(replacement, nextReady.join());
      assertSame(first, firstReady.join());
      assertFalse(pending.detachClosedEventTailer(first));
      assertSame(replacement, pending.eventTailer().orElseThrow());
    }
  }

  @Test
  void eventHistoryWorkerStartsImmediatelyAndStartupReadinessWaitsForTailer() throws Exception {
    try (var scope = RequestScope.withoutDeadline()) {
      var pending = new M3TableGroupRuntime.Pending(scope, null, true);
      var expiry = mock(LeaseExpiryWorker.class);
      var expiryReady = new java.util.concurrent.CompletableFuture<Void>();
      when(expiry.initialReadiness()).thenReturn(expiryReady);
      pending.startExpiryWorker(expiry);

      var history = mock(EventHistoryWorker.class);
      var historyReady = new java.util.concurrent.CompletableFuture<Void>();
      when(history.initialReadiness()).thenReturn(historyReady);
      pending.installEventHistoryWorker(history);

      verify(history, times(1)).start();
      var ready = pending.initialReadiness();
      assertFalse(ready.isDone(), "startup readiness waits for expiry, backfill and tailer readiness");
      expiryReady.complete(null);
      historyReady.complete(null);
      assertFalse(ready.isDone(), "a healthy worker without an installed tailer is not ready");

      var tailer = mock(SharedEventTailer.class);
      when(tailer.isClosed()).thenReturn(false);
      pending.installEventTailer(tailer);
      ready.get(5, java.util.concurrent.TimeUnit.SECONDS);
      pending.ownerLost();
      verify(history).stop();
    }
  }

  @Test
  void readinessRequiresEventPinsAHealthyWorkerAndAnOpenTailer() {
    var history = mock(EventHistoryWorker.class);
    var tailer = mock(SharedEventTailer.class);
    when(tailer.isClosed()).thenReturn(false, true);

    assertFalse(M3TableGroupRuntime.eventHistoryReady(false, history, tailer));
    assertFalse(M3TableGroupRuntime.eventHistoryReady(true, history, tailer),
        "a worker that has not completed its initial cycle cannot make the runtime ready");

    when(history.isReady()).thenReturn(true);
    assertTrue(M3TableGroupRuntime.eventHistoryReady(true, history, tailer));
    assertFalse(M3TableGroupRuntime.eventHistoryReady(true, history, tailer),
        "a closed tailer revokes runtime readiness");
  }

  @Test
  void ownerLossBeforeEventHistoryWorkerInstallationRejectsLateWorker() {
    try (var scope = RequestScope.withoutDeadline()) {
      var pending = new M3TableGroupRuntime.Pending(scope, null);
      var expiry = mock(LeaseExpiryWorker.class);
      var history = mock(EventHistoryWorker.class);
      pending.startExpiryWorker(expiry);

      pending.ownerLost();

      verify(expiry).stop();
      assertThrows(IllegalStateException.class, () -> pending.installEventHistoryWorker(history));
      verify(history, never()).start();
    }
  }

  @Test
  void ownerLossBeforeInstallationRejectsBothWorkers() {
    try (var scope = RequestScope.withoutDeadline()) {
      var pending = new M3TableGroupRuntime.Pending(scope, null);
      var expiry = mock(LeaseExpiryWorker.class);
      var history = mock(EventHistoryWorker.class);
      pending.ownerLost();

      assertThrows(IllegalStateException.class, () -> pending.startExpiryWorker(expiry));
      assertThrows(IllegalStateException.class, () -> pending.installEventHistoryWorker(history));
      verify(expiry, never()).start();
      verify(history, never()).start();
    }
  }
}
