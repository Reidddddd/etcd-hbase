package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.commit.CompactionOperation;
import io.etcdhbase.core.commit.HistoryGcCheckpoint;
import io.etcdhbase.core.commit.HistoryGcCheckpointStore;
import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class HistoryGcCompletionScanTest {
  @Test
  void conservativePinBoundsDoNotCompleteTheRequestedTarget() {
    var store = mock(HistoryGcCheckpointStore.class);
    var progress = new AtomicReference<>(checkpoint(5, 50, 100));
    when(store.read(anyInt(), any())).thenAnswer(call ->
        CompletableFuture.completedFuture(Optional.of(progress.get())));
    var target = target(100, 200);
    try (var scope = RequestScope.withoutDeadline()) {
      var scan = new HistoryGcCompletionScan(store, 2);
      assertEquals(PhysicalCompactionWaiter.Progress.WAITING, scan.readNext(target, scope).join());
      progress.set(checkpoint(5, 100, 200));
      assertEquals(PhysicalCompactionWaiter.Progress.ADVANCED, scan.readNext(target, scope).join());
      assertEquals(PhysicalCompactionWaiter.Progress.COMPLETE, scan.readNext(target, scope).join());
    }
  }

  @Test
  void defaultZeroCountersAreNotACompletedZeroRevisionSweep() {
    var store = mock(HistoryGcCheckpointStore.class);
    var progress = new AtomicReference<>(checkpoint(0, 0, 0));
    when(store.read(anyInt(), any())).thenAnswer(call ->
        CompletableFuture.completedFuture(Optional.of(progress.get())));
    try (var scope = RequestScope.withoutDeadline()) {
      var scan = new HistoryGcCompletionScan(store, 1);
      var zero = target(0, 0);
      assertEquals(PhysicalCompactionWaiter.Progress.WAITING, scan.readNext(zero, scope).join());
      progress.set(checkpoint(5, 0, 0));
      assertEquals(PhysicalCompactionWaiter.Progress.COMPLETE, scan.readNext(zero, scope).join());
    }
  }

  @Test
  void bothRevisionAndSequenceAndEveryBucketMustBeConfirmed() {
    var store = mock(HistoryGcCheckpointStore.class);
    var lowSequence = checkpoint(5, 100, 199);
    var lowRevision = checkpoint(5, 99, 200);
    var complete = checkpoint(5, 100, 200);
    when(store.read(anyInt(), any())).thenReturn(
        CompletableFuture.completedFuture(Optional.of(lowSequence)),
        CompletableFuture.completedFuture(Optional.of(lowRevision)),
        CompletableFuture.completedFuture(Optional.of(complete)),
        CompletableFuture.completedFuture(Optional.empty()));
    try (var scope = RequestScope.withoutDeadline()) {
      var scan = new HistoryGcCompletionScan(store, 2);
      var target = target(100, 200);
      assertEquals(PhysicalCompactionWaiter.Progress.WAITING, scan.readNext(target, scope).join());
      assertEquals(PhysicalCompactionWaiter.Progress.WAITING, scan.readNext(target, scope).join());
      assertEquals(PhysicalCompactionWaiter.Progress.ADVANCED, scan.readNext(target, scope).join());
      assertEquals(PhysicalCompactionWaiter.Progress.WAITING, scan.readNext(target, scope).join());
    }
  }

  private static HistoryGcCheckpoint checkpoint(long generation, long revision, long sequence) {
    var checkpoint = mock(HistoryGcCheckpoint.class);
    when(checkpoint.completedGeneration()).thenReturn(generation);
    when(checkpoint.completedRevision()).thenReturn(revision);
    when(checkpoint.completedSequence()).thenReturn(sequence);
    return checkpoint;
  }

  private static CompactionOperation target(long revision, long sequence) {
    var operation = mock(CompactionOperation.class);
    when(operation.revision()).thenReturn(revision);
    when(operation.baseSequence()).thenReturn(sequence);
    return operation;
  }
}
