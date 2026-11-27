package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.*;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.storage.HBaseHistoryCollector;
import io.etcdhbase.hbase.storage.HBaseHistoryKeyEnumerator;
import io.etcdhbase.server.commit.GcCheckpointWorker.Outcome;
import io.etcdhbase.server.observability.RequestMetrics;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class HistoryGcCycleTest {
  private static final CommitOwnerService.OwnerToken OWNER = new CommitOwnerService.OwnerToken("owner", 3);
  private static final ByteKey PROFILE = ByteKey.copyOf(new byte[32]);
  private static final ByteKey REGISTRY = ByteKey.copyOf(TransactionId.create().bytes());
  private static final ByteKey KEY = ByteKey.copyOf(new byte[] {2});
  private final AtomicReference<Optional<CommitOwnerService.OwnerToken>> owner =
      new AtomicReference<>(Optional.of(OWNER));
  private final CommitControlStore control = mock(CommitControlStore.class);
  private final GcCoordinator coordinator = mock(GcCoordinator.class);
  private final HistoryGcCheckpointStore checkpoints = mock(HistoryGcCheckpointStore.class);
  private final HBaseHistoryKeyEnumerator enumerator = mock(HBaseHistoryKeyEnumerator.class);
  private final HBaseHistoryCollector collector = mock(HBaseHistoryCollector.class);
  private final RequestMetrics metrics = new RequestMetrics();
  private final BudgetManager budgets = budgets(256L << 20);

  @Test
  void persistsActiveKeyIntentBeforeAnyDeletionCanStart() {
    var permit = selected();
    held(HistoryGcCheckpoint.start("gc-group", 3, 0, 1, permit));
    when(enumerator.nextKey(any(), eq(0), eq(Optional.empty()), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(KEY)));
    when(checkpoints.beginKey(any(), eq(KEY), any())).thenReturn(CompletableFuture.completedFuture(true));
    try (var scope = scope()) {
      assertEquals(Outcome.PROGRESSED, cycle(budgets).run(scope).join());
      verify(checkpoints).beginKey(any(), eq(KEY), same(scope));
      verifyNoInteractions(collector);
      verify(checkpoints, never()).finishBucket(any(), any());
    }
    assertEquals(0, budgets.snapshot().requests());
  }

  @Test
  void resumesOnlyOneConfirmedVersionPageUnderTheFrozenSafeBounds() {
    var permit = selected();
    var checkpoint = new HistoryGcCheckpoint("gc-group", PROFILE, REGISTRY, 3, 0, 1,
        permit.operationId(), 5, 80, 160, Optional.empty(), Optional.of(KEY), 64, false, 0, 0, 0);
    held(checkpoint);
    var narrowed = mock(HistoryGcPermit.class);
    when(permit.restrictTo(80, 160)).thenReturn(narrowed);
    when(collector.collectKey(same(narrowed), eq(KEY), eq(64L), eq(64), any()))
        .thenReturn(CompletableFuture.completedFuture(new HBaseHistoryCollector.Page(79, true, 15)));
    when(checkpoints.afterPage(any(), eq(79L), eq(true), eq(15), any()))
        .thenReturn(CompletableFuture.completedFuture(true));
    try (var scope = scope()) {
      assertEquals(Outcome.PROGRESSED, cycle(budgets).run(scope).join());
      verify(collector).collectKey(same(narrowed), eq(KEY), eq(64L), eq(64), same(scope));
      verify(checkpoints).afterPage(any(), eq(79L), eq(true), eq(15), same(scope));
      verifyNoInteractions(enumerator);
      verify(checkpoints, never()).finishBucket(any(), any());
    }
  }

  @Test
  void unknownDeletionCannotAdvanceTheDurableVersionCursor() {
    var permit = selected();
    held(HistoryGcCheckpoint.start("gc-group", 3, 0, 1, permit).beginKey(KEY));
    when(collector.collectKey(any(), eq(KEY), eq(0L), eq(64), any())).thenReturn(
        CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN)));
    try (var scope = scope()) {
      assertThrows(CompletionException.class, () -> cycle(budgets).run(scope).join());
      verify(checkpoints, never()).afterPage(any(), anyLong(), anyBoolean(), anyInt(), any());
      verify(checkpoints, never()).finishBucket(any(), any());
    }
    assertEquals(0, budgets.snapshot().requests());
  }

  @Test
  void onlyBackendEofAfterTheLastCompleteKeyPublishesTheBucketWatermark() {
    var permit = selected();
    var checkpoint = HistoryGcCheckpoint.start("gc-group", 3, 0, 1, permit)
        .beginKey(KEY).afterPage(99, true, 10);
    held(checkpoint);
    when(enumerator.nextKey(any(), eq(0), eq(Optional.of(KEY)), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    when(checkpoints.finishBucket(any(), any())).thenReturn(CompletableFuture.completedFuture(true));
    try (var scope = scope()) {
      assertEquals(Outcome.PROGRESSED, cycle(budgets).run(scope).join());
      verify(checkpoints).finishBucket(any(), same(scope));
      verifyNoInteractions(collector);
    }
  }

  @Test
  void ownerLossBeforeDeleteReadbackCannotPublishProgress() {
    var permit = selected();
    held(HistoryGcCheckpoint.start("gc-group", 3, 0, 1, permit).beginKey(KEY));
    var completion = new CompletableFuture<HBaseHistoryCollector.Page>();
    when(collector.collectKey(any(), any(), anyLong(), anyInt(), any())).thenReturn(completion);
    try (var scope = scope()) {
      var pending = cycle(budgets).run(scope);
      owner.set(Optional.empty());
      completion.complete(new HBaseHistoryCollector.Page(64, false, 64));
      assertThrows(CompletionException.class, pending::join);
      verify(checkpoints, never()).afterPage(any(), anyLong(), anyBoolean(), anyInt(), any());
    }
  }

  @Test
  void maintenanceAdmissionDoesNotReleaseUntilTheActualProducerDrains() {
    var permit = selected();
    held(HistoryGcCheckpoint.start("gc-group", 3, 0, 1, permit).beginKey(KEY));
    var actual = new CompletableFuture<Void>();
    var response = new CompletableFuture<HBaseHistoryCollector.Page>();
    when(collector.collectKey(any(), any(), anyLong(), anyInt(), any())).thenAnswer(call -> {
      RequestScope scope = call.getArgument(4);
      scope.trackPhysicalProducer(actual);
      return response;
    });
    var scope = scope();
    var pending = cycle(budgets).run(scope);
    scope.cancel();
    response.completeExceptionally(new BackendFailure(BackendFailure.Category.CANCELLED));
    assertThrows(CompletionException.class, pending::join);
    assertEquals(1, budgets.snapshot().requests());
    assertFalse(scope.physicalProducersCompletion().toCompletableFuture().isDone());
    actual.complete(null);
    scope.cleanupOutcomeCompletion().toCompletableFuture().join();
    assertEquals(0, budgets.snapshot().requests());
  }

  @Test
  void unavailableMaintenanceCapacityStartsNoStorageWork() {
    try (var scope = scope()) {
      assertEquals(Outcome.BACKLOG, cycle(budgets(1)).run(scope).join());
      verifyNoInteractions(control, coordinator, checkpoints, enumerator, collector);
      assertEquals(1, metrics.gc().backlog());
    }
  }

  private HistoryGcPermit selected() {
    var state = mock(CommitState.class);
    var permit = mock(HistoryGcPermit.class);
    when(permit.profile()).thenReturn(PROFILE);
    when(permit.registryIdentity()).thenReturn(REGISTRY);
    var operationId = InvocationId.create();
    when(permit.operationId()).thenReturn(operationId);
    when(permit.generation()).thenReturn(5L);
    when(permit.safeRevision()).thenReturn(100L);
    when(permit.safeSequence()).thenReturn(200L);
    when(permit.restrictTo(anyLong(), anyLong())).thenReturn(permit);
    when(state.compactOperationId()).thenReturn(Optional.of(operationId));
    when(state.publication()).thenReturn(Optional.empty());
    when(control.read(any())).thenReturn(CompletableFuture.completedFuture(
        Optional.of(new ControlSnapshot(state, new byte[] {1}))));
    when(coordinator.authorizeHistory(eq(OWNER), any())).thenReturn(CompletableFuture.completedFuture(permit));
    return permit;
  }

  private void held(HistoryGcCheckpoint checkpoint) {
    when(checkpoints.claim(anyLong(), anyInt(), any(), any())).thenReturn(CompletableFuture.completedFuture(
        Optional.of(new HistoryGcCheckpointStore.Held(checkpoint, checkpoint.encode()))));
  }

  private HistoryGcCycle cycle(BudgetManager budgets) {
    return new HistoryGcCycle("gc-group", owner::get, control, coordinator, checkpoints,
        enumerator, collector, budgets, new BudgetManager.Reservation(64L << 10, 32L << 20, 1L << 20, 0, 0),
        metrics, 1);
  }

  private static RequestScope scope() { return RequestScope.withTimeout(Duration.ofSeconds(5)); }
  private static BudgetManager budgets(long bytes) {
    var limits = new BudgetManager.Limits(4, 0, 4, 4, 4, 0, bytes);
    return new BudgetManager(limits, Map.of("gc-group", limits), limits);
  }
}
