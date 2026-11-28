package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.*;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.server.observability.RequestMetrics;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class GcCheckpointCycleTest {
  private static final InvocationId OPERATION = InvocationId.create();
  private static final ByteKey PROFILE = ByteKey.copyOf(new byte[32]);
  private static final CommitOwnerService.OwnerToken OWNER = new CommitOwnerService.OwnerToken("owner", 3);
  private final AtomicReference<Optional<CommitOwnerService.OwnerToken>> owner = new AtomicReference<>(Optional.of(OWNER));
  private final CommitControlStore control = mock(CommitControlStore.class);
  private final GcCoordinator coordinator = mock(GcCoordinator.class);
  private final EventHistoryRetention retention = mock(EventHistoryRetention.class);
  private final SystemRecordClosure closure = mock(SystemRecordClosure.class);
  private final GcCheckpointStore checkpoints = mock(GcCheckpointStore.class);
  private final RequestMetrics metrics = new RequestMetrics();

  @Test
  void checkpointWaitsForPhysicalProducerAndCleanupBeforeMetadataCas() throws Exception {
    var physical = new CompletableFuture<Void>();
    var cleanup = new CompletableFuture<Void>();
    collectWithDrain(physical, cleanup);
    var budgets = budgets(256L << 20);
    var scope = scope();
    var result = cycle(budgets).run(scope);
    assertFalse(result.isDone());
    verify(checkpoints, never()).advance(any(), any(), any());
    physical.complete(null);
    assertFalse(result.isDone());
    verify(checkpoints, never()).advance(any(), any(), any());
    cleanup.complete(null);
    assertEquals(GcCheckpointWorker.Outcome.PROGRESSED, result.join());
    verify(checkpoints).advance(any(), any(), same(scope));
    scope.close();
    scope.cleanupOutcomeCompletion().toCompletableFuture().join();
    assertEquals(0, budgets.snapshot().requests());
  }

  @Test
  void failedProducerOrCleanupCannotPublishCheckpointProgress() throws Exception {
    for (boolean producerFails : new boolean[] {true, false}) {
      var physical = new CompletableFuture<Void>();
      var cleanup = new CompletableFuture<Void>();
      collectWithDrain(physical, cleanup);
      var scope = scope();
      var result = cycle(budgets(256L << 20)).run(scope);
      var failure = new IllegalStateException(producerFails ? "producer failure" : "cleanup failure");
      if (producerFails) physical.completeExceptionally(failure);
      else physical.complete(null);
      if (producerFails) cleanup.complete(null);
      else cleanup.completeExceptionally(failure);
      assertSame(failure, assertThrows(java.util.concurrent.CompletionException.class, result::join).getCause());
      verify(checkpoints, never()).advance(any(), any(), any());
      scope.close();
    }
  }

  @Test
  void durableOwnerChangeDuringDrainCannotAdvanceEvenWithAnUnchangedLocalToken() throws Exception {
    var physical = new CompletableFuture<Void>();
    var cleanup = new CompletableFuture<Void>();
    collectWithDrain(physical, cleanup);
    try (var scope = scope()) {
      var result = cycle(budgets(256L << 20)).run(scope);
      var changed = mock(CommitState.class);
      when(changed.ownerEpoch()).thenReturn(OWNER.epoch() + 1);
      when(control.read(any())).thenReturn(CompletableFuture.completedFuture(
          Optional.of(new ControlSnapshot(changed, new byte[] {2}))));
      physical.complete(null);
      cleanup.complete(null);
      var failure = assertThrows(java.util.concurrent.CompletionException.class, result::join);
      assertEquals(BackendFailure.Category.UNAVAILABLE, assertInstanceOf(BackendFailure.class, failure.getCause()).category());
      verify(checkpoints, never()).advance(any(), any(), any());
    }
  }

  private void collectWithDrain(CompletableFuture<Void> physical, CompletableFuture<Void> cleanup) {
    selected();
    var permit = permit();
    when(coordinator.authorizeSystemRecords(any(), same(retention), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(permit));
    doAnswer(call -> CompletableFuture.completedFuture(scanningLease(call.getArgument(0))))
        .when(checkpoints).claim(any(), any());
    when(closure.collect(any(), anyInt(), any(), anyInt(), same(permit), any())).thenAnswer(call -> {
      RequestScope page = call.getArgument(5);
      page.trackPhysicalProducer(physical);
      page.registerScopedCleanup(() -> {}, cleanup);
      return CompletableFuture.completedFuture(new SystemRecordClosure.Page(call.getArgument(0),
          0, 0, Optional.empty(), true, 0));
    });
    when(checkpoints.advance(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(true));
  }

  @Test
  void failedFencePageCannotAdvanceOrStartRetirement() throws Exception {
    selected();
    var permit = permit();
    when(coordinator.authorizeSystemRecords(any(), same(retention), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(permit));
    when(checkpoints.claim(any(), any())).thenAnswer(call -> {
      GcCheckpoint checkpoint = call.getArgument(0);
      return CompletableFuture.completedFuture(new GcCheckpointStore.Held(checkpoint, checkpoint.encode()));
    });
    when(closure.fence(any(), anyInt(), any(), anyInt(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN)));
    var scope = scope();
    assertThrows(java.util.concurrent.CompletionException.class, () -> cycle(budgets(256L << 20)).run(scope).join());
    scope.close();
    scope.cleanupOutcomeCompletion().toCompletableFuture().join();
    verify(checkpoints, never()).advance(any(), any(), any());
    verify(closure, never()).collect(any(), anyInt(), any(), anyInt(), any(), any());
  }

  @Test
  void metadataLeaseRunsOnlyTheFencePhaseBeforeCollection() throws Exception {
    selected();
    var permit = permit();
    when(coordinator.authorizeSystemRecords(any(), same(retention), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(permit));
    when(checkpoints.claim(any(), any())).thenAnswer(call -> {
      GcCheckpoint checkpoint = call.getArgument(0);
      assertEquals(GcCheckpoint.Phase.FENCING, checkpoint.phase());
      return CompletableFuture.completedFuture(new GcCheckpointStore.Held(checkpoint, checkpoint.encode()));
    });
    when(closure.fence(any(), anyInt(), any(), anyInt(), same(permit), any())).thenReturn(
        CompletableFuture.completedFuture(new SystemRecordClosure.Page(SystemRecordClosure.Domain.INVOCATION,
            0, 0, Optional.empty(), true, 0)));
    when(checkpoints.advance(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(true));
    var scope = scope();
    assertEquals(GcCheckpointWorker.Outcome.PROGRESSED, cycle(budgets(256L << 20)).run(scope).join());
    scope.close();
    scope.cleanupOutcomeCompletion().toCompletableFuture().join();
    verify(closure).fence(eq(SystemRecordClosure.Domain.INVOCATION), eq(0), eq(Optional.empty()), eq(1), same(permit), argThat(child -> child != scope && child.isClosed()));
    verify(closure, never()).collect(any(), anyInt(), any(), anyInt(), any(), any());
  }

  @Test
  void resumedSweepUsesOnlyANarrowerDerivativeOfTheFreshAuthority() throws Exception {
    selected();
    var permit = permit();
    var narrowed = mock(SystemRecordRetirement.Permit.class);
    when(permit.restrictTo(7, 8)).thenReturn(narrowed);
    when(coordinator.authorizeSystemRecords(any(), same(retention), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(permit));
    when(checkpoints.claim(any(), any())).thenAnswer(call -> {
      GcCheckpoint proposed = call.getArgument(0);
      var held = new GcCheckpoint(proposed.tableGroupId(), proposed.domain(), proposed.maintenanceEpoch(),
          proposed.bucket(), proposed.bucketCount(), new GcCheckpoint.Plan(permit.operationId(), 4, 8, 7, proposed.plan().registries()),
          new GcCheckpoint.Position(Optional.of(ByteKey.copyOf(new byte[] {6}))), GcCheckpoint.Position.start(),
          GcCheckpoint.Phase.SCANNING, 2, 0, false, 0, GcCheckpoint.Completion.none());
      return CompletableFuture.completedFuture(new GcCheckpointStore.Held(held, held.encode()));
    });
    when(closure.collect(any(), anyInt(), any(), anyInt(), same(narrowed), any())).thenReturn(
        CompletableFuture.completedFuture(new SystemRecordClosure.Page(SystemRecordClosure.Domain.INVOCATION,
            0, 0, Optional.empty(), true, 0)));
    when(checkpoints.advance(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(true));
    var scope = scope();
    cycle(budgets(256L << 20)).run(scope).join();
    scope.close();
    scope.cleanupOutcomeCompletion().toCompletableFuture().join();
    verify(permit).restrictTo(7, 8);
    verify(closure).collect(eq(SystemRecordClosure.Domain.INVOCATION), eq(0),
        eq(Optional.of(ByteKey.copyOf(new byte[] {6}))), eq(1), same(narrowed), argThat(child -> child != scope && child.isClosed()));
  }

  @Test
  void aBusyBucketYieldsTheNextPageToTheNextShard() throws Exception {
    selected();
    var permit = permit();
    when(coordinator.authorizeSystemRecords(any(), same(retention), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(permit));
    when(checkpoints.claim(any(), any())).thenAnswer(call -> {
      GcCheckpoint checkpoint = call.getArgument(0);
      return CompletableFuture.completedFuture(scanningLease(checkpoint));
    });
    var visited = new java.util.ArrayList<Integer>();
    when(closure.collect(any(), anyInt(), any(), anyInt(), any(), any())).thenAnswer(call -> {
      visited.add(call.getArgument(1));
      return CompletableFuture.completedFuture(new SystemRecordClosure.Page(call.getArgument(0), 1, 0,
          Optional.of(ByteKey.copyOf(new byte[] {7})), false, 0));
    });
    when(checkpoints.advance(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(true));
    var cycle = cycle(budgets(256L << 20));
    for (int attempt = 0; attempt < 2; attempt++) {
      var scope = scope();
      cycle.run(scope).join();
      scope.close();
      scope.cleanupOutcomeCompletion().toCompletableFuture().join();
    }
    assertEquals(java.util.List.of(0, 1), visited, "a hot shard cannot starve the other buckets");
  }

  @Test
  void nonOwnerNeverAdmitsOrTouchesStorage() throws Exception {
    owner.set(Optional.empty());
    try (var scope = scope()) {
      assertEquals(GcCheckpointWorker.Outcome.IDLE, cycle(budgets(256L << 20)).run(scope).join());
    }
    verifyNoInteractions(control, coordinator, checkpoints, closure);
  }

  @Test
  void absentCompactWaitsWithoutCallingAuthorityOrCreatingCheckpoints() throws Exception {
    var state = CommitState.initial(ByteKey.copyOf(new byte[32]), 1);
    when(control.read(any())).thenReturn(CompletableFuture.completedFuture(
        Optional.of(new ControlSnapshot(state, new byte[] {1}))));
    var budgets = budgets(256L << 20);
    var scope = scope();
    assertEquals(GcCheckpointWorker.Outcome.IDLE, cycle(budgets).run(scope).join());
    scope.close();
    scope.cleanupOutcomeCompletion().toCompletableFuture().join();
    assertEquals(0, budgets.snapshot().requests());
    verifyNoInteractions(coordinator, checkpoints, closure);
  }

  @Test
  void regularAdmissionFailureYieldsAndRecordsGcBacklogWithoutUsingReserve() throws Exception {
    var budgets = budgets(1);
    var scope = scope();
    assertEquals(GcCheckpointWorker.Outcome.BACKLOG, cycle(budgets).run(scope).join());
    scope.close();
    scope.cleanupOutcomeCompletion().toCompletableFuture().join();
    assertEquals(1, metrics.gc().backlog());
    assertEquals(0, budgets.snapshot().requests());
    verifyNoInteractions(control, coordinator, checkpoints, closure);
  }

  @Test
  void authorizationFailureCannotCreateOrAdvanceACheckpoint() throws Exception {
    selected();
    when(coordinator.authorizeSystemRecords(any(), same(retention), any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION)));
    var scope = scope();
    assertThrows(java.util.concurrent.CompletionException.class, () -> cycle(budgets(256L << 20)).run(scope).join());
    scope.close();
    scope.cleanupOutcomeCompletion().toCompletableFuture().join();
    verifyNoInteractions(checkpoints, closure);
  }

  @Test
  void ownerLossAfterCollectionCannotAdvanceDurableProgressAndAdmissionWaitsForProducer() throws Exception {
    selected();
    var permit = permit();
    when(coordinator.authorizeSystemRecords(any(), same(retention), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(permit));
    when(checkpoints.claim(any(), any())).thenAnswer(call -> {
      GcCheckpoint checkpoint = call.getArgument(0);
      assertEquals(GcCheckpoint.Plan.from(permit), checkpoint.plan());
      return CompletableFuture.completedFuture(scanningLease(checkpoint));
    });
    var physical = new CompletableFuture<Void>();
    when(closure.collect(eq(SystemRecordClosure.Domain.INVOCATION), eq(0), eq(Optional.empty()), eq(1),
        same(permit), any())).thenAnswer(call -> {
          RequestScope scope = call.getArgument(5);
          scope.startIfOpen(() -> scope.trackPhysicalProducer(physical));
          owner.set(Optional.empty());
          return CompletableFuture.completedFuture(new SystemRecordClosure.Page(SystemRecordClosure.Domain.INVOCATION,
              1, 0, Optional.of(ByteKey.copyOf(new byte[] {7})), true, 0));
        });
    var budgets = budgets(256L << 20);
    var scope = scope();
    var result = cycle(budgets).run(scope);
    assertFalse(result.isDone());
    assertEquals(1, budgets.snapshot().requests(), "cancelled GC still owns physical capacity");
    verify(checkpoints, never()).advance(any(), any(), any());
    physical.complete(null);
    assertThrows(java.util.concurrent.CompletionException.class, result::join);
    scope.close();
    scope.cleanupOutcomeCompletion().toCompletableFuture().join();
    assertEquals(0, budgets.snapshot().requests());
    verify(checkpoints, never()).advance(any(), any(), any());
  }

  private static SystemRecordRetirement.Permit permit() {
    var permit = mock(SystemRecordRetirement.Permit.class);
    when(permit.operationId()).thenReturn(OPERATION);
    when(permit.profile()).thenReturn(PROFILE);
    when(permit.registryIdentity()).thenReturn(ByteKey.copyOf(new byte[16]));
    var authority = mock(SystemRecordRetirement.Authority.class);
    when(authority.eventRegistryIdentity()).thenReturn(ByteKey.copyOf(new byte[16]));
    when(authority.eventNamespace()).thenReturn(ByteKey.copyOf(new byte[16]));
    when(permit.authority()).thenReturn(authority);
    when(permit.generation()).thenReturn(4L);
    when(permit.safeSequence()).thenReturn(10L);
    when(permit.safeRevision()).thenReturn(9L);
    return permit;
  }

  private static GcCheckpointStore.Held scanningLease(GcCheckpoint proposal) {
    var active = new GcCheckpoint(proposal.tableGroupId(), proposal.domain(), proposal.maintenanceEpoch(),
        proposal.bucket(), proposal.bucketCount(), proposal.plan(), GcCheckpoint.Position.start(),
        GcCheckpoint.Position.start(), GcCheckpoint.Phase.SCANNING, 1, 0, false, 0, GcCheckpoint.Completion.none());
    return new GcCheckpointStore.Held(active, active.encode());
  }

  private GcCheckpointCycle cycle(BudgetManager budgets) {
    return new GcCheckpointCycle("gc-group", owner::get, control, coordinator, retention,
        ByteKey.copyOf(new byte[32]), closure, checkpoints, budgets, GcCheckpointCycle.MEMORY, 2, 1, metrics);
  }

  private void selected() {
    var state = mock(CommitState.class);
    when(state.compactOperationId()).thenReturn(Optional.of(OPERATION));
    when(state.ownerEpoch()).thenReturn(OWNER.epoch());
    when(state.ownerInstance()).thenReturn(Optional.of(OWNER.instance()));
    when(state.profileFingerprint()).thenReturn(PROFILE);
    when(state.publication()).thenReturn(Optional.empty());
    when(control.read(any())).thenReturn(CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1}))));
  }
  private static RequestScope scope() { return RequestScope.withTimeout(Duration.ofSeconds(5)); }
  private static BudgetManager budgets(long bytes) {
    var limits = new BudgetManager.Limits(4, 0, 4, 4, 4, 0, bytes);
    return new BudgetManager(limits, Map.of("gc-group", limits), limits);
  }
}
