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
  private static final CommitOwnerService.OwnerToken OWNER = new CommitOwnerService.OwnerToken("owner", 3);
  private final AtomicReference<Optional<CommitOwnerService.OwnerToken>> owner = new AtomicReference<>(Optional.of(OWNER));
  private final CommitControlStore control = mock(CommitControlStore.class);
  private final GcCoordinator coordinator = mock(GcCoordinator.class);
  private final EventHistoryRetention retention = mock(EventHistoryRetention.class);
  private final SystemRecordClosure closure = mock(SystemRecordClosure.class);
  private final GcCheckpointStore checkpoints = mock(GcCheckpointStore.class);
  private final RequestMetrics metrics = new RequestMetrics();

  @Test
  void nonOwnerNeverAdmitsOrTouchesStorage() throws Exception {
    owner.set(Optional.empty());
    try (var scope = scope()) {
      assertEquals(GcCheckpointCycle.Outcome.IDLE, cycle(budgets(256L << 20)).run(scope).join());
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
    assertEquals(GcCheckpointCycle.Outcome.IDLE, cycle(budgets).run(scope).join());
    scope.close();
    scope.cleanupOutcomeCompletion().toCompletableFuture().join();
    assertEquals(0, budgets.snapshot().requests());
    verifyNoInteractions(coordinator, checkpoints, closure);
  }

  @Test
  void regularAdmissionFailureYieldsAndRecordsGcBacklogWithoutUsingReserve() throws Exception {
    var budgets = budgets(1);
    var scope = scope();
    assertEquals(GcCheckpointCycle.Outcome.BACKLOG, cycle(budgets).run(scope).join());
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
    var permit = mock(SystemRecordRetirement.Permit.class);
    when(permit.operationId()).thenReturn(InvocationId.create());
    when(permit.generation()).thenReturn(4L);
    when(permit.safeSequence()).thenReturn(10L);
    when(permit.safeRevision()).thenReturn(9L);
    when(coordinator.authorizeSystemRecords(any(), same(retention), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(permit));
    when(checkpoints.claim(any(), any())).thenAnswer(call -> {
      GcCheckpoint checkpoint = call.getArgument(0);
      assertEquals(GcCheckpoint.Plan.from(permit), checkpoint.plan());
      return CompletableFuture.completedFuture(new GcCheckpointStore.Acquired(checkpoint, checkpoint.encode()));
    });
    var physical = new CompletableFuture<Void>();
    when(closure.collect(eq(SystemRecordClosure.Domain.INVOCATION), eq(0), eq(Optional.empty()), eq(1),
        same(permit), any())).thenAnswer(call -> {
          RequestScope scope = call.getArgument(5);
          scope.startIfOpen(() -> scope.trackPhysicalProducer(physical));
          owner.set(Optional.empty());
          return CompletableFuture.completedFuture(new SystemRecordClosure.Page(SystemRecordClosure.Domain.INVOCATION,
              1, 0, Optional.of(ByteKey.copyOf(new byte[] {7})), true));
        });
    var budgets = budgets(256L << 20);
    var scope = scope();
    assertThrows(java.util.concurrent.CompletionException.class, () -> cycle(budgets).run(scope).join());
    scope.close();
    assertEquals(1, budgets.snapshot().requests(), "cancelled GC still owns physical capacity");
    physical.complete(null);
    scope.cleanupOutcomeCompletion().toCompletableFuture().join();
    assertEquals(0, budgets.snapshot().requests());
    verify(checkpoints, never()).advance(any(), any(), any());
  }

  private GcCheckpointCycle cycle(BudgetManager budgets) {
    return new GcCheckpointCycle("gc-group", owner::get, control, coordinator, retention,
        ByteKey.copyOf(new byte[32]), closure, checkpoints, budgets, GcCheckpointCycle.MEMORY, 2, 1, metrics);
  }

  private void selected() {
    var state = mock(CommitState.class);
    when(state.compactOperationId()).thenReturn(Optional.of(InvocationId.create()));
    when(state.publication()).thenReturn(Optional.empty());
    when(control.read(any())).thenReturn(CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1}))));
  }
  private static RequestScope scope() { return RequestScope.withTimeout(Duration.ofSeconds(5)); }
  private static BudgetManager budgets(long bytes) {
    var limits = new BudgetManager.Limits(4, 0, 4, 4, 4, 0, bytes);
    return new BudgetManager(limits, Map.of("gc-group", limits), limits);
  }
}
