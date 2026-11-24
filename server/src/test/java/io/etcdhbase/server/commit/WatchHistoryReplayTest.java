package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventReference;
import io.etcdhbase.core.commit.EventRevisionIndex;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.ProtectedEventCursor;
import io.etcdhbase.core.commit.SelectedSequenceReader;
import io.etcdhbase.core.commit.SharedEventTailer;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.server.observability.RequestMetrics;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class WatchHistoryReplayTest {
  private static final String TABLE_GROUP = "watch-history-test";
  private static final ByteKey AUTHORIZATION = ByteKey.copyOf(new byte[32]);
  private static final int RESPONSE_BYTES = 1 << 20;

  @Test
  void closeAttemptsRemainingDeliveryReleasesAndPropagatesAggregatedFailures() throws Exception {
    var fixture = new ReplayFixture();
    var firstFailure = new AssertionError("first delivery release failed");
    var secondFailure = new IllegalStateException("second delivery release failed");
    var thirdReleased = new AtomicBoolean();
    var deliveries = List.of(
        delivery(() -> { throw firstFailure; }),
        delivery(() -> { throw secondFailure; }),
        delivery(() -> thirdReleased.set(true)));

    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      WatchHistoryReplay replay = fixture.openReplay(watchAccount, KeyRange.all(), 3);
      ArrayDeque<WatchStream.Delivery> pending = pendingDeliveries(replay);
      pending.addAll(deliveries);

      replay.close();
      CompletionException failure = assertThrows(CompletionException.class,
          () -> replay.settled().toCompletableFuture().join());
      AssertionError actual = (AssertionError) failure.getCause();

      assertSame(firstFailure, actual);
      assertEquals(1, actual.getSuppressed().length);
      assertSame(secondFailure, actual.getSuppressed()[0]);
      assertTrue(thirdReleased.get(), "later delivery release must run after an Error");
      assertTrue(pending.isEmpty(), "pending deliveries must be cleared after release attempts");
      verify(fixture.pin).close();
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void resourceLimitCancelsOnlyReplayWhenCleanupSucceeds() throws Exception {
    var fixture = new ReplayFixture();
    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      WatchHistoryReplay replay = fixture.openReplay(watchAccount, KeyRange.all(), 3);
      exceedReplayPageLimit(replay);

      assertTrue(replay.poll().isEmpty());
      assertEquals(Optional.of(WatchTermination.resourceLimit(
          "etcd-hbase: Watch history replay resource limit exceeded")), replay.termination());
      verify(fixture.pin).close();
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void resourceLimitCleanupFailurePropagatesThroughReplayPoll() throws Exception {
    var fixture = new ReplayFixture();
    var cleanupFailure = new IllegalStateException("history pin release failed");
    doThrow(cleanupFailure).when(fixture.pin).close();

    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      WatchHistoryReplay replay = fixture.openReplay(watchAccount, KeyRange.all(), 3);
      exceedReplayPageLimit(replay);

      assertTrue(replay.poll().isEmpty());
      assertEquals(Optional.of(WatchTermination.resourceLimit(
          "etcd-hbase: Watch history replay resource limit exceeded")), replay.termination());
      CompletionException settledFailure = assertThrows(CompletionException.class,
          () -> replay.settled().toCompletableFuture().join());
      assertSame(cleanupFailure, settledFailure.getCause());
      verify(fixture.pin).close();
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void replaysInclusiveStartInRevisionAndOrdinalOrder() throws Exception {
    var fixture = new ReplayFixture();
    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      WatchHistoryReplay replay = fixture.openReplay(watchAccount, KeyRange.all(), 3);

      List<Position> actual = drainReplay(replay);

      assertEquals(List.of(
          new Position(4, 3, 0), new Position(4, 3, 1),
          new Position(6, 4, 0), new Position(6, 4, 1)), actual);
      assertTrue(replay.complete());
      verify(fixture.pin).close();
      replay.close();
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void clearingWakeupKeepsReplayReadableWithoutCallingTheClosedWatcher() throws Exception {
    var fixture = new ReplayFixture();
    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      WatchHistoryReplay replay = fixture.openReplay(watchAccount, KeyRange.all(), 3);
      var wakeups = new AtomicInteger();

      replay.setWakeup(wakeups::incrementAndGet);
      replay.setWakeup(null);

      assertEquals(List.of(
          new Position(4, 3, 0), new Position(4, 3, 1),
          new Position(6, 4, 0), new Position(6, 4, 1)), drainReplay(replay));
      assertEquals(0, wakeups.get(), "clearing the listener prevents callbacks after Watch close");
      replay.close();
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void replayToLiveHandoffDropsTheWholeBarrierSequenceWithoutGapsOrDuplicates()
      throws Exception {
    var fixture = new ReplayFixture();
    try (var startup = RequestScope.withoutDeadline(); var streamScope = RequestScope.withoutDeadline()) {
      var control = mock(CommitControlStore.class);
      when(control.read(any(RequestScope.class))).thenReturn(
          CompletableFuture.completedFuture(Optional.of(fixture.snapshot)));
      var pending = new M3TableGroupRuntime.Pending(startup, null, true);
      var gateway = new WatchGateway(TABLE_GROUP, pending, control, fixture.budgets,
          Runnable::run, mock(ScheduledExecutorService.class), 11, 12, RESPONSE_BYTES,
          fixture.pins, fixture.selected, AUTHORIZATION);
      SharedEventTailer tailer = mock(SharedEventTailer.class);
      SharedEventTailer.Subscription live = mock(SharedEventTailer.Subscription.class);
      when(tailer.isClosed()).thenReturn(false);
      var queued = new ArrayDeque<SharedEventTailer.Delivery>();
      SharedEventTailer.Delivery barrierFirst = liveDelivery(fixture.events.get(6L).get(0), 6);
      SharedEventTailer.Delivery barrierSecond = liveDelivery(fixture.events.get(6L).get(1), 6);
      SharedEventTailer.Delivery afterBarrier = liveDelivery(fixture.events.get(7L).get(0), 7);
      queued.add(barrierFirst);
      queued.add(barrierSecond);
      queued.add(afterBarrier);
      when(live.poll()).thenAnswer(ignored -> Optional.ofNullable(queued.pollFirst()));
      when(live.termination()).thenReturn(Optional.empty());
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(live, 0, 1));

      Rpc.WatchCreateRequest request = Rpc.WatchCreateRequest.newBuilder()
          .setStartRevision(3).build();
      WatchStream.OpenedWatch opened = tailerSession(gateway, tailer, streamScope)
          .subscribe(KeyRange.all(), request).toCompletableFuture().join();
      assertTrue(opened.historicalReplaySupported());

      List<Position> actual = new ArrayList<>();
      for (int attempt = 0; attempt < 16 && actual.size() < 5; attempt++) {
        Optional<WatchStream.Delivery> next = opened.subscription().poll();
        if (next.isEmpty()) continue;
        try (WatchStream.Delivery delivery = next.orElseThrow()) {
          actual.add(new Position(delivery.sequence(), delivery.revision(), delivery.ordinal()));
        }
      }

      assertEquals(List.of(
          new Position(4, 3, 0), new Position(4, 3, 1),
          new Position(6, 4, 0), new Position(6, 4, 1),
          new Position(7, 5, 0)), actual);
      verify(barrierFirst).close();
      verify(barrierSecond).close();
      verify(afterBarrier).close();
      opened.subscription().close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void compactAdvancingDuringReplayRegistrationReturnsPerWatchCancellationBoundary()
      throws Exception {
    var fixture = new ReplayFixture();
    CommitState compacted = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION,
        ByteKey.copyOf(new byte[32]), Optional.empty(), 0, 0, 2, 8, 6, 8, 6,
        Optional.empty(), 4);
    when(fixture.selected.readPublishedState(any(RequestScope.class),
        any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(CompletableFuture.completedFuture(compacted));

    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      WatchHistoryReplay.OpenResult result = WatchHistoryReplay.open(TABLE_GROUP, KeyRange.all(), 3,
          fixture.snapshot, 6, AUTHORIZATION, fixture.pins, fixture.selected,
          fixture.budgets, watchAccount).join();

      assertEquals(6, result.revision());
      assertEquals(8, result.sequence());
      assertEquals(4, result.compactRevision());
      assertTrue(result.replay().isEmpty(), "a compacted registration has no replay cursor");
      verify(fixture.pin).close();
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void compactDuringInitialIndexLookupReturnsCancellationBoundaryInsteadOfLookupFailure()
      throws Exception {
    var fixture = new ReplayFixture();
    var locatedSequence = new CompletableFuture<Long>();
    CommitState compacted = compactedState(fixture.state, 4);
    when(fixture.selected.sequenceForRevision(anyLong(), any(CommitState.class),
        any(RequestScope.class), any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(locatedSequence);
    when(fixture.selected.readPublishedState(any(RequestScope.class),
        any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(CompletableFuture.completedFuture(compacted));

    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      CompletableFuture<WatchHistoryReplay.OpenResult> opening = WatchHistoryReplay.open(
          TABLE_GROUP, KeyRange.all(), 3, fixture.snapshot, 6, AUTHORIZATION, fixture.pins,
          fixture.selected, fixture.budgets, watchAccount);

      assertFalse(opening.isDone(), "the first revision index read is held at the race barrier");
      locatedSequence.completeExceptionally(
          new SelectedSequenceReader.MissingFinalizedRevisionIndex());

      WatchHistoryReplay.OpenResult result = opening.join();
      assertEquals(4, result.compactRevision());
      assertTrue(result.replay().isEmpty(), "compacted history has no replay cursor");
      verify(fixture.pins, org.mockito.Mockito.never()).registerProtection(any(), any());
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void reportsLookupAccountAdmissionResourceLimitWithoutChangingWatchFailure() throws Exception {
    var fixture = new ReplayFixture();
    var reservedScopes = new ArrayList<RequestScope>();
    var stages = new ArrayList<RequestMetrics.WatchReplayOpenResourceFailureStage>();
    var metrics = new RequestMetrics();
    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      try {
        for (int index = 0; index < 4; index++) {
          RequestScope reservedScope = RequestScope.withoutDeadline();
          reservedScopes.add(reservedScope);
          fixture.budgets.admitReserved(TABLE_GROUP, reservedScope,
              new BudgetManager.Reservation(0, 1, 1, 0, 0));
        }

        CompletableFuture<WatchHistoryReplay.OpenResult> opening = WatchHistoryReplay.open(
            TABLE_GROUP, KeyRange.all(), 3, fixture.snapshot, 6, AUTHORIZATION, fixture.pins,
            fixture.selected, fixture.budgets, watchAccount, null, stages::add,
            metrics::recordWatchReplayLookupAdmissionFailure);
        CompletionException failure = assertThrows(CompletionException.class, opening::join);

        assertTrue(failure.getCause() instanceof BackendFailure backendFailure
            && backendFailure.category() == BackendFailure.Category.RESOURCE_LIMIT);
        assertEquals(List.of(RequestMetrics.WatchReplayOpenResourceFailureStage.LOOKUP_ACCOUNT_ADMISSION),
            stages);
        var admission = metrics.watchReplayLookupAdmissionFailure().orElseThrow();
        assertEquals(4, admission.globalReservedRequests());
        assertEquals(8, admission.globalReservedBytes());
        assertEquals(4, admission.globalRequestCapacity());
        assertEquals(80L << 20, admission.globalReservedByteCapacity());
        assertEquals(4, admission.groupReservedRequests());
        assertEquals(8, admission.groupReservedBytes());
        assertEquals(4, admission.groupRequestCapacity());
        assertEquals(80L << 20, admission.groupReservedByteCapacity());
        assertTrue(admission.attemptedBytes() > 0);
      } finally {
        watchAccount.close();
        reservedScopes.forEach(RequestScope::close);
      }
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void reportsSequenceLookupResourceLimitWithoutChangingWatchFailure() throws Exception {
    var fixture = new ReplayFixture();
    var stages = new ArrayList<RequestMetrics.WatchReplayOpenResourceFailureStage>();
    BackendFailure resourceLimit = new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
    when(fixture.selected.sequenceForRevision(anyLong(), any(CommitState.class),
        any(RequestScope.class), any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(CompletableFuture.failedFuture(resourceLimit));

    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      CompletableFuture<WatchHistoryReplay.OpenResult> opening = WatchHistoryReplay.open(
          TABLE_GROUP, KeyRange.all(), 3, fixture.snapshot, 6, AUTHORIZATION, fixture.pins,
          fixture.selected, fixture.budgets, watchAccount, null, stages::add);
      CompletionException failure = assertThrows(CompletionException.class, opening::join);

      assertSame(resourceLimit, failure.getCause());
      assertEquals(List.of(RequestMetrics.WatchReplayOpenResourceFailureStage.SEQUENCE_LOOKUP),
          stages);
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void reportsSynchronousSequenceLookupResourceLimitWithoutChangingWatchFailure() throws Exception {
    var fixture = new ReplayFixture();
    var stages = new ArrayList<RequestMetrics.WatchReplayOpenResourceFailureStage>();
    BackendFailure resourceLimit = new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
    doThrow(new CompletionException(resourceLimit)).when(fixture.selected).sequenceForRevision(anyLong(),
        any(CommitState.class), any(RequestScope.class),
        any(ManifestCatalog.ProofReadBudget.class));

    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      CompletableFuture<WatchHistoryReplay.OpenResult> opening = WatchHistoryReplay.open(
          TABLE_GROUP, KeyRange.all(), 3, fixture.snapshot, 6, AUTHORIZATION, fixture.pins,
          fixture.selected, fixture.budgets, watchAccount, null, stages::add);
      CompletionException failure = assertThrows(CompletionException.class, opening::join);

      assertSame(resourceLimit, failure.getCause());
      assertEquals(List.of(RequestMetrics.WatchReplayOpenResourceFailureStage.SEQUENCE_LOOKUP),
          stages);
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void reportsPinRenewalResourceLimitWithoutChangingWatchFailure() throws Exception {
    var fixture = new ReplayFixture();
    var stages = new ArrayList<RequestMetrics.WatchReplayOpenResourceFailureStage>();
    BackendFailure resourceLimit = new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
    when(fixture.protection.renew(any(RequestScope.class)))
        .thenReturn(CompletableFuture.failedFuture(resourceLimit));

    assertOpenResourceFailure(fixture, resourceLimit,
        RequestMetrics.WatchReplayOpenResourceFailureStage.PIN_RENEWAL, stages);
  }

  @Test
  void reportsControlRevalidationResourceLimitWithoutChangingWatchFailure() throws Exception {
    var fixture = new ReplayFixture();
    var stages = new ArrayList<RequestMetrics.WatchReplayOpenResourceFailureStage>();
    BackendFailure resourceLimit = new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
    when(fixture.selected.readPublishedState(any(RequestScope.class),
        any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(CompletableFuture.failedFuture(resourceLimit));

    assertOpenResourceFailure(fixture, resourceLimit,
        RequestMetrics.WatchReplayOpenResourceFailureStage.CONTROL_REVALIDATION_READ, stages);
  }

  @Test
  void reportsSequenceRevalidationResourceLimitWithoutChangingWatchFailure() throws Exception {
    var fixture = new ReplayFixture();
    var stages = new ArrayList<RequestMetrics.WatchReplayOpenResourceFailureStage>();
    BackendFailure resourceLimit = new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
    when(fixture.selected.sequenceForRevision(anyLong(), any(CommitState.class),
        any(RequestScope.class), any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(CompletableFuture.completedFuture(4L), CompletableFuture.failedFuture(resourceLimit));

    assertOpenResourceFailure(fixture, resourceLimit,
        RequestMetrics.WatchReplayOpenResourceFailureStage.SEQUENCE_REVALIDATION_READ, stages);
  }

  @Test
  void reportsControlReadAdmissionResourceLimitSeparatelyFromReadFailure() throws Exception {
    var fixture = new ReplayFixture();
    var stages = new ArrayList<RequestMetrics.WatchReplayOpenResourceFailureStage>();
    var reservedScopes = new ArrayList<RequestScope>();
    when(fixture.pins.registerProtection(any(EventHistoryPinManager.Requirement.class),
        any(RequestScope.class))).thenAnswer(invocation -> {
          for (int index = 0; index < 4; index++) {
            RequestScope reservedScope = RequestScope.withoutDeadline();
            reservedScopes.add(reservedScope);
            fixture.budgets.admitReserved(TABLE_GROUP, reservedScope,
                new BudgetManager.Reservation(0, 1, 1, 0, 0));
          }
          return CompletableFuture.completedFuture(fixture.protection);
        });

    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      try {
        CompletableFuture<WatchHistoryReplay.OpenResult> opening = WatchHistoryReplay.open(
            TABLE_GROUP, KeyRange.all(), 3, fixture.snapshot, 6, AUTHORIZATION, fixture.pins,
            fixture.selected, fixture.budgets, watchAccount, null, stages::add);
        CompletionException failure = assertThrows(CompletionException.class, opening::join);

        assertTrue(failure.getCause() instanceof BackendFailure backendFailure
            && backendFailure.category() == BackendFailure.Category.RESOURCE_LIMIT);
        assertEquals(List.of(
            RequestMetrics.WatchReplayOpenResourceFailureStage.CONTROL_REVALIDATION_ACCOUNT_ADMISSION),
            stages);
      } finally {
        watchAccount.close();
        reservedScopes.forEach(RequestScope::close);
      }
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void registrationFloorRejectionAfterIndexLookupUsesCommittedCompactBoundary()
      throws Exception {
    var fixture = new ReplayFixture();
    CommitState compacted = compactedState(fixture.state, 4);
    when(fixture.pins.registerProtection(any(EventHistoryPinManager.Requirement.class),
        any(RequestScope.class))).thenReturn(CompletableFuture.failedFuture(
            new io.etcdhbase.core.commit.SnapshotPinManager.RegistrationFloorExceeded()));
    when(fixture.selected.readPublishedState(any(RequestScope.class),
        any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(CompletableFuture.completedFuture(compacted));

    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      WatchHistoryReplay.OpenResult result = WatchHistoryReplay.open(
          TABLE_GROUP, KeyRange.all(), 3, fixture.snapshot, 6, AUTHORIZATION, fixture.pins,
          fixture.selected, fixture.budgets, watchAccount).join();

      assertEquals(4, result.compactRevision());
      assertTrue(result.replay().isEmpty(), "the rejected pin cannot authorize a replay");
      verify(fixture.pins).registerProtection(any(EventHistoryPinManager.Requirement.class), any());
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void missingIndexWithoutCommittedCompactionRemainsAStorageFailure() throws Exception {
    var fixture = new ReplayFixture();
    when(fixture.selected.sequenceForRevision(anyLong(), any(CommitState.class),
        any(RequestScope.class), any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(CompletableFuture.failedFuture(
            new SelectedSequenceReader.MissingFinalizedRevisionIndex()));
    when(fixture.selected.readPublishedState(any(RequestScope.class),
        any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(CompletableFuture.completedFuture(fixture.state));

    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      java.util.concurrent.CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
          java.util.concurrent.CompletionException.class,
          () -> WatchHistoryReplay.open(TABLE_GROUP, KeyRange.all(), 3, fixture.snapshot, 6,
              AUTHORIZATION, fixture.pins, fixture.selected, fixture.budgets, watchAccount).join());

      assertTrue(failure.getCause() instanceof SelectedSequenceReader.MissingFinalizedRevisionIndex);
      verify(fixture.pins, org.mockito.Mockito.never()).registerProtection(any(), any());
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void permissionFailureDuringIndexLookupIsNotRetriedOrConvertedToCompaction() throws Exception {
    var fixture = new ReplayFixture();
    BackendFailure permissionFailure = new BackendFailure(BackendFailure.Category.PERMISSION);
    when(fixture.selected.sequenceForRevision(anyLong(), any(CommitState.class),
        any(RequestScope.class), any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(CompletableFuture.failedFuture(permissionFailure));

    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      java.util.concurrent.CompletionException failure = org.junit.jupiter.api.Assertions.assertThrows(
          java.util.concurrent.CompletionException.class,
          () -> WatchHistoryReplay.open(TABLE_GROUP, KeyRange.all(), 3, fixture.snapshot, 6,
              AUTHORIZATION, fixture.pins, fixture.selected, fixture.budgets, watchAccount).join());

      assertSame(permissionFailure, failure.getCause());
      verify(fixture.selected, org.mockito.Mockito.never()).readPublishedState(any(), any());
      verify(fixture.pins, org.mockito.Mockito.never()).registerProtection(any(), any());
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void authenticationAndPermissionFailuresDuringCompactRecheckRemainTyped() throws Exception {
    List<Throwable> openingFailures = List.of(
        new SelectedSequenceReader.MissingFinalizedRevisionIndex(),
        new SnapshotPinManager.RegistrationFloorExceeded());
    List<BackendFailure.Category> checkFailureCategories = List.of(
        BackendFailure.Category.AUTHENTICATION, BackendFailure.Category.PERMISSION);

    for (Throwable openingFailure : openingFailures) {
      for (BackendFailure.Category category : checkFailureCategories) {
        var fixture = new ReplayFixture();
        BackendFailure checkFailure = new BackendFailure(category);
        if (openingFailure instanceof SnapshotPinManager.RegistrationFloorExceeded) {
          when(fixture.pins.registerProtection(
              any(EventHistoryPinManager.Requirement.class), any(RequestScope.class)))
              .thenReturn(CompletableFuture.failedFuture(openingFailure));
        } else {
          when(fixture.selected.sequenceForRevision(anyLong(), any(CommitState.class),
              any(RequestScope.class), any(ManifestCatalog.ProofReadBudget.class)))
              .thenReturn(CompletableFuture.failedFuture(openingFailure));
        }
        when(fixture.selected.readPublishedState(any(RequestScope.class),
            any(ManifestCatalog.ProofReadBudget.class)))
            .thenReturn(CompletableFuture.failedFuture(checkFailure));

        try (var scope = RequestScope.withoutDeadline()) {
          BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
          java.util.concurrent.CompletionException failure =
              org.junit.jupiter.api.Assertions.assertThrows(
                  java.util.concurrent.CompletionException.class,
                  () -> WatchHistoryReplay.open(TABLE_GROUP, KeyRange.all(), 3,
                      fixture.snapshot, 6, AUTHORIZATION, fixture.pins, fixture.selected,
                      fixture.budgets, watchAccount).join());

          assertSame(checkFailure, failure.getCause());
          assertTrue(List.of(checkFailure.getSuppressed()).contains(openingFailure),
              "the original opening failure remains available as suppressed context");
          if (openingFailure instanceof SnapshotPinManager.RegistrationFloorExceeded) {
            verify(fixture.pins).registerProtection(
                any(EventHistoryPinManager.Requirement.class), any());
          } else {
            verify(fixture.pins, org.mockito.Mockito.never()).registerProtection(any(), any());
          }
          watchAccount.close();
          assertNoBudgetLeaks(fixture.budgets);
        }
      }
    }
  }

  @Test
  void compactAdvancingAfterReplayRegistrationPreservesTheBoundary() throws Exception {
    var fixture = new ReplayFixture();
    CommitState compacted = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION,
        ByteKey.copyOf(new byte[32]), Optional.empty(), 0, 0, 2, 8, 6, 8, 6,
        Optional.empty(), 4);
    when(fixture.selected.readPublishedState(any(RequestScope.class),
        any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(CompletableFuture.completedFuture(fixture.state))
        .thenReturn(CompletableFuture.completedFuture(compacted));

    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      WatchHistoryReplay replay = fixture.openReplay(watchAccount, KeyRange.all(), 3);

      assertTrue(replay.poll().isEmpty(), "compaction terminates history instead of yielding events");
      assertEquals(Optional.of(WatchTermination.compacted(4)), replay.termination());
      verify(fixture.pin).close();
      replay.close();
      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void compactBeyondNextRevisionCancelsOnlyAfterCurrentRevisionIsDelivered() throws Exception {
    var fixture = new ReplayFixture();
    var currentState = new AtomicReference<>(fixture.state);
    when(fixture.selected.readPublishedState(any(RequestScope.class),
        any(ManifestCatalog.ProofReadBudget.class)))
        .thenAnswer(invocation -> CompletableFuture.completedFuture(currentState.get()));

    CommitState compacted = CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION,
        ByteKey.copyOf(new byte[32]), Optional.empty(), 0, 0, 2, 7, 5, 7, 5,
        Optional.empty(), 5);
    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      WatchHistoryReplay replay = fixture.openReplay(watchAccount, KeyRange.all(), 3);
      var wakeups = new LinkedBlockingQueue<Boolean>();
      replay.setWakeup(() -> wakeups.offer(Boolean.TRUE));
      try {
        assertTrue(replay.poll().isEmpty(), "the first revision begins an asynchronous read");
        assertTrue(wakeups.poll(5, TimeUnit.SECONDS) != null);
        WatchStream.Delivery delivery = replay.poll().orElseThrow();
        try {
          assertEquals(3, delivery.revision());
          assertEquals(0, delivery.ordinal());
        } finally {
          delivery.close();
        }
        WatchStream.Delivery lastEventInRevision = replay.poll().orElseThrow();
        try {
          assertEquals(3, lastEventInRevision.revision());
          assertEquals(1, lastEventInRevision.ordinal());
        } finally {
          lastEventInRevision.close();
        }

        currentState.set(compacted);
        assertTrue(replay.poll().isEmpty(), "the next revision is rejected before its events are read");
        assertTrue(wakeups.poll(5, TimeUnit.SECONDS) != null);
        assertEquals(Optional.of(WatchTermination.compacted(5)), replay.termination());
        replay.settled().toCompletableFuture().get(5, TimeUnit.SECONDS);
      } finally {
        replay.close();
        watchAccount.close();
      }
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void compactDuringInitialIndexLookupSendsCreatedThenCanceledWithTheBoundary() throws Exception {
    var fixture = new ReplayFixture();
    CommitState compacted = compactedState(fixture.state, 4);
    when(fixture.selected.readPublishedState(any(RequestScope.class),
        any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(CompletableFuture.completedFuture(compacted));
    when(fixture.selected.sequenceForRevision(anyLong(), any(CommitState.class),
        any(RequestScope.class), any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(CompletableFuture.failedFuture(
            new SelectedSequenceReader.MissingFinalizedRevisionIndex()));

    try (var startup = RequestScope.withoutDeadline();
         var streamScope = RequestScope.withoutDeadline()) {
      var control = mock(CommitControlStore.class);
      when(control.read(any(RequestScope.class))).thenReturn(
          CompletableFuture.completedFuture(Optional.of(fixture.snapshot)));
      var pending = new M3TableGroupRuntime.Pending(startup, null, true);
      var gateway = new WatchGateway(TABLE_GROUP, pending, control, fixture.budgets,
          Runnable::run, mock(ScheduledExecutorService.class), 11, 12, RESPONSE_BYTES,
          fixture.pins, fixture.selected, AUTHORIZATION);
      SharedEventTailer tailer = mock(SharedEventTailer.class);
      SharedEventTailer.Subscription live = mock(SharedEventTailer.Subscription.class);
      when(tailer.isClosed()).thenReturn(false);
      when(tailer.completedRevision()).thenReturn(fixture.state.publishedRevision());
      when(tailer.completedSequence()).thenReturn(fixture.state.publishedSequence());
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(live, 0, 1));
      when(live.poll()).thenReturn(Optional.empty());
      when(live.termination()).thenReturn(Optional.empty());

      var responses = new ArrayList<Rpc.WatchResponse>();
      var transportFailure = new AtomicReference<Throwable>();
      var observer = new io.grpc.stub.StreamObserver<Rpc.WatchResponse>() {
        @Override public void onNext(Rpc.WatchResponse response) { responses.add(response); }
        @Override public void onError(Throwable failure) { transportFailure.set(failure); }
        @Override public void onCompleted() { }
      };
      WatchStream.Session session = tailerSession(gateway, tailer, streamScope);
      var stream = new WatchStream(observer, streamScope, Runnable::run,
          mock(ScheduledExecutorService.class),
          () -> CompletableFuture.completedFuture(session), 11, 12, RESPONSE_BYTES);
      Rpc.WatchCreateRequest create = Rpc.WatchCreateRequest.newBuilder()
          .setWatchId(17).setKey(com.google.protobuf.ByteString.copyFrom(new byte[] {1}))
          .setStartRevision(3).build();
      stream.onNext(Rpc.WatchRequest.newBuilder().setCreateRequest(create).build());

      assertEquals(2, responses.size());
      assertTrue(responses.get(0).getCreated());
      Rpc.WatchResponse cancelled = responses.get(1);
      assertTrue(cancelled.getCanceled());
      assertEquals(17, cancelled.getWatchId());
      assertEquals(4, cancelled.getCompactRevision());
      assertEquals("", cancelled.getCancelReason());
      assertEquals(null, transportFailure.get());
      verify(fixture.pins, org.mockito.Mockito.never()).registerProtection(any(), any());
      verify(live).close();
      stream.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void cancelDuringHistoricalCreateClosesTailerAndWaitsForReplayOpenToSettle() throws Exception {
    var fixture = new ReplayFixture();
    var locatedSequence = new CompletableFuture<Long>();
    when(fixture.selected.sequenceForRevision(anyLong(), any(CommitState.class),
        any(RequestScope.class), any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(locatedSequence);

    try (var startup = RequestScope.withoutDeadline();
         var streamScope = RequestScope.withoutDeadline()) {
      var control = mock(CommitControlStore.class);
      when(control.read(any(RequestScope.class))).thenReturn(
          CompletableFuture.completedFuture(Optional.of(fixture.snapshot)));
      var pending = new M3TableGroupRuntime.Pending(startup, null, true);
      var gateway = new WatchGateway(TABLE_GROUP, pending, control, fixture.budgets,
          Runnable::run, mock(ScheduledExecutorService.class), 11, 12, RESPONSE_BYTES,
          fixture.pins, fixture.selected, AUTHORIZATION);
      SharedEventTailer tailer = mock(SharedEventTailer.class);
      SharedEventTailer.Subscription live = mock(SharedEventTailer.Subscription.class);
      when(tailer.isClosed()).thenReturn(false);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(live, 0, 1));
      when(live.termination()).thenReturn(Optional.empty());

      CompletionStage<WatchStream.OpenedWatch> opening = tailerSession(gateway, tailer, streamScope)
          .subscribe(KeyRange.all(), Rpc.WatchCreateRequest.newBuilder()
              .setStartRevision(3).build());

      assertFalse(opening.toCompletableFuture().isDone(), "history lookup is held at a barrier");
      assertTrue(fixture.budgets.snapshot().requests() > 0);
      streamScope.cancel();

      verify(live).close();
      assertFalse(streamScope.cleanupCompletion().toCompletableFuture().isDone(),
          "the stream cleanup waits while the replay lookup is still outstanding");
      locatedSequence.complete(4L);

      org.junit.jupiter.api.Assertions.assertThrows(
          java.util.concurrent.CompletionException.class,
          () -> opening.toCompletableFuture().join());
      streamScope.cleanupCompletion().toCompletableFuture().join();
      org.mockito.Mockito.verify(fixture.pins,
          org.mockito.Mockito.never()).registerProtection(any(), any());
      verify(fixture.selected, org.mockito.Mockito.never()).readPublishedState(any(), any());
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void clientCancelDuringReplayReadReturnsPerWatchCanceledWhenReadBecomesUnavailable()
      throws Exception {
    var fixture = new ReplayFixture();
    var selectedOpen = new CompletableFuture<Optional<SelectedSequenceReader.Selected>>();
    var replayReadStarted = new CompletableFuture<Void>();
    var physicalProducer = new CompletableFuture<Void>();
    when(fixture.selected.openUnderProtection(anyLong(), anyLong(), any(ByteKey.class),
        any(EventHistoryPinManager.Protection.class), any(RequestScope.class),
        any(ManifestCatalog.ProofReadBudget.class))).thenAnswer(invocation -> {
          RequestScope readScope = invocation.getArgument(4);
          readScope.trackPhysicalProducer(physicalProducer);
          replayReadStarted.complete(null);
          return selectedOpen;
        });

    try (var startup = RequestScope.withoutDeadline();
         var streamScope = RequestScope.withoutDeadline()) {
      var control = mock(CommitControlStore.class);
      when(control.read(any(RequestScope.class))).thenReturn(
          CompletableFuture.completedFuture(Optional.of(fixture.snapshot)));
      var pending = new M3TableGroupRuntime.Pending(startup, null, true);
      var gateway = new WatchGateway(TABLE_GROUP, pending, control, fixture.budgets,
          Runnable::run, mock(ScheduledExecutorService.class), 11, 12, RESPONSE_BYTES,
          fixture.pins, fixture.selected, AUTHORIZATION);
      SharedEventTailer tailer = mock(SharedEventTailer.class);
      SharedEventTailer.Subscription live = mock(SharedEventTailer.Subscription.class);
      when(tailer.isClosed()).thenReturn(false);
      when(tailer.completedRevision()).thenReturn(fixture.state.publishedRevision());
      when(tailer.completedSequence()).thenReturn(fixture.state.publishedSequence());
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(live, 0, 1));
      when(live.poll()).thenReturn(Optional.empty());
      when(live.termination()).thenReturn(Optional.empty());
      doAnswer(invocation -> {
        selectedOpen.completeExceptionally(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
        physicalProducer.complete(null);
        return null;
      }).when(live).close();

      var responses = new ArrayList<Rpc.WatchResponse>();
      var transportFailure = new AtomicReference<Throwable>();
      var observer = new io.grpc.stub.StreamObserver<Rpc.WatchResponse>() {
        @Override public void onNext(Rpc.WatchResponse response) { responses.add(response); }
        @Override public void onError(Throwable failure) { transportFailure.set(failure); }
        @Override public void onCompleted() { }
      };
      var worker = new QueuedExecutor();
      WatchStream.Session session = tailerSession(gateway, tailer, streamScope);
      var stream = new WatchStream(observer, streamScope, worker,
          mock(ScheduledExecutorService.class),
          () -> CompletableFuture.completedFuture(session), 11, 12, RESPONSE_BYTES);
      Rpc.WatchCreateRequest create = Rpc.WatchCreateRequest.newBuilder()
          .setWatchId(17).setKey(com.google.protobuf.ByteString.copyFrom(new byte[] {1}))
          .setStartRevision(3).build();
      stream.onNext(Rpc.WatchRequest.newBuilder().setCreateRequest(create).build());
      worker.runUntilIdle();

      assertEquals(1, responses.size());
      assertTrue(responses.getFirst().getCreated());
      replayReadStarted.get(5, TimeUnit.SECONDS);

      stream.onNext(Rpc.WatchRequest.newBuilder().setCancelRequest(
          Rpc.WatchCancelRequest.newBuilder().setWatchId(17)).build());
      worker.runUntilIdle();

      assertEquals(2, responses.size());
      assertTrue(responses.getLast().getCanceled());
      assertEquals(17, responses.getLast().getWatchId());
      assertNull(transportFailure.get(), "canceling one Watch must not fail the stream");
      verify(live).close();
      stream.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void closingReplayWaitsForPhysicalProducerBeforeReleasingItsHistoryPin() throws Exception {
    var fixture = new ReplayFixture();
    var backendRead = new CompletableFuture<CommitState>();
    var physicalProducer = new CompletableFuture<Void>();
    var pinRelease = new CompletableFuture<Void>();
    when(fixture.pin.releaseCompletion()).thenReturn(pinRelease);
    when(fixture.selected.readPublishedState(any(RequestScope.class),
        any(ManifestCatalog.ProofReadBudget.class)))
        .thenReturn(CompletableFuture.completedFuture(fixture.state))
        .thenAnswer(invocation -> {
          RequestScope readScope = invocation.getArgument(0);
          readScope.trackPhysicalProducer(physicalProducer);
          return backendRead;
        });

    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      WatchHistoryReplay replay = fixture.openReplay(watchAccount, KeyRange.all(), 3);
      assertTrue(replay.poll().isEmpty(), "the next replay revision starts an asynchronous read");

      replay.close();
      backendRead.completeExceptionally(new BackendFailure(BackendFailure.Category.CANCELLED));

      assertFalse(replay.settled().toCompletableFuture().isDone());
      org.mockito.Mockito.verify(fixture.pin, org.mockito.Mockito.never()).close();
      physicalProducer.complete(null);
      verify(fixture.pin).close();
      assertFalse(replay.settled().toCompletableFuture().isDone(),
          "replay cleanup remains unsettled until durable pin release completes");
      pinRelease.complete(null);
      replay.settled().toCompletableFuture().join();

      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void cancellingDuringProtectedEventOpenDrainsProducerBeforeReleasingReplayProtection()
      throws Exception {
    var fixture = new ReplayFixture();
    var selectedOpen = new CompletableFuture<Optional<SelectedSequenceReader.Selected>>();
    var physicalProducer = new CompletableFuture<Void>();
    var openStarted = new CompletableFuture<Void>();
    var pinRelease = new CompletableFuture<Void>();
    when(fixture.pin.releaseCompletion()).thenReturn(pinRelease);
    when(fixture.selected.openUnderProtection(anyLong(), anyLong(), any(ByteKey.class),
        any(EventHistoryPinManager.Protection.class), any(RequestScope.class),
        any(ManifestCatalog.ProofReadBudget.class))).thenAnswer(invocation -> {
          RequestScope readScope = invocation.getArgument(4);
          readScope.trackPhysicalProducer(physicalProducer);
          openStarted.complete(null);
          return selectedOpen;
        });

    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      WatchHistoryReplay replay = fixture.openReplay(watchAccount, KeyRange.all(), 3);
      assertTrue(replay.poll().isEmpty(), "the first poll begins protected event lookup");
      openStarted.get(5, TimeUnit.SECONDS);

      replay.close();
      selectedOpen.completeExceptionally(new BackendFailure(BackendFailure.Category.CANCELLED));

      assertFalse(replay.settled().toCompletableFuture().isDone(),
          "the replay remains unsettled while its selected-event producer is draining");
      org.mockito.Mockito.verify(fixture.pin, org.mockito.Mockito.never()).close();

      physicalProducer.complete(null);
      verify(fixture.pin).close();
      assertFalse(replay.settled().toCompletableFuture().isDone(),
          "replay settlement also waits for durable protection release");
      pinRelease.complete(null);
      replay.settled().toCompletableFuture().join();

      watchAccount.close();
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  @Test
  void authenticationAndPermissionFailuresRemainTypedDuringReplay() throws Exception {
    for (BackendFailure.Category category : List.of(
        BackendFailure.Category.AUTHENTICATION,
        BackendFailure.Category.INVALID_AUTH_TOKEN,
        BackendFailure.Category.PERMISSION)) {
      var fixture = new ReplayFixture();
      BackendFailure failure = new BackendFailure(category);
      when(fixture.selected.readPublishedState(any(RequestScope.class),
          any(ManifestCatalog.ProofReadBudget.class)))
          .thenReturn(CompletableFuture.completedFuture(fixture.state))
          .thenReturn(CompletableFuture.failedFuture(failure));

      try (var scope = RequestScope.withoutDeadline()) {
        BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
        WatchHistoryReplay replay = fixture.openReplay(watchAccount, KeyRange.all(), 3);

        assertTrue(replay.poll().isEmpty(), "a rejected replay read does not produce an event");
        WatchTermination termination = replay.termination().orElseThrow();
        assertEquals(WatchTermination.Kind.FAILURE, termination.kind());
        assertSame(failure, termination.failure());
        verify(fixture.pin).close();
        replay.close();
        watchAccount.close();
        assertNoBudgetLeaks(fixture.budgets);
      }
    }
  }

  private static List<Position> drainReplay(WatchHistoryReplay replay) {
    List<Position> actual = new ArrayList<>();
    for (int attempt = 0; attempt < 16 && !replay.complete(); attempt++) {
      Optional<WatchStream.Delivery> next = replay.poll();
      if (next.isEmpty()) {
        assertTrue(replay.termination().isEmpty());
        continue;
      }
      try (WatchStream.Delivery delivery = next.orElseThrow()) {
        actual.add(new Position(delivery.sequence(), delivery.revision(), delivery.ordinal()));
      }
    }
    assertFalse(actual.isEmpty());
    return actual;
  }

  private static SharedEventTailer.Delivery liveDelivery(PreparedEvent event, long sequence) {
    SharedEventTailer.Delivery delivery = mock(SharedEventTailer.Delivery.class);
    when(delivery.event()).thenReturn(event);
    when(delivery.sequence()).thenReturn(sequence);
    when(delivery.revision()).thenReturn(event.current().metadata().modificationRevision());
    return delivery;
  }

  private static WatchStream.Delivery delivery(Runnable release) {
    PreparedEvent event = event(2, 1, 0, 61);
    return new WatchStream.Delivery(1, 2, event, release);
  }

  private static void exceedReplayPageLimit(WatchHistoryReplay replay)
      throws ReflectiveOperationException {
    Field readPagesField = WatchHistoryReplay.class.getDeclaredField("readPages");
    readPagesField.setAccessible(true);
    readPagesField.setInt(replay, Integer.MAX_VALUE);
  }

  @SuppressWarnings("unchecked")
  private static ArrayDeque<WatchStream.Delivery> pendingDeliveries(WatchHistoryReplay replay)
      throws ReflectiveOperationException {
    Field pendingField = WatchHistoryReplay.class.getDeclaredField("pending");
    pendingField.setAccessible(true);
    return (ArrayDeque<WatchStream.Delivery>) pendingField.get(replay);
  }

  private static WatchStream.Session tailerSession(WatchGateway gateway,
      SharedEventTailer tailer, RequestScope scope) {
    try {
      Class<?> sessionType = java.util.Arrays.stream(WatchGateway.class.getDeclaredClasses())
          .filter(type -> type.getSimpleName().equals("TailerSession"))
          .findFirst().orElseThrow();
      Constructor<?> constructor = sessionType.getDeclaredConstructor(
          WatchGateway.class, SharedEventTailer.class, RequestScope.class);
      constructor.setAccessible(true);
      return (WatchStream.Session) constructor.newInstance(gateway, tailer, scope);
    } catch (InvocationTargetException invocation) {
      Throwable cause = invocation.getCause();
      if (cause instanceof RuntimeException runtimeFailure) throw runtimeFailure;
      if (cause instanceof Error fatal) throw fatal;
      throw new AssertionError("could not construct the Watch tailer session", cause);
    } catch (ReflectiveOperationException reflectionFailure) {
      throw new AssertionError("could not invoke the Watch tailer session", reflectionFailure);
    }
  }

  private static void assertNoBudgetLeaks(BudgetManager budgets) {
    BudgetManager.Snapshot snapshot = budgets.snapshot();
    assertEquals(0, snapshot.requests());
    assertEquals(0, snapshot.reservedBytes());
    assertEquals(0, snapshot.allocatedBytes());
  }

  private static void assertOpenResourceFailure(ReplayFixture fixture, BackendFailure expectedFailure,
      RequestMetrics.WatchReplayOpenResourceFailureStage expectedStage,
      List<RequestMetrics.WatchReplayOpenResourceFailureStage> stages) throws Exception {
    try (var scope = RequestScope.withoutDeadline()) {
      BudgetManager.Account watchAccount = fixture.admitWatchAccount(scope);
      try {
        CompletableFuture<WatchHistoryReplay.OpenResult> opening = WatchHistoryReplay.open(
            TABLE_GROUP, KeyRange.all(), 3, fixture.snapshot, 6, AUTHORIZATION, fixture.pins,
            fixture.selected, fixture.budgets, watchAccount, null, stages::add);
        CompletionException failure = assertThrows(CompletionException.class, opening::join);

        assertSame(expectedFailure, failure.getCause());
        assertEquals(List.of(expectedStage), stages);
      } finally {
        watchAccount.close();
      }
      assertNoBudgetLeaks(fixture.budgets);
    }
  }

  private static CommitState compactedState(CommitState current, long compactRevision) {
    return CommitState.restore(current.formatVersion(), current.profileFingerprint(),
        current.ownerInstance(), current.ownerEpoch(), current.ownerLeaseDeadlineEpochMillis(),
        Math.addExact(current.stateVersion(), 1), current.publishedSequence(),
        current.publishedRevision(), current.finalizedSequence(), current.finalizedRevision(),
        current.publication(), compactRevision, current.eventRegistryNamespace(),
        current.pendingEventRegistryNamespace(), current.eventRegistryAttempts());
  }

  private static final class QueuedExecutor implements Executor {
    private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();

    @Override
    public synchronized void execute(Runnable task) {
      tasks.addLast(task);
    }

    private void runUntilIdle() {
      while (true) {
        Runnable task;
        synchronized (this) { task = tasks.pollFirst(); }
        if (task == null) return;
        task.run();
      }
    }
  }

  private record Position(long sequence, long revision, long ordinal) {}

  private static final class ReplayFixture {
    private final EventHistoryPinManager pins = mock(EventHistoryPinManager.class);
    private final EventHistoryPinManager.Pin pin = mock(EventHistoryPinManager.Pin.class);
    private final EventHistoryPinManager.Protection protection =
        mock(EventHistoryPinManager.Protection.class);
    private final SelectedSequenceReader selected = mock(SelectedSequenceReader.class);
    private final BudgetManager budgets = budgets();
    private final Map<Long, Long> sequenceByRevision = Map.of(3L, 4L, 4L, 6L);
    private final Map<Long, List<PreparedEvent>> events = Map.of(
        4L, List.of(event(3, 4, 0, 31), event(3, 4, 1, 32)),
        6L, List.of(event(4, 6, 0, 41), event(4, 6, 1, 42)),
        7L, List.of(event(5, 7, 0, 51)));
    private final Map<Long, SelectedSequenceReader.Selected> selectedBySequence;
    private final CommitState state = CommitState.restore(
        CommitState.EVENT_INDEX_FORMAT_VERSION, ByteKey.copyOf(new byte[32]), Optional.empty(),
        0, 0, 1, 6, 4, 6, 4, Optional.empty(), 0);
    private final ControlSnapshot snapshot = new ControlSnapshot(state, new byte[] {1});

    private ReplayFixture() {
      selectedBySequence = Map.of(
          4L, selected(3, 4, events.get(4L)),
          6L, selected(4, 6, events.get(6L)));
      when(pins.registerProtection(any(EventHistoryPinManager.Requirement.class),
          any(RequestScope.class))).thenReturn(CompletableFuture.completedFuture(protection));
      doAnswer(invocation -> {
        pin.close();
        return null;
      }).when(protection).close();
      when(pin.releaseCompletion()).thenReturn(CompletableFuture.completedFuture(null));
      when(pin.renew(any(RequestScope.class))).thenReturn(CompletableFuture.completedFuture(null));
      when(protection.releaseCompletion()).thenAnswer(invocation -> pin.releaseCompletion());
      when(protection.renew(any(RequestScope.class))).thenAnswer(
          invocation -> pin.renew(invocation.getArgument(0)));
      when(selected.readPublishedState(any(RequestScope.class),
          any(ManifestCatalog.ProofReadBudget.class)))
          .thenReturn(CompletableFuture.completedFuture(state));
      when(selected.sequenceForRevision(anyLong(), any(CommitState.class),
          any(RequestScope.class), any(ManifestCatalog.ProofReadBudget.class)))
          .thenAnswer(invocation -> CompletableFuture.completedFuture(
              sequenceByRevision.get(invocation.<Long>getArgument(0))));
      when(selected.openUnderProtection(anyLong(), anyLong(), any(ByteKey.class),
          any(EventHistoryPinManager.Protection.class), any(RequestScope.class),
          any(ManifestCatalog.ProofReadBudget.class)))
          .thenAnswer(invocation -> CompletableFuture.completedFuture(Optional.of(
              selectedBySequence.get(invocation.<Long>getArgument(0)))));
    }

    private BudgetManager.Account admitWatchAccount(RequestScope scope) throws BackendFailure {
      return budgets.admit(TABLE_GROUP, scope,
          new BudgetManager.Reservation(0, 1, 1, RESPONSE_BYTES, 0));
    }

    private WatchHistoryReplay openReplay(BudgetManager.Account watchAccount,
        KeyRange range, long startRevision) {
      return WatchHistoryReplay.open(TABLE_GROUP, range, startRevision, snapshot, 6,
          AUTHORIZATION, pins, selected, budgets, watchAccount).join().replay().orElseThrow();
    }

    private static SelectedSequenceReader.Selected selected(long revision, long sequence,
        List<PreparedEvent> events) {
      TransactionId transaction = new TransactionId(new UUID(1, sequence));
      CommitDecision decision = new CommitDecision(transaction,
          new InvocationId(new UUID(2, sequence)), sequence, revision - 1, revision,
          ByteKey.copyOf(new byte[] {(byte) revision}), ByteKey.copyOf(new byte[32]), true);
      long totalBytes = events.stream().mapToLong(PreparedEvent::retainedBytes).sum();
      EventReference reference = new EventReference(ByteKey.copyOf(new byte[] {(byte) sequence}),
          ByteKey.copyOf(new byte[32]), 0, events.size() - 1, events.size(), totalBytes, 1, 0);
      var index = new EventRevisionIndex(decision, reference);
      ProtectedEventCursor cursor = mock(ProtectedEventCursor.class);
      var remaining = new ArrayDeque<>(events);
      when(cursor.next(any(ManifestCatalog.ProofReadBudget.class))).thenAnswer(ignored ->
          CompletableFuture.completedFuture(Optional.ofNullable(remaining.pollFirst())));
      when(cursor.releaseCompletion()).thenReturn(CompletableFuture.completedFuture(null));
      return new SelectedSequenceReader.Selected(decision, Optional.of(cursor), Optional.of(index));
    }

    private static BudgetManager budgets() {
      long memory = 128L << 20;
      var limits = new BudgetManager.Limits(16, 16, 16, 8, 8, 1L << 20, memory);
      var reserved = new BudgetManager.ReservedCapacity(4, 4, 4, 4, 4, 1L << 20, 80L << 20);
      return new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits,
          reserved, Map.of(TABLE_GROUP, reserved));
    }
  }

  private static PreparedEvent event(long revision, long sequence, long ordinal, int key) {
    byte[] keyBytes = {(byte) key};
    byte[] value = {(byte) (key + 1)};
    StoredRecord current = StoredRecord.live(ByteKey.copyOf(keyBytes),
        new StorageMetadata(1, revision, ordinal + 1, 0, StorageMetadata.State.LIVE,
            value.length), value);
    return new PreparedEvent(new TransactionId(new UUID(1, sequence)), 1, sequence - 1,
        revision - 1, ordinal, PreparedEvent.Kind.PUT, current, Optional.empty());
  }
}
