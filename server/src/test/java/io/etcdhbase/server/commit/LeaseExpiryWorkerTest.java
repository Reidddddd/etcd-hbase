package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.PublicationSlot;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordEntry;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseExpiryCheckpoint;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.Test;

class LeaseExpiryWorkerTest {
  @Test
  void completedEmptyWindowWaitsForTheIdleIntervalBeforePublishingAgain() throws Exception {
    assertIdleWindowCadence(1_000);
    assertIdleWindowCadence(1_999);
  }

  private static void assertIdleWindowCadence(long checkpointTime) throws Exception {
    var token = new CommitOwnerService.OwnerToken("owner-a", 7);
    var owner = mock(M3OwnerLease.class);
    when(owner.currentToken()).thenReturn(Optional.of(token));
    var control = mock(CommitControlStore.class);
    var state = checkpointState(1);
    when(control.read(any())).thenReturn(CompletableFuture.completedFuture(
        Optional.of(new ControlSnapshot(state, new byte[] {1}))));
    var records = mock(SystemRecordStore.class);
    var checkpoint = new LeaseExpiryCheckpoint(7, 1, 4, 0, checkpointTime,
        LeaseExpiryCheckpoint.idleAt(1_001));
    when(records.latestAtOrBefore(eq(SystemRecordType.LEASE_EXPIRY_CHECKPOINT), any(), eq(1L), any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(new SystemRecordEntry(
            LeaseExpiryCheckpoint.versionIdentity(1), checkpoint.encode()))));
    var relay = mock(MutationRelay.class);
    when(relay.forward(any(), any())).thenAnswer(call -> CompletableFuture.completedFuture(
        new MutationRelay.KnownResult(((MutationForwardRequest)
            call.getArgument(0)).invocationId().bytes())));
    var timer = mock(ScheduledExecutorService.class);
    var ticks = new LinkedBlockingQueue<Runnable>();
    var delays = new LinkedBlockingQueue<Long>();
    when(timer.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS))).thenAnswer(call -> {
      ticks.add(call.getArgument(0));
      delays.add(call.getArgument(1));
      return mock(ScheduledFuture.class);
    });
    var now = new AtomicLong(1_999);
    var identity = new AuthorizedBackendIdentity("test-provider", "test-subject", "fixture-context",
        new AuthenticationScope("test-group", "test-profile"), Set.of("test-group"), 0);
    var worker = new LeaseExpiryWorker("test-group", ByteKey.copyOf(new byte[32]), 4,
        10_000, control, records, owner, relay, identity,
        () -> new LeaseTimeObservation(now.get(), now.get(), true), reservedWorkerBudgets(),
        LeaseBudgetProfile.EXPIRY_WORKER_MEMORY, Runnable::run, timer,
        Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC), new RequestMetrics());
    try {
      worker.start();
      assertEquals(0L, delays.poll(5, TimeUnit.SECONDS));
      ticks.poll(5, TimeUnit.SECONDS).run();
      var next = ticks.poll(5, TimeUnit.SECONDS);
      assertEquals(1_000L, delays.poll(5, TimeUnit.SECONDS));
      verify(relay, never()).forward(any(), any());
      now.set(2_999);
      next.run();
      assertTrue(ticks.poll(5, TimeUnit.SECONDS) != null);
      verify(relay).forward(any(), any());
    } finally {
      worker.stop();
      worker.drained().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void cleanupExecutorRejectionStillWaitsForPhysicalProducerDrain() {
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      CompletableFuture<Void> physicalProducer = new CompletableFuture<>();
      scope.trackPhysicalProducer(physicalProducer);

      CompletableFuture<Void> drain = LeaseExpiryWorker.closeAndDrain(scope, ignored -> {
        throw new RejectedExecutionException("cleanup executor is stopping");
      });

      assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone(),
          "RequestScope must run its rejected cleanup batch inline");
      assertFalse(scope.physicalProducersCompletion().toCompletableFuture().isDone());
      assertFalse(drain.isDone(), "worker drain must retain ownership of the physical producer");

      physicalProducer.complete(null);

      CompletionException failure = assertThrows(CompletionException.class, drain::join);
      assertTrue(failure.getCause() instanceof RejectedExecutionException);
    }
  }

  @Test
  void successfulCloseWaitsForBothCleanupAndPhysicalProducerCompletion() {
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      CompletableFuture<Void> physicalProducer = new CompletableFuture<>();
      scope.trackPhysicalProducer(physicalProducer);

      CompletableFuture<Void> drain = LeaseExpiryWorker.closeAndDrain(scope, Runnable::run);

      assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone());
      assertFalse(drain.isDone());
      physicalProducer.complete(null);
      drain.join();
      assertTrue(drain.isDone());
    }
  }

  @Test
  void staleExpiryWorkerDoesNotForwardAfterOwnerChangesDuringCheckpointRead() throws Exception {
    CommitOwnerService.OwnerToken oldToken = new CommitOwnerService.OwnerToken("owner-a", 7);
    CommitOwnerService.OwnerToken newToken = new CommitOwnerService.OwnerToken("owner-b", 8);
    AtomicReference<CommitOwnerService.OwnerToken> currentToken = new AtomicReference<>(oldToken);
    M3OwnerLease owner = mock(M3OwnerLease.class);
    when(owner.currentToken()).thenAnswer(ignored -> Optional.of(currentToken.get()));
    CompletableFuture<Optional<ControlSnapshot>> checkpointRead = new CompletableFuture<>();
    CommitControlStore control = mock(CommitControlStore.class);
    when(control.read(any(RequestScope.class))).thenReturn(checkpointRead);
    SystemRecordStore records = mock(SystemRecordStore.class);
    MutationRelay relay = mock(MutationRelay.class);
    ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
    AtomicReference<Runnable> scheduledTick = new AtomicReference<>();
    AtomicInteger scheduledCount = new AtomicInteger();
    var retryScheduled = new java.util.concurrent.CountDownLatch(1);
    ScheduledFuture<?> future = mock(ScheduledFuture.class);
    when(timer.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenAnswer(invocation -> {
          scheduledTick.set(invocation.getArgument(0));
          if (scheduledCount.getAndIncrement() > 0) retryScheduled.countDown();
          return future;
        });
    BudgetManager budgets = reservedWorkerBudgets();
    var identity = new AuthorizedBackendIdentity("test-provider", "test-subject", "fixture-context",
        new AuthenticationScope("test-group", "test-profile"), Set.of("test-group"), 0);
    LeaseTimeObservation observation = new LeaseTimeObservation(1_000, 1_000, true);
    var worker = new LeaseExpiryWorker("test-group", ByteKey.copyOf(new byte[32]), 4,
        10_000, control, records, owner, relay, identity, (LeaseTimeSource) () -> observation,
        budgets, LeaseBudgetProfile.EXPIRY_WORKER_MEMORY, Runnable::run, timer,
        Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC), new RequestMetrics());

    worker.start();
    scheduledTick.get().run();
    assertFalse(checkpointRead.isDone(), "the old owner's checkpoint read must be held at the barrier");

    currentToken.set(newToken);
    checkpointRead.complete(Optional.of(new ControlSnapshot(checkpointState(0), new byte[] {1})));
    assertTrue(retryScheduled.await(1, TimeUnit.SECONDS),
        "the expired owner cycle must finish before another poll is scheduled");
    worker.stop();
    worker.drained().get(1, TimeUnit.SECONDS);

    verify(relay, never()).forward(any(), any());
  }

  @Test
  void asynchronousCheckpointReadFailureClosesPreviouslyEstablishedOwnerReadiness() throws Exception {
    CommitOwnerService.OwnerToken token = new CommitOwnerService.OwnerToken("owner-a", 7);
    M3OwnerLease owner = mock(M3OwnerLease.class);
    when(owner.currentToken()).thenReturn(Optional.of(token));
    CommitControlStore control = mock(CommitControlStore.class);
    SystemRecordStore records = mock(SystemRecordStore.class);
    CommitState initialState = checkpointState(0);
    CommitState advancedState = checkpointState(1);
    when(control.read(any(RequestScope.class))).thenReturn(
        CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(initialState, new byte[] {1}))),
        CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(advancedState, new byte[] {2}))));
    when(records.latestAtOrBefore(eq(SystemRecordType.LEASE_EXPIRY_CHECKPOINT),
        any(ByteKey.class), eq(1L), any(RequestScope.class)))
        .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("checkpoint read failed")));
    ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> scheduledTick = mock(ScheduledFuture.class);
    when(timer.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenAnswer(ignored -> scheduledTick);
    LeaseTimeObservation observation = new LeaseTimeObservation(1_000, 1_000, true);
    AuthorizedBackendIdentity identity = new AuthorizedBackendIdentity("test-provider", "test-subject",
        "fixture-context", new AuthenticationScope("test-group", "test-profile"),
        Set.of("test-group"), 0);
    BudgetManager budgets = regularWorkerBudgets();
    var worker = new LeaseExpiryWorker("test-group", ByteKey.copyOf(new byte[32]), 4,
        10_000, control, records, owner, mock(MutationRelay.class), identity,
        (LeaseTimeSource) () -> observation, budgets,
        LeaseBudgetProfile.EXPIRY_WORKER_MEMORY, Runnable::run, timer,
        Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC), new RequestMetrics());

    CompletableFuture<Void> initialReadiness = worker.initialReadiness();
    worker.start();
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      try (BudgetManager.Account account = budgets.admit("test-group", scope,
          LeaseBudgetProfile.EXPIRY_WORKER_MEMORY, LeaseBudgetProfile.EXPIRY_WORKER_READ_LIMITS)) {
        assertFalse(initialReadiness.isDone(),
            "an owner runtime must not become ready before its persisted checkpoint is inspected");
        worker.inspectCheckpoint(token, observation, scope, account).join();
        initialReadiness.join();
        assertTrue(worker.isReady(),
            "a valid checkpoint read establishes readiness for this owner epoch");

        CompletionException failure = assertThrows(CompletionException.class,
            () -> worker.inspectCheckpoint(token, observation, scope, account).join());
        assertTrue(failure.getCause() instanceof IllegalStateException);
        assertFalse(worker.isReady(), "an asynchronous checkpoint read failure must revoke readiness");
        assertEquals(0, budgets.snapshot().operations());
        assertEquals(0, budgets.snapshot().scanners());
        assertEquals(0, budgets.snapshot().cleanup());
      }
    } finally {
      worker.stop();
    }
  }

  @Test
  void pendingPublicationPreservesOnlyAnEstablishedCheckpointForTheSameOwner() throws Exception {
    var token = new CommitOwnerService.OwnerToken("owner-a", 7);
    M3OwnerLease owner = mock(M3OwnerLease.class);
    when(owner.currentToken()).thenReturn(Optional.of(token));
    CommitState pending = checkpointState(0);
    when(pending.publication()).thenReturn(Optional.of(mock(PublicationSlot.class)));
    CommitState stable = checkpointState(0);
    CommitState otherOwner = checkpointState(0);
    when(otherOwner.ownerEpoch()).thenReturn(8L);
    CommitControlStore control = mock(CommitControlStore.class);
    when(control.read(any(RequestScope.class))).thenReturn(
        CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(pending, new byte[] {1}))),
        CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(stable, new byte[] {2}))),
        CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(pending, new byte[] {3}))),
        CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(otherOwner, new byte[] {4}))));
    SystemRecordStore records = mock(SystemRecordStore.class);
    ScheduledExecutorService timer = mock(ScheduledExecutorService.class);
    when(timer.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(mock(ScheduledFuture.class));
    var observation = new LeaseTimeObservation(1_000, 1_000, true);
    var identity = new AuthorizedBackendIdentity("test-provider", "test-subject", "fixture-context",
        new AuthenticationScope("test-group", "test-profile"), Set.of("test-group"), 0);
    BudgetManager budgets = regularWorkerBudgets();
    var worker = new LeaseExpiryWorker("test-group", ByteKey.copyOf(new byte[32]), 4,
        10_000, control, records, owner, mock(MutationRelay.class), identity,
        () -> observation, budgets, LeaseBudgetProfile.EXPIRY_WORKER_MEMORY, Runnable::run, timer,
        Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC), new RequestMetrics());

    worker.start();
    try (RequestScope scope = RequestScope.withoutDeadline();
         BudgetManager.Account account = budgets.admit("test-group", scope,
             LeaseBudgetProfile.EXPIRY_WORKER_MEMORY, LeaseBudgetProfile.EXPIRY_WORKER_READ_LIMITS)) {
      worker.inspectCheckpoint(token, observation, scope, account).join();
      assertFalse(worker.isReady(), "a new owner must inspect a finalized checkpoint first");
      worker.inspectCheckpoint(token, observation, scope, account).join();
      assertTrue(worker.isReady());
      worker.inspectCheckpoint(token, observation, scope, account).join();
      assertTrue(worker.isReady(), "a pending commit must not discard the current owner's proof");
      worker.inspectCheckpoint(token, observation, scope, account).join();
      assertFalse(worker.isReady(), "an owner epoch mismatch must invalidate the proof");
      verify(records, never()).latestAtOrBefore(any(), any(), anyLong(), any());
    } finally {
      worker.stop();
    }
  }

  @Test
  void checkpointScanHoldsItsRealBudgetUntilTheUnderlyingReadCompletes() throws Exception {
    CommitOwnerService.OwnerToken token = new CommitOwnerService.OwnerToken("owner-a", 7);
    M3OwnerLease owner = mock(M3OwnerLease.class);
    CommitControlStore control = mock(CommitControlStore.class);
    CommitState state = checkpointState(1);
    when(control.read(any(RequestScope.class))).thenReturn(CompletableFuture.completedFuture(
        Optional.of(new ControlSnapshot(state, new byte[] {1}))));
    CompletableFuture<Optional<io.etcdhbase.core.commit.SystemRecordEntry>> checkpointRead =
        new CompletableFuture<>();
    SystemRecordStore records = mock(SystemRecordStore.class);
    when(records.latestAtOrBefore(eq(SystemRecordType.LEASE_EXPIRY_CHECKPOINT),
        any(ByteKey.class), eq(1L), any(RequestScope.class))).thenReturn(checkpointRead);
    BudgetManager budgets = regularWorkerBudgets();
    AuthorizedBackendIdentity identity = new AuthorizedBackendIdentity("test-provider", "test-subject",
        "fixture-context", new AuthenticationScope("test-group", "test-profile"),
        Set.of("test-group"), 0);
    LeaseTimeObservation observation = new LeaseTimeObservation(1_000, 1_000, true);
    var worker = new LeaseExpiryWorker("test-group", ByteKey.copyOf(new byte[32]), 4,
        10_000, control, records, owner, mock(MutationRelay.class), identity,
        (LeaseTimeSource) () -> observation, budgets, LeaseBudgetProfile.EXPIRY_WORKER_MEMORY,
        Runnable::run, mock(ScheduledExecutorService.class),
        Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC), new RequestMetrics());

    try (RequestScope scope = RequestScope.withoutDeadline();
         BudgetManager.Account account = budgets.admit("test-group", scope,
             LeaseBudgetProfile.EXPIRY_WORKER_MEMORY,
             LeaseBudgetProfile.EXPIRY_WORKER_READ_LIMITS)) {
      CompletableFuture<?> inspection = worker.inspectCheckpoint(token, observation, scope, account);

      assertFalse(inspection.isDone(), "the checkpoint scanner remains pending at the barrier");
      assertEquals(1, budgets.snapshot().operations());
      assertEquals(1, budgets.snapshot().scanners());
      assertEquals(1, budgets.snapshot().cleanup());
      assertEquals(LeaseBudgetProfile.MAX_EXPIRY_CHECKPOINT_READ_BYTES,
          budgets.snapshot().allocatedBytes());

      checkpointRead.complete(Optional.empty());
      inspection.join();

      assertEquals(0, budgets.snapshot().operations());
      assertEquals(0, budgets.snapshot().scanners());
      assertEquals(0, budgets.snapshot().cleanup());
      assertEquals(0, budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void checkpointBudgetRejectionReleasesReservedReadBeforeStartingHBaseGet() throws Exception {
    CommitOwnerService.OwnerToken token = new CommitOwnerService.OwnerToken("owner-a", 7);
    CommitControlStore control = mock(CommitControlStore.class);
    SystemRecordStore records = mock(SystemRecordStore.class);
    BudgetManager.Limits limits = new BudgetManager.Limits(1, 0, 2, 1, 2, 0, 2L << 20);
    BudgetManager budgets = new BudgetManager(limits, Map.of("test-group", limits), limits);
    BudgetManager.ReadLimits readLimits = new BudgetManager.ReadLimits(1,
        LeaseBudgetProfile.MAX_EXPIRY_CONTROL_READ_BYTES - 1, 1, 1, 1);
    AuthorizedBackendIdentity identity = new AuthorizedBackendIdentity("test-provider", "test-subject",
        "fixture-context", new AuthenticationScope("test-group", "test-profile"),
        Set.of("test-group"), 0);
    LeaseTimeObservation observation = new LeaseTimeObservation(1_000, 1_000, true);
    var worker = new LeaseExpiryWorker("test-group", ByteKey.copyOf(new byte[32]), 4,
        10_000, control, records, mock(M3OwnerLease.class), mock(MutationRelay.class), identity,
        (LeaseTimeSource) () -> observation, budgets, LeaseBudgetProfile.EXPIRY_WORKER_MEMORY,
        Runnable::run, mock(ScheduledExecutorService.class),
        Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC), new RequestMetrics());

    try (RequestScope scope = RequestScope.withoutDeadline();
         BudgetManager.Account account = budgets.admit("test-group", scope,
             LeaseBudgetProfile.EXPIRY_WORKER_MEMORY, readLimits)) {
      CompletionException failure = assertThrows(CompletionException.class,
          () -> worker.inspectCheckpoint(token, new LeaseTimeObservation(1_000, 1_000, true),
              scope, account).join());
      assertTrue(failure.getCause() instanceof BackendFailure);
      verify(control, never()).read(any(RequestScope.class));
      assertEquals(0, budgets.snapshot().operations());
      assertEquals(0, budgets.snapshot().scanners());
      assertEquals(0, budgets.snapshot().cleanup());
      assertEquals(0, budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void healthyStandbyWorkerSignalsInitialReadinessWithoutAnOwnerCheckpoint() throws Exception {
    M3OwnerLease owner = mock(M3OwnerLease.class);
    when(owner.currentToken()).thenReturn(Optional.empty());
    AuthorizedBackendIdentity identity = new AuthorizedBackendIdentity("test-provider", "test-subject",
        "fixture-context", new AuthenticationScope("test-group", "test-profile"),
        Set.of("test-group"), 0);
    LeaseTimeObservation observation = new LeaseTimeObservation(1_000, 1_000, true);
    try (ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1)) {
      var worker = new LeaseExpiryWorker("test-group", ByteKey.copyOf(new byte[32]), 4,
          10_000, mock(CommitControlStore.class), mock(SystemRecordStore.class), owner,
          mock(MutationRelay.class), identity, (LeaseTimeSource) () -> observation,
          mock(BudgetManager.class), new BudgetManager.Reservation(1, 1, 1, 1, 1),
          Runnable::run, timer, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC),
          new RequestMetrics());

      worker.start();
      try {
        worker.initialReadiness().get(1, TimeUnit.SECONDS);
        assertTrue(worker.isReady(),
            "a healthy standby may serve local protocol paths without owning expiry mutations");
      } finally {
        worker.stop();
        worker.drained().get(1, TimeUnit.SECONDS);
      }
    }
  }

  private static CommitState checkpointState(long finalizedSequence) {
    CommitState state = mock(CommitState.class);
    when(state.supportsLeaseExpiryCheckpoints()).thenReturn(true);
    when(state.ownerEpoch()).thenReturn(7L);
    when(state.ownerInstance()).thenReturn(Optional.of("owner-a"));
    when(state.ownerLeaseExpired(anyLong())).thenReturn(false);
    when(state.publication()).thenReturn(Optional.empty());
    when(state.finalizedSequence()).thenReturn(finalizedSequence);
    return state;
  }

  private static BudgetManager regularWorkerBudgets() {
    BudgetManager.Limits limits = new BudgetManager.Limits(2, 0, 4, 2, 4, 0, 2L << 20);
    return new BudgetManager(limits, Map.of("test-group", limits), limits);
  }

  private static BudgetManager reservedWorkerBudgets() {
    var capacity = LeaseBudgetProfile.perGroupCapacity(1_572_864);
    var limits = new BudgetManager.Limits(capacity.requests(), capacity.queued(),
        capacity.operations(), capacity.scanners(), capacity.cleanup(), capacity.queuedBytes(),
        capacity.memoryBytes());
    return new BudgetManager(limits, Map.of("test-group", limits), limits,
        capacity, Map.of("test-group", capacity));
  }
}
