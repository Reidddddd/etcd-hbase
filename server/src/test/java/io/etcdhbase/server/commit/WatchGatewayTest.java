package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitControlStore;
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
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.grpc.Status;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.grpc.stub.StreamObserver;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WatchGatewayTest {
  private static final String TABLE_GROUP = "watch-test";
  private static final int RESPONSE_BYTES = 64 * 1024;
  private static final String AUTH_TOKEN = "watch-auth-token";
  private static final AuthenticationScope AUTH_SCOPE =
      new AuthenticationScope(TABLE_GROUP, "profile");

  @Test
  void mapsEveryTailerTerminationToItsTypedWatchTermination() throws Exception {
    assertTailerTermination(SharedEventTailer.Termination.CANCELLED,
        WatchTermination.cancelled("etcd-hbase: Watch subscription was cancelled"));
    assertTailerTermination(SharedEventTailer.Termination.RESOURCE_LIMIT,
        WatchTermination.resourceLimit("etcd-hbase: Watch subscription resource limit exceeded"));
    assertTailerTermination(SharedEventTailer.Termination.TAILER_CLOSED,
        WatchTermination.unavailable("etcd-hbase: Watch event source is unavailable"));
  }

  @Test
  void periodicRevalidationUsesResolvedIdentityAndDefersReleaseUntilSessionDrain() throws Exception {
    assertPeriodicRevalidationDrain(null, null);
  }

  @Test
  void watchDrainReportsReplayAndBudgetCleanupFailuresTogether() throws Exception {
    assertPeriodicRevalidationDrain(new IllegalStateException("history pin release failed"),
        new IllegalStateException("Watch account cleanup failed"));
  }

  private static void assertPeriodicRevalidationDrain(RuntimeException replayFailure,
      RuntimeException budgetFailure) throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var budgets = historicalWatchBudgets();
      var gatewayBudgets = budgetFailure == null ? budgets : failWatchBudgetClose(budgets, budgetFailure);
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(subscription.poll()).thenReturn(Optional.empty());
      when(subscription.termination()).thenReturn(Optional.empty());
      var watchAccount = new AtomicReference<BudgetManager.Account>();
      var subscriptionCloses = new AtomicInteger();
      doAnswer(ignored -> {
        subscriptionCloses.incrementAndGet();
        return null;
      }).when(subscription).close();
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenAnswer(invocation -> {
            watchAccount.set(invocation.getArgument(1));
            return new SharedEventTailer.SubscriptionStart(subscription, 0, 1);
          });

      var barrier = snapshot(3, 0, 1);
      CommitControlStore control = mock(CommitControlStore.class);
      when(control.read(any(RequestScope.class))).thenReturn(
          CompletableFuture.completedFuture(Optional.of(barrier)));
      EventHistoryPinManager historyPins = mock(EventHistoryPinManager.class);
      EventHistoryPinManager.Protection protection =
          mock(EventHistoryPinManager.Protection.class);
      var pinReleases = new AtomicInteger();
      when(historyPins.registerProtection(any(EventHistoryPinManager.Requirement.class),
          any(RequestScope.class))).thenReturn(CompletableFuture.completedFuture(protection));
      when(protection.renew(any(RequestScope.class)))
          .thenReturn(CompletableFuture.completedFuture(null));
      when(protection.releaseCompletion()).thenReturn(replayFailure == null
          ? CompletableFuture.completedFuture(null) : CompletableFuture.failedFuture(replayFailure));
      doAnswer(ignored -> {
        pinReleases.incrementAndGet();
        return null;
      }).when(protection).close();

      SelectedSequenceReader selectedEvents = mock(SelectedSequenceReader.class);
      when(selectedEvents.sequenceForRevision(anyLong(), any(CommitState.class),
          any(RequestScope.class), any(ManifestCatalog.ProofReadBudget.class)))
          .thenReturn(CompletableFuture.completedFuture(1L));
      when(selectedEvents.readPublishedState(any(RequestScope.class),
          any(ManifestCatalog.ProofReadBudget.class)))
          .thenReturn(CompletableFuture.completedFuture(barrier.state()));
      var physicalProducer = new CompletableFuture<Void>();
      var pendingEvent = new CompletableFuture<Optional<PreparedEvent>>();
      var historyReadScope = new AtomicReference<RequestScope>();
      var historyReadStarted = new CompletableFuture<Void>();
      ProtectedEventCursor cursor = mock(ProtectedEventCursor.class);
      when(cursor.next(any(ManifestCatalog.ProofReadBudget.class))).thenReturn(pendingEvent);
      when(cursor.releaseCompletion()).thenAnswer(ignored ->
          historyReadScope.get().physicalProducersCompletion());
      doAnswer(ignored -> {
        pendingEvent.completeExceptionally(new BackendFailure(BackendFailure.Category.CANCELLED));
        return null;
      }).when(cursor).close();
      PreparedEvent replayEvent = event(3);
      var decision = new CommitDecision(replayEvent.transactionId(), InvocationId.create(),
          1, 2, 3, ByteKey.copyOf(new byte[] {1}), ByteKey.copyOf(new byte[32]), true);
      var eventReference = new EventReference(ByteKey.copyOf(new byte[] {2}),
          ByteKey.copyOf(new byte[32]), 0, 0, 1, replayEvent.retainedBytes(), 1, 0);
      var selected = new SelectedSequenceReader.Selected(decision, Optional.of(cursor),
          Optional.of(new EventRevisionIndex(decision, eventReference)));
      when(selectedEvents.openUnderProtection(anyLong(), anyLong(), any(ByteKey.class),
          any(EventHistoryPinManager.Protection.class), any(RequestScope.class),
          any(ManifestCatalog.ProofReadBudget.class))).thenAnswer(invocation -> {
            RequestScope readScope = invocation.getArgument(4);
            historyReadScope.set(readScope);
            readScope.trackPhysicalProducer(physicalProducer);
            readScope.registerScopedCleanup(cursor, readScope.physicalProducersCompletion());
            historyReadStarted.complete(null);
            return CompletableFuture.completedFuture(Optional.of(selected));
          });

      var session = authSession();
      var identity = authorizedIdentity();
      var initialIdentity = mock(AuthSessionService.ResolvedIdentity.class);
      when(initialIdentity.session()).thenReturn(session);
      when(initialIdentity.identity()).thenReturn(identity);
      var identityReleases = new AtomicInteger();
      when(initialIdentity.release()).thenAnswer(ignored -> {
        identityReleases.incrementAndGet();
        return CompletableFuture.completedFuture(null);
      });

      var activeRecheckIdentity = mock(AuthSessionService.ResolvedIdentity.class);
      when(activeRecheckIdentity.session()).thenReturn(session);
      when(activeRecheckIdentity.identity()).thenReturn(identity);
      when(activeRecheckIdentity.release()).thenReturn(CompletableFuture.completedFuture(null));
      var revokedRecheck = new CompletableFuture<AuthSessionService.ResolvedIdentity>();
      AuthSessionService authSessions = mock(AuthSessionService.class);
      when(authSessions.resolve(AUTH_TOKEN, scope))
          .thenReturn(CompletableFuture.completedFuture(initialIdentity));
      when(authSessions.resolveByDigest(eq(session.tokenDigest()), any(RequestScope.class)))
          .thenReturn(CompletableFuture.completedFuture(activeRecheckIdentity), revokedRecheck);

      var pending = new M3TableGroupRuntime.Pending(startup, null, true);
      pending.installEventTailer(tailer);
      var periodicTick = new AtomicReference<Runnable>();
      var worker = new QueuedExecutor();
      ScheduledExecutorService timers = timers(periodicTick, 13);
      ByteKey serviceAuthorization = ByteKey.copyOf(new byte[32]);
      var invalidatedAuthorization = new AtomicReference<ByteKey>();
      var gateway = new WatchGateway(TABLE_GROUP, pending, control, gatewayBudgets,
          Runnable::run, timers, 11, 12, RESPONSE_BYTES, historyPins,
          selectedEvents, serviceAuthorization, new RequestMetrics(),
          identity, authSessions, invalidatedAuthorization::set);
      var observer = new RecordingWatchObserver();
      var identityReleasesAtError = new AtomicInteger(-1);
      observer.onErrorHook = () -> identityReleasesAtError.set(identityReleases.get());
      var stream = new WatchStream(observer, scope, worker, timers,
          () -> gateway.openSession(scope, Optional.of(AUTH_TOKEN)), 11, 12, RESPONSE_BYTES,
          new CompactedWatchIdBudget(8), TimeUnit.MINUTES.toMillis(10), 13);

      try (stream) {
        stream.onNext(Rpc.WatchRequest.newBuilder()
            .setCreateRequest(Rpc.WatchCreateRequest.newBuilder()
                .setWatchId(72).setKey(ByteString.copyFrom(new byte[] {1}))
                .setStartRevision(3))
            .build());
        worker.runUntilIdle();
        assertEquals(1, observer.responses.size());
        assertTrue(observer.responses.getFirst().getCreated());
        assertFalse(observer.responses.getFirst().getCanceled(),
            () -> "the historical Watch must be admitted before replay starts: "
                + observer.responses.getFirst());
        historyReadStarted.get(5, TimeUnit.SECONDS);
        BudgetManager.Account admittedWatchAccount = watchAccount.get();
        assertNotNull(admittedWatchAccount, "the live Watch owns its request budget account");
        assertTrue(budgets.snapshot().requests() > 0,
            "the active Watch retains its admitted budget before revocation is observed");

        Runnable tick = periodicTick.getAndSet(null);
        assertNotNull(tick, "the real WatchGateway session installs a bounded silent-stream check");
        tick.run();
        worker.runUntilIdle();
        assertEquals(0, identityReleases.get());

        revokedRecheck.completeExceptionally(
            new AuthSessionException(AuthSessionException.Reason.INVALID_TOKEN));
        worker.runUntilIdle();
        assertEquals(AuthorizationScopeDigest.fromSessionRead(identity, session),
            invalidatedAuthorization.get());

        boolean terminatedBeforeDrain = observer.failure != null;
        int responsesBeforeDrain = observer.responses.size();
        int identityReleasesBeforeDrain = identityReleases.get();
        int identityReleasesAtTerminalError = identityReleasesAtError.get();
        boolean watchBudgetActiveBeforeDrain = admittedWatchAccount.isActive();
        int pinReleasesBeforeDrain = pinReleases.get();
        int subscriptionClosesBeforeDrain = subscriptionCloses.get();

        var drain = scope.physicalProducersCompletion().toCompletableFuture();
        if (!terminatedBeforeDrain) scope.cancel();
        physicalProducer.complete(null);
        if (replayFailure == null) {
          drain.join();
        } else {
          var failure = assertThrows(CompletionException.class, drain::join);
          assertSame(replayFailure, failure.getCause());
          assertEquals(List.of(budgetFailure), List.of(replayFailure.getSuppressed()));
          verify(admittedWatchAccount).close();
        }

        assertTrue(terminatedBeforeDrain, "durable session invalidation terminates a silent Watch");
        assertEquals(1, responsesBeforeDrain, "no event is needed to terminate the Watch");
        assertEquals(0, identityReleasesAtTerminalError,
            "the RPC error is sent while its Watch producer still owns the identity");
        assertEquals(0, identityReleasesBeforeDrain,
            "the identity remains held until the replay producer drains");
        assertTrue(watchBudgetActiveBeforeDrain,
            "the Watch budget remains active while its replay producer drains");
        assertEquals(0, pinReleasesBeforeDrain,
            "the history pin remains held while its replay producer drains");
        assertEquals(1, subscriptionClosesBeforeDrain,
            "the live subscription closes as soon as authorization is revoked");
        assertEquals(1, identityReleases.get(),
            "the identity is released once the tracked Watch session and replay drain");
        assertFalse(admittedWatchAccount.isActive(), "the drained Watch releases its budget");
        assertEquals(1, pinReleases.get(), "the drained replay releases its history pin");
        verify(subscription).close();
        assertReleased(budgets);
      }
    }
  }

  private static BudgetManager failWatchBudgetClose(BudgetManager budgets, RuntimeException failure)
      throws BackendFailure {
    BudgetManager wrapped = mock(BudgetManager.class, delegatesTo(budgets));
    doAnswer(invocation -> {
      BudgetManager.Account account = budgets.admit(invocation.getArgument(0),
          invocation.getArgument(1), invocation.getArgument(2));
      BudgetManager.Account wrappedAccount = mock(BudgetManager.Account.class, delegatesTo(account));
      doAnswer(ignored -> {
        account.close();
        throw failure;
      }).when(wrappedAccount).close();
      return wrappedAccount;
    }).when(wrapped).admit(eq(TABLE_GROUP), any(RequestScope.class), any(BudgetManager.Reservation.class));
    return wrapped;
  }

  private static void assertTailerTermination(SharedEventTailer.Termination tailerTermination,
      WatchTermination expected) throws Exception {
    try (var startup = RequestScope.withoutDeadline(); var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(subscription.termination()).thenReturn(Optional.of(tailerTermination));
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 0, 0));
      var budgets = budgets();
      var opened = subscribe(gateway(startup, budgets), tailer, scope);

      assertEquals(Optional.of(expected), opened.subscription().termination());

      opened.subscription().close();
      assertReleased(budgets);
    }
  }

  @Test
  void tailerSubscribeFailureReleasesWatchReservationAndStateMemory() throws Exception {
    try (var startup = RequestScope.withoutDeadline(); var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var budgets = budgets();
      var metrics = new RequestMetrics();
      var gateway = gateway(startup, budgets, RESPONSE_BYTES, control(12, 0), metrics);
      var failure = new IllegalStateException("tailer rejected subscription");
      doThrow(failure).when(tailer).subscribeWithCheckpoint(
          any(KeyRange.class), any(BudgetManager.Account.class), anyInt(), anyLong(), anyLong());

      assertSame(failure, assertThrows(IllegalStateException.class,
          () -> subscribe(gateway, tailer, scope)));
      assertEquals(0, metrics.watchCreateResourceFailures().get(
          RequestMetrics.WatchCreateResourceFailureStage.TAILER_SUBSCRIPTION),
          "non-resource failures are not counted as resource refusals");
      assertReleased(budgets);
    }
  }

  @Test
  void requestAdmissionResourceFailureIsCountedAtItsBoundedStage() throws Exception {
    try (var startup = RequestScope.withoutDeadline(); var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var budgets = budgetsWithMemory(1_024);
      var metrics = new RequestMetrics();
      var gateway = gateway(startup, budgets, RESPONSE_BYTES, control(12, 0), metrics);

      BackendFailure failure = assertThrows(BackendFailure.class,
          () -> subscribe(gateway, tailer, scope));

      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
      assertEquals(1, metrics.watchCreateResourceFailures().get(
          RequestMetrics.WatchCreateResourceFailureStage.REQUEST_ACCOUNT_ADMISSION));
      assertEquals(0, metrics.watchCreateResourceFailures().get(
          RequestMetrics.WatchCreateResourceFailureStage.TAILER_SUBSCRIPTION));
      assertReleased(budgets);
    }
  }

  @Test
  void stateMemoryRetentionResourceFailureIsCountedSeparatelyFromAdmission() throws Exception {
    try (var startup = RequestScope.withoutDeadline(); var scope = RequestScope.withoutDeadline()) {
      BudgetManager budgets = mock(BudgetManager.class);
      BudgetManager.Account account = mock(BudgetManager.Account.class);
      when(budgets.admit(eq(TABLE_GROUP), any(RequestScope.class), any(BudgetManager.Reservation.class)))
          .thenReturn(account);
      when(account.tryRetain(eq(BudgetManager.Memory.HEAD), anyLong()))
          .thenReturn(Optional.empty());
      var metrics = new RequestMetrics();
      var gateway = gateway(startup, budgets, RESPONSE_BYTES, control(12, 0), metrics);

      BackendFailure failure = assertThrows(BackendFailure.class,
          () -> subscribe(gateway, openTailer(), scope));

      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
      assertEquals(1, metrics.watchCreateResourceFailures().get(
          RequestMetrics.WatchCreateResourceFailureStage.STATE_MEMORY_RETENTION));
      assertEquals(0, metrics.watchCreateResourceFailures().get(
          RequestMetrics.WatchCreateResourceFailureStage.REQUEST_ACCOUNT_ADMISSION));
      verify(account).close();
    }
  }

  @Test
  void tailerSubscriptionResourceFailureIsCountedAndReleasesAdmission() throws Exception {
    try (var startup = RequestScope.withoutDeadline(); var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var budgets = budgets();
      var metrics = new RequestMetrics();
      var gateway = gateway(startup, budgets, RESPONSE_BYTES, control(12, 0), metrics);
      var failure = new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      doThrow(failure).when(tailer).subscribeWithCheckpoint(
          any(KeyRange.class), any(BudgetManager.Account.class), anyInt(), anyLong(), anyLong());

      assertSame(failure, assertThrows(BackendFailure.class,
          () -> subscribe(gateway, tailer, scope)));

      assertEquals(1, metrics.watchCreateResourceFailures().get(
          RequestMetrics.WatchCreateResourceFailureStage.TAILER_SUBSCRIPTION));
      assertEquals(0, metrics.watchCreateResourceFailures().get(
          RequestMetrics.WatchCreateResourceFailureStage.REQUEST_ACCOUNT_ADMISSION));
      assertReleased(budgets);
    }
  }

  @Test
  void barrierReadResourceFailureIsCountedAndReleasesWatchSubscription() throws Exception {
    try (var startup = RequestScope.withoutDeadline(); var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 0, 1));
      var budgets = budgets();
      var control = mock(CommitControlStore.class);
      var failure = new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      when(control.read(scope)).thenReturn(CompletableFuture.failedFuture(failure));
      var metrics = new RequestMetrics();
      var gateway = gateway(startup, budgets, RESPONSE_BYTES, control, metrics);

      CompletionException openingFailure = assertThrows(CompletionException.class,
          () -> subscribe(gateway, tailer, scope));

      assertTrue(openingFailure.getCause() instanceof BackendFailure);
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
          ((BackendFailure) openingFailure.getCause()).category());
      assertEquals(1, metrics.watchCreateResourceFailures().get(
          RequestMetrics.WatchCreateResourceFailureStage.BARRIER_READ));
      assertEquals(0, metrics.watchCreateResourceFailures().get(
          RequestMetrics.WatchCreateResourceFailureStage.HISTORY_REPLAY_OPEN));
      verify(subscription).close();
      assertReleased(budgets);
    }
  }

  @Test
  void historyReplayResourceFailureIsCountedAtReplayOpenStage() throws Exception {
    try (var startup = RequestScope.withoutDeadline(); var scope = RequestScope.withoutDeadline()) {
      var limits = new BudgetManager.Limits(8, 8, 8, 8, 8, 1L << 20, 128L << 20);
      var reserved = new BudgetManager.ReservedCapacity(2, 2, 2, 2, 2, 1L << 20, 64L << 20);
      var budgets = new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits,
          reserved, Map.of(TABLE_GROUP, reserved));
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 0, 1));
      var selected = mock(SelectedSequenceReader.class);
      var failure = new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      when(selected.sequenceForRevision(anyLong(), any(CommitState.class),
          any(RequestScope.class), any(ManifestCatalog.ProofReadBudget.class)))
          .thenReturn(CompletableFuture.failedFuture(failure));
      var metrics = new RequestMetrics();
      var gateway = new WatchGateway(TABLE_GROUP,
          new M3TableGroupRuntime.Pending(startup, null, true), control(12, 0), budgets,
          Runnable::run, mock(ScheduledExecutorService.class), 11, 12, RESPONSE_BYTES,
          mock(EventHistoryPinManager.class), selected, ByteKey.copyOf(new byte[32]), metrics);
      var request = Rpc.WatchCreateRequest.newBuilder().setStartRevision(3).build();

      CompletionException openingFailure = assertThrows(CompletionException.class,
          () -> subscribe(gateway, tailer, scope, KeyRange.all(), request));

      assertTrue(openingFailure.getCause() instanceof BackendFailure);
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
          ((BackendFailure) openingFailure.getCause()).category());
      assertEquals(1, metrics.watchCreateResourceFailures().get(
          RequestMetrics.WatchCreateResourceFailureStage.HISTORY_REPLAY_OPEN));
      assertEquals(0, metrics.watchCreateResourceFailures().get(
          RequestMetrics.WatchCreateResourceFailureStage.TAILER_SUBSCRIPTION));
      verify(subscription).close();
      assertReleased(budgets);
    }
  }

  @Test
  void tailerShutdownRacePreservesUnavailableAndReleasesWatchReservation() throws Exception {
    try (var startup = RequestScope.withoutDeadline(); var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var budgets = budgets();
      var gateway = gateway(startup, budgets);
      var failure = new BackendFailure(BackendFailure.Category.UNAVAILABLE);
      doThrow(failure).when(tailer).subscribeWithCheckpoint(
          any(KeyRange.class), any(BudgetManager.Account.class), anyInt(), anyLong(), anyLong());

      assertSame(failure, assertThrows(BackendFailure.class,
          () -> subscribe(gateway, tailer, scope)));
      assertEquals(BackendFailure.Category.UNAVAILABLE, failure.category());
      assertReleased(budgets);
    }
  }

  @Test
  void realTailerResourceLimitWakesWatchAndReleasesDeliveryBudgetWithoutAnotherRequest() throws Exception {
    var budgets = budgetsWithMemory(2L << 20);
    try (var startup = RequestScope.withoutDeadline();
         var tailerScope = RequestScope.withoutDeadline();
         var readScope = RequestScope.withoutDeadline();
         var streamScope = RequestScope.withoutDeadline();
         var tailerMemory = budgets.admit(TABLE_GROUP, tailerScope,
             new BudgetManager.Reservation(0, 1, 1, 8192));
         var readMemory = budgets.admit(TABLE_GROUP, readScope,
             new BudgetManager.Reservation(0, 1, 1, 1L << 20))) {
      TransactionId transactionId = new TransactionId(new UUID(0, 11));
      PreparedEvent firstEvent = preparedEvent(transactionId, 0, 11, new byte[] {1});
      PreparedEvent secondEvent = preparedEvent(transactionId, 1, 11, new byte[] {1});
      SharedEventTailer tailer = realTailer(tailerMemory, transactionId, firstEvent, secondEvent);
      try (tailer) {
        // Allow one event after index accounting, but leave the second same-sized event one byte over budget.
        int subscriptionByteLimit = Math.subtractExact(Math.multiplyExact(firstEvent.retainedBytes(), 2), 1);
        var control = mock(CommitControlStore.class);
        AtomicInteger controlReads = new AtomicInteger();
        when(control.read(any(RequestScope.class))).thenAnswer(ignored -> {
          long revision = controlReads.getAndIncrement() == 0 ? 10 : 11;
          return CompletableFuture.completedFuture(Optional.of(snapshot(revision, 0)));
        });
        WatchGateway gateway = gateway(startup, budgets, subscriptionByteLimit, control);
        WatchStream.Session session = session(gateway, tailer, streamScope);
        ScheduledExecutorService timers = mock(ScheduledExecutorService.class);
        List<Rpc.WatchResponse> responses = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        StreamObserver<Rpc.WatchResponse> observer = new StreamObserver<>() {
          @Override public void onNext(Rpc.WatchResponse response) { responses.add(response); }
          @Override public void onError(Throwable problem) { failure.set(problem); }
          @Override public void onCompleted() {}
        };
        AtomicInteger workerExecutions = new AtomicInteger();
        Executor worker = task -> {
          workerExecutions.incrementAndGet();
          task.run();
        };
        var stream = new WatchStream(observer, streamScope, worker, timers,
            () -> CompletableFuture.completedFuture(session), 11, 12, subscriptionByteLimit,
            new CompactedWatchIdBudget(8));
        try (stream) {
          BudgetManager.Snapshot baseline = budgets.snapshot();
          stream.onNext(Rpc.WatchRequest.newBuilder()
              .setCreateRequest(Rpc.WatchCreateRequest.newBuilder()
                  .setWatchId(77).setKey(ByteString.copyFrom(new byte[] {1})))
              .build());
          assertEquals(1, responses.size());
          assertTrue(responses.getFirst().getCreated());
          assertEquals(77, responses.getFirst().getWatchId(),
              "unexpected rejected Create: " + responses.getFirst().getCancelReason());
          assertFalse(responses.getFirst().getCanceled());
          assertEquals(1, tailer.subscriptionCount(), "the Created response must retain a live tailer subscription");

          assertEquals(SharedEventTailer.Step.OPENED, advanceTailer(tailer, readScope, readMemory));
          assertEquals(SharedEventTailer.Step.BUFFERED, advanceTailer(tailer, readScope, readMemory));
          assertEquals(SharedEventTailer.Step.BUFFERED, advanceTailer(tailer, readScope, readMemory));
          int workerExecutionsBeforePublish = workerExecutions.get();
          assertEquals(SharedEventTailer.Step.PUBLISHED, advanceTailer(tailer, readScope, readMemory));

          assertTrue(workerExecutions.get() > workerExecutionsBeforePublish,
              "the tailer must invoke the installed Watch stream wakeup after terminating a subscriber");
          assertEquals(0, tailer.subscriptionCount(),
              "the real tailer must terminate a subscriber whose byte budget is exceeded");
          assertEquals(2, responses.size(),
              "the real tailer must wake the Watch stream without another client request");
          Rpc.WatchResponse cancelled = responses.getLast();
          assertTrue(cancelled.getCanceled());
          assertEquals(77, cancelled.getWatchId());
          assertEquals("etcd-hbase: Watch subscription resource limit exceeded",
              cancelled.getCancelReason());
          assertEquals(baseline.requests(), budgets.snapshot().requests());
          assertEquals(baseline.reservedBytes(), budgets.snapshot().reservedBytes());
          assertEquals(baseline.allocatedBytes(), budgets.snapshot().allocatedBytes(),
              "queued delivery memory is released when RESOURCE_LIMIT is delivered");
          assertNull(failure.get());
          verify(timers, never()).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
        }
      }
    }
  }

  @Test
  void checkedAdmissionFailureReleasesAccountBeforeItEscapes() throws Exception {
    AtomicLong clock = new AtomicLong();
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withTimeoutNanos(2, clock::getAndIncrement)) {
      var tailer = openTailer();
      var budgets = budgets();
      var gateway = gateway(startup, budgets);

      BackendFailure failure = assertThrows(BackendFailure.class,
          () -> subscribe(gateway, tailer, scope));

      assertEquals(BackendFailure.Category.TIMEOUT, failure.category());
      assertReleased(budgets);
    }
  }

  @Test
  void cancellingStreamRemovesInstalledTailerSubscriptionBeforeBarrierReadCompletes()
      throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var tailerScope = RequestScope.withoutDeadline();
         var streamScope = RequestScope.withoutDeadline()) {
      var budgets = budgets();
      var tailerMemory = budgets.admit(TABLE_GROUP, tailerScope,
          new BudgetManager.Reservation(0, 1, 1, 1));
      BudgetManager.Snapshot tailerBaseline = budgets.snapshot();
      try (var tailer = new SharedEventTailer(mock(SelectedSequenceReader.class),
          ByteKey.copyOf(new byte[32]), tailerMemory, 0, 1, 4,
          WatchEventBatchEncoder.MAX_PENDING_EVENTS_PER_WATCH,
          LeaseBudgetProfile.EVENT_TAILER_MAX_REVISION_BYTES)) {
        var control = mock(CommitControlStore.class);
        var barrierRead = new CompletableFuture<Optional<ControlSnapshot>>();
        when(control.read(streamScope)).thenReturn(barrierRead);
        var gateway = gateway(startup, budgets, RESPONSE_BYTES, control);

        CompletableFuture<WatchStream.OpenedWatch> opening = session(gateway, tailer, streamScope)
            .subscribe(KeyRange.all(), Rpc.WatchCreateRequest.getDefaultInstance())
            .toCompletableFuture();
        assertEquals(1, tailer.subscriptionCount());

        streamScope.cancel();

        assertEquals(0, tailer.subscriptionCount(),
            "cancellation releases the shared range index without waiting for the barrier read");
        assertEquals(0, budgets.snapshot().allocatedBytes(),
            "cancellation also releases the watch state memory before the barrier read completes");
        assertEquals(tailerBaseline.requests(), budgets.snapshot().requests(),
            "cancellation releases the watch admission before the barrier read completes");
        assertEquals(tailerBaseline.reservedBytes(), budgets.snapshot().reservedBytes(),
            "cancellation releases the watch reservation before the barrier read completes");
        barrierRead.complete(Optional.of(snapshot(12, 0)));
        assertThrows(CompletionException.class, opening::join);
      }
      tailerScope.close();
      assertReleased(budgets);
    }
  }

  @Test
  void cleanupFailuresAreSuppressedAndDoNotPreventOtherResourcesFromReleasing() throws Exception {
    try (var startup = RequestScope.withoutDeadline(); var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      var budgets = budgets();
      var control = mock(CommitControlStore.class);
      var barrierFailure = new IllegalStateException("control barrier read failed");
      when(control.read(scope)).thenReturn(CompletableFuture.failedFuture(barrierFailure));
      var gateway = gateway(startup, budgets, RESPONSE_BYTES, control);
      var cleanupFailure = new IllegalStateException("subscription cleanup failed");
      doThrow(cleanupFailure).when(subscription).close();
      when(tailer.subscribeWithCheckpoint(
          any(KeyRange.class), any(BudgetManager.Account.class), anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 0, 1));

      CompletionException failure = assertThrows(CompletionException.class,
          () -> subscribe(gateway, tailer, scope));

      assertEquals(1, failure.getSuppressed().length);
      assertSame(cleanupFailure, failure.getSuppressed()[0]);
      verify(subscription).close();
      assertReleased(budgets);
    }
  }

  @Test
  void gatewayChargesRetainedRangeBytesAndReleasesThemOnClose() throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 0, 0));
      byte[] lower = new byte[1_024];
      byte[] upper = new byte[1_024];
      upper[0] = 1;
      KeyRange range = KeyRange.between(ByteKey.copyOf(lower), ByteKey.copyOf(upper));
      long retainedBytes = 512 + lower.length + upper.length;
      var budgets = budgetsWithMemory(8_192);
      var gateway = gateway(startup, budgets, 1);

      WatchStream.OpenedWatch opened = subscribe(gateway, tailer, scope, range);
      assertEquals(retainedBytes, budgets.snapshot().allocatedBytes(),
          "Gateway accounts for the retained range bounds and active watch state");
      opened.subscription().close();
      assertReleased(budgets);

      var smallBudgets = budgetsWithMemory(1_024);
      var tooSmall = gateway(startup, smallBudgets, 1);
      var rejectedTailer = openTailer();
      BackendFailure rejected = assertThrows(BackendFailure.class,
          () -> subscribe(tooSmall, rejectedTailer, scope, range));
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, rejected.category());
      verify(rejectedTailer, never()).subscribeWithCheckpoint(any(KeyRange.class),
          any(BudgetManager.Account.class), anyInt(), anyLong(), anyLong());
      assertReleased(smallBudgets);
    }
  }

  @Test
  void passesInclusiveStartRevisionToTailerSoEarlierEventsDoNotConsumeWatchBudget() throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 3, 12));
      var gateway = gateway(startup, budgets());
      var request = Rpc.WatchCreateRequest.newBuilder().setStartRevision(15).build();

      WatchStream.OpenedWatch opened = subscribe(gateway, tailer, scope, KeyRange.all(), request);

      assertEquals(12, opened.revision());
      verify(tailer).subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          eq(WatchEventBatchEncoder.MAX_PENDING_EVENTS_PER_WATCH), eq((long) RESPONSE_BYTES), eq(15L));
      opened.subscription().close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void fragmentFlagControlsRevisionAggregationBeforeWatchResponseFragmenter(boolean fragment)
      throws Exception {
    int messageLimit = 1_024;
    byte[] authorization = new byte[32];
    ByteKey authorizationDigest = ByteKey.copyOf(authorization);
    TransactionId transactionId = TransactionId.create();
    CommitDecision decision = new CommitDecision(transactionId, InvocationId.create(), 4, 4, 5,
        ByteKey.copyOf(new byte[] {1}), ByteKey.copyOf(new byte[32]), true);
    List<PreparedEvent> events = List.of(
        revisionEvent(transactionId, 0, new byte[] {1}, new byte[400]),
        revisionEvent(transactionId, 1, new byte[] {1, 0}, new byte[400]),
        revisionEvent(transactionId, 2, new byte[] {1, (byte) 0xff}, new byte[400]));
    assertTrue(events.stream().mapToLong(PreparedEvent::retainedBytes).sum() > messageLimit);

    try (var startup = RequestScope.withoutDeadline();
         var tailerScope = RequestScope.withoutDeadline();
         var readScope = RequestScope.withoutDeadline();
         var streamScope = RequestScope.withoutDeadline()) {
      var budgets = watchBudgets();
      var tailerReservation = new BudgetManager.Reservation(1, 1, 1,
          LeaseBudgetProfile.EVENT_TAILER_MAX_REVISION_BYTES, 0);
      var readReservation = new BudgetManager.Reservation(1, 1, 1, 1, 0);
      long indexMemoryBytes = 512 + 1 + 1;
      long expectedRevisionBytes = fragment
          ? LeaseBudgetProfile.EVENT_TAILER_MAX_REVISION_BYTES : messageLimit;
      var watchReservation = new BudgetManager.Reservation(indexMemoryBytes, 1, 1,
          Math.addExact(expectedRevisionBytes, indexMemoryBytes), 0);
      try (var tailerMemory = budgets.admit(TABLE_GROUP, tailerScope, tailerReservation);
           var readMemory = budgets.admit(TABLE_GROUP, readScope, readReservation)) {
        var readBudget = new ManifestCatalog.ProofReadBudget(readScope, readMemory, 4, 1L << 20);
        var source = mock(SelectedSequenceReader.class);
        var cursor = mock(ProtectedEventCursor.class);
        var selected = new SelectedSequenceReader.Selected(decision, Optional.of(cursor),
            Optional.of(new EventRevisionIndex(decision, new EventReference(
                ByteKey.copyOf(new byte[] {2}), ByteKey.copyOf(new byte[32]), 0,
                events.size() - 1, events.size(),
                events.stream().mapToLong(PreparedEvent::retainedBytes).sum(), 1, 0))));
        when(source.openNext(eq(3L), eq(4L), eq(authorizationDigest), eq(readScope), any()))
            .thenReturn(CompletableFuture.completedFuture(
                new SelectedSequenceReader.ReadAhead(3, Optional.of(selected))));
        AtomicLong nextEvent = new AtomicLong();
        when(cursor.next(any(ManifestCatalog.ProofReadBudget.class))).thenAnswer(ignored -> {
          long index = nextEvent.getAndIncrement();
          Optional<PreparedEvent> event = index < events.size()
              ? Optional.of(events.get(Math.toIntExact(index))) : Optional.empty();
          return CompletableFuture.completedFuture(event);
        });
        when(cursor.releaseCompletion()).thenReturn(CompletableFuture.completedFuture(null));

        try (var tailer = new SharedEventTailer(source, authorizationDigest, tailerMemory,
            3, 4, 4, WatchEventBatchEncoder.MAX_PENDING_EVENTS_PER_WATCH,
            LeaseBudgetProfile.EVENT_TAILER_MAX_REVISION_BYTES)) {
          var pending = new M3TableGroupRuntime.Pending(startup, null, true);
          pending.installEventTailer(tailer);
          var gateway = new WatchGateway(TABLE_GROUP, pending, control(4, 0), budgets,
              Runnable::run, mock(ScheduledExecutorService.class), 11, 12, messageLimit);
          WatchStream.Session session = session(gateway, tailer, streamScope);
          var observer = new RecordingResponseObserver();
          try (var stream = new WatchStream(observer, streamScope, Runnable::run,
              mock(ScheduledExecutorService.class), () -> CompletableFuture.completedFuture(session),
              11, 12, messageLimit)) {
            Rpc.WatchCreateRequest create = Rpc.WatchCreateRequest.newBuilder()
                .setWatchId(31)
                .setKey(ByteString.copyFrom(new byte[] {1}))
                .setRangeEnd(ByteString.copyFrom(new byte[] {2}))
                .setFragment(fragment)
                .build();
            stream.onNext(Rpc.WatchRequest.newBuilder().setCreateRequest(create).build());
            assertTrue(observer.responses.stream().anyMatch(Rpc.WatchResponse::getCreated));
            assertEquals(tailerReservation.total() + readReservation.total() + watchReservation.total(),
                budgets.snapshot().reservedBytes(),
                "the OUTPUT reservation includes both the event revision and tailer index");
            assertEquals(2 * indexMemoryBytes, budgets.snapshot().allocatedBytes(),
                "the watch state and tailer index are retained in separate memory lanes");

            assertEquals(SharedEventTailer.Step.OPENED, tailer.step(readScope, readBudget).join());
            for (int index = 0; index < events.size(); index++) {
              assertEquals(SharedEventTailer.Step.BUFFERED, tailer.step(readScope, readBudget).join());
            }
            assertEquals(SharedEventTailer.Step.PUBLISHED, tailer.step(readScope, readBudget).join());
            stream.onNext(Rpc.WatchRequest.getDefaultInstance());

            List<Rpc.WatchResponse> eventResponses = observer.responses.stream()
                .filter(response -> response.getEventsCount() > 0).toList();
            if (fragment) {
              assertNull(observer.failure);
              assertTrue(eventResponses.size() > 1,
                  "the complete revision reaches the sender and is split into response-sized fragments");
              assertTrue(eventResponses.getFirst().getFragment());
              assertFalse(eventResponses.getLast().getFragment());
              assertTrue(eventResponses.stream()
                  .allMatch(response -> response.getSerializedSize() <= messageLimit));
              assertEquals(events.stream().map(event -> ByteString.copyFrom(
                      event.current().value().orElseThrow())).toList(),
                  eventResponses.stream().flatMap(response -> response.getEventsList().stream())
                      .map(event -> event.getKv().getValue()).toList());
              assertEquals(2 * indexMemoryBytes, budgets.snapshot().allocatedBytes(),
                  "the final fragment releases all event output leases");
            } else {
              assertTrue(eventResponses.isEmpty(), "a non-fragment watch never emits part of a revision");
              assertTrue(observer.responses.stream().anyMatch(response -> response.getWatchId() == 31
                  && response.getCanceled()
                  && response.getCancelReason().contains("subscription resource limit")));
              assertEquals(0, budgets.snapshot().allocatedBytes(),
                  "a rejected non-fragment revision releases the state and tailer index");
            }
          }
          assertEquals(0, budgets.snapshot().allocatedBytes(),
              "closing the stream releases remaining watch memory");
        }
      }
      assertReleased(budgets);
    }
  }

  @Test
  void currentRevisionReadsTheAuthoritativeControlSnapshotInsteadOfTheTailerCheckpoint()
      throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      when(tailer.completedRevision()).thenReturn(3L);
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 2, 3));
      var control = mock(CommitControlStore.class);
      when(control.read(scope)).thenReturn(
          CompletableFuture.completedFuture(Optional.of(snapshot(5, 0))),
          CompletableFuture.completedFuture(Optional.of(snapshot(9, 0))));
      var gateway = gateway(startup, budgets(), RESPONSE_BYTES, control);
      var session = session(gateway, tailer, scope);
      var opened = session.subscribe(KeyRange.all(), Rpc.WatchCreateRequest.getDefaultInstance())
          .toCompletableFuture().join();

      assertEquals(5, opened.revision(), "create captures the current control revision");
      assertEquals(3, session.completedRevision(), "the tailer remains at its older completed checkpoint");
      assertEquals(9, session.currentRevision().toCompletableFuture().join(),
          "cancel acknowledgements read the current control revision");
      verify(control, org.mockito.Mockito.times(2)).read(scope);
      opened.subscription().close();
    }
  }

  @Test
  void passesNegativeStartRevisionAndCompactBoundaryAndReleasesBudgetsOnClose() throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 3, 4));
      var budgets = budgetsWithMemory(2_048);
      var gateway = gateway(startup, budgets, 1, control(4, 2));
      var request = Rpc.WatchCreateRequest.newBuilder().setStartRevision(-2).build();

      WatchStream.OpenedWatch opened = subscribe(gateway, tailer, scope, KeyRange.all(), request);

      assertEquals(4, opened.revision());
      assertEquals(2, opened.compactRevision());
      assertEquals(512, budgets.snapshot().allocatedBytes());
      verify(tailer).subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          eq(WatchEventBatchEncoder.MAX_PENDING_EVENTS_PER_WATCH), eq(1L), eq(-2L));

      opened.subscription().close();
      assertReleased(budgets);
      opened.subscription().close();
      verify(subscription).close();
      assertReleased(budgets);
    }
  }

  @Test
  void subscriptionCloseErrorStillReleasesBudgetsAndIsPropagated() throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 0, 0));
      var budgets = budgets();
      var gateway = gateway(startup, budgets);
      var opened = subscribe(gateway, tailer, scope);
      var closeFailure = new AssertionError("subscription cleanup failed");
      doThrow(closeFailure).when(subscription).close();

      assertSame(closeFailure,
          assertThrows(AssertionError.class, opened.subscription()::close));
      assertReleased(budgets);

      opened.subscription().close();
      verify(subscription).close();
      assertReleased(budgets);
    }
  }

  @Test
  void initiallyCompactedPositiveStartSkipsHistoryRegistrationAndKeepsPerWatchBoundary()
      throws Exception {
    try (var startup = RequestScope.withoutDeadline(); var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var sharedSubscription = mock(SharedEventTailer.Subscription.class);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), eq(3L)))
          .thenReturn(new SharedEventTailer.SubscriptionStart(sharedSubscription, 4, 5));
      var budgets = budgets();
      var gateway = gateway(startup, budgets, RESPONSE_BYTES, control(5, 4));
      var request = Rpc.WatchCreateRequest.newBuilder().setStartRevision(3).build();

      WatchStream.OpenedWatch opened = subscribe(gateway, tailer, scope, KeyRange.all(), request);

      assertEquals(5, opened.revision());
      assertEquals(4, opened.compactRevision());
      assertFalse(opened.historicalReplaySupported());
      verify(sharedSubscription, never()).requireRevisionAtLeast(anyLong());
      opened.subscription().close();
      verify(sharedSubscription).close();
      assertReleased(budgets);
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {0, -1})
  void nonPositiveStartRegistersBeforeTheBarrierAndDropsOnlyEventsAtOrBeforeIt(
      long startRevision) throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      when(tailer.completedRevision()).thenReturn(3L);
      var sharedSubscription = mock(SharedEventTailer.Subscription.class);
      var eventAtBarrier = mock(SharedEventTailer.Delivery.class);
      var eventAfterBarrier = mock(SharedEventTailer.Delivery.class);
      var queued = new java.util.ArrayDeque<SharedEventTailer.Delivery>();
      queued.add(eventAtBarrier);
      queued.add(eventAfterBarrier);
      when(eventAtBarrier.revision()).thenReturn(4L);
      when(eventAtBarrier.sequence()).thenReturn(4L);
      when(eventAtBarrier.event()).thenReturn(event(4));
      when(eventAfterBarrier.revision()).thenReturn(5L);
      when(eventAfterBarrier.sequence()).thenReturn(5L);
      when(eventAfterBarrier.event()).thenReturn(event(5));
      when(sharedSubscription.poll()).thenAnswer(ignored -> Optional.ofNullable(queued.pollFirst()));
      when(sharedSubscription.termination()).thenReturn(Optional.empty());
      doAnswer(invocation -> null).when(sharedSubscription).requireRevisionAtLeast(anyLong());
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(sharedSubscription, 2, 3));

      var control = mock(CommitControlStore.class);
      var barrier = new CompletableFuture<Optional<ControlSnapshot>>();
      when(control.read(scope)).thenReturn(barrier);
      var budgets = budgets();
      var gateway = gateway(startup, budgets, RESPONSE_BYTES, control);
      var request = Rpc.WatchCreateRequest.newBuilder().setWatchId(19)
          .setStartRevision(startRevision).build();

      var opening = subscribeAsync(gateway, tailer, scope, KeyRange.all(), request);
      assertFalse(opening.toCompletableFuture().isDone(),
          "the create cannot publish before the authoritative barrier read completes");
      var order = inOrder(tailer, control);
      order.verify(tailer).subscribeWithCheckpoint(any(KeyRange.class),
          any(BudgetManager.Account.class), eq(WatchEventBatchEncoder.MAX_PENDING_EVENTS_PER_WATCH),
          eq((long) RESPONSE_BYTES), eq(startRevision));
      order.verify(control).read(scope);

      barrier.complete(Optional.of(snapshot(4, 2, 4)));
      WatchStream.OpenedWatch opened = opening.toCompletableFuture().join();

      assertEquals(4, opened.revision());
      assertEquals(2, opened.compactRevision(), "compact boundary comes from the same control snapshot");
      assertEquals(4, opened.barrierSequence(), "the handoff captures the selected sequence too");
      verify(sharedSubscription).requireRevisionAtLeast(5L);
      try (var delivery = opened.subscription().poll().orElseThrow()) {
        assertEquals(5, delivery.revision(), "B+1 is retained after raising the floor");
      }
      verify(eventAtBarrier).close();
      verify(eventAfterBarrier).close();
      opened.subscription().close();
      assertReleased(budgets);
    }
  }

  @Test
  void pollCancellationHandoffClosesTheDetachedDeliveryBeforeReleasingItsAccount() throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var sharedSubscription = mock(SharedEventTailer.Subscription.class);
      var sharedDelivery = mock(SharedEventTailer.Delivery.class);
      var budgets = budgets();
      var gateway = gateway(startup, budgets);
      var wrappedSubscription = new AtomicReference<WatchStream.Subscription>();
      var memory = new AtomicReference<BudgetManager.Lease>();
      PreparedEvent event = event();
      when(sharedDelivery.revision()).thenReturn(4L);
      when(sharedDelivery.event()).thenReturn(event);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong())).thenAnswer(invocation -> {
            BudgetManager.Account account = invocation.getArgument(1);
            memory.set(account.tryRetain(BudgetManager.Memory.OUTPUT, event.retainedBytes()).orElseThrow());
            doAnswer(closing -> {
              memory.get().close();
              return null;
            }).when(sharedDelivery).close();
            return new SharedEventTailer.SubscriptionStart(sharedSubscription, 3, 3);
          });
      when(sharedSubscription.poll()).thenAnswer(invocation -> {
        wrappedSubscription.get().close();
        return Optional.of(sharedDelivery);
      });

      WatchStream.OpenedWatch opened = subscribe(gateway, tailer, scope);
      wrappedSubscription.set(opened.subscription());
      assertTrue(opened.subscription().poll().isEmpty());

      verify(sharedDelivery).close();
      assertReleased(budgets);
    }
  }

  @Test
  void workerRejectionAfterWatchSubscriptionOpensReleasesTailerAndBudgetResources()
      throws Exception {
    assertWorkerFailureAfterWatchOpensReleasesResources(
        new RejectedExecutionException("completion worker rejected the Watch"),
        Status.Code.RESOURCE_EXHAUSTED, "backend resource limit was reached");
  }

  @Test
  void workerErrorAfterWatchSubscriptionOpensReleasesTailerAndBudgetResources() throws Exception {
    assertWorkerFailureAfterWatchOpensReleasesResources(
        new AssertionError("completion worker failed fatally"),
        Status.Code.INTERNAL, "backend operation failed");
  }

  private static void assertWorkerFailureAfterWatchOpensReleasesResources(
      Throwable workerFailure, Status.Code expectedCode, String expectedDescription)
      throws Exception {
    try (var startup = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline()) {
      var tailer = openTailer();
      var subscription = mock(SharedEventTailer.Subscription.class);
      when(tailer.subscribeWithCheckpoint(any(KeyRange.class), any(BudgetManager.Account.class),
          anyInt(), anyLong(), anyLong()))
          .thenReturn(new SharedEventTailer.SubscriptionStart(subscription, 0, 0));
      var barrier = new CompletableFuture<Optional<ControlSnapshot>>();
      var control = mock(CommitControlStore.class);
      when(control.read(scope)).thenReturn(barrier);
      var budgets = budgets();
      var gateway = gateway(startup, budgets, RESPONSE_BYTES, control);
      var worker = new ToggleRejectingExecutor();
      var observer = new RecordingWatchObserver();
      var stream = new WatchStream(observer, scope, worker, mock(ScheduledExecutorService.class),
          () -> CompletableFuture.completedFuture(session(gateway, tailer, scope)),
          11, 12, RESPONSE_BYTES, new CompactedWatchIdBudget(8));

      stream.onNext(Rpc.WatchRequest.newBuilder().setCreateRequest(
          Rpc.WatchCreateRequest.newBuilder().setWatchId(7)
              .setKey(com.google.protobuf.ByteString.copyFromUtf8("key"))).build());

      verify(control).read(scope);
      assertTrue(budgets.snapshot().allocatedBytes() > 0,
          "the delayed control barrier leaves the accepted subscription and state memory retained");
      worker.rejection = workerFailure;
      barrier.complete(Optional.of(snapshot(4, 0)));

      verify(subscription).close();
      assertReleased(budgets);
      assertNotNull(observer.failure, "the worker failure is delivered through the RPC error path");
      Status status = Status.fromThrowable(observer.failure);
      assertEquals(expectedCode, status.getCode());
      assertEquals(expectedDescription, status.getDescription());
      assertFalse(status.getDescription().contains(workerFailure.getMessage()),
          "the RPC error must not expose the executor failure details");
    }
  }

  private static SharedEventTailer openTailer() {
    SharedEventTailer tailer = mock(SharedEventTailer.class);
    when(tailer.isClosed()).thenReturn(false);
    return tailer;
  }

  private static SharedEventTailer realTailer(BudgetManager.Account memory, TransactionId transactionId,
      PreparedEvent firstEvent, PreparedEvent secondEvent) {
    var decision = new CommitDecision(transactionId, InvocationId.create(), 1, 10, 11,
        ByteKey.copyOf(new byte[] {1}), ByteKey.copyOf(new byte[32]), true);
    var eventReference = new EventReference(ByteKey.copyOf(new byte[] {2}), ByteKey.copyOf(new byte[32]),
        0, 1, 2, firstEvent.retainedBytes() + secondEvent.retainedBytes(), 1, 0);
    var selected = new SelectedSequenceReader.Selected(decision, Optional.of(mock(ProtectedEventCursor.class)),
        Optional.of(new EventRevisionIndex(decision, eventReference)));
    ProtectedEventCursor cursor = selected.events().orElseThrow();
    when(cursor.next(any(ManifestCatalog.ProofReadBudget.class))).thenReturn(
        CompletableFuture.completedFuture(Optional.of(firstEvent)),
        CompletableFuture.completedFuture(Optional.of(secondEvent)),
        CompletableFuture.completedFuture(Optional.empty()));
    when(cursor.releaseCompletion()).thenReturn(CompletableFuture.completedFuture(null));
    var source = mock(SelectedSequenceReader.class);
    when(source.openNext(anyLong(), anyLong(), any(ByteKey.class), any(RequestScope.class),
        any(ManifestCatalog.ProofReadBudget.class))).thenReturn(CompletableFuture.completedFuture(
            new SelectedSequenceReader.ReadAhead(0, Optional.of(selected))));
    return new SharedEventTailer(source, ByteKey.copyOf(new byte[32]), memory,
        0, 10, 4, 4, 1L << 20);
  }

  private static PreparedEvent preparedEvent(TransactionId transactionId, long ordinal,
      long revision, byte[] key) {
    byte[] value = new byte[1 << 10];
    StoredRecord current = StoredRecord.live(ByteKey.copyOf(key),
        new StorageMetadata(revision, revision, 1, 0, StorageMetadata.State.LIVE, value.length), value);
    return new PreparedEvent(transactionId, 1, 0, revision - 1, ordinal,
        PreparedEvent.Kind.PUT, current, Optional.empty());
  }

  private static SharedEventTailer.Step advanceTailer(SharedEventTailer tailer,
      RequestScope scope, BudgetManager.Account memory) {
    var budget = new ManifestCatalog.ProofReadBudget(scope, memory, 10, 1L << 20);
    return tailer.step(scope, budget).join();
  }

  private static BudgetManager budgets() {
    var limits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1L << 20, 2L << 20);
    return new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits);
  }

  private static BudgetManager budgetsWithMemory(long memoryBytes) {
    var limits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1L << 20, memoryBytes);
    return new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits);
  }

  private static BudgetManager watchBudgets() {
    var limits = new BudgetManager.Limits(8, 8, 8, 8, 8, 1L << 20, 64L << 20);
    return new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits);
  }

  private static BudgetManager historicalWatchBudgets() {
    var limits = new BudgetManager.Limits(16, 16, 16, 8, 8, 1L << 20, 128L << 20);
    var reserved = new BudgetManager.ReservedCapacity(4, 4, 4, 4, 4, 1L << 20, 80L << 20);
    return new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits,
        reserved, Map.of(TABLE_GROUP, reserved));
  }

  private static ScheduledExecutorService timers(AtomicReference<Runnable> periodicTick,
      long periodicDelayMillis) {
    ScheduledExecutorService timers = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> future = mock(ScheduledFuture.class);
    doAnswer(invocation -> {
      Runnable task = invocation.getArgument(0);
      long delay = invocation.getArgument(1);
      if (delay == periodicDelayMillis) periodicTick.set(task);
      return future;
    }).when(timers).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    return timers;
  }

  private static AuthSession authSession() {
    var envelope = new CredentialEnvelope("key-1", CredentialEnvelope.CURRENT_AAD_VERSION,
        new byte[CredentialEnvelope.NONCE_BYTES], new byte[CredentialEnvelope.TAG_BYTES]);
    return new AuthSession(ByteKey.copyOf(new byte[16]), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), "milvus", "provider", AUTH_SCOPE,
        ByteKey.copyOf(new byte[32]), 1, 1, 1, 1_000, 0, 3,
        AuthSession.Status.ACTIVE, envelope);
  }

  private static AuthorizedBackendIdentity authorizedIdentity() {
    return new AuthorizedBackendIdentity("provider", "milvus-user", "context",
        AUTH_SCOPE, Set.of(TABLE_GROUP), 1);
  }

  private static WatchGateway gateway(RequestScope startup, BudgetManager budgets) {
    return gateway(startup, budgets, RESPONSE_BYTES);
  }

  private static WatchGateway gateway(RequestScope startup, BudgetManager budgets,
      int maximumResponseBytes) {
    return gateway(startup, budgets, maximumResponseBytes, control(12, 0));
  }

  private static WatchGateway gateway(RequestScope startup, BudgetManager budgets,
      int maximumResponseBytes, CommitControlStore control) {
    return gateway(startup, budgets, maximumResponseBytes, control, new RequestMetrics());
  }

  private static WatchGateway gateway(RequestScope startup, BudgetManager budgets,
      int maximumResponseBytes, CommitControlStore control, RequestMetrics metrics) {
    var pending = new M3TableGroupRuntime.Pending(startup, null, true);
    return new WatchGateway(TABLE_GROUP, pending, control, budgets, Runnable::run,
        mock(ScheduledExecutorService.class), 11, 12, maximumResponseBytes,
        null, null, null, metrics);
  }

  private static CommitControlStore control(long revision, long compactRevision) {
    var control = mock(CommitControlStore.class);
    when(control.read(any(RequestScope.class))).thenReturn(
        CompletableFuture.completedFuture(Optional.of(snapshot(revision, compactRevision))));
    return control;
  }

  private static ControlSnapshot snapshot(long revision, long compactRevision) {
    return snapshot(revision, compactRevision, 0);
  }

  private static ControlSnapshot snapshot(long revision, long compactRevision, long sequence) {
    CommitState state = CommitState.restore(CommitState.FORMAT_VERSION, ByteKey.copyOf(new byte[32]),
        Optional.empty(), 0, 0, 1, sequence, revision, sequence, revision,
        Optional.empty(), compactRevision);
    return new ControlSnapshot(state, new byte[] {1});
  }

  private static WatchStream.OpenedWatch subscribe(WatchGateway gateway,
      SharedEventTailer tailer, RequestScope scope) throws BackendFailure {
    return subscribe(gateway, tailer, scope, KeyRange.all(), Rpc.WatchCreateRequest.getDefaultInstance());
  }

  private static WatchStream.OpenedWatch subscribe(WatchGateway gateway,
      SharedEventTailer tailer, RequestScope scope, KeyRange range) throws BackendFailure {
    return subscribe(gateway, tailer, scope, range, Rpc.WatchCreateRequest.getDefaultInstance());
  }

  private static WatchStream.OpenedWatch subscribe(WatchGateway gateway,
      SharedEventTailer tailer, RequestScope scope, KeyRange range,
      Rpc.WatchCreateRequest request) throws BackendFailure {
    return subscribeAsync(gateway, tailer, scope, range, request).toCompletableFuture().join();
  }

  private static java.util.concurrent.CompletionStage<WatchStream.OpenedWatch> subscribeAsync(
      WatchGateway gateway, SharedEventTailer tailer, RequestScope scope, KeyRange range,
      Rpc.WatchCreateRequest request) throws BackendFailure {
    return session(gateway, tailer, scope).subscribe(range, request);
  }

  private static WatchStream.Session session(WatchGateway gateway, SharedEventTailer tailer,
      RequestScope scope) {
    try {
      Class<?> sessionType = Arrays.stream(WatchGateway.class.getDeclaredClasses())
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

  private static void assertReleased(BudgetManager budgets) {
    BudgetManager.Snapshot snapshot = budgets.snapshot();
    assertEquals(0, snapshot.requests());
    assertEquals(0, snapshot.reservedBytes());
    assertEquals(0, snapshot.allocatedBytes());
  }

  private static final class ToggleRejectingExecutor implements Executor {
    private Throwable rejection;

    @Override public void execute(Runnable command) {
      if (rejection instanceof RuntimeException runtimeFailure) throw runtimeFailure;
      if (rejection instanceof Error fatal) throw fatal;
      command.run();
    }
  }

  private static final class QueuedExecutor implements Executor {
    private final ArrayDeque<Runnable> queued = new ArrayDeque<>();

    @Override public void execute(Runnable task) { queued.addLast(task); }

    void runUntilIdle() {
      int remaining = 256;
      while (!queued.isEmpty()) {
        if (--remaining < 0) throw new AssertionError("Watch stream work did not become idle");
        queued.removeFirst().run();
      }
    }
  }

  private static final class RecordingWatchObserver implements StreamObserver<Rpc.WatchResponse> {
    private final List<Rpc.WatchResponse> responses = new ArrayList<>();
    private Throwable failure;
    private Runnable onErrorHook;

    @Override public void onNext(Rpc.WatchResponse response) { responses.add(response); }

    @Override public void onError(Throwable cause) {
      if (onErrorHook != null) onErrorHook.run();
      failure = cause;
    }

    @Override public void onCompleted() {}
  }

  private static PreparedEvent event() { return event(4); }

  private static PreparedEvent event(long revision) {
    byte[] key = {1};
    byte[] value = {2, 3};
    var stored = StoredRecord.live(ByteKey.copyOf(key),
        new StorageMetadata(1, revision, revision, 0, StorageMetadata.State.LIVE, value.length), value);
    return new PreparedEvent(new TransactionId(new UUID(0, revision)), 1, revision - 1,
        revision - 1, 0,
        PreparedEvent.Kind.PUT, stored, Optional.empty());
  }

  private static PreparedEvent revisionEvent(TransactionId transactionId, long ordinal,
      byte[] key, byte[] value) {
    var stored = StoredRecord.live(ByteKey.copyOf(key),
        new StorageMetadata(1, 5, 5, 0, StorageMetadata.State.LIVE, value.length), value);
    return new PreparedEvent(transactionId, 1, 3, 4, ordinal,
        PreparedEvent.Kind.PUT, stored, Optional.empty());
  }

  private static final class RecordingResponseObserver implements StreamObserver<Rpc.WatchResponse> {
    private final List<Rpc.WatchResponse> responses = new ArrayList<>();
    private Throwable failure;

    @Override public void onNext(Rpc.WatchResponse response) { responses.add(response); }
    @Override public void onError(Throwable error) { failure = error; }
    @Override public void onCompleted() {}
  }
}
