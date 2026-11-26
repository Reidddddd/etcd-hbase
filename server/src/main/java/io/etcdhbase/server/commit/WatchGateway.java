package io.etcdhbase.server.commit;

import etcdserverpb.Rpc;
import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.SelectedSequenceReader;
import io.etcdhbase.core.commit.SharedEventTailer;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** Adapts a table-group's shared tailer and per-watch budgets to the Watch RPC stream controller. */
final class WatchGateway {
  private static final long ACTIVE_WATCH_STATE_BYTES = 512;
  private record EventPosition(long sequence, long ordinal) implements Comparable<EventPosition> {
    private EventPosition {
      if (sequence < 0 || ordinal < 0) throw new IllegalArgumentException("invalid Watch event position");
    }

    @Override public int compareTo(EventPosition other) {
      int sequenceOrder = Long.compare(sequence, other.sequence);
      return sequenceOrder != 0 ? sequenceOrder : Long.compare(ordinal, other.ordinal);
    }
  }

  private final String tableGroup;
  private final M3TableGroupRuntime.Pending pending;
  private final CommitControlStore control;
  private final EventHistoryPinManager historyPins;
  private final SelectedSequenceReader selectedEvents;
  private final ByteKey authorization;
  private final AuthenticatedRequestIdentityResolver requestIdentities;
  private final BudgetManager budgets;
  private final Executor worker;
  private final ScheduledExecutorService timers;
  private final long clusterId;
  private final long memberId;
  private final int maximumResponseBytes;
  private final RequestMetrics metrics;

  WatchGateway(String tableGroup, M3TableGroupRuntime.Pending pending, CommitControlStore control,
      BudgetManager budgets,
      Executor worker, ScheduledExecutorService timers, long clusterId, long memberId,
      int maximumResponseBytes) {
    this(tableGroup, pending, control, budgets, worker, timers, clusterId, memberId,
        maximumResponseBytes, null, null, null, new RequestMetrics());
  }

  WatchGateway(String tableGroup, M3TableGroupRuntime.Pending pending, CommitControlStore control,
      BudgetManager budgets, Executor worker, ScheduledExecutorService timers,
      long clusterId, long memberId, int maximumResponseBytes,
      EventHistoryPinManager historyPins, SelectedSequenceReader selectedEvents,
      ByteKey authorization) {
    this(tableGroup, pending, control, budgets, worker, timers, clusterId, memberId,
        maximumResponseBytes, historyPins, selectedEvents, authorization, new RequestMetrics());
  }

  WatchGateway(String tableGroup, M3TableGroupRuntime.Pending pending, CommitControlStore control,
      BudgetManager budgets, Executor worker, ScheduledExecutorService timers,
      long clusterId, long memberId, int maximumResponseBytes,
      EventHistoryPinManager historyPins, SelectedSequenceReader selectedEvents,
      ByteKey authorization, RequestMetrics metrics) {
    this(tableGroup, pending, control, budgets, worker, timers, clusterId, memberId,
        maximumResponseBytes, historyPins, selectedEvents, authorization, metrics, null, null);
  }

  WatchGateway(String tableGroup, M3TableGroupRuntime.Pending pending, CommitControlStore control,
      BudgetManager budgets, Executor worker, ScheduledExecutorService timers,
      long clusterId, long memberId, int maximumResponseBytes,
      EventHistoryPinManager historyPins, SelectedSequenceReader selectedEvents,
      ByteKey authorization, RequestMetrics metrics, AuthorizedBackendIdentity serviceIdentity,
      AuthSessionService authSessions) {
    this(tableGroup, pending, control, budgets, worker, timers, clusterId, memberId,
        maximumResponseBytes, historyPins, selectedEvents, authorization, metrics, serviceIdentity,
        authSessions, null);
  }

  WatchGateway(String tableGroup, M3TableGroupRuntime.Pending pending, CommitControlStore control,
      BudgetManager budgets, Executor worker, ScheduledExecutorService timers,
      long clusterId, long memberId, int maximumResponseBytes,
      EventHistoryPinManager historyPins, SelectedSequenceReader selectedEvents,
      ByteKey authorization, RequestMetrics metrics, AuthorizedBackendIdentity serviceIdentity,
      AuthSessionService authSessions, Consumer<ByteKey> invalidateReadAuthorization) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.pending = Objects.requireNonNull(pending, "pending");
    this.control = Objects.requireNonNull(control, "control");
    this.historyPins = historyPins;
    this.selectedEvents = selectedEvents;
    this.authorization = authorization;
    if (authSessions != null && (serviceIdentity == null || authorization == null)) {
      throw new IllegalArgumentException("authenticated Watch requires its service identity and digest");
    }
    this.requestIdentities = serviceIdentity == null || authorization == null ? null
        : new AuthenticatedRequestIdentityResolver(tableGroup, serviceIdentity, authorization,
            authSessions, invalidateReadAuthorization);
    if ((historyPins == null || selectedEvents == null || authorization == null)
        && !(historyPins == null && selectedEvents == null && authorization == null)) {
      throw new IllegalArgumentException("Watch replay dependencies must be provided together");
    }
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.worker = Objects.requireNonNull(worker, "worker");
    this.timers = Objects.requireNonNull(timers, "timers");
    if (tableGroup.isBlank() || clusterId == 0 || memberId == 0 || maximumResponseBytes < 1) {
      throw new IllegalArgumentException("invalid Watch gateway identity or limits");
    }
    this.clusterId = clusterId;
    this.memberId = memberId;
    this.maximumResponseBytes = maximumResponseBytes;
  }

  Function<StreamObserver<Rpc.WatchResponse>, StreamObserver<Rpc.WatchRequest>> handler() {
    return responseObserver -> {
      RequestScope scope = RequestScopeInterceptor.currentScope();
      Optional<String> authToken = RequestScopeInterceptor.currentAuthToken();
      return new WatchStream(responseObserver, scope, worker, timers,
          () -> openSession(scope, authToken),
          clusterId, memberId, maximumResponseBytes);
    };
  }

  CompletionStage<WatchStream.Session> openSession(RequestScope scope, Optional<String> authToken) {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(authToken, "authToken");
    SharedEventTailer current = pending.eventTailer().orElse(null);
    CompletionStage<SharedEventTailer> ready = current == null
        ? pending.eventTailerReady() : CompletableFuture.completedFuture(current);
    return ready.thenCompose(tailer -> {
      if (scope.isCancelled() || tailer.isClosed()) {
        return CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      if (requestIdentities == null) {
        if (authToken.isPresent()) {
          return CompletableFuture.failedFuture(
              new AuthSessionException(AuthSessionException.Reason.INVALID_TOKEN));
        }
        return registerSession(scope, tailer, authorization, null);
      }
      return requestIdentities.resolve(authToken, scope).thenCompose(requestIdentity ->
          registerSession(scope, tailer, requestIdentity.authorizationDigest(), requestIdentity));
    });
  }

  private CompletionStage<WatchStream.Session> registerSession(RequestScope scope,
      SharedEventTailer tailer, ByteKey authorizationDigest,
      AuthenticatedRequestIdentityResolver.ResolvedRequestIdentity requestIdentity) {
    var session = new TailerSession(tailer, scope, authorizationDigest, requestIdentity);
    try {
      boolean registered = scope.startIfOpen(() -> {
        scope.trackPhysicalProducer(session.drainCompletion());
        if (requestIdentity != null) requestIdentity.releaseAfterPhysicalDrain(scope);
      });
      if (!registered) {
        if (requestIdentity != null) requestIdentity.releaseAfterPhysicalDrain(scope);
        session.close();
        return CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.CANCELLED));
      }
      return CompletableFuture.completedFuture(session);
    } catch (RuntimeException | Error failure) {
      if (requestIdentity != null) requestIdentity.releaseAfterPhysicalDrain(scope);
      try {
        session.close();
      } catch (RuntimeException | Error closeFailure) {
        if (closeFailure != failure) failure.addSuppressed(closeFailure);
      }
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
  }

  private final class TailerSession implements WatchStream.Session {
    private final SharedEventTailer tailer;
    private final RequestScope streamScope;
    private final ByteKey authorizationDigest;
    private final AuthenticatedRequestIdentityResolver.ResolvedRequestIdentity requestIdentity;
    private final Object lifecycleLock = new Object();
    private final Set<TailerSubscription> subscriptions =
        Collections.newSetFromMap(new IdentityHashMap<>());
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private int activeOperations;
    private boolean closed;
    private Throwable drainFailure;

    private TailerSession(SharedEventTailer tailer, RequestScope streamScope) {
      this(tailer, streamScope, authorization, null);
    }

    private TailerSession(SharedEventTailer tailer, RequestScope streamScope,
        ByteKey authorizationDigest,
        AuthenticatedRequestIdentityResolver.ResolvedRequestIdentity requestIdentity) {
      this.tailer = tailer;
      this.streamScope = streamScope;
      this.authorizationDigest = authorizationDigest;
      this.requestIdentity = requestIdentity;
    }

    @Override public long completedRevision() { return tailer.completedRevision(); }
    @Override public long completedSequence() { return tailer.completedSequence(); }

    @Override
    public boolean authorizationRevalidationRequired() {
      return requestIdentity != null && requestIdentity.sessionIdentity() != null;
    }

    CompletionStage<Void> drainCompletion() { return drained.minimalCompletionStage(); }

    @Override
    public CompletionStage<Long> currentRevision() {
      return trackOperation(() -> {
        if (tailer.isClosed() || streamScope.isCancelled()) {
          return CompletableFuture.failedFuture(
              new BackendFailure(BackendFailure.Category.UNAVAILABLE));
        }
        CompletionStage<Optional<ControlSnapshot>> latest;
        try {
          latest = Objects.requireNonNull(control.read(streamScope), "Watch revision snapshot future");
        } catch (RuntimeException | Error failure) {
          return CompletableFuture.failedFuture(failure);
        }
        return latest.thenApply(snapshot -> {
          if (streamScope.isCancelled() || tailer.isClosed()) throw unavailable();
          return snapshot.map(ControlSnapshot::state)
              .map(state -> state.publishedRevision())
              .orElseThrow(WatchGateway::unavailable);
        });
      });
    }

    @Override
    public CompletionStage<Void> revalidateAuthorization() {
      if (requestIdentity == null) return CompletableFuture.completedFuture(null);
      return trackOperation(() -> requestIdentities.revalidate(requestIdentity, streamScope));
    }

    @Override
    public CompletionStage<WatchStream.OpenedWatch> subscribe(KeyRange range,
        Rpc.WatchCreateRequest request)
        throws BackendFailure {
      if (!beginOperation()) {
        throw new BackendFailure(BackendFailure.Category.CANCELLED);
      }
      CompletionStage<WatchStream.OpenedWatch> opening;
      try {
        opening = subscribeUntracked(range, request);
      } catch (BackendFailure | RuntimeException | Error failure) {
        finishOperation();
        throw failure;
      }
      var result = new CompletableFuture<WatchStream.OpenedWatch>();
      opening.whenComplete((opened, failure) -> {
        if (failure != null) {
          result.completeExceptionally(failure);
          finishOperation();
          return;
        }
        TailerSubscription subscription = (TailerSubscription) opened.subscription();
        boolean[] installed = {false};
        streamScope.startIfOpen(() -> {
          synchronized (lifecycleLock) {
            if (!closed) installed[0] = subscriptions.add(subscription);
          }
        });
        if (!installed[0]) {
          Throwable closeFailure = WatchCleanup.closeAndCollect(subscription::close, null);
          subscription.drainCompletion().whenComplete((ignored, drainFailure) -> {
            Throwable cause = drainFailure == null ? closeFailure : unwrap(drainFailure);
            synchronized (lifecycleLock) {
              if (this.drainFailure == null) this.drainFailure = cause;
            }
            result.completeExceptionally(cause == null
                ? new BackendFailure(BackendFailure.Category.CANCELLED) : cause);
            finishOperation();
          });
          return;
        }
        subscription.drainCompletion().whenComplete(
            (ignored, drainFailure) -> subscriptionDrained(subscription, drainFailure));
        result.complete(opened);
        finishOperation();
      });
      return result;
    }

    private CompletionStage<WatchStream.OpenedWatch> subscribeUntracked(KeyRange range,
        Rpc.WatchCreateRequest request) throws BackendFailure {
      if (tailer.isClosed() || streamScope.isCancelled()) {
        throw new BackendFailure(BackendFailure.Category.UNAVAILABLE);
      }
      long retainedStateBytes = retainedStateBytes(range);
      long maximumPendingRevisionBytes = request.getFragment()
          ? LeaseBudgetProfile.EVENT_TAILER_MAX_REVISION_BYTES
          : Math.min(maximumResponseBytes, LeaseBudgetProfile.EVENT_TAILER_MAX_REVISION_BYTES);
      // The tailer retains its subscription index in OUTPUT alongside the pending revision.
      long outputReservationBytes = Math.addExact(maximumPendingRevisionBytes, retainedStateBytes);
      var reservation = new BudgetManager.Reservation(retainedStateBytes, 1, 1,
          outputReservationBytes, 0);
      WatchBudget watchBudget;
      RequestScope budgetScope = RequestScope.withoutDeadline();
      try {
        if (streamScope.isClosed()) throw new BackendFailure(BackendFailure.Category.CANCELLED);
        if (streamScope.isDeadlineExceeded()) throw new BackendFailure(BackendFailure.Category.TIMEOUT);
        watchBudget = new WatchBudget(budgetScope, budgets.admit(tableGroup, budgetScope, reservation),
            streamScope);
      } catch (BackendFailure | RuntimeException | Error failure) {
        closeAfterFailure(budgetScope, failure);
        recordResourceFailure(
            RequestMetrics.WatchCreateResourceFailureStage.REQUEST_ACCOUNT_ADMISSION, failure);
        throw failure;
      }
      BudgetManager.Account account = watchBudget.account;
      BudgetManager.Lease stateMemory = null;
      RequestScope.ScopedCleanup stateMemoryCleanup = null;
      SharedEventTailer.SubscriptionStart start;
      try {
        stateMemory = account.tryRetain(BudgetManager.Memory.HEAD, retainedStateBytes)
            .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
        stateMemoryCleanup = streamScope.registerScopedCleanup(stateMemory);
        if (streamScope.isClosed()) {
          throw new BackendFailure(BackendFailure.Category.CANCELLED);
        }
      } catch (BackendFailure | RuntimeException | Error failure) {
        recordResourceFailure(
            RequestMetrics.WatchCreateResourceFailureStage.STATE_MEMORY_RETENTION, failure);
        closeAfterFailure(stateMemoryCleanup == null ? stateMemory : stateMemoryCleanup, failure);
        closeAfterFailure(watchBudget, failure);
        throw failure;
      }

      try {
        start = tailer.subscribeWithCheckpoint(range, account,
            WatchEventBatchEncoder.MAX_PENDING_EVENTS_PER_WATCH, maximumPendingRevisionBytes,
            request.getStartRevision());
      } catch (BackendFailure | RuntimeException | Error failure) {
        recordResourceFailure(
            RequestMetrics.WatchCreateResourceFailureStage.TAILER_SUBSCRIPTION, failure);
        closeAfterFailure(stateMemoryCleanup, failure);
        closeAfterFailure(watchBudget, failure);
        throw failure;
      }

      SharedEventTailer.Subscription subscription = start.subscription();
      RequestScope.ScopedCleanup subscriptionCleanup = streamScope.registerScopedCleanup(subscription);
      if (streamScope.isClosed()) {
        BackendFailure cancelled = new BackendFailure(BackendFailure.Category.CANCELLED);
        closeAfterFailure(subscriptionCleanup, cancelled);
        closeAfterFailure(stateMemoryCleanup, cancelled);
        closeAfterFailure(watchBudget, cancelled);
        throw cancelled;
      }
      RequestScope.ScopedCleanup retainedStateMemoryCleanup = stateMemoryCleanup;
      CompletionStage<Optional<ControlSnapshot>> barrier;
      try {
        barrier = Objects.requireNonNull(control.read(streamScope), "Watch control snapshot future");
      } catch (RuntimeException | Error failure) {
        recordResourceFailure(
            RequestMetrics.WatchCreateResourceFailureStage.BARRIER_READ, failure);
        closeAfterFailure(subscriptionCleanup, failure);
        closeAfterFailure(retainedStateMemoryCleanup, failure);
        closeAfterFailure(watchBudget, failure);
        throw failure;
      }
      barrier = barrier.whenComplete((snapshot, failure) -> recordResourceFailure(
          RequestMetrics.WatchCreateResourceFailureStage.BARRIER_READ, failure));
      return barrier
          .thenCompose(snapshot -> openedWatch(range, snapshot, start, subscription,
              subscriptionCleanup, watchBudget, retainedStateMemoryCleanup, request))
          .whenComplete((opened, failure) -> {
            if (failure == null) return;
            closeAfterFailure(subscriptionCleanup, failure);
            closeAfterFailure(retainedStateMemoryCleanup, failure);
            closeAfterFailure(watchBudget, failure);
          });
    }

    private <T> CompletionStage<T> trackOperation(Supplier<? extends CompletionStage<T>> operation) {
      if (!beginOperation()) {
        return CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.CANCELLED));
      }
      CompletionStage<T> result;
      try {
        result = Objects.requireNonNull(operation.get(), "Watch session operation future");
      } catch (RuntimeException | Error failure) {
        finishOperation();
        if (failure instanceof Error fatal) throw fatal;
        return CompletableFuture.failedFuture(failure);
      }
      var completed = new CompletableFuture<T>();
      result.whenComplete((value, failure) -> {
        if (failure == null) completed.complete(value);
        else completed.completeExceptionally(failure);
        finishOperation();
      });
      return completed;
    }

    private boolean beginOperation() {
      boolean[] started = {false};
      boolean scopeOpen = streamScope.startIfOpen(() -> {
        synchronized (lifecycleLock) {
          if (closed) return;
          activeOperations = Math.addExact(activeOperations, 1);
          started[0] = true;
        }
      });
      return scopeOpen && started[0];
    }

    private void finishOperation() {
      synchronized (lifecycleLock) {
        if (activeOperations <= 0) {
          throw new IllegalStateException("Watch session operation count is unbalanced");
        }
        activeOperations--;
      }
      completeDrainIfReady();
    }

    private void subscriptionDrained(TailerSubscription subscription, Throwable failure) {
      synchronized (lifecycleLock) {
        subscriptions.remove(subscription);
        // Keep the first failed subscription's outcome without growing an error list per stream.
        if (drainFailure == null && failure != null) drainFailure = unwrap(failure);
      }
      completeDrainIfReady();
    }

    private void completeDrainIfReady() {
      Throwable failure;
      synchronized (lifecycleLock) {
        if (!closed || activeOperations != 0 || !subscriptions.isEmpty()) return;
        failure = drainFailure;
      }
      if (failure == null) drained.complete(null);
      else drained.completeExceptionally(failure);
    }

    @Override
    public void close() {
      List<TailerSubscription> closing;
      synchronized (lifecycleLock) {
        if (closed) return;
        closed = true;
        closing = new ArrayList<>(subscriptions);
      }
      Throwable failure = null;
      for (TailerSubscription subscription : closing) {
        failure = WatchCleanup.closeAndCollect(subscription::close, failure);
      }
      completeDrainIfReady();
      WatchCleanup.throwIfPresent(failure);
    }

    private CompletionStage<WatchStream.OpenedWatch> openedWatch(KeyRange range,
        Optional<ControlSnapshot> snapshot,
        SharedEventTailer.SubscriptionStart start, SharedEventTailer.Subscription subscription,
        RequestScope.ScopedCleanup subscriptionCleanup, WatchBudget watchBudget,
        RequestScope.ScopedCleanup stateMemoryCleanup,
        Rpc.WatchCreateRequest request) {
      if (streamScope.isCancelled() || tailer.isClosed()) {
        throw unavailable();
      }
      var state = snapshot.map(ControlSnapshot::state).orElseThrow(WatchGateway::unavailable);
      long revision = state.publishedRevision();
      if (revision < start.revision()) {
        throw unavailable();
      }
      if (request.getStartRevision() > 0
          && request.getStartRevision() < state.compactRevision()) {
        return CompletableFuture.completedFuture(new WatchStream.OpenedWatch(revision,
            state.publishedSequence(), state.compactRevision(),
            new TailerSubscription(subscription, subscriptionCleanup, watchBudget,
                stateMemoryCleanup, null, null,
                state.publishedSequence())));
      }
      if (request.getStartRevision() <= 0) {
        subscription.requireRevisionAtLeast(nextRevision(revision));
        return CompletableFuture.completedFuture(new WatchStream.OpenedWatch(revision,
            state.publishedSequence(),
            state.compactRevision() == 0 ? -1 : state.compactRevision(),
            new TailerSubscription(subscription, subscriptionCleanup, watchBudget,
                stateMemoryCleanup, null, null,
                state.publishedSequence())));
      }
      if (request.getStartRevision() <= revision) {
        if (historyPins == null) {
          return CompletableFuture.completedFuture(new WatchStream.OpenedWatch(revision,
              state.publishedSequence(),
              state.compactRevision() == 0 ? -1 : state.compactRevision(),
              new TailerSubscription(subscription, subscriptionCleanup, watchBudget,
                  stateMemoryCleanup, null, null,
                  state.publishedSequence())));
        }
        subscription.requireRevisionAtLeast(nextRevision(revision));
        var barrierSnapshot = snapshot.orElseThrow(WatchGateway::unavailable);
        CompletionStage<WatchHistoryReplay.OpenResult> replayOpen;
        try {
          watchBudget.holdThroughReplay(streamScope);
          replayOpen = WatchHistoryReplay.open(tableGroup, range, request.getStartRevision(),
              barrierSnapshot, state.publishedSequence(), authorizationDigest,
              historyPins, selectedEvents,
              budgets, watchBudget.account, streamScope, metrics::recordWatchReplayOpenResourceFailure,
              metrics::recordWatchReplayLookupAdmissionFailure);
        } catch (RuntimeException | Error failure) {
          recordResourceFailure(
              RequestMetrics.WatchCreateResourceFailureStage.HISTORY_REPLAY_OPEN, failure);
          throw failure;
        }
        return replayOpen.whenComplete((result, failure) -> recordResourceFailure(
            RequestMetrics.WatchCreateResourceFailureStage.HISTORY_REPLAY_OPEN, failure))
            .thenApply(result -> new WatchStream.OpenedWatch(result.revision(), result.sequence(),
                result.compactRevision() == 0 ? -1 : result.compactRevision(),
                new TailerSubscription(subscription, subscriptionCleanup, watchBudget,
                    stateMemoryCleanup, result.replay().orElse(null),
                    result.cleanup().orElse(null), result.sequence()),
                result.replay().isPresent()));
      }
      return CompletableFuture.completedFuture(new WatchStream.OpenedWatch(revision,
          state.publishedSequence(),
          state.compactRevision() == 0 ? -1 : state.compactRevision(),
          new TailerSubscription(subscription, subscriptionCleanup, watchBudget,
              stateMemoryCleanup, null, null,
              state.publishedSequence())));
    }

  }

  private void recordResourceFailure(
      RequestMetrics.WatchCreateResourceFailureStage stage, Throwable failure) {
    Throwable cause = failure;
    while ((cause instanceof CompletionException || cause instanceof ExecutionException)
        && cause.getCause() != null) {
      cause = cause.getCause();
    }
    if (cause instanceof BackendFailure backendFailure
        && backendFailure.category() == BackendFailure.Category.RESOURCE_LIMIT) {
      metrics.recordWatchCreateResourceFailure(stage);
    }
  }

  private static java.util.concurrent.CompletionException unavailable() {
    return new java.util.concurrent.CompletionException(
        new BackendFailure(BackendFailure.Category.UNAVAILABLE));
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable cause = failure;
    while ((cause instanceof CompletionException || cause instanceof ExecutionException)
        && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }

  private static Throwable collectDrainFailure(Throwable failure, Throwable drainFailure) {
    if (drainFailure == null) return failure;
    Throwable cause = unwrap(drainFailure);
    if (failure == null) return cause;
    if (failure != cause) failure.addSuppressed(cause);
    return failure;
  }

  private static long nextRevision(long revision) {
    return revision == Long.MAX_VALUE ? Long.MAX_VALUE : revision + 1;
  }

  private static long retainedStateBytes(KeyRange range) {
    Objects.requireNonNull(range, "range");
    long lowerBoundBytes = range.lowerInclusive().map(bound -> (long) bound.length()).orElse(0L);
    long upperBoundBytes = range.upperExclusive().map(bound -> (long) bound.length()).orElse(0L);
    return Math.addExact(ACTIVE_WATCH_STATE_BYTES, Math.addExact(lowerBoundBytes, upperBoundBytes));
  }

  private static void closeAfterFailure(AutoCloseable resource, Throwable failure) {
    if (resource == null) return;
    try {
      resource.close();
    } catch (Exception | Error cleanupFailure) {
      if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
    }
  }

  private static void closeScopedCleanup(RequestScope.ScopedCleanup cleanup, String description) {
    try {
      cleanup.close();
    } catch (RuntimeException | Error failure) {
      throw failure;
    } catch (Exception failure) {
      throw new IllegalStateException(description, failure);
    }
  }

  private static final class WatchBudget implements AutoCloseable {
    private enum Owner { STREAM, REPLAY, CLOSED }

    private final RequestScope scope;
    private final BudgetManager.Account account;
    private final AtomicReference<Owner> owner = new AtomicReference<>(Owner.STREAM);
    private final RequestScope.ScopedCleanup streamCleanup;

    private WatchBudget(RequestScope scope, BudgetManager.Account account, RequestScope streamScope) {
      this.scope = scope;
      this.account = Objects.requireNonNull(account, "Watch budget account");
      // Cancellation before the replay starts need not wait for a stalled barrier read.
      streamCleanup = streamScope.registerScopedCleanup(() -> {
        if (owner.compareAndSet(Owner.STREAM, Owner.CLOSED)) closeAccount();
      });
    }

    private void holdThroughReplay(RequestScope streamScope) {
      // Transfer ownership before starting replay; the RPC scope can then cancel without releasing it.
      if (!streamScope.startIfOpen(() -> {
        if (!owner.compareAndSet(Owner.STREAM, Owner.REPLAY)) throw unavailable();
      })) throw unavailable();
      closeScopedCleanup(streamCleanup, "Watch budget cleanup registration failed");
    }

    @Override
    public void close() {
      if (owner.getAndSet(Owner.CLOSED) == Owner.CLOSED) return;
      Throwable failure = WatchCleanup.closeAndCollect(this::closeAccount, null);
      failure = WatchCleanup.closeAndCollect(
          () -> closeScopedCleanup(streamCleanup, "Watch budget cleanup registration failed"), failure);
      WatchCleanup.throwIfPresent(failure);
    }

    private void closeAccount() {
      try (scope) {
        account.close();
      }
    }
  }

  private static final class TailerSubscription implements WatchStream.Subscription {
    private final SharedEventTailer.Subscription subscription;
    private final RequestScope.ScopedCleanup subscriptionCleanup;
    private final WatchBudget watchBudget;
    private final RequestScope.ScopedCleanup stateMemoryCleanup;
    private final WatchHistoryReplay history;
    private final RequestScope.ScopedCleanup historyCleanup;
    private final long barrierSequence;
    private final CompletableFuture<Void> drainCompletion = new CompletableFuture<>();
    private boolean liveDeliveryClosed;
    private boolean closed;

    private TailerSubscription(SharedEventTailer.Subscription subscription,
        RequestScope.ScopedCleanup subscriptionCleanup, WatchBudget watchBudget,
        RequestScope.ScopedCleanup stateMemoryCleanup,
        WatchHistoryReplay history, RequestScope.ScopedCleanup historyCleanup,
        long barrierSequence) {
      this.subscription = subscription;
      this.subscriptionCleanup = subscriptionCleanup;
      this.watchBudget = watchBudget;
      this.stateMemoryCleanup = stateMemoryCleanup;
      this.history = history;
      this.historyCleanup = historyCleanup;
      this.barrierSequence = barrierSequence;
    }

    private CompletionStage<Void> drainCompletion() {
      return drainCompletion.minimalCompletionStage();
    }

    @Override
    public synchronized Optional<WatchStream.Delivery> poll() {
      if (closed || liveDeliveryClosed) return Optional.empty();
      if (history != null) {
        Optional<WatchStream.Delivery> replayed = history.poll();
        if (replayed.isPresent()) return replayed;
        if (!history.complete() || history.termination().isPresent()) return Optional.empty();
      }
      Optional<SharedEventTailer.Delivery> polled;
      do {
        polled = subscription.poll();
        if (polled.isEmpty()) return Optional.empty();
        SharedEventTailer.Delivery candidate = polled.orElseThrow();
        // The snapshot barrier is the exclusive cursor (sequence, Long.MAX_VALUE). Revision
        // filtering is only a queue optimization; this identity check owns the handoff.
        var position = new EventPosition(candidate.sequence(), candidate.event().ordinal());
        if (position.compareTo(new EventPosition(barrierSequence, Long.MAX_VALUE)) <= 0) {
          candidate.close();
          polled = Optional.empty();
        }
      } while (polled.isEmpty() && !closed && !liveDeliveryClosed);
      if (closed || liveDeliveryClosed) {
        polled.ifPresent(SharedEventTailer.Delivery::close);
        return Optional.empty();
      }
      return polled.map(delivery -> new WatchStream.Delivery(
          delivery.sequence(), delivery.revision(), delivery.event(), delivery::close));
    }

    @Override
    public synchronized Optional<WatchTermination> termination() {
      if (liveDeliveryClosed) return Optional.empty();
      if (history != null) {
        Optional<WatchTermination> historyTermination = history.termination();
        if (historyTermination.isPresent()) return historyTermination;
        if (!history.complete()) return Optional.empty();
      }
      return subscription.termination().map(reason -> switch (reason) {
        case RESOURCE_LIMIT -> WatchTermination.resourceLimit(
            "etcd-hbase: Watch subscription resource limit exceeded");
        case TAILER_CLOSED -> WatchTermination.unavailable(
            "etcd-hbase: Watch event source is unavailable");
        case CANCELLED -> WatchTermination.cancelled(
            "etcd-hbase: Watch subscription was cancelled");
      });
    }

    @Override
    public synchronized boolean progressReady(long revision) {
      return !closed && (history == null || history.complete());
    }

    @Override
    public void setWakeup(Runnable wakeup) {
      synchronized (this) {
        if (closed || liveDeliveryClosed) return;
        if (history != null) history.setWakeup(wakeup);
        subscription.setWakeup(wakeup);
      }
    }

    @Override
    public synchronized void closeLiveDelivery() {
      if (closed || liveDeliveryClosed) return;
      liveDeliveryClosed = true;
      Throwable failure = WatchCleanup.closeAndCollect(() -> subscription.setWakeup(null), null);
      failure = WatchCleanup.closeAndCollect(
          () -> closeScopedCleanup(subscriptionCleanup, "Watch subscription cleanup failed"), failure);
      WatchCleanup.throwIfPresent(failure);
    }

    @Override
    public synchronized void close() {
      if (closed) return;
      Throwable failure = WatchCleanup.closeAndCollect(this::closeLiveDelivery, null);
      failure = WatchCleanup.closeAndCollect(
          () -> closeScopedCleanup(subscriptionCleanup,
              "Watch subscription cleanup registration failed"), failure);
      closed = true;
      failure = WatchCleanup.closeAndCollect(() -> {
        if (historyCleanup != null) {
          closeScopedCleanup(historyCleanup, "Watch history cleanup registration failed");
        } else if (history != null) {
          history.close();
        }
      }, failure);
      failure = WatchCleanup.closeAndCollect(
          () -> closeScopedCleanup(stateMemoryCleanup, "Watch state memory cleanup failed"), failure);
      Throwable closeFailure = failure;
      CompletionStage<Void> replayDrain = history == null
          ? CompletableFuture.completedFuture(null) : history.settled();
      replayDrain.whenComplete((ignored, replayFailure) -> {
        Throwable resultFailure = collectDrainFailure(closeFailure, replayFailure);
        resultFailure = WatchCleanup.closeAndCollect(watchBudget::close, resultFailure);
        if (resultFailure == null) drainCompletion.complete(null);
        else drainCompletion.completeExceptionally(resultFailure);
      });
      WatchCleanup.throwIfPresent(failure);
    }
  }
}
