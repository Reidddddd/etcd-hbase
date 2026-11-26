package io.etcdhbase.server.commit;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.ProgressFrontier;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.protocol.ErrorProjection;
import io.etcdhbase.grpc.protocol.ResponseHeaderFactory;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** One stream-scoped watch registry and single writer for control responses and event batches. */
final class WatchStream implements StreamObserver<Rpc.WatchRequest>, AutoCloseable {
  private static final long DEFAULT_PROGRESS_NOTIFY_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(10);
  private static final long DEFAULT_AUTHORIZATION_RECHECK_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(5);
  private static final long AUTHORIZATION_RECHECK_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);
  private static final int MAX_FILTERED_REVISIONS_PER_DRAIN = 128;
  private static final long REJECTED_CREATE_RESPONSE_WATCH_ID = -1;
  private static final String HISTORICAL_REPLAY_UNSUPPORTED_REASON =
      "etcd-hbase: historical Watch replay is not implemented";
  interface SessionProvider {
    CompletionStage<Session> open();
  }

  interface Session {
    long completedRevision();
    default long completedSequence() { return completedRevision(); }
    CompletionStage<Long> currentRevision();
    default boolean authorizationRevalidationRequired() { return false; }
    /** Rechecks the durable session and currently effective backend authorization. */
    default CompletionStage<Void> revalidateAuthorization() {
      return java.util.concurrent.CompletableFuture.completedFuture(null);
    }
    CompletionStage<OpenedWatch> subscribe(KeyRange range, Rpc.WatchCreateRequest request)
        throws BackendFailure;
    /** Stops session-owned resources and waits until their physical cleanup has drained. */
    default void close() {}
  }

  record OpenedWatch(long revision, long barrierSequence, long compactRevision,
      Subscription subscription, boolean historicalReplaySupported) {
    OpenedWatch(long revision, Subscription subscription) {
      this(revision, 0, -1, subscription, false);
    }

    OpenedWatch(long revision, long compactRevision, Subscription subscription) {
      this(revision, 0, compactRevision, subscription, false);
    }

    OpenedWatch(long revision, long barrierSequence, long compactRevision,
        Subscription subscription) {
      this(revision, barrierSequence, compactRevision, subscription, false);
    }

    OpenedWatch {
      if (revision < 0 || barrierSequence < 0 || compactRevision < -1) {
        throw new IllegalArgumentException("invalid Watch revision checkpoint");
      }
      Objects.requireNonNull(subscription, "subscription");
    }
  }

  interface Subscription extends AutoCloseable {
    Optional<Delivery> poll();
    Optional<WatchTermination> termination();
    void setWakeup(Runnable wakeup);
    default boolean progressReady(long revision) { return true; }
    default void closeLiveDelivery() {}
    @Override void close();
  }

  static final class Delivery implements AutoCloseable {
    private final long sequence;
    private final long revision;
    private final PreparedEvent event;
    private final Runnable release;
    private final AtomicBoolean closed = new AtomicBoolean();

    Delivery(long revision, PreparedEvent event, Runnable release) {
      this(Math.addExact(Objects.requireNonNull(event, "event").baseSequence(), 1),
          revision, event, release);
    }

    Delivery(long sequence, long revision, PreparedEvent event, Runnable release) {
      if (sequence <= 0) throw new IllegalArgumentException("watch event sequence must be positive");
      if (revision <= 0) throw new IllegalArgumentException("watch event revision must be positive");
      this.sequence = sequence;
      this.revision = revision;
      this.event = Objects.requireNonNull(event, "event");
      this.release = Objects.requireNonNull(release, "release");
    }

    long sequence() { return sequence; }
    long revision() { return revision; }
    long ordinal() { return event.ordinal(); }
    PreparedEvent event() { return event; }

    @Override public void close() {
      if (closed.compareAndSet(false, true)) release.run();
    }
  }

  private record PendingResponse(Rpc.WatchResponse response, Runnable afterSend) {
    private PendingResponse {
      Objects.requireNonNull(response, "response");
      Objects.requireNonNull(afterSend, "afterSend");
    }
  }

  private enum DrainKind {
    STOP, SEND_FAILURE, AUTHORIZE_RESPONSE, SEND_RESPONSE, PROCESS_REQUEST, PUMP
  }

  private record DrainAction(
      DrainKind kind, PendingResponse response, Rpc.WatchRequest request, Throwable failure) {}
  private record FragmentResponse(Rpc.WatchResponse response, boolean finalFragment) {
    private FragmentResponse { Objects.requireNonNull(response, "response"); }
  }

  /** Retains one complete revision and its delivery leases until its final fragment is sent. */
  private static final class ActiveFragment implements AutoCloseable {
    private final WatchResponseFragmenter.Cursor cursor;
    private final WatchEventBatchEncoder.Output output;
    private final long watchId;
    private boolean closed;

    private ActiveFragment(WatchResponseFragmenter.Cursor cursor, WatchEventBatchEncoder.Output output) {
      this.cursor = Objects.requireNonNull(cursor, "cursor");
      this.output = Objects.requireNonNull(output, "output");
      this.watchId = output.response().getWatchId();
    }

    private long watchId() { return watchId; }

    private synchronized FragmentResponse next() {
      if (closed) throw new IllegalStateException("watch fragments are closed");
      Rpc.WatchResponse response = cursor.next();
      return new FragmentResponse(response, !cursor.hasNext());
    }

    @Override public synchronized void close() {
      if (closed) return;
      closed = true;
      output.release();
    }
  }

  private record EventPoll(WatchEventBatchEncoder.Output output, boolean consumed) {}
  static record ProgressBarrier(long revision, long sequence) {
    ProgressBarrier {
      if (revision < 0 || sequence < 0) {
        throw new IllegalArgumentException("invalid Watch progress checkpoint");
      }
    }
  }

  private enum PumpResult { ADVANCED, IDLE }

  private final Object lock = new Object();
  private final StreamObserver<Rpc.WatchResponse> responseObserver;
  private final ServerCallStreamObserver<Rpc.WatchResponse> serverObserver;
  private final RequestScope streamScope;
  private final Executor worker;
  private final ScheduledExecutorService timers;
  private final SessionProvider sessions;
  private final long clusterId;
  private final long memberId;
  private final WatchRegistry watchRegistry;
  private final WatchEventBatchEncoder eventBatchEncoder;
  private final int maximumResponseBytes;
  private final long progressNotifyIntervalMillis;
  private final long authorizationRecheckIntervalMillis;
  private final ArrayDeque<PendingResponse> responses = new ArrayDeque<>();
  private final AtomicBoolean draining = new AtomicBoolean();
  private final AtomicBoolean drainRequested = new AtomicBoolean();
  private final AtomicBoolean sessionCloseStarted = new AtomicBoolean();

  private CompletionStage<Session> sessionFuture;
  private Session session;
  private Rpc.WatchRequest pendingRequest;
  private ScheduledFuture<?> drainYieldTask;
  private ScheduledFuture<?> progressNotifyTask;
  private ScheduledFuture<?> authorizationRecheckTask;
  private int nextWatchIndex;
  private int pendingTerminationResponses;
  private boolean processingRequest;
  private boolean requestOutstanding;
  private boolean inboundClosed;
  private boolean terminal;
  private boolean peerCancelled;
  private boolean progressNotifyInFlight;
  private boolean outboundAuthorizationCheckInFlight;
  private boolean periodicAuthorizationCheckInFlight;
  private boolean outboundAuthorizationReady;
  private long readinessGeneration;
  private boolean sendingResponse;
  private ActiveFragment activeFragment;
  private Throwable terminalFailure;
  private ProgressBarrier pendingProgress;

  WatchStream(StreamObserver<Rpc.WatchResponse> responseObserver, RequestScope streamScope,
      Executor worker, ScheduledExecutorService timers, SessionProvider sessions,
      long clusterId, long memberId, int maximumResponseBytes) {
    this(responseObserver, streamScope, worker, timers, sessions, clusterId, memberId,
        maximumResponseBytes, CompactedWatchIdBudget.processWide(),
        DEFAULT_PROGRESS_NOTIFY_INTERVAL_MILLIS);
  }

  WatchStream(StreamObserver<Rpc.WatchResponse> responseObserver, RequestScope streamScope,
      Executor worker, ScheduledExecutorService timers, SessionProvider sessions,
      long clusterId, long memberId, int maximumResponseBytes, long progressNotifyIntervalMillis) {
    this(responseObserver, streamScope, worker, timers, sessions, clusterId, memberId,
        maximumResponseBytes, CompactedWatchIdBudget.processWide(), progressNotifyIntervalMillis);
  }

  WatchStream(StreamObserver<Rpc.WatchResponse> responseObserver, RequestScope streamScope,
      Executor worker, ScheduledExecutorService timers, SessionProvider sessions,
      long clusterId, long memberId, int maximumResponseBytes,
      CompactedWatchIdBudget compactedWatchIdBudget) {
    this(responseObserver, streamScope, worker, timers, sessions, clusterId, memberId,
        maximumResponseBytes, compactedWatchIdBudget, DEFAULT_PROGRESS_NOTIFY_INTERVAL_MILLIS);
  }

  WatchStream(StreamObserver<Rpc.WatchResponse> responseObserver, RequestScope streamScope,
      Executor worker, ScheduledExecutorService timers, SessionProvider sessions,
      long clusterId, long memberId, int maximumResponseBytes,
      CompactedWatchIdBudget compactedWatchIdBudget, long progressNotifyIntervalMillis) {
    this(responseObserver, streamScope, worker, timers, sessions, clusterId, memberId,
        maximumResponseBytes, compactedWatchIdBudget, progressNotifyIntervalMillis,
        DEFAULT_AUTHORIZATION_RECHECK_INTERVAL_MILLIS);
  }

  WatchStream(StreamObserver<Rpc.WatchResponse> responseObserver, RequestScope streamScope,
      Executor worker, ScheduledExecutorService timers, SessionProvider sessions,
      long clusterId, long memberId, int maximumResponseBytes,
      CompactedWatchIdBudget compactedWatchIdBudget, long progressNotifyIntervalMillis,
      long authorizationRecheckIntervalMillis) {
    this.responseObserver = Objects.requireNonNull(responseObserver, "responseObserver");
    this.streamScope = Objects.requireNonNull(streamScope, "streamScope");
    this.worker = Objects.requireNonNull(worker, "worker");
    this.timers = Objects.requireNonNull(timers, "timers");
    this.sessions = Objects.requireNonNull(sessions, "sessions");
    if (clusterId == 0 || memberId == 0 || maximumResponseBytes < 1
        || progressNotifyIntervalMillis < 1 || authorizationRecheckIntervalMillis < 1) {
      throw new IllegalArgumentException("invalid Watch stream identity or response budget");
    }
    this.clusterId = clusterId;
    this.memberId = memberId;
    this.watchRegistry = new WatchRegistry(Objects.requireNonNull(
        compactedWatchIdBudget, "compactedWatchIdBudget"));
    this.eventBatchEncoder = new WatchEventBatchEncoder(clusterId, memberId, maximumResponseBytes);
    this.maximumResponseBytes = maximumResponseBytes;
    this.progressNotifyIntervalMillis = progressNotifyIntervalMillis;
    this.authorizationRecheckIntervalMillis = authorizationRecheckIntervalMillis;
    if (responseObserver instanceof ServerCallStreamObserver<?> serverCall) {
      @SuppressWarnings("unchecked")
      ServerCallStreamObserver<Rpc.WatchResponse> typed =
          (ServerCallStreamObserver<Rpc.WatchResponse>) serverCall;
      serverObserver = typed;
      serverObserver.disableAutoInboundFlowControl();
      serverObserver.setOnReadyHandler(this::onTransportReady);
      serverObserver.setOnCancelHandler(this::cancelFromPeer);
    } else {
      serverObserver = null;
    }
    streamScope.registerCleanup(this);
    requestNext();
  }

  @Override
  public void onNext(Rpc.WatchRequest request) {
    Objects.requireNonNull(request, "request");
    boolean accepted;
    synchronized (lock) {
      if (terminal || peerCancelled) return;
      accepted = requestOutstanding && pendingRequest == null;
      if (accepted) {
        requestOutstanding = false;
        pendingRequest = request;
      }
    }
    if (!accepted) {
      fail(new RejectedExecutionException("Watch stream exceeded its inbound request window"));
      return;
    }
    scheduleDrain();
  }

  @Override
  public void onError(Throwable failure) {
    Objects.requireNonNull(failure, "failure");
    cancelFromPeer();
  }

  @Override
  public void onCompleted() {
    // J47 must compare request half-close behavior with etcd before this lifecycle is treated as
    // contract.
    synchronized (lock) {
      if (terminal || peerCancelled) return;
      inboundClosed = true;
      requestOutstanding = false;
    }
    scheduleDrain();
  }

  private void scheduleDrain() {
    drainRequested.set(true);
    if (!draining.compareAndSet(false, true)) return;
    try {
      worker.execute(this::runOwnedDrain);
    } catch (RejectedExecutionException rejected) {
      drainAfterRejectedExecution(rejected);
    } catch (Error fatal) {
      try {
        drainRequested.set(false);
        fail(fatal, false);
        drainLoop();
      } finally {
        draining.set(false);
        drainRequested.set(false);
      }
      throw fatal;
    }
  }

  /** Runs only while this caller owns the stream writer, including an authorization handoff. */
  private void runOwnedDrain() {
    boolean handedOff = false;
    try {
      drainRequested.getAndSet(false);
      handedOff = drainLoop();
    } catch (RuntimeException | Error failure) {
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
    } finally {
      if (!handedOff) {
        draining.set(false);
        if (drainRequested.get() || hasRunnableWork()) scheduleDrain();
      }
    }
  }

  /** The executor rejected the only writer; this caller owns the drain until it returns. */
  private void drainAfterRejectedExecution(RejectedExecutionException rejected) {
    Throwable cleanupFailure = null;
    try {
      drainRequested.set(false);
      fail(rejected, false);
    } catch (RuntimeException | Error failure) {
      cleanupFailure = failure;
    }
    try {
      drainLoop();
    } catch (RuntimeException | Error failure) {
      if (cleanupFailure == null) cleanupFailure = failure;
      else if (failure != cleanupFailure) cleanupFailure.addSuppressed(failure);
    } finally {
      draining.set(false);
      drainRequested.set(false);
    }
    if (cleanupFailure instanceof Error fatal) throw fatal;
    if (cleanupFailure instanceof RuntimeException failure) throw failure;
  }

  /** Returns true only when an authorization completion now owns the writer. */
  private boolean drainLoop() {
    int filteredRevisions = 0;
    for (;;) {
      ActiveFragment cancelledFragment = null;
      synchronized (lock) {
        if (activeFragment != null && isCancelRequest(pendingRequest)
            && activeFragment.watchId() == pendingRequest.getCancelRequest().getWatchId()) {
          cancelledFragment = activeFragment;
          activeFragment = null;
        }
      }
      if (cancelledFragment != null) {
        try { cancelledFragment.close(); }
        catch (RuntimeException | Error failure) {
          fail(failure);
          if (failure instanceof Error fatal) throw fatal;
          return false;
        }
      }
      DrainAction action = nextDrainAction();
      switch (action.kind()) {
        case STOP -> { return false; }
        case SEND_FAILURE -> {
          sendTerminalFailure(action.failure());
          return false;
        }
        case AUTHORIZE_RESPONSE -> {
          if (startOutboundAuthorizationCheck()) return true;
          continue;
        }
        case SEND_RESPONSE -> {
          sendResponse(action.response());
          continue;
        }
        case PROCESS_REQUEST -> {
          processRequest(action.request());
          continue;
        }
        case PUMP -> { }
      }

      PumpResult pumpResult = pumpOneWatcher();
      if (pumpResult == PumpResult.ADVANCED) {
        requestNext();
        if (++filteredRevisions >= MAX_FILTERED_REVISIONS_PER_DRAIN) {
          scheduleDrainYield();
          return false;
        }
        continue;
      }
      if (enqueuePendingProgress()) continue;
      requestNext();
      return false;
    }
  }

  private DrainAction nextDrainAction() {
    synchronized (lock) {
      if (peerCancelled) return new DrainAction(DrainKind.STOP, null, null, null);
      if (terminalFailure != null) {
        Throwable failure = terminalFailure;
        terminalFailure = null;
        return new DrainAction(DrainKind.SEND_FAILURE, null, null, failure);
      }
      if (terminal) return new DrainAction(DrainKind.STOP, null, null, null);
      if (pendingTerminationResponses > 0) return new DrainAction(DrainKind.STOP, null, null, null);
      if (outboundAuthorizationCheckInFlight || periodicAuthorizationCheckInFlight) {
        return new DrainAction(DrainKind.STOP, null, null, null);
      }
      if (activeFragment != null && isCancelRequest(pendingRequest) && !processingRequest) {
        Rpc.WatchRequest request = pendingRequest;
        pendingRequest = null;
        processingRequest = true;
        return new DrainAction(DrainKind.PROCESS_REQUEST, null, request, null);
      }
      if (activeFragment != null) {
        if (!isReady()) return new DrainAction(DrainKind.STOP, null, null, null);
        if (authorizationRevalidationRequiredLocked() && !outboundAuthorizationReady) {
          outboundAuthorizationCheckInFlight = true;
          return new DrainAction(DrainKind.AUTHORIZE_RESPONSE, null, null, null);
        }
        outboundAuthorizationReady = false;
        sendingResponse = true;
        ActiveFragment fragments = activeFragment;
        FragmentResponse next = fragments.next();
        return new DrainAction(DrainKind.SEND_RESPONSE, new PendingResponse(next.response(),
            () -> finishFragmentResponse(fragments, next.finalFragment())), null, null);
      }
      if (!responses.isEmpty()) {
        if (!isReady()) return new DrainAction(DrainKind.STOP, null, null, null);
        if (authorizationRevalidationRequiredLocked() && !outboundAuthorizationReady) {
          outboundAuthorizationCheckInFlight = true;
          return new DrainAction(DrainKind.AUTHORIZE_RESPONSE, null, null, null);
        }
        outboundAuthorizationReady = false;
        sendingResponse = true;
        return new DrainAction(DrainKind.SEND_RESPONSE, responses.removeFirst(), null, null);
      }
      if (pendingRequest != null && !processingRequest) {
        Rpc.WatchRequest request = pendingRequest;
        pendingRequest = null;
        processingRequest = true;
        return new DrainAction(DrainKind.PROCESS_REQUEST, null, request, null);
      }
      if (!isReady()) return new DrainAction(DrainKind.STOP, null, null, null);
      return new DrainAction(DrainKind.PUMP, null, null, null);
    }
  }

  private void sendTerminalFailure(Throwable failure) {
    Throwable cause = unwrap(failure);
    try {
      responseObserver.onError(cause instanceof StatusRuntimeException statusFailure
          ? statusFailure : ErrorProjection.project(cause));
    } finally {
      streamScope.cancel();
    }
  }

  private void sendResponse(PendingResponse response) {
    try {
      responseObserver.onNext(response.response());
      response.afterSend().run();
    } catch (RuntimeException | Error failure) {
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
    } finally {
      synchronized (lock) { sendingResponse = false; }
      refreshAuthorizationRecheckSchedule();
    }
  }

  private void onTransportReady() {
    synchronized (lock) {
      readinessGeneration++;
      outboundAuthorizationReady = false;
    }
    scheduleDrain();
  }

  private boolean startOutboundAuthorizationCheck() {
    final long startedAtReadinessGeneration;
    synchronized (lock) { startedAtReadinessGeneration = readinessGeneration; }
    return OutboundAuthorizationHandoff.start(this::requestAuthorizationCheck, failure -> {
      synchronized (lock) {
        outboundAuthorizationCheckInFlight = false;
        outboundAuthorizationReady = failure == null && !terminal && !peerCancelled
            && readinessGeneration == startedAtReadinessGeneration && isReady();
      }
      if (failure != null) fail(unwrap(failure));
    }, this::runOwnedDrain);
  }

  private void startPeriodicAuthorizationCheck() {
    requestAuthorizationCheck(failure -> executeCompletion(() -> {
      synchronized (lock) {
        periodicAuthorizationCheckInFlight = false;
        outboundAuthorizationReady = false;
      }
      if (failure != null) {
        fail(unwrap(failure));
      } else {
        refreshAuthorizationRecheckSchedule();
        scheduleDrain();
      }
    }, schedulingFailure -> {
      synchronized (lock) { periodicAuthorizationCheckInFlight = false; }
    }));
  }

  private void requestAuthorizationCheck(Consumer<Throwable> completed) {
    var settled = new AtomicBoolean();
    final ScheduledFuture<?> timeout;
    try {
      timeout = timers.schedule(() -> completeAuthorizationRequest(settled, completed,
          new BackendFailure(BackendFailure.Category.TIMEOUT), null),
          AUTHORIZATION_RECHECK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException rejected) {
      completeAuthorizationRequest(settled, completed, rejected, null);
      return;
    } catch (Error fatal) {
      completeAuthorizationRequest(settled, completed, fatal, null);
      throw fatal;
    }

    session().whenComplete((openedSession, sessionFailure) -> {
      if (sessionFailure != null) {
        completeAuthorizationRequest(settled, completed, unwrap(sessionFailure), timeout);
        return;
      }
      CompletionStage<Void> authorization;
      try {
        authorization = Objects.requireNonNull(openedSession.revalidateAuthorization(),
            "Watch authorization revalidation future");
      } catch (RuntimeException | Error failure) {
        completeAuthorizationRequest(settled, completed, failure, timeout);
        if (failure instanceof Error fatal) throw fatal;
        return;
      }
      authorization.whenComplete((ignored, failure) -> completeAuthorizationRequest(settled,
          completed, failure == null ? null : unwrap(failure), timeout));
    });
  }

  private static void completeAuthorizationRequest(AtomicBoolean settled,
      Consumer<Throwable> completed, Throwable failure, ScheduledFuture<?> timeout) {
    if (!settled.compareAndSet(false, true)) return;
    if (timeout != null) timeout.cancel(false);
    completed.accept(failure);
  }

  private void processRequest(Rpc.WatchRequest request) {
    switch (request.getRequestUnionCase()) {
      case CREATE_REQUEST -> processCreate(request.getCreateRequest());
      case CANCEL_REQUEST -> processCancel(request.getCancelRequest());
      case PROGRESS_REQUEST -> processProgressRequest();
      case REQUESTUNION_NOT_SET -> finishRequestWithoutResponse();
    }
  }

  private void processProgressRequest() {
    boolean hasActiveWatch;
    synchronized (lock) {
      hasActiveWatch = watchRegistry.activeSnapshot().stream()
          .anyMatch(watcher -> watcher.createdSent() && !watcher.isClosed());
    }
    if (!hasActiveWatch) {
      finishRequestWithoutResponse();
      return;
    }
    session().whenComplete((openedSession, sessionFailure) -> executeCompletion(() -> {
      if (sessionFailure != null) {
        fail(sessionFailure);
        return;
      }
      CompletionStage<Long> revision;
      try {
        revision = Objects.requireNonNull(openedSession.currentRevision(),
            "current Watch revision future");
      } catch (RuntimeException | Error failure) {
        fail(failure);
        if (failure instanceof Error fatal) throw fatal;
        return;
      }
      revision.whenComplete((currentRevision, revisionFailure) -> executeCompletion(() ->
          completeProgressRequest(openedSession, currentRevision, revisionFailure)));
    }));
  }

  private void completeProgressRequest(Session openedSession, Long revision, Throwable failure) {
    if (failure != null) {
      fail(unwrap(failure));
      return;
    }
    if (revision == null || revision < 0) {
      fail(new IllegalStateException("Watch session revision is invalid"));
      return;
    }
    boolean ready;
    try {
      synchronized (lock) {
        if (terminal || peerCancelled) return;
        List<WatchRegistry.Watcher> active = watchRegistry.activeSnapshot().stream()
            .filter(watcher -> watcher.createdSent() && !watcher.isClosed())
            .toList();
        // etcd v3.5.23 drops the request while a stream watcher is unsynced or starts in the future.
        ready = !active.isEmpty() && openedSession.completedRevision() >= revision
            && active.stream().allMatch(watcher -> watcher.startRevision() <= 0
                || watcher.startRevision() <= revision)
            && active.stream().allMatch(watcher -> watcher.progressReady(revision));
        long sequence = openedSession.completedSequence();
        if (sequence < 0) throw new IllegalStateException("Watch session sequence is invalid");
        if (ready) pendingProgress = new ProgressBarrier(revision, sequence);
        else processingRequest = false;
      }
    } catch (RuntimeException | Error failureToCheck) {
      fail(failureToCheck);
      if (failureToCheck instanceof Error fatal) throw fatal;
      return;
    }
    if (!ready) requestNext();
    scheduleDrain();
  }

  private void processCreate(Rpc.WatchCreateRequest request) {
    if (request == null) {
      finishRequestWithoutResponse();
      return;
    }
    CompletionStage<Session> opening = session();
    opening.whenComplete((openedSession, failure) -> executeCompletion(() -> {
      if (failure != null) {
        fail(failure);
        return;
      }
      completeCreate(openedSession, request);
    }));
  }

  private CompletionStage<Session> session() {
    synchronized (lock) {
      if (session != null) return java.util.concurrent.CompletableFuture.completedFuture(session);
      if (sessionFuture != null) return sessionFuture;
      try {
        sessionFuture = Objects.requireNonNull(sessions.open(), "Watch session future");
      } catch (RuntimeException | Error failure) {
        sessionFuture = java.util.concurrent.CompletableFuture.failedFuture(failure);
      }
      return sessionFuture;
    }
  }

  private void completeCreate(Session openedSession, Rpc.WatchCreateRequest request) {
    Objects.requireNonNull(openedSession, "openedSession");
    try {
      synchronized (lock) { session = openedSession; }
      KeyRange range;
      try {
        range = keyRange(request);
      } catch (InvalidRange invalidRange) {
        rejectCreateAtCurrentRevision(openedSession, invalidRange.getMessage());
        return;
      }
      if (rejectUnadmittedCreate(openedSession, request)) return;
      subscribe(openedSession, request, range);
    } catch (RuntimeException | Error failure) {
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
    }
  }

  private boolean rejectUnadmittedCreate(Session openedSession, Rpc.WatchCreateRequest request) {
    boolean stopped;
    WatchRegistry.Rejection rejection = WatchRegistry.Rejection.NONE;
    synchronized (lock) {
      // Half-close stops new requests, but a create accepted before it still owns completion.
      stopped = terminal || peerCancelled;
      if (!stopped) rejection = watchRegistry.checkCreate(request.getWatchId());
      if (stopped) processingRequest = false;
    }
    if (stopped) {
      scheduleDrain();
      return true;
    }
    if (rejection == WatchRegistry.Rejection.NONE) return false;
    rejectCreateAtCurrentRevision(openedSession, rejectionReason(rejection));
    return true;
  }

  private void subscribe(Session openedSession, Rpc.WatchCreateRequest request, KeyRange range) {
    CompletionStage<OpenedWatch> opening;
    try {
      opening = Objects.requireNonNull(
          openedSession.subscribe(range, request), "opened Watch subscription");
    } catch (BackendFailure failure) {
      if (failure.category() == BackendFailure.Category.RESOURCE_LIMIT) {
        rejectCreateAtCurrentRevision(openedSession, "etcd-hbase: Watch resource limit exceeded");
      } else {
        fail(failure);
      }
      return;
    }
    opening.whenComplete((opened, failure) -> executeCompletion(
        () -> completeSubscribedCreate(openedSession, request, opened, failure),
        schedulingFailure -> closeUnregisteredWatch(opened, failure, schedulingFailure)));
  }

  private void completeSubscribedCreate(Session openedSession, Rpc.WatchCreateRequest request,
      OpenedWatch opened, Throwable subscriptionFailure) {
    if (subscriptionFailure != null) {
      handleSubscriptionFailure(openedSession, subscriptionFailure);
      return;
    }
    try {
      Objects.requireNonNull(opened, "opened Watch subscription");
      if (request.getStartRevision() != 0
          && request.getStartRevision() < opened.compactRevision()) {
        completeCompactedCreate(opened, request);
        return;
      }
      if (request.getStartRevision() > 0 && request.getStartRevision() <= opened.revision()
          && !opened.historicalReplaySupported()) {
        rejectUnsupportedHistoricalReplay(opened);
        return;
      }
      registerSubscribedWatch(opened, request);
    } catch (RuntimeException | Error failure) {
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
    }
  }

  private void handleSubscriptionFailure(Session openedSession, Throwable failure) {
    Throwable cause = unwrap(failure);
    if (cause instanceof BackendFailure backendFailure
        && backendFailure.category() == BackendFailure.Category.RESOURCE_LIMIT) {
      rejectCreateAtCurrentRevision(openedSession, "etcd-hbase: Watch resource limit exceeded");
    } else {
      fail(cause);
    }
  }

  private void rejectUnsupportedHistoricalReplay(OpenedWatch opened) {
    opened.subscription().close();
    rejectCreate(opened.revision(), HISTORICAL_REPLAY_UNSUPPORTED_REASON);
  }

  private void registerSubscribedWatch(OpenedWatch opened, Rpc.WatchCreateRequest request) {
    WatchRegistry.Registration registration = null;
    boolean stopped;
    synchronized (lock) {
      // Preserve an already accepted create across client half-close.
      stopped = terminal || peerCancelled;
      if (!stopped) {
        long minimumEventRevision = request.getStartRevision() <= 0
            ? nextRevision(opened.revision()) : request.getStartRevision();
        registration = watchRegistry.registerActive(request.getWatchId(), request.getStartRevision(), minimumEventRevision,
            request.getPrevKv(),
            request.getFiltersList().contains(Rpc.WatchCreateRequest.FilterType.NOPUT),
            request.getFiltersList().contains(Rpc.WatchCreateRequest.FilterType.NODELETE),
            request.getProgressNotify(), request.getFragment(), opened.subscription());
        if (registration.rejection() == WatchRegistry.Rejection.NONE) {
          WatchRegistry.Watcher watcher = registration.watcher();
          enqueueResponse(createdResponse(registration.watchId(), opened.revision()), () -> {
            watcher.markCreatedSent();
            refreshProgressNotifySchedule();
            refreshAuthorizationRecheckSchedule();
            requestNext();
          });
        }
      }
      processingRequest = false;
    }
    if (!stopped && registration != null
        && registration.rejection() == WatchRegistry.Rejection.NONE) {
      try {
        registration.watcher().setWakeup(this::scheduleDrain);
      } catch (RuntimeException | Error failure) {
        fail(failure);
        if (failure instanceof Error fatal) throw fatal;
        return;
      }
    }
    if (stopped || registration == null
        || registration.rejection() != WatchRegistry.Rejection.NONE) {
      opened.subscription().close();
      if (stopped) {
        scheduleDrain();
      } else {
        rejectCreate(opened.revision(), rejectionReason(registration.rejection()));
      }
      return;
    }
    scheduleDrain();
  }

  private void rejectCreateAtCurrentRevision(Session openedSession, String reason) {
    CompletionStage<Long> currentRevision;
    try {
      currentRevision = Objects.requireNonNull(
          openedSession.currentRevision(), "current Watch revision future");
    } catch (RuntimeException | Error failure) {
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }
    currentRevision.whenComplete((revision, failure) -> executeCompletion(() -> {
      if (failure != null) {
        fail(unwrap(failure));
      } else if (revision == null || revision < 0) {
        fail(new IllegalStateException("Watch session revision is invalid"));
      } else {
        rejectCreate(revision, reason);
      }
    }));
  }

  private void completeCompactedCreate(OpenedWatch opened, Rpc.WatchCreateRequest request) {
    try {
      opened.subscription().close();
    } catch (RuntimeException | Error failure) {
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }

    boolean stopped;
    WatchRegistry.Registration registration = null;
    synchronized (lock) {
      // A compacted accepted create must still receive its Created/Canceled response.
      stopped = terminal || peerCancelled;
      if (!stopped) {
        registration = watchRegistry.retainCompacted(request.getWatchId());
        if (registration.rejection() == WatchRegistry.Rejection.NONE) {
          try {
            enqueueResponse(createdResponse(registration.watchId(), opened.revision()), () -> {});
            enqueueResponse(compactedResponse(
                registration.watchId(), 0, opened.compactRevision()), this::requestNext);
          } catch (RuntimeException | Error failure) {
            watchRegistry.removeCompacted(registration.watchId());
            throw failure;
          }
        }
      }
      processingRequest = false;
    }
    if (stopped) {
      scheduleDrain();
    } else if (registration.rejection() != WatchRegistry.Rejection.NONE) {
      rejectCreate(opened.revision(), rejectionReason(registration.rejection()));
    } else {
      scheduleDrain();
    }
  }

  private static String rejectionReason(WatchRegistry.Rejection rejection) {
    return switch (rejection) {
      case DUPLICATE_ID -> "mvcc: duplicate watch ID provided on the WatchStream";
      case STREAM_LIMIT -> "etcd-hbase: Watch stream subscription limit exceeded";
      case RETENTION_LIMIT -> "etcd-hbase: compacted Watch ID retention limit exceeded";
      case NONE -> throw new IllegalArgumentException("successful Watch registration has no rejection");
    };
  }

  private void processCancel(Rpc.WatchCancelRequest request) {
    if (request == null) {
      finishRequestWithoutResponse();
      return;
    }
    WatchRegistry.Watcher watcher;
    boolean compactedTombstone;
    Session currentSession;
    synchronized (lock) {
      watcher = watchRegistry.removeActive(request.getWatchId());
      compactedTombstone = watcher == null && watchRegistry.removeCompacted(request.getWatchId());
      currentSession = session;
      if (watcher == null) processingRequest = false;
    }
    refreshProgressNotifySchedule();
    refreshAuthorizationRecheckSchedule();
    if (watcher == null && !compactedTombstone) {
      requestNext();
      return;
    }
    if (watcher != null) {
      try {
        watcher.close();
      } catch (RuntimeException | Error failure) {
        fail(failure);
        if (failure instanceof Error fatal) throw fatal;
        return;
      }
    }
    CompletionStage<Long> revision;
    try {
      revision = currentSession == null
          ? java.util.concurrent.CompletableFuture.completedFuture(0L)
          : Objects.requireNonNull(currentSession.currentRevision(), "current Watch revision future");
    } catch (RuntimeException | Error failure) {
      synchronized (lock) { processingRequest = false; }
      fail(failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }
    revision.whenComplete((currentRevision, failure) -> executeCompletion(() ->
        completeCancel(request.getWatchId(), currentRevision, failure)));
  }

  private void completeCancel(long watchId, Long revision, Throwable failure) {
    if (failure != null) {
      synchronized (lock) { processingRequest = false; }
      fail(unwrap(failure));
      return;
    }
    if (revision == null || revision < 0) {
      synchronized (lock) { processingRequest = false; }
      fail(new IllegalStateException("Watch session revision is invalid"));
      return;
    }
    synchronized (lock) {
      processingRequest = false;
      if (terminal || peerCancelled) return;
      enqueueResponse(cancelledResponse(watchId, revision), this::requestNext);
    }
    scheduleDrain();
  }

  private void rejectCreate(long revision, String reason) {
    enqueueResponse(Rpc.WatchResponse.newBuilder()
        .setHeader(header(revision))
        .setWatchId(REJECTED_CREATE_RESPONSE_WATCH_ID)
        .setCreated(true)
        .setCanceled(true)
        .setCancelReason(reason)
        .build(), this::requestNext);
    synchronized (lock) { processingRequest = false; }
    scheduleDrain();
  }

  private void finishRequestWithoutResponse() {
    synchronized (lock) { processingRequest = false; }
    requestNext();
    scheduleDrain();
  }

  private void enqueueResponse(Rpc.WatchResponse response, Runnable afterSend) {
    synchronized (lock) { responses.addLast(new PendingResponse(response, afterSend)); }
  }

  private PumpResult pumpOneWatcher() {
    List<WatchRegistry.Watcher> snapshot;
    ProgressBarrier streamProgress;
    int first;
    synchronized (lock) {
      snapshot = watchRegistry.activeSnapshot();
      if (snapshot.isEmpty()) return PumpResult.IDLE;
      streamProgress = pendingProgress;
      first = Math.floorMod(nextWatchIndex, snapshot.size());
    }
    for (int offset = 0; offset < snapshot.size(); offset++) {
      int index = (first + offset) % snapshot.size();
      WatchRegistry.Watcher watcher = snapshot.get(index);
      if (!watcher.createdSent()) continue;
      ProgressBarrier watchProgress = streamProgress == null ? watcher.pendingNotifyProgress() : null;
      ProgressBarrier limit = streamProgress != null ? streamProgress : watchProgress;
      WatchEventBatchEncoder.Poll poll;
      synchronized (watcher) { poll = eventBatchEncoder.nextBatch(watcher, limit == null ? null : limit.revision()); }
      if (poll.termination() != null) {
        terminateWatcher(watcher, poll.termination());
        advanceWatchIndex(index);
        return PumpResult.ADVANCED;
      }
      WatchEventBatchEncoder.Output output = poll.output();
      if (output != null) {
        advanceWatchIndex(index);
        return queueOutput(watcher, output);
      }
      if (poll.consumed()) {
        advanceWatchIndex(index);
        return PumpResult.ADVANCED;
      }
      if (watchProgress != null && enqueueWatcherProgress(watcher, watchProgress)) {
        advanceWatchIndex(index);
        return PumpResult.ADVANCED;
      }
    }
    return PumpResult.IDLE;
  }

  private PumpResult queueOutput(WatchRegistry.Watcher watcher, WatchEventBatchEncoder.Output output) {
    boolean transferred = false;
    try {
      WatchResponseFragmenter.Cursor cursor = WatchResponseFragmenter.split(
          output.response(), maximumResponseBytes);
      ActiveFragment fragments = new ActiveFragment(cursor, output);
      synchronized (lock) {
        if (terminal || peerCancelled || !watchRegistry.containsActive(watcher.id(), watcher)
            || watcher.isClosed()) return PumpResult.IDLE;
        // Only the drain writer pumps events, and it sends an active fragment before pumping again.
        if (activeFragment != null) {
          throw new IllegalStateException("a Watch revision is already being fragmented");
        }
        activeFragment = fragments;
        transferred = true;
      }
      return PumpResult.ADVANCED;
    } catch (BackendFailure oversizedEvent) {
      terminateWatcher(watcher, WatchTermination.resourceLimit(
          "etcd-hbase: Watch response exceeds the configured message budget"));
      return PumpResult.ADVANCED;
    } finally {
      if (!transferred) output.release();
    }
  }

  private void finishFragmentResponse(ActiveFragment fragments, boolean finalFragment) {
    if (!finalFragment) {
      requestNext();
      return;
    }
    synchronized (lock) {
      if (activeFragment != fragments) return;
      activeFragment = null;
    }
    fragments.close();
    requestNext();
  }

  private boolean enqueueWatcherProgress(WatchRegistry.Watcher watcher, ProgressBarrier barrier) {
    synchronized (lock) {
      if (terminal || peerCancelled || pendingProgress != null
          || !watchRegistry.containsActive(watcher.id(), watcher) || watcher.pendingNotifyProgress() != barrier) {
        return false;
      }
      ProgressFrontier.WatcherState frontier = drainedProgressFrontier(watcher, barrier);
      long safeRevision = ProgressFrontier.forWatcher(List.of(frontier), watcher.id()).orElse(-1);
      if (safeRevision != barrier.revision()) {
        watcher.clearNotifyProgress(barrier);
        return false;
      }
      watcher.clearNotifyProgress(barrier);
      responses.addLast(new PendingResponse(
          Rpc.WatchResponse.newBuilder().setHeader(header(safeRevision))
              .setWatchId(watcher.id()).build(), this::requestNext));
      return true;
    }
  }

  private void advanceWatchIndex(int index) {
    synchronized (lock) {
      nextWatchIndex = (index + 1) % Math.max(1, watchRegistry.activeCount());
    }
  }

  private boolean enqueuePendingProgress() {
    boolean noActiveWatch;
    boolean verified;
    synchronized (lock) {
      if (pendingProgress == null) return false;
      ProgressBarrier barrier = pendingProgress;
      List<ProgressFrontier.WatcherState> frontiers = watchRegistry.activeSnapshot().stream()
          .filter(watcher -> watcher.createdSent() && !watcher.isClosed())
          .map(watcher -> drainedProgressFrontier(watcher, barrier))
          .toList();
      noActiveWatch = frontiers.isEmpty();
      verified = !noActiveWatch
          && ProgressFrontier.forAllActive(frontiers).orElse(-1) == barrier.revision();
      long revision = barrier.revision();
      pendingProgress = null;
      if (noActiveWatch || !verified) {
        processingRequest = false;
      } else {
        responses.addLast(new PendingResponse(
            Rpc.WatchResponse.newBuilder().setHeader(header(revision)).setWatchId(-1).build(), () -> {
              synchronized (lock) {
                watchRegistry.activeSnapshot().forEach(watcher -> watcher.clearNotifyProgressThrough(revision));
                processingRequest = false;
              }
              requestNext();
            }));
      }
    }
    if (noActiveWatch || !verified) requestNext();
    return true;
  }

  // Called only after the pump found no eligible event for this watcher (or every active watcher).
  // Tailer publishes all deliveries before advancing completedRevision; later deliveries cannot
  // belong to the verified prefix. Replay readiness also requires its historical prefix drained.
  private ProgressFrontier.WatcherState drainedProgressFrontier(
      WatchRegistry.Watcher watcher, ProgressBarrier barrier) {
    boolean active = watcher.createdSent() && !watcher.isClosed();
    boolean replayComplete = watcher.progressReady(barrier.revision());
    boolean futureStartReached = watcher.startRevision() <= 0
        || watcher.startRevision() <= barrier.revision();
    Optional<ProgressFrontier.VerifiedFrontier> verifiedThrough =
        session == null || session.completedRevision() < barrier.revision()
            ? Optional.empty()
            : Optional.of(new ProgressFrontier.VerifiedFrontier(
                barrier.sequence(), barrier.revision()));
    return new ProgressFrontier.WatcherState(watcher.id(), verifiedThrough, active,
        replayComplete, futureStartReached, false);
  }

  private void refreshProgressNotifySchedule() {
    ScheduledFuture<?> cancel = null;
    synchronized (lock) {
      boolean needed = !terminal && !peerCancelled && hasProgressNotifyWatcherLocked();
      if (!needed && progressNotifyTask != null) {
        cancel = progressNotifyTask;
        progressNotifyTask = null;
      } else if (needed && progressNotifyTask == null && !progressNotifyInFlight) {
        try {
          progressNotifyTask = timers.schedule(this::onProgressNotifyTick,
              progressNotifyIntervalMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException rejected) {
          fail(rejected);
        }
      }
    }
    if (cancel != null) cancel.cancel(false);
  }

  private void refreshAuthorizationRecheckSchedule() {
    ScheduledFuture<?> cancel = null;
    synchronized (lock) {
      boolean needed = !terminal && !peerCancelled && hasCreatedActiveWatcherLocked()
          && authorizationRevalidationRequiredLocked();
      if (!needed && authorizationRecheckTask != null) {
        cancel = authorizationRecheckTask;
        authorizationRecheckTask = null;
      } else if (needed && authorizationRecheckTask == null
          && !periodicAuthorizationCheckInFlight) {
        try {
          authorizationRecheckTask = timers.schedule(this::onAuthorizationRecheckTick,
              authorizationRecheckIntervalMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException rejected) {
          fail(rejected);
        }
      }
    }
    if (cancel != null) cancel.cancel(false);
  }

  private boolean hasCreatedActiveWatcherLocked() {
    return watchRegistry.activeSnapshot().stream()
        .anyMatch(watcher -> watcher.createdSent() && !watcher.isClosed());
  }

  private boolean authorizationRevalidationRequiredLocked() {
    return session != null && session.authorizationRevalidationRequired();
  }

  private void onAuthorizationRecheckTick() {
    boolean defer;
    synchronized (lock) {
      authorizationRecheckTask = null;
      if (terminal || peerCancelled || !hasCreatedActiveWatcherLocked()
          || !authorizationRevalidationRequiredLocked()) return;
      defer = outboundAuthorizationCheckInFlight || periodicAuthorizationCheckInFlight
          || sendingResponse;
      if (!defer) {
        periodicAuthorizationCheckInFlight = true;
        outboundAuthorizationReady = false;
      }
    }
    if (defer) {
      refreshAuthorizationRecheckSchedule();
      return;
    }
    startPeriodicAuthorizationCheck();
  }


  private boolean hasProgressNotifyWatcherLocked() {
    return watchRegistry.activeSnapshot().stream().anyMatch(watcher -> watcher.progressNotify()
        && watcher.createdSent() && !watcher.isClosed());
  }

  private void onProgressNotifyTick() {
    synchronized (lock) {
      progressNotifyTask = null;
      if (terminal || peerCancelled || !hasProgressNotifyWatcherLocked()) return;
      progressNotifyInFlight = true;
    }
    executeCompletion(this::requestPeriodicProgress);
  }

  private void requestPeriodicProgress() {
    List<WatchRegistry.Watcher> active;
    synchronized (lock) {
      if (terminal || peerCancelled) {
        finishPeriodicProgress();
        return;
      }
      active = watchRegistry.activeSnapshot().stream()
          .filter(watcher -> watcher.progressNotify() && watcher.createdSent()
              && !watcher.isClosed())
          .toList();
    }
    if (active.isEmpty()) {
      finishPeriodicProgress();
      return;
    }
    session().whenComplete((openedSession, sessionFailure) -> executeCompletion(() -> {
      if (sessionFailure != null) {
        fail(unwrap(sessionFailure));
        finishPeriodicProgress();
        return;
      }
      CompletionStage<Long> revision;
      try {
        revision = Objects.requireNonNull(openedSession.currentRevision(),
            "current Watch revision future");
      } catch (RuntimeException | Error failure) {
        fail(failure);
        finishPeriodicProgress();
        if (failure instanceof Error fatal) throw fatal;
        return;
      }
      revision.whenComplete((currentRevision, revisionFailure) -> executeCompletion(() ->
          completePeriodicProgress(openedSession, active, currentRevision, revisionFailure)));
    }));
  }

  private void completePeriodicProgress(Session openedSession, List<WatchRegistry.Watcher> candidates,
      Long revision, Throwable failure) {
    if (failure != null) {
      fail(unwrap(failure));
      finishPeriodicProgress();
      return;
    }
    if (revision == null || revision < 0) {
      fail(new IllegalStateException("Watch session revision is invalid"));
      finishPeriodicProgress();
      return;
    }
    long sequence = openedSession.completedSequence();
    if (sequence < 0) {
      fail(new IllegalStateException("Watch session sequence is invalid"));
      finishPeriodicProgress();
      return;
    }
    boolean scheduled = false;
    try {
      synchronized (lock) {
        if (!terminal && !peerCancelled && pendingProgress == null
            && openedSession.completedRevision() >= revision) {
          ProgressBarrier barrier = new ProgressBarrier(revision, sequence);
          for (WatchRegistry.Watcher watcher : candidates) {
            if (!watchRegistry.containsActive(watcher.id(), watcher) || watcher.startRevision() > revision
                || !watcher.progressReady(revision)) continue;
            watcher.requestNotifyProgress(barrier);
            scheduled = true;
          }
        }
      }
    } catch (RuntimeException | Error progressFailure) {
      fail(progressFailure);
      if (progressFailure instanceof Error fatal) throw fatal;
    } finally {
      finishPeriodicProgress();
    }
    if (scheduled) scheduleDrain();
  }

  private void finishPeriodicProgress() {
    synchronized (lock) { progressNotifyInFlight = false; }
    refreshProgressNotifySchedule();
  }

  private void terminateWatcher(WatchRegistry.Watcher watcher, WatchTermination termination) {
    if (termination.kind() == WatchTermination.Kind.FAILURE) {
      fail(termination.failure());
      return;
    }
    Session currentSession;
    synchronized (lock) {
      if (!watchRegistry.removeActive(watcher.id(), watcher)) return;
      pendingTerminationResponses++;
      currentSession = session;
    }
    refreshAuthorizationRecheckSchedule();
    try {
      watcher.close();
    } catch (RuntimeException | Error failure) {
      completeTerminationResponse(watcher.id(), termination, null, failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }

    CompletionStage<Long> revision;
    try {
      revision = currentSession == null
          ? java.util.concurrent.CompletableFuture.completedFuture(0L)
          : Objects.requireNonNull(currentSession.currentRevision(), "current Watch revision future");
    } catch (RuntimeException | Error failure) {
      completeTerminationResponse(watcher.id(), termination, null, failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }
    revision.whenComplete((currentRevision, failure) -> executeCompletion(
        () -> completeTerminationResponse(watcher.id(), termination, currentRevision, failure),
        ignored -> releasePendingTerminationResponse()));
  }

  private void completeTerminationResponse(
      long watchId, WatchTermination termination, Long revision, Throwable failure) {
    Throwable cause = failure == null ? null : unwrap(failure);
    if (cause == null && (revision == null || revision < 0)) {
      cause = new IllegalStateException("Watch session revision is invalid");
    }
    synchronized (lock) {
      releasePendingTerminationResponseLocked();
      if (terminal || peerCancelled) return;
      if (cause == null) {
        Rpc.WatchResponse response = termination.kind() == WatchTermination.Kind.COMPACTED
            ? compactedResponse(watchId, revision, termination.compactRevision())
            : cancelledResponse(watchId, revision, termination.reason());
        enqueueResponse(response, this::requestNext);
      }
    }
    if (cause != null) {
      fail(cause);
      return;
    }
    scheduleDrain();
  }

  private void releasePendingTerminationResponse() {
    synchronized (lock) { releasePendingTerminationResponseLocked(); }
  }

  private void releasePendingTerminationResponseLocked() {
    if (pendingTerminationResponses <= 0) {
      throw new IllegalStateException("Watch termination response count is unbalanced");
    }
    pendingTerminationResponses--;
  }

  private void requestNext() {
    if (serverObserver == null) {
      synchronized (lock) {
        if (!terminal && !peerCancelled && !inboundClosed && !processingRequest
            && pendingRequest == null && responses.isEmpty()) {
          requestOutstanding = true;
        }
      }
      return;
    }
    boolean request;
    synchronized (lock) {
      request = !terminal && !peerCancelled && !inboundClosed && !processingRequest
          && pendingRequest == null && responses.isEmpty()
          && !requestOutstanding;
      if (request) requestOutstanding = true;
    }
    if (!request) return;
    try {
      serverObserver.request(1);
    } catch (RuntimeException failure) {
      fail(failure);
    }
  }

  private void scheduleDrainYield() {
    synchronized (lock) {
      if (terminal || peerCancelled || drainYieldTask != null) return;
      try {
        drainYieldTask = timers.schedule(() -> {
          synchronized (lock) { drainYieldTask = null; }
          scheduleDrain();
        }, 0, TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException rejected) {
        // The bounded drain must yield to avoid monopolizing the worker on filtered revisions.
        fail(rejected);
      }
    }
  }

  private void cancelDrainYield() {
    ScheduledFuture<?> scheduled;
    synchronized (lock) {
      scheduled = drainYieldTask;
      drainYieldTask = null;
    }
    if (scheduled != null) scheduled.cancel(false);
  }

  private void cancelProgressNotifyTimer() {
    ScheduledFuture<?> scheduled;
    synchronized (lock) {
      scheduled = progressNotifyTask;
      progressNotifyTask = null;
      progressNotifyInFlight = false;
    }
    if (scheduled != null) scheduled.cancel(false);
  }

  private void cancelAuthorizationRecheckTimer() {
    ScheduledFuture<?> scheduled;
    synchronized (lock) {
      scheduled = authorizationRecheckTask;
      authorizationRecheckTask = null;
    }
    if (scheduled != null) scheduled.cancel(false);
  }

  private boolean isReady() {
    return serverObserver == null || serverObserver.isReady();
  }

  private boolean hasRunnableWork() {
    synchronized (lock) {
      if (peerCancelled) return false;
      if (terminalFailure != null) return true;
      if (terminal || pendingTerminationResponses > 0) return false;
      if (outboundAuthorizationCheckInFlight || periodicAuthorizationCheckInFlight) return false;
      if (activeFragment != null) {
        return isReady() || isCancelRequest(pendingRequest) && !processingRequest;
      }
      return pendingRequest != null && !processingRequest || !responses.isEmpty() && isReady();
    }
  }

  private void executeCompletion(Runnable completion) {
    executeCompletion(completion, ignored -> {});
  }

  private void executeCompletion(Runnable completion, Consumer<Throwable> schedulingFailureCleanup) {
    try {
      worker.execute(completion);
    } catch (RejectedExecutionException rejected) {
      failCompletion(rejected, schedulingFailureCleanup);
    } catch (Error fatal) {
      failCompletion(fatal, schedulingFailureCleanup);
      throw fatal;
    }
  }

  private void failCompletion(Throwable failure, Consumer<Throwable> schedulingFailureCleanup) {
    try {
      schedulingFailureCleanup.accept(failure);
    } catch (RuntimeException | Error cleanupFailure) {
      if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
    }
    fail(failure);
  }

  private static void closeUnregisteredWatch(OpenedWatch opened, Throwable openingFailure,
      Throwable schedulingFailure) {
    if (openingFailure != null || opened == null) return;
    try {
      opened.subscription().close();
    } catch (RuntimeException | Error cleanupFailure) {
      if (cleanupFailure != schedulingFailure) schedulingFailure.addSuppressed(cleanupFailure);
    }
  }

  private static long nextRevision(long revision) {
    return revision == Long.MAX_VALUE ? Long.MAX_VALUE : revision + 1;
  }

  private static KeyRange keyRange(Rpc.WatchCreateRequest request) {
    byte[] keyBytes = request.getKey().toByteArray();
    if (keyBytes.length == 0) keyBytes = new byte[] {0};
    ByteKey key = ByteKey.copyOf(keyBytes);
    byte[] endBytes = request.getRangeEnd().toByteArray();
    if (endBytes.length == 0) return KeyRange.point(key);
    if (endBytes.length == 1 && endBytes[0] == 0) {
      return KeyRange.interval(Optional.of(key), Optional.empty());
    }
    ByteKey end = ByteKey.copyOf(endBytes);
    if (key.compareTo(end) >= 0) throw new InvalidRange();
    return KeyRange.between(key, end);
  }

  private Rpc.WatchResponse createdResponse(long id, long revision) {
    return Rpc.WatchResponse.newBuilder().setHeader(header(revision)).setWatchId(id)
        .setCreated(true).build();
  }

  private Rpc.WatchResponse cancelledResponse(long id, long revision) {
    return cancelledResponse(id, revision, "");
  }

  private Rpc.WatchResponse cancelledResponse(long id, long revision, String reason) {
    var response = Rpc.WatchResponse.newBuilder().setHeader(header(revision))
        .setWatchId(id).setCanceled(true);
    if (!reason.isEmpty()) response.setCancelReason(reason);
    return response.build();
  }

  private Rpc.WatchResponse compactedResponse(long id, long revision, long compactRevision) {
    return Rpc.WatchResponse.newBuilder().setHeader(header(revision))
        .setWatchId(id).setCanceled(true).setCompactRevision(compactRevision)
        .build();
  }

  private Rpc.ResponseHeader header(long revision) {
    return ResponseHeaderFactory.create(clusterId, memberId, revision, 0);
  }

  private void cancelFromPeer() {
    synchronized (lock) {
      if (terminal || peerCancelled) return;
      peerCancelled = true;
      requestOutstanding = false;
      pendingRequest = null;
      pendingProgress = null;
      responses.clear();
    }
    streamScope.cancel();
    close();
  }

  private void fail(Throwable failure) {
    fail(failure, true);
  }

  private void fail(Throwable failure, boolean scheduleWriter) {
    Objects.requireNonNull(failure, "failure");
    ActiveFragment fragments;
    synchronized (lock) {
      if (peerCancelled) return;
      if (terminal) {
        if (terminalFailure != null && terminalFailure != failure) {
          terminalFailure.addSuppressed(failure);
        }
        return;
      }
      terminal = true;
      terminalFailure = failure;
      requestOutstanding = false;
      pendingRequest = null;
      pendingProgress = null;
      responses.clear();
      fragments = activeFragment;
      activeFragment = null;
    }
    cancelDrainYield();
    cancelProgressNotifyTimer();
    cancelAuthorizationRecheckTimer();
    Throwable cleanupFailure = closeWatchersCollecting();
    cleanupFailure = closeActiveFragmentCollecting(fragments, cleanupFailure);
    cleanupFailure = closeSessionCollecting(cleanupFailure);
    if (cleanupFailure != null && cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
    if (scheduleWriter) scheduleDrain();
    if (cleanupFailure instanceof Error fatal) throw fatal;
  }

  private static boolean isCancelRequest(Rpc.WatchRequest request) {
    return request != null
        && request.getRequestUnionCase() == Rpc.WatchRequest.RequestUnionCase.CANCEL_REQUEST;
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable cause = failure;
    while ((cause instanceof java.util.concurrent.CompletionException
        || cause instanceof java.util.concurrent.ExecutionException) && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }

  @Override
  public void close() {
    List<WatchRegistry.Watcher> closing;
    ActiveFragment fragments;
    synchronized (lock) {
      if (terminal && watchRegistry.isEmpty() && activeFragment == null) return;
      terminal = true;
      requestOutstanding = false;
      pendingRequest = null;
      pendingProgress = null;
      responses.clear();
      closing = watchRegistry.detachAll();
      fragments = activeFragment;
      activeFragment = null;
    }
    cancelDrainYield();
    cancelProgressNotifyTimer();
    cancelAuthorizationRecheckTimer();
    Throwable cleanupFailure = closeWatchersCollecting(closing);
    cleanupFailure = closeActiveFragmentCollecting(fragments, cleanupFailure);
    cleanupFailure = closeSessionCollecting(cleanupFailure);
    if (cleanupFailure instanceof Error fatal) throw fatal;
    if (cleanupFailure instanceof RuntimeException cleanup) throw cleanup;
  }

  private static Throwable closeActiveFragmentCollecting(
      ActiveFragment fragments, Throwable failure) {
    if (fragments == null) return failure;
    try {
      fragments.close();
    } catch (RuntimeException | Error cleanup) {
      return appendCleanupFailure(failure, cleanup);
    }
    return failure;
  }

  private Throwable closeSessionCollecting(Throwable failure) {
    if (!sessionCloseStarted.compareAndSet(false, true)) return failure;
    Session current;
    CompletionStage<Session> opening;
    synchronized (lock) {
      current = session;
      opening = sessionFuture;
    }
    if (current != null) {
      try {
        current.close();
      } catch (RuntimeException | Error closeFailure) {
        return appendCleanupFailure(failure, closeFailure);
      }
    } else if (opening != null) {
      opening.whenComplete((opened, openFailure) -> {
        if (opened == null) return;
        try {
          opened.close();
        } catch (RuntimeException | Error closeFailure) {
          if (openFailure != null && openFailure != closeFailure) {
            openFailure.addSuppressed(closeFailure);
          } else {
            fail(closeFailure);
          }
        }
      });
    }
    return failure;
  }

  private static Throwable appendCleanupFailure(Throwable failure, Throwable additional) {
    if (failure == null) return additional;
    if (additional != failure) {
      if (additional instanceof Error && !(failure instanceof Error)) {
        additional.addSuppressed(failure);
        return additional;
      }
      failure.addSuppressed(additional);
    }
    return failure;
  }

  private Throwable closeWatchersCollecting() {
    List<WatchRegistry.Watcher> closing;
    synchronized (lock) {
      closing = watchRegistry.detachAll();
    }
    return closeWatchersCollecting(closing);
  }

  private static Throwable closeWatchersCollecting(List<WatchRegistry.Watcher> closing) {
    Throwable failure = null;
    for (WatchRegistry.Watcher watcher : closing) {
      try {
        watcher.close();
      } catch (RuntimeException | Error cleanup) {
        if (failure == null) failure = cleanup;
        else if (cleanup != failure) {
          if (cleanup instanceof Error && !(failure instanceof Error)) {
            cleanup.addSuppressed(failure);
            failure = cleanup;
          } else {
            failure.addSuppressed(cleanup);
          }
        }
      }
    }
    return failure;
  }

  private static final class InvalidRange extends RuntimeException {
    private InvalidRange() { super("mvcc: watcher range is empty"); }
  }
}
