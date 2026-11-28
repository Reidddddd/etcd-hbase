package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.SystemRecordEntry;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.lease.LeaseExpiryCheckpoint;
import io.etcdhbase.core.lease.LeaseExpiryStepCommand;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Duration;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/** Runs a single bounded expiry step at a time while this table-group runtime owns M2. */
public final class LeaseExpiryWorker {
  private static final long IDLE_POLL_MILLIS = 1_000;
  private static final long PROGRESS_DELAY_MILLIS = 20;
  private static final long RETRY_DELAY_MILLIS = 250;
  private static final Executor CLEANUP = task -> Thread.ofVirtual().name("m4-expiry-cleanup").start(task);

  private final Object lock = new Object();
  private final String tableGroup;
  private final ByteKey authorizationDigest;
  private final ByteKey checkpointPrefix = LeaseExpiryCheckpoint.routePrefix();
  private final int bucketCount;
  private final long timeoutMillis;
  private final CommitControlStore control;
  private final SystemRecordStore records;
  private final M3OwnerLease owner;
  private final MutationRelay relay;
  private final AuthorizedBackendIdentity identity;
  private final LeaseTimeSource time;
  private final BudgetManager budgets;
  private final BudgetManager.Reservation reservation;
  private final Executor worker;
  private final ScheduledExecutorService timer;
  private final Clock clock;
  private final RequestMetrics metrics;
  private final AtomicBoolean clockHealthy = new AtomicBoolean(true);
  private final AtomicBoolean checkpointHealthy = new AtomicBoolean(false);
  private final AtomicLong checkpointOwnerEpoch = new AtomicLong(-1);
  private final CompletableFuture<Void> initialReadiness = new CompletableFuture<>();
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private ScheduledFuture<?> scheduled;
  private RequestScope activeScope;
  private boolean cycleOutstanding;
  private boolean running;
  private long lastIdleWindowStart = -1;

  public LeaseExpiryWorker(String tableGroup, ByteKey authorizationDigest, int bucketCount,
      long timeoutMillis, CommitControlStore control, SystemRecordStore records,
      M3OwnerLease owner, MutationRelay relay, AuthorizedBackendIdentity identity,
      LeaseTimeSource time, BudgetManager budgets, BudgetManager.Reservation reservation,
      Executor worker, ScheduledExecutorService timer, Clock clock, RequestMetrics metrics) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.authorizationDigest = Objects.requireNonNull(authorizationDigest, "authorizationDigest");
    this.control = Objects.requireNonNull(control, "control");
    this.records = Objects.requireNonNull(records, "records");
    this.owner = Objects.requireNonNull(owner, "owner");
    this.relay = Objects.requireNonNull(relay, "relay");
    this.identity = Objects.requireNonNull(identity, "identity");
    this.time = Objects.requireNonNull(time, "time");
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.reservation = Objects.requireNonNull(reservation, "reservation");
    this.worker = Objects.requireNonNull(worker, "worker");
    this.timer = Objects.requireNonNull(timer, "timer");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    if (tableGroup.isBlank() || authorizationDigest.length() != 32 || bucketCount < 1
        || bucketCount > 65_536 || timeoutMillis < 1 || timeoutMillis > 300_000
        || !identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)) {
      throw new IllegalArgumentException("invalid Lease expiry worker configuration");
    }
    this.bucketCount = bucketCount;
    this.timeoutMillis = timeoutMillis;
    try {
      clockHealthy.set(observeClock().healthy());
    } catch (RuntimeException unavailable) {
      clockHealthy.set(false);
    }
  }

  public void start() {
    synchronized (lock) {
      if (running || drained.isDone()) throw new IllegalStateException("Lease expiry worker cannot start twice");
      running = true;
      try {
        scheduleLocked(0);
      } catch (RuntimeException | Error failure) {
        running = false;
        throw failure;
      }
    }
  }

  /** A clock failure closes readiness because TTL promises and safe expiry depend on this source. */
  public boolean isReady() {
    synchronized (lock) {
      if (!running || !clockHealthy.get()) return false;
      Optional<CommitOwnerService.OwnerToken> token = owner.currentToken();
      return token.isEmpty() || (checkpointHealthy.get()
          && checkpointOwnerEpoch.get() == token.orElseThrow().epoch());
    }
  }

  public boolean isClockHealthy() { return clockHealthy.get(); }

  /** Completes after the first healthy clock and owner/checkpoint inspection. */
  CompletableFuture<Void> initialReadiness() { return initialReadiness.copy(); }

  /** Stops new ticks and cancels only the active unselected owner attempt. */
  public void stop() {
    RequestScope toCancel;
    synchronized (lock) {
      if (!running) {
        if (!cycleOutstanding) drained.complete(null);
        return;
      }
      running = false;
      if (scheduled != null) {
        scheduled.cancel(false);
        scheduled = null;
      }
      toCancel = activeScope;
      if (!cycleOutstanding) drained.complete(null);
    }
    initialReadiness.completeExceptionally(
        new IllegalStateException("Lease expiry worker stopped before becoming ready"));
    if (toCancel != null) toCancel.cancelAsync(CLEANUP);
  }

  public CompletableFuture<Void> drained() { return drained.copy(); }

  private void scheduleLocked(long delayMillis) {
    if (!running || scheduled != null || cycleOutstanding) return;
    scheduled = timer.schedule(this::dispatch, delayMillis, TimeUnit.MILLISECONDS);
  }

  private void dispatch() {
    synchronized (lock) {
      scheduled = null;
      if (!running || cycleOutstanding) return;
      cycleOutstanding = true;
    }
    try {
      worker.execute(this::runCycle);
    } catch (RuntimeException rejected) {
      metrics.recordLeaseExpiryRetry();
      finishCycle(RETRY_DELAY_MILLIS, null, null);
    }
  }

  private void runCycle() {
    LeaseTimeObservation observation;
    try {
      observation = observeClock();
    } catch (RuntimeException failure) {
      clockHealthy.set(false);
      markCheckpointUnhealthy();
      metrics.recordLeaseExpiryRetry();
      finishCycle(IDLE_POLL_MILLIS, null, null);
      return;
    }
    if (!observation.healthy()) {
      clockHealthy.set(false);
      markCheckpointUnhealthy();
      metrics.recordLeaseExpiryRetry();
      finishCycle(IDLE_POLL_MILLIS, null, null);
      return;
    }
    Optional<CommitOwnerService.OwnerToken> currentOwner = owner.currentToken();
    if (currentOwner.isEmpty()) {
      clockHealthy.set(true);
      completeInitialReadinessIfReady();
      finishCycle(IDLE_POLL_MILLIS, null, null);
      return;
    }
    CommitOwnerService.OwnerToken token = currentOwner.orElseThrow();
    RequestScope scope = RequestScope.withTimeout(Duration.ofMillis(timeoutMillis));
    final BudgetManager.Account account;
    try {
      account = budgets.admitReserved(tableGroup, scope, reservation,
          LeaseBudgetProfile.EXPIRY_WORKER_READ_LIMITS);
    } catch (BackendFailure | RuntimeException rejected) {
      metrics.recordLeaseExpiryRetry();
      finishAfterDrain(scope, RETRY_DELAY_MILLIS);
      return;
    }
    var cycleDone = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(cycleDone))) {
      account.close();
      finishAfterDrain(scope, RETRY_DELAY_MILLIS);
      return;
    }
    boolean stopped;
    synchronized (lock) {
      stopped = !running;
      if (!stopped) activeScope = scope;
    }
    if (stopped) {
      account.close();
      scope.cancelAsync(CLEANUP);
      cycleDone.complete(null);
      finishAfterDrain(scope, RETRY_DELAY_MILLIS);
      return;
    }

    CompletableFuture<StepOutcome> operation;
    try {
      operation = inspectCheckpoint(token, observation, scope, account)
          .thenCompose(status -> {
            if (!status.shouldStep()) return CompletableFuture.completedFuture(StepOutcome.IDLE);
            if (!sameOwner(token)) return CompletableFuture.completedFuture(StepOutcome.RETRY);
            return sendStep(token, status.checkpointSequence(), scope)
                .thenApply(sent -> sent ? StepOutcome.PROGRESSED : StepOutcome.RETRY);
          });
    } catch (RuntimeException | Error failure) {
      operation = CompletableFuture.failedFuture(failure);
    }
    operation.whenComplete((outcome, failure) -> {
      long delay = failure != null ? RETRY_DELAY_MILLIS
          : outcome == StepOutcome.PROGRESSED ? PROGRESS_DELAY_MILLIS : IDLE_POLL_MILLIS;
      if (failure != null || outcome == StepOutcome.RETRY) metrics.recordLeaseExpiryRetry();
      account.close();
      cycleDone.complete(null);
      finishAfterDrain(scope, delay);
    });
  }

  private void finishAfterDrain(RequestScope scope, long delayMillis) {
    closeAndDrain(scope, CLEANUP).whenComplete((ignored, failure) ->
        finishCycle(delayMillis, scope, failure));
  }

  static CompletableFuture<Void> closeAndDrain(RequestScope scope, Executor cleanupExecutor) {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(cleanupExecutor, "cleanupExecutor");
    Throwable closeFailure = null;
    try {
      scope.closeAsync(cleanupExecutor);
    } catch (RuntimeException | Error failure) {
      closeFailure = failure;
    }
    Throwable closeFailureToReport = closeFailure;
    return CompletableFuture.allOf(scope.cleanupCompletion().toCompletableFuture(),
        scope.physicalProducersCompletion().toCompletableFuture()).handle((ignored, drainFailure) -> {
          Throwable actualFailure = drainFailure == null
              ? null : CompletionFailures.unwrapCompletion(drainFailure);
          if (closeFailureToReport != null) {
            if (actualFailure != null && actualFailure != closeFailureToReport) {
              closeFailureToReport.addSuppressed(actualFailure);
            }
            throw new CompletionException(closeFailureToReport);
          }
          if (actualFailure != null) throw new CompletionException(actualFailure);
          return null;
        });
  }

  private void finishCycle(long delayMillis, RequestScope scope, Throwable failure) {
    boolean completeDrain;
    synchronized (lock) {
      if (scope != null && activeScope == scope) activeScope = null;
      cycleOutstanding = false;
      completeDrain = !running;
      if (!completeDrain) scheduleLocked(delayMillis);
    }
    if (failure != null) metrics.recordLeaseExpiryRetry();
    if (completeDrain) completeDrained(failure);
  }

  private void completeDrained(Throwable failure) {
    if (failure == null) drained.complete(null);
    else drained.completeExceptionally(failure);
  }

  CompletableFuture<CheckpointStatus> inspectCheckpoint(
      CommitOwnerService.OwnerToken token, LeaseTimeObservation observation, RequestScope scope,
      BudgetManager.Account account) {
    final CompletableFuture<Optional<ControlSnapshot>> read = readCheckpointControl(scope, account);
    CompletableFuture<CheckpointStatus> inspection = read.thenCompose(snapshot -> {
      if (snapshot.isEmpty()) {
        markCheckpointUnhealthy();
        return CompletableFuture.failedFuture(
            new IllegalStateException("Lease expiry control state is missing"));
      }
      CommitState state = snapshot.orElseThrow().state();
      if (!state.supportsLeaseExpiryCheckpoints() || state.ownerEpoch() != token.epoch()
          || state.ownerInstance().filter(token.instance()::equals).isEmpty()
          || state.ownerLeaseExpired(clock.millis())) {
        markCheckpointUnhealthy();
        return CompletableFuture.completedFuture(new CheckpointStatus(0, false));
      }
      if (state.publication().isPresent()) {
        // A selected commit must finish before scanning, but it does not invalidate a checkpoint
        // already verified for this owner. A new owner still waits for its first inspection.
        return CompletableFuture.completedFuture(new CheckpointStatus(0, false));
      }
      long sequence = state.finalizedSequence();
      CompletableFuture<Optional<SystemRecordEntry>> latest = sequence == 0
          ? CompletableFuture.completedFuture(Optional.empty())
          : readLatestCheckpoint(sequence, scope, account);
      return latest.thenApply(entry -> checkpointStatus(entry, sequence, token, observation));
    });
    return inspection.whenComplete((status, failure) -> {
      if (failure != null) {
        markCheckpointUnhealthy();
      } else {
        completeInitialReadinessIfReady();
      }
    });
  }

  private CompletableFuture<Optional<ControlSnapshot>> readCheckpointControl(
      RequestScope scope, BudgetManager.Account account) {
    final ReadReservation reservation;
    try {
      reservation = reserveAndChargeRead(account, LeaseBudgetProfile.EXPIRY_CONTROL_READ_WORK,
          LeaseBudgetProfile.MAX_EXPIRY_CONTROL_READ_BYTES,
          budget -> budget.chargeInternalPointRead(
              LeaseBudgetProfile.MAX_EXPIRY_CONTROL_READ_BYTES));
    } catch (BackendFailure | RuntimeException | Error failure) {
      markCheckpointUnhealthy();
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    return runReservedRead(reservation, () -> control.read(scope), "Lease expiry control read");
  }

  private CompletableFuture<Optional<SystemRecordEntry>> readLatestCheckpoint(
      long sequence, RequestScope scope, BudgetManager.Account account) {
    final ReadReservation reservation;
    try {
      reservation = reserveAndChargeRead(account, LeaseBudgetProfile.EXPIRY_CHECKPOINT_SCAN_WORK,
          LeaseBudgetProfile.MAX_EXPIRY_CHECKPOINT_READ_BYTES,
          budget -> budget.chargeInternalScanPage(1,
              LeaseBudgetProfile.MAX_EXPIRY_CHECKPOINT_RECORD_BYTES, 1));
    } catch (BackendFailure | RuntimeException | Error failure) {
      markCheckpointUnhealthy();
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    CompletableFuture<Optional<SystemRecordEntry>> read = runReservedRead(reservation,
        () -> records.latestAtOrBefore(SystemRecordType.LEASE_EXPIRY_CHECKPOINT,
            checkpointPrefix, sequence, scope), "Lease expiry checkpoint scan");
    return read.thenApply(entry -> {
      long bytes = entry.map(SystemRecordEntry::encodedBytes).orElse(0L);
      if (bytes > LeaseBudgetProfile.MAX_EXPIRY_CHECKPOINT_RECORD_BYTES) {
        throw new IllegalStateException("Lease expiry checkpoint exceeds its encoded size limit");
      }
      return entry;
    });
  }

  private static ReadReservation reserveAndChargeRead(BudgetManager.Account account,
      BudgetManager.Work work, long maximumReadBytes, ReadCharge charge) throws BackendFailure {
    ReadReservation reservation = reserveRead(account, work, maximumReadBytes);
    try {
      charge.charge(account);
      return reservation;
    } catch (BackendFailure | RuntimeException | Error failure) {
      reservation.close();
      throw failure;
    }
  }

  private static ReadReservation reserveRead(BudgetManager.Account account,
      BudgetManager.Work work, long maximumReadBytes) throws BackendFailure {
    BudgetManager.Lease io = account.tryRetain(BudgetManager.Memory.IO,
        maximumReadBytes).orElseThrow(LeaseExpiryWorker::resourceLimit);
    try {
      BudgetManager.Lease capacity = account.tryAcquire(work)
          .orElseThrow(LeaseExpiryWorker::resourceLimit);
      return new ReadReservation(io, capacity);
    } catch (BackendFailure | RuntimeException | Error failure) {
      io.close();
      throw failure;
    }
  }

  private static <T> CompletableFuture<T> runReservedRead(ReadReservation reservation,
      Supplier<CompletableFuture<T>> operation, String description) {
    final CompletableFuture<T> read;
    try {
      read = Objects.requireNonNull(operation.get(), description);
    } catch (RuntimeException | Error failure) {
      reservation.close();
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    return read.whenComplete((ignored, failure) -> reservation.close());
  }

  private record ReadReservation(BudgetManager.Lease io, BudgetManager.Lease capacity)
      implements AutoCloseable {
    private ReadReservation {
      Objects.requireNonNull(io, "io");
      Objects.requireNonNull(capacity, "capacity");
    }

    @Override
    public void close() {
      try {
        capacity.close();
      } finally {
        io.close();
      }
    }
  }

  private void completeInitialReadinessIfReady() {
    if (isReady()) initialReadiness.complete(null);
  }

  private CheckpointStatus checkpointStatus(Optional<SystemRecordEntry> entry, long finalizedSequence,
      CommitOwnerService.OwnerToken token, LeaseTimeObservation observation) {
    if (entry.isEmpty()) {
      metrics.recordLeaseExpiryCheckpoint(token.epoch(), 0, 0);
      clockHealthy.set(observation.healthy());
      markCheckpointHealthy(token.epoch());
      return new CheckpointStatus(0, true);
    }
    final long sequence;
    final LeaseExpiryCheckpoint checkpoint;
    try {
      SystemRecordEntry latest = entry.orElseThrow();
      sequence = LeaseExpiryCheckpoint.sequence(latest.identity());
      checkpoint = LeaseExpiryCheckpoint.decode(latest.value());
      if (checkpoint.sequence() != sequence || sequence > finalizedSequence
          || checkpoint.bucketCount() != bucketCount || checkpoint.ownerEpoch() > token.epoch()) {
        throw new IllegalStateException(
            "persisted Lease expiry checkpoint failed its owner barrier");
      }
    } catch (RuntimeException invalidCheckpoint) {
      markCheckpointUnhealthy();
      throw invalidCheckpoint;
    }
    metrics.recordLeaseExpiryCheckpoint(token.epoch(), sequence,
        checkpoint.position().windowStopExclusive());
    if (observation.nowLowerEpochMillis() < checkpoint.trustedTimeLowerEpochMillis()) {
      clockHealthy.set(false);
      markCheckpointHealthy(token.epoch());
      return new CheckpointStatus(sequence, false);
    }
    clockHealthy.set(observation.healthy());
    markCheckpointHealthy(token.epoch());
    if (!checkpoint.position().idle()) return new CheckpointStatus(sequence, true);
    long windowStart = checkpoint.position().windowStartInclusive();
    if (lastIdleWindowStart != windowStart) {
      // Pause between completed expiry windows to leave time for other maintenance.
      // Lease mutations may advance the clock watermark without advancing this scan cursor.
      lastIdleWindowStart = windowStart;
      return new CheckpointStatus(sequence, false);
    }
    long closeStop = LeaseExpiryCheckpoint.closedWindowStopExclusive(observation.nowLowerEpochMillis());
    return new CheckpointStatus(sequence, closeStop > windowStart);
  }

  private void markCheckpointHealthy(long ownerEpoch) {
    checkpointOwnerEpoch.set(ownerEpoch);
    checkpointHealthy.set(true);
  }

  private void markCheckpointUnhealthy() {
    checkpointHealthy.set(false);
    checkpointOwnerEpoch.set(-1);
  }

  private CompletableFuture<Boolean> sendStep(CommitOwnerService.OwnerToken token,
      long checkpointSequence, RequestScope scope) {
    InvocationId invocation = InvocationId.create();
    ByteKey operation = MutationOperationEnvelope.leaseExpiryStep(
        new LeaseExpiryStepCommand(checkpointSequence).encode().bytes());
    ByteKey digest = MutationForwardRequest.digest(tableGroup, operation);
    long remaining = Math.min(scope.remainingDeadlineNanos(), MutationForwardRequest.MAX_TIMEOUT_NANOS);
    if (remaining <= 0) return CompletableFuture.completedFuture(false);
    MutationForwardRequest request = new MutationForwardRequest(tableGroup, invocation, digest,
        authorizationDigest, ServiceAuthorizationContextReference.create(identity, authorizationDigest),
        token.epoch(), remaining, 0, operation);
    return relay.forward(request, scope).thenApply(result -> {
      if (result instanceof MutationRelay.KnownResult known) {
        return java.util.Arrays.equals(known.responseBytes(), invocation.bytes());
      }
      return false;
    });
  }

  private boolean sameOwner(CommitOwnerService.OwnerToken token) {
    return owner.currentToken().filter(current -> current.instance().equals(token.instance())
        && current.epoch() == token.epoch()).isPresent();
  }

  private LeaseTimeObservation observeClock() {
    return Objects.requireNonNull(time.observe(), "Lease time observation");
  }

  private static BackendFailure resourceLimit() {
    return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
  }

  @FunctionalInterface
  private interface ReadCharge {
    void charge(BudgetManager.Account account) throws BackendFailure;
  }

  private enum StepOutcome { IDLE, PROGRESSED, RETRY }
  private record CheckpointStatus(long checkpointSequence, boolean shouldStep) {}
}
