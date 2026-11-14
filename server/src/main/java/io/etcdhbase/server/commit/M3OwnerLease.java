package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.RecoveryManager;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Recovers a group before enabling its owner lane and fences it on uncertain renewal. */
final class M3OwnerLease implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(M3OwnerLease.class);
  private static final Executor CLEANUP = task ->
      Thread.ofVirtual().name("m3-owner-cleanup").start(task);

  private final Object lock = new Object();
  private CommitOwnerService owners;
  private CommitOwnerService.OwnerToken token;
  private final long leaseMillis;
  private final long heartbeatTimeoutMillis;
  private final Clock clock;
  private final Runnable onLost;
  private final ScheduledThreadPoolExecutor timer;
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private boolean ready = true;
  private boolean stopping;
  private boolean stopCallbackFinished;
  private boolean fenceTransition;
  private RequestScope inFlight;
  private RequestScope transitionScope;
  private CompletableFuture<Void> heartbeatCompletion = CompletableFuture.completedFuture(null);
  private Throwable stopFailure;
  private long verifiedDeadlineMillis;

  private M3OwnerLease(CommitOwnerService owners, CommitOwnerService.OwnerToken token,
      long leaseMillis, long heartbeatMillis, long heartbeatTimeoutMillis, long verifiedDeadlineMillis,
      Runnable onLost, Clock clock) {
    this.owners = owners;
    this.token = token;
    this.leaseMillis = leaseMillis;
    this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
    this.verifiedDeadlineMillis = verifiedDeadlineMillis;
    this.clock = clock;
    this.onLost = onLost;
    timer = new ScheduledThreadPoolExecutor(1,
        Thread.ofPlatform().daemon().name("m3-owner-heartbeat-", 0).factory());
    timer.setRemoveOnCancelPolicy(true);
    timer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    timer.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    try {
      timer.scheduleWithFixedDelay(this::heartbeat, heartbeatMillis, heartbeatMillis, TimeUnit.MILLISECONDS);
    } catch (RuntimeException | Error failure) {
      timer.shutdownNow();
      throw failure;
    }
  }

  static CompletableFuture<M3OwnerLease> acquire(RecoveryManager recovery, CommitOwnerService owners,
      String instance, Duration lease, Duration heartbeat, Duration heartbeatTimeout,
      RequestScope startup, Runnable onLost) {
    return acquire(recovery, owners, instance, lease, heartbeat, heartbeatTimeout, startup, onLost,
        Clock.systemUTC());
  }

  static CompletableFuture<M3OwnerLease> acquire(RecoveryManager recovery, CommitOwnerService owners,
      String instance, Duration lease, Duration heartbeat, Duration heartbeatTimeout,
      RequestScope startup, Runnable onLost, Clock clock) {
    Objects.requireNonNull(recovery, "recovery");
    Objects.requireNonNull(owners, "owners");
    Objects.requireNonNull(instance, "instance");
    Objects.requireNonNull(startup, "startup");
    Objects.requireNonNull(onLost, "onLost");
    Objects.requireNonNull(clock, "clock");
    long leaseMillis = millis(lease);
    long heartbeatMillis = millis(heartbeat);
    long timeoutMillis = millis(heartbeatTimeout);
    if (heartbeatMillis > leaseMillis / 3 || timeoutMillis >= leaseMillis) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid owner renewal schedule"));
    }
    long now = clock.millis();
    long deadline = Math.addExact(now, leaseMillis);
    return recovery.recover(instance, now, deadline, startup).thenCompose(result -> {
      if (result.state() == RecoveryManager.State.OWNER_ACTIVE) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      if (clock.millis() >= deadline) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      var token = result.owner().orElseThrow();
      long verifiedDeadline = Math.addExact(clock.millis(), leaseMillis);
      return owners.heartbeat(token, verifiedDeadline, startup).thenApply(verified -> {
        if (!Boolean.TRUE.equals(verified) || clock.millis() >= deadline || clock.millis() >= verifiedDeadline
            || startup.isClosed() || startup.isDeadlineExceeded()) {
          throw new CompletionException(new BackendFailure(startup.isDeadlineExceeded()
              ? BackendFailure.Category.TIMEOUT : startup.isClosed()
                  ? BackendFailure.Category.CANCELLED : BackendFailure.Category.UNAVAILABLE));
        }
        return new M3OwnerLease(owners, token, leaseMillis, heartbeatMillis, timeoutMillis,
            verifiedDeadline, onLost, clock);
      });
    });
  }

  CommitOwnerService.OwnerToken token() {
    synchronized (lock) { return token; }
  }

  boolean isReady() {
    synchronized (lock) {
      return ready && !fenceTransition && clock.millis() < verifiedDeadlineMillis;
    }
  }

  /**
   * Changes the control store and/or fencing token while owner renewal remains verified.
   * The transition is reserved against the periodic heartbeat, and both sides of the
   * transition verify the same owner through the control row before the new token is exposed.
   */
  CompletableFuture<CommitOwnerService.OwnerToken> transitionFence(
      CommitOwnerService nextOwners,
      Function<CommitOwnerService.OwnerToken, CompletableFuture<CommitOwnerService.OwnerToken>> transition,
      RequestScope scope) {
    Objects.requireNonNull(nextOwners, "nextOwners");
    Objects.requireNonNull(transition, "transition");
    Objects.requireNonNull(scope, "scope");

    CommitOwnerService previousOwners;
    CommitOwnerService.OwnerToken previousToken;
    CompletableFuture<Void> previousHeartbeat;
    synchronized (lock) {
      if (stopping || !ready || fenceTransition || clock.millis() >= verifiedDeadlineMillis
          || scope.isClosed() || scope.isDeadlineExceeded()) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      fenceTransition = true;
      transitionScope = scope;
      previousOwners = owners;
      previousToken = token;
      previousHeartbeat = heartbeatCompletion;
    }

    CompletableFuture<TransitionResult> prepared = previousHeartbeat.thenCompose(ignored -> {
      if (!scopeIsUsable(scope)) {
        return CompletableFuture.failedFuture(scopeFailure(scope));
      }
      synchronized (lock) {
        if (stopping || !fenceTransition) {
          return CompletableFuture.failedFuture(
              new BackendFailure(BackendFailure.Category.UNAVAILABLE));
        }
      }
      long refreshedDeadline = Math.addExact(clock.millis(), leaseMillis);
      return previousOwners.heartbeat(previousToken, refreshedDeadline, scope)
          .thenCompose(renewed -> {
            if (!Boolean.TRUE.equals(renewed) || clock.millis() >= refreshedDeadline
                || !scopeIsUsable(scope)) {
              return CompletableFuture.failedFuture(scopeFailure(scope));
            }
            synchronized (lock) {
              if (stopping || !fenceTransition) {
                return CompletableFuture.failedFuture(
                    new BackendFailure(BackendFailure.Category.UNAVAILABLE));
              }
              verifiedDeadlineMillis = Math.max(verifiedDeadlineMillis, refreshedDeadline);
            }
            return transition.apply(previousToken).thenApply(nextToken -> {
              if (nextToken == null || !nextToken.instance().equals(previousToken.instance())
                  || nextToken.epoch() < previousToken.epoch()
                  || clock.millis() >= refreshedDeadline || !scopeIsUsable(scope)) {
                throw new CompletionException(scopeFailure(scope));
              }
              return new TransitionResult(nextToken, refreshedDeadline);
            });
          });
    });

    CompletableFuture<TransitionResult> verified = prepared.thenCompose(preparedToken -> {
      long nextDeadline = Math.addExact(clock.millis(), leaseMillis);
      if (clock.millis() >= preparedToken.deadlineMillis() || !scopeIsUsable(scope)) {
        return CompletableFuture.failedFuture(scopeFailure(scope));
      }
      return nextOwners.heartbeat(preparedToken.token(), nextDeadline, scope)
          .thenApply(renewed -> {
            if (!Boolean.TRUE.equals(renewed) || clock.millis() >= nextDeadline
                || !scopeIsUsable(scope)) {
              throw new CompletionException(scopeFailure(scope));
            }
            return new TransitionResult(preparedToken.token(), nextDeadline);
          });
    });

    return verified.handle((result, failure) -> {
      DrainStatus status;
      synchronized (lock) {
        transitionScope = null;
        fenceTransition = false;
        if (failure == null && !stopping) {
          owners = nextOwners;
          token = result.token();
          verifiedDeadlineMillis = result.deadlineMillis();
        }
        status = drainStatus();
      }
      if (failure != null) {
        stop(LossReason.FENCE_TRANSITION_FAILED);
        completeDrain(status);
        throw new CompletionException(failure);
      }
      if (stopping) {
        completeDrain(status);
        throw new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      completeDrain(status);
      return result.token();
    }).copy();
  }

  CompletableFuture<Void> drained() { return drained.copy(); }

  boolean awaitTermination(Duration timeout) throws InterruptedException {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) throw new IllegalArgumentException("negative termination timeout");
    long nanos;
    try { nanos = timeout.toNanos(); } catch (ArithmeticException overflow) { nanos = Long.MAX_VALUE; }
    return timer.awaitTermination(nanos, TimeUnit.NANOSECONDS);
  }

  @Override public void close() { stop(false); }

  private void heartbeat() {
    RequestScope scope;
    long previousDeadline;
    boolean expired;
    CommitOwnerService activeOwners;
    CommitOwnerService.OwnerToken activeToken;
    CompletableFuture<Void> completion;
    synchronized (lock) {
      if (stopping || fenceTransition || inFlight != null) return;
      previousDeadline = verifiedDeadlineMillis;
      expired = clock.millis() >= previousDeadline;
      scope = expired ? null : RequestScope.withTimeout(Duration.ofMillis(heartbeatTimeoutMillis));
      if (!expired) {
        inFlight = scope;
        completion = new CompletableFuture<>();
        heartbeatCompletion = completion;
        activeOwners = owners;
        activeToken = token;
      } else {
        completion = null;
        activeOwners = null;
        activeToken = null;
      }
    }
    if (expired) { stop(LossReason.LEASE_EXPIRED_BEFORE_RENEWAL); return; }
    ScheduledFuture<?> timeout;
    try {
      timeout = timer.schedule(() -> {
        stop(LossReason.HEARTBEAT_TIMEOUT);
        scope.cancelAsync(CLEANUP);
      }, heartbeatTimeoutMillis, TimeUnit.MILLISECONDS);
      long deadline = Math.addExact(clock.millis(), leaseMillis);
      activeOwners.heartbeat(activeToken, deadline, scope).whenComplete((renewed, failure) -> {
        timeout.cancel(false);
        long now = clock.millis();
        if (failure != null || !Boolean.TRUE.equals(renewed)
            || now >= previousDeadline || now >= deadline) {
          stop(renewalFailure(failure, renewed, now, previousDeadline, deadline));
        } else {
          synchronized (lock) {
            if (!stopping) verifiedDeadlineMillis = Math.max(verifiedDeadlineMillis, deadline);
          }
        }
        finishHeartbeat(scope, completion);
      });
    } catch (RuntimeException failure) {
      stop(LossReason.HEARTBEAT_SCHEDULING_FAILED);
      finishHeartbeat(scope, completion);
    } catch (Error fatal) {
      stop(LossReason.HEARTBEAT_SCHEDULING_FAILED);
      finishHeartbeat(scope, completion);
      throw fatal;
    }
  }

  private void finishHeartbeat(RequestScope scope, CompletableFuture<Void> completion) {
    try {
      scope.closeAsync(CLEANUP);
    } catch (RuntimeException | Error failure) {
      recordFailure(failure);
      stop(LossReason.HEARTBEAT_CLEANUP_FAILED);
    } finally {
      CompletableFuture.allOf(scope.cleanupCompletion().toCompletableFuture(),
          scope.physicalProducersCompletion().toCompletableFuture()).whenComplete((ignored, failure) -> {
            if (failure != null) {
              recordFailure(failure);
              stop(LossReason.HEARTBEAT_CLEANUP_FAILED);
            }
            DrainStatus status;
            synchronized (lock) {
              if (inFlight == scope) inFlight = null;
              if (heartbeatCompletion == completion) {
                heartbeatCompletion = CompletableFuture.completedFuture(null);
              }
              status = drainStatus();
            }
            completion.complete(null);
            completeDrain(status);
          });
    }
  }

  private void stop(boolean lost) {
    stop(lost, null);
  }

  private void stop(LossReason reason) {
    stop(true, Objects.requireNonNull(reason, "reason"));
  }

  private void stop(boolean lost, LossReason reason) {
    RequestScope cancel;
    RequestScope cancelTransition;
    synchronized (lock) {
      if (stopping) return;
      stopping = true;
      ready = false;
      cancel = inFlight;
      cancelTransition = transitionScope;
    }
    if (lost) LOGGER.warn("M3 table-group owner lease lost; reason={}", reason);
    timer.shutdown();
    try {
      if (lost) onLost.run();
    } catch (RuntimeException | Error failure) {
      recordFailure(failure);
    } finally {
      try {
        if (cancel != null) cancel.cancelAsync(CLEANUP);
      } catch (RuntimeException | Error failure) {
        recordFailure(failure);
      } finally {
        try {
          if (cancelTransition != null && cancelTransition != cancel) {
            cancelTransition.cancelAsync(CLEANUP);
          }
        } catch (RuntimeException | Error failure) {
          recordFailure(failure);
        }
        DrainStatus status;
        synchronized (lock) {
          stopCallbackFinished = true;
          status = drainStatus();
        }
        completeDrain(status);
      }
    }
  }

  private void recordFailure(Throwable failure) {
    synchronized (lock) {
      if (stopFailure == null) stopFailure = failure;
      else if (stopFailure != failure) stopFailure.addSuppressed(failure);
    }
  }

  /** Called under lock; no owned heartbeat may still retain a physical producer. */
  private DrainStatus drainStatus() {
    return stopping && stopCallbackFinished && inFlight == null && !fenceTransition
        && transitionScope == null ? new DrainStatus(stopFailure) : null;
  }

  private boolean scopeIsUsable(RequestScope scope) {
    return !scope.isClosed() && !scope.isDeadlineExceeded();
  }

  private static BackendFailure scopeFailure(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : scope.isClosed()
            ? BackendFailure.Category.CANCELLED : BackendFailure.Category.UNAVAILABLE);
  }

  private record TransitionResult(CommitOwnerService.OwnerToken token, long deadlineMillis) {}

  private void completeDrain(DrainStatus status) {
    if (status == null) return;
    if (status.failure() == null) drained.complete(null);
    else drained.completeExceptionally(status.failure());
  }

  private record DrainStatus(Throwable failure) {}

  private static LossReason renewalFailure(Throwable failure, Boolean renewed, long now,
      long previousDeadline, long deadline) {
    if (failure != null) return LossReason.HEARTBEAT_FAILED;
    if (!Boolean.TRUE.equals(renewed)) return LossReason.HEARTBEAT_REJECTED;
    if (now >= previousDeadline || now >= deadline) {
      return LossReason.LEASE_WINDOW_EXPIRED;
    }
    return LossReason.HEARTBEAT_FAILED;
  }

  private enum LossReason {
    LEASE_EXPIRED_BEFORE_RENEWAL,
    HEARTBEAT_TIMEOUT,
    HEARTBEAT_FAILED,
    HEARTBEAT_REJECTED,
    LEASE_WINDOW_EXPIRED,
    HEARTBEAT_SCHEDULING_FAILED,
    HEARTBEAT_CLEANUP_FAILED,
    FENCE_TRANSITION_FAILED
  }

  private static long millis(Duration value) {
    Objects.requireNonNull(value, "duration");
    try {
      long millis = value.toMillis();
      if (millis < 1 || millis > 600_000 || !Duration.ofMillis(millis).equals(value)) {
        throw new IllegalArgumentException("invalid owner duration");
      }
      return millis;
    } catch (ArithmeticException overflow) {
      throw new IllegalArgumentException("invalid owner duration", overflow);
    }
  }
}
