package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.*;

import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.observability.RequestMetrics;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class GcCheckpointWorkerTest {
  @Test
  void queuedCycleDoesNotStartIoAfterStop() throws Exception {
    try (var timer = new ScheduledThreadPoolExecutor(1)) {
      var queued = new LinkedBlockingQueue<Runnable>();
      var calls = new AtomicInteger();
      var worker = new GcCheckpointWorker(queued::add, timer, Duration.ofSeconds(5), scope -> {
        calls.incrementAndGet();
        return CompletableFuture.completedFuture(GcCheckpointWorker.Outcome.IDLE);
      }, new RequestMetrics());
      worker.start();
      var task = queued.poll(5, TimeUnit.SECONDS);
      assertNotNull(task);
      worker.stop();
      assertFalse(worker.drained().isDone());
      task.run();
      worker.drained().get(5, TimeUnit.SECONDS);
      assertEquals(0, calls.get());
    }
  }

  @Test
  void stopWaitsForPhysicalProducerAndActualCleanupAfterLogicalCompletion() throws Exception {
    try (var timer = new ScheduledThreadPoolExecutor(1)) {
      var started = new CountDownLatch(1);
      var cleanupStarted = new CountDownLatch(1);
      var physical = new CompletableFuture<Void>();
      var cleanup = new CompletableFuture<Void>();
      var scopeRef = new AtomicReference<RequestScope>();
      var worker = new GcCheckpointWorker(Runnable::run, timer, Duration.ofSeconds(5), scope -> {
        scopeRef.set(scope);
        scope.startIfOpen(() -> scope.trackPhysicalProducer(physical));
        scope.registerScopedCleanup(cleanupStarted::countDown, cleanup);
        started.countDown();
        return CompletableFuture.completedFuture(GcCheckpointWorker.Outcome.PROGRESSED);
      }, new RequestMetrics());
      worker.start();
      assertTrue(started.await(5, TimeUnit.SECONDS));
      assertTrue(cleanupStarted.await(5, TimeUnit.SECONDS));
      assertTrue(scopeRef.get().isClosed(), "successful cycles must close their scope too");
      worker.stop();
      assertFalse(worker.drained().isDone());
      physical.complete(null);
      assertFalse(worker.drained().isDone(), "cleanup is still physically outstanding");
      cleanup.complete(null);
      worker.drained().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void cancellationReachesLogicalIoButDoesNotPretendPhysicalDrain() throws Exception {
    try (var timer = new ScheduledThreadPoolExecutor(1)) {
      var started = new CountDownLatch(1);
      var physical = new CompletableFuture<Void>();
      var logical = new CompletableFuture<GcCheckpointWorker.Outcome>();
      var worker = new GcCheckpointWorker(Runnable::run, timer, Duration.ofSeconds(5), scope -> {
        scope.startIfOpen(() -> scope.trackPhysicalProducer(physical));
        scope.registerCleanup(() -> logical.complete(GcCheckpointWorker.Outcome.IDLE));
        started.countDown();
        return logical;
      }, new RequestMetrics());
      worker.start();
      assertTrue(started.await(5, TimeUnit.SECONDS));
      worker.stop();
      logical.get(5, TimeUnit.SECONDS);
      assertFalse(worker.drained().isDone());
      physical.complete(null);
      worker.drained().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void physicalAndCleanupFailuresSurviveStop() throws Exception {
    try (var timer = new ScheduledThreadPoolExecutor(1)) {
      var started = new CountDownLatch(1);
      var physical = new CompletableFuture<Void>();
      var cleanup = new CompletableFuture<Void>();
      var worker = new GcCheckpointWorker(Runnable::run, timer, Duration.ofSeconds(5), scope -> {
        scope.startIfOpen(() -> scope.trackPhysicalProducer(physical));
        scope.registerScopedCleanup(() -> {}, cleanup);
        started.countDown();
        return CompletableFuture.completedFuture(GcCheckpointWorker.Outcome.IDLE);
      }, new RequestMetrics());
      worker.start();
      assertTrue(started.await(5, TimeUnit.SECONDS));
      worker.stop();
      var cleanupFailure = new IllegalStateException("cleanup");
      cleanup.completeExceptionally(cleanupFailure);
      var producerFailure = new IllegalArgumentException("producer");
      physical.completeExceptionally(producerFailure);
      var failure = assertThrows(ExecutionException.class, () -> worker.drained().get(5, TimeUnit.SECONDS));
      assertSame(cleanupFailure, failure.getCause());
      assertTrue(java.util.Arrays.asList(failure.getCause().getSuppressed()).contains(producerFailure));
    }
  }

  @Test
  void executorRejectionClosesScopeAndRetriesWithoutLeakingOutstandingWork() throws Exception {
    try (var timer = new ScheduledThreadPoolExecutor(1)) {
      var rejected = new CountDownLatch(1);
      var metrics = new RequestMetrics();
      var worker = new GcCheckpointWorker(task -> {
        rejected.countDown();
        throw new RejectedExecutionException("full");
      }, timer, Duration.ofSeconds(5), scope -> fail("must not execute"), metrics);
      worker.start();
      assertTrue(rejected.await(5, TimeUnit.SECONDS));
      worker.stop();
      worker.drained().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void schedulerRejectionIsVisibleAndStopBeforeStartCannotResurrectWorker() throws Exception {
    var timer = new ScheduledThreadPoolExecutor(1);
    timer.shutdown();
    var metrics = new RequestMetrics();
    var worker = new GcCheckpointWorker(Runnable::run, timer, Duration.ofSeconds(5),
        scope -> fail("must not execute"), metrics);
    worker.start();
    assertThrows(ExecutionException.class, () -> worker.drained().get(5, TimeUnit.SECONDS));
    assertEquals(1, metrics.gc().failures());
    worker.stop();
    var unstarted = new GcCheckpointWorker(Runnable::run, timer, Duration.ofSeconds(5),
        scope -> fail("must not execute"), metrics);
    unstarted.stop();
    unstarted.drained().get(5, TimeUnit.SECONDS);
    assertThrows(IllegalStateException.class, unstarted::start);
  }
}
