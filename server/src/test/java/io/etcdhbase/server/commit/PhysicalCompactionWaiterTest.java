package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import io.etcdhbase.core.commit.CompactionOperation;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PhysicalCompactionWaiterTest {
  @Test
  void waitsForDurableProgressAndGivesEachCallerAnIndependentScan() throws Exception {
    var budgets = budgets();
    var ready = new AtomicBoolean();
    var started = new CountDownLatch(2);
    var scans = new AtomicInteger();
    try (var timer = new ScheduledThreadPoolExecutor(1);
        var first = RequestScope.withTimeout(Duration.ofSeconds(5));
        var second = RequestScope.withTimeout(Duration.ofSeconds(5));
        var waiter = new PhysicalCompactionWaiter("group", budgets, timer, Runnable::run,
            Duration.ofSeconds(5), () -> {
              scans.incrementAndGet();
              return (target, scope) -> {
                started.countDown();
                return CompletableFuture.completedFuture(ready.get()
                    ? PhysicalCompactionWaiter.Progress.COMPLETE : PhysicalCompactionWaiter.Progress.WAITING);
              };
            })) {
      var one = waiter.awaitCompletion(mock(CompactionOperation.class), first);
      var two = waiter.awaitCompletion(mock(CompactionOperation.class), second);
      assertTrue(started.await(5, TimeUnit.SECONDS));
      assertFalse(one.isDone());
      assertFalse(two.isDone());
      ready.set(true);
      one.get(5, TimeUnit.SECONDS);
      two.get(5, TimeUnit.SECONDS);
      waiter.close();
      waiter.drained().get(5, TimeUnit.SECONDS);
      assertEquals(2, scans.get());
      assertEquals(0, budgets.snapshot().requests());
    }
  }

  @Test
  void cancellationDetachesTheRpcButRetainsAdmissionUntilTheRealReadDrains() throws Exception {
    var budgets = budgets();
    var actual = new CompletableFuture<Void>();
    var started = new CountDownLatch(1);
    try (var timer = new ScheduledThreadPoolExecutor(1);
        var request = RequestScope.withTimeout(Duration.ofSeconds(5));
        var waiter = new PhysicalCompactionWaiter("group", budgets, timer, Runnable::run,
            Duration.ofSeconds(5), () -> (target, scope) -> {
              var response = new CompletableFuture<PhysicalCompactionWaiter.Progress>();
              scope.trackPhysicalProducer(actual);
              scope.registerScopedCleanup(() -> response.completeExceptionally(
                  new BackendFailure(BackendFailure.Category.CANCELLED)));
              started.countDown();
              return response;
            })) {
      var response = waiter.awaitCompletion(mock(CompactionOperation.class), request);
      assertTrue(started.await(5, TimeUnit.SECONDS));
      request.cancel();
      assertEquals(BackendFailure.Category.CANCELLED, failure(response).category());
      assertEquals(1, budgets.snapshot().requests());
      waiter.close();
      assertFalse(waiter.drained().isDone());
      actual.complete(null);
      waiter.drained().get(5, TimeUnit.SECONDS);
      assertEquals(0, budgets.snapshot().requests());
    } finally { actual.complete(null); }
  }

  @Test
  void deadlineTerminatesEvenWhenTheReadFutureNeverResponds() throws Exception {
    var started = new CountDownLatch(1);
    try (var timer = new ScheduledThreadPoolExecutor(1);
        var request = RequestScope.withTimeout(Duration.ofSeconds(5));
        var waiter = new PhysicalCompactionWaiter("group", budgets(), timer, Runnable::run,
            Duration.ofSeconds(1), () -> (target, scope) -> {
              started.countDown();
              return new CompletableFuture<>();
            })) {
      var response = waiter.awaitCompletion(mock(CompactionOperation.class), request);
      assertTrue(started.await(5, TimeUnit.SECONDS));
      assertEquals(BackendFailure.Category.TIMEOUT, failure(response).category());
      waiter.close();
      waiter.drained().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void successWaitsForCleanupAndCleanupFailureCannotFollowASuccessResponse() throws Exception {
    var cleanup = new CompletableFuture<Void>();
    var closing = new CountDownLatch(1);
    try (var timer = new ScheduledThreadPoolExecutor(1);
        var request = RequestScope.withTimeout(Duration.ofSeconds(5));
        var waiter = new PhysicalCompactionWaiter("group", budgets(), timer, Runnable::run,
            Duration.ofSeconds(5), () -> (target, scope) -> {
              scope.registerScopedCleanup(closing::countDown, cleanup);
              return CompletableFuture.completedFuture(PhysicalCompactionWaiter.Progress.COMPLETE);
            })) {
      var response = waiter.awaitCompletion(mock(CompactionOperation.class), request);
      assertTrue(closing.await(5, TimeUnit.SECONDS));
      assertFalse(response.isDone());
      cleanup.completeExceptionally(new IllegalStateException("fixture cleanup failed"));
      assertInstanceOf(IllegalStateException.class,
          assertThrows(ExecutionException.class, () -> response.get(5, TimeUnit.SECONDS)).getCause());
      waiter.close();
      assertThrows(ExecutionException.class, () -> waiter.drained().get(5, TimeUnit.SECONDS));
    } finally { cleanup.complete(null); }
  }

  @Test
  void shutdownRejectsWaitersAndPreventsQueuedPollsFromStartingStorageWork() throws Exception {
    var queued = new LinkedBlockingQueue<Runnable>();
    var reads = new AtomicInteger();
    try (var timer = new ScheduledThreadPoolExecutor(1);
        var request = RequestScope.withTimeout(Duration.ofSeconds(5));
        var waiter = new PhysicalCompactionWaiter("group", budgets(), timer, queued::add,
            Duration.ofSeconds(5), () -> (target, scope) -> {
              reads.incrementAndGet();
              return CompletableFuture.completedFuture(PhysicalCompactionWaiter.Progress.COMPLETE);
            })) {
      var response = waiter.awaitCompletion(mock(CompactionOperation.class), request);
      Runnable poll = queued.poll(5, TimeUnit.SECONDS);
      assertNotNull(poll);
      waiter.close();
      assertEquals(BackendFailure.Category.UNAVAILABLE, failure(response).category());
      poll.run();
      assertEquals(0, reads.get());
      assertEquals(BackendFailure.Category.UNAVAILABLE,
          failure(waiter.awaitCompletion(mock(CompactionOperation.class), request)).category());
      waiter.drained().get(5, TimeUnit.SECONDS);
    }
  }

  private static BackendFailure failure(CompletableFuture<Void> future) throws Exception {
    return assertInstanceOf(BackendFailure.class,
        assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS)).getCause());
  }

  private static BudgetManager budgets() {
    var limits = new BudgetManager.Limits(8, 0, 8, 8, 8, 0, 256L << 20);
    return new BudgetManager(limits, Map.of("group", limits), limits);
  }
}
