package io.etcdhbase.server.auth;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class AuthDeploymentRecoveryWorkerTest {
  @Test
  void cleanupFailureStopsRecoveryAndIsReportedToTheOwner() {
    var calls = new AtomicInteger();
    var worker = new AuthDeploymentRecoveryWorker(scope -> {
      calls.incrementAndGet();
      scope.registerCleanup(() -> { throw new IllegalStateException("cleanup failed"); });
      return CompletableFuture.completedFuture(null);
    }, Duration.ofSeconds(2), Duration.ofMillis(1));
    worker.start();
    try {
      assertThrows(ExecutionException.class, () -> worker.drained().get(2, TimeUnit.SECONDS));
      assertFalse(worker.isReady());
      assertTrue(worker.lastFailure().isPresent());
      assertTrue(calls.get() == 1);
    } finally {
      worker.stop();
    }
  }

  @Test
  void retriesARecoveredOperationAfterFailure() throws Exception {
    var calls = new AtomicInteger();
    var retried = new CountDownLatch(1);
    var worker = new AuthDeploymentRecoveryWorker(scope -> {
      if (calls.incrementAndGet() == 1) return CompletableFuture.failedFuture(new IllegalStateException());
      retried.countDown();
      return CompletableFuture.completedFuture(null);
    }, Duration.ofSeconds(2), Duration.ofMillis(1));
    worker.start();
    assertTrue(retried.await(5, TimeUnit.SECONDS));
    worker.initialReadiness().get(5, TimeUnit.SECONDS);
    assertTrue(worker.isReady());
    worker.stop();
    worker.drained().get(5, TimeUnit.SECONDS);
    assertTrue(calls.get() >= 2);
  }

  @Test
  void stopCancelsTheScopeAndWaitsForThePhysicalProducer() throws Exception {
    var started = new CountDownLatch(1);
    var activeScope = new AtomicReference<RequestScope>();
    var logical = new CompletableFuture<Void>();
    var physical = new CompletableFuture<Void>();
    var worker = new AuthDeploymentRecoveryWorker(scope -> {
      activeScope.set(scope);
      scope.trackPhysicalProducer(physical);
      started.countDown();
      return logical;
    }, Duration.ofSeconds(2), Duration.ofSeconds(1));
    worker.start();
    assertTrue(started.await(5, TimeUnit.SECONDS));
    worker.stop();
    assertTrue(activeScope.get().isClosed());
    logical.complete(null);
    assertFalse(worker.drained().isDone());
    physical.complete(null);
    worker.drained().get(5, TimeUnit.SECONDS);
  }
}
