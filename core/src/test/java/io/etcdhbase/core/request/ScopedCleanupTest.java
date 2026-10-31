package io.etcdhbase.core.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ScopedCleanupTest {
  @Test
  void completedPagesDoNotAccumulateResourcesAndCloseIsIdempotent() throws Exception {
    var closed = new AtomicInteger();
    try (var scope = RequestScope.withoutDeadline()) {
      for (int index = 0; index < 10_000; index++) {
        var registration = scope.registerScopedCleanup(closed::incrementAndGet);
        assertEquals(1, scope.registeredResourceCount());
        registration.close();
        registration.close();
        assertEquals(0, scope.registeredResourceCount());
      }
    }
    assertEquals(10_000, closed.get());
  }

  @Test
  void lateAndCancellationCleanupStillHaveExactlyOneOwner() throws Exception {
    var closed = new AtomicInteger();
    var scope = RequestScope.withoutDeadline();
    var registration = scope.registerScopedCleanup(closed::incrementAndGet);
    scope.cancel();
    registration.close();
    scope.registerScopedCleanup(closed::incrementAndGet).close();
    assertEquals(2, closed.get());
    assertEquals(0, scope.registeredResourceCount());
  }

  @Test
  void externalCloseKeepsScopeCleanupPendingUntilTheResourceActuallyCloses() throws Exception {
    for (boolean cancellation : List.of(false, true)) {
      try (var scope = RequestScope.withoutDeadline()) {
        var closeEntered = new CountDownLatch(1);
        var releaseClose = new CountDownLatch(1);
        var registration = scope.registerScopedCleanup(() -> {
          closeEntered.countDown();
          assertTrue(releaseClose.await(10, TimeUnit.SECONDS));
        });
        var closing = CompletableFuture.runAsync(() -> close(registration));
        try {
          assertTrue(closeEntered.await(10, TimeUnit.SECONDS));
          if (cancellation) scope.cancel();
          else scope.close();
          assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
          assertFalse(closing.isDone());
        } finally {
          releaseClose.countDown();
          closing.get(10, TimeUnit.SECONDS);
        }
        assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone());
        assertEquals(0, scope.registeredResourceCount());
      }
    }
  }

  @Test
  void aResourceCanReenterScopeCancellationWithoutWaitingForItsOwnClose() throws Exception {
    var scope = RequestScope.withoutDeadline();
    var closed = new AtomicInteger();
    var registration = scope.registerScopedCleanup(() -> {
      closed.incrementAndGet();
      scope.cancel();
      assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
    });
    registration.close();
    registration.close();
    assertEquals(1, closed.get());
    assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone());
    assertEquals(0, scope.registeredResourceCount());
  }

  @Test
  void aConcurrentClosingFailureIsObservedOnceWithoutReplacingTheOriginalCause() throws Exception {
    for (Throwable failure : List.of(new IllegalStateException("synthetic close failure"),
        new AssertionError("synthetic fatal close failure"))) {
      try (var scope = RequestScope.withoutDeadline()) {
        var closeEntered = new CountDownLatch(1);
        var releaseClose = new CountDownLatch(1);
        var registration = scope.registerScopedCleanup(() -> {
          closeEntered.countDown();
          assertTrue(releaseClose.await(10, TimeUnit.SECONDS));
          if (failure instanceof Error fatal) throw fatal;
          throw (Exception) failure;
        });
        var closing = CompletableFuture.runAsync(() -> close(registration));
        try {
          assertTrue(closeEntered.await(10, TimeUnit.SECONDS));
          scope.cancel();
          assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
        } finally {
          releaseClose.countDown();
          var observed = assertThrows(ExecutionException.class, () -> closing.get(10, TimeUnit.SECONDS));
          assertSame(failure, observed.getCause());
        }
        registration.close();
        assertEquals(1, scope.cleanupFailureCount());
        assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone());
      }
    }
  }

  @Test
  void lateScopedCleanupJoinsTheStillPendingClosingBatch() throws Exception {
    var scope = RequestScope.withoutDeadline();
    var pendingBatch = new AtomicReference<Runnable>();
    var closed = new AtomicInteger();
    scope.closeAsync(pendingBatch::set);
    var registration = scope.registerScopedCleanup(() -> {
      assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
      closed.incrementAndGet();
    });
    registration.close();
    assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
    pendingBatch.get().run();
    assertEquals(1, closed.get());
    assertEquals(0, scope.registeredResourceCount());
    assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone());
  }

  @Test
  void fatalLateScopedCleanupKeepsItsCauseAndIsCountedOnce() {
    var scope = RequestScope.withoutDeadline();
    scope.close();
    var fatal = new AssertionError("synthetic late close failure");
    assertSame(fatal, assertThrows(AssertionError.class, () -> scope.registerScopedCleanup(() -> {
      throw fatal;
    })));
    assertEquals(1, scope.cleanupFailureCount());
    assertEquals(0, scope.registeredResourceCount());
    assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone());
  }

  @Test
  void aSeparatePhysicalCompletionRetainsItsRegistrationUntilCleanupReallyEnds() throws Exception {
    var closed = new AtomicInteger();
    var physicalCompletion = new CompletableFuture<Void>();
    try (var scope = RequestScope.withoutDeadline()) {
      var registration = scope.registerScopedCleanup(closed::incrementAndGet, physicalCompletion);
      registration.close();
      registration.close();
      assertEquals(1, closed.get());
      assertEquals(1, scope.registeredResourceCount());
      scope.cancel();
      assertFalse(physicalCompletion.isCancelled());
      assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
      physicalCompletion.complete(null);
      assertEquals(0, scope.registeredResourceCount());
      assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone());
      assertEquals(0, scope.cleanupFailureCount());
    }
  }

  @Test
  void aDeferredPhysicalFailureIsCountedOnce() throws Exception {
    var physicalCompletion = new CompletableFuture<Void>();
    try (var scope = RequestScope.withoutDeadline()) {
      var registration = scope.registerScopedCleanup(() -> {}, physicalCompletion);
      scope.cancel();
      registration.close();
      assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
      assertEquals(0, scope.cleanupFailureCount());
      physicalCompletion.completeExceptionally(new IllegalStateException("synthetic deferred close failure"));
      registration.close();
      assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone());
      assertEquals(1, scope.cleanupFailureCount());
      assertEquals(0, scope.registeredResourceCount());
    }
  }

  @Test
  void lateDeferredCleanupStillJoinsTheClosingBatch() {
    var physicalCompletion = new CompletableFuture<Void>();
    var pendingBatch = new AtomicReference<Runnable>();
    var closed = new AtomicInteger();
    var scope = RequestScope.withoutDeadline();
    scope.closeAsync(pendingBatch::set);
    scope.registerScopedCleanup(closed::incrementAndGet, physicalCompletion);
    pendingBatch.get().run();
    assertEquals(1, closed.get());
    assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
    physicalCompletion.complete(null);
    assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone());
    assertEquals(0, scope.registeredResourceCount());
  }

  private static void close(RequestScope.ScopedCleanup registration) {
    try {
      registration.close();
    } catch (Exception failure) {
      throw new CompletionException(failure);
    }
  }
}
