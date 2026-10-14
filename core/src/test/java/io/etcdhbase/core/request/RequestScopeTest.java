package io.etcdhbase.core.request;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequestScopeTest {
  @Test
  void closesRegisteredResourcesOnceInReverseOrder() {
    RequestScope scope = RequestScope.withoutDeadline();
    List<Integer> closed = new ArrayList<>();
    AutoCloseable first = () -> closed.add(1);
    AutoCloseable second = () -> closed.add(2);

    scope.registerCleanup(first);
    scope.registerCleanup(first);
    scope.registerCleanup(second);
    scope.close();
    scope.close();

    assertEquals(List.of(2, 1), closed);
    assertTrue(scope.isClosed());
  }

  @Test
  void closesLateRegistrationsImmediatelyAndOnlyOnce() {
    RequestScope scope = RequestScope.withoutDeadline();
    List<String> closed = new ArrayList<>();
    AutoCloseable resource = () -> closed.add("closed");
    scope.close();

    scope.registerCleanup(resource);
    scope.registerCleanup(resource);

    assertEquals(List.of("closed"), closed);
  }

  @Test
  void cancellationClosesResourcesAndIsIdempotent() {
    RequestScope scope = RequestScope.withoutDeadline();
    AtomicLong closes = new AtomicLong();
    scope.registerCleanup(closes::incrementAndGet);

    assertTrue(scope.cancel());
    assertFalse(scope.cancel());
    scope.close();

    assertTrue(scope.isCancelled());
    assertEquals(1, closes.get());
  }

  @Test
  void usesMonotonicTimeForRemainingDeadline() {
    AtomicLong now = new AtomicLong(100);
    RequestScope scope = new RequestScope(10, now::get);

    assertEquals(10, scope.remainingDeadlineNanos());
    now.addAndGet(11);
    assertTrue(scope.isDeadlineExceeded());
    assertEquals(0, scope.remainingDeadlineNanos());
  }

  @Test
  void boundsAndRejectsInvalidTimeouts() {
    assertEquals(Long.MAX_VALUE, RequestScope.withoutDeadline().remainingDeadlineNanos());
    assertThrows(
        IllegalArgumentException.class,
        () -> RequestScope.withTimeout(Duration.ofNanos(-1)));
  }

  @Test
  void operationCompletionClosesScopeWithoutReplacingItsFailure() {
    RequestScope scope = RequestScope.withoutDeadline();
    IOException cleanupFailure = new IOException("credential-must-not-be-logged");
    IOException operationFailure = new IOException("operation failed");
    scope.registerCleanup(
        () -> {
          throw cleanupFailure;
        });
    CompletableFuture<String> operation = new CompletableFuture<>();

    CompletableFuture<String> owned = scope.own(operation).toCompletableFuture();
    operation.completeExceptionally(operationFailure);

    CompletionException observed =
        assertThrows(CompletionException.class, owned::join);
    assertSame(operationFailure, observed.getCause());
    assertEquals(1, scope.cleanupFailureCount());
    assertTrue(scope.isClosed());
  }
}
