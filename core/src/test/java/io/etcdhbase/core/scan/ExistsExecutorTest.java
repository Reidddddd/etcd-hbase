package io.etcdhbase.core.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ExistsExecutorTest {
  @Test
  void aPositiveCanStopBeforeOtherBucketsAndDoesNotFetchAnyValues() throws Exception {
    var live = live(2, 8192);
    var source = new FrozenScanSource(List.of(List.of(SortedMergeCursorTest.record(new byte[] {1}, true)), List.of(live), List.of(live(3, 8192))));
    var budget = SortedMergeCursorTest.budget();
    try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, SortedMergeCursorTest.memory(1, 4096));
        var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1));
        var executor = new ExistsExecutor(scheduler.bind(source, account), SortedMergeCursorTest.template(), 3, scope)) {
      assertEquals(1, executor.execute().join().count());
      assertEquals(2, source.requests.size());
      assertTrue(source.requests.stream().allMatch(request -> request.projection() == ReadProjection.METADATA));
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void aNegativeRequiresAllBucketsAndErrorsNeverBecomeZero() throws Exception {
    var source = new FrozenScanSource(List.of(List.of(), List.of(SortedMergeCursorTest.record(new byte[] {1}, true)), List.of()));
    var budget = SortedMergeCursorTest.budget();
    try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, SortedMergeCursorTest.memory(1, 4096));
        var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1));
        var executor = new ExistsExecutor(scheduler.bind(source, account), SortedMergeCursorTest.template(), 3, scope)) {
      assertEquals(0, executor.execute().join().count());
      assertEquals(3, source.requests.size());
    }
    ScanSource failing = request -> new ScanOperation() {
      @Override public CompletionStage<StoragePage> completion() { return CompletableFuture.failedStage(new IllegalStateException("synthetic")); }
      @Override public void close() {}
    };
    try (var scope = RequestScope.withoutDeadline(); var executor = new ExistsExecutor(failing, SortedMergeCursorTest.template(), 3, scope)) {
      assertThrows(CompletionException.class, () -> executor.execute().join());
    }
  }

  @Test
  void pointExistenceReadsMetadataAtTheRequestedRevisionAndReturnsOnlyZeroOrOne() throws Exception {
    var key = ByteKey.copyOf(new byte[] {1});
    var store = new FrozenRecordStore(List.of(List.of(live(1, 8192))));
    var budget = SortedMergeCursorTest.budget();
    try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, SortedMergeCursorTest.memory(1, 4096))) {
      assertEquals(1, ExistsExecutor.point(store, key, 7, account).join());
      assertEquals(List.of(new FrozenRecordStore.Get(key, 7, ReadProjection.METADATA)), store.gets);
      assertEquals(0, budget.snapshot().allocatedBytes());
      assertEquals(0, ExistsExecutor.point(store, ByteKey.empty(), 7, account).join());
    }
  }

  @Test
  void cancellationSettlesBeforeFatalCleanupEscapesAndRepeatedCloseRemainsSafe() {
    var physical = new CompletableFuture<StoragePage>();
    var closes = new AtomicInteger();
    var fatal = new AssertionError("synthetic fatal close failure");
    try (var scope = RequestScope.withoutDeadline();
        var executor = new ExistsExecutor(controlled(physical, closes, fatal),
            SortedMergeCursorTest.template(), 1, scope)) {
      var query = executor.execute();
      assertFalse(query.isDone());
      assertSame(fatal, assertThrows(AssertionError.class, executor::close));
      assertTrue(query.isDone());
      var failure = assertInstanceOf(BackendFailure.class,
          assertThrows(CompletionException.class, query::join).getCause());
      assertEquals(BackendFailure.Category.CANCELLED, failure.category());
      assertEquals(List.of(fatal), List.of(failure.getSuppressed()));
      executor.close();
      scope.close();
      assertTrue(scope.cleanupCompletion().toCompletableFuture().isDone());
      assertEquals(1, closes.get());
      assertTrue(physical.complete(new StoragePage(List.of(live(1, 0)), true, new StoragePage.Cost(1, 1, 1))));
      assertTrue(query.isCompletedExceptionally());
      assertSame(failure, assertThrows(CompletionException.class, query::join).getCause());
      assertEquals(1, closes.get());
    }
  }

  @Test
  void cancellationCompletesWithNormalOrExceptionalCleanupWithoutReturningZero() {
    for (Throwable cleanup : new Throwable[] {null, new IllegalStateException("synthetic close failure")}) {
      var physical = new CompletableFuture<StoragePage>();
      var closes = new AtomicInteger();
      try (var scope = RequestScope.withoutDeadline();
          var executor = new ExistsExecutor(controlled(physical, closes, cleanup),
              SortedMergeCursorTest.template(), 1, scope)) {
        var query = executor.execute();
        executor.close();
        var failure = assertInstanceOf(BackendFailure.class,
            assertThrows(CompletionException.class, query::join).getCause());
        assertEquals(BackendFailure.Category.CANCELLED, failure.category());
        assertEquals(cleanup == null ? List.of() : List.of(cleanup), List.of(failure.getSuppressed()));
        executor.close();
        assertEquals(1, closes.get());
      }
    }
  }

  @Test
  void aPositivePageWithFatalCleanupFailsInsteadOfReportingExistence() {
    var physical = new CompletableFuture<StoragePage>();
    var closes = new AtomicInteger();
    var fatal = new AssertionError("synthetic positive-page close failure");
    try (var scope = RequestScope.withoutDeadline();
        var executor = new ExistsExecutor(controlled(physical, closes, fatal),
            SortedMergeCursorTest.template(), 1, scope)) {
      var query = executor.execute();
      physical.complete(new StoragePage(List.of(live(1, 0)), true, new StoragePage.Cost(1, 1, 1)));
      assertTrue(query.isDone());
      assertSame(fatal, assertThrows(CompletionException.class, query::join).getCause());
      executor.close();
      assertEquals(1, closes.get());
    }
  }

  @Test
  void aBackendFailureRetainsItsCauseWhenPhysicalCleanupAlsoFailsFatally() {
    var physical = new CompletableFuture<StoragePage>();
    var closes = new AtomicInteger();
    var fatal = new AssertionError("synthetic backend close failure");
    var backend = new BackendFailure(BackendFailure.Category.UNAVAILABLE);
    try (var scope = RequestScope.withoutDeadline();
        var executor = new ExistsExecutor(controlled(physical, closes, fatal),
            SortedMergeCursorTest.template(), 1, scope)) {
      var query = executor.execute();
      physical.completeExceptionally(backend);
      assertTrue(query.isDone());
      assertSame(backend, assertThrows(CompletionException.class, query::join).getCause());
      assertTrue(List.of(backend.getSuppressed()).contains(fatal));
      executor.close();
      assertEquals(1, closes.get());
    }
  }

  private static ScanSource controlled(CompletableFuture<StoragePage> physical, AtomicInteger closes,
      Throwable closeFailure) {
    return request -> new ScanOperation() {
      private boolean closed;
      @Override public CompletionStage<StoragePage> completion() { return physical; }
      @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        closes.incrementAndGet();
        if (closeFailure instanceof Error fatal) throw fatal;
        if (closeFailure instanceof RuntimeException failure) throw failure;
      }
    };
  }

  static StoredRecord live(int key, int bytes) {
    return StoredRecord.live(ByteKey.copyOf(new byte[] {(byte) key}),
        new StorageMetadata(1, 7, 2, 0, StorageMetadata.State.LIVE, bytes), new byte[bytes]);
  }
}
