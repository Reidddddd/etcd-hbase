package io.etcdhbase.core.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class DirectRecordReaderTest {
  private enum EarlyCompletion { CANCELLED, TIMED_OUT, FALLBACK }

  @Test
  void responseCancellationClosesItsLateResultWithoutClosingTheRequest() throws Exception {
    assertAbandonedLateResultClosed(EarlyCompletion.CANCELLED);
  }

  @Test
  void responseTimeoutClosesItsLateResultWithoutCancellingTheProducer() throws Exception {
    assertAbandonedLateResultClosed(EarlyCompletion.TIMED_OUT);
  }

  @Test
  void responseFallbackClosesItsLateResultWithoutCancellingTheProducer() throws Exception {
    assertAbandonedLateResultClosed(EarlyCompletion.FALLBACK);
  }

  @Test
  void repeatedReadsUseTheBackendAndRetainedResultsOwnTheirOutputBytes() throws Exception {
    var record = ExistsExecutorTest.live(1, 16);
    var store = new FrozenRecordStore(List.of(List.of(record)));
    var budget = SortedMergeCursorTest.budget();
    try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, SortedMergeCursorTest.memory(1, 1024))) {
      RecordReader reader = new DirectRecordReader(store, account, 1024);
      for (int attempt = 0; attempt < 2; attempt++) {
        try (var result = reader.read(record.key(), 7, ReadProjection.FULL).join()) {
          assertTrue(result.record().orElseThrow().hasValue());
          assertEquals(1024, budget.snapshot().allocatedBytes());
          assertThrows(CompletionException.class, () -> reader.read(record.key(), 7, ReadProjection.FULL).join());
        }
        assertEquals(0, budget.snapshot().allocatedBytes());
      }
      assertEquals(2, store.gets.size(), "neither caching nor request coalescing is enabled");
    }
  }

  @Test
  void cancellationCannotReleaseIoBeforeTheBackendCompletesAndAbandonedResultsAreClosed() throws Exception {
    var store = new FrozenRecordStore(List.of(List.of()));
    store.pending = new CompletableFuture<>();
    var budget = SortedMergeCursorTest.budget();
    try (var scope = RequestScope.withoutDeadline()) {
      var account = budget.admit("public", scope, SortedMergeCursorTest.memory(1, 1024));
      var reader = new DirectRecordReader(store, account, 1024);
      var result = reader.read(ByteKey.empty(), 1, ReadProjection.METADATA);
      result.cancel(false);
      scope.cancel();
      assertFalse(store.pending.isCancelled());
      assertEquals(1, budget.snapshot().operations());
      assertFalse(account.termination().toCompletableFuture().isDone());
      store.pending.complete(Optional.empty());
      account.termination().toCompletableFuture().join();
      assertEquals(0, budget.snapshot().allocatedBytes());
      assertEquals(0, budget.snapshot().requests());
    }
  }

  @Test
  void missingRecordsAreRealAbsenceWhileMissingValuesAndBackendErrorsRemainErrors() throws Exception {
    var record = ExistsExecutorTest.live(1, 16);
    var store = new FrozenRecordStore(List.of(List.of(record)));
    var budget = SortedMergeCursorTest.budget();
    try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, SortedMergeCursorTest.memory(1, 1024))) {
      var reader = new DirectRecordReader(store, account, 1024);
      try (var absent = reader.read(ByteKey.empty(), 7, ReadProjection.METADATA).join()) {
        assertTrue(absent.record().isEmpty());
      }
      var backendFailure = new IllegalStateException("synthetic");
      store.pending = CompletableFuture.failedFuture(backendFailure);
      var observed = assertThrows(CompletionException.class,
          () -> reader.read(record.key(), 7, ReadProjection.FULL).join());
      assertSame(backendFailure, observed.getCause());
      store.pending = CompletableFuture.completedFuture(Optional.of(StoredRecord.metadataOnly(record.key(), record.metadata())));
      assertThrows(CompletionException.class, () -> reader.read(record.key(), 7, ReadProjection.FULL).join());
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void fatalStartupReleasesReservedOutputAndScopeCloseInvalidatesDeliveredResults() throws Exception {
    var record = ExistsExecutorTest.live(1, 16);
    var store = new FrozenRecordStore(List.of(List.of(record)));
    var budget = SortedMergeCursorTest.budget();
    try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, SortedMergeCursorTest.memory(1, 1024))) {
      var reader = new DirectRecordReader(store, account, 1024);
      store.fatal = true;
      assertThrows(AssertionError.class, () -> reader.read(record.key(), 7, ReadProjection.FULL));
      assertEquals(0, budget.snapshot().allocatedBytes());
      store.fatal = false;
      var result = reader.read(record.key(), 7, ReadProjection.FULL).join();
      scope.cancel();
      assertThrows(IllegalStateException.class, result::record);
      account.termination().toCompletableFuture().join();
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  private static void assertAbandonedLateResultClosed(EarlyCompletion completion) throws Exception {
    var record = ExistsExecutorTest.live(1, 16);
    var store = new FrozenRecordStore(List.of(List.of(record)));
    store.pending = new CompletableFuture<>();
    var budget = SortedMergeCursorTest.budget();
    try (var scope = RequestScope.withoutDeadline();
        var account = budget.admit("public", scope, SortedMergeCursorTest.memory(1, 1024))) {
      var reader = new DirectRecordReader(store, account, 1024);
      var response = reader.read(record.key(), 7, ReadProjection.FULL);
      completeEarly(response, completion);
      assertFalse(store.pending.isCancelled());
      assertEquals(1, budget.snapshot().operations());
      assertEquals(account.reservation().io() + 1024, budget.snapshot().allocatedBytes());
      store.pending.complete(Optional.of(record));
      assertEquals(0, budget.snapshot().operations());
      assertEquals(0, budget.snapshot().allocatedBytes());
      assertFalse(scope.isClosed());
      assertTrue(account.isActive());
      try (var subsequent = reader.read(record.key(), 7, ReadProjection.FULL).join()) {
        assertEquals(record.key(), subsequent.record().orElseThrow().key());
      }
      assertEquals(2, store.gets.size());
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  private static void completeEarly(CompletableFuture<PointResult> response, EarlyCompletion completion)
      throws Exception {
    switch (completion) {
      case CANCELLED -> assertTrue(response.cancel(false));
      case TIMED_OUT -> {
        response.orTimeout(0, TimeUnit.NANOSECONDS);
        var failure = assertThrows(ExecutionException.class, () -> response.get(10, TimeUnit.SECONDS));
        assertInstanceOf(TimeoutException.class, failure.getCause());
      }
      case FALLBACK -> assertNull(response.completeOnTimeout(null, 0, TimeUnit.NANOSECONDS)
          .get(10, TimeUnit.SECONDS));
    }
  }
}
