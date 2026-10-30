package io.etcdhbase.core.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.BatchWriteResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class BucketCursorTest {
  private final ControlledStore store = new ControlledStore();
  private final ScanRequest template = new ScanRequest(0, KeyRange.all(), Optional.empty(),
      Long.MAX_VALUE, ReadProjection.METADATA, 2, 4096, 16);

  @Test
  void pausesAndResumesFromOnlyTheLastConsumedKey() {
    try (var scope = RequestScope.withoutDeadline(); var cursor = new BucketCursor(store, template, scope)) {
      var pending = cursor.fetch();
      assertEquals(BucketCursor.State.FETCHING, cursor.state());
      assertThrows(CompletionException.class, () -> cursor.fetch().join());
      store.complete(page(false, record(1), record(2)));
      var first = pending.join();
      assertEquals(record(1).key(), first.next().orElseThrow().key());
      cursor.pause();
      first.close();
      assertEquals(BucketCursor.State.PAUSED, cursor.state());
      assertThrows(CompletionException.class, () -> cursor.fetch().join());
      cursor.resume();
      var resumed = cursor.fetch();
      assertEquals(Optional.of(record(1).key()), store.requests.getLast().after());
      store.complete(page(true, record(2)));
      try (var last = resumed.join()) {
        assertEquals(record(2).key(), last.next().orElseThrow().key());
        assertTrue(last.exhausted());
      }
      assertEquals(BucketCursor.State.EXHAUSTED, cursor.state());
      assertFalse(scope.isClosed());
    }
  }

  @Test
  void cancellationRejectsLatePagesWithoutClosingTheSharedParentAgain() {
    try (var scope = RequestScope.withoutDeadline()) {
      var cursor = new BucketCursor(store, template, scope);
      var pending = cursor.fetch();
      scope.cancel();
      var failure = assertThrows(CompletionException.class, pending::join);
      assertEquals(BackendFailure.Category.CANCELLED, ((BackendFailure) failure.getCause()).category());
      store.complete(page(true, record(1)));
      assertEquals(BucketCursor.State.CLOSED, cursor.state());
      assertThrows(CompletionException.class, () -> cursor.fetch().join());
      cursor.close();
      assertEquals(1, store.requests.size());
    }
  }

  @Test
  void closingOneCursorCancelsItsPhysicalReadButKeepsTheSharedRequestOpen() {
    try (var scope = RequestScope.withoutDeadline()) {
      var cursor = new BucketCursor(store, template, scope);
      var result = cursor.fetch();
      cursor.close();
      assertTrue(store.operations.getLast().cancelled);
      assertFalse(scope.isClosed());
      assertFalse(store.pending.getFirst().isDone(), "backend cleanup remains authoritative");
      assertThrows(CompletionException.class, result::join);
      store.complete(page(true, record(1)));
    }
  }

  @Test
  void aBackendFailureIsNotEofAndAnUnconsumedPageCannotBeFetchedTwice() {
    try (var scope = RequestScope.withoutDeadline(); var cursor = new BucketCursor(store, template, scope)) {
      var pending = cursor.fetch();
      var backend = new BackendFailure(BackendFailure.Category.UNAVAILABLE);
      store.pending.remove().completeExceptionally(backend);
      assertSame(backend, assertThrows(CompletionException.class, pending::join).getCause());
      assertEquals(BucketCursor.State.FAILED, cursor.state());
      assertThrows(CompletionException.class, () -> cursor.fetch().join());
      assertEquals(1, store.requests.size());
    }
    try (var scope = RequestScope.withoutDeadline(); var cursor = new BucketCursor(store, template, scope)) {
      var pending = cursor.fetch();
      store.complete(page(true, record(1)));
      var ready = pending.join();
      assertThrows(CompletionException.class, () -> cursor.fetch().join());
      ready.close();
      assertEquals(BucketCursor.State.IDLE, cursor.state());
      assertFalse(scope.isClosed());
    }
  }

  @Test
  void anEmptyNonterminalOrOutOfOrderPageCannotInventProgress() {
    for (StoragePage invalid : List.of(page(false), page(true, record(2), record(1)))) {
      try (var scope = RequestScope.withoutDeadline(); var cursor = new BucketCursor(store, template, scope)) {
        var pending = cursor.fetch();
        store.complete(invalid);
        assertThrows(CompletionException.class, pending::join);
        assertEquals(BucketCursor.State.FAILED, cursor.state());
      }
    }
  }

  private static StoredRecord record(int value) {
    return StoredRecord.metadataOnly(ByteKey.copyOf(new byte[] {(byte) value}),
        new StorageMetadata(1, 1, 1, 0, StorageMetadata.State.LIVE, 0));
  }

  private static StoragePage page(boolean exhausted, StoredRecord... records) {
    return new StoragePage(List.of(records), exhausted, new StoragePage.Cost(records.length, 128L * records.length, 1));
  }

  private static final class ControlledStore implements RecordStore {
    final List<ScanRequest> requests = new ArrayList<>();
    final ArrayDeque<CompletableFuture<StoragePage>> pending = new ArrayDeque<>();
    final List<ControlledOperation> operations = new ArrayList<>();

    @Override
    public ScanOperation startScan(ScanRequest request) {
      var operation = new ControlledOperation(scan(request));
      operations.add(operation);
      return operation;
    }

    @Override
    public CompletableFuture<StoragePage> scan(ScanRequest request) {
      requests.add(request);
      var result = new CompletableFuture<StoragePage>();
      pending.add(result);
      return result;
    }

    void complete(StoragePage page) { pending.remove().complete(page); }

    private static final class ControlledOperation implements ScanOperation {
      private final CompletableFuture<StoragePage> result;
      private boolean cancelled;

      ControlledOperation(CompletableFuture<StoragePage> result) { this.result = result; }
      @Override public CompletionStage<StoragePage> completion() { return result.minimalCompletionStage(); }
      @Override public void close() { cancelled = true; }
    }

    @Override public CompletableFuture<Optional<StoredRecord>> get(ByteKey key, long upper, ReadProjection projection) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Void> put(StoredRecord record) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Boolean> compareAndSet(ByteKey key, Optional<StorageMetadata> expected, StoredRecord record) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<List<BatchWriteResult>> putBatch(List<StoredRecord> records) { throw new UnsupportedOperationException(); }
  }
}
