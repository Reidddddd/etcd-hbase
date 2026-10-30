package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.codec.StorageMessages;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.junit.jupiter.api.Test;

class HBasePageReaderTest {
  private final HBaseRecordStore store = mock(HBaseRecordStore.class);
  private final AsyncTable<?> table = mock(AsyncTable.class);
  private final ResultScanner scanner = mock(ResultScanner.class);
  private final StorageTableBinding binding = new StorageTableBinding(TableName.valueOf("synthetic"),
      new byte[] {'d'}, StorageTableBinding.Layout.CURRENT, new HashSaltStrategy());

  private static ScanRequest request(KeyRange range, Optional<ByteKey> after) {
    return new ScanRequest(0, range, after, Long.MAX_VALUE, ReadProjection.METADATA, 1, 1024, 16);
  }

  @Test
  void reportedBytesAreSerializedCellsRatherThanHeapEstimates() throws Exception {
    ByteKey key = ByteKey.empty();
    var metadata = new StorageMetadata(1, 1, 1, 0, StorageMetadata.State.LIVE, 0);
    var cell = new KeyValue(binding.codec().encode(key, 1), binding.family(), HBaseRecordStore.METADATA,
        0, StorageMessages.metadata(metadata));
    var row = Result.create(java.util.List.of(cell));
    doReturn(table).when(store).table();
    doReturn(scanner).when(table).getScanner(any(Scan.class));
    doReturn(row, null).when(scanner).next();
    doReturn(new ScanMetrics()).when(scanner).getScanMetrics();
    doReturn(StoredRecord.metadataOnly(key, metadata)).when(store).decode(row, ReadProjection.METADATA);
    try (var scope = RequestScope.withoutDeadline()) {
      var page = new HBasePageReader(store, binding, scope, Runnable::run)
          .read(request(KeyRange.all(), Optional.empty())).join();
      assertEquals(cell.getSerializedSize(), page.cost().returnedCellBytes());
    }
  }

  @Test
  void rejectedWorkerReportsResourceLimitRatherThanCancellation() {
    try (var scope = RequestScope.withoutDeadline()) {
      var reader = new HBasePageReader(store, binding, scope, command -> { throw new RejectedExecutionException(); });
      var failure = assertThrows(CompletionException.class, () -> reader.read(request(KeyRange.all(), Optional.empty())).join());
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, ((BackendFailure) failure.getCause()).category());
      verify(store, never()).table();
    }
  }

  @Test
  void aResumeAtTheEndDoesNotOpenAnEmptyPhysicalScanner() {
    try (var scope = RequestScope.withoutDeadline()) {
      ByteKey key = ByteKey.copyOf(new byte[] {0x61});
      var reader = new HBasePageReader(store, binding, scope, Runnable::run);
      var page = reader.read(request(KeyRange.point(key), Optional.of(key))).join();
      assertTrue(page.exhausted());
      assertTrue(page.records().isEmpty());
      verify(store, never()).table();
    }
  }

  @Test
  void queuedCancellationNeverCreatesAScanner() {
    var tasks = new ArrayDeque<Runnable>();
    try (var scope = RequestScope.withoutDeadline()) {
      var reader = new HBasePageReader(store, binding, scope, tasks::add);
      var result = reader.read(request(KeyRange.all(), Optional.empty()));
      scope.cancel();
      tasks.remove().run();
      var failure = assertThrows(CompletionException.class, result::join);
      assertEquals(BackendFailure.Category.CANCELLED, ((BackendFailure) failure.getCause()).category());
      verify(store, never()).table();
    }
  }

  @Test
  void cancellingOneQueuedReadDoesNotCancelTheSharedScope() {
    var tasks = new ArrayDeque<Runnable>();
    try (var scope = RequestScope.withoutDeadline()) {
      var operation = new HBasePageReader(store, binding, scope, tasks::add)
          .start(request(KeyRange.all(), Optional.empty()));
      operation.close();
      operation.close();
      tasks.remove().run();
      assertFalse(scope.isClosed());
      var failure = assertThrows(CompletionException.class, () -> operation.completion().toCompletableFuture().join());
      assertEquals(BackendFailure.Category.CANCELLED, ((BackendFailure) failure.getCause()).category());
      verify(store, never()).table();
    }
  }

  @Test
  void fatalCleanupStillSettlesWaitersBeforePropagating() {
    doReturn(table).when(store).table();
    doReturn(scanner).when(table).getScanner(any(Scan.class));
    doReturn(new ScanMetrics()).when(scanner).getScanMetrics();
    doThrow(new AssertionError("synthetic cleanup failure")).when(scanner).close();
    var tasks = new ArrayDeque<Runnable>();
    try (var scope = RequestScope.withoutDeadline()) {
      var result = new HBasePageReader(store, binding, scope, tasks::add).read(request(KeyRange.all(), Optional.empty()));
      assertThrows(AssertionError.class, tasks.remove()::run);
      assertTrue(result.isDone(), "a fatal cleanup must not leave physical accounting pending forever");
      assertThrows(CompletionException.class, result::join);
    }
  }

  @Test
  void completionWaitsForConcurrentScannerCleanup() throws Exception {
    doReturn(table).when(store).table();
    doReturn(scanner).when(table).getScanner(any(Scan.class));
    doReturn(new ScanMetrics()).when(scanner).getScanMetrics();
    var enteredNext = new CountDownLatch(1);
    var nextReleased = new CountDownLatch(1);
    var enteredClose = new CountDownLatch(1);
    var closeReleased = new CountDownLatch(1);
    doAnswer(invocation -> {
      enteredNext.countDown();
      assertTrue(nextReleased.await(10, TimeUnit.SECONDS));
      return null;
    }).when(scanner).next();
    doAnswer(invocation -> {
      enteredClose.countDown();
      nextReleased.countDown();
      assertTrue(closeReleased.await(10, TimeUnit.SECONDS));
      return null;
    }).when(scanner).close();
    var tasks = new ArrayDeque<Runnable>();
    try (var scope = RequestScope.withoutDeadline()) {
      var operation = new HBasePageReader(store, binding, scope, tasks::add).start(request(KeyRange.all(), Optional.empty()));
      var result = operation.completion().toCompletableFuture();
      var running = CompletableFuture.runAsync(tasks.remove());
      assertTrue(enteredNext.await(10, TimeUnit.SECONDS));
      var cancelling = CompletableFuture.runAsync(operation::close);
      try {
        assertTrue(enteredClose.await(10, TimeUnit.SECONDS));
        running.get(10, TimeUnit.SECONDS);
        assertFalse(result.isDone(), "cleanup still owns the scanner and its physical permit");
      } finally {
        closeReleased.countDown();
        cancelling.get(10, TimeUnit.SECONDS);
        running.get(10, TimeUnit.SECONDS);
      }
      assertThrows(CompletionException.class, result::join);
      assertFalse(scope.isClosed());
      verify(scanner).close();
    }
  }
}
