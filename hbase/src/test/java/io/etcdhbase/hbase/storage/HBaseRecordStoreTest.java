package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.protobuf.CodedOutputStream;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.BatchWriteResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.codec.StorageMessages;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltStrategy;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

class HBaseRecordStoreTest {
  @Test
  void cancellationDoesNotReleaseAnOperationBeforeItsBackendCallback() {
    var connection = mock(AsyncConnection.class);
    var admin = mock(AsyncAdmin.class);
    var factory = mock(HBaseConnectionFactory.class);
    var identity = mock(AuthorizedBackendIdentity.class);
    AsyncTableBuilder<?> builder = mock(AsyncTableBuilder.class, RETURNS_SELF);
    AsyncTable<?> table = mock(AsyncTable.class);
    var binding = new StorageTableBinding(TableName.valueOf("synthetic"), new byte[] {'d'},
        StorageTableBinding.Layout.CURRENT, new HashSaltStrategy());
    doReturn(CompletableFuture.completedFuture(connection)).when(factory).connectionFor(identity);
    doReturn(admin).when(connection).getAdmin();
    doReturn(CompletableFuture.completedFuture(binding.descriptor())).when(admin).getDescriptor(binding.table());
    doReturn(builder).when(connection).getTableBuilder(binding.table());
    doReturn(table).when(builder).build();
    var backend = new CompletableFuture<Result>();
    doReturn(backend).when(table).get(any(Get.class));
    try (var scope = RequestScope.withoutDeadline()) {
      var store = HBaseRecordStore.open(factory, identity, binding, scope, Runnable::run).join();
      var result = store.get(ByteKey.empty(), Long.MAX_VALUE, ReadProjection.METADATA);
      scope.cancel();
      assertFalse(backend.isCancelled(), "shared/client authority is not request cancellation state");
      assertFalse(result.isDone(), "an in-flight backend operation still owns its physical permit");
      backend.complete(Result.EMPTY_RESULT);
      var failure = assertThrows(CompletionException.class, result::join);
      assertEquals(BackendFailure.Category.CANCELLED, ((BackendFailure) failure.getCause()).category());
    }
  }

  @Test
  void rejectsOversizedMetadataBeforeAccessingItsBackingArray() {
    var binding = binding();
    try (var scope = RequestScope.withoutDeadline()) {
      var store = decoder(binding, scope);
      var cell = cell(binding, HBaseRecordStore.METADATA,
          StorageMessages.metadata(metadata(1, StorageMetadata.State.LIVE)));
      var row = Result.create(List.of(guardedValue(cell, 4097)));
      assertThrows(IllegalArgumentException.class, () -> store.decode(row, ReadProjection.METADATA));
    }
  }

  @Test
  void rejectsOversizedValuesBeforeCopyingWhileMetadataAndTombstoneIgnoreThem() {
    var binding = binding();
    try (var scope = RequestScope.withoutDeadline()) {
      var store = decoder(binding, scope);
      var value = guardedValue(cell(binding, HBaseRecordStore.VALUE, new byte[] {1}),
          StorageMessages.MAX_VALUE_BYTES + 129);
      var metadata = cell(binding, HBaseRecordStore.METADATA,
          StorageMessages.metadata(metadata(1, StorageMetadata.State.LIVE)));
      var row = Result.create(List.of(metadata, value));
      assertThrows(IllegalArgumentException.class, () -> store.decode(row, ReadProjection.FULL));
      assertFalse(store.decode(row, ReadProjection.METADATA).hasValue());
      var tombstone = cell(binding, HBaseRecordStore.METADATA,
          StorageMessages.metadata(metadata(0, StorageMetadata.State.TOMBSTONE)));
      var deleted = store.decode(Result.create(List.of(tombstone, value)), ReadProjection.FULL);
      assertEquals(StorageMetadata.State.TOMBSTONE, deleted.metadata().state());
      assertFalse(deleted.hasValue());
    }
  }

  @Test
  void acceptsValidMessagesAtTheSerializedMetadataAndPayloadLimits() throws Exception {
    var binding = binding();
    var value = new byte[StorageMessages.MAX_VALUE_BYTES];
    var metadata = metadata(value.length, StorageMetadata.State.LIVE);
    var record = StoredRecord.live(ByteKey.empty(), metadata, value);
    byte[] metadataMessage = padded(StorageMessages.metadata(metadata), 4096);
    byte[] valueMessage = padded(StorageMessages.value(record), StorageMessages.MAX_VALUE_BYTES + 128);
    try (var scope = RequestScope.withoutDeadline()) {
      var row = Result.create(List.of(cell(binding, HBaseRecordStore.METADATA, metadataMessage),
          cell(binding, HBaseRecordStore.VALUE, valueMessage)));
      var decoded = decoder(binding, scope).decode(row, ReadProjection.FULL);
      assertEquals(metadata, decoded.metadata());
      assertArrayEquals(value, decoded.value().orElseThrow());
    }
  }

  @Test
  void allLayoutsRejectInvalidMetadataTimestampsForBothProjections() {
    for (var layout : StorageTableBinding.Layout.values()) {
      var binding = binding(layout);
      try (var scope = RequestScope.withoutDeadline()) {
        var store = decoder(binding, scope);
        for (var projection : ReadProjection.values()) {
          for (long timestamp : new long[] {99, Long.MAX_VALUE}) {
            var row = storedRow(binding, 1, metadata(1, StorageMetadata.State.LIVE), timestamp);
            assertThrows(IllegalArgumentException.class, () -> store.decode(row, projection),
                layout + "/" + projection + "/" + timestamp);
          }
        }
      }
    }
  }

  @Test
  void allLayoutsAcceptMatchingTimestampsWithoutRequiringATombstoneValue() {
    for (var layout : StorageTableBinding.Layout.values()) {
      var binding = binding(layout);
      try (var scope = RequestScope.withoutDeadline()) {
        var store = decoder(binding, scope);
        var live = storedRow(binding, 1, metadata(1, StorageMetadata.State.LIVE), 0);
        var deleted = storedRow(binding, 1, metadata(0, StorageMetadata.State.TOMBSTONE), 0);
        for (var projection : ReadProjection.values()) {
          var decoded = store.decode(live, projection);
          assertEquals(1, decoded.metadata().modificationRevision());
          assertEquals(projection == ReadProjection.FULL, decoded.hasValue());
          assertEquals(StorageMetadata.State.TOMBSTONE, store.decode(deleted, projection).metadata().state());
          assertFalse(store.decode(deleted, projection).hasValue());
        }
      }
    }
  }

  @Test
  void historyStillRejectsARowRevisionThatDiffersFromMetadataAndTimestamp() {
    var binding = binding(StorageTableBinding.Layout.HISTORY);
    var metadata = new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, 1);
    try (var scope = RequestScope.withoutDeadline()) {
      var store = decoder(binding, scope);
      var row = storedRow(binding, 1, metadata, 1);
      for (var projection : ReadProjection.values()) {
        assertThrows(IllegalArgumentException.class, () -> store.decode(row, projection));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(StorageTableBinding.Layout.class)
  void pointReadsDecodeLargeValuesOnTheProvidedWorker(StorageTableBinding.Layout layout) throws Exception {
    var binding = binding(layout);
    var tasks = new ArrayDeque<Runnable>();
    var decodedOn = new AtomicReference<Thread>();
    var completedOn = new AtomicReference<Thread>();
    var row = observedRow(storedRow(binding, 1,
        metadata(StorageMessages.MAX_VALUE_BYTES, StorageMetadata.State.LIVE), 0), decodedOn);
    try (var scope = RequestScope.withoutDeadline()) {
      var store = decoder(binding, scope, tasks::add);
      Runnable complete = pendingPointRead(store.table(), layout, row);
      var result = store.get(ByteKey.empty(), 1, ReadProjection.FULL);
      CompletableFuture.runAsync(() -> {
        completedOn.set(Thread.currentThread());
        complete.run();
      }).get(10, TimeUnit.SECONDS);
      assertFalse(result.isDone(), "backend callbacks must not decode the value");
      assertNull(decodedOn.get());
      assertEquals(1, tasks.size());
      tasks.remove().run();
      assertEquals(StorageMessages.MAX_VALUE_BYTES, result.join().orElseThrow().value().orElseThrow().length);
      assertSame(Thread.currentThread(), decodedOn.get());
      assertNotSame(completedOn.get(), decodedOn.get());
    }
  }

  @ParameterizedTest
  @EnumSource(StorageTableBinding.Layout.class)
  void cancellationBeforeQueuedDecodeDoesNotCopyValues(StorageTableBinding.Layout layout) {
    var binding = binding(layout);
    var tasks = new ArrayDeque<Runnable>();
    var decodedOn = new AtomicReference<Thread>();
    var row = observedRow(storedRow(binding, 1, metadata(1, StorageMetadata.State.LIVE), 0), decodedOn);
    try (var scope = RequestScope.withoutDeadline()) {
      var store = decoder(binding, scope, tasks::add);
      Runnable complete = pendingPointRead(store.table(), layout, row);
      var result = store.get(ByteKey.empty(), 1, ReadProjection.FULL);
      complete.run();
      assertFalse(result.isDone());
      scope.cancel();
      tasks.remove().run();
      var failure = assertThrows(CompletionException.class, result::join);
      assertEquals(BackendFailure.Category.CANCELLED, ((BackendFailure) failure.getCause()).category());
      assertNull(decodedOn.get(), "cancelled queued work must not allocate a decoded value");
    }
  }

  @ParameterizedTest
  @EnumSource(StorageTableBinding.Layout.class)
  void deadlineBeforeQueuedDecodeDoesNotCopyValues(StorageTableBinding.Layout layout) {
    var binding = binding(layout);
    var tasks = new ArrayDeque<Runnable>();
    var decodedOn = new AtomicReference<Thread>();
    var row = observedRow(storedRow(binding, 1, metadata(1, StorageMetadata.State.LIVE), 0), decodedOn);
    try (var scope = spy(RequestScope.withoutDeadline())) {
      doReturn(false).when(scope).isDeadlineExceeded();
      var store = decoder(binding, scope, tasks::add);
      Runnable complete = pendingPointRead(store.table(), layout, row);
      var result = store.get(ByteKey.empty(), 1, ReadProjection.FULL);
      complete.run();
      assertFalse(result.isDone());
      doReturn(true).when(scope).isDeadlineExceeded();
      tasks.remove().run();
      var failure = assertThrows(CompletionException.class, result::join);
      assertEquals(BackendFailure.Category.TIMEOUT, ((BackendFailure) failure.getCause()).category());
      assertNull(decodedOn.get(), "expired queued work must not allocate a decoded value");
    }
  }

  @ParameterizedTest
  @EnumSource(StorageTableBinding.Layout.class)
  void aRejectedDecodeReportsResourceLimitWithoutCopyingValues(StorageTableBinding.Layout layout) {
    var binding = binding(layout);
    var decodedOn = new AtomicReference<Thread>();
    var row = observedRow(storedRow(binding, 1, metadata(1, StorageMetadata.State.LIVE), 0), decodedOn);
    try (var scope = RequestScope.withoutDeadline()) {
      var store = decoder(binding, scope, task -> { throw new RejectedExecutionException("synthetic full worker"); });
      Runnable complete = pendingPointRead(store.table(), layout, row);
      var result = store.get(ByteKey.empty(), 1, ReadProjection.FULL);
      complete.run();
      var failure = assertThrows(CompletionException.class, result::join);
      var backend = assertInstanceOf(BackendFailure.class, failure.getCause());
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, backend.category());
      assertInstanceOf(RejectedExecutionException.class, backend.getCause());
      assertNull(decodedOn.get());
    }
  }

  @ParameterizedTest
  @EnumSource(StorageTableBinding.Layout.class)
  void stoppedScopesDoNotSubmitLateResultsToTheDecodeWorker(StorageTableBinding.Layout layout) {
    for (boolean expired : new boolean[] {false, true}) {
      var submitted = new AtomicInteger();
      var decodedOn = new AtomicReference<Thread>();
      var binding = binding(layout);
      var row = observedRow(storedRow(binding, 1, metadata(1, StorageMetadata.State.LIVE), 0), decodedOn);
      try (var scope = spy(RequestScope.withoutDeadline())) {
        var store = decoder(binding, scope, task -> {
          submitted.incrementAndGet();
          throw new RejectedExecutionException("synthetic full worker");
        });
        Runnable complete = pendingPointRead(store.table(), layout, row);
        var result = store.get(ByteKey.empty(), 1, ReadProjection.FULL);
        if (expired) doReturn(true).when(scope).isDeadlineExceeded();
        else scope.cancel();
        assertFalse(result.isDone(), "the backend callback still owns the in-flight operation");
        complete.run();
        var failure = assertThrows(CompletionException.class, result::join);
        assertEquals(expired ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED,
            ((BackendFailure) failure.getCause()).category());
        assertEquals(0, submitted.get(), "stopped requests must not enqueue new decode work");
        assertNull(decodedOn.get());
      }
    }
  }

  @Test
  void localBatchRejectionKeepsTheSuccessfulNeighbourAndInputIndexes() {
    var metadata = metadata(1, StorageMetadata.State.LIVE);
    try (var scope = RequestScope.withoutDeadline()) {
      var store = decoder(binding(), scope);
      var table = store.table();
      doReturn(CompletableFuture.completedFuture(null)).when(table).put(any(Put.class));
      var results = store.putBatch(List.of(StoredRecord.metadataOnly(ByteKey.empty(), metadata),
          StoredRecord.live(ByteKey.empty(), metadata, new byte[] {1}))).join();
      assertEquals(0, results.get(0).index());
      assertEquals(BatchWriteResult.State.FAILED, results.get(0).state());
      assertInstanceOf(IllegalArgumentException.class, results.get(0).failure().orElseThrow().getCause());
      assertEquals(1, results.get(1).index());
      assertEquals(BatchWriteResult.State.SUCCEEDED, results.get(1).state());
      assertTrue(results.get(1).failure().isEmpty());
      verify(table, times(1)).put(any(Put.class));
    }
  }

  @ParameterizedTest
  @EnumSource(StorageTableBinding.Layout.class)
  void stoppedScopesFailBatchItemsBeforeInvokingTheBackend(StorageTableBinding.Layout layout) {
    for (boolean expired : new boolean[] {false, true}) {
      try (var scope = spy(RequestScope.withoutDeadline())) {
        var store = decoder(binding(layout), scope);
        var table = store.table();
        if (expired) doReturn(true).when(scope).isDeadlineExceeded();
        else scope.cancel();
        var record = StoredRecord.live(ByteKey.empty(), metadata(1, StorageMetadata.State.LIVE), new byte[] {1});
        var result = store.putBatch(List.of(record)).join().getFirst();
        assertEquals(BatchWriteResult.State.FAILED, result.state());
        assertEquals(expired ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED,
            result.failure().orElseThrow().category());
        verify(table, never()).put(any(Put.class));
        verify(table, never()).checkAndMutate(any(CheckAndMutate.class));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(StorageTableBinding.Layout.class)
  void invokedWritesRemainUnknownAndPendingUntilTheirBackendCallback(StorageTableBinding.Layout layout) {
    try (var scope = RequestScope.withoutDeadline()) {
      var store = decoder(binding(layout), scope);
      var backend = pendingWrite(store.table(), layout);
      var record = StoredRecord.live(ByteKey.empty(), metadata(1, StorageMetadata.State.LIVE), new byte[] {1});
      var result = store.putBatch(List.of(record));
      scope.cancel();
      assertFalse(backend.isCancelled());
      assertFalse(result.isDone(), "invoked writes still own their physical operation until the callback");
      backend.completeExceptionally(new TimeoutException("synthetic late timeout"));
      var outcome = result.join().getFirst();
      assertEquals(BatchWriteResult.State.UNKNOWN, outcome.state());
      assertEquals(BackendFailure.Category.TIMEOUT, outcome.failure().orElseThrow().category());
    }
  }

  @Test
  void anExplicitBackendPermissionRejectionRemainsFailed() {
    try (var scope = RequestScope.withoutDeadline()) {
      var store = decoder(binding(), scope);
      var table = store.table();
      doReturn(CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION)))
          .when(table).put(any(Put.class));
      var record = StoredRecord.live(ByteKey.empty(), metadata(1, StorageMetadata.State.LIVE), new byte[] {1});
      var result = store.putBatch(List.of(record)).join().getFirst();
      assertEquals(BatchWriteResult.State.FAILED, result.state());
      assertEquals(BackendFailure.Category.PERMISSION, result.failure().orElseThrow().category());
    }
  }

  @Test
  void historyRejectsAnOversizedRawKeyBeforeCallingTheSaltProvider() {
    assertHistoryKeyRejectedBeforeSalting(new byte[20 * 1024 * 1024]);
  }

  @Test
  void historyRejectsEscapeExpansionBeforeCallingTheSaltProvider() {
    assertHistoryKeyRejectedBeforeSalting(new byte[20_000]);
  }

  @Test
  void historyKeepsTheEncodedUpperRevisionAndReversedSameKeyBounds() {
    var binding = new StorageTableBinding(TableName.valueOf("synthetic"), new byte[] {'d'},
        StorageTableBinding.Layout.HISTORY, new HashSaltStrategy());
    ByteKey key = ByteKey.copyOf(new byte[] {0x61, 0});
    var record = StoredRecord.live(key, metadata(1, StorageMetadata.State.LIVE), new byte[] {7});
    byte[] rowKey = binding.codec().encode(key, 1);
    var row = Result.create(List.of(
        new KeyValue(rowKey, binding.family(), HBaseRecordStore.METADATA, 0,
            StorageMessages.metadata(record.metadata())),
        new KeyValue(rowKey, binding.family(), HBaseRecordStore.VALUE, 0,
            StorageMessages.value(record))));
    try (var scope = RequestScope.withoutDeadline()) {
      var store = decoder(binding, scope);
      var table = store.table();
      doReturn(CompletableFuture.completedFuture(List.of(row))).when(table).scanAll(any(Scan.class));
      var result = store.get(key, 3, ReadProjection.FULL).join().orElseThrow();
      assertEquals(key, result.key());
      assertEquals(record.metadata(), result.metadata());
      assertArrayEquals(new byte[] {7}, result.value().orElseThrow());
      var scan = ArgumentCaptor.forClass(Scan.class);
      verify(table).scanAll(scan.capture());
      assertTrue(scan.getValue().isReversed());
      assertArrayEquals(binding.codec().encode(key, 3), scan.getValue().getStartRow());
      assertArrayEquals(binding.codec().physicalRange(binding.codec().bucket(key),
          KeyRange.point(key)).lowerInclusive().orElseThrow().bytes(),
          scan.getValue().getStopRow());
    }
  }

  private static void assertHistoryKeyRejectedBeforeSalting(byte[] keyBytes) {
    var salt = mock(SaltStrategy.class);
    doReturn(new HashSaltStrategy().profile()).when(salt).profile();
    doThrow(new AssertionError("oversized history key reached the salt provider"))
        .when(salt).salt(any(byte[].class));
    var binding = new StorageTableBinding(TableName.valueOf("synthetic"), new byte[] {'d'},
        StorageTableBinding.Layout.HISTORY, salt);
    try (var scope = RequestScope.withoutDeadline()) {
      var store = decoder(binding, scope);
      var key = ByteKey.copyOf(keyBytes);
      var failure = assertThrows(CompletionException.class,
          () -> store.get(key, Long.MAX_VALUE, ReadProjection.METADATA).join());
      var backend = assertInstanceOf(BackendFailure.class, failure.getCause());
      assertInstanceOf(IllegalArgumentException.class, backend.getCause());
      verify(salt, never()).salt(any(byte[].class));
    }
  }

  private static StorageTableBinding binding() {
    return binding(StorageTableBinding.Layout.CURRENT);
  }

  private static StorageTableBinding binding(StorageTableBinding.Layout layout) {
    return new StorageTableBinding(TableName.valueOf("synthetic"), new byte[] {'d'},
        layout, new HashSaltStrategy());
  }

  private static Result storedRow(StorageTableBinding binding, long rowRevision,
      StorageMetadata metadata, long timestamp) {
    var key = ByteKey.empty();
    byte[] rowKey = binding.codec().encode(key, rowRevision);
    var metadataCell = new KeyValue(rowKey, binding.family(), HBaseRecordStore.METADATA, timestamp,
        StorageMessages.metadata(metadata));
    if (metadata.state() == StorageMetadata.State.TOMBSTONE) {
      return Result.create(List.of(metadataCell));
    }
    var record = StoredRecord.live(key, metadata, new byte[metadata.valueLength()]);
    return Result.create(List.of(metadataCell,
        new KeyValue(rowKey, binding.family(), HBaseRecordStore.VALUE, timestamp, StorageMessages.value(record))));
  }

  private static Runnable pendingPointRead(AsyncTable<?> table, StorageTableBinding.Layout layout, Result row) {
    if (layout == StorageTableBinding.Layout.HISTORY) {
      var pending = new CompletableFuture<List<Result>>();
      doReturn(pending).when(table).scanAll(any(Scan.class));
      return () -> pending.complete(List.of(row));
    }
    var pending = new CompletableFuture<Result>();
    doReturn(pending).when(table).get(any(Get.class));
    return () -> pending.complete(row);
  }

  private static CompletableFuture<?> pendingWrite(AsyncTable<?> table, StorageTableBinding.Layout layout) {
    if (layout == StorageTableBinding.Layout.HISTORY) {
      var pending = new CompletableFuture<CheckAndMutateResult>();
      doReturn(pending).when(table).checkAndMutate(any(CheckAndMutate.class));
      return pending;
    }
    var pending = new CompletableFuture<Void>();
    doReturn(pending).when(table).put(any(Put.class));
    return pending;
  }

  private static Result observedRow(Result row, AtomicReference<Thread> decodedOn) {
    List<Cell> cells = Arrays.stream(row.rawCells()).map(cell ->
        (Cell) Proxy.newProxyInstance(Cell.class.getClassLoader(), new Class<?>[] {Cell.class},
            (proxy, method, arguments) -> {
              if (method.getName().equals("getValueArray")) decodedOn.set(Thread.currentThread());
              return method.invoke(cell, arguments);
            })).toList();
    return Result.create(cells);
  }

  private static StorageMetadata metadata(int valueLength, StorageMetadata.State state) {
    return new StorageMetadata(1, 1, 1, 0, state, valueLength);
  }

  private static KeyValue cell(StorageTableBinding binding, byte[] qualifier, byte[] payload) {
    return new KeyValue(binding.codec().encode(ByteKey.empty(), 1), binding.family(), qualifier, 0, payload);
  }

  private static Cell guardedValue(Cell cell, int length) {
    return (Cell) Proxy.newProxyInstance(Cell.class.getClassLoader(), new Class<?>[] {Cell.class},
        (proxy, method, arguments) -> {
          if (method.getName().equals("getValueLength")) return length;
          if (method.getName().equals("getValueArray")) {
            throw new AssertionError("oversized cell value accessed before rejection");
          }
          return method.invoke(cell, arguments);
        });
  }

  private static byte[] padded(byte[] message, int length) throws Exception {
    int remaining = length - message.length;
    int tagSize = CodedOutputStream.computeTagSize(100);
    for (int padding = Math.max(0, remaining - tagSize - 5); padding <= remaining; padding++) {
      if (tagSize + CodedOutputStream.computeUInt32SizeNoTag(padding) + padding != remaining) continue;
      var output = new ByteArrayOutputStream(length);
      var coded = CodedOutputStream.newInstance(output);
      coded.writeRawBytes(message);
      coded.writeByteArray(100, new byte[padding]);
      coded.flush();
      return output.toByteArray();
    }
    throw new AssertionError("requested message length cannot be padded");
  }

  private static HBaseRecordStore decoder(StorageTableBinding binding, RequestScope scope) {
    return decoder(binding, scope, Runnable::run);
  }

  private static HBaseRecordStore decoder(StorageTableBinding binding, RequestScope scope, Executor worker) {
    var connection = mock(AsyncConnection.class);
    var admin = mock(AsyncAdmin.class);
    var factory = mock(HBaseConnectionFactory.class);
    var identity = mock(AuthorizedBackendIdentity.class);
    doReturn(CompletableFuture.completedFuture(connection)).when(factory).connectionFor(identity);
    doReturn(admin).when(connection).getAdmin();
    doReturn(CompletableFuture.completedFuture(binding.descriptor())).when(admin).getDescriptor(binding.table());
    AsyncTableBuilder<?> builder = mock(AsyncTableBuilder.class, RETURNS_SELF);
    doReturn(builder).when(connection).getTableBuilder(binding.table());
    doReturn(mock(AsyncTable.class)).when(builder).build();
    return HBaseRecordStore.open(factory, identity, binding, scope, worker).join();
  }

}
