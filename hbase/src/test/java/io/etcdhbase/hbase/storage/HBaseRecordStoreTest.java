package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.protobuf.CodedOutputStream;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.jupiter.api.Test;
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
    return new StorageTableBinding(TableName.valueOf("synthetic"), new byte[] {'d'},
        StorageTableBinding.Layout.CURRENT, new HashSaltStrategy());
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
    return HBaseRecordStore.open(factory, identity, binding, scope, Runnable::run).join();
  }

}
