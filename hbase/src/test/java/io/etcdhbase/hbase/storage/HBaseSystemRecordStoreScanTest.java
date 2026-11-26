package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.time.Duration;
import java.util.Optional;
import java.util.List;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import io.etcdhbase.spi.salt.SaltProfile;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.jupiter.api.Test;

class HBaseSystemRecordStoreScanTest {
  @SuppressWarnings("unchecked")
  @Test
  void cancelledScanRetainsPhysicalOwnershipUntilHBaseSignalsCompletion() throws Exception {
    AsyncConnection connection = mock(AsyncConnection.class);
    AsyncTableBuilder<AdvancedScanResultConsumer> builder =
        mock(AsyncTableBuilder.class);
    AsyncTable<AdvancedScanResultConsumer> table = mock(AsyncTable.class);
    var tableName = TableName.valueOf("j40_scan_unit");
    var binding = new SystemTableBinding(tableName, new byte[] {'s'}, "j40-scan", "j40-salt",
        new HashSaltStrategy());
    when(connection.getTableBuilder(tableName)).thenReturn(builder);
    when(builder.setOperationTimeout(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(builder);
    when(builder.setRpcTimeout(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(builder);
    when(builder.setReadRpcTimeout(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(builder);
    when(builder.setScanTimeout(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(builder);
    when(builder.build()).thenReturn(table);

    var receivedConsumer = new AtomicReference<AdvancedScanResultConsumer>();
    doAnswer(invocation -> {
      receivedConsumer.set(invocation.getArgument(1));
      return null;
    }).when(table).scan(any(Scan.class), any(AdvancedScanResultConsumer.class));

    var store = new HBaseSystemRecordStore(connection, binding, true, false, false, false, false, false);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      ByteKey identity = ByteKey.copyOf(new byte[] {1});
      int bucket = binding.keyCodec().bucket(SystemRecordType.WORKSET, identity);
      var pending = store.scanBucket(SystemRecordType.WORKSET, bucket,
          Optional.empty(), 2, 100, scope);
      assertNotNull(receivedConsumer.get());

      scope.cancel();
      assertThrows(CompletionException.class, pending::join);
      var physicalCompletion = scope.physicalProducersCompletion().toCompletableFuture();
      assertFalse(physicalCompletion.isDone());

      var controller = mock(AdvancedScanResultConsumer.ScanController.class);
      receivedConsumer.get().onHeartbeat(controller);
      verify(controller).terminate();
      assertFalse(physicalCompletion.isDone());

      receivedConsumer.get().onComplete();
      physicalCompletion.get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void validatesPhysicalRowOrderForLengthFramedIdentities() {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var scan = new ScanHarness();
      var shortIdentity = ByteKey.copyOf(new byte[] {9});
      var longIdentity = ByteKey.copyOf(new byte[] {1, 1});
      var result = scan.store.scanBucket(SystemRecordType.WORKSET, 0, Optional.empty(), 4, 100, scope);
      scan.deliver(scan.row(shortIdentity), scan.row(longIdentity));
      assertEquals(List.of(shortIdentity, longIdentity), result.join().entries().stream()
          .map(io.etcdhbase.core.commit.SystemRecordEntry::identity).toList());
    }
  }

  @Test
  void rejectsBackwardPhysicalRowsEvenWhenLogicalIdentitiesAreIncreasing() {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var scan = new ScanHarness();
      var result = scan.store.scanBucket(SystemRecordType.WORKSET, 0, Optional.empty(), 4, 100, scope);
      scan.deliver(scan.row(ByteKey.copyOf(new byte[] {1, 1})), scan.row(ByteKey.copyOf(new byte[] {9})));
      assertThrows(CompletionException.class, result::join);
    }
  }

  private static final class ScanHarness {
    private final AtomicReference<AdvancedScanResultConsumer> consumer = new AtomicReference<>();
    private final SystemTableBinding binding;
    private final HBaseSystemRecordStore store;

    @SuppressWarnings("unchecked")
    private ScanHarness() {
      var connection = mock(AsyncConnection.class);
      AsyncTableBuilder<AdvancedScanResultConsumer> builder = mock(AsyncTableBuilder.class);
      AsyncTable<AdvancedScanResultConsumer> table = mock(AsyncTable.class);
      var salt = new HashSaltStrategy(new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 1, 2));
      binding = new SystemTableBinding(TableName.valueOf("j52_scan_order"), new byte[] {'s'}, "j52", "salt", salt);
      when(connection.getTableBuilder(binding.table())).thenReturn(builder);
      when(builder.setOperationTimeout(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(builder);
      when(builder.setRpcTimeout(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(builder);
      when(builder.setReadRpcTimeout(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(builder);
      when(builder.setScanTimeout(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(builder);
      when(builder.build()).thenReturn(table);
      doAnswer(invocation -> { consumer.set(invocation.getArgument(1)); return null; })
          .when(table).scan(any(Scan.class), any(AdvancedScanResultConsumer.class));
      store = new HBaseSystemRecordStore(connection, binding, true, false, false, false, false, false);
    }

    private Result row(ByteKey identity) {
      return Result.create(List.of(new KeyValue(binding.row(SystemRecordType.WORKSET, identity),
          binding.family(), new byte[] {'r'}, new byte[] {1})));
    }

    private void deliver(Result... rows) {
      consumer.get().onNext(rows, mock(AdvancedScanResultConsumer.ScanController.class));
      consumer.get().onComplete();
    }
  }

  @Test
  void rejectsCompactRecordsBeforeTouchingAnUndeclaredTable() {
    var binding = new SystemTableBinding(TableName.valueOf("j52_legacy"), new byte[] {'s'},
        "j52-legacy", "j52-salt", new HashSaltStrategy());
    var connection = mock(AsyncConnection.class);
    var store = new HBaseSystemRecordStore(connection, binding, true,
        false, false, false, false, false);
    try (var scope = RequestScope.withoutDeadline()) {
      assertThrows(IllegalStateException.class, () -> store.read(SystemRecordType.COMPACTION,
          ByteKey.copyOf(new byte[16]), scope));
      org.mockito.Mockito.verifyNoInteractions(connection);
    }
  }

  @Test
  void rejectsAuxiliaryAccessBeforeTouchingAnOlderSystemTable() {
    var binding = new SystemTableBinding(TableName.valueOf("j40_legacy"), new byte[] {'s'},
        "j40-legacy", "j40-salt", new HashSaltStrategy());
    var store = new HBaseSystemRecordStore(mock(AsyncConnection.class), binding, true,
        false, false, false, false, false);
    ByteKey identity = io.etcdhbase.core.lease.LeaseRecordIdentity.enumeration(-7);
    assertThrows(IllegalStateException.class, () -> store.bucket(SystemRecordType.LEASE_ENUMERATION, identity));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(1))) {
      assertThrows(IllegalStateException.class, () -> store.read(SystemRecordType.LEASE_ENUMERATION, identity, scope));
    }
  }

  @Test
  void rejectsLeaseBindingsWhenOnlyJ40AuxiliaryCapabilityIsPresent() {
    var binding = new SystemTableBinding(TableName.valueOf("j40_auxiliary_only"), new byte[] {'s'},
        "j40-auxiliary-only", "j40-salt", new HashSaltStrategy());
    var store = new HBaseSystemRecordStore(mock(AsyncConnection.class), binding, true,
        true, false, false, false, false);
    ByteKey identity = new io.etcdhbase.core.lease.LeaseBindingIdentity(
        1, 1, ByteKey.copyOf(new byte[] {1}), 1).encode();

    assertThrows(IllegalStateException.class,
        () -> store.bucket(SystemRecordType.LEASE_BINDING, identity));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(1))) {
      assertThrows(IllegalStateException.class,
          () -> store.read(SystemRecordType.LEASE_BINDING, identity, scope));
    }
  }

  @Test
  void rejectsEventIndexWhenOnlyLeaseExpiryCapabilityIsPresent() {
    var binding = new SystemTableBinding(TableName.valueOf("j44_expiry_only"), new byte[] {'s'},
        "j44-expiry-only", "j44-salt", new HashSaltStrategy());
    var store = new HBaseSystemRecordStore(mock(AsyncConnection.class), binding, true,
        true, true, true, false, false);
    ByteKey identity = ByteKey.copyOf(java.nio.ByteBuffer.allocate(Long.BYTES).putLong(1).array());
    assertThrows(IllegalStateException.class,
        () -> store.bucket(SystemRecordType.EVENT_REVISION_INDEX, identity));
  }
}
