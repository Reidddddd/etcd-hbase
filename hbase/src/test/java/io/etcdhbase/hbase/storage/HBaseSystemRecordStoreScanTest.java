package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertFalse;
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

    var store = new HBaseSystemRecordStore(connection, binding, true, false);
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
  void rejectsAuxiliaryAccessBeforeTouchingAnOlderSystemTable() {
    var binding = new SystemTableBinding(TableName.valueOf("j40_legacy"), new byte[] {'s'},
        "j40-legacy", "j40-salt", new HashSaltStrategy());
    var store = new HBaseSystemRecordStore(mock(AsyncConnection.class), binding, true, false);
    ByteKey identity = io.etcdhbase.core.lease.LeaseRecordIdentity.enumeration(-7);
    assertThrows(IllegalStateException.class, () -> store.bucket(SystemRecordType.LEASE_ENUMERATION, identity));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(1))) {
      assertThrows(IllegalStateException.class, () -> store.read(SystemRecordType.LEASE_ENUMERATION, identity, scope));
    }
  }
}
