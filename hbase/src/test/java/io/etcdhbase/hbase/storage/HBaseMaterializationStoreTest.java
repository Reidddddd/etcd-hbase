package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.codec.RowFenceMessages;
import io.etcdhbase.hbase.codec.StorageMessages;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

class HBaseMaterializationStoreTest {
  private static final String PLAN = "a".repeat(64);

  @Test
  void rejectsInvalidStorageDescriptorDuringTableGroupStartup() {
    var salt = new HashSaltStrategy();
    var system = new SystemTableBinding(TableName.valueOf("group_system"), new byte[] {'s'},
        "group-a", "salt-a", salt);
    var primary = new StorageTableBinding(TableName.valueOf("group_kv"), new byte[] {'d'},
        StorageTableBinding.Layout.NATIVE, salt);
    var identity = new AuthorizedBackendIdentity("provider", "subject", "context",
        new AuthenticationScope("group-a", "profile-a"), Set.of("group-a"), 0);
    var systemTable = ownedDescriptor(systemDescriptor(system, salt));
    var primaryTable = ownedDescriptor(primary.descriptor());
    var ttlFamily = ColumnFamilyDescriptorBuilder.newBuilder(
        primaryTable.getColumnFamily(primary.family())).setTimeToLive(60).build();
    var invalidPrimaryTable = TableDescriptorBuilder.newBuilder(primaryTable)
        .modifyColumnFamily(ttlFamily).build();
    var connection = mock(AsyncConnection.class);
    var admin = mock(AsyncAdmin.class);
    var factory = mock(HBaseConnectionFactory.class);
    doReturn(CompletableFuture.completedFuture(connection)).when(factory).connectionFor(identity);
    doReturn(admin).when(connection).getAdmin();
    doReturn(CompletableFuture.completedFuture(systemTable)).when(admin).getDescriptor(system.table());
    doReturn(CompletableFuture.completedFuture(invalidPrimaryTable))
        .when(admin).getDescriptor(primary.table());

    try (var scope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(CompletionException.class, () ->
          HBaseMaterializationStore.openForTableGroup(factory, identity, system, primary,
              Optional.empty(), scope).join());
      var backendFailure = assertInstanceOf(BackendFailure.class, failure.getCause());
      assertInstanceOf(IllegalArgumentException.class, backendFailure.getCause());
      verify(admin).getDescriptor(system.table());
      verify(admin).getDescriptor(primary.table());
    }
  }

  @Test
  void requiresSystemAndDataTablesToBelongToTheSameProvisionedGroup() {
    var salt = new HashSaltStrategy();
    var system = new SystemTableBinding(TableName.valueOf("group_system"), new byte[] {'s'},
        "group-a", "salt-a", salt);
    var primary = new StorageTableBinding(TableName.valueOf("group_kv"), new byte[] {'d'},
        StorageTableBinding.Layout.NATIVE, salt);
    var systemTable = ownedDescriptor(systemDescriptor(system, salt));
    var dataTable = ownedDescriptor(primary.descriptor());

    assertDoesNotThrow(() -> HBaseMaterializationStore.validateTableGroupDescriptors(
        system, systemTable, primary, dataTable, Optional.empty()));
    for (var attribute : List.of("etcdhbase.m0.table-group-id",
        "etcdhbase.m0.backend-cluster-id", "etcdhbase.m0.plan-fingerprint",
        "etcdhbase.m0.history-layout", "etcdhbase.m0.salt-profile-id",
        "etcdhbase.m0.schema-version")) {
      var foreign = TableDescriptorBuilder.newBuilder(dataTable).setValue(attribute, "other").build();
      assertThrows(IllegalArgumentException.class,
          () -> HBaseMaterializationStore.validateTableGroupDescriptors(
              system, systemTable, primary, foreign, Optional.empty()), attribute);
    }
    var missing = TableDescriptorBuilder.newBuilder(dataTable)
        .removeValue("etcdhbase.m0.plan-fingerprint").build();
    assertThrows(IllegalArgumentException.class,
        () -> HBaseMaterializationStore.validateTableGroupDescriptors(
            system, systemTable, primary, missing, Optional.empty()));
  }

  @Test
  void requiresTheCurrentTableToBelongToTheHistoryGroup() {
    var salt = new HashSaltStrategy();
    var system = new SystemTableBinding(TableName.valueOf("group_system"), new byte[] {'s'},
        "group-a", "salt-a", salt);
    var history = new StorageTableBinding(TableName.valueOf("group_history"), new byte[] {'h'},
        StorageTableBinding.Layout.HISTORY, salt);
    var current = new StorageTableBinding(TableName.valueOf("group_current"), new byte[] {'d'},
        StorageTableBinding.Layout.CURRENT, salt);
    var systemTable = historyLayout(ownedDescriptor(systemDescriptor(system, salt)));
    var historyTable = historyLayout(ownedDescriptor(history.descriptor()));
    var currentTable = historyLayout(ownedDescriptor(current.descriptor()));

    assertDoesNotThrow(() -> HBaseMaterializationStore.validateTableGroupDescriptors(
        system, systemTable, history, historyTable, Optional.of(currentTable)));
    var foreignCurrent = TableDescriptorBuilder.newBuilder(currentTable)
        .setValue("etcdhbase.m0.table-group-id", "foreign").build();
    assertThrows(IllegalArgumentException.class,
        () -> HBaseMaterializationStore.validateTableGroupDescriptors(
            system, systemTable, history, historyTable, Optional.of(foreignCurrent)));
    assertThrows(IllegalArgumentException.class,
        () -> HBaseMaterializationStore.validateTableGroupDescriptors(
            system, systemTable, history, historyTable, Optional.empty()));
  }

  private static TableDescriptor systemDescriptor(SystemTableBinding binding, HashSaltStrategy salt) {
    return TableDescriptorBuilder.newBuilder(binding.table())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(binding.family())
            .setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.salt-strategy-id", salt.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version", "1")
        .setValue("etcdhbase.m0.salt-bucket-count", "16")
        .setValue("etcdhbase.m0.salt-width-bytes", "2")
        .setValue("etcdhbase.m0.record-categories",
            "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION,SNAPSHOT_PIN,RESULT")
        .build();
  }

  private static TableDescriptor ownedDescriptor(TableDescriptor descriptor) {
    return TableDescriptorBuilder.newBuilder(descriptor)
        .setValue("etcdhbase.m0.table-group-id", "group-a")
        .setValue("etcdhbase.m0.backend-cluster-id", "fixture-cluster")
        .setValue("etcdhbase.m0.plan-fingerprint", PLAN)
        .setValue("etcdhbase.m0.history-layout", "NATIVE_CELL_VERSIONS")
        .setValue("etcdhbase.m0.salt-profile-id", "salt-a")
        .setValue("etcdhbase.m0.schema-version", "1")
        .build();
  }

  private static TableDescriptor historyLayout(TableDescriptor descriptor) {
    return TableDescriptorBuilder.newBuilder(descriptor)
        .setValue("etcdhbase.m0.history-layout", "SEPARATE_HISTORY_TABLE").build();
  }
  @Test
  void rejectsOversizedUnknownQualifierBeforeAccessingItsBackingArray() {
    var binding = new StorageTableBinding(TableName.valueOf("synthetic"), new byte[] {'d'},
        StorageTableBinding.Layout.NATIVE, new HashSaltStrategy());
    var row = Result.create(List.of(guardedQualifier(cell(), 4097)));

    assertThrows(IllegalArgumentException.class,
        () -> HBaseMaterializationStore.validateQualifiers(
            binding, row, RowFenceMessages.RowKind.NATIVE));
  }

  @Test
  void rejectsOversizedCellsBeforeAccessingTheirBackingArrays() {
    Cell cell = cell();

    assertThrows(IllegalArgumentException.class,
        () -> HBaseMaterializationStore.copyMetadata(guardedValue(cell, 4097)));
    assertThrows(IllegalArgumentException.class,
        () -> HBaseMaterializationStore.copyValuePayload(guardedValue(cell,
            StorageMessages.MAX_VALUE_BYTES + 129)));
    assertThrows(IllegalArgumentException.class,
        () -> HBaseMaterializationStore.copyFence(guardedValue(cell,
            RowFenceMessages.MAX_BYTES + 1)));
  }

  private static Cell cell() {
    return new KeyValue(new byte[] {1}, new byte[] {'d'}, new byte[] {'q'}, 1L, new byte[] {1});
  }

  private static Cell guardedQualifier(Cell cell, int length) {
    return (Cell) Proxy.newProxyInstance(Cell.class.getClassLoader(), new Class<?>[] {Cell.class},
        (proxy, method, arguments) -> {
          if (method.getName().equals("getQualifierLength")) return length;
          if (method.getName().equals("getQualifierArray")) {
            throw new AssertionError("oversized cell qualifier accessed before rejection");
          }
          return method.invoke(cell, arguments);
        });
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
}
