package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BucketCursor;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.core.scan.SortedMergeCursor;
import io.etcdhbase.core.scan.ExistsExecutor;
import io.etcdhbase.core.scan.DirectRecordReader;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class M1StorageIT {
  private static TemporaryHBaseClusterProcess cluster;
  private static HBaseBackendIdentityProvider provider;
  private static AsyncConnection connection;
  private static AuthorizedBackendIdentity identity;
  private static AuthorizedBackendIdentity independentIdentity;
  private static ThreadPoolExecutor worker;
  private static final List<TableName> tables = new ArrayList<>();
  private static final HashSaltStrategy SALT = new HashSaltStrategy(new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2));

  @BeforeAll
  static void start() throws Exception {
    cluster = TemporaryHBaseClusterProcess.start();
    worker = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(32));
    var authentication = new AuthenticationScope("m1", "public");
    provider = new HBaseBackendIdentityProvider("m1-simple", authentication, cluster.clientConfiguration(),
        HBaseAuthenticators.simpleDevelopment(), (client, scope) -> client.getAdmin().listNamespaces().thenApply(ignored -> null),
        2, 2, Duration.ofSeconds(30));
    try (var credentials = new BackendCredentials("m1-fixture", new char[0])) {
      identity = await(provider.authenticate(credentials, authentication).toCompletableFuture());
    }
    try (var credentials = new BackendCredentials("m1-independent", new char[0])) {
      independentIdentity = await(provider.authenticate(credentials, authentication).toCompletableFuture());
    }
    connection = await(provider.connectionFor(identity).toCompletableFuture());
  }

  @AfterAll
  static void close() throws Exception {
    try {
      if (connection != null) {
        for (TableName table : tables) {
          await(connection.getAdmin().disableTable(table));
          await(connection.getAdmin().deleteTable(table));
        }
      }
    } finally {
      try {
        if (worker != null) {
          worker.shutdownNow();
          assertTrue(worker.awaitTermination(20, TimeUnit.SECONDS));
        }
      } finally {
        try {
          if (provider != null) provider.close();
        } finally {
          if (cluster != null) cluster.close();
        }
      }
    }
  }

  @Test
  void realGetPutAtomicCasBatchAndRawByteBucketOrder() throws Exception {
    var binding = table(StorageTableBinding.Layout.CURRENT);
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(60))) {
      var store = await(HBaseRecordStore.open(provider, identity, binding, scope, worker));
      ByteKey key = ByteKey.copyOf(new byte[] {0, (byte) 0xff});
      StoredRecord first = live(key, 1, new byte[] {1});
      assertTrue(await(store.get(key, Long.MAX_VALUE, ReadProjection.FULL)).isEmpty());
      assertTrue(await(store.compareAndSet(key, Optional.empty(), first)));
      assertFalse(await(store.compareAndSet(key, Optional.empty(), live(key, 2, new byte[] {2}))));
      assertTrue(await(store.compareAndSet(key, Optional.of(first.metadata()), live(key, 2, new byte[] {2}))));
      assertFalse(await(store.compareAndSet(key, Optional.of(first.metadata()), live(key, 3, new byte[] {3}))));
      assertArrayEquals(new byte[] {2}, await(store.get(key, Long.MAX_VALUE, ReadProjection.FULL)).orElseThrow().value().orElseThrow());
      try (var independentScope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
        var independent = await(HBaseRecordStore.open(provider, independentIdentity, binding, independentScope, worker));
        assertNotSame(connection, await(provider.connectionFor(independentIdentity).toCompletableFuture()));
        assertArrayEquals(new byte[] {2}, await(independent.get(key, Long.MAX_VALUE, ReadProjection.FULL)).orElseThrow().value().orElseThrow());
      }
      assertFalse(await(store.get(key, Long.MAX_VALUE, ReadProjection.METADATA)).orElseThrow().hasValue());
      var records = List.of(live(ByteKey.copyOf(new byte[] {0x61}), 4, new byte[0]), live(ByteKey.copyOf(new byte[] {0x61, 0}), 5, new byte[] {5}));
      var outcomes = await(store.putBatch(records));
      assertTrue(outcomes.stream().allMatch(outcome -> outcome.failure().isEmpty()));
      List<ByteKey> observed = new ArrayList<>();
      for (int bucket = 0; bucket < 4; bucket++) {
        var page = await(store.scan(new ScanRequest(bucket, KeyRange.all(), Optional.empty(), Long.MAX_VALUE, ReadProjection.FULL, 16, 1_000_000, 128)));
        observed.addAll(page.records().stream().map(StoredRecord::key).toList());
        assertTrue(page.exhausted());
      }
      assertEquals(List.of(key, records.get(0).key(), records.get(1).key()).stream().sorted().toList(), observed.stream().sorted().toList());
      scope.cancel();
      assertFalse(connection.isClosed());
    }
  }

  @Test
  void cursorReopensAfterOnlyConsumedKeysAndCancelsWithoutClosingSharedConnections() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      var binding = table(layout);
      try (var scope = RequestScope.withTimeout(Duration.ofSeconds(60))) {
        var store = await(HBaseRecordStore.open(provider, identity, binding, scope, worker));
        List<ByteKey> expected = new ArrayList<>();
        for (int index = 0; index < 40; index++) {
          ByteKey key = ByteKey.copyOf(new byte[] {0, (byte) index, (byte) 0xff});
          expected.add(key);
          await(store.put(live(key, 1, new byte[] {(byte) index})));
        }
        List<ByteKey> observed = new ArrayList<>();
        for (int bucket = 0; bucket < SALT.profile().bucketCount(); bucket++) {
          var request = new ScanRequest(bucket, KeyRange.all(), Optional.empty(), Long.MAX_VALUE,
              ReadProjection.METADATA, 3, 4096, 128);
          try (var cursor = new BucketCursor(store, request, scope)) {
            boolean first = true;
            while (cursor.state() != BucketCursor.State.EXHAUSTED) {
              var result = cursor.fetch();
              if (first) cursor.pause();
              try (var page = await(result)) {
                if (first) page.next().ifPresent(record -> observed.add(record.key()));
                else {
                  Optional<StoredRecord> record;
                  while ((record = page.next()).isPresent()) observed.add(record.get().key());
                }
              }
              cursor.resume();
              first = false;
            }
          }
        }
        assertEquals(expected.stream().sorted().toList(), observed.stream().sorted().toList());
        assertEquals(expected.size(), observed.stream().distinct().count());
        var cursor = new BucketCursor(store, new ScanRequest(0, KeyRange.all(), Optional.empty(),
            Long.MAX_VALUE, ReadProjection.METADATA, 1, 4096, 128), scope);
        cursor.close();
        assertFalse(scope.isClosed());
        assertTrue(await(store.get(expected.getFirst(), 1, ReadProjection.FULL)).isPresent());
        assertFalse(connection.isClosed());
      }
    }
  }

  @Test
  void onePhysicalScannerCanSupplyEveryBucketHeadWithoutClosingTheRequest() throws Exception {
    var binding = table(StorageTableBinding.Layout.NATIVE);
    var limits = new BudgetManager.Limits(2, 32, 1, 1, 1, 65536, 128L << 20);
    var budget = new BudgetManager(limits, Map.of("public", limits), limits);
    var reservation = new BudgetManager.Reservation(4 * 33024L, 4096, 64L << 20, 1L << 20);
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(60));
        var account = budget.admit("public", scope, reservation);
        var scheduler = new ScanScheduler(budget, Duration.ofSeconds(10))) {
      var store = await(HBaseRecordStore.open(provider, identity, binding, scope, worker));
      for (int index = 0; index < 40; index++) await(store.put(live(ByteKey.copyOf(new byte[] {(byte) index}), 1, new byte[0])));
      var source = scheduler.bind(store, account);
      List<BucketCursor> cursors = new ArrayList<>();
      List<CompletableFuture<BucketCursor.Page>> pending = new ArrayList<>();
      List<BudgetManager.Lease> heads = new ArrayList<>();
      try {
        for (int bucket = 0; bucket < 4; bucket++) {
          var cursor = new BucketCursor(source, new ScanRequest(bucket, KeyRange.all(), Optional.empty(),
              Long.MAX_VALUE, ReadProjection.METADATA, 1, 4096, 128), scope);
          cursors.add(cursor);
          pending.add(cursor.fetch());
        }
        for (var result : pending) {
          try (var page = await(result)) {
            var head = page.next().orElseThrow();
            heads.add(account.tryRetain(BudgetManager.Memory.HEAD, head.retainedBytes()).orElseThrow());
            assertEquals(0, budget.snapshot().scanners(), "head memory does not own a physical scanner");
          }
        }
        assertEquals(4, heads.size());
      } finally {
        for (var cursor : cursors) cursor.close();
        for (var head : heads) head.close();
      }
      assertEquals(0, budget.snapshot().allocatedBytes());
      assertFalse(scope.isClosed());
      assertFalse(connection.isClosed());
    }
    assertEquals(0, budget.snapshot().requests());
  }

  @Test
  void globallySortedLimitMoreAndLightCountMatchBothFrozenHistoryLayouts() throws Exception {
    var limits = new BudgetManager.Limits(2, 32, 1, 1, 1, 65536, 128L << 20);
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      var binding = table(layout);
      List<ByteKey> expected = new ArrayList<>();
      try (var writing = RequestScope.withTimeout(Duration.ofSeconds(60))) {
        var store = await(HBaseRecordStore.open(provider, identity, binding, writing, worker));
        for (int index = 0; index < 40; index++) {
          ByteKey key = ByteKey.copyOf(new byte[] {(byte) (index * 7), 0, (byte) 0xff});
          await(store.put(live(key, 1, new byte[0])));
          if (index % 5 == 0) await(store.put(StoredRecord.metadataOnly(key,
              new StorageMetadata(1, 2, 1, 0, StorageMetadata.State.TOMBSTONE, 0))));
          else expected.add(key);
        }
      }
      expected.sort(ByteKey::compareTo);
      for (int limit : List.of(0, 1, 31, 32, 33)) {
        var budget = new BudgetManager(limits, Map.of("public", limits), limits);
        try (var scope = RequestScope.withTimeout(Duration.ofSeconds(60));
            var account = budget.admit("public", scope, new BudgetManager.Reservation(4 * 4096L, 4096, 64L << 20, 1L << 20));
            var scheduler = new ScanScheduler(budget, Duration.ofSeconds(10))) {
          var store = await(HBaseRecordStore.open(provider, identity, binding, scope, worker));
          try (var cursor = new SortedMergeCursor(scheduler.bind(store, account), new ScanRequest(0, KeyRange.all(), Optional.empty(),
              Long.MAX_VALUE, ReadProjection.METADATA, 1, 4096, 256), 4, account, scope);
              var page = await(cursor.read(limit))) {
            var selected = limit == 0 ? expected : expected.stream().limit(limit).toList();
            assertEquals(selected, page.records().stream().map(StoredRecord::key).toList());
            assertEquals(selected.size(), page.count());
            assertEquals(limit > 0 && expected.size() > limit, page.more());
          }
          assertEquals(0, budget.snapshot().allocatedBytes());
        }
      }
    }
  }

  @Test
  void metadataExistsAvoidsUnselectedValuesAndFullResultsPinTheSelectedVersion() throws Exception {
    var limits = new BudgetManager.Limits(2, 32, 1, 1, 1, 65536, 128L << 20);
    ByteKey first = ByteKey.copyOf(new byte[] {1});
    ByteKey corrupt = ByteKey.copyOf(new byte[] {2});
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      var binding = table(layout);
      var budget = new BudgetManager(limits, Map.of("public", limits), limits);
      try (var scope = RequestScope.withTimeout(Duration.ofSeconds(60));
          var account = budget.admit("public", scope, new BudgetManager.Reservation(4 * 4096L, 4096, 64L << 20, 1L << 20));
          var scheduler = new ScanScheduler(budget, Duration.ofSeconds(10))) {
        var store = await(HBaseRecordStore.open(provider, identity, binding, scope, worker));
        await(store.put(live(first, 1, new byte[] {1})));
        await(store.put(live(first, 2, new byte[] {2})));
        await(store.put(live(corrupt, 1, new byte[] {9})));
        await(connection.getTable(binding.table()).delete(new org.apache.hadoop.hbase.client.Delete(binding.codec().encode(corrupt, 1))
            .addColumns(binding.family(), HBaseRecordStore.VALUE)));
        assertEquals(1, await(ExistsExecutor.point(store, corrupt, 1, account)));
        var template = new ScanRequest(0, KeyRange.all(), Optional.empty(), 1, ReadProjection.METADATA, 1, 4096, 256);
        var source = scheduler.bind(store, account);
        try (var exists = new ExistsExecutor(source, template, 4, scope)) {
          assertEquals(1, await(exists.execute()).count());
        }
        try (var cursor = new SortedMergeCursor(source, template, 4, account, scope); var page = await(cursor.read(0))) {
          assertEquals(2, page.count());
          assertTrue(page.records().stream().noneMatch(StoredRecord::hasValue));
          assertEquals(0, page.pointReads());
        }
        try (var cursor = SortedMergeCursor.values(store, source, template, 4, account, scope); var page = await(cursor.read(1))) {
          assertEquals(1, page.count());
          assertTrue(page.more());
          assertEquals(1, page.pointReads());
          assertArrayEquals(new byte[] {1}, page.records().getFirst().value().orElseThrow());
          assertEquals(1, page.records().getFirst().metadata().modificationRevision());
        }
        try (var cursor = SortedMergeCursor.values(store, source, template, 4, account, scope)) {
          assertThrows(java.util.concurrent.ExecutionException.class, () -> await(cursor.read(2)));
        }
        assertEquals(0, budget.snapshot().allocatedBytes());
      }
    }
  }

  @Test
  void directReaderDoesNotCacheAndRequestCancellationClosesRetainedResults() throws Exception {
    var binding = table(StorageTableBinding.Layout.CURRENT);
    var limits = new BudgetManager.Limits(2, 8, 1, 1, 1, 65536, 128L << 20);
    var budget = new BudgetManager(limits, Map.of("public", limits), limits);
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(30));
        var account = budget.admit("public", scope, new BudgetManager.Reservation(4096, 4096, 64L << 20, 1L << 20))) {
      var store = await(HBaseRecordStore.open(provider, identity, binding, scope, worker));
      ByteKey key = ByteKey.copyOf(new byte[] {1});
      await(store.put(live(key, 1, new byte[] {1})));
      var reader = new DirectRecordReader(store, account, 1L << 20);
      try (var first = await(reader.read(key, Long.MAX_VALUE, ReadProjection.FULL))) {
        assertArrayEquals(new byte[] {1}, first.record().orElseThrow().value().orElseThrow());
      }
      await(store.put(live(key, 2, new byte[] {2})));
      var second = await(reader.read(key, Long.MAX_VALUE, ReadProjection.FULL));
      assertArrayEquals(new byte[] {2}, second.record().orElseThrow().value().orElseThrow());
      assertEquals(2, account.pointReads());
      scope.cancel();
      assertThrows(IllegalStateException.class, second::record);
      await(account.termination().toCompletableFuture());
      assertEquals(0, budget.snapshot().allocatedBytes());
      assertFalse(connection.isClosed());
    }
  }

  @Test
  void partialBatchResultsKeepIndexesAndValueMismatchIsNotAnAbsentKey() throws Exception {
    var binding = table(StorageTableBinding.Layout.HISTORY);
    ByteKey first = ByteKey.copyOf(new byte[] {1});
    ByteKey second = ByteKey.copyOf(new byte[] {2});
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
      var store = await(HBaseRecordStore.open(provider, identity, binding, scope, worker));
      await(store.put(live(first, 1, new byte[] {1})));
      var outcomes = await(store.putBatch(List.of(live(first, 1, new byte[] {9}), live(second, 2, new byte[] {2}))));
      assertEquals(0, outcomes.get(0).index());
      assertEquals(io.etcdhbase.core.storage.BatchWriteResult.State.FAILED, outcomes.get(0).state());
      assertTrue(outcomes.get(0).failure().isPresent());
      assertEquals(1, outcomes.get(1).index());
      assertTrue(outcomes.get(1).failure().isEmpty());
      assertArrayEquals(new byte[] {1}, await(store.get(first, 1, ReadProjection.FULL)).orElseThrow().value().orElseThrow());
      byte[] physical = binding.codec().encode(second, 2);
      await(connection.getTable(binding.table()).delete(new org.apache.hadoop.hbase.client.Delete(physical)
          .addColumns(binding.family(), HBaseRecordStore.VALUE)));
      assertThrows(java.util.concurrent.ExecutionException.class, () -> await(store.get(second, 2, ReadProjection.FULL)));
      assertTrue(await(store.get(second, 2, ReadProjection.METADATA)).isPresent());
      scope.cancel();
      assertThrows(java.util.concurrent.ExecutionException.class, () -> await(store.get(first, 1, ReadProjection.METADATA)));
    }
  }

  @Test
  void nativeTimestampCollisionIsRecordedAsACandidateLimitation() throws Exception {
    var binding = table(StorageTableBinding.Layout.NATIVE);
    ByteKey key = ByteKey.copyOf(new byte[] {7});
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
      var store = await(HBaseRecordStore.open(provider, identity, binding, scope, worker));
      await(store.put(live(key, 1, new byte[] {1})));
      await(store.put(live(key, 1, new byte[] {9})));
      assertArrayEquals(new byte[] {9}, await(store.get(key, 1, ReadProjection.FULL)).orElseThrow().value().orElseThrow());
      System.out.println("j11-native sameTimestampOverwrite=true transactionIsolation=not_provided");
    }
  }

  @Test
  void bothHistoryLayoutsKeepDeleteRecreateAndVersionProjectionAfterFlush() throws Exception {
    ByteKey key = ByteKey.copyOf(new byte[] {0x61, 0, (byte) 0xff});
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      var binding = table(layout);
      try (var scope = RequestScope.withTimeout(Duration.ofSeconds(60))) {
        var store = await(HBaseRecordStore.open(provider, identity, binding, scope, worker));
        await(store.put(live(key, 1, new byte[] {1})));
        await(store.put(StoredRecord.metadataOnly(key, new StorageMetadata(1, 2, 1, 0, StorageMetadata.State.TOMBSTONE, 0))));
        await(store.put(recreated(key, 3)));
        for (boolean flushed : List.of(false, true)) {
          if (flushed) await(connection.getAdmin().flush(binding.table()));
          assertArrayEquals(new byte[] {1}, await(store.get(key, 1, ReadProjection.FULL)).orElseThrow().value().orElseThrow());
          assertEquals(StorageMetadata.State.TOMBSTONE, await(store.get(key, 2, ReadProjection.FULL)).orElseThrow().metadata().state());
          assertArrayEquals(new byte[0], await(store.get(key, 3, ReadProjection.FULL)).orElseThrow().value().orElseThrow());
          assertEquals(3, await(store.get(key, 3, ReadProjection.METADATA)).orElseThrow().metadata().createRevision());
        }
      }
    }
  }

  private static StorageTableBinding table(StorageTableBinding.Layout layout) throws Exception {
    return table(layout, new Retention(0, layout == StorageTableBinding.Layout.NATIVE ? 64 : 1, KeepDeletedCells.FALSE, false));
  }

  private static StorageTableBinding table(StorageTableBinding.Layout layout, Retention retention) throws Exception {
    var name = TableName.valueOf("m1_" + UUID.randomUUID().toString().replace("-", ""));
    var binding = new StorageTableBinding(name, new byte[] {'d'}, layout, SALT);
    var family = ColumnFamilyDescriptorBuilder.newBuilder(binding.family())
        .setMinVersions(retention.minimum()).setMaxVersions(retention.maximum())
        .setKeepDeletedCells(retention.keep()).setNewVersionBehavior(retention.newBehavior())
        .setConfiguration("hbase.hstore.compaction.min", "2").build();
    var descriptor = TableDescriptorBuilder.newBuilder(binding.descriptor()).modifyColumnFamily(family).build();
    await(connection.getAdmin().createTable(descriptor, SALT.splitKeys().toArray(byte[][]::new)));
    tables.add(name);
    return binding;
  }

  @Test
  void bothLayoutsRetainVersionsAfterObservedMinorAndMajorCompaction() throws Exception {
    var admin = connection.getAdmin();
    var servers = await(admin.getRegionServers()).stream().map(ServerName::getServerName).toList();
    Map<ServerName, Boolean> previous = await(admin.compactionSwitch(false, servers));
    try {
      for (var retention : List.of(new Retention(0, 64, KeepDeletedCells.FALSE, false),
          new Retention(1, 64, KeepDeletedCells.FALSE, false),
          new Retention(0, 64, KeepDeletedCells.TRUE, false),
          new Retention(0, 64, KeepDeletedCells.TRUE, true))) {
        for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
          compactAndVerify(layout, retention, servers);
        }
      }
    } finally {
      for (var entry : previous.entrySet()) {
        await(admin.compactionSwitch(entry.getValue(), List.of(entry.getKey().getServerName())));
      }
    }
  }

  private static void compactAndVerify(StorageTableBinding.Layout layout, Retention retention, List<String> servers) throws Exception {
    var admin = connection.getAdmin();
    await(admin.compactionSwitch(false, servers));
    var binding = table(layout, layout == StorageTableBinding.Layout.HISTORY
        ? new Retention(Math.min(1, retention.minimum()), 1, retention.keep(), retention.newBehavior()) : retention);
    var currentBinding = layout == StorageTableBinding.Layout.HISTORY ? table(StorageTableBinding.Layout.CURRENT) : binding;
    var physicalTables = currentBinding == binding ? List.of(binding) : List.of(binding, currentBinding);
    ByteKey key = ByteKey.copyOf(new byte[] {0x61, 0, (byte) 0xff});
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(180))) {
      var history = await(HBaseRecordStore.open(provider, identity, binding, scope, worker));
      var current = layout == StorageTableBinding.Layout.HISTORY
          ? await(HBaseRecordStore.open(provider, identity, currentBinding, scope, worker)) : history;
      var first = live(key, 1, new byte[] {1});
      var deleted = StoredRecord.metadataOnly(key, new StorageMetadata(1, 2, 1, 0, StorageMetadata.State.TOMBSTONE, 0));
      int applicationPutCalls = writeAndFlush(history, current, first, physicalTables);
      applicationPutCalls += writeAndFlush(history, current, deleted, physicalTables);
      Waiter.waitFor(cluster.clientConfiguration(), 30_000, 100, () -> filesAtLeast(physicalTables, 2));
      verifyHistoryPair(history, key);
      await(admin.compactionSwitch(true, servers));
      compactTables(physicalTables, key, false);
      verifyHistoryPair(history, key);
      await(admin.compactionSwitch(false, servers));
      var recreated = recreated(key, 3);
      applicationPutCalls += writeAndFlush(history, current, recreated, physicalTables);
      Waiter.waitFor(cluster.clientConfiguration(), 30_000, 100, () -> filesAtLeast(physicalTables, 2));
      await(admin.compactionSwitch(true, servers));
      compactTables(physicalTables, key, true);
      verifyHistoryPair(history, key);
      assertArrayEquals(new byte[0], await(history.get(key, 3, ReadProjection.FULL)).orElseThrow().value().orElseThrow());
      assertArrayEquals(new byte[0], await(current.get(key, Long.MAX_VALUE, ReadProjection.FULL)).orElseThrow().value().orElseThrow());
      var page = await(history.scan(new ScanRequest(binding.codec().bucket(key), KeyRange.point(key), Optional.empty(),
          3, ReadProjection.METADATA, 8, 64_000, 64)));
      assertEquals(3, page.records().getFirst().metadata().modificationRevision());
      var effective = await(admin.getDescriptor(binding.table())).getColumnFamily(binding.family());
      System.out.printf("j11-layout layout=%s comparisonCase=%s tableMinVersions=%d tableMaxVersions=%d"
          + " keepDeletedCells=%s newVersionBehavior=%s minorObserved=true majorFileObserved=true"
          + " applicationPutCalls=%d returnedCellRows=%d returnedCellBytes=%d clientScanRpcCalls=%d%n",
          layout, retention, effective.getMinVersions(), effective.getMaxVersions(),
          effective.getKeepDeletedCells(), effective.isNewVersionBehavior(), applicationPutCalls, page.cost().physicalRows(),
          page.cost().returnedCellBytes(), page.cost().scanRpcCalls());
    }
  }

  private static int writeAndFlush(HBaseRecordStore history, HBaseRecordStore current,
      StoredRecord record, List<StorageTableBinding> bindings) throws Exception {
    int invocations = 0;
    await(history.put(record));
    invocations++;
    if (current != history) {
      await(current.put(record));
      invocations++;
    }
    for (var binding : bindings) await(connection.getAdmin().flush(binding.table()));
    return invocations;
  }

  private static boolean filesAtLeast(List<StorageTableBinding> bindings, int minimum) throws Exception {
    for (var binding : bindings) if (fileCount(binding.table()) < minimum) return false;
    return true;
  }

  private static void compactTables(List<StorageTableBinding> bindings, ByteKey key, boolean major) throws Exception {
    var admin = connection.getAdmin();
    for (var binding : bindings) {
      var region = await(admin.getRegions(binding.table())).stream()
          .filter(candidate -> candidate.containsRow(binding.codec().encode(key, 1))).findFirst().orElseThrow();
      long previous = await(admin.getLastMajorCompactionTimestampForRegion(region.getRegionName())).orElse(0L);
      if (major) await(admin.majorCompact(binding.table()));
      else await(admin.compact(binding.table()));
      Waiter.waitFor(cluster.clientConfiguration(), 30_000, 100, () -> fileCount(binding.table()) == 1
          && await(admin.getCompactionState(binding.table())) == CompactionState.NONE
          && (!major || await(admin.getLastMajorCompactionTimestampForRegion(region.getRegionName())).orElse(0L) > previous));
    }
  }

  private static void verifyHistoryPair(HBaseRecordStore store, ByteKey key) throws Exception {
    assertArrayEquals(new byte[] {1}, await(store.get(key, 1, ReadProjection.FULL)).orElseThrow().value().orElseThrow());
    assertEquals(StorageMetadata.State.TOMBSTONE, await(store.get(key, 2, ReadProjection.FULL)).orElseThrow().metadata().state());
  }

  private static int fileCount(TableName table) throws Exception {
    int count = 0;
    for (var server : await(connection.getAdmin().getRegionServers())) {
      for (var region : await(connection.getAdmin().getRegionMetrics(server, table))) count += region.getStoreFileCount();
    }
    return count;
  }

  @Test
  void wallClockTtlAndSmallVersionRetentionCannotStandInForLogicalCompaction() throws Exception {
    var binding = table(StorageTableBinding.Layout.NATIVE, new Retention(0, 2, KeepDeletedCells.FALSE, false));
    ByteKey key = ByteKey.copyOf(new byte[] {8});
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(60))) {
      var store = await(HBaseRecordStore.open(provider, identity, binding, scope, worker));
      for (long revision = 1; revision <= 4; revision++) await(store.put(live(key, revision, new byte[] {(byte) revision})));
      await(connection.getAdmin().flush(binding.table()));
      assertTrue(await(store.get(key, 1, ReadProjection.FULL)).isEmpty());
      assertArrayEquals(new byte[] {4}, await(store.get(key, 4, ReadProjection.FULL)).orElseThrow().value().orElseThrow());
    }
    var ttlBinding = table(StorageTableBinding.Layout.NATIVE);
    var family = ColumnFamilyDescriptorBuilder.newBuilder(ttlBinding.descriptor().getColumnFamily(ttlBinding.family()))
        .setTimeToLive(60).build();
    await(connection.getAdmin().modifyColumnFamily(ttlBinding.table(), family));
    var rawTable = connection.getTable(ttlBinding.table());
    byte[] row = ttlBinding.codec().encode(key, 1);
    await(rawTable.put(new Put(row).addColumn(ttlBinding.family(), HBaseRecordStore.VALUE, 0, new byte[] {1})));
    assertTrue(await(rawTable.get(new Get(row))).isEmpty());
    try (var scope = RequestScope.withoutDeadline()) {
      assertThrows(java.util.concurrent.ExecutionException.class,
          () -> await(HBaseRecordStore.open(provider, identity, ttlBinding, scope, worker)));
    }
    System.out.println("j11-retention wallClockTtlRejected=true maxVersionsNotLogicalCompact=true");
  }

  private record Retention(int minimum, int maximum, KeepDeletedCells keep, boolean newBehavior) {}

  private static StoredRecord live(ByteKey key, long revision, byte[] value) {
    return StoredRecord.live(key, new StorageMetadata(1, revision, revision, 0, StorageMetadata.State.LIVE, value.length), value);
  }

  private static StoredRecord recreated(ByteKey key, long revision) {
    return StoredRecord.live(key, new StorageMetadata(revision, revision, 1, 0, StorageMetadata.State.LIVE, 0), new byte[0]);
  }

  private static <T> T await(CompletableFuture<T> operation) throws Exception {
    return operation.get(60, TimeUnit.SECONDS);
  }
}
