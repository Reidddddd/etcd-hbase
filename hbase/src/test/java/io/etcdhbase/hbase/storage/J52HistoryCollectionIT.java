package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.CompactionOperation;
import io.etcdhbase.core.commit.CompactionService;
import io.etcdhbase.core.commit.GcCoordinator;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PublicationSlot;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.codec.RowFenceMessages;
import io.etcdhbase.hbase.codec.CommitControlMessages;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Real HBase CAS/Delete evidence; result/manifest/event closure is outside this storage gate. */
class J52HistoryCollectionIT {
  private static final HashSaltStrategy SALT = new HashSaltStrategy(
      new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2));
  private static final SnapshotPinConfiguration PIN_CONFIGURATION =
      new SnapshotPinConfiguration(2, 4, Duration.ofMinutes(1));
  private static final ByteKey AUTHORIZATION = ByteKey.copyOf(new byte[32]);
  private static final CommitOwnerService.OwnerToken OWNER = new CommitOwnerService.OwnerToken("owner", 2);
  private static M1TestEnvironment environment;

  @BeforeAll static void start() throws Exception { environment = M1TestEnvironment.start(1); }
  @AfterAll static void stop() throws Exception { if (environment != null) environment.close(); }

  @Test
  void enumeratesEveryKeyWithBucketLocalExclusiveResumePoints() throws Exception {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var group = group(StorageTableBinding.Layout.HISTORY, scope);
      var keys = List.of(ByteKey.copyOf(new byte[] {1}), ByteKey.copyOf(new byte[] {2}),
          ByteKey.copyOf(new byte[] {3}));
      for (var key : keys) {
        for (int version = 0; version < 4; version++) group.append(false, scope, key);
      }
      group.compact(4, scope);
      var permit = await(new GcCoordinator(group.control, group.compactions, group.pins,
          Clock.systemUTC()).authorizeHistory(OWNER, scope));
      var enumerator = await(HBaseHistoryKeyEnumerator.open(environment.factory(),
          environment.identity(), group.system, group.primary, group.current, scope));
      var all = new java.util.HashSet<ByteKey>();
      for (int bucket = 0; bucket < SALT.profile().bucketCount(); bucket++) {
        Optional<ByteKey> after = Optional.empty();
        while (true) {
          var next = await(enumerator.nextKey(permit, bucket, after, scope));
          if (next.isEmpty()) break;
          assertTrue(all.add(next.orElseThrow()), "each complete key is visited exactly once");
          assertEquals(bucket, group.primary.codec().bucket(next.orElseThrow()));
          after = next;
        }
      }
      assertEquals(new java.util.HashSet<>(keys), all);
    }
  }

  @Test
  void doesNotEndEnumerationInsideAHighChurnKey() throws Exception {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var group = group(StorageTableBinding.Layout.HISTORY, scope);
      var keys = java.util.stream.IntStream.range(1, 256)
          .mapToObj(value -> ByteKey.copyOf(new byte[] {(byte) value}))
          .filter(key -> group.primary.codec().bucket(key) == 0).limit(2).toList();
      assertEquals(2, keys.size());
      for (int version = 0; version < 40; version++) group.append(false, scope, keys.getFirst());
      group.append(false, scope, keys.getLast());
      group.compact(42, scope);
      var permit = await(new GcCoordinator(group.control, group.compactions, group.pins,
          Clock.systemUTC()).authorizeHistory(OWNER, scope));
      var enumerator = await(HBaseHistoryKeyEnumerator.open(environment.factory(),
          environment.identity(), group.system, group.primary, group.current, scope));
      var first = await(enumerator.nextKey(permit, 0, Optional.empty(), scope));
      assertEquals(Optional.of(keys.getFirst()), first);
      var second = await(enumerator.nextKey(permit, 0, first, scope));
      assertEquals(Optional.of(keys.getLast()), second, "all 40 versions of the prior key must be skipped");
      assertTrue(await(enumerator.nextKey(permit, 0, second, scope)).isEmpty());
    }
  }

  @Test
  void resumesAcrossSaltBucketsWithoutComparingGlobalLogicalOrder() throws Exception {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var group = group(StorageTableBinding.Layout.HISTORY, scope);
      var candidates = java.util.stream.IntStream.range(1, 256)
          .mapToObj(value -> ByteKey.copyOf(new byte[] {(byte) value})).toList();
      var firstKey = candidates.stream().filter(key -> group.primary.codec().bucket(key) == 0)
          .reduce((left, right) -> right).orElseThrow();
      var nextKey = candidates.stream().filter(key -> group.primary.codec().bucket(key) == 3)
          .findFirst().orElseThrow();
      assertTrue(firstKey.compareTo(nextKey) > 0);
      group.append(false, scope, firstKey);
      group.append(false, scope, nextKey);
      group.compact(3, scope);
      var permit = await(new GcCoordinator(group.control, group.compactions, group.pins,
          Clock.systemUTC()).authorizeHistory(OWNER, scope));
      var enumerator = await(HBaseHistoryKeyEnumerator.open(environment.factory(),
          environment.identity(), group.system, group.primary, group.current, scope));
      var first = await(enumerator.nextKey(permit, 0, Optional.empty(), scope));
      assertEquals(Optional.of(firstKey), first);
      assertTrue(await(enumerator.nextKey(permit, 0, first, scope)).isEmpty());
      assertEquals(Optional.of(nextKey), await(enumerator.nextKey(permit, 3, Optional.empty(), scope)),
          "the next bucket begins with its own exclusive cursor");
      assertThrows(IllegalArgumentException.class, () -> enumerator.nextKey(permit, 3, first, scope));
    }
  }

  @Test
  void boundsKeyEnumerationToOneDurableBucketInBothLayouts() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
        var group = group(layout, scope);
        var candidates = java.util.stream.IntStream.range(1, 256)
            .mapToObj(value -> ByteKey.copyOf(new byte[] {(byte) value})).toList();
        var inBucket = candidates.stream().filter(key -> group.primary.codec().bucket(key) == 0)
            .limit(2).toList();
        var other = candidates.stream().filter(key -> group.primary.codec().bucket(key) == 1)
            .findFirst().orElseThrow();
        for (var key : inBucket) group.append(false, scope, key);
        group.append(false, scope, other);
        group.compact(4, scope);
        var permit = await(new GcCoordinator(group.control, group.compactions, group.pins,
            Clock.systemUTC()).authorizeHistory(OWNER, scope));
        var enumerator = await(HBaseHistoryKeyEnumerator.open(environment.factory(),
            environment.identity(), group.system, group.primary, group.current, scope));
        var first = await(enumerator.nextKey(permit, 0, Optional.empty(), scope));
        assertEquals(Optional.of(inBucket.getFirst()), first);
        var second = await(enumerator.nextKey(permit, 0, first, scope));
        assertEquals(Optional.of(inBucket.getLast()), second);
        assertTrue(await(enumerator.nextKey(permit, 0, second, scope)).isEmpty());
        assertEquals(Optional.of(other), await(enumerator.nextKey(permit, 1, Optional.empty(), scope)));
        assertThrows(IllegalArgumentException.class,
            () -> enumerator.nextKey(permit, 0, Optional.of(other), scope));
      }
    }
  }

  @Test
  void preservesPinnedBaselineTombstoneNewerWritesAndRetiredGuardsInBothLayouts() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (var scope = RequestScope.withTimeout(Duration.ofMinutes(3));
          var readerScope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
        var group = group(layout, scope);
        var first = group.append(false, scope);
        group.append(false, scope);
        group.append(true, scope);
        group.append(false, scope);
        var views = new ReadViewService(group.control,
            new ManifestCatalog(new HBaseManifestRepository(group.records)), group.pins, group.system.profileFingerprint());
        var oldRead = await(views.open(OptionalLong.of(2), AUTHORIZATION, readerScope));
        group.compact(4, scope);
        var collector = await(HBaseHistoryCollector.open(environment.factory(), environment.identity(),
            group.system, group.primary, group.current, PIN_CONFIGURATION, scope));
        var coordinator = new GcCoordinator(group.control, group.compactions, group.pins, Clock.systemUTC());
        var protectedPermit = await(coordinator.authorizeHistory(OWNER, scope));
        assertEquals(2, protectedPermit.safeRevision());
        assertEquals(0, await(collector.collectKey(protectedPermit, group.key, 0, 1, scope)).deletedVersions());
        assertArrayEquals(new byte[] {2}, await(environment.store(group.primary, scope)
            .get(group.key, 2, ReadProjection.FULL)).orElseThrow().value().orElseThrow());
        oldRead.close();
        await(oldRead.termination().toCompletableFuture());
        var permit = await(coordinator.authorizeHistory(OWNER, scope));
        assertEquals(4, permit.safeRevision());
        int deleted = 0;
        long after = 0;
        HBaseHistoryCollector.Page page;
        do {
          page = await(collector.collectKey(permit, group.key, after, 1, scope));
          deleted += page.deletedVersions();
          after = page.lastRevision();
        } while (!page.exhausted());
        assertEquals(2, deleted);
        var reader = environment.store(group.primary, scope);
        assertTrue(await(reader.get(group.key, 3, ReadProjection.FULL)).isEmpty());
        assertEquals(StorageMetadata.State.TOMBSTONE,
            await(reader.get(group.key, 4, ReadProjection.FULL)).orElseThrow().metadata().state());
        assertArrayEquals(new byte[] {5}, await(reader.get(group.key, 5, ReadProjection.FULL))
            .orElseThrow().value().orElseThrow());
        group.append(false, scope);
        if (layout == StorageTableBinding.Layout.HISTORY) {
          assertThrows(Exception.class, () -> await(group.materializer.applySelected(decision(first), first, scope)));
          Result retired = await(environment.connection().getTable(group.primary.table()).get(
              new Get(group.primary.codec().encode(group.key, 2)).addFamily(group.primary.family())));
          assertNull(retired.getValue(group.primary.family(), new byte[] {'m'}));
          assertNull(retired.getValue(group.primary.family(), new byte[] {'v'}));
          assertEquals(RowFenceMessages.State.RETIRED, RowFenceMessages.decode(
              retired.getValue(group.primary.family(), new byte[] {'g'})).state());
        } else {
          assertEquals(io.etcdhbase.core.commit.MaterializationStore.ApplyResult.CURRENT_SUPERSEDED,
              await(group.materializer.applySelected(decision(first), first, scope)));
          Result exact = await(environment.connection().getTable(group.primary.table()).get(
              new Get(group.primary.codec().encode(group.key, 1)).setTimeRange(1, 3)
                  .readVersions(64).addFamily(group.primary.family())));
          assertTrue(exact.isEmpty(), "m/v/f must all be deleted at the old timestamps");
        }
        assertArrayEquals(new byte[] {6}, await(reader.get(group.key, 6, ReadProjection.FULL))
            .orElseThrow().value().orElseThrow());
        assertEquals(0, await(collector.collectKey(permit, group.key, 0, 64, scope)).deletedVersions());
        System.out.printf("j52-history layout=%s pinned=true tombstone=true exactDelete=true lateWriterFenced=true%n", layout);
      }
    }
  }

  @Test
  void resolvesLostDeleteAcknowledgementAndReplansWhenANewerNativeWriteWins() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
        var group = group(layout, scope);
        group.append(false, scope);
        group.append(false, scope);
        group.append(false, scope);
        group.compact(3, scope);
        var permit = await(new GcCoordinator(group.control, group.compactions, group.pins,
            Clock.systemUTC()).authorizeHistory(OWNER, scope));
        var faults = new Faults(environment.connection());
        faults.loseAcknowledgement.set(true);
        var collector = await(HBaseHistoryCollector.open(faults, environment.identity(),
            group.system, group.primary, group.current, PIN_CONFIGURATION, scope));
        assertEquals(1, await(collector.collectKey(permit, group.key, 0, 64, scope)).deletedVersions());
        assertFalse(faults.loseAcknowledgement.get());
        group.compact(4, scope);
        permit = await(new GcCoordinator(group.control, group.compactions, group.pins,
            Clock.systemUTC()).authorizeHistory(OWNER, scope));
        faults.gateNext.set(true);
        var collection = collector.collectKey(permit, group.key, 0, 64, scope);
        await(faults.mutationReached);
        group.append(false, scope);
        faults.resume.complete(null);
        assertEquals(1, await(collection).deletedVersions());
        assertArrayEquals(new byte[] {5}, await(environment.store(group.primary, scope)
            .get(group.key, 5, ReadProjection.FULL)).orElseThrow().value().orElseThrow());
      }
    }
  }

  @Test
  void cancellationRetainsPhysicalProducerUntilLateDeleteDrains() throws Exception {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(3));
        var collectionScope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var group = group(StorageTableBinding.Layout.NATIVE, scope);
      group.append(false, scope);
      group.append(false, scope);
      group.compact(3, scope);
      var permit = await(new GcCoordinator(group.control, group.compactions, group.pins,
          Clock.systemUTC()).authorizeHistory(OWNER, collectionScope));
      var faults = new Faults(environment.connection());
      faults.gateNext.set(true);
      var collector = await(HBaseHistoryCollector.open(faults, environment.identity(),
          group.system, group.primary, group.current, PIN_CONFIGURATION, scope));
      var result = collector.collectKey(permit, group.key, 0, 64, collectionScope);
      await(faults.mutationReached);
      collectionScope.cancel();
      assertThrows(Exception.class, () -> await(result));
      assertFalse(collectionScope.physicalProducersCompletion().toCompletableFuture().isDone());
      faults.resume.complete(null);
      await(collectionScope.physicalProducersCompletion().toCompletableFuture());
      var reopened = await(HBaseHistoryCollector.open(environment.factory(), environment.identity(),
          group.system, group.primary, group.current, PIN_CONFIGURATION, scope));
      var resumedPermit = await(new GcCoordinator(group.control, group.compactions, group.pins,
          Clock.systemUTC()).authorizeHistory(OWNER, scope));
      assertEquals(0, await(reopened.collectKey(resumedPermit, group.key, 0, 64, scope)).deletedVersions());
    }
  }

  @Test
  void rejectsPermissionFromAnotherPinRegistryBeforeDeletingData() throws Exception {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var group = group(StorageTableBinding.Layout.NATIVE, scope);
      var unrelated = group(StorageTableBinding.Layout.NATIVE, scope);
      group.append(false, scope);
      group.append(false, scope);
      group.compact(3, scope);
      var wrongRegistry = await(new GcCoordinator(group.control, group.compactions, unrelated.pins,
          Clock.systemUTC()).authorizeHistory(OWNER, scope));
      var collector = await(HBaseHistoryCollector.open(environment.factory(), environment.identity(),
          group.system, group.primary, group.current, PIN_CONFIGURATION, scope));
      assertThrows(IllegalStateException.class,
          () -> collector.collectKey(wrongRegistry, group.key, 0, 64, scope));
      assertArrayEquals(new byte[] {2}, await(environment.store(group.primary, scope)
          .get(group.key, 2, ReadProjection.FULL)).orElseThrow().value().orElseThrow());
    }
  }

  @Test
  void rejectsCompactControlWithoutDescriptorCapabilityAndDuringInitialization() throws Exception {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var group = group(StorageTableBinding.Layout.HISTORY, false, scope);
      var invalidInitial = CommitState.restore(CommitState.FORMAT_VERSION,
          group.system.profileFingerprint(), Optional.empty(), 0, 0, 1, 0, 1, 0, 1,
          Optional.empty(), 0, Optional.empty(), Optional.empty(), 0, false, 0,
          Optional.of(InvocationId.create()));
      assertInstanceOf(IllegalArgumentException.class, assertThrows(CompletionException.class,
          () -> group.control.initializeIfAbsent(invalidInitial, scope).join()).getCause());
      var observed = await(group.control.read(scope)).orElseThrow();
      var compacted = observed.state().selectCompaction(1, InvocationId.create());
      assertInstanceOf(IllegalArgumentException.class, assertThrows(CompletionException.class,
          () -> group.control.compareAndSet(observed, compacted, scope).join()).getCause());
      // Simulate persisted control/descriptor drift through the real table, bypassing the adapter.
      await(environment.connection().getTable(group.system.table()).put(
          new Put(group.system.controlRow()).setDurability(Durability.SYNC_WAL)
              .addColumn(group.system.family(), new byte[] {'s'}, CommitControlMessages.encode(compacted))));
      var failure = assertThrows(CompletionException.class, () -> group.control.read(scope).join());
      assertInstanceOf(IllegalStateException.class,
          assertInstanceOf(BackendFailure.class, failure.getCause()).getCause());
    }
  }

  private static Group group(StorageTableBinding.Layout layout, RequestScope scope) throws Exception {
    return group(layout, true, scope);
  }

  private static Group group(StorageTableBinding.Layout layout, boolean compaction,
      RequestScope scope) throws Exception {
    String name = "j52_" + UUID.randomUUID().toString().replace("-", "");
    var system = new SystemTableBinding(TableName.valueOf(name + "_system"), new byte[] {'s'}, "m1", "salt", SALT);
    var primary = new StorageTableBinding(TableName.valueOf(name + "_history"), new byte[] {'d'}, layout, SALT);
    Optional<StorageTableBinding> current = layout == StorageTableBinding.Layout.HISTORY
        ? Optional.of(new StorageTableBinding(TableName.valueOf(name + "_current"), new byte[] {'d'},
            StorageTableBinding.Layout.CURRENT, SALT)) : Optional.empty();
    var descriptor = TableDescriptorBuilder.newBuilder(system.table())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(system.family()).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.salt-strategy-id", SALT.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version", "1")
        .setValue("etcdhbase.m0.salt-bucket-count", "4")
        .setValue("etcdhbase.m0.salt-width-bytes", "2")
        .setValue("etcdhbase.m0.record-categories", "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION,SNAPSHOT_PIN,RESULT")
        .build();
    var systemDescriptor = owned(descriptor, layout);
    environment.create(system.table(), compaction ? system.withCompactionCapability(systemDescriptor)
        : systemDescriptor, SALT.splitKeys().toArray(byte[][]::new));
    environment.create(primary.table(), owned(primary.descriptor(), layout), SALT.splitKeys().toArray(byte[][]::new));
    if (current.isPresent()) environment.create(current.orElseThrow().table(),
        owned(current.orElseThrow().descriptor(), layout), SALT.splitKeys().toArray(byte[][]::new));
    var records = await(HBaseSystemRecordStore.open(environment.factory(), environment.identity(), system, scope));
    var control = await(HBaseCommitControlStore.open(environment.factory(), environment.identity(), system, scope));
    assertTrue(await(control.initializeIfAbsent(CommitState.initial(system.profileFingerprint(), 1), scope)));
    var observed = await(control.read(scope)).orElseThrow();
    assertTrue(await(control.compareAndSet(observed,
        observed.state().claimOwner("owner", System.currentTimeMillis(), System.currentTimeMillis() + 180_000), scope)));
    observed = await(control.read(scope)).orElseThrow();
    assertTrue(await(control.compareAndSet(observed,
        observed.state().enableAuthorizationBarrier("owner", 1, System.currentTimeMillis()), scope)));
    var pins = await(SnapshotPinManager.initializeFresh(records,
        PIN_CONFIGURATION, scope));
    var materializer = await(HBaseMaterializationStore.openForTableGroup(environment.factory(),
        environment.identity(), system, primary, current, scope));
    return new Group(system, primary, current, records, control, pins, materializer);
  }

  private static TableDescriptor owned(TableDescriptor descriptor, StorageTableBinding.Layout layout) {
    return TableDescriptorBuilder.newBuilder(descriptor).setValue("etcdhbase.m0.table-group-id", "m1")
        .setValue("etcdhbase.m0.salt-profile-id", "salt")
        .setValue("etcdhbase.m0.backend-cluster-id", "fixture-cluster")
        .setValue("etcdhbase.m0.plan-fingerprint", "a".repeat(64))
        .setValue("etcdhbase.m0.schema-version", "1")
        .setValue("etcdhbase.m0.history-layout", layout == StorageTableBinding.Layout.HISTORY
            ? "SEPARATE_HISTORY_TABLE" : "NATIVE_CELL_VERSIONS").build();
  }

  private static CommitDecision decision(PreparedCandidate candidate) {
    byte[] root = ByteBuffer.allocate(17).put(candidate.transactionId().bytes()).put((byte) 3).array();
    return new CommitDecision(candidate.transactionId(), InvocationId.create(), candidate.baseSequence() + 1,
        candidate.baseRevision(), candidate.record().metadata().modificationRevision(), ByteKey.copyOf(root),
        candidate.contentDigest(), true);
  }

  private static <T> T await(CompletableFuture<T> future) throws Exception { return future.get(60, TimeUnit.SECONDS); }

  private static final class Group {
    final SystemTableBinding system;
    final StorageTableBinding primary;
    final Optional<StorageTableBinding> current;
    final HBaseSystemRecordStore records;
    final HBaseCommitControlStore control;
    final SnapshotPinManager pins;
    final HBaseMaterializationStore materializer;
    final CompactionService compactions;
    final ByteKey key = ByteKey.copyOf(new byte[] {0, (byte) 0xff, 1});
    Group(SystemTableBinding system, StorageTableBinding primary, Optional<StorageTableBinding> current,
        HBaseSystemRecordStore records, HBaseCommitControlStore control, SnapshotPinManager pins,
        HBaseMaterializationStore materializer) {
      this.system = system; this.primary = primary; this.current = current; this.records = records;
      this.control = control; this.pins = pins; this.materializer = materializer;
      compactions = new CompactionService(control, records, Clock.systemUTC());
    }
    PreparedCandidate append(boolean tombstone, RequestScope scope) throws Exception {
      return append(tombstone, scope, key);
    }

    PreparedCandidate append(boolean tombstone, RequestScope scope, ByteKey selectedKey)
        throws Exception {
      var observed = await(control.read(scope)).orElseThrow();
      var state = observed.state();
      long revision = state.publishedRevision() + 1;
      byte[] value = {(byte) revision};
      var metadata = new StorageMetadata(2, revision, revision - 1, 0,
          tombstone ? StorageMetadata.State.TOMBSTONE : StorageMetadata.State.LIVE, tombstone ? 0 : 1);
      var record = tombstone
          ? StoredRecord.metadataOnly(selectedKey, metadata)
          : StoredRecord.live(selectedKey, metadata, value);
      var candidate = new PreparedCandidate(TransactionId.create(), OWNER.epoch(),
          state.publishedSequence(), state.publishedRevision(), record);
      CommitDecision decision = decision(candidate);
      var slot = new PublicationSlot(candidate.transactionId(), decision.invocationId(), OWNER.epoch(),
          state.publishedSequence(), state.publishedRevision(), decision.sequence(), revision, true,
          decision.manifestRoot(), decision.manifestDigest());
      assertTrue(await(control.compareAndSet(observed, state.publish(slot), scope)));
      await(materializer.applySelected(decision, candidate, scope));
      observed = await(control.read(scope)).orElseThrow();
      assertTrue(await(control.compareAndSet(observed, observed.state().finalizePublication(candidate.transactionId()), scope)));
      return candidate;
    }
    void compact(long revision, RequestScope scope) throws Exception {
      var state = await(control.read(scope)).orElseThrow().state();
      var binding = InvocationBinding.begin(InvocationId.create(), AUTHORIZATION, AUTHORIZATION,
          TransactionId.create(), OWNER.epoch(), state.publishedSequence(), state.publishedRevision());
      var proof = CommitAuthorizationProof.capture(AUTHORIZATION, 0,
          (identity, generation, checked) -> CompletableFuture.completedFuture(null));
      assertEquals(CompactionOperation.Stage.LOGICAL, await(compactions.compact(revision, binding, OWNER, proof, scope)).stage());
    }
  }

  /** Keeps actual HBase CAS/Delete underneath controlled acknowledgement and dispatch races. */
  private static final class Faults implements HBaseConnectionFactory {
    final AtomicBoolean loseAcknowledgement = new AtomicBoolean();
    final AtomicBoolean gateNext = new AtomicBoolean();
    final CompletableFuture<Void> mutationReached = new CompletableFuture<>();
    final CompletableFuture<Void> resume = new CompletableFuture<>();
    final AsyncConnection proxy;
    Faults(AsyncConnection connection) {
      proxy = (AsyncConnection) Proxy.newProxyInstance(AsyncConnection.class.getClassLoader(),
          new Class<?>[] {AsyncConnection.class}, (self, method, args) -> {
            Object result = invoke(connection, method, args);
            return method.getName().equals("getTableBuilder") ? builder(result) : result;
          });
    }
    private Object builder(Object target) {
      return Proxy.newProxyInstance(AsyncTableBuilder.class.getClassLoader(), new Class<?>[] {AsyncTableBuilder.class},
          (self, method, args) -> {
            Object result = invoke(target, method, args);
            if (result == target) return self;
            return method.getName().equals("build") ? table(result) : result;
          });
    }
    private Object table(Object target) {
      return Proxy.newProxyInstance(AsyncTable.class.getClassLoader(), new Class<?>[] {AsyncTable.class},
          (self, method, args) -> {
            if (!method.getName().equals("checkAndMutate")) return invoke(target, method, args);
            if (gateNext.compareAndSet(true, false)) {
              mutationReached.complete(null);
              return resume.thenCompose(ignored -> future(invoke(target, method, args)));
            }
            CompletableFuture<Object> actual = future(invoke(target, method, args));
            return actual.thenCompose(value -> loseAcknowledgement.compareAndSet(true, false)
                ? CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN))
                : CompletableFuture.completedFuture(value));
          });
    }
    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args) {
      try { return method.invoke(target, args); }
      catch (InvocationTargetException failure) { throw new CompletionException(failure.getCause()); }
      catch (ReflectiveOperationException failure) { throw new CompletionException(failure); }
    }
    @SuppressWarnings("unchecked")
    private static CompletableFuture<Object> future(Object value) { return (CompletableFuture<Object>) value; }
    @Override public CompletableFuture<AsyncConnection> connectionFor(AuthorizedBackendIdentity identity) {
      return CompletableFuture.completedFuture(proxy);
    }
    @Override public void close() { }
  }
}
