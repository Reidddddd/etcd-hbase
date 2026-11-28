package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.GcCheckpoint;
import io.etcdhbase.core.commit.GcCheckpointStore;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.RetirementTombstone;
import io.etcdhbase.core.commit.SystemRecordClosure;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.codec.ManifestMessages;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/**
 * MiniCluster coverage for the retirement contract in §6.3 of the M5 design: a retired system
 * record must never be resurrected by a late create or an old absent-CAS.
 */
class HBaseSystemRecordRetirementIT {
  private static final byte[] FAMILY = {'s'};
  private static final String RECORD_CATEGORIES = String.join(",",
      "CONTROL", "INVOCATION", "CANDIDATE", "MANIFEST", "DECISION", "SNAPSHOT_PIN",
      "RESULT", "TABLE_GROUP_IDENTITY", "WORKSET", "LEASE_STATE", "LEASE_ENUMERATION",
      "EXPIRY_CANDIDATE", "AUXILIARY", "LEASE_RESULT", "LEASE_BINDING",
      "LEASE_EXPIRY_CHECKPOINT");
  private static final ByteKey AUTHORIZATION_DIGEST = ByteKey.copyOf(new byte[32]);

  @Test
  void retirementAcknowledgementRequiresStrongTombstoneReadbackBeforeSuccess() throws Exception {
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var actual = open(environment, scope);
      var store = org.mockito.Mockito.spy(actual);
      var identity = ByteKey.copyOf(new byte[] {97});
      var value = ByteKey.copyOf(new byte[] {98});
      actual.createIfAbsent(SystemRecordType.WORKSET, identity, value, scope).join();
      var permit = permit(actual, scope);
      var failure = new BackendFailure(BackendFailure.Category.UNAVAILABLE);
      org.mockito.Mockito.doReturn(CompletableFuture.failedFuture(failure)).when(store)
          .readTombstone(SystemRecordType.WORKSET, identity, scope);
      var observed = assertThrows(CompletionException.class, () -> store.retire(SystemRecordType.WORKSET,
          identity, value, permit, SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, scope).join());
      assertEquals(BackendFailure.Category.UNAVAILABLE,
          assertInstanceOf(BackendFailure.class, CompletionFailures.unwrapCompletion(observed)).category());
      assertTrue(actual.read(SystemRecordType.WORKSET, identity, scope).join().isEmpty(),
          "the actual retirement committed before the failed proof read");
      assertTrue(actual.readTombstone(SystemRecordType.WORKSET, identity, scope).join().isPresent());
      org.mockito.Mockito.doCallRealMethod().when(store).readTombstone(SystemRecordType.WORKSET, identity, scope);
      assertTrue(store.retire(SystemRecordType.WORKSET, identity, value, permit,
          SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, scope).join().alreadyRetired());
    }
  }

  @Test
  void descriptorChangesAfterOpenRejectFenceAndRetirementWithoutMutations() throws Exception {
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var table = createRetirementCapableSystemTable(environment);
      var store = HBaseSystemRecordStore.open(environment.factory(), environment.identity(), table, scope).join();
      var permit = permit(store, scope);
      var fenceTarget = ByteKey.copyOf(new byte[] {94});
      var retireTarget = ByteKey.copyOf(new byte[] {95});
      var value = ByteKey.copyOf(new byte[] {96});
      store.createIfAbsent(SystemRecordType.WORKSET, fenceTarget, value, scope).join();
      store.createIfAbsent(SystemRecordType.WORKSET, retireTarget, value, scope).join();
      var admin = environment.connection().getAdmin();
      var valid = admin.getDescriptor(table.table()).join();
      var invalid = java.util.List.of(
          TableDescriptorBuilder.newBuilder(valid).removeValue("etcdhbase.m5.gc-fence-format").build(),
          TableDescriptorBuilder.newBuilder(valid).setValue("etcdhbase.m5.gc-fence-format", "2").build(),
          TableDescriptorBuilder.newBuilder(valid).setValue("etcdhbase.m5.gc-fence-format", "broken").build(),
          TableDescriptorBuilder.newBuilder(valid).removeColumnFamily(SystemTableBinding.GC_FENCE_FAMILY).build(),
          TableDescriptorBuilder.newBuilder(valid).modifyColumnFamily(ColumnFamilyDescriptorBuilder
              .newBuilder(SystemTableBinding.GC_FENCE_FAMILY).setMaxVersions(2).build()).build(),
          TableDescriptorBuilder.newBuilder(valid).modifyColumnFamily(ColumnFamilyDescriptorBuilder
              .newBuilder(SystemTableBinding.GC_FENCE_FAMILY).setTimeToLive(60).build()).build(),
          TableDescriptorBuilder.newBuilder(valid).removeValue("etcdhbase.m5.compaction-format").build());
      var backend = environment.connection().getTable(table.table());
      for (var descriptor : invalid) {
        admin.modifyTable(descriptor).join();
        assertAll(
            () -> assertThrows(CompletionException.class, () -> store.fenceForGc(
                SystemRecordType.WORKSET, fenceTarget, permit, scope).join()),
            () -> assertThrows(CompletionException.class, () -> store.retire(SystemRecordType.WORKSET,
                retireTarget, value, permit, SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, scope).join()),
            () -> assertEquals(1, backend.get(new org.apache.hadoop.hbase.client.Get(
                table.row(SystemRecordType.WORKSET, fenceTarget))).join().size(), "no maintenance write"),
            () -> assertEquals(Optional.of(value), store.read(SystemRecordType.WORKSET, retireTarget, scope).join()),
            () -> assertTrue(store.readTombstone(SystemRecordType.WORKSET, retireTarget, scope).join().isEmpty()));
        admin.modifyTable(valid).join();
      }
      store.fenceForGc(SystemRecordType.WORKSET, fenceTarget, permit, scope).join();
      assertTrue(store.retire(SystemRecordType.WORKSET, retireTarget, value, permit,
          SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, scope).join().retiredNow());
    }
  }

  @Test
  void legacyCompactTableRejectsGcWithoutAutomaticSchemaMutation() throws Exception {
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var table = createRetirementCapableSystemTable(environment, false);
      var store = HBaseSystemRecordStore.open(environment.factory(), environment.identity(), table, scope).join();
      assertTrue(store.supportsCompaction());
      assertThrows(IllegalStateException.class, store::requireGcFencing);
      assertThrows(IllegalStateException.class, () -> new HBaseSystemRecordClosure(store));
      assertThrows(IllegalStateException.class, () -> store.fenceForGc(
          SystemRecordType.WORKSET, ByteKey.copyOf(new byte[] {90}), null, scope));
      var descriptor = environment.connection().getAdmin().getDescriptor(table.table()).join();
      assertEquals(1, descriptor.getColumnFamilyCount());
      assertNull(descriptor.getValue("etcdhbase.m5.gc-fence-format"));
      var identity = ByteKey.copyOf(new byte[] {89});
      var value = ByteKey.copyOf(new byte[] {88});
      store.createIfAbsent(SystemRecordType.WORKSET, identity, value, scope).join();
      assertEquals(Optional.of(value), store.read(SystemRecordType.WORKSET, identity, scope).join());
    }
  }

  @Test
  void fencePreservesLegacyPayloadGetScanAndConditionalWriteLayouts() throws Exception {
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var table = createRetirementCapableSystemTable(environment);
      var store = HBaseSystemRecordStore.open(environment.factory(), environment.identity(), table, scope).join();
      var identity = ByteKey.copyOf(new byte[] {91});
      var value = ByteKey.copyOf(new byte[] {92});
      var replacement = ByteKey.copyOf(new byte[] {93});
      store.createIfAbsent(SystemRecordType.WORKSET, identity, value, scope).join();
      var permit = permit(store, scope);
      store.fenceForGc(SystemRecordType.WORKSET, identity, permit, scope).join();
      var backend = environment.connection().getTable(table.table());
      byte[] row = table.row(SystemRecordType.WORKSET, identity);
      var live = backend.get(new org.apache.hadoop.hbase.client.Get(row).addFamily(FAMILY)).join();
      assertEquals(1, live.size(), "the base reader's family projection must still contain only r");
      assertEquals(Optional.of(value), HBaseSystemRecordStore.decode(live, FAMILY));
      var scan = backend.scanAll(new org.apache.hadoop.hbase.client.Scan().withStartRow(row, true)
          .withStopRow(row, true).addFamily(FAMILY)).join();
      assertEquals(1, scan.size());
      assertEquals(1, scan.getFirst().size(), "legacy scan must not expose maintenance cells");
      assertTrue(store.compareAndSet(SystemRecordType.WORKSET, identity, Optional.of(value), replacement, scope).join());
      assertTrue(store.retire(SystemRecordType.WORKSET, identity, replacement, permit,
          SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, scope).join().retiredNow());
      var retired = backend.get(new org.apache.hadoop.hbase.client.Get(row).addFamily(FAMILY)).join();
      assertEquals(2, retired.size(), "the base retired layout remains exactly r/t");
      assertTrue(HBaseSystemRecordStore.decode(retired, FAMILY).isEmpty());
      backend.put(new Put(row).setDurability(Durability.SYNC_WAL).addColumn(
          SystemTableBinding.GC_FENCE_FAMILY, SystemRecordGcFence.COLUMN, SystemRecordGcFence.TIMESTAMP,
          new SystemRecordGcFence(permit.authority().owner().epoch(), permit.generation() + 1).encode())).join();
      assertThrows(CompletionException.class, () -> store.fenceForGc(
          SystemRecordType.WORKSET, identity, permit, scope).join(), "corrupt retired metadata cannot be confirmed");
      assertTrue(store.read(SystemRecordType.WORKSET, identity, scope).join().isEmpty(),
          "maintenance corruption cannot change the payload reader's retired result");
    }
  }

  @Test
  void lostFenceAcknowledgementAndPartialRestartNeverActivateAnIncompleteShard() throws Exception {
    try (var environment = M1TestEnvironment.start(1);
        var rootScope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var table = createRetirementCapableSystemTable(environment);
      var store = HBaseSystemRecordStore.open(environment.factory(), environment.identity(), table, rootScope).join();
      var authority = SystemRecordGcTestFixture.prepare(store, rootScope);
      var targets = manifestsInOneBucket(store, rootScope);
      int bucket = store.bucket(SystemRecordType.MANIFEST, targets.getFirst());
      var checkpoints = new GcCheckpointStore(store);
      var faults = new Faults(environment.connection());
      var delayed = HBaseSystemRecordStore.open(faults, environment.identity(), table, rootScope).join();
      rejectUnknownFenceAcknowledgement(authority, table, checkpoints, delayed, faults, bucket);
      var first = persistFirstFencePage(authority, table, checkpoints, store, bucket);
      try (var resumed = RequestScope.withTimeout(Duration.ofMinutes(1))) {
        var permit = authority.authorize(resumed);
        var proposal = GcCheckpoint.start(table.tableGroupId(),
            SystemRecordClosure.Domain.MANIFEST, authority.owner().epoch(),
            bucket, table.saltProfile().bucketCount(), permit);
        var held = (GcCheckpointStore.Held) checkpoints.claim(proposal, resumed).join();
        assertEquals(first, held.checkpoint().fencePosition().afterIdentity().orElseThrow());
        assertEquals(GcCheckpoint.Phase.FENCING, held.checkpoint().phase());
        fenceShard(store, table, checkpoints, permit, bucket, resumed);
        for (var target : targets) assertTrue(store.read(SystemRecordType.MANIFEST, target, resumed).join().isPresent());
        assertEquals(3, new HBaseSystemRecordClosure(store).collect(
            SystemRecordClosure.Domain.MANIFEST, bucket, Optional.empty(), 16, permit, resumed).join().retired());
      }
    }
  }

  @Test
  void lateRetirementIsFencedBeforeSuccessorStartsCollection() throws Exception {
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3));
        var successorScope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var table = createRetirementCapableSystemTable(environment);
      var store = HBaseSystemRecordStore.open(environment.factory(), environment.identity(), table, scope).join();
      var authority = SystemRecordGcTestFixture.prepare(store, scope);
      var permit = authority.authorize(scope);
      var root = ManifestRoot.create(
          TransactionId.create(), 1, 0, 1, 0, 0, Optional.empty());
      var identity = root.systemIdentity();
      var encoded = ByteKey.copyOf(ManifestMessages.encode(root));
      assertEquals(SystemRecordStore.CreateResult.CREATED,
          store.createIfAbsent(SystemRecordType.MANIFEST, identity, encoded, scope).join());
      var domain = SystemRecordClosure.Domain.MANIFEST;
      int bucket = store.bucket(SystemRecordType.MANIFEST, identity);
      var checkpoints = new GcCheckpointStore(store);
      var held = fenceShard(store, table, checkpoints, permit, bucket, scope);
      var faults = new Faults(environment.connection());
      var delayed = HBaseSystemRecordStore.open(faults, environment.identity(), table, scope).join();
      faults.gateNext.set(true);
      var collection = new HBaseSystemRecordClosure(delayed).collect(domain, bucket, Optional.empty(), 16, permit, scope);
      try {
        faults.mutationReached.get(20, TimeUnit.SECONDS);
        var nextPermit = takeOverAndFence(authority, table, checkpoints, store, bucket, successorScope);
        assertEquals(Optional.of(encoded), store.read(SystemRecordType.MANIFEST, identity, successorScope).join());
        assertThrows(CompletionException.class, () -> authority.eventPins().register(
            new EventHistoryPinManager.Requirement(2, 1,
                SystemRecordGcTestFixture.DIGEST, Optional.empty(), Optional.empty()), successorScope).join());
        assertThrows(CompletionException.class,
            () -> store.fenceForGc(SystemRecordType.MANIFEST, identity, permit, scope).join(),
            "an older fence pass cannot overwrite the successor's row fence");
        faults.resume.complete(null);
        var page = collection.get(20, TimeUnit.SECONDS);
        assertFalse(checkpoints.advance(held, page, scope).join(), "stale checkpoint CAS is fenced");
        assertEquals(0, page.retired(), "the completed same-row fence rejects the old retirement");
        assertEquals(Optional.of(encoded), store.read(SystemRecordType.MANIFEST, identity, successorScope).join());
        var collected = new HBaseSystemRecordClosure(store).collect(domain, bucket, Optional.empty(), 16,
            nextPermit, successorScope).join();
        assertEquals(1, collected.retired(), "the successor can still collect through its actual authority");
      } finally {
        faults.resume.complete(null);
      }
    }
  }

  @Test
  void retirementReplacesTheValueAndBlocksEveryRecreationPath() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      HBaseSystemRecordStore store = open(environment, scope);
      ByteKey identity = ByteKey.copyOf(new byte[] {1, 2, 3});
      ByteKey value = ByteKey.copyOf(new byte[] {9, 8, 7});
      assertEquals(SystemRecordStore.CreateResult.CREATED,
          store.createIfAbsent(SystemRecordType.WORKSET, identity, value, scope).join());

      var permit = permit(store, scope);
      var outcome = store.retire(SystemRecordType.WORKSET, identity, value, permit,
          SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, scope).join();
      assertTrue(outcome.retiredNow());
      assertFalse(outcome.alreadyRetired());

      assertTrue(store.read(SystemRecordType.WORKSET, identity, scope).join().isEmpty(),
          "a retired record no longer serves a value");
      RetirementTombstone tombstone =
          store.readTombstone(SystemRecordType.WORKSET, identity, scope).join().orElseThrow();
      assertEquals(SystemRecordType.WORKSET, tombstone.type());
      assertEquals(identity, tombstone.identity());
      assertEquals(permit.generation(), tombstone.maintenanceGeneration());
      assertEquals(SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, tombstone.reason());

      // The core hazard the design warns about: absence must not be permission to recreate.
      assertThrows(CompletionException.class,
          () -> store.createIfAbsent(SystemRecordType.WORKSET, identity, value, scope).join(),
          "a retired identity must not be recreated by createIfAbsent");
      assertFalse(store.compareAndSet(SystemRecordType.WORKSET, identity, Optional.empty(), value,
              scope).join(),
          "a retired identity must not be recreated by an absent-CAS");
      assertTrue(store.readTombstone(SystemRecordType.WORKSET, identity, scope)
          .join().isPresent(), "the tombstone survives both rejected recreations");
    }
  }

  @Test
  void retirementIsIdempotentForTheSamePlanAndRejectsAChangedValue() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      HBaseSystemRecordStore store = open(environment, scope);
      ByteKey identity = ByteKey.copyOf(new byte[] {4, 5});
      ByteKey value = ByteKey.copyOf(new byte[] {6});
      store.createIfAbsent(SystemRecordType.WORKSET, identity, value, scope).join();
      var permit = permit(store, scope);

      assertTrue(store.retire(SystemRecordType.WORKSET, identity, value, permit,
          SystemRecordRetirement.Reason.COMPACTED_HISTORY, scope).join().retiredNow());
      var replay = store.retire(SystemRecordType.WORKSET, identity, value, permit,
          SystemRecordRetirement.Reason.COMPACTED_HISTORY, scope).join();
      assertFalse(replay.retiredNow());
      assertTrue(replay.alreadyRetired());

      // A different value was never observed under this identity, so the plan is stale.
      assertThrows(CompletionException.class,
          () -> store.retire(SystemRecordType.WORKSET, identity, ByteKey.copyOf(new byte[] {7}),
              permit, SystemRecordRetirement.Reason.COMPACTED_HISTORY, scope).join());
    }
  }

  @Test
  void repeatedCreateCannotOverwriteAnExistingImmutableRecord() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
         RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      HBaseSystemRecordStore store = open(environment, scope);
      ByteKey identity = ByteKey.copyOf(new byte[] {31});
      ByteKey value = ByteKey.copyOf(new byte[] {1});
      ByteKey replacement = ByteKey.copyOf(new byte[] {2});
      store.createIfAbsent(SystemRecordType.WORKSET, identity, value, scope).join();
      var repeated = store.createIfAbsent(SystemRecordType.WORKSET, identity, value, scope).join();
      var conflicting = store.createIfAbsent(SystemRecordType.WORKSET, identity, replacement, scope).join();
      var observed = store.read(SystemRecordType.WORKSET, identity, scope).join();
      assertAll(() -> assertEquals(SystemRecordStore.CreateResult.ALREADY_PRESENT, repeated),
          () -> assertEquals(SystemRecordStore.CreateResult.CONFLICT, conflicting),
          () -> assertEquals(Optional.of(value), observed));
    }
  }

  @Test
  void absentCasCannotReplaceALiveValue() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
         RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      HBaseSystemRecordStore store = open(environment, scope);
      ByteKey identity = ByteKey.copyOf(new byte[] {32});
      ByteKey value = ByteKey.copyOf(new byte[] {1});
      ByteKey replacement = ByteKey.copyOf(new byte[] {2});
      store.createIfAbsent(SystemRecordType.WORKSET, identity, value, scope).join();
      assertFalse(store.compareAndSet(SystemRecordType.WORKSET, identity, Optional.empty(),
          replacement, scope).join());
      assertEquals(Optional.of(value), store.read(SystemRecordType.WORKSET, identity, scope).join());
    }
  }

  @Test
  void staleRetirementCannotDeleteAReplacementValue() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
         RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      HBaseSystemRecordStore store = open(environment, scope);
      ByteKey identity = ByteKey.copyOf(new byte[] {33});
      ByteKey value = ByteKey.copyOf(new byte[] {1});
      ByteKey replacement = ByteKey.copyOf(new byte[] {2});
      store.createIfAbsent(SystemRecordType.WORKSET, identity, value, scope).join();
      assertTrue(store.compareAndSet(SystemRecordType.WORKSET, identity, Optional.of(value),
          replacement, scope).join());
      var outcome = store.retire(SystemRecordType.WORKSET, identity, value, permit(store, scope),
          SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, scope).join();
      assertFalse(outcome.retiredNow());
      assertFalse(outcome.alreadyRetired());
      assertEquals(replacement.length(), outcome.visiblePayloadBytes());
      assertEquals(Optional.of(replacement), store.read(SystemRecordType.WORKSET, identity, scope).join());
      assertTrue(store.readTombstone(SystemRecordType.WORKSET, identity, scope).join().isEmpty());
    }
  }

  @Test
  void bucketScanAdvancesAcrossTombstonesAndReturnsFollowingLiveRecord() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
         RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      HBaseSystemRecordStore store = open(environment, scope);
      var identities = identitiesInOneBucket(store);
      ByteKey value = ByteKey.copyOf(new byte[] {9});
      var permit = permit(store, scope);
      for (ByteKey identity : identities) {
        store.createIfAbsent(SystemRecordType.WORKSET, identity, value, scope).join();
      }
      for (int index = 0; index < 2; index++) {
        store.retire(SystemRecordType.WORKSET, identities.get(index), value, permit,
            SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, scope).join();
      }
      int bucket = store.bucket(SystemRecordType.WORKSET, identities.getFirst());
      Optional<ByteKey> cursor = Optional.empty();
      for (int index = 0; index < 3; index++) {
        var page = store.scanBucket(SystemRecordType.WORKSET, bucket, cursor, 1,
            1L << 20, scope).join();
        assertEquals(Optional.of(identities.get(index)), page.lastIdentity());
        assertEquals(index == 2, page.exhausted());
        assertTrue(page.physicalRows() >= 1 && page.physicalRows() <= 2);
        assertTrue(page.encodedBytes() > 0, "retired rows still consume read bytes");
        if (index < 2) {
          assertTrue(page.entries().isEmpty(), "retired payloads are never emitted");
        } else {
          assertEquals(1, page.entries().size());
          assertEquals(value, page.entries().getFirst().value());
        }
        cursor = page.lastIdentity();
      }
    }
  }

  private static java.util.List<ByteKey> identitiesInOneBucket(HBaseSystemRecordStore store) {
    var identities = new java.util.ArrayList<ByteKey>();
    for (int index = 0; index < 4096 && identities.size() < 3; index++) {
      ByteKey identity = ByteKey.copyOf(java.nio.ByteBuffer.allocate(4).putInt(index).array());
      if (identities.isEmpty() || store.bucket(SystemRecordType.WORKSET, identity)
          == store.bucket(SystemRecordType.WORKSET, identities.getFirst())) {
        identities.add(identity);
      }
    }
    assertEquals(3, identities.size());
    return java.util.List.copyOf(identities);
  }

  @Test
  void permitIsUnusableAfterItsScopeClosesAndForAnotherProfile() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1)) {
      RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3));
      HBaseSystemRecordStore store = open(environment, scope);
      ByteKey identity = ByteKey.copyOf(new byte[] {8});
      ByteKey value = ByteKey.copyOf(new byte[] {9});
      store.createIfAbsent(SystemRecordType.WORKSET, identity, value, scope).join();
      var permit = permit(store, scope);

      try (RequestScope other = RequestScope.withTimeout(Duration.ofMinutes(1))) {
        assertThrows(IllegalStateException.class, () -> permit.requireUsable(
            permit.profile(), other), "a permit cannot be replayed under another scope");
      }
      assertThrows(IllegalStateException.class, () -> permit.requireUsable(
          ByteKey.copyOf(new byte[32]), scope), "a permit is bound to its profile");
      scope.close();
      assertThrows(IllegalStateException.class, () -> permit.requireUsable(
          permit.profile(), scope), "a permit cannot outlive its collection scope");
    }
  }

  @Test
  void staleOwnerCannotRetireOrCloseAnInvocationWindow() throws Exception {
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var store = open(environment, scope);
      var permit = permit(store, scope);
      var identity = ByteKey.copyOf(new byte[] {44});
      var value = ByteKey.copyOf(new byte[] {45});
      store.createIfAbsent(SystemRecordType.WORKSET, identity, value, scope).join();
      var binding = io.etcdhbase.core.commit.InvocationBinding.begin(InvocationId.create(),
          AUTHORIZATION_DIGEST, AUTHORIZATION_DIGEST, TransactionId.create(), 1, 0, 1);
      var invocation = ByteKey.copyOf(binding.invocationId().bytes());
      var encoded = ByteKey.copyOf(io.etcdhbase.hbase.codec.InvocationMessages.encode(binding));
      store.createIfAbsent(SystemRecordType.INVOCATION, invocation, encoded, scope).join();
      var control = store.controlStore();
      var observed = control.read(scope).join().orElseThrow();
      long takeover = observed.state().ownerLeaseDeadlineEpochMillis() + 1;
      assertTrue(control.compareAndSet(observed,
          observed.state().claimOwner("successor", takeover, takeover + 180_000), scope).join());
      assertThrows(CompletionException.class, () -> store.retire(SystemRecordType.WORKSET, identity,
          value, permit, SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, scope).join());
      assertThrows(CompletionException.class, () -> new InvocationWindowCloser(store, SystemRecordGcTestFixture.resultWindowRetention())
          .close(invocation, permit, scope).join());
      assertEquals(Optional.of(value), store.read(SystemRecordType.WORKSET, identity, scope).join());
      assertEquals(Optional.of(encoded), store.read(SystemRecordType.INVOCATION, invocation, scope).join());
      assertTrue(store.readTombstone(SystemRecordType.WORKSET, identity, scope).join().isEmpty());
    }
  }

  @Test
  void successorRowFenceRejectsAnAlreadyDispatchedWindowClose() throws Exception {
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3));
        var successorScope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var table = createRetirementCapableSystemTable(environment);
      var store = HBaseSystemRecordStore.open(environment.factory(), environment.identity(), table, scope).join();
      var authority = SystemRecordGcTestFixture.prepare(store, scope);
      var binding = io.etcdhbase.core.commit.InvocationBinding.begin(InvocationId.create(),
          AUTHORIZATION_DIGEST, AUTHORIZATION_DIGEST, TransactionId.create(), 1, 0, 1)
          .seal(ByteKey.copyOf(new byte[] {7})).selected(ByteKey.copyOf(new byte[] {8}));
      var identity = ByteKey.copyOf(binding.invocationId().bytes());
      var encoded = ByteKey.copyOf(io.etcdhbase.hbase.codec.InvocationMessages.encode(binding));
      store.createIfAbsent(SystemRecordType.INVOCATION, identity, encoded, scope).join();
      var permit = authority.authorize(scope);
      store.fenceForGc(SystemRecordType.INVOCATION, identity, permit, scope).join();
      SystemRecordGcTestFixture.prepareExpiredResultWindow(store, identity, authority.authorize(scope), scope);
      var faults = new Faults(environment.connection());
      var delayed = HBaseSystemRecordStore.open(faults, environment.identity(), table, scope).join();
      faults.gateNextWindow.set(true);
      var close = new InvocationWindowCloser(delayed, SystemRecordGcTestFixture.resultWindowRetention()).close(identity, permit, scope);
      try {
        faults.mutationReached.get(20, TimeUnit.SECONDS);
        var observed = authority.control().read(successorScope).join().orElseThrow();
        long takeover = observed.state().ownerLeaseDeadlineEpochMillis() + 1;
        assertTrue(authority.control().compareAndSet(observed,
            observed.state().claimOwner("successor", takeover, takeover + 180_000), successorScope).join());
        var state = authority.control().read(successorScope).join().orElseThrow().state();
        var successor = new SystemRecordGcTestFixture.Authority(authority.control(), authority.coordinator(),
            authority.retention(), new CommitOwnerService.OwnerToken("successor", state.ownerEpoch()),
            authority.pins(), authority.eventPins());
        var nextPermit = successor.authorize(successorScope);
        int bucket = store.bucket(SystemRecordType.INVOCATION, identity);
        new HBaseSystemRecordClosure(store).fence(SystemRecordClosure.Domain.INVOCATION, bucket,
            Optional.empty(), 16, nextPermit, successorScope).join();
        faults.resume.complete(null);
        var retained = assertInstanceOf(InvocationWindowCloser.Retained.class, close.get(20, TimeUnit.SECONDS));
        assertEquals(InvocationWindowCloser.Skip.ROW_CHANGED, retained.skip());
        assertEquals(Optional.of(encoded), store.read(SystemRecordType.INVOCATION, identity, successorScope).join());
        assertInstanceOf(InvocationWindowCloser.Closed.class,
            new InvocationWindowCloser(store, SystemRecordGcTestFixture.resultWindowRetention()).close(identity, nextPermit, successorScope).join());
      } finally {
        faults.resume.complete(null);
      }
    }
  }

  @Test
  void lostWindowCloseAcknowledgementIsConfirmedByTheDurableClosedBinding() throws Exception {
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var table = createRetirementCapableSystemTable(environment);
      var store = HBaseSystemRecordStore.open(environment.factory(), environment.identity(), table, scope).join();
      var permit = SystemRecordGcTestFixture.authorize(store, scope);
      var binding = io.etcdhbase.core.commit.InvocationBinding.begin(InvocationId.create(),
          AUTHORIZATION_DIGEST, AUTHORIZATION_DIGEST, TransactionId.create(), 1, 0, 1)
          .seal(ByteKey.copyOf(new byte[] {7})).selected(ByteKey.copyOf(new byte[] {8}));
      var identity = ByteKey.copyOf(binding.invocationId().bytes());
      store.createIfAbsent(SystemRecordType.INVOCATION, identity,
          ByteKey.copyOf(io.etcdhbase.hbase.codec.InvocationMessages.encode(binding)), scope).join();
      SystemRecordGcTestFixture.prepareExpiredResultWindow(store, identity, permit, scope);
      var faults = new Faults(environment.connection());
      var delayed = HBaseSystemRecordStore.open(faults, environment.identity(), table, scope).join();
      faults.loseWindowAcknowledgement.set(true);
      assertThrows(CompletionException.class, () -> new InvocationWindowCloser(delayed, SystemRecordGcTestFixture.resultWindowRetention()).close(identity, permit, scope).join());
      var stored = new HBaseInvocationRepository(store).read(binding.invocationId(), scope).join().orElseThrow();
      assertEquals(io.etcdhbase.core.commit.InvocationBinding.State.WINDOW_CLOSED, stored.state());
      assertTrue(stored.sameAttempt(binding));
      var again = assertInstanceOf(InvocationWindowCloser.Retained.class,
          new InvocationWindowCloser(store, SystemRecordGcTestFixture.resultWindowRetention()).close(identity, permit, scope).join());
      assertEquals(InvocationWindowCloser.Skip.ALREADY_CLOSED, again.skip());
    }
  }

  @Test
  void durableRegistryDamageRejectsAnAlreadyIssuedPermit() throws Exception {
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var store = open(environment, scope);
      var permit = permit(store, scope);
      var identity = ByteKey.copyOf(new byte[] {46});
      var value = ByteKey.copyOf(new byte[] {47});
      store.createIfAbsent(SystemRecordType.WORKSET, identity, value, scope).join();
      // The persisted SPIN configuration is deliberately corrupted after legitimate issuance.
      var configuration = ByteKey.copyOf(new byte[] {0x53, 0x50, 0x49, 0x4e, 0x01, 0x00});
      var expected = store.read(SystemRecordType.SNAPSHOT_PIN, configuration, scope).join();
      assertTrue(store.compareAndSet(SystemRecordType.SNAPSHOT_PIN, configuration, expected,
          ByteKey.copyOf(new byte[] {0}), scope).join());
      assertThrows(CompletionException.class, () -> store.retire(SystemRecordType.WORKSET, identity,
          value, permit, SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, scope).join());
      assertEquals(Optional.of(value), store.read(SystemRecordType.WORKSET, identity, scope).join());
      assertTrue(store.readTombstone(SystemRecordType.WORKSET, identity, scope).join().isEmpty());
    }
  }

  private static HBaseSystemRecordStore open(M1TestEnvironment environment, RequestScope scope)
      throws Exception {
    SystemTableBinding binding = createRetirementCapableSystemTable(environment);
    return HBaseSystemRecordStore.open(environment.factory(), environment.identity(), binding,
        scope).join();
  }

  /** Uses the same durable GC authority as the production consumer. */
  private static SystemRecordRetirement.Permit permit(HBaseSystemRecordStore store,
      RequestScope scope) throws io.etcdhbase.core.error.BackendFailure {
    return SystemRecordGcTestFixture.authorize(store, scope);
  }

  private static SystemTableBinding createRetirementCapableSystemTable(
      M1TestEnvironment environment) throws Exception {
    return createRetirementCapableSystemTable(environment, true);
  }

  private static SystemTableBinding createRetirementCapableSystemTable(
      M1TestEnvironment environment, boolean gcFencing) throws Exception {
    String suffix = java.util.UUID.randomUUID().toString().replace("-", "");
    String group = "j52-retire-" + suffix;
    String saltProfileId = "j52-salt-" + suffix;
    HashSaltStrategy salt = new HashSaltStrategy();
    TableName table = TableName.valueOf("j52_retire_" + suffix);
    SystemTableBinding binding = new SystemTableBinding(table, FAMILY, group, saltProfileId, salt);
    var descriptor = TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", group)
        .setValue("etcdhbase.m0.salt-profile-id", saltProfileId)
        .setValue("etcdhbase.m0.salt-strategy-id", salt.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version",
            Integer.toString(salt.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count", Integer.toString(salt.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes", Integer.toString(salt.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories", RECORD_CATEGORIES)
        .setValue("etcdhbase.m4.auxiliary-format", "3")
        .build();
    environment.create(table, gcFencing ? SystemRecordGcTestFixture.descriptor(binding, descriptor)
        : binding.withCompactionCapability(binding.withEventRevisionIndexCapability(descriptor)), salt.splitKeys().toArray(byte[][]::new));
    return binding;
  }
  private static void rejectUnknownFenceAcknowledgement(SystemRecordGcTestFixture.Authority authority,
      SystemTableBinding table, GcCheckpointStore checkpoints,
      HBaseSystemRecordStore delayed, Faults faults, int bucket) throws Exception {
    var scope = RequestScope.withTimeout(Duration.ofMinutes(1));
    try {
      var permit = authority.authorize(scope);
      var domain = SystemRecordClosure.Domain.MANIFEST;
      var proposal = GcCheckpoint.start(table.tableGroupId(), domain,
          authority.owner().epoch(), bucket, table.saltProfile().bucketCount(), permit);
      checkpoints.claim(proposal, scope).join();
      faults.loseFenceAcknowledgement.set(true);
      var failure = assertThrows(CompletionException.class, () -> new HBaseSystemRecordClosure(delayed)
          .fence(domain, bucket, Optional.empty(), 1, permit, scope).join());
      assertEquals(BackendFailure.Category.UNKNOWN,
          ((BackendFailure) CompletionFailures.unwrapCompletion(failure)).category());
      assertFalse(faults.loseFenceAcknowledgement.get());
      assertTrue(faults.fenceWriteApplied.get(), "the actual row mutation committed before its ACK was lost");
      var pending = checkpoints.read(table.tableGroupId(), domain, bucket, scope).join().orElseThrow();
      assertEquals(GcCheckpoint.Phase.FENCING, pending.phase());
      assertTrue(pending.fencePosition().afterIdentity().isEmpty());
      assertEquals(0, pending.completedPages());
    } finally {
      scope.close();
      scope.cleanupOutcomeCompletion().toCompletableFuture().get(20, TimeUnit.SECONDS);
      // The injected lost ACK is an observed failed producer outcome, not an unobserved drain.
      scope.physicalProducersCompletion().handle((ignored, failure) -> null).toCompletableFuture()
          .get(20, TimeUnit.SECONDS);
    }
  }

  private static ByteKey persistFirstFencePage(SystemRecordGcTestFixture.Authority authority,
      SystemTableBinding table, GcCheckpointStore checkpoints,
      HBaseSystemRecordStore store, int bucket) throws Exception {
    var scope = RequestScope.withTimeout(Duration.ofMinutes(1));
    try {
      var permit = authority.authorize(scope);
      var domain = SystemRecordClosure.Domain.MANIFEST;
      var proposal = GcCheckpoint.start(table.tableGroupId(), domain,
          authority.owner().epoch(), bucket, table.saltProfile().bucketCount(), permit);
      var held = (GcCheckpointStore.Held) checkpoints.claim(proposal, scope).join();
      var page = new HBaseSystemRecordClosure(store).fence(domain, bucket, held.checkpoint().fencePosition().afterIdentity(),
          1, permit, scope).join();
      assertFalse(page.exhausted());
      assertTrue(checkpoints.advance(held, page, scope).join());
      return page.lastIdentity().orElseThrow();
    } finally {
      scope.cancel();
      scope.cleanupOutcomeCompletion().toCompletableFuture().get(20, TimeUnit.SECONDS);
      scope.physicalProducersCompletion().toCompletableFuture().get(20, TimeUnit.SECONDS);
    }
  }

  private static java.util.List<ByteKey> manifestsInOneBucket(HBaseSystemRecordStore store, RequestScope scope) {
    var identities = new java.util.ArrayList<ByteKey>();
    for (int attempt = 0; identities.size() < 3 && attempt < 4096; attempt++) {
      var root = ManifestRoot.create(TransactionId.create(),
          1, 0, 1, 0, 0, Optional.empty());
      var identity = root.systemIdentity();
      if (!identities.isEmpty() && store.bucket(SystemRecordType.MANIFEST, identities.getFirst())
          != store.bucket(SystemRecordType.MANIFEST, identity)) continue;
      assertEquals(SystemRecordStore.CreateResult.CREATED, store.createIfAbsent(SystemRecordType.MANIFEST,
          identity, ByteKey.copyOf(ManifestMessages.encode(root)), scope).join());
      identities.add(identity);
    }
    assertEquals(3, identities.size());
    return identities;
  }

  private static SystemRecordRetirement.Permit takeOverAndFence(SystemRecordGcTestFixture.Authority authority,
      SystemTableBinding table, GcCheckpointStore checkpoints, HBaseSystemRecordStore store,
      int bucket, RequestScope scope) throws BackendFailure {
    var observed = authority.control().read(scope).join().orElseThrow();
    long takeover = observed.state().ownerLeaseDeadlineEpochMillis() + 1;
    assertTrue(authority.control().compareAndSet(observed,
        observed.state().claimOwner("successor", takeover, takeover + 180_000), scope).join());
    var nextState = authority.control().read(scope).join().orElseThrow().state();
    var successor = new CommitOwnerService.OwnerToken("successor", nextState.ownerEpoch());
    var nextAuthority = new SystemRecordGcTestFixture.Authority(authority.control(), authority.coordinator(),
        authority.retention(), successor, authority.pins(), authority.eventPins());
    var permit = nextAuthority.authorize(scope);
    var held = fenceShard(store, table, checkpoints, permit, bucket, scope);
    assertEquals(GcCheckpoint.Phase.SCANNING, held.checkpoint().phase());
    assertEquals(successor.epoch(), held.checkpoint().maintenanceEpoch());
    return permit;
  }

  private static GcCheckpointStore.Held fenceShard(HBaseSystemRecordStore store,
      SystemTableBinding table, GcCheckpointStore checkpoints,
      SystemRecordRetirement.Permit permit, int bucket, RequestScope scope) {
    var domain = SystemRecordClosure.Domain.MANIFEST;
    var proposal = GcCheckpoint.start(table.tableGroupId(), domain,
        permit.authority().owner().epoch(), bucket, table.saltProfile().bucketCount(), permit);
    var held = (GcCheckpointStore.Held) checkpoints.claim(proposal, scope).join();
    for (int pages = 0; held.checkpoint().phase() == GcCheckpoint.Phase.FENCING; pages++) {
      assertTrue(pages < 32);
      var page = new HBaseSystemRecordClosure(store).fence(domain, bucket,
          held.checkpoint().fencePosition().afterIdentity(), 16, permit, scope).join();
      assertTrue(checkpoints.advance(held, page, scope).join());
      held = (GcCheckpointStore.Held) checkpoints.claim(proposal, scope).join();
    }
    return held;
  }

  /** Keeps actual HBase CAS/Delete underneath controlled acknowledgement and dispatch races. */
  private static final class Faults implements HBaseConnectionFactory {
    final AtomicBoolean gateNext = new AtomicBoolean();
    final AtomicBoolean gateNextWindow = new AtomicBoolean();
    final AtomicBoolean loseWindowAcknowledgement = new AtomicBoolean();
    final AtomicBoolean loseFenceAcknowledgement = new AtomicBoolean();
    final AtomicBoolean fenceWriteApplied = new AtomicBoolean();
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
            if (args[0] instanceof CheckAndMutate mutation
                && (mutation.getAction() instanceof RowMutations && gateNext.compareAndSet(true, false)
                    || mutation.getAction() instanceof Put put && put.getFamilyCellMap().containsKey(FAMILY)
                        && gateNextWindow.compareAndSet(true, false))) {
              mutationReached.complete(null);
              return resume.thenCompose(ignored -> future(invoke(target, method, args)));
            }
            var actual = future(invoke(target, method, args));
            if (args[0] instanceof CheckAndMutate mutation && mutation.getAction() instanceof Put put
                && put.getFamilyCellMap().containsKey(FAMILY)
                && loseWindowAcknowledgement.compareAndSet(true, false)) {
              return actual.thenCompose(value -> CompletableFuture.failedFuture(
                  new BackendFailure(BackendFailure.Category.UNKNOWN)));
            }
            if (args[0] instanceof CheckAndMutate mutation
                && mutation.getAction() instanceof Put
                && loseFenceAcknowledgement.compareAndSet(true, false)) {
              return actual.thenCompose(value -> {
                fenceWriteApplied.set(((CheckAndMutateResult) value).isSuccess());
                return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
              });
            }
            return actual;
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
