package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.RetirementTombstone;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
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
          AUTHORIZATION_DIGEST, AUTHORIZATION_DIGEST, io.etcdhbase.core.commit.TransactionId.create(), 1, 0, 1);
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
      assertThrows(CompletionException.class, () -> new InvocationWindowCloser(store)
          .close(invocation, permit, scope).join());
      assertEquals(Optional.of(value), store.read(SystemRecordType.WORKSET, identity, scope).join());
      assertEquals(Optional.of(encoded), store.read(SystemRecordType.INVOCATION, invocation, scope).join());
      assertTrue(store.readTombstone(SystemRecordType.WORKSET, identity, scope).join().isEmpty());
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
    environment.create(table, SystemRecordGcTestFixture.descriptor(binding, descriptor), salt.splitKeys().toArray(byte[][]::new));
    return binding;
  }
}
