package io.etcdhbase.hbase.storage;

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

  private static HBaseSystemRecordStore open(M1TestEnvironment environment, RequestScope scope)
      throws Exception {
    SystemTableBinding binding = createRetirementCapableSystemTable(environment);
    return HBaseSystemRecordStore.open(environment.factory(), environment.identity(), binding,
        scope).join();
  }

  /** Production permits come from the GC coordinator; this fixture issues an equivalent one. */
  private static SystemRecordRetirement.Permit permit(HBaseSystemRecordStore store,
      RequestScope scope) {
    return SystemRecordRetirement.Permit.issue(store.profileFingerprint(), AUTHORIZATION_DIGEST,
        InvocationId.create(), 1, 4, 4, scope);
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
        .setValue("etcdhbase.m4.auxiliary-format", "1")
        .build();
    environment.create(table, descriptor, salt.splitKeys().toArray(byte[][]::new));
    return binding;
  }
}
