package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.commit.AuxiliaryFormatUpgrade;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.LeaseBindingFormatUpgrade;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/** Verifies the J40-to-J41 control-row upgrade and stale writer fence on MiniCluster. */
class J41CommitControlStoreIT {
  private static final byte[] FAMILY = {'s'};
  private static final String TABLE_GROUP = "j41-control";
  private static final String SALT_PROFILE = "j41-control-salt";
  private static final HashSaltStrategy SALT = new HashSaltStrategy();

  @Test
  void requiresTheJ41DescriptorMarkerAndReopenedControlStoreBeforeFormatUpgrade() throws Exception {
    TableName table = TableName.valueOf("j41_" + UUID.randomUUID().toString().replace("-", ""));
    var binding = new SystemTableBinding(table, FAMILY, TABLE_GROUP, SALT_PROFILE, SALT);
    var baseDescriptor = TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", TABLE_GROUP)
        .setValue("etcdhbase.m0.salt-profile-id", SALT_PROFILE)
        .setValue("etcdhbase.m0.salt-strategy-id", SALT.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version",
            Integer.toString(SALT.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count", Integer.toString(SALT.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes", Integer.toString(SALT.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories",
            "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION,SNAPSHOT_PIN,RESULT")
        .build();
    var auxiliaryDescriptor = binding.withAuxiliaryCapability(baseDescriptor);

    try (var environment = M1TestEnvironment.start(1);
         var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      environment.create(table, auxiliaryDescriptor, SALT.splitKeys().toArray(byte[][]::new));
      var openedBeforeJ41Marker = M1TestEnvironment.await(HBaseCommitControlStore.open(
          environment.factory(), environment.identity(), binding, scope));
      assertTrue(openedBeforeJ41Marker.supportsAuxiliary());
      assertFalse(openedBeforeJ41Marker.supportsLeaseBindings());
      assertTrue(M1TestEnvironment.await(openedBeforeJ41Marker.initializeIfAbsent(
          CommitState.initial(binding.profileFingerprint(), 1), scope)));

      var initial = M1TestEnvironment.await(openedBeforeJ41Marker.read(scope)).orElseThrow();
      CommitState claimed = initial.state().claimOwner("owner-a", 10, 100);
      assertTrue(M1TestEnvironment.await(openedBeforeJ41Marker.compareAndSet(initial, claimed, scope)));
      var owner = M1TestEnvironment.await(new AuxiliaryFormatUpgrade(openedBeforeJ41Marker)
          .upgrade(new CommitOwnerService.OwnerToken("owner-a", claimed.ownerEpoch()), 11, scope));
      var formatTwo = M1TestEnvironment.await(openedBeforeJ41Marker.read(scope)).orElseThrow();
      assertEquals(CommitState.AUXILIARY_FORMAT_VERSION, formatTwo.state().formatVersion());

      assertThrows(ExecutionException.class, () -> M1TestEnvironment.await(
          new LeaseBindingFormatUpgrade(openedBeforeJ41Marker).upgrade(owner, 12, scope)),
          "a J40 descriptor snapshot must reject the v2-to-v3 control CAS");

      var descriptor = environment.connection().getAdmin().getDescriptor(table).join();
      environment.connection().getAdmin().modifyTable(binding.withLeaseBindingCapability(descriptor)).join();
      assertTrue(binding.supportsLeaseBindings(
          environment.connection().getAdmin().getDescriptor(table).join()));
      assertThrows(ExecutionException.class, () -> M1TestEnvironment.await(
          new LeaseBindingFormatUpgrade(openedBeforeJ41Marker).upgrade(owner, 13, scope)),
          "the old adapter must keep its J40 capability snapshot after the descriptor changes");

      var reopened = M1TestEnvironment.await(HBaseCommitControlStore.open(
          environment.factory(), environment.identity(), binding, scope));
      assertTrue(reopened.supportsLeaseBindings());
      var j41Owner = M1TestEnvironment.await(new LeaseBindingFormatUpgrade(reopened)
          .upgrade(owner, 14, scope));
      var formatThree = M1TestEnvironment.await(reopened.read(scope)).orElseThrow();
      assertTrue(formatThree.state().supportsLeaseBindings());
      assertEquals(j41Owner.epoch(), formatThree.state().ownerEpoch());
      assertFalse(M1TestEnvironment.await(openedBeforeJ41Marker.compareAndSet(formatTwo,
          formatTwo.state().heartbeat("owner-a", owner.epoch(), 200), scope)),
          "the pre-upgrade J40 control snapshot must not publish after J41 is enabled");
    }
  }
}
