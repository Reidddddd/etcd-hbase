package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.util.Arrays;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

class SystemTableBindingTest {
  private static final TableName TABLE = TableName.valueOf("m2", "system");
  private static final byte[] FAMILY = {'s'};
  private static final String TABLE_GROUP = "system-group";
  private static final String SALT_PROFILE = "system-salt";
  private static final String REQUIRED_CATEGORIES =
      "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION,SNAPSHOT_PIN";
  private static final HashSaltStrategy SALT = new HashSaltStrategy();
  private static final SystemTableBinding BINDING =
      new SystemTableBinding(TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, SALT);

  @Test
  void acceptsTheExactDurableSingleVersionSystemTable() {
    BINDING.validate(descriptor(TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, REQUIRED_CATEGORIES,
        Durability.SYNC_WAL, HConstants.FOREVER, 1));
  }

  @Test
  void resultCapabilityIsExplicitAndDoesNotChangeLegacyValidation() {
    var legacy = descriptor(TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, REQUIRED_CATEGORIES,
        Durability.SYNC_WAL, HConstants.FOREVER, 1);
    var withResult = descriptor(TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, REQUIRED_CATEGORIES + ",RESULT",
        Durability.SYNC_WAL, HConstants.FOREVER, 1);
    assertFalse(BINDING.supportsResults(legacy));
    assertTrue(BINDING.supportsResults(withResult));
    assertFalse(BINDING.supportsAuxiliary(withResult));
    var withAuxiliary = TableDescriptorBuilder.newBuilder(withResult)
        .setValue("etcdhbase.m0.record-categories", BINDING.auxiliaryRecordCategories(withResult))
        .setValue("etcdhbase.m4.auxiliary-format", "1").build();
    assertTrue(BINDING.supportsAuxiliary(withAuxiliary));
    assertFalse(BINDING.supportsLeaseBindings(withAuxiliary));
    var withLeaseBindings = BINDING.withLeaseBindingCapability(withAuxiliary);
    assertTrue(BINDING.supportsAuxiliary(withLeaseBindings));
    assertTrue(BINDING.supportsLeaseBindings(withLeaseBindings));
    assertFalse(BINDING.supportsLeaseExpiryCheckpoints(withLeaseBindings));
    var withExpiry = BINDING.withLeaseExpiryCheckpointCapability(withLeaseBindings);
    assertTrue(BINDING.supportsAuxiliary(withExpiry));
    assertTrue(BINDING.supportsLeaseBindings(withExpiry));
    assertTrue(BINDING.supportsLeaseExpiryCheckpoints(withExpiry));
    assertFalse(BINDING.supportsEventRevisionIndex(withExpiry));
    var withEventIndex = BINDING.withEventRevisionIndexCapability(withExpiry);
    assertTrue(BINDING.supportsEventRevisionIndex(withEventIndex));
    assertTrue(BINDING.supportsLeaseExpiryCheckpoints(withEventIndex));
    assertThrows(IllegalStateException.class,
        () -> BINDING.withEventRevisionIndexCapability(withLeaseBindings));
    var unknownEventIndex = TableDescriptorBuilder.newBuilder(withExpiry)
        .setValue("etcdhbase.m4.event-index-format", "2").build();
    assertFalse(BINDING.supportsEventRevisionIndex(unknownEventIndex));
    assertThrows(IllegalStateException.class,
        () -> BINDING.withEventRevisionIndexCapability(unknownEventIndex));
    var unknownAuxiliary = TableDescriptorBuilder.newBuilder(withResult)
        .setValue("etcdhbase.m4.auxiliary-format", "4").build();
    assertFalse(BINDING.supportsAuxiliary(unknownAuxiliary));
    assertThrows(IllegalStateException.class,
        () -> BINDING.withAuxiliaryCapability(unknownAuxiliary));
  }

  @Test
  void compactRecordsRequireExplicitDescriptorCapability() {
    var legacy = descriptor(TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, REQUIRED_CATEGORIES,
        Durability.SYNC_WAL, HConstants.FOREVER, 1);
    assertFalse(BINDING.supportsCompaction(legacy));
    var declared = BINDING.withCompactionCapability(legacy);
    assertTrue(BINDING.supportsCompaction(declared));
    assertTrue(BINDING.supportsCompaction(BINDING.withCompactionCapability(declared)));
    assertFalse(BINDING.supportsCompaction(TableDescriptorBuilder.newBuilder(declared)
        .setValue("etcdhbase.m0.record-categories", REQUIRED_CATEGORIES).build()));
    var unknown = TableDescriptorBuilder.newBuilder(declared)
        .setValue("etcdhbase.m5.compaction-format", "2").build();
    assertFalse(BINDING.supportsCompaction(unknown));
    assertThrows(IllegalStateException.class, () -> BINDING.withCompactionCapability(unknown));
  }

  @Test
  void gcFencingRequiresAnExplicitDurableIsolatedFamily() {
    var base = descriptor(TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, REQUIRED_CATEGORIES,
        Durability.SYNC_WAL, HConstants.FOREVER, 1);
    assertThrows(IllegalStateException.class, () -> BINDING.withGcFencingCapability(base));
    var compact = BINDING.withCompactionCapability(base);
    assertFalse(BINDING.supportsGcFencing(compact));
    assertEquals(1, compact.getColumnFamilyCount());
    var enabled = BINDING.withGcFencingCapability(compact);
    assertTrue(BINDING.supportsGcFencing(enabled));
    assertEquals(2, enabled.getColumnFamilyCount());
    assertEquals(enabled, BINDING.withGcFencingCapability(enabled));
    var missingFamily = TableDescriptorBuilder.newBuilder(compact)
        .setValue("etcdhbase.m5.gc-fence-format", "1").build();
    assertThrows(IllegalArgumentException.class, () -> BINDING.supportsGcFencing(missingFamily));
    var undeclaredFamily = TableDescriptorBuilder.newBuilder(enabled)
        .removeValue("etcdhbase.m5.gc-fence-format").build();
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(undeclaredFamily));
    var unknown = TableDescriptorBuilder.newBuilder(enabled)
        .setValue("etcdhbase.m5.gc-fence-format", "2").build();
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(unknown));
    for (var badPolicy : java.util.List.of(
        ColumnFamilyDescriptorBuilder.newBuilder(SystemTableBinding.GC_FENCE_FAMILY).setTimeToLive(60).build(),
        ColumnFamilyDescriptorBuilder.newBuilder(SystemTableBinding.GC_FENCE_FAMILY).setMaxVersions(2).build())) {
      assertThrows(IllegalArgumentException.class, () -> BINDING.validate(
          TableDescriptorBuilder.newBuilder(enabled).modifyColumnFamily(badPolicy).build()));
    }
  }

  @Test
  void rejectsTableFamilyProfileCategoryAndDurabilityMismatches() {
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TableName.valueOf("m2", "other"), FAMILY, TABLE_GROUP, SALT_PROFILE, REQUIRED_CATEGORIES,
        Durability.SYNC_WAL, HConstants.FOREVER, 1)));
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TABLE, new byte[] {'x'}, TABLE_GROUP, SALT_PROFILE, REQUIRED_CATEGORIES,
        Durability.SYNC_WAL, HConstants.FOREVER, 1)));
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TABLE, FAMILY, "other-group", SALT_PROFILE, REQUIRED_CATEGORIES,
        Durability.SYNC_WAL, HConstants.FOREVER, 1)));
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TABLE, FAMILY, TABLE_GROUP, "other-salt", REQUIRED_CATEGORIES,
        Durability.SYNC_WAL, HConstants.FOREVER, 1)));
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, "PREPARATION",
        Durability.SYNC_WAL, HConstants.FOREVER, 1)));
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, REQUIRED_CATEGORIES,
        Durability.ASYNC_WAL, HConstants.FOREVER, 1)));
  }

  @Test
  void rejectsExpiringOrMultiVersionFamilies() {
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, REQUIRED_CATEGORIES,
        Durability.SYNC_WAL, 60, 1)));
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(descriptor(
        TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, REQUIRED_CATEGORIES,
        Durability.SYNC_WAL, HConstants.FOREVER, 2)));
  }

  @Test
  void rejectsDifferentRoutingParametersWithTheSameProfileId() {
    var differentSalt = new HashSaltStrategy(
        new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 32, 2));
    var differentBinding = new SystemTableBinding(
        TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, differentSalt);
    var persistedDescriptor = descriptor(TABLE, FAMILY, TABLE_GROUP, SALT_PROFILE, REQUIRED_CATEGORIES,
        Durability.SYNC_WAL, HConstants.FOREVER, 1);

    assertFalse(Arrays.equals(BINDING.controlRow(), differentBinding.controlRow()));
    assertThrows(IllegalArgumentException.class,
        () -> differentBinding.validate(persistedDescriptor));
  }

  private static TableDescriptor descriptor(TableName table, byte[] family, String tableGroup,
      String saltProfile, String categories, Durability durability, int ttl, int maxVersions) {
    var column = ColumnFamilyDescriptorBuilder.newBuilder(family)
        .setTimeToLive(ttl).setMaxVersions(maxVersions).build();
    return TableDescriptorBuilder.newBuilder(table).setColumnFamily(column)
        .setDurability(durability).setValue("etcdhbase.m0.table-group-id", tableGroup)
        .setValue("etcdhbase.m0.salt-profile-id", saltProfile)
        .setValue("etcdhbase.m0.salt-strategy-id", SALT.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version",
            Integer.toString(SALT.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count",
            Integer.toString(SALT.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes",
            Integer.toString(SALT.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories", categories).build();
  }
}
