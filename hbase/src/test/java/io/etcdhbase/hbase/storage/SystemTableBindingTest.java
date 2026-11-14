package io.etcdhbase.hbase.storage;

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
    var unknownAuxiliary = TableDescriptorBuilder.newBuilder(withResult)
        .setValue("etcdhbase.m4.auxiliary-format", "2").build();
    assertFalse(BINDING.supportsAuxiliary(unknownAuxiliary));
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
