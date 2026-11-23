package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

class AuthControlBindingTest {
  private static final TableName TABLE = TableName.valueOf("auth", "control");
  private static final HashSaltStrategy SALT = new HashSaltStrategy(
      new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 16, 2));
  private static final AuthControlBinding BINDING = new AuthControlBinding(TABLE, "milvus", SALT);

  @Test
  void acceptsOnlyTheExactNonExpiringSingleVersionSyncWalTable() {
    assertDoesNotThrow(() -> BINDING.validate(BINDING.descriptor()));

    var wrongTable = new AuthControlBinding(
        TableName.valueOf("auth", "other"), "milvus", SALT).descriptor();
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(wrongTable));

    var wrongDurability = TableDescriptorBuilder.newBuilder(BINDING.descriptor())
        .setDurability(Durability.ASYNC_WAL).build();
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(wrongDurability));

    var wrongFamily = TableDescriptorBuilder.newBuilder(TABLE)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(new byte[] {'x'})
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL).build();
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(wrongFamily));

    var expiringFamily = ColumnFamilyDescriptorBuilder.newBuilder(BINDING.family())
        .setTimeToLive(60).setMaxVersions(1).build();
    var expiring = TableDescriptorBuilder.newBuilder(BINDING.descriptor())
        .modifyColumnFamily(expiringFamily).build();
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(expiring));

    var multiVersionFamily = ColumnFamilyDescriptorBuilder.newBuilder(BINDING.family())
        .setTimeToLive(HConstants.FOREVER).setMaxVersions(2).build();
    var multiVersion = TableDescriptorBuilder.newBuilder(BINDING.descriptor())
        .modifyColumnFamily(multiVersionFamily).build();
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(multiVersion));
  }

  @Test
  void bindsTrustDomainAndSaltProfileIntoDescriptor() {
    var otherDomain = new AuthControlBinding(TABLE, "other-domain", SALT);
    assertThrows(IllegalArgumentException.class,
        () -> BINDING.validate(otherDomain.descriptor()));
    var otherSalt = new AuthControlBinding(TABLE, "milvus",
        new HashSaltStrategy(new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 8, 2)));
    assertThrows(IllegalArgumentException.class, () -> BINDING.validate(otherSalt.descriptor()));
  }
}
