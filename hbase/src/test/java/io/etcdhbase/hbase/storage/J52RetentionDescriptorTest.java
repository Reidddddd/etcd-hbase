package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/** Descriptor contract checks only; no HBase retention or compaction I/O is exercised. */
class J52RetentionDescriptorTest {
  private static final byte[] FAMILY = {'d'};
  private static final HashSaltStrategy SALT = new HashSaltStrategy(
      new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2));

  @Test
  void rejectsFiniteTtlForEveryLayout() {
    for (var layout : StorageTableBinding.Layout.values()) {
      var binding = binding(layout);
      var descriptor = binding.descriptor();
      binding.validate(descriptor);
      var withTtl = TableDescriptorBuilder.newBuilder(descriptor)
          .modifyColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(
              descriptor.getColumnFamily(FAMILY)).setTimeToLive(60).build())
          .build();
      assertThrows(IllegalArgumentException.class, () -> binding.validate(withTtl),
          "finite TTL must be rejected for " + layout);
    }
  }

  @Test
  void rejectsSingleVersionForNativeLayout() {
    var binding = binding(StorageTableBinding.Layout.NATIVE);
    var descriptor = binding.descriptor();
    binding.validate(descriptor);
    var singleVersion = TableDescriptorBuilder.newBuilder(descriptor)
        .modifyColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(
            descriptor.getColumnFamily(FAMILY)).setMaxVersions(1).build())
        .build();
    assertThrows(IllegalArgumentException.class, () -> binding.validate(singleVersion));
  }

  @Test
  void historyDescriptorDefaultsToOneVersionAndForeverTtl() {
    var binding = binding(StorageTableBinding.Layout.HISTORY);
    var descriptor = binding.descriptor();
    assertEquals(1, descriptor.getColumnFamily(FAMILY).getMaxVersions());
    assertEquals(HConstants.FOREVER, descriptor.getColumnFamily(FAMILY).getTimeToLive());
    binding.validate(descriptor);
  }

  @Test
  void nativeDescriptorDefaultsTo64AndAccepts128Versions() {
    var binding = binding(StorageTableBinding.Layout.NATIVE);
    var descriptor = binding.descriptor();
    assertEquals(64, descriptor.getColumnFamily(FAMILY).getMaxVersions());
    assertEquals(HConstants.FOREVER, descriptor.getColumnFamily(FAMILY).getTimeToLive());
    binding.validate(descriptor);
    var moreVersions = TableDescriptorBuilder.newBuilder(descriptor)
        .modifyColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(
            descriptor.getColumnFamily(FAMILY)).setMaxVersions(128).build())
        .build();
    binding.validate(moreVersions);
  }

  private static StorageTableBinding binding(StorageTableBinding.Layout layout) {
    return new StorageTableBinding(TableName.valueOf("j52_descriptor_" + layout), FAMILY,
        layout, SALT);
  }
}
