package io.etcdhbase.hbase.storage;

import io.etcdhbase.hbase.codec.RowKeyCodec;
import io.etcdhbase.spi.salt.SaltStrategy;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/** A dedicated KV table has exactly one row format/layout and one persistent salt profile. */
public final class StorageTableBinding {
  public enum Layout { CURRENT, HISTORY, NATIVE }
  private final TableName table;
  private final byte[] family;
  private final Layout layout;
  private final SaltStrategy salt;
  private final RowKeyCodec codec;

  public StorageTableBinding(TableName table, byte[] family, Layout layout, SaltStrategy salt) {
    this.table = Objects.requireNonNull(table, "table");
    this.family = Objects.requireNonNull(family, "family").clone();
    this.layout = Objects.requireNonNull(layout, "layout");
    this.salt = Objects.requireNonNull(salt, "salt");
    if (family.length == 0) throw new IllegalArgumentException("column family must not be empty");
    this.codec = new RowKeyCodec(salt, layout == Layout.HISTORY ? RowKeyCodec.Shape.HISTORY : RowKeyCodec.Shape.CURRENT);
  }

  public TableName table() { return table; }
  public byte[] family() { return family.clone(); }
  public Layout layout() { return layout; }
  public RowKeyCodec codec() { return codec; }

  /** Prototype provisioning only; existing unknown/nonempty tables are never adopted or rewritten. */
  public TableDescriptor descriptor() {
    var builder = TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(family)
            .setMaxVersions(layout == Layout.NATIVE ? 64 : 1).build())
        .setDurability(Durability.SYNC_WAL);
    attributes().forEach(builder::setValue);
    return builder.build();
  }

  public void validate(TableDescriptor descriptor) {
    if (!descriptor.getTableName().equals(table) || !descriptor.hasColumnFamily(family)
        || descriptor.getColumnFamilyCount() != 1 || descriptor.getDurability() != Durability.SYNC_WAL) {
      throw new IllegalArgumentException("table does not match its storage binding");
    }
    attributes().forEach((name, value) -> {
      if (!value.equals(descriptor.getValue(name))) {
        throw new IllegalArgumentException("table storage format/profile mismatch");
      }
    });
    var columnFamily = descriptor.getColumnFamily(family);
    if (columnFamily.getTimeToLive() != Integer.MAX_VALUE
        || layout == Layout.NATIVE && columnFamily.getMaxVersions() < 2) {
      throw new IllegalArgumentException("revision timestamps require explicit history retention");
    }
  }

  private Map<String, String> attributes() {
    var profile = salt.profile();
    return Map.of("etcdhbase.storage.rowkey", RowKeyCodec.FORMAT_ID,
        "etcdhbase.storage.layout", layout.name(),
        "etcdhbase.storage.family", HexFormat.of().formatHex(family),
        "etcdhbase.storage.salt.strategy", profile.strategyId(),
        "etcdhbase.storage.salt.version", Integer.toString(profile.encodingVersion()),
        "etcdhbase.storage.salt.buckets", Integer.toString(profile.bucketCount()),
        "etcdhbase.storage.salt.width", Integer.toString(profile.saltWidthBytes()));
  }
}
