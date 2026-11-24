package io.etcdhbase.hbase.storage;

import io.etcdhbase.hbase.codec.AuthControlKeyCodec;
import io.etcdhbase.spi.salt.SaltStrategy;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/** Exact binding to the separate, non-expiring auth-control table. */
public final class AuthControlBinding {
  private static final byte[] FAMILY = {'a'};
  private static final String FORMAT_VERSION = "1";

  private final TableName table;
  private final String trustDomain;
  private final SaltStrategy salt;
  private final AuthControlKeyCodec keyCodec;

  public AuthControlBinding(TableName table, String trustDomain, SaltStrategy salt) {
    this.table = Objects.requireNonNull(table, "table");
    this.trustDomain = requireIdentifier(trustDomain, "trustDomain");
    this.salt = Objects.requireNonNull(salt, "salt");
    if (!salt.id().equals(salt.profile().strategyId())) {
      throw new IllegalArgumentException("auth-control salt strategy does not match its profile");
    }
    this.keyCodec = new AuthControlKeyCodec(salt);
  }

  public TableName table() { return table; }
  public String trustDomain() { return trustDomain; }
  public AuthControlKeyCodec keyCodec() { return keyCodec; }
  public byte[] family() { return FAMILY.clone(); }
  public java.util.List<byte[]> splitKeys() {
    return salt.splitKeys().stream().map(byte[]::clone).toList();
  }

  public TableDescriptor descriptor() {
    var builder = TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
            .setMaxVersions(1).setTimeToLive(HConstants.FOREVER).build())
        .setDurability(Durability.SYNC_WAL);
    attributes().forEach(builder::setValue);
    return builder.build();
  }

  public void validate(TableDescriptor descriptor) {
    Objects.requireNonNull(descriptor, "descriptor");
    if (!descriptor.getTableName().equals(table)
        || descriptor.getColumnFamilyCount() != 1
        || !descriptor.hasColumnFamily(FAMILY)
        || descriptor.getDurability() != Durability.SYNC_WAL
        || !attributes().entrySet().stream().allMatch(entry ->
            entry.getValue().equals(descriptor.getValue(entry.getKey())))) {
      throw new IllegalArgumentException("table does not match its auth-control binding");
    }
    var family = descriptor.getColumnFamily(FAMILY);
    if (family.getTimeToLive() != HConstants.FOREVER || family.getMaxVersions() != 1) {
      throw new IllegalArgumentException("auth-control records require non-expiring single-version storage");
    }
  }

  private Map<String, String> attributes() {
    var profile = salt.profile();
    return Map.of(
        "etcdhbase.auth.format-version", FORMAT_VERSION,
        "etcdhbase.auth.trust-domain", trustDomain,
        "etcdhbase.auth.family", HexFormat.of().formatHex(FAMILY),
        "etcdhbase.auth.rowkey-version", Integer.toString(AuthControlKeyCodec.FORMAT_VERSION),
        "etcdhbase.auth.salt-strategy", profile.strategyId(),
        "etcdhbase.auth.salt-encoding-version", Integer.toString(profile.encodingVersion()),
        "etcdhbase.auth.salt-bucket-count", Integer.toString(profile.bucketCount()),
        "etcdhbase.auth.salt-width-bytes", Integer.toString(profile.saltWidthBytes()));
  }

  private static String requireIdentifier(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!value.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException(name + " must be a bounded identifier");
    }
    return value;
  }
}
