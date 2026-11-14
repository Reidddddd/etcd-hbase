package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.hbase.codec.SystemRecordKeyCodec;
import io.etcdhbase.spi.salt.SaltProfile;
import io.etcdhbase.spi.salt.SaltStrategy;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Set;
import java.util.Objects;
import java.util.LinkedHashSet;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/** Validated binding to the pre-provisioned system table; it never creates or adopts a table. */
public final class SystemTableBinding {
  private static final Set<String> REQUIRED_RECORD_CATEGORIES =
      Set.of("CONTROL", "INVOCATION", "CANDIDATE", "MANIFEST", "DECISION", "SNAPSHOT_PIN");
  private static final Set<String> AUXILIARY_RECORD_CATEGORIES = Set.of(
      "AUXILIARY", "LEASE_STATE", "LEASE_ENUMERATION", "EXPIRY_CANDIDATE",
      "LEASE_RESULT", "LEASE_BINDING");
  private final TableName table;
  private final byte[] family;
  private final String tableGroupId;
  private final String saltProfileId;
  private final SaltProfile saltProfile;
  private final SystemRecordKeyCodec keyCodec;
  private final ByteKey profileFingerprint;

  public SystemTableBinding(TableName table, byte[] family, String tableGroupId,
      String saltProfileId, SaltStrategy salt) {
    this.table = Objects.requireNonNull(table, "table");
    this.family = Objects.requireNonNull(family, "family").clone();
    this.tableGroupId = requireIdentifier(tableGroupId, "tableGroupId");
    this.saltProfileId = requireIdentifier(saltProfileId, "saltProfileId");
    SaltStrategy checkedSalt = Objects.requireNonNull(salt, "salt");
    this.saltProfile = Objects.requireNonNull(checkedSalt.profile(), "salt profile");
    if (!checkedSalt.id().equals(saltProfile.strategyId())) {
      throw new IllegalArgumentException("salt strategy ID does not match its profile");
    }
    this.keyCodec = new SystemRecordKeyCodec(checkedSalt);
    this.profileFingerprint = ByteKey.copyOf(fingerprint(table, this.family, this.tableGroupId,
        this.saltProfileId, saltProfile, keyCodec.formatVersion()));
    if (this.family.length == 0) throw new IllegalArgumentException("system family must not be empty");
  }

  public TableName table() { return table; }
  public byte[] family() { return family.clone(); }
  public String tableGroupId() { return tableGroupId; }
  public SaltProfile saltProfile() { return saltProfile; }
  public SystemRecordKeyCodec keyCodec() { return keyCodec; }
  public ByteKey profileFingerprint() { return profileFingerprint; }

  public byte[] controlRow() {
    return keyCodec.encode(SystemRecordType.CONTROL,
        ByteKey.copyOf(tableGroupId.getBytes(StandardCharsets.UTF_8)));
  }

  public byte[] row(SystemRecordType type, ByteKey identity) {
    return keyCodec.encode(type, identity);
  }

  /** Requires the exact pre-provisioned M0 table identity and a durable, non-expiring family. */
  public void validate(TableDescriptor descriptor) {
    Objects.requireNonNull(descriptor, "descriptor");
    if (!descriptor.getTableName().equals(table)
        || descriptor.getColumnFamilyCount() != 1
        || !descriptor.hasColumnFamily(family)
        || descriptor.getDurability() != Durability.SYNC_WAL
        || !tableGroupId.equals(descriptor.getValue("etcdhbase.m0.table-group-id"))
        || !saltProfileId.equals(descriptor.getValue("etcdhbase.m0.salt-profile-id"))
        || !saltProfile.strategyId().equals(
            descriptor.getValue("etcdhbase.m0.salt-strategy-id"))
        || !Integer.toString(saltProfile.encodingVersion()).equals(
            descriptor.getValue("etcdhbase.m0.salt-encoding-version"))
        || !Integer.toString(saltProfile.bucketCount()).equals(
            descriptor.getValue("etcdhbase.m0.salt-bucket-count"))
        || !Integer.toString(saltProfile.saltWidthBytes()).equals(
            descriptor.getValue("etcdhbase.m0.salt-width-bytes"))
        || !containsRequiredCategories(descriptor.getValue("etcdhbase.m0.record-categories"))) {
      throw new IllegalArgumentException("table does not match its M2 system binding");
    }
    var columnFamily = descriptor.getColumnFamily(family);
    if (columnFamily.getTimeToLive() != HConstants.FOREVER || columnFamily.getMaxVersions() != 1) {
      throw new IllegalArgumentException(
          "system records require a non-expiring single-version column family");
    }
  }

  /** M3 result, identity and workset records require this capability; older M2 tables remain readable. */
  public boolean supportsResults(TableDescriptor descriptor) {
    validate(descriptor);
    String categories = descriptor.getValue("etcdhbase.m0.record-categories");
    return Arrays.asList(categories.split(",", -1)).contains("RESULT");
  }

  /** Descriptor capability only; safe activation also requires the control-row upgrade fence. */
  public boolean supportsAuxiliary(TableDescriptor descriptor) {
    return supportsResults(descriptor)
        && "1".equals(descriptor.getValue("etcdhbase.m4.auxiliary-format"))
        && containsAllCategories(descriptor.getValue("etcdhbase.m0.record-categories"),
            AUXILIARY_RECORD_CATEGORIES);
  }

  /** Returns an additive record-category declaration for the fenced M4 capability upgrade. */
  public String auxiliaryRecordCategories(TableDescriptor descriptor) {
    validate(descriptor);
    if (!supportsResults(descriptor)) {
      throw new IllegalStateException("M4 auxiliary records require the M3 result capability");
    }
    var categories = new LinkedHashSet<>(Arrays.asList(
        descriptor.getValue("etcdhbase.m0.record-categories").split(",", -1)));
    categories.addAll(AUXILIARY_RECORD_CATEGORIES);
    return String.join(",", categories);
  }

  /** Adds the M4 record capability without changing the table's rows or family policy. */
  public TableDescriptor withAuxiliaryCapability(TableDescriptor descriptor) {
    validate(descriptor);
    if (supportsAuxiliary(descriptor)) return descriptor;
    TableDescriptor upgraded = TableDescriptorBuilder.newBuilder(descriptor)
        .setValue("etcdhbase.m0.record-categories", auxiliaryRecordCategories(descriptor))
        .setValue("etcdhbase.m4.auxiliary-format", "1")
        .build();
    if (!supportsAuxiliary(upgraded)) {
      throw new IllegalStateException("M4 auxiliary descriptor capability could not be constructed");
    }
    return upgraded;
  }

  private static boolean containsRequiredCategories(String categories) {
    return containsAllCategories(categories, REQUIRED_RECORD_CATEGORIES);
  }

  private static boolean containsAllCategories(String categories, Set<String> required) {
    if (categories == null || categories.isBlank()) return false;
    String[] items = categories.split(",", -1);
    if (Arrays.stream(items).anyMatch(String::isBlank)) return false;
    Set<String> declared = Set.copyOf(Arrays.asList(items));
    return declared.containsAll(required);
  }

  private static String requireIdentifier(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!value.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")) {
      throw new IllegalArgumentException("invalid " + name);
    }
    return value;
  }

  private static byte[] fingerprint(TableName table, byte[] family, String group,
      String saltProfileId, SaltProfile profile, int keyFormatVersion) {
    try {
      var bytes = new ByteArrayOutputStream();
      try (var output = new DataOutputStream(bytes)) {
        output.writeInt(2);
        write(output, table.getName());
        write(output, family);
        write(output, group.getBytes(StandardCharsets.UTF_8));
        write(output, saltProfileId.getBytes(StandardCharsets.UTF_8));
        write(output, profile.strategyId().getBytes(StandardCharsets.UTF_8));
        output.writeInt(profile.encodingVersion());
        output.writeInt(profile.bucketCount());
        output.writeInt(profile.saltWidthBytes());
        output.writeInt(keyFormatVersion);
      }
      return MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
    } catch (IOException impossible) {
      throw new IllegalStateException("in-memory system binding fingerprint failed", impossible);
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private static void write(DataOutputStream output, byte[] value) throws IOException {
    output.writeInt(value.length);
    output.write(value);
  }
}
