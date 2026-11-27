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
  static final byte[] GC_FENCE_FAMILY = {'g', 'c'};
  private static final String GC_FENCE_FORMAT = "etcdhbase.m5.gc-fence-format";
  private static final String GC_FENCE_FORMAT_VALUE = "1";
  private static final String AUXILIARY_FORMAT_VALUE = "1";
  private static final String LEASE_BINDING_FORMAT_VALUE = "2";
  private static final String LEASE_EXPIRY_FORMAT_VALUE = "3";
  private static final String EVENT_INDEX_FORMAT_VALUE = "1";
  private static final Set<String> REQUIRED_RECORD_CATEGORIES =
      Set.of("CONTROL", "INVOCATION", "CANDIDATE", "MANIFEST", "DECISION", "SNAPSHOT_PIN");
  private static final Set<String> AUXILIARY_RECORD_CATEGORIES = Set.of(
      "AUXILIARY", "LEASE_STATE", "LEASE_ENUMERATION", "EXPIRY_CANDIDATE",
      "LEASE_RESULT", "LEASE_BINDING");
  private static final Set<String> LEASE_EXPIRY_RECORD_CATEGORIES =
      Set.of("LEASE_EXPIRY_CHECKPOINT");
  private static final Set<String> EVENT_INDEX_RECORD_CATEGORIES =
      Set.of("EVENT_REVISION_INDEX", "EVENT_INDEX_BACKFILL_CHECKPOINT");
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
        || !hasValidFamilies(descriptor)
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

  private boolean hasValidFamilies(TableDescriptor descriptor) {
    String format = descriptor.getValue(GC_FENCE_FORMAT);
    if (format == null) return descriptor.getColumnFamilyCount() == 1;
    if (!GC_FENCE_FORMAT_VALUE.equals(format) || descriptor.getColumnFamilyCount() != 2
        || Arrays.equals(family, GC_FENCE_FAMILY)) return false;
    var maintenance = descriptor.getColumnFamily(GC_FENCE_FAMILY);
    return maintenance != null && maintenance.getTimeToLive() == HConstants.FOREVER
        && maintenance.getMaxVersions() == 1;
  }

  /** Maintenance metadata is isolated from the payload family read by earlier binaries. */
  public boolean supportsGcFencing(TableDescriptor descriptor) {
    return supportsCompaction(descriptor) && GC_FENCE_FORMAT_VALUE.equals(descriptor.getValue(GC_FENCE_FORMAT));
  }

  /** Builds an explicit provisioning descriptor; this method never alters an existing table. */
  public TableDescriptor withGcFencingCapability(TableDescriptor descriptor) {
    validate(descriptor);
    if (!supportsCompaction(descriptor)) throw new IllegalStateException("GC fencing requires Compact capability");
    if (supportsGcFencing(descriptor)) return descriptor;
    if (Arrays.equals(family, GC_FENCE_FAMILY)) throw new IllegalStateException("payload family conflicts with GC family");
    return TableDescriptorBuilder.newBuilder(descriptor)
        .setColumnFamily(org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.newBuilder(GC_FENCE_FAMILY)
            .setMaxVersions(1).setTimeToLive(HConstants.FOREVER).build())
        .setValue(GC_FENCE_FORMAT, GC_FENCE_FORMAT_VALUE).build();
  }

  /** M3 result, identity and workset records require this capability; older M2 tables remain readable. */
  public boolean supportsResults(TableDescriptor descriptor) {
    validate(descriptor);
    String categories = descriptor.getValue("etcdhbase.m0.record-categories");
    return Arrays.asList(categories.split(",", -1)).contains("RESULT");
  }

  /** Descriptor capability only; safe activation also requires the control-row upgrade fence. */
  public boolean supportsAuxiliary(TableDescriptor descriptor) {
    String format = descriptor.getValue("etcdhbase.m4.auxiliary-format");
    return supportsResults(descriptor)
        && (AUXILIARY_FORMAT_VALUE.equals(format) || LEASE_BINDING_FORMAT_VALUE.equals(format)
            || LEASE_EXPIRY_FORMAT_VALUE.equals(format))
        && containsAllCategories(descriptor.getValue("etcdhbase.m0.record-categories"),
            AUXILIARY_RECORD_CATEGORIES);
  }

  /** J41 writers require this descriptor marker in addition to the control-row epoch fence. */
  public boolean supportsLeaseBindings(TableDescriptor descriptor) {
    String format = descriptor.getValue("etcdhbase.m4.auxiliary-format");
    return supportsAuxiliary(descriptor)
        && (LEASE_BINDING_FORMAT_VALUE.equals(format) || LEASE_EXPIRY_FORMAT_VALUE.equals(format));
  }

  /** J43 checkpoint records require their descriptor declaration and the control-row epoch fence. */
  public boolean supportsLeaseExpiryCheckpoints(TableDescriptor descriptor) {
    return supportsLeaseBindings(descriptor)
        && LEASE_EXPIRY_FORMAT_VALUE.equals(descriptor.getValue("etcdhbase.m4.auxiliary-format"))
        && containsAllCategories(descriptor.getValue("etcdhbase.m0.record-categories"),
            LEASE_EXPIRY_RECORD_CATEGORIES);
  }

  /** Descriptor capability only; writers must also acquire the upgraded control-row fence. */
  public boolean supportsEventRevisionIndex(TableDescriptor descriptor) {
    return supportsLeaseExpiryCheckpoints(descriptor)
        && EVENT_INDEX_FORMAT_VALUE.equals(descriptor.getValue("etcdhbase.m4.event-index-format"))
        && containsAllCategories(descriptor.getValue("etcdhbase.m0.record-categories"),
            EVENT_INDEX_RECORD_CATEGORIES);
  }

  /** Declares event-index storage independently from Lease expiry support. */
  public TableDescriptor withEventRevisionIndexCapability(TableDescriptor descriptor) {
    validate(descriptor);
    if (supportsEventRevisionIndex(descriptor)) return descriptor;
    if (!supportsLeaseExpiryCheckpoints(descriptor)) {
      throw new IllegalStateException("event indexes require Lease expiry checkpoint capability");
    }
    String format = descriptor.getValue("etcdhbase.m4.event-index-format");
    if (format != null && !EVENT_INDEX_FORMAT_VALUE.equals(format)) {
      throw new IllegalStateException("system table declares an unsupported event index format");
    }
    var categories = new LinkedHashSet<>(Arrays.asList(
        descriptor.getValue("etcdhbase.m0.record-categories").split(",", -1)));
    categories.addAll(EVENT_INDEX_RECORD_CATEGORIES);
    TableDescriptor upgraded = TableDescriptorBuilder.newBuilder(descriptor)
        .setValue("etcdhbase.m0.record-categories", String.join(",", categories))
        .setValue("etcdhbase.m4.event-index-format", EVENT_INDEX_FORMAT_VALUE)
        .build();
    if (!supportsEventRevisionIndex(upgraded)) {
      throw new IllegalStateException("event index descriptor capability could not be constructed");
    }
    return upgraded;
  }

  /** A capability gate for Compact operation identities and exact KV version retirement. */
  public boolean supportsCompaction(TableDescriptor descriptor) {
    validate(descriptor);
    return "1".equals(descriptor.getValue("etcdhbase.m5.compaction-format"))
        && containsAllCategories(descriptor.getValue("etcdhbase.m0.record-categories"),
            Set.of("COMPACTION"));
  }

  /** Declares storage support; activation and RPC assembly remain separate responsibilities. */
  public TableDescriptor withCompactionCapability(TableDescriptor descriptor) {
    validate(descriptor);
    String format = descriptor.getValue("etcdhbase.m5.compaction-format");
    if (format != null && !"1".equals(format)) {
      throw new IllegalStateException("system table declares an unsupported Compact format");
    }
    var categories = new LinkedHashSet<>(Arrays.asList(
        descriptor.getValue("etcdhbase.m0.record-categories").split(",", -1)));
    categories.add("COMPACTION");
    return TableDescriptorBuilder.newBuilder(descriptor)
        .setValue("etcdhbase.m0.record-categories", String.join(",", categories))
        .setValue("etcdhbase.m5.compaction-format", "1").build();
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
    requireKnownAuxiliaryFormat(descriptor);
    TableDescriptor upgraded = TableDescriptorBuilder.newBuilder(descriptor)
        .setValue("etcdhbase.m0.record-categories", auxiliaryRecordCategories(descriptor))
        .setValue("etcdhbase.m4.auxiliary-format", AUXILIARY_FORMAT_VALUE)
        .build();
    if (!supportsAuxiliary(upgraded)) {
      throw new IllegalStateException("M4 auxiliary descriptor capability could not be constructed");
    }
    return upgraded;
  }

  /** Adds the J41 binding capability without changing table rows or family policy. */
  public TableDescriptor withLeaseBindingCapability(TableDescriptor descriptor) {
    validate(descriptor);
    if (supportsLeaseBindings(descriptor)) return descriptor;
    requireKnownAuxiliaryFormat(descriptor);
    if (!supportsAuxiliary(descriptor)) {
      throw new IllegalStateException("J41 binding capability requires M4 auxiliary records");
    }
    TableDescriptor upgraded = TableDescriptorBuilder.newBuilder(descriptor)
        .setValue("etcdhbase.m4.auxiliary-format", LEASE_BINDING_FORMAT_VALUE)
        .build();
    if (!supportsLeaseBindings(upgraded)) {
      throw new IllegalStateException("J41 binding descriptor capability could not be constructed");
    }
    return upgraded;
  }

  /** Adds the J43 checkpoint record category and format without changing table rows or family policy. */
  public TableDescriptor withLeaseExpiryCheckpointCapability(TableDescriptor descriptor) {
    validate(descriptor);
    if (supportsLeaseExpiryCheckpoints(descriptor)) return descriptor;
    requireKnownAuxiliaryFormat(descriptor);
    if (!supportsLeaseBindings(descriptor)) {
      throw new IllegalStateException("J43 expiry checkpoints require J41 Lease binding capability");
    }
    var categories = new LinkedHashSet<>(Arrays.asList(
        descriptor.getValue("etcdhbase.m0.record-categories").split(",", -1)));
    categories.addAll(LEASE_EXPIRY_RECORD_CATEGORIES);
    TableDescriptor upgraded = TableDescriptorBuilder.newBuilder(descriptor)
        .setValue("etcdhbase.m0.record-categories", String.join(",", categories))
        .setValue("etcdhbase.m4.auxiliary-format", LEASE_EXPIRY_FORMAT_VALUE)
        .build();
    if (!supportsLeaseExpiryCheckpoints(upgraded)) {
      throw new IllegalStateException("J43 expiry descriptor capability could not be constructed");
    }
    return upgraded;
  }

  private static void requireKnownAuxiliaryFormat(TableDescriptor descriptor) {
    String format = descriptor.getValue("etcdhbase.m4.auxiliary-format");
    if (format != null && !AUXILIARY_FORMAT_VALUE.equals(format)
        && !LEASE_BINDING_FORMAT_VALUE.equals(format)
        && !LEASE_EXPIRY_FORMAT_VALUE.equals(format)) {
      throw new IllegalStateException("system table declares an unsupported M4 format");
    }
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
