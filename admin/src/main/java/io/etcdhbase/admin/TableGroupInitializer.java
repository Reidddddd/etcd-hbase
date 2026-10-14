package io.etcdhbase.admin;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/** Creates only an explicitly confirmed table group and resumes matching partial creations. */
final class TableGroupInitializer {
  private static final String GROUP_ID = "etcdhbase.m0.table-group-id";
  private static final String SCHEMA_VERSION = "etcdhbase.m0.schema-version";
  private static final String TABLE_SCHEMA_VERSION = "etcdhbase.m0.table-schema-version";
  private static final String PROFILE_VERSION = "etcdhbase.m0.profile-version";
  private static final String SALT_PROFILE_ID = "etcdhbase.m0.salt-profile-id";
  private static final String HISTORY_LAYOUT = "etcdhbase.m0.history-layout";
  private static final String PLAN_FINGERPRINT = "etcdhbase.m0.plan-fingerprint";
  private static final String RECORD_CATEGORIES = "etcdhbase.m0.record-categories";
  private static final String SPLIT_KEYS_SHA256 = "etcdhbase.m0.split-keys-sha256";

  private TableGroupInitializer() {}

  static InitializationResult initialize(
      TableGroupInitializationPlan plan, TableGroupAdmin admin) throws Exception {
    List<PlannedTable> plannedTables =
        plan.tables().stream().map(table -> plannedTable(plan, table)).toList();
    Set<TableName> configuredNamespaceNames =
        plan.configuredNamespaceTableNames().stream()
            .map(name -> TableName.valueOf(plan.namespace(), name))
            .collect(Collectors.toUnmodifiableSet());

    List<String> namespaces = admin.listNamespaces();
    boolean namespaceCreated = !namespaces.contains(plan.namespace());
    if (namespaceCreated) {
      admin.createNamespace(plan.namespace());
    }

    List<TableName> actualNames = admin.listTableNames(plan.namespace());
    List<TableName> unrelatedTables =
        actualNames.stream().filter(name -> !configuredNamespaceNames.contains(name)).toList();
    if (!unrelatedTables.isEmpty()) {
      throw new IllegalArgumentException(
          "namespace contains tables outside the configured table scope; refusing to modify it");
    }

    List<PlannedTable> existingTables = new ArrayList<>();
    for (PlannedTable planned : plannedTables) {
      TableName tableName = planned.descriptor().getTableName();
      if (actualNames.contains(tableName)) {
        verifyExistingTableDescriptor(planned, admin);
        existingTables.add(planned);
      }
    }

    if (existingTables.size() == plannedTables.size()) {
      return new InitializationResult(
          plan.namespace(), List.of(), namespaceCreated, true, plan.fingerprint());
    }
    for (PlannedTable existing : existingTables) {
      verifyExistingRegionBoundaries(existing, admin);
      if (!admin.isEmpty(existing.descriptor().getTableName())) {
        throw new IllegalArgumentException(
            "a partial table group already contains data; refusing to add more tables");
      }
    }

    List<TableName> createdTables = new ArrayList<>();
    try {
      for (PlannedTable planned : plannedTables) {
        TableName tableName = planned.descriptor().getTableName();
        if (!actualNames.contains(tableName)) {
          admin.createTable(planned.descriptor(), planned.splitKeys());
          createdTables.add(tableName);
        }
      }
    } catch (Exception failure) {
      throw new PartialInitializationException(
          plan.namespace(), createdTables, plan.fingerprint(), failure);
    }
    return new InitializationResult(
        plan.namespace(),
        createdTables,
        namespaceCreated,
        false,
        plan.fingerprint());
  }

  private static void verifyExistingTableDescriptor(PlannedTable planned, TableGroupAdmin admin)
      throws Exception {
    TableName tableName = planned.descriptor().getTableName();
    TableDescriptor actual = admin.getDescriptor(tableName);
    if (!matchesPlannedDescriptor(planned.descriptor(), actual)) {
      throw new IllegalArgumentException(
          "an existing table does not match the confirmed schema and profile");
    }
  }

  private static void verifyExistingRegionBoundaries(PlannedTable planned, TableGroupAdmin admin)
      throws Exception {
    TableName tableName = planned.descriptor().getTableName();
    List<byte[]> expectedStartKeys = new ArrayList<>();
    expectedStartKeys.add(new byte[0]);
    expectedStartKeys.addAll(Arrays.asList(planned.splitKeys()));
    expectedStartKeys.sort(Bytes.BYTES_COMPARATOR);
    List<byte[]> actualStartKeys =
        admin.getRegions(tableName).stream()
            .map(RegionInfo::getStartKey)
            .sorted(Bytes.BYTES_COMPARATOR)
            .toList();
    if (expectedStartKeys.size() != actualStartKeys.size()) {
      throw new IllegalArgumentException("an existing table has different region split boundaries");
    }
    for (int index = 0; index < expectedStartKeys.size(); index++) {
      if (!Arrays.equals(expectedStartKeys.get(index), actualStartKeys.get(index))) {
        throw new IllegalArgumentException(
            "an existing table has different region split boundaries");
      }
    }
  }

  private static boolean matchesPlannedDescriptor(TableDescriptor expected, TableDescriptor actual) {
    if (!expected.getTableName().equals(actual.getTableName())
        || expected.getColumnFamilyCount() != actual.getColumnFamilyCount()) {
      return false;
    }

    // HBase may add server-owned defaults; require every planned attribute but allow extras.
    for (Map.Entry<Bytes, Bytes> attribute : expected.getValues().entrySet()) {
      if (!attribute.getValue().equals(actual.getValue(attribute.getKey()))) {
        return false;
      }
    }

    for (ColumnFamilyDescriptor expectedFamily : expected.getColumnFamilies()) {
      ColumnFamilyDescriptor actualFamily = actual.getColumnFamily(expectedFamily.getName());
      if (actualFamily == null
          || expectedFamily.getMinVersions() != actualFamily.getMinVersions()
          || expectedFamily.getMaxVersions() != actualFamily.getMaxVersions()
          || expectedFamily.getTimeToLive() != actualFamily.getTimeToLive()
          || expectedFamily.getKeepDeletedCells() != actualFamily.getKeepDeletedCells()) {
        return false;
      }
    }
    return true;
  }

  private static PlannedTable plannedTable(
      TableGroupInitializationPlan plan, TableGroupInitializationPlan.TablePlan tablePlan) {
    TableName tableName = TableName.valueOf(plan.namespace(), tablePlan.name());
    TableDescriptorBuilder descriptor = TableDescriptorBuilder.newBuilder(tableName);
    descriptor
        .setValue(GROUP_ID, plan.tableGroupId())
        .setValue(SCHEMA_VERSION, Integer.toString(plan.schemaVersion()))
        .setValue(TABLE_SCHEMA_VERSION, Integer.toString(tablePlan.schemaVersion()))
        .setValue(PROFILE_VERSION, Integer.toString(tablePlan.profileVersion()))
        .setValue(SALT_PROFILE_ID, plan.saltProfileId())
        .setValue(HISTORY_LAYOUT, plan.historyLayout().name())
        .setValue(PLAN_FINGERPRINT, plan.fingerprint())
        .setValue(RECORD_CATEGORIES, String.join(",", tablePlan.recordCategories()));

    for (TableGroupInitializationPlan.ColumnFamilyPlan family : tablePlan.columnFamilies()) {
      int ttlSeconds =
          family.ttlSeconds() == -1 ? HConstants.FOREVER : family.ttlSeconds();
      ColumnFamilyDescriptor familyDescriptor =
          ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family.name()))
              .setMinVersions(family.minVersions())
              .setMaxVersions(family.maxVersions())
              .setTimeToLive(ttlSeconds)
              .setKeepDeletedCells(KeepDeletedCells.valueOf(family.keepDeletedCells()))
              .build();
      descriptor.setColumnFamily(familyDescriptor);
    }

    byte[][] splitKeys =
        tablePlan.splitKeysHex().stream().map(HexFormat.of()::parseHex).toArray(byte[][]::new);
    descriptor.setValue(SPLIT_KEYS_SHA256, splitKeysFingerprint(splitKeys));
    return new PlannedTable(descriptor.build(), splitKeys);
  }

  private static String splitKeysFingerprint(byte[][] splitKeys) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      for (byte[] splitKey : splitKeys) {
        digest.update(splitKey);
        digest.update((byte) 0);
      }
      return HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  record InitializationResult(
      String namespace,
      List<TableName> createdTables,
      boolean namespaceCreated,
      boolean alreadyInitialized,
      String planFingerprint) {
    InitializationResult {
      createdTables = List.copyOf(createdTables);
    }
  }

  static final class PartialInitializationException extends Exception {
    private final String namespace;
    private final List<TableName> createdTables;
    private final String planFingerprint;

    private PartialInitializationException(
        String namespace,
        List<TableName> createdTables,
        String planFingerprint,
        Throwable cause) {
      super("table creation failed; rerun the same confirmed plan to continue", cause);
      this.namespace = namespace;
      this.createdTables = List.copyOf(createdTables);
      this.planFingerprint = planFingerprint;
    }

    String namespace() {
      return namespace;
    }

    List<TableName> createdTables() {
      return createdTables;
    }

    String planFingerprint() {
      return planFingerprint;
    }
  }

  private record PlannedTable(TableDescriptor descriptor, byte[][] splitKeys) {}
}
