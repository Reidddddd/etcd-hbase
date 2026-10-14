package io.etcdhbase.admin;

import io.etcdhbase.server.config.HBaseBackendScope;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration.AuthenticationMode;
import io.etcdhbase.server.config.RuntimeConfiguration.ColumnFamilySchema;
import io.etcdhbase.server.config.RuntimeConfiguration.HBaseProfile;
import io.etcdhbase.server.config.RuntimeConfiguration.HistoryLayout;
import io.etcdhbase.server.config.RuntimeConfiguration.HistoryLayoutCandidate;
import io.etcdhbase.server.config.RuntimeConfiguration.RecordCategory;
import io.etcdhbase.server.config.RuntimeConfiguration.SaltProfile;
import io.etcdhbase.server.config.RuntimeConfiguration.TableGroup;
import io.etcdhbase.server.config.RuntimeConfiguration.TableSchemaCandidate;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/** Immutable, credential-free view of one explicitly selected table-group candidate. */
public record TableGroupInitializationPlan(
    String tableGroupId,
    String identityProviderId,
    String namespace,
    String hbaseProfileId,
    String quorum,
    int clientPort,
    AuthenticationMode authenticationMode,
    int schemaVersion,
    String saltProfileId,
    String saltStrategyId,
    int saltEncodingVersion,
    int saltBucketCount,
    int saltWidthBytes,
    HistoryLayout historyLayout,
    List<String> configuredNamespaceTableNames,
    List<TablePlan> tables,
    String fingerprint) {

  public TableGroupInitializationPlan {
    configuredNamespaceTableNames = List.copyOf(configuredNamespaceTableNames);
    tables = List.copyOf(tables);
  }

  static TableGroupInitializationPlan create(
      RuntimeConfiguration configuration,
      TableGroup tableGroup,
      HistoryLayout selectedLayout) {
    HBaseProfile hbaseProfile =
        configuration.hbaseProfiles().stream()
            .filter(profile -> profile.id().equals(tableGroup.hbaseProfileId()))
            .findFirst()
            .orElseThrow(
                () -> new IllegalArgumentException("table group references an unknown HBase profile"));
    SaltProfile saltProfile =
        configuration.saltProfiles().stream()
            .filter(profile -> profile.id().equals(tableGroup.saltProfileId()))
            .findFirst()
            .orElseThrow(
                () -> new IllegalArgumentException("table group references an unknown salt profile"));
    HistoryLayoutCandidate selectedCandidate =
        tableGroup.historyCandidates().stream()
            .filter(candidate -> candidate.historyLayout() == selectedLayout)
            .findFirst()
            .orElseThrow(
                () -> new IllegalArgumentException("selected history layout is not configured"));

    List<TablePlan> tables = new ArrayList<>();
    tables.add(TablePlan.from(tableGroup.systemTable()));
    for (TableSchemaCandidate candidate : selectedCandidate.tables()) {
      tables.add(TablePlan.from(candidate));
    }
    List<TablePlan> immutableTables = List.copyOf(tables);
    List<String> configuredNamespaceTableNames =
        configuredNamespaceTableNames(configuration, tableGroup, hbaseProfile, selectedLayout);
    String fingerprint =
        fingerprint(
            tableGroup,
            hbaseProfile,
            saltProfile,
            selectedLayout,
            configuredNamespaceTableNames,
            immutableTables);
    return new TableGroupInitializationPlan(
        tableGroup.id(),
        tableGroup.identityProviderId(),
        hbaseProfile.namespace(),
        hbaseProfile.id(),
        hbaseProfile.quorum(),
        hbaseProfile.clientPort(),
        hbaseProfile.authenticationMode(),
        tableGroup.schemaVersion(),
        saltProfile.id(),
        saltProfile.strategyId(),
        saltProfile.encodingVersion(),
        saltProfile.bucketCount(),
        saltProfile.saltWidthBytes(),
        selectedLayout,
        configuredNamespaceTableNames,
        immutableTables,
        fingerprint);
  }

  private static List<String> configuredNamespaceTableNames(
      RuntimeConfiguration configuration,
      TableGroup selectedGroup,
      HBaseProfile selectedProfile,
      HistoryLayout selectedLayout) {
    HBaseBackendScope selectedScope = HBaseBackendScope.of(selectedProfile);
    Set<String> tableNames = new TreeSet<>();
    for (TableGroup group : configuration.tableGroups()) {
      HBaseProfile profile =
          configuration.hbaseProfiles().stream()
              .filter(candidate -> candidate.id().equals(group.hbaseProfileId()))
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException(
                  "table group references an unknown HBase profile"));
      if (!selectedScope.equals(HBaseBackendScope.of(profile))) {
        continue;
      }
      tableNames.add(group.systemTable().tableName());
      if (group.id().equals(selectedGroup.id())) {
        HistoryLayoutCandidate selectedCandidate =
            group.historyCandidates().stream()
                .filter(candidate -> candidate.historyLayout() == selectedLayout)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                    "selected history layout is not configured"));
        selectedCandidate.tables().stream()
            .map(TableSchemaCandidate::tableName)
            .forEach(tableNames::add);
      } else {
        if (group.historyLayout() == HistoryLayout.UNDECIDED) {
          group.historyCandidates().stream()
              .flatMap(candidate -> candidate.tables().stream())
              .map(TableSchemaCandidate::tableName)
              .forEach(tableNames::add);
        } else {
          group.historyCandidates().stream()
              .filter(candidate -> candidate.historyLayout() == group.historyLayout())
              .flatMap(candidate -> candidate.tables().stream())
              .map(TableSchemaCandidate::tableName)
              .forEach(tableNames::add);
        }
      }
    }
    return List.copyOf(tableNames);
  }

  private static String fingerprint(
      TableGroup tableGroup,
      HBaseProfile hbaseProfile,
      SaltProfile saltProfile,
      HistoryLayout selectedLayout,
      List<String> configuredNamespaceTableNames,
      List<TablePlan> tables) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      update(digest, tableGroup.id());
      update(digest, tableGroup.schemaVersion());
      update(digest, tableGroup.identityProviderId());
      update(digest, hbaseProfile.id());
      update(digest, hbaseProfile.namespace());
      update(digest, hbaseProfile.quorum());
      update(digest, hbaseProfile.clientPort());
      update(digest, hbaseProfile.authenticationMode().name());
      update(digest, Objects.toString(hbaseProfile.credentialReference(), ""));
      update(digest, saltProfile.id());
      update(digest, saltProfile.strategyId());
      update(digest, saltProfile.encodingVersion());
      update(digest, saltProfile.bucketCount());
      update(digest, saltProfile.saltWidthBytes());
      update(digest, selectedLayout.name());
      for (String tableName : configuredNamespaceTableNames) {
        update(digest, tableName);
      }
      for (TablePlan table : tables.stream().sorted(Comparator.comparing(TablePlan::name)).toList()) {
        update(digest, table.name());
        update(digest, table.schemaVersion());
        update(digest, table.profileVersion());
        for (String category : table.recordCategories()) {
          update(digest, category);
        }
        for (ColumnFamilyPlan family : table.columnFamilies()) {
          update(digest, family.name());
          update(digest, family.minVersions());
          update(digest, family.maxVersions());
          update(digest, family.ttlSeconds());
          update(digest, family.keepDeletedCells());
        }
        for (String splitKey : table.splitKeysHex()) {
          update(digest, splitKey);
        }
      }
      return HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private static void update(MessageDigest digest, String value) {
    digest.update(value.getBytes(StandardCharsets.UTF_8));
    digest.update((byte) 0);
  }

  private static void update(MessageDigest digest, int value) {
    update(digest, Integer.toString(value));
  }

  public record TablePlan(
      String name,
      int schemaVersion,
      int profileVersion,
      List<ColumnFamilyPlan> columnFamilies,
      List<String> splitKeysHex,
      List<String> recordCategories) {

    public TablePlan {
      columnFamilies =
          columnFamilies.stream()
              .sorted(Comparator.comparing(ColumnFamilyPlan::name))
              .toList();
      splitKeysHex = splitKeysHex.stream().map(value -> value.toLowerCase(Locale.ROOT)).toList();
      recordCategories = recordCategories.stream().sorted().toList();
    }

    private static TablePlan from(TableSchemaCandidate table) {
      return new TablePlan(
          table.tableName(),
          table.schemaVersion(),
          table.profileVersion(),
          table.columnFamilies().stream().map(ColumnFamilyPlan::from).toList(),
          table.splitKeysHex(),
          table.recordCategories().stream().map(RecordCategory::name).toList());
    }
  }

  public record ColumnFamilyPlan(
      String name,
      int minVersions,
      int maxVersions,
      int ttlSeconds,
      String keepDeletedCells) {

    private static ColumnFamilyPlan from(ColumnFamilySchema schema) {
      return new ColumnFamilyPlan(
          schema.name(),
          schema.minVersions(),
          schema.maxVersions(),
          schema.ttlSeconds(),
          schema.keepDeletedCells().name());
    }
  }
}
