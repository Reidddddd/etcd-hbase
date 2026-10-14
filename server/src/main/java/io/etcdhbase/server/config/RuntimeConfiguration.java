package io.etcdhbase.server.config;

import java.util.List;

/** Immutable server configuration model validated by {@link RuntimeConfigurationLoader}. */
public record RuntimeConfiguration(
    int schemaVersion,
    List<Listener> listeners,
    List<HBaseProfile> hbaseProfiles,
    List<SaltProfile> saltProfiles,
    List<TableGroup> tableGroups,
    ResourceBudgets resources) {

  public RuntimeConfiguration {
    listeners = immutableOrEmpty(listeners);
    hbaseProfiles = immutableOrEmpty(hbaseProfiles);
    saltProfiles = immutableOrEmpty(saltProfiles);
    tableGroups = immutableOrEmpty(tableGroups);
  }

  private static <T> List<T> immutableOrEmpty(List<T> values) {
    return values == null ? List.of() : List.copyOf(values);
  }

  public record Listener(String id, String bindHost, int port, String tableGroupId) {}

  public record HBaseProfile(
      String id,
      String quorum,
      int clientPort,
      String namespace,
      AuthenticationMode authenticationMode,
      String credentialReference) {}

  public record SaltProfile(
      String id,
      String strategyId,
      int encodingVersion,
      int bucketCount,
      int saltWidthBytes,
      List<String> splitKeysHex) {
    public SaltProfile {
      splitKeysHex = immutableOrEmpty(splitKeysHex);
    }
  }

  public record TableGroup(
      String id,
      int schemaVersion,
      String hbaseProfileId,
      String saltProfileId,
      String identityProviderId,
      HistoryLayout historyLayout,
      TableSchemaCandidate systemTable,
      List<HistoryLayoutCandidate> historyCandidates) {
    public TableGroup {
      historyCandidates = immutableOrEmpty(historyCandidates);
    }
  }

  /** Physical table schema used by an unselected M0 table-layout candidate. */
  public record TableSchemaCandidate(
      String tableName,
      int schemaVersion,
      int profileVersion,
      List<ColumnFamilySchema> columnFamilies,
      List<String> splitKeysHex,
      List<RecordCategory> recordCategories,
      List<IdentityPermission> identityPermissions) {
    public TableSchemaCandidate {
      columnFamilies = immutableOrEmpty(columnFamilies);
      splitKeysHex = immutableOrEmpty(splitKeysHex);
      recordCategories = immutableOrEmpty(recordCategories);
      identityPermissions = immutableOrEmpty(identityPermissions);
    }
  }

  /** One of the two history layouts retained for prototype comparison. */
  public record HistoryLayoutCandidate(
      HistoryLayout historyLayout, List<TableSchemaCandidate> tables) {
    public HistoryLayoutCandidate {
      tables = immutableOrEmpty(tables);
    }
  }

  /** Logical HBase operation permissions for a role reference, not a target account name. */
  public record IdentityPermission(String identityRef, List<TablePermission> permissions) {
    public IdentityPermission {
      permissions = immutableOrEmpty(permissions);
    }
  }

  /**
   * HBase column-family settings kept in the candidate config until target inputs are known.
   * {@code ttlSeconds == -1} means no expiry in this configuration schema; an HBase adapter must
   * translate it to the pinned client's unlimited-TTL value.
   */
  public record ColumnFamilySchema(
      String name,
      int minVersions,
      int maxVersions,
      int ttlSeconds,
      KeepDeletedCells keepDeletedCells) {}

  public record ResourceBudgets(
      int maxConfigBytes,
      int maxConcurrentRequests,
      int maxScannersPerTable,
      int requestDeadlineMs,
      long maxRpcBytes,
      long maxRequestBytes,
      int maxQueuedRequests) {}

  public enum AuthenticationMode {
    DEV_NO_AUTH,
    CREDENTIAL_REFERENCE
  }

  public enum HistoryLayout {
    UNDECIDED,
    SEPARATE_HISTORY_TABLE,
    NATIVE_CELL_VERSIONS
  }

  public enum KeepDeletedCells {
    FALSE,
    TRUE,
    TTL
  }

  public enum RecordCategory {
    CURRENT,
    HISTORY,
    DECISION,
    EVENT,
    LEASE,
    AUTH,
    CONTROL
  }

  public enum TablePermission {
    READ,
    WRITE,
    CREATE,
    ADMIN
  }
}
