package io.etcdhbase.server.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.server.config.RuntimeConfiguration.AuthenticationMode;
import io.etcdhbase.server.config.RuntimeConfiguration.HistoryLayout;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RuntimeConfigurationLoaderTest {
  private static final String VALID_YAML = readCandidateYaml();

  private final RuntimeConfigurationLoader loader =
      new RuntimeConfigurationLoader(
          Set.of("dev-no-auth", "backend-hbase"), Set.of("crc32-bucket-v1"), Map::of);

  @Test
  void loadsCandidateAndRetainsUnresolvedHistoryLayout() {
    RuntimeConfiguration configuration = loader.load(bytes(VALID_YAML));

    assertEquals(1, configuration.schemaVersion());
    assertEquals("127.0.0.1", configuration.listeners().getFirst().bindHost());
    assertEquals(
        AuthenticationMode.DEV_NO_AUTH,
        configuration.hbaseProfiles().getFirst().authenticationMode());
    var tableGroup = configuration.tableGroups().getFirst();
    assertEquals(HistoryLayout.UNDECIDED, tableGroup.historyLayout());
    assertEquals("system", tableGroup.systemTable().tableName());
    assertEquals(2, tableGroup.historyCandidates().size());
    var separateHistory = tableGroup.historyCandidates().getFirst();
    assertEquals(HistoryLayout.SEPARATE_HISTORY_TABLE, separateHistory.historyLayout());
    assertEquals(2, separateHistory.tables().size());
    var historyTable =
        separateHistory.tables().stream()
            .filter(table -> table.recordCategories().contains(RuntimeConfiguration.RecordCategory.HISTORY))
            .findFirst()
            .orElseThrow();
    assertEquals("history", historyTable.tableName());
    assertEquals(1, historyTable.columnFamilies().getFirst().maxVersions());
    var nativeVersions = tableGroup.historyCandidates().getLast();
    assertEquals(HistoryLayout.NATIVE_CELL_VERSIONS, nativeVersions.historyLayout());
    assertEquals(1, nativeVersions.tables().size());
    assertEquals("kv", nativeVersions.tables().getFirst().tableName());
    assertEquals(64, nativeVersions.tables().getFirst().columnFamilies().getFirst().maxVersions());
    assertEquals(-1, nativeVersions.tables().getFirst().columnFamilies().getFirst().ttlSeconds());
    assertEquals(15, nativeVersions.tables().getFirst().splitKeysHex().size());
    assertEquals(2, nativeVersions.tables().getFirst().identityPermissions().size());
    assertEquals(16, configuration.saltProfiles().getFirst().bucketCount());
    assertEquals(65536, configuration.resources().maxConfigBytes());
    assertThrows(
        UnsupportedOperationException.class,
        () -> configuration.listeners().add(configuration.listeners().getFirst()));
  }

  @Test
  void loadsTheCheckedInCandidateProfile() throws IOException {
    try (var input = getClass().getResourceAsStream("/runtime-candidate.yaml")) {
      assertNotNull(input);
      assertEquals("local", loader.load(input).tableGroups().getFirst().id());
    }
  }

  @Test
  void loadsBothCheckedInHistoryLayoutExamples() throws IOException {
    for (String example : List.of("history-table.yaml", "native-cell-versions.yaml")) {
      try (var input = getClass().getResourceAsStream("/m0-examples/" + example)) {
        assertNotNull(input, example);
        var group = loader.load(input).tableGroups().getFirst();
        HistoryLayout expectedLayout =
            example.equals("history-table.yaml")
                ? HistoryLayout.SEPARATE_HISTORY_TABLE
                : HistoryLayout.NATIVE_CELL_VERSIONS;
        assertEquals(expectedLayout, group.historyLayout(), example);
      }
    }
  }

  @Test
  void loadsLoopbackRandomPortSkeletonWithExplicitDevelopmentAuthentication() throws IOException {
    try (var input = getClass().getResourceAsStream("/m0-examples/server-skeleton.yaml")) {
      assertNotNull(input);
      RuntimeConfiguration configuration = loader.load(input);

      assertEquals(0, configuration.listeners().getFirst().port());
      assertEquals("127.0.0.1", configuration.listeners().getFirst().bindHost());
      assertEquals(
          AuthenticationMode.DEV_NO_AUTH,
          configuration.hbaseProfiles().getFirst().authenticationMode());
    }
  }

  @Test
  void acceptsControlledCredentialReferenceAndRequiresAuthenticationOutsideLoopback() {
    String securedYaml =
        VALID_YAML
            .replace("127.0.0.1", "service.example")
            .replace(
                "authentication_mode: DEV_NO_AUTH",
                "authentication_mode: CREDENTIAL_REFERENCE\n"
                    + "    credential_reference: env://HBASE_CREDENTIALS")
            .replace("identity_provider_id: dev-no-auth", "identity_provider_id: backend-hbase");

    RuntimeConfiguration configuration = loader.load(bytes(securedYaml));

    assertEquals("env://HBASE_CREDENTIALS", configuration.hbaseProfiles().getFirst().credentialReference());
  }

  @Test
  void rejectsWildcardTokenInsteadOfAcceptingAnUnbindableHost() {
    String securedYaml =
        VALID_YAML.replace(
            "authentication_mode: DEV_NO_AUTH",
            "authentication_mode: CREDENTIAL_REFERENCE\n"
                + "    credential_reference: env://HBASE_CREDENTIALS")
            .replace("identity_provider_id: dev-no-auth", "identity_provider_id: backend-hbase");

    assertRejected(
        securedYaml.replace("bind_host: 127.0.0.1", "bind_host: \"*\""),
        "listeners[0].bind_host",
        null);
    assertRejected(
        securedYaml.replace("quorum: 127.0.0.1", "quorum: \"*\""),
        "hbase_profiles.quorum",
        null);
  }

  @Test
  void requiresCredentialReferenceWhenCredentialAuthenticationIsSelected() {
    assertRejected(
        VALID_YAML.replace("    authentication_mode: DEV_NO_AUTH\n", ""),
        "authentication_mode",
        null);
    assertRejected(
        VALID_YAML.replace(
            "authentication_mode: DEV_NO_AUTH", "authentication_mode: CREDENTIAL_REFERENCE"),
        "credential_reference",
        null);
  }

  @Test
  void rejectsMismatchedAuthenticationModeAndIdentityProvider() {
    assertRejected(
        VALID_YAML.replace(
            "identity_provider_id: dev-no-auth", "identity_provider_id: backend-hbase"),
        "table_groups[0].identity_provider_id",
        null);

    String credentialProfile =
        VALID_YAML.replace(
            "authentication_mode: DEV_NO_AUTH",
            "authentication_mode: CREDENTIAL_REFERENCE\n"
                + "    credential_reference: env://HBASE_CREDENTIALS");
    assertRejected(
        credentialProfile,
        "table_groups[0].identity_provider_id",
        null);
  }

  @Test
  void rejectsNullCredentialReferenceEvenWhenDevelopmentAuthenticationIsSelected() {
    assertRejected(
        VALID_YAML.replace(
            "authentication_mode: DEV_NO_AUTH",
            "authentication_mode: DEV_NO_AUTH\n    credential_reference: null"),
        "credential_reference",
        null);
  }

  @Test
  void rejectsInvalidPortWithFieldPath() {
    assertRejected(VALID_YAML.replace("port: 2379", "port: 65536"), "listeners[0].port", null);
  }

  @Test
  void acceptsLoopbackRandomPortAndDoesNotTreatEphemeralPortsAsConflicts() {
    String twoEphemeralListeners =
        VALID_YAML.replace("port: 2379", "port: 0")
            .replace(
                "    table_group_id: local\nhbase_profiles:",
                "    table_group_id: local\n"
                    + "  - id: second-listener\n"
                    + "    bind_host: 127.0.0.1\n"
                    + "    port: 0\n"
                    + "    table_group_id: local\n"
                    + "hbase_profiles:");

    RuntimeConfiguration configuration = loader.load(bytes(twoEphemeralListeners));

    assertEquals(0, configuration.listeners().getFirst().port());
    assertEquals(0, configuration.listeners().getLast().port());
  }

  @Test
  void rejectsRandomPortForNonLoopbackListener() {
    String secured =
        VALID_YAML.replace(
            "authentication_mode: DEV_NO_AUTH",
            "authentication_mode: CREDENTIAL_REFERENCE\n"
                + "    credential_reference: env://HBASE_CREDENTIALS")
            .replace("identity_provider_id: dev-no-auth", "identity_provider_id: backend-hbase");
    assertRejected(
        secured.replace("127.0.0.1", "service.example").replace("port: 2379", "port: 0"),
        "port 0 requires a loopback listener",
        null);
  }

  @Test
  void appliesDocumentedEnvironmentOverridesAfterReadingTheFile() {
    RuntimeConfiguration configuration =
        loader.load(
            bytes(VALID_YAML),
            Map.of(
                "ETCD_HBASE_LISTENER_PORT", "0",
                "ETCD_HBASE_REQUEST_DEADLINE_MS", "9000",
                "ETCD_HBASE_MAX_REQUEST_BYTES", "8388608",
                "ETCD_HBASE_MAX_RPC_BYTES", "4194304"));

    assertEquals(0, configuration.listeners().getFirst().port());
    assertEquals(9000, configuration.resources().requestDeadlineMs());
    assertEquals(8_388_608, configuration.resources().maxRequestBytes());
    assertEquals(4_194_304, configuration.resources().maxRpcBytes());
  }

  @Test
  void ignoresKnownBuildEnvironmentVariablesButKeepsRuntimeOverrideValidation() {
    RuntimeConfigurationLoader buildEnvironmentLoader =
        new RuntimeConfigurationLoader(
            Set.of("dev-no-auth", "backend-hbase"),
            Set.of("crc32-bucket-v1"),
            () ->
                Map.of(
                    "ETCD_HBASE_JDK21_HOME", "/opt/temurin-21",
                    "ETCD_HBASE_MAVEN_REPOSITORY", "/tmp/maven-repository"));

    RuntimeConfiguration configuration = buildEnvironmentLoader.load(bytes(VALID_YAML));

    assertEquals("local", configuration.tableGroups().getFirst().id());
  }

  @Test
  void defaultEnvironmentEntryPointLoadsWithTheProcessEnvironment() {
    RuntimeConfigurationLoader defaultLoader =
        new RuntimeConfigurationLoader(
            Set.of("dev-no-auth", "backend-hbase"), Set.of("crc32-bucket-v1"));

    assertEquals("local", defaultLoader.load(bytes(VALID_YAML)).tableGroups().getFirst().id());
  }

  @Test
  void rejectsUnknownAndMalformedEnvironmentOverridesWithoutEchoingValues() {
    String sentinel = "do-not-disclose-this-env-value";
    RuntimeConfigurationException unknown =
        assertThrows(
            RuntimeConfigurationException.class,
            () -> loader.load(bytes(VALID_YAML), Map.of("ETCD_HBASE_UNKNOWN_TOKEN", sentinel)));
    assertTrue(unknown.getMessage().contains("unsupported ETCD_HBASE_ override"));
    assertFalse(unknown.getMessage().contains(sentinel));
    assertNull(unknown.getCause());

    RuntimeConfigurationException malformed =
        assertThrows(
            RuntimeConfigurationException.class,
            () -> loader.load(bytes(VALID_YAML), Map.of("ETCD_HBASE_LISTENER_PORT", sentinel)));
    assertTrue(malformed.getMessage().contains("listeners[0].port"));
    assertFalse(malformed.getMessage().contains(sentinel));
    assertNull(malformed.getCause());
  }

  @Test
  void boundsEffectiveFileAndEnvironmentConfigurationSize() {
    RuntimeConfigurationException oversized =
        assertThrows(
            RuntimeConfigurationException.class,
            () ->
                loader.load(
                    bytes(VALID_YAML),
                    Map.of(
                        "ETCD_HBASE_LISTENER_BIND_HOST",
                        "x".repeat(RuntimeConfigurationLoader.ABSOLUTE_MAX_CONFIG_BYTES))));
    assertTrue(oversized.getMessage().contains("file and environment overrides"));
    assertFalse(oversized.getMessage().contains("x".repeat(64)));
    assertNull(oversized.getCause());

    RuntimeConfigurationException tooSmallBudget =
        assertThrows(
            RuntimeConfigurationException.class,
            () -> loader.load(bytes(VALID_YAML), Map.of("ETCD_HBASE_MAX_CONFIG_BYTES", "1")));
    assertTrue(tooSmallBudget.getMessage().contains("resources.max_config_bytes"));
    assertNull(tooSmallBudget.getCause());
  }

  @Test
  void rejectsDuplicateListenerAddress() {
    String duplicate =
        VALID_YAML.replace(
            "    table_group_id: local\nhbase_profiles:",
            "    table_group_id: local\n"
                + "  - id: second-listener\n"
                + "    bind_host: 127.0.0.1\n"
                + "    port: 2379\n"
                + "    table_group_id: local\n"
                + "hbase_profiles:");

    assertRejected(duplicate, "listeners[1].port", null);
  }

  @Test
  void rejectsLocalhostListenerAliasesWithoutResolvingHostnames() {
    for (String alias : List.of("127.0.0.1", "::1")) {
      String localhostListener = VALID_YAML.replace("bind_host: 127.0.0.1", "bind_host: localhost");
      String duplicate =
          localhostListener.replace(
              "    table_group_id: local\nhbase_profiles:",
              "    table_group_id: local\n"
                  + "  - id: loopback-alias-listener\n"
                  + "    bind_host: "
                  + alias
                  + "\n"
                  + "    port: 2379\n"
                  + "    table_group_id: local\n"
                  + "hbase_profiles:");

      assertRejected(duplicate, "listeners[1].port", null);
    }
  }

  @Test
  void rejectsEquivalentIpv6ListenerAddressesAcrossBracketAndCompressionForms() {
    for (List<String> hostPair :
        List.of(List.of("::1", "\"[::1]\""), List.of("0:0:0:0:0:0:0:1", "::1"))) {
      String firstHost = hostPair.getFirst();
      String secondHost = hostPair.getLast();
      String firstListener = VALID_YAML.replace("127.0.0.1", firstHost);
      String duplicate =
          firstListener.replace(
              "    table_group_id: local\nhbase_profiles:",
              "    table_group_id: local\n"
                  + "  - id: equivalent-ipv6-listener\n"
                  + "    bind_host: "
                  + secondHost
                  + "\n"
                  + "    port: 2379\n"
                  + "    table_group_id: local\n"
                  + "hbase_profiles:");

      assertRejected(duplicate, "listeners[1].port", null);
    }
  }

  @Test
  void rejectsEquivalentListenerAddressesAcrossScopeIpv4AndMappedIpv6Forms() {
    for (List<String> hostPair :
        List.of(
            List.of("::1", "::1%0"),
            List.of("127.0.0.1", "127.000.000.001"),
            List.of("127.0.0.1", "::ffff:127.0.0.1"))) {
      String firstListener =
          VALID_YAML.replace("bind_host: 127.0.0.1", "bind_host: " + hostPair.getFirst());
      String duplicate =
          firstListener.replace(
              "    table_group_id: local\nhbase_profiles:",
              "    table_group_id: local\n"
                  + "  - id: equivalent-listener\n"
                  + "    bind_host: "
                  + hostPair.getLast()
                  + "\n"
                  + "    port: 2379\n"
                  + "    table_group_id: local\n"
                  + "hbase_profiles:");

      assertRejected(duplicate, "listeners[1].port", null);
    }
  }

  @Test
  void keepsNonzeroIpv6ScopeIdsDistinctWithoutResolvingHostnames() {
    String scopedYaml =
        VALID_YAML
            .replace("bind_host: 127.0.0.1", "bind_host: fe80::1%2")
            .replace(
                "authentication_mode: DEV_NO_AUTH",
                "authentication_mode: CREDENTIAL_REFERENCE\n"
                    + "    credential_reference: env://HBASE_CREDENTIALS")
            .replace("identity_provider_id: dev-no-auth", "identity_provider_id: backend-hbase")
            .replace(
                "    table_group_id: local\nhbase_profiles:",
                "    table_group_id: local\n"
                    + "  - id: other-scope-listener\n"
                    + "    bind_host: fe80::1%3\n"
                    + "    port: 2379\n"
                    + "    table_group_id: local\n"
                    + "hbase_profiles:");

    assertDoesNotThrow(() -> loader.load(bytes(scopedYaml)));
  }

  @Test
  void rejectsAmbiguousNumericIpv4AddressForms() {
    assertRejected(
        VALID_YAML.replace("bind_host: 127.0.0.1", "bind_host: 127.1"),
        "listeners[0].bind_host",
        null);
  }

  @Test
  void rejectsWildcardListenerConflictOnTheSamePort() {
    String secured =
        VALID_YAML.replace(
            "authentication_mode: DEV_NO_AUTH",
            "authentication_mode: CREDENTIAL_REFERENCE\n"
                + "    credential_reference: env://HBASE_CREDENTIALS")
            .replace("identity_provider_id: dev-no-auth", "identity_provider_id: backend-hbase");
    String wildcardConflict =
        secured.replace(
            "    table_group_id: local\nhbase_profiles:",
            "    table_group_id: local\n"
                + "  - id: wildcard-listener\n"
                + "    bind_host: 0.0.0.0\n"
                + "    port: 2379\n"
                + "    table_group_id: local\n"
                + "hbase_profiles:");

    assertRejected(wildcardConflict, "listeners[1].port", null);
  }

  @Test
  void rejectsUnknownReferencesAndProviderIds() {
    assertRejected(
        VALID_YAML.replace("table_group_id: local", "table_group_id: missing"),
        "listeners[0].table_group_id",
        null);
    assertRejected(
        VALID_YAML.replace("identity_provider_id: dev-no-auth", "identity_provider_id: unknown-provider"),
        "identity_provider_id",
        null);
    assertRejected(
        VALID_YAML.replace("strategy_id: crc32-bucket-v1", "strategy_id: unknown-salt"),
        "strategy_id",
        null);
  }

  @Test
  void rejectsUnknownSchemaVersionsAndScalarCoercion() {
    assertRejected(VALID_YAML.replace("schema_version: 1", "schema_version: 2"), "schema_version", null);
    assertRejected(VALID_YAML.replace("port: 2379", "port: \"2379\""), "listeners[0].port", null);
  }

  @Test
  void rejectsMissingRequiredPropertiesBeforePrimitiveDefaultsCanBeBound() {
    assertRejected(
        VALID_YAML.replace("    port: 2379\n", ""), "listeners[0].port", null);
    assertRejected(
        VALID_YAML.replaceFirst("          min_versions: 0\\n", ""),
        "table_groups[0].system_table.column_families[0].min_versions",
        null);
  }

  @Test
  void rejectsNumericEnumOrdinalsForEveryConfiguredEnumType() {
    assertRejected(
        VALID_YAML.replace("authentication_mode: DEV_NO_AUTH", "authentication_mode: 0"),
        "authentication_mode",
        null);
    assertRejected(
        VALID_YAML.replace("history_layout: UNDECIDED", "history_layout: 0"),
        "history_layout",
        null);
    assertRejected(
        VALID_YAML.replaceFirst("history_layout: SEPARATE_HISTORY_TABLE", "history_layout: 0"),
        "history_layout",
        null);
    assertRejected(
        VALID_YAML.replace("keep_deleted_cells: \"FALSE\"", "keep_deleted_cells: 0"),
        "keep_deleted_cells",
        null);
    assertRejected(
        VALID_YAML.replace(
            "record_categories: [DECISION, EVENT, LEASE, AUTH, CONTROL]",
            "record_categories: [0, EVENT, LEASE, AUTH, CONTROL]"),
        "record_categories",
        null);
    assertRejected(
        VALID_YAML.replace("permissions: [READ, WRITE]", "permissions: [0, WRITE]"),
        "permissions",
        null);
  }

  @Test
  void rejectsFractionalValuesForIntegerConfigurationFields() {
    assertRejected(
        VALID_YAML.replace("    port: 2379", "    port: 2379.9"),
        "listeners[0].port",
        null);
    assertRejected(
        VALID_YAML.replace("ttl_seconds: -1", "ttl_seconds: -1.9"),
        "ttl_seconds",
        null);
  }

  @Test
  void rejectsUnsortedAndIncorrectCrcSplitKeys() {
    assertRejected(
        VALID_YAML.replace(
            "[\"0001\", \"0002\", \"0003\", \"0004\", \"0005\", \"0006\", \"0007\", \"0008\", \"0009\", \"000a\", \"000b\", \"000c\", \"000d\", \"000e\", \"000f\"]",
            "[\"0002\", \"0001\", \"0003\", \"0004\", \"0005\", \"0006\", \"0007\", \"0008\", \"0009\", \"000a\", \"000b\", \"000c\", \"000d\", \"000e\", \"000f\"]"),
        "split_keys_hex[1]",
        null);
    assertRejected(
        VALID_YAML.replace("\"000f\"]", "\"0010\"]"),
        "split_keys_hex[14]",
        null);

    int systemTableStart = VALID_YAML.indexOf("system_table:");
    int systemSplitKeysStart = VALID_YAML.indexOf("split_keys_hex:", systemTableStart);
    String tableSplitMismatch =
        VALID_YAML.substring(0, systemSplitKeysStart)
            + VALID_YAML.substring(systemSplitKeysStart).replaceFirst("\"0001\"", "\"1234\"");
    assertRejected(tableSplitMismatch, "system_table.split_keys_hex", null);
    assertRejected(
        VALID_YAML.replaceFirst("profile_version: 1", "profile_version: 2"),
        "system_table.profile_version",
        null);
  }

  @Test
  void rejectsColumnFamilyVersionAndDeleteRetentionCombinationsThatCannotBeApplied() {
    assertRejected(
        VALID_YAML.replaceFirst("min_versions: 0", "min_versions: 2"),
        "min_versions",
        null);
    assertRejected(
        VALID_YAML.replaceFirst(
            "keep_deleted_cells: \"TRUE\"",
            "keep_deleted_cells: \"TTL\""),
        "keep_deleted_cells",
        null);
    assertRejected(
        VALID_YAML.replace("max_versions: 64", "max_versions: 1"),
        "max_versions",
        null);
  }

  @Test
  void acceptsEqualMinimumAndMaximumVersionsWithFiniteTtl() {
    String equalVersionsWithFiniteTtl =
        VALID_YAML.replaceFirst("min_versions: 0", "min_versions: 1")
            .replaceFirst("ttl_seconds: -1\\n", "ttl_seconds: 3600\n");

    assertDoesNotThrow(() -> loader.load(bytes(equalVersionsWithFiniteTtl)));
  }

  @Test
  void rejectsMinimumVersionsWithoutFiniteTtl() {
    String invalidMinimumVersionPolicy =
        VALID_YAML.replaceFirst("min_versions: 0", "min_versions: 1")
            .replaceFirst("max_versions: 1", "max_versions: 2");

    assertRejected(invalidMinimumVersionPolicy, "min_versions", null);
  }

  @Test
  void rejectsHBaseReservedCandidateNames() {
    assertRejected(VALID_YAML.replace("table_name: system", "table_name: zookeeper"), "table_name", null);
    assertRejected(
        VALID_YAML.replace("name: s", "name: recovered.edits"),
        "column_families[0].name",
        null);
  }

  @Test
  void rejectsWallClockTtlForNativeRevisionVersions() {
    int nativeCandidateStart = VALID_YAML.lastIndexOf("history_layout: NATIVE_CELL_VERSIONS");
    String invalidNativeCandidate =
        VALID_YAML.substring(0, nativeCandidateStart)
            + VALID_YAML.substring(nativeCandidateStart).replaceFirst("ttl_seconds: -1", "ttl_seconds: 86400");
    assertRejected(
        invalidNativeCandidate,
        "ttl_seconds",
        null);
  }

  @Test
  void rejectsDuplicateOrIncompleteHistoryCandidates() {
    assertRejected(
        VALID_YAML.replace(
            "history_layout: NATIVE_CELL_VERSIONS",
            "history_layout: SEPARATE_HISTORY_TABLE"),
        "history_layout",
        null);
    int nativeCandidateStart = VALID_YAML.lastIndexOf("- history_layout: NATIVE_CELL_VERSIONS");
    int candidateLineStart = VALID_YAML.lastIndexOf("\n", nativeCandidateStart) + 1;
    int resourcesStart = VALID_YAML.indexOf("resources:", nativeCandidateStart);
    String missingNativeCandidate =
        VALID_YAML.substring(0, candidateLineStart) + VALID_YAML.substring(resourcesStart);
    assertRejected(missingNativeCandidate, "history_candidates", null);
    assertRejected(
        VALID_YAML.replace("table_name: history", "table_name: system"),
        "tables[1].table_name",
        null);
    int tableGroupsStart = VALID_YAML.indexOf("table_groups:");
    int firstTableGroupStart = VALID_YAML.indexOf("  - id: local\n", tableGroupsStart);
    int tableResourcesStart = VALID_YAML.indexOf("resources:\n", firstTableGroupStart);
    String secondTableGroup =
        VALID_YAML.substring(firstTableGroupStart, tableResourcesStart)
            .replaceFirst("id: local", "id: other");
    String duplicatePhysicalTables =
        VALID_YAML.substring(0, tableResourcesStart)
            + secondTableGroup
            + VALID_YAML.substring(tableResourcesStart);
    assertRejected(duplicatePhysicalTables, "table_groups[1].tables", null);
    assertRejected(
        VALID_YAML.replace("permissions: [READ, WRITE]", "permissions: []"),
        "identity_permissions[0].permissions",
        null);
  }

  @Test
  void rejectsDuplicatePhysicalTablesAcrossHBaseProfileAliases() {
    String withProfileAlias =
        VALID_YAML.replace(
            "    authentication_mode: DEV_NO_AUTH\nsalt_profiles:",
            "    authentication_mode: DEV_NO_AUTH\n"
                + "  - id: other-profile\n"
                + "    quorum: 127.000.000.001\n"
                + "    client_port: 2181\n"
                + "    namespace: etcd_candidate\n"
                + "    authentication_mode: DEV_NO_AUTH\n"
                + "salt_profiles:");
    String withAliasListener =
        withProfileAlias.replace(
            "    table_group_id: local\nhbase_profiles:",
            "    table_group_id: local\n"
                + "  - id: alias-listener\n"
                + "    bind_host: 127.0.0.1\n"
                + "    port: 2380\n"
                + "    table_group_id: other\n"
                + "hbase_profiles:");
    int tableGroupsStart = withAliasListener.indexOf("table_groups:\n");
    int firstGroupStart = withAliasListener.indexOf("  - id: local\n", tableGroupsStart);
    int resourcesStart = withAliasListener.indexOf("resources:\n", firstGroupStart);
    String secondGroup =
        withAliasListener
            .substring(firstGroupStart, resourcesStart)
            .replaceFirst("id: local", "id: other")
            .replace("hbase_profile_id: local-hbase", "hbase_profile_id: other-profile");
    String duplicateTables =
        withAliasListener.substring(0, resourcesStart)
            + secondGroup
            + withAliasListener.substring(resourcesStart);

    assertRejected(duplicateTables, "table_groups[1].tables", null);
  }

  @Test
  void rejectsEmbeddedSecretAndDoesNotEchoIt() {
    String sentinel = "do-not-disclose-this-secret-value";
    RuntimeConfigurationException exception =
        assertThrows(
            RuntimeConfigurationException.class,
            () -> loader.load(bytes(VALID_YAML + "\nunknown_password: " + sentinel + "\n")));

    assertFalse(exception.getMessage().contains(sentinel));
    assertTrue(exception.getMessage().contains("unknown_password"), exception.getMessage());
    assertNull(exception.getCause());
  }

  @Test
  void rejectsCredentialValueInsteadOfControlledReferenceWithoutEchoingIt() {
    String sentinel = "inline-secret-value";
    String secretYaml =
        VALID_YAML.replace(
            "authentication_mode: DEV_NO_AUTH",
            "authentication_mode: CREDENTIAL_REFERENCE\n"
                + "    credential_reference: "
                + sentinel);
    RuntimeConfigurationException exception =
        assertThrows(RuntimeConfigurationException.class, () -> loader.load(bytes(secretYaml)));

    assertTrue(exception.getMessage().contains("credential_reference"));
    assertFalse(exception.getMessage().contains(sentinel));
    assertNull(exception.getCause());
  }

  @Test
  void rejectsDuplicateKeysCollectionAliasesAndMultipleDocumentsWithoutParserDetails() {
    String sentinel = "duplicate-secret-marker";
    String duplicate =
        VALID_YAML.replace(
            "schema_version: 1", "schema_version: 1\nschema_version: " + sentinel);
    RuntimeConfigurationException duplicateError =
        assertThrows(RuntimeConfigurationException.class, () -> loader.load(bytes(duplicate)));
    assertFalse(duplicateError.getMessage().contains(sentinel));
    assertNull(duplicateError.getCause());

    assertRejected(VALID_YAML + "\n---\nschema_version: 1\n", "one YAML document", null);
    String anchored = VALID_YAML.replace("  - id: local-etcd", "  - &listener\n    id: local-etcd");
    String withAlias =
        anchored.replace(
            "    table_group_id: local\nhbase_profiles:",
            "    table_group_id: local\n"
                + "  - *listener\n"
                + "hbase_profiles:");
    assertRejected(withAlias, "YAML", null);
  }

  @Test
  void rejectsInvalidBudgetsAndNonLoopbackNoAuth() {
    assertRejected(
        VALID_YAML.replace("max_scanners_per_table: 32", "max_scanners_per_table: 1025"),
        "resources.max_scanners_per_table",
        null);
    assertRejected(
        VALID_YAML.replace("request_deadline_ms: 5000", "request_deadline_ms: 0"),
        "resources.request_deadline_ms",
        null);
    assertRejected(
        VALID_YAML.replace("max_rpc_bytes: 4194304", "max_rpc_bytes: 0"),
        "resources.max_rpc_bytes",
        null);
    assertRejected(
        VALID_YAML.replace("max_rpc_bytes: 4194304", "max_rpc_bytes: 4194305"),
        "must not exceed max_request_bytes",
        null);
    assertRejected(
        VALID_YAML.replace("max_queued_requests: 1024", "max_queued_requests: 0"),
        "resources.max_queued_requests",
        null);
    assertRejected(
        VALID_YAML.replace("bind_host: 127.0.0.1", "bind_host: 0.0.0.0"),
        "DEV_NO_AUTH requires a loopback listener",
        null);
    assertRejected(
        VALID_YAML.replace("bind_host: 127.0.0.1", "bind_host: 127.untrusted"),
        "DEV_NO_AUTH requires a loopback listener",
        null);
    assertRejected(
        VALID_YAML.replace("quorum: 127.0.0.1", "quorum: hbase.example"),
        "DEV_NO_AUTH requires loopback HBase quorum hosts",
        null);
    assertRejected(
        VALID_YAML.replace("quorum: 127.0.0.1", "quorum: 127.0.0.1,hbase.example"),
        "DEV_NO_AUTH requires loopback HBase quorum hosts",
        null);
    assertDoesNotThrow(
        () ->
            loader.load(
                bytes(VALID_YAML.replace("quorum: 127.0.0.1", "quorum: localhost,127.0.0.1"))));
  }

  @Test
  void enforcesByteLimitUtf8AndDoesNotCloseCallerStream() {
    byte[] tooLarge = new byte[RuntimeConfigurationLoader.ABSOLUTE_MAX_CONFIG_BYTES + 1];
    RuntimeConfigurationException sizeError =
        assertThrows(RuntimeConfigurationException.class, () -> loader.load(tooLarge));
    assertTrue(sizeError.getMessage().contains("65536-byte limit"));
    assertThrows(
        RuntimeConfigurationException.class,
        () -> loader.load(new ByteArrayInputStream(tooLarge)));

    RuntimeConfigurationException encodingError =
        assertThrows(RuntimeConfigurationException.class, () -> loader.load(new byte[] {(byte) 0xC3, 0x28}));
    assertTrue(encodingError.getMessage().contains("UTF-8"));

    TrackingInputStream input = new TrackingInputStream(bytes(VALID_YAML));
    assertNotNull(loader.load(input));
    assertFalse(input.closed);
  }

  private void assertRejected(String yaml, String messagePart, String forbidden) {
    RuntimeConfigurationException exception =
        assertThrows(RuntimeConfigurationException.class, () -> loader.load(bytes(yaml)));
    assertTrue(exception.getMessage().contains(messagePart), exception.getMessage());
    assertNull(exception.getCause());
    if (forbidden != null) {
      assertFalse(exception.getMessage().contains(forbidden));
    }
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static String readCandidateYaml() {
    try (InputStream input = RuntimeConfigurationLoaderTest.class.getResourceAsStream("/runtime-candidate.yaml")) {
      if (input == null) {
        throw new IllegalStateException("runtime-candidate.yaml is missing from the test classpath");
      }
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException exception) {
      throw new UncheckedIOException(exception);
    }
  }

  private static final class TrackingInputStream extends ByteArrayInputStream {
    private boolean closed;

    private TrackingInputStream(byte[] content) {
      super(content);
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
