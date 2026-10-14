package io.etcdhbase.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration.HBaseProfile;
import io.etcdhbase.server.config.RuntimeConfiguration.HistoryLayoutCandidate;
import io.etcdhbase.server.config.RuntimeConfiguration.TableGroup;
import io.etcdhbase.server.config.RuntimeConfiguration.TableSchemaCandidate;
import io.etcdhbase.server.config.RuntimeConfigurationLoader;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

class TableGroupInitializerTest {
  @Test
  void resumesMatchingPartialCreationAndThenBecomesANoOp() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.yaml", "m0_init_partial");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();
    admin.failOnCreateNumber = 2;

    TableGroupInitializer.PartialInitializationException failure =
        assertThrows(
            TableGroupInitializer.PartialInitializationException.class,
            () -> TableGroupInitializer.initialize(plan, admin));
    assertEquals(plan.namespace(), failure.namespace());
    assertEquals(1, failure.createdTables().size());
    assertEquals(1, admin.descriptors.size());
    assertTrue(failure.getMessage().contains("rerun the same confirmed plan"));

    admin.failOnCreateNumber = 0;
    TableGroupInitializer.InitializationResult resumed =
        TableGroupInitializer.initialize(plan, admin);
    assertEquals(plan.tables().size() - 1, resumed.createdTables().size());
    assertFalse(resumed.alreadyInitialized());
    assertEquals(plan.tables().size(), admin.descriptors.size());

    TableGroupInitializer.InitializationResult repeated =
        TableGroupInitializer.initialize(plan, admin);
    assertTrue(repeated.alreadyInitialized());
    assertTrue(repeated.createdTables().isEmpty());
    assertEquals(plan.tables().size(), admin.descriptors.size());
  }

  @Test
  void refusesConflictingDescriptorsAndUnrelatedTablesWithoutModifyingThem() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.yaml", "m0_init_conflict");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();
    TableGroupInitializer.initialize(plan, admin);
    TableName firstTable = admin.descriptors.keySet().stream().findFirst().orElseThrow();
    admin.descriptors.put(
        firstTable,
        TableDescriptorBuilder.newBuilder(admin.descriptors.get(firstTable))
            .setValue("etcdhbase.m0.schema-version", "999")
            .build());

    assertThrows(IllegalArgumentException.class, () -> TableGroupInitializer.initialize(plan, admin));
    assertEquals(plan.tables().size(), admin.descriptors.size());

    TableName unrelated = TableName.valueOf(plan.namespace(), "business-data");
    admin.descriptors.put(unrelated, TableDescriptorBuilder.newBuilder(unrelated).build());
    assertThrows(IllegalArgumentException.class, () -> TableGroupInitializer.initialize(plan, admin));
    assertTrue(admin.descriptors.containsKey(unrelated));
  }

  @Test
  void acceptsBackendAddedDescriptorDefaultsWhenPlannedSchemaStillMatches() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.yaml", "m0_init_backend_default");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();
    TableGroupInitializer.initialize(plan, admin);
    TableName firstTable = admin.descriptors.keySet().stream().findFirst().orElseThrow();
    admin.descriptors.put(
        firstTable,
        TableDescriptorBuilder.newBuilder(admin.descriptors.get(firstTable))
            .setValue("hbase.store.file-tracker.impl", "DEFAULT")
            .build());

    TableGroupInitializer.InitializationResult repeated =
        TableGroupInitializer.initialize(plan, admin);

    assertTrue(repeated.alreadyInitialized());
    assertTrue(repeated.createdTables().isEmpty());
  }

  @Test
  void refusesToExtendAPartialGroupThatAlreadyContainsRows() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.yaml", "m0_init_nonempty");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();
    admin.failOnCreateNumber = 2;
    assertThrows(
        TableGroupInitializer.PartialInitializationException.class,
        () -> TableGroupInitializer.initialize(plan, admin));
    TableName existing = admin.descriptors.keySet().stream().findFirst().orElseThrow();
    admin.nonEmptyTables.add(existing);
    int existingTableCount = admin.descriptors.size();

    assertThrows(
        IllegalArgumentException.class, () -> TableGroupInitializer.initialize(plan, admin));
    assertEquals(existingTableCount, admin.descriptors.size());
  }

  @Test
  void fullGroupRetryIgnoresOperationalRegionSplitsAndPreservesData() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.yaml", "m0_init_split_full");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();
    TableGroupInitializer.initialize(plan, admin);
    TableName tableName = TableName.valueOf(plan.namespace(), "system");
    int createdTableCount = admin.createNumber;
    admin.splitRegion(tableName, HexFormat.of().parseHex("000080"));
    admin.nonEmptyTables.add(tableName);

    TableGroupInitializer.InitializationResult repeated =
        TableGroupInitializer.initialize(plan, admin);

    assertTrue(repeated.alreadyInitialized());
    assertTrue(repeated.createdTables().isEmpty());
    assertEquals(createdTableCount, admin.createNumber);
    assertTrue(admin.nonEmptyTables.contains(tableName));
  }

  @Test
  void partialGroupWithChangedRegionBoundariesCannotResume() throws Exception {
    TableGroupInitializationPlan plan = plan("history-table.yaml", "m0_init_split_partial");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();
    admin.failOnCreateNumber = 2;
    assertThrows(
        TableGroupInitializer.PartialInitializationException.class,
        () -> TableGroupInitializer.initialize(plan, admin));
    TableName existing = admin.descriptors.keySet().stream().findFirst().orElseThrow();
    admin.splitRegion(existing, HexFormat.of().parseHex("000080"));
    int existingTableCount = admin.descriptors.size();
    int createCount = admin.createNumber;

    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class, () -> TableGroupInitializer.initialize(plan, admin));

    assertTrue(failure.getMessage().contains("different region split boundaries"));
    assertEquals(existingTableCount, admin.descriptors.size());
    assertEquals(createCount, admin.createNumber);
  }

  @Test
  void mapsBothHistoryCandidatesIntoDistinctExplicitTableGroups() throws Exception {
    TableGroupInitializationPlan separate = plan("history-table.yaml", "m0_init_separate");
    TableGroupInitializationPlan nativeVersions = plan("native-cell-versions.yaml", "m0_init_native");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();

    TableGroupInitializer.initialize(separate, admin);
    TableGroupInitializer.initialize(nativeVersions, admin);

    assertEquals(5, admin.descriptors.size());
    assertEquals(64, admin.descriptors.get(TableName.valueOf("m0_init_native:kv"))
        .getColumnFamily(Bytes.toBytes("d"))
        .getMaxVersions());
  }

  @Test
  void initializesConfiguredSiblingGroupsInTheSameNamespaceWithoutModifyingThem() throws Exception {
    RuntimeConfiguration configuration = configurationWithSiblingGroup("shared_namespace");
    TableGroupInitializationPlan local =
        TableGroupPlanFactory.create(configuration, "local", null);
    TableGroupInitializationPlan sibling =
        TableGroupPlanFactory.create(configuration, "sibling", null);
    TableGroupInitializationPlan singleGroup = plan("history-table.yaml", "shared_namespace");
    InMemoryTableGroupAdmin admin = new InMemoryTableGroupAdmin();
    Set<String> expectedConfiguredTables =
        Stream.concat(
                local.tables().stream().map(TableGroupInitializationPlan.TablePlan::name),
                sibling.tables().stream().map(TableGroupInitializationPlan.TablePlan::name))
            .collect(Collectors.toUnmodifiableSet());

    assertEquals(expectedConfiguredTables, Set.copyOf(local.configuredNamespaceTableNames()));
    assertEquals(expectedConfiguredTables, Set.copyOf(sibling.configuredNamespaceTableNames()));
    assertNotEquals(singleGroup.fingerprint(), local.fingerprint());

    TableGroupInitializer.InitializationResult localResult =
        TableGroupInitializer.initialize(local, admin);
    Map<TableName, TableDescriptor> localDescriptors = Map.copyOf(admin.descriptors);
    TableGroupInitializer.InitializationResult siblingResult =
        TableGroupInitializer.initialize(sibling, admin);

    assertFalse(localResult.alreadyInitialized());
    assertFalse(siblingResult.alreadyInitialized());
    assertEquals(local.tables().size(), localDescriptors.size());
    assertEquals(local.tables().size() + sibling.tables().size(), admin.descriptors.size());
    for (Map.Entry<TableName, TableDescriptor> entry : localDescriptors.entrySet()) {
      assertSame(entry.getValue(), admin.descriptors.get(entry.getKey()));
      assertEquals("local", entry.getValue().getValue("etcdhbase.m0.table-group-id"));
    }
    for (TableGroupInitializationPlan.TablePlan table : sibling.tables()) {
      assertEquals(
          "sibling",
          admin.descriptors
              .get(TableName.valueOf(sibling.namespace(), table.name()))
              .getValue("etcdhbase.m0.table-group-id"));
    }
    assertTrue(TableGroupInitializer.initialize(local, admin).alreadyInitialized());
    assertTrue(TableGroupInitializer.initialize(sibling, admin).alreadyInitialized());
  }

  private static TableGroupInitializationPlan plan(String resourceName, String namespace)
      throws Exception {
    RuntimeConfiguration configuration = configuration(resourceName);
    HBaseProfile profile = configuration.hbaseProfiles().getFirst();
    RuntimeConfiguration isolated =
        new RuntimeConfiguration(
            configuration.schemaVersion(),
            configuration.listeners(),
            List.of(
                new HBaseProfile(
                    profile.id(),
                    profile.quorum(),
                    profile.clientPort(),
                    namespace,
                    profile.authenticationMode(),
                    profile.credentialReference())),
            configuration.saltProfiles(),
            configuration.tableGroups(),
            configuration.resources());
    return TableGroupPlanFactory.create(isolated, "local", null);
  }

  private static RuntimeConfiguration configuration(String resourceName) throws Exception {
    RuntimeConfiguration configuration;
    try (InputStream input =
        TableGroupInitializerTest.class.getResourceAsStream("/m0-examples/" + resourceName)) {
      if (input == null) {
        throw new IllegalStateException("missing M0 configuration fixture");
      }
      configuration =
          new RuntimeConfigurationLoader(
                  Set.of(BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID),
                  Set.of(HashSaltStrategy.STRATEGY_ID))
              .load(input.readAllBytes(), Map.of());
    }
    return configuration;
  }

  private static RuntimeConfiguration configurationWithSiblingGroup(String namespace)
      throws Exception {
    RuntimeConfiguration configuration = configuration("history-table.yaml");
    HBaseProfile profile = configuration.hbaseProfiles().getFirst();
    TableGroup selected = configuration.tableGroups().getFirst();
    TableGroup sibling =
        new TableGroup(
            "sibling",
            selected.schemaVersion(),
            "sibling-hbase",
            selected.saltProfileId(),
            selected.identityProviderId(),
            selected.historyLayout(),
            rename(selected.systemTable(), "sibling-system"),
            selected.historyCandidates().stream()
                .map(
                    candidate ->
                        new HistoryLayoutCandidate(
                            candidate.historyLayout(),
                            candidate.tables().stream()
                                .map(table -> rename(table, "sibling-" + table.tableName()))
                                .toList()))
                .toList());
    RuntimeConfiguration.HBaseProfile isolatedProfile =
        new HBaseProfile(
            profile.id(),
            profile.quorum(),
            profile.clientPort(),
            namespace,
            profile.authenticationMode(),
            profile.credentialReference());
    RuntimeConfiguration.HBaseProfile aliasedProfile =
        new HBaseProfile(
            "sibling-hbase",
            "127.000.000.001",
            profile.clientPort(),
            namespace,
            profile.authenticationMode(),
            profile.credentialReference());
    return new RuntimeConfiguration(
        configuration.schemaVersion(),
        configuration.listeners(),
        List.of(isolatedProfile, aliasedProfile),
        configuration.saltProfiles(),
        List.of(selected, sibling),
        configuration.resources());
  }

  private static TableSchemaCandidate rename(TableSchemaCandidate table, String name) {
    return new TableSchemaCandidate(
        name,
        table.schemaVersion(),
        table.profileVersion(),
        table.columnFamilies(),
        table.splitKeysHex(),
        table.recordCategories(),
        table.identityPermissions());
  }

  private static final class InMemoryTableGroupAdmin implements TableGroupAdmin {
    private final Set<String> namespaces = new HashSet<>();
    private final Map<TableName, TableDescriptor> descriptors = new HashMap<>();
    private final Map<TableName, List<RegionInfo>> regions = new HashMap<>();
    private final Set<TableName> nonEmptyTables = new HashSet<>();
    private int createNumber;
    private int failOnCreateNumber;

    @Override
    public List<String> listNamespaces() {
      return List.copyOf(namespaces);
    }

    @Override
    public void createNamespace(String namespace) {
      namespaces.add(namespace);
    }

    @Override
    public List<TableName> listTableNames(String namespace) {
      return descriptors.keySet().stream()
          .filter(tableName -> tableName.getNamespaceAsString().equals(namespace))
          .sorted()
          .toList();
    }

    @Override
    public TableDescriptor getDescriptor(TableName tableName) {
      return descriptors.get(tableName);
    }

    @Override
    public List<RegionInfo> getRegions(TableName tableName) {
      return regions.get(tableName);
    }

    @Override
    public boolean isEmpty(TableName tableName) {
      return !nonEmptyTables.contains(tableName);
    }

    @Override
    public void createTable(TableDescriptor descriptor, byte[][] splitKeys) throws Exception {
      createNumber++;
      if (createNumber == failOnCreateNumber) {
        throw new Exception("simulated create failure");
      }
      TableName tableName = descriptor.getTableName();
      descriptors.put(tableName, descriptor);
      namespaces.add(tableName.getNamespaceAsString());
      List<byte[]> starts = new ArrayList<>();
      starts.add(new byte[0]);
      starts.addAll(Arrays.asList(splitKeys));
      starts.sort(Bytes.BYTES_COMPARATOR);
      List<RegionInfo> tableRegions = new ArrayList<>();
      for (int index = 0; index < starts.size(); index++) {
        byte[] endKey = index + 1 == starts.size() ? new byte[0] : starts.get(index + 1);
        tableRegions.add(
            RegionInfoBuilder.newBuilder(tableName)
                .setStartKey(starts.get(index))
                .setEndKey(endKey)
                .build());
      }
      regions.put(tableName, List.copyOf(tableRegions));
    }

    private void splitRegion(TableName tableName, byte[] splitPoint) {
      List<RegionInfo> tableRegions = new ArrayList<>(regions.get(tableName));
      for (int index = 0; index < tableRegions.size(); index++) {
        RegionInfo parent = tableRegions.get(index);
        byte[] startKey = parent.getStartKey();
        byte[] endKey = parent.getEndKey();
        if (Bytes.compareTo(startKey, splitPoint) < 0
            && (endKey.length == 0 || Bytes.compareTo(splitPoint, endKey) < 0)) {
          tableRegions.remove(index);
          tableRegions.add(
              RegionInfoBuilder.newBuilder(tableName)
                  .setStartKey(startKey)
                  .setEndKey(splitPoint)
                  .build());
          tableRegions.add(
              RegionInfoBuilder.newBuilder(tableName)
                  .setStartKey(splitPoint)
                  .setEndKey(endKey)
                  .build());
          tableRegions.sort(
              (left, right) -> Bytes.compareTo(left.getStartKey(), right.getStartKey()));
          regions.put(tableName, List.copyOf(tableRegions));
          return;
        }
      }
      throw new IllegalArgumentException("split point is not inside an existing region");
    }
  }
}
