package io.etcdhbase.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.junit.jupiter.api.Test;

class InitializeTablesIT {
  private static final long TIMEOUT_SECONDS = 60;
  // The HBase test harness is module-local and runs unshaded server classes in a child JVM.
  private static final String CLUSTER_PROCESS_CLASS =
      "io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess";
  private static final List<String> EXPECTED_SPLITS =
      List.of(
          "0001", "0002", "0003", "0004", "0005", "0006", "0007", "0008",
          "0009", "000a", "000b", "000c", "000d", "000e", "000f");

  @Test
  void createsAndRerunsBothExplicitHistoryLayoutsOnTemporaryHBase() throws Exception {
    try (LocalCluster cluster = LocalCluster.start()) {
      String runId = UUID.randomUUID().toString().replace("-", "");
      String separateNamespace = "m0_init_s_" + runId;
      String nativeNamespace = "m0_init_n_" + runId;
      Path separateConfig =
          runtimeConfiguration(
              cluster.clientConfiguration(), "history-table.yaml", separateNamespace);
      Path nativeConfig =
          runtimeConfiguration(
              cluster.clientConfiguration(), "native-cell-versions.yaml", nativeNamespace);

      CommandResult separatePlan = run("init", "plan", "--config", separateConfig.toString());
      assertEquals(0, separatePlan.exitCode(), separatePlan.error());
      assertTrue(
          separatePlan.output().contains("history_layout: SEPARATE_HISTORY_TABLE"),
          separatePlan.output());
      assertTrue(
          separatePlan.output().contains(separateNamespace + ":history"), separatePlan.output());
      String separateFingerprint = fingerprint(separatePlan.output());

      CommandResult separateApply = apply(separateConfig, "local", separateFingerprint);
      assertEquals(0, separateApply.exitCode(), separateApply.error());
      assertTrue(separateApply.output().contains("status=created"), separateApply.output());
      CommandResult separateRetry = apply(separateConfig, "local", separateFingerprint);
      assertEquals(0, separateRetry.exitCode(), separateRetry.error());
      assertTrue(
          separateRetry.output().contains("status=already-initialized"), separateRetry.output());

      CommandResult nativePlan = run("init", "plan", "--config", nativeConfig.toString());
      assertEquals(0, nativePlan.exitCode(), nativePlan.error());
      assertTrue(
          nativePlan.output().contains("history_layout: NATIVE_CELL_VERSIONS"),
          nativePlan.output());
      assertTrue(nativePlan.output().contains(nativeNamespace + ":kv"), nativePlan.output());
      assertFalse(nativePlan.output().contains(nativeNamespace + ":history"), nativePlan.output());
      String nativeFingerprint = fingerprint(nativePlan.output());

      CommandResult nativeApply = apply(nativeConfig, "local", nativeFingerprint);
      assertEquals(0, nativeApply.exitCode(), nativeApply.error());
      assertTrue(nativeApply.output().contains("status=created"), nativeApply.output());
      CommandResult nativeRetry = apply(nativeConfig, "local", nativeFingerprint);
      assertEquals(0, nativeRetry.exitCode(), nativeRetry.error());
      assertTrue(nativeRetry.output().contains("status=already-initialized"), nativeRetry.output());

      verifyTableGroup(
          cluster.clientConfiguration(),
          separateNamespace,
          List.of(
              new FamilyExpectation("system", "s", 1, KeepDeletedCells.FALSE),
              new FamilyExpectation("latest", "d", 1, KeepDeletedCells.FALSE),
              new FamilyExpectation("history", "h", 1, KeepDeletedCells.TRUE)));
      verifyTableGroup(
          cluster.clientConfiguration(),
          nativeNamespace,
          List.of(new FamilyExpectation("kv", "d", 64, KeepDeletedCells.TRUE)));
    }
  }

  private static void verifyTableGroup(
      Configuration configuration,
      String namespace,
      List<FamilyExpectation> expectedTables)
      throws Exception {
    try (Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin()) {
      for (FamilyExpectation expected : expectedTables) {
        TableName tableName = TableName.valueOf(namespace, expected.tableName());
        TableDescriptor descriptor = admin.getDescriptor(tableName);
        ColumnFamilyDescriptor family =
            descriptor.getColumnFamily(expected.family().getBytes(StandardCharsets.UTF_8));
        assertNotNull(family, tableName.toString());
        assertEquals(expected.maxVersions(), family.getMaxVersions(), tableName.toString());
        assertEquals(HConstants.FOREVER, family.getTimeToLive(), tableName.toString());
        assertEquals(expected.keepDeletedCells(), family.getKeepDeletedCells(), tableName.toString());

        List<byte[]> actualStartKeys =
            admin.getRegions(tableName).stream()
                .map(RegionInfo::getStartKey)
                .sorted(Arrays::compareUnsigned)
                .toList();
        List<byte[]> expectedStartKeys = new ArrayList<>();
        expectedStartKeys.add(new byte[0]);
        EXPECTED_SPLITS.stream().map(HexFormat.of()::parseHex).forEach(expectedStartKeys::add);
        expectedStartKeys.sort(Arrays::compareUnsigned);
        assertEquals(expectedStartKeys.size(), actualStartKeys.size(), tableName.toString());
        for (int index = 0; index < expectedStartKeys.size(); index++) {
          assertTrue(
              Arrays.equals(expectedStartKeys.get(index), actualStartKeys.get(index)),
              tableName.toString());
        }

        try (Table table = connection.getTable(tableName);
            ResultScanner scanner = table.getScanner(new Scan().setLimit(1))) {
          assertNull(scanner.next(), tableName + " must remain empty after initialization");
        }
      }
    }
  }

  private static Path runtimeConfiguration(
      Configuration hbaseConfiguration, String fixtureName, String namespace) throws Exception {
    String source;
    try (InputStream input =
        InitializeTablesIT.class.getResourceAsStream("/m0-examples/" + fixtureName)) {
      if (input == null) {
        throw new IllegalStateException("missing M0 configuration fixture: " + fixtureName);
      }
      source = new String(input.readAllBytes(), StandardCharsets.UTF_8);
    }
    String quorum = hbaseConfiguration.get("hbase.zookeeper.quorum");
    int clientPort = hbaseConfiguration.getInt("hbase.zookeeper.property.clientPort", -1);
    assertNotNull(quorum);
    assertTrue(clientPort > 0);
    source = replaceScalar(source, "quorum", quorum);
    source = replaceScalar(source, "client_port", Integer.toString(clientPort));
    source = replaceScalar(source, "namespace", namespace);
    Path path = Files.createTempFile("m0-admin-it-", ".yaml");
    Files.writeString(path, source, StandardCharsets.UTF_8);
    path.toFile().deleteOnExit();
    return path;
  }

  private static CommandResult apply(Path configuration, String tableGroup, String fingerprint) {
    return run(
        "init",
        "apply",
        "--config",
        configuration.toString(),
        "--confirm-table-group",
        tableGroup,
        "--confirm-plan",
        fingerprint);
  }

  private static CommandResult run(String... args) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ByteArrayOutputStream error = new ByteArrayOutputStream();
    try (PrintStream outputStream = new PrintStream(output, true, StandardCharsets.UTF_8);
        PrintStream errorStream = new PrintStream(error, true, StandardCharsets.UTF_8)) {
      int exitCode = InitializeTablesCommand.run(args, outputStream, errorStream);
      return new CommandResult(
          exitCode,
          output.toString(StandardCharsets.UTF_8),
          error.toString(StandardCharsets.UTF_8));
    }
  }

  private static String fingerprint(String output) {
    return output.lines()
        .filter(line -> line.startsWith("confirmation_fingerprint: "))
        .map(line -> line.substring("confirmation_fingerprint: ".length()))
        .findFirst()
        .orElseThrow(
            () -> new IllegalStateException("plan did not print a confirmation fingerprint"));
  }

  private static String replaceScalar(String yaml, String key, String value) {
    Pattern pattern =
        Pattern.compile("(?m)^([\\t ]*" + Pattern.quote(key) + ":[\\t ]*).*$");
    Matcher matcher = pattern.matcher(yaml);
    if (!matcher.find()) {
      throw new IllegalArgumentException("runtime configuration has no " + key + " scalar");
    }
    String replacement = Matcher.quoteReplacement(matcher.group(1) + value);
    return matcher.replaceFirst(replacement);
  }

  private record FamilyExpectation(
      String tableName, String family, int maxVersions, KeepDeletedCells keepDeletedCells) {}

  private record CommandResult(int exitCode, String output, String error) {}

  private record LocalCluster(AutoCloseable process, Configuration clientConfiguration)
      implements AutoCloseable {
    private static LocalCluster start() throws Exception {
      Class<?> processType = Class.forName(CLUSTER_PROCESS_CLASS);
      try {
        Object process = processType.getMethod("start").invoke(null);
        Configuration configuration =
            (Configuration) processType.getMethod("clientConfiguration").invoke(process);
        return new LocalCluster((AutoCloseable) process, configuration);
      } catch (InvocationTargetException failure) {
        Throwable cause = failure.getCause();
        if (cause instanceof Exception exception) {
          throw exception;
        }
        if (cause instanceof Error error) {
          throw error;
        }
        throw new IllegalStateException("temporary HBase cluster failed", cause);
      }
    }

    @Override
    public void close() throws Exception {
      process.close();
    }
  }
}
