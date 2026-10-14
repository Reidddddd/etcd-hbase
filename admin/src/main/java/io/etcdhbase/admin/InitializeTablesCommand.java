package io.etcdhbase.admin;

import io.etcdhbase.hbase.connection.OfficialHBaseConnectionFactory;
import io.etcdhbase.hbase.identity.DevelopmentIdentityProvider;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration.HistoryLayout;
import io.etcdhbase.server.config.RuntimeConfigurationLoader;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;

/** Plans or applies one explicitly confirmed M0 HBase table group. */
public final class InitializeTablesCommand {
  private static final long OPERATION_TIMEOUT_SECONDS = 30;
  private static final Set<String> SUPPORTED_PROVIDER_IDS =
      Set.of(BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID);
  private static final Set<String> SUPPORTED_SALT_STRATEGY_IDS =
      Set.of(HashSaltStrategy.STRATEGY_ID);
  private static final Set<String> ALL_OPTIONS =
      Set.of(
          "--config",
          "--table-group",
          "--history-layout",
          "--confirm-table-group",
          "--confirm-plan");

  private InitializeTablesCommand() {}

  public static void main(String[] args) {
    int exitCode = run(args, System.out, System.err);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  static int run(String[] args, PrintStream output, PrintStream error) {
    return run(args, output, error, InitializeTablesCommand::apply);
  }

  static int run(
      String[] args, PrintStream output, PrintStream error, ApplyOperation applyOperation) {
    try {
      Options options = Options.parse(args);
      RuntimeConfiguration configuration;
      try {
        configuration = load(options.configPath());
      } catch (IOException failure) {
        error.println("m0-admin error: configuration cannot be read");
        return 2;
      }
      String requestedGroup =
          options.action().equals("plan") ? options.tableGroupId() : options.confirmTableGroup();
      TableGroupInitializationPlan plan =
          TableGroupPlanFactory.create(configuration, requestedGroup, options.historyLayout());
      if (options.action().equals("plan")) {
        printPlan(plan, output);
      } else {
        confirm(plan, options);
        applyOperation.apply(plan, output);
      }
      return 0;
    } catch (TableGroupInitializer.PartialInitializationException failure) {
      error.printf(
          "m0-admin partial: namespace=%s created_tables=%s confirmation_fingerprint=sha256:%s; "
              + "rerun the same confirmed plan to continue%n",
          failure.namespace(),
          tableNames(failure.createdTables()),
          failure.planFingerprint());
      return 4;
    } catch (IllegalArgumentException failure) {
      error.println("m0-admin error: " + failure.getMessage());
      return 2;
    } catch (Exception failure) {
      error.println(
          "m0-admin error: HBase operation failed (" + failure.getClass().getSimpleName() + ")");
      return 4;
    }
  }

  @FunctionalInterface
  interface ApplyOperation {
    void apply(TableGroupInitializationPlan plan, PrintStream output) throws Exception;
  }

  private static RuntimeConfiguration load(Path configPath) throws IOException {
    RuntimeConfigurationLoader loader =
        new RuntimeConfigurationLoader(SUPPORTED_PROVIDER_IDS, SUPPORTED_SALT_STRATEGY_IDS);
    try (InputStream input = Files.newInputStream(configPath)) {
      return loader.load(input);
    }
  }

  private static void confirm(TableGroupInitializationPlan plan, Options options) {
    if (!plan.tableGroupId().equals(options.confirmTableGroup())) {
      throw new IllegalArgumentException(
          "--confirm-table-group does not match the selected table group");
    }
    if (!options.confirmPlan().equals("sha256:" + plan.fingerprint())) {
      throw new IllegalArgumentException(
          "--confirm-plan does not match the current plan fingerprint");
    }
    if (!BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID.equals(plan.identityProviderId())
        || plan.authenticationMode() != RuntimeConfiguration.AuthenticationMode.DEV_NO_AUTH) {
      throw new IllegalArgumentException(
          "missing-input: the configured HBase identity provider is not available for apply");
    }
  }

  private static void apply(TableGroupInitializationPlan plan, PrintStream output)
      throws Exception {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", plan.quorum());
    configuration.setInt("hbase.zookeeper.property.clientPort", plan.clientPort());

    DevelopmentIdentityProvider identityProvider = new DevelopmentIdentityProvider();
    OfficialHBaseConnectionFactory connectionFactory =
        new OfficialHBaseConnectionFactory(configuration, plan.hbaseProfileId(), 1);
    try {
      AuthorizedBackendIdentity identity;
      try (BackendCredentials credentials = new BackendCredentials("m0-admin", new char[0])) {
        identity =
            await(
                identityProvider
                    .authenticate(
                        credentials,
                        new AuthenticationScope(plan.tableGroupId(), plan.hbaseProfileId()))
                    .toCompletableFuture());
      }
      AsyncConnection connection =
          await(connectionFactory.connectionFor(identity).toCompletableFuture());
      TableGroupInitializer.InitializationResult result =
          TableGroupInitializer.initialize(plan, new AsyncTableGroupAdmin(connection));
      output.printf(
          "m0-admin apply namespace=%s table_group=%s history_layout=%s status=%s "
              + "created_tables=%s confirmation_fingerprint=sha256:%s%n",
          result.namespace(),
          plan.tableGroupId(),
          plan.historyLayout(),
          result.alreadyInitialized() ? "already-initialized" : "created",
          tableNames(result.createdTables()),
          result.planFingerprint());
    } finally {
      try {
        connectionFactory.close();
      } finally {
        identityProvider.close();
      }
    }
  }

  private static <T> T await(CompletableFuture<T> future) throws Exception {
    try {
      return future.get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException timeout) {
      future.cancel(true);
      throw new IllegalStateException("HBase identity or connection operation timed out", timeout);
    } catch (InterruptedException interrupted) {
      future.cancel(true);
      Thread.currentThread().interrupt();
      throw interrupted;
    } catch (ExecutionException failure) {
      Throwable cause = failure.getCause();
      if (cause instanceof Exception exception) {
        throw exception;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw new IllegalStateException("HBase identity or connection operation failed", cause);
    }
  }

  private static String tableNames(List<TableName> names) {
    return names.stream().map(TableName::toString).toList().toString();
  }

  private static void printPlan(TableGroupInitializationPlan plan, PrintStream output) {
    output.println("M0 HBase table initialization plan");
    output.println("table_group_id: " + plan.tableGroupId());
    output.println("identity_provider_id: " + plan.identityProviderId());
    output.println("hbase_profile_id: " + plan.hbaseProfileId());
    output.println("namespace: " + plan.namespace());
    output.println("quorum: " + plan.quorum());
    output.println("client_port: " + plan.clientPort());
    output.println("authentication_mode: " + plan.authenticationMode());
    output.println("schema_version: " + plan.schemaVersion());
    output.println("salt_profile_id: " + plan.saltProfileId());
    output.println("salt_strategy_id: " + plan.saltStrategyId());
    output.println("history_layout: " + plan.historyLayout());
    output.println("confirmation_fingerprint: sha256:" + plan.fingerprint());
    output.println("configured_namespace_tables:");
    for (String tableName : plan.configuredNamespaceTableNames()) {
      output.println("  - " + plan.namespace() + ":" + tableName);
    }
    output.println("tables:");
    for (TableGroupInitializationPlan.TablePlan table : plan.tables()) {
      output.println("  - name: " + plan.namespace() + ":" + table.name());
      output.println("    schema_version: " + table.schemaVersion());
      output.println("    profile_version: " + table.profileVersion());
      output.println("    record_categories: " + String.join(",", table.recordCategories()));
      output.println("    column_families:");
      for (TableGroupInitializationPlan.ColumnFamilyPlan family : table.columnFamilies()) {
        output.printf(
            "      - name: %s min_versions: %d max_versions: %d ttl_seconds: %d "
                + "keep_deleted_cells: %s%n",
            family.name(),
            family.minVersions(),
            family.maxVersions(),
            family.ttlSeconds(),
            family.keepDeletedCells());
      }
      output.println(
          "    split_keys_hex: "
              + (table.splitKeysHex().isEmpty()
                  ? "[]"
                  : "[" + String.join(",", table.splitKeysHex()) + "]"));
    }
    output.println("No HBase connection was opened; this command only read and validated configuration.");
  }

  private record Options(
      String action,
      Path configPath,
      String tableGroupId,
      HistoryLayout historyLayout,
      String confirmTableGroup,
      String confirmPlan) {

    private static Options parse(String[] args) {
      if (args.length < 2 || !args[0].equals("init")) {
        throw new IllegalArgumentException(
            "usage: init plan|apply --config PATH [plan options] [apply confirmations]");
      }
      String action = args[1];
      if (!action.equals("plan") && !action.equals("apply")) {
        throw new IllegalArgumentException("init action must be plan or apply");
      }

      Map<String, String> values = new HashMap<>();
      for (int index = 2; index < args.length; index += 2) {
        if (index + 1 >= args.length) {
          throw new IllegalArgumentException("every option requires a value");
        }
        String option = args[index];
        if (!ALL_OPTIONS.contains(option)) {
          throw new IllegalArgumentException("unsupported option: " + option);
        }
        if (values.putIfAbsent(option, args[index + 1]) != null) {
          throw new IllegalArgumentException("duplicate option: " + option);
        }
      }
      String config = values.get("--config");
      if (config == null || config.isBlank()) {
        throw new IllegalArgumentException("provide --config PATH");
      }

      HistoryLayout historyLayout = parseHistoryLayout(values.get("--history-layout"));
      if (action.equals("plan")) {
        requireOnly(values, Set.of("--config", "--table-group", "--history-layout"));
        return new Options(
            action,
            Path.of(config),
            values.get("--table-group"),
            historyLayout,
            null,
            null);
      }

      requireOnly(
          values,
          Set.of("--config", "--history-layout", "--confirm-table-group", "--confirm-plan"));
      String confirmGroup = values.get("--confirm-table-group");
      if (confirmGroup == null || confirmGroup.isBlank()) {
        throw new IllegalArgumentException("provide --confirm-table-group ID");
      }
      String confirmPlan = values.get("--confirm-plan");
      if (confirmPlan == null || !confirmPlan.matches("sha256:[0-9a-f]{64}")) {
        throw new IllegalArgumentException(
            "provide --confirm-plan sha256:<64 lowercase hex digits>");
      }
      return new Options(action, Path.of(config), null, historyLayout, confirmGroup, confirmPlan);
    }

    private static void requireOnly(Map<String, String> values, Set<String> allowed) {
      if (!allowed.containsAll(values.keySet())) {
        throw new IllegalArgumentException("option is not valid for this init action");
      }
    }

    private static HistoryLayout parseHistoryLayout(String value) {
      if (value == null) {
        return null;
      }
      try {
        HistoryLayout layout = HistoryLayout.valueOf(value);
        if (layout == HistoryLayout.UNDECIDED) {
          throw new IllegalArgumentException("history layout must select a configured candidate");
        }
        return layout;
      } catch (IllegalArgumentException invalidLayout) {
        throw new IllegalArgumentException("history layout must select a configured candidate");
      }
    }
  }
}
