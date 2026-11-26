package io.etcdhbase.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.core.auth.AuthDeploymentRegistryAdmin;
import io.etcdhbase.core.auth.AuthDeploymentRegistryStore;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.M3RuntimeConfigurationLoader;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfigurationLoader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Operator entry point for the complete, durable authentication deployment inventory. */
public final class AuthRegistryAdminCommand {
  private static final ObjectMapper JSON = new ObjectMapper();

  private AuthRegistryAdminCommand() {}

  public static void main(String[] args) {
    int result = run(args, System.out, System.err);
    if (result != 0) System.exit(result);
  }

  static int run(String[] args, PrintStream output, PrintStream error) {
    return run(args, output, error, AuthRegistryAdminCommand::open);
  }

  @FunctionalInterface
  interface Connector {
    Handle open(RuntimeConfiguration config, M3RuntimeConfiguration activation, RequestScope scope)
        throws Exception;
  }

  interface Handle extends AutoCloseable {
    AuthDeploymentRegistryStore store();
    @Override void close();
  }

  static int run(String[] args, PrintStream output, PrintStream error, Connector connector) {
    boolean writeAttempted = false;
    try {
      Options options = Options.parse(args);
      RuntimeConfiguration config;
      M3RuntimeConfiguration activation;
      try (var input = Files.newInputStream(options.config())) {
        config = new RuntimeConfigurationLoader().load(input);
      }
      try (var input = Files.newInputStream(options.activation())) {
        activation = new M3RuntimeConfigurationLoader().load(input);
      }
      if (config.authentication() == null) throw new IllegalArgumentException("authentication is required");
      List<AuthDeploymentRegistry.Group> inventory = null;
      if (options.inventory() != null) {
        try (var input = Files.newInputStream(options.inventory())) {
          inventory = AuthRegistryInventory.load(input, config);
        }
      }
      if (options.action().equals("plan")) {
        output.println(JSON.writeValueAsString(inventoryJson(config.authentication().trustDomain(), inventory)));
        return 0;
      }
      var scope = RequestScope.withTimeout(Duration.ofMillis(activation.startupTimeoutMs()));
      Handle opened;
      try {
        opened = connector.open(config, activation, scope);
      } catch (Exception | Error failure) {
        scope.close();
        throw failure;
      }
      try (var cleanup = new DrainingClose(scope, opened::close, activation.shutdownTimeoutMs())) {
        var store = opened.store();
        if (options.action().equals("status")) {
          var found = store.readRegistry(scope).toCompletableFuture()
              .get(activation.startupTimeoutMs(), TimeUnit.MILLISECONDS);
          output.println(JSON.writeValueAsString(statusJson(found.orElse(null))));
          return 0;
        }
        var admin = new AuthDeploymentRegistryAdmin(store);
        writeAttempted = true;
        boolean applied = (options.action().equals("initialize")
            ? admin.initialize(config.authentication().trustDomain(), inventory, scope)
            : admin.reserveUpdate(options.expectedGeneration(), inventory, scope))
            .toCompletableFuture().get(activation.startupTimeoutMs(), TimeUnit.MILLISECONDS);
        if (!applied) {
          error.println("auth-registry conflict: inspect status before retry");
          return 3;
        }
        output.println(options.action().equals("initialize") ? "initialized" : "update reserved; inspect status");
        return 0;
      }
    } catch (Exception failure) {
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      error.println(writeAttempted
          ? "auth-registry write outcome may be unknown; inspect status before retry"
          : "auth-registry input or backend unavailable");
      return 2;
    }
  }

  private static Handle open(RuntimeConfiguration config, M3RuntimeConfiguration activation,
      RequestScope scope) throws Exception {
    var resources = new M3RuntimeGroups.GroupResources();
    try {
      var store = M3RuntimeGroups.openAuthControlStore(config, activation,
          config.authentication(), scope, resources);
      return new Handle() {
        @Override public AuthDeploymentRegistryStore store() { return store; }
        @Override public void close() { resources.close(); }
      };
    } catch (Exception | Error failure) {
      try { new DrainingClose(scope, resources::close, activation.shutdownTimeoutMs()).close(); }
      catch (Exception | Error cleanup) {
        if (cleanup instanceof InterruptedException) Thread.currentThread().interrupt();
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  /** Stop new work, then retain the backend until all physical producers and cleanup finish. */
  private record DrainingClose(RequestScope scope, Runnable release, int timeoutMs)
      implements AutoCloseable {
    @Override public void close() throws Exception {
      scope.close();
      var drained = CompletableFuture.allOf(scope.cleanupOutcomeCompletion().toCompletableFuture(),
          scope.physicalProducersCompletion().toCompletableFuture());
      var closed = drained.thenRunAsync(release, task ->
          Thread.ofVirtual().name("auth-registry-cleanup").start(task));
      // On timeout the continuation still owns the handle and closes it when the producer stops.
      closed.get(timeoutMs, TimeUnit.MILLISECONDS);
    }
  }

  private static ObjectNode inventoryJson(String trustDomain, List<AuthDeploymentRegistry.Group> groups) {
    var result = JSON.createObjectNode().put("trust_domain", trustDomain);
    var values = result.putArray("groups");
    for (var group : groups) {
      values.addObject().put("table_group_id", group.tableGroupId())
          .put("provider_id", group.providerId()).put("backend_identity_domain", group.backendIdentityDomain())
          .put("connection_profile_id", group.connectionProfileId())
          .put("dataset_digest", HexFormat.of().formatHex(group.datasetDigest().bytes()))
          .put("configuration_digest", HexFormat.of().formatHex(group.configurationDigest().bytes()))
          .put("enabled", group.enabled());
    }
    return result;
  }

  private static ObjectNode statusJson(AuthDeploymentRegistry current) {
    if (current == null) return JSON.createObjectNode().put("initialized", false);
    ObjectNode result = inventoryJson(current.trustDomain(), current.groups());
    result.put("initialized", true).put("configuration_generation", current.configurationGeneration())
        .put("revision", current.revision());
    if (current.revocation() != null) result.put("revocation_state", current.revocation().state().name());
    if (current.configurationUpdate() != null) {
      var update = current.configurationUpdate();
      var operation = result.putObject("configuration_update").put("operation_sequence", update.operationSequence());
      operation.set("replacement", inventoryJson(current.trustDomain(), update.replacement()).get("groups"));
      var targets = operation.putArray("targets");
      update.targets().forEach(target -> targets.addObject().put("table_group_id", target.tableGroupId())
          .put("barrier_generation", target.barrierGeneration()).put("installed", target.installed()));
    }
    return result;
  }

  private record Options(String action, Path config, Path activation, Path inventory,
      long expectedGeneration) {
    static Options parse(String[] args) {
      if (args == null || args.length < 1) throw new IllegalArgumentException();
      String action = args[0];
      if (!List.of("plan", "status", "initialize", "update").contains(action)) {
        throw new IllegalArgumentException();
      }
      Map<String, String> values = new HashMap<>();
      for (int index = 1; index < args.length; index += 2) {
        if (index + 1 >= args.length || !List.of("--config", "--m3-config", "--inventory",
            "--expected-generation").contains(args[index])
            || values.putIfAbsent(args[index], args[index + 1]) != null) throw new IllegalArgumentException();
      }
      boolean needsInventory = !action.equals("status");
      if (!values.containsKey("--config") || !values.containsKey("--m3-config")
          || values.containsKey("--inventory") != needsInventory
          || values.containsKey("--expected-generation") != action.equals("update")) {
        throw new IllegalArgumentException();
      }
      long expected = action.equals("update") ? Long.parseLong(values.get("--expected-generation")) : 0;
      if (action.equals("update") && expected < 1) throw new IllegalArgumentException();
      return new Options(action, Path.of(values.get("--config")), Path.of(values.get("--m3-config")),
          needsInventory ? Path.of(values.get("--inventory")) : null, expected);
    }
  }
}
