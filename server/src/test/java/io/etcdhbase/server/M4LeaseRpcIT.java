package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import etcdserverpb.LeaseGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.admin.M3TableGroupInitializer;
import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.admin.TableGroupPlanFactory;
import io.etcdhbase.admin.config.InitializationConfiguration;
import io.etcdhbase.admin.config.InitializationConfigurationLoader;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.M3RuntimeConfigurationLoader;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Verifies J40 Lease RPCs against a disposable HBase cluster, both layouts, and runtime restart. */
@Timeout(600)
class M4LeaseRpcIT {
  private static final int MAX_DRIVER_DIAGNOSTIC_BYTES = 8 * 1024;
  private static final int TEST_OWNER_LEASE_MILLIS = 6_000;
  private static final int TEST_HEARTBEAT_MILLIS = 1_000;
  private static final List<String> LAYOUT_FILES =
      List.of("history-table.json", "native-cell-versions.json");
  private static final RuntimeConfiguration.ResourceBudgets RESOURCES =
      new RuntimeConfiguration.ResourceBudgets(65_536, 4, 30_000,
          4 << 20, 4 << 20, 16, 0);

  @Test
  void realGoLeaseCallsMatchNativeAndSurviveRestartAcrossLayouts() throws Exception {
    RunContext run = prepareRun();
    prepareNativeEvidence(run);
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      for (int index = 0; index < LAYOUT_FILES.size(); index++) {
        String layoutFile = LAYOUT_FILES.get(index);
        String layout = index == 0 ? "HISTORY" : "NATIVE";
        runLayout(cluster, clientPort, layoutFile, layout, run);
      }
    }
    compare(run, "HISTORY");
    compare(run, "NATIVE");
  }

  static void verifyGoLeaseBindingsMatchNativeAndSurviveRestartAcrossLayouts() throws Exception {
    RunContext run = prepareBindingRun();
    prepareNativeBindingEvidence(run);
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      for (int index = 0; index < LAYOUT_FILES.size(); index++) {
        String layoutFile = LAYOUT_FILES.get(index);
        String layout = index == 0 ? "HISTORY" : "NATIVE";
        runBindingLayout(cluster, clientPort, layoutFile, layout, run);
      }
    }
    compareBindings(run, "HISTORY");
    compareBindings(run, "NATIVE");
  }

  private static void runLayout(TemporaryHBaseClusterProcess cluster, int clientPort,
      String layoutFile, String layout, RunContext run) throws Exception {
    InitializationConfiguration initialization = initialization(layoutFile, clientPort);
    var source = TableGroupPlanFactory.create(initialization, "local", null);
    M3TableGroupPlan plan = M3TableGroupPlan.from(source);
    var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
        source.tableGroupId(), source.hbaseProfile().id());
    try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
            cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            1, 1, Duration.ofSeconds(30));
         var credentials = new io.etcdhbase.spi.identity.BackendCredentials("m4-initializer", new char[0])) {
      var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
          .get(30, TimeUnit.SECONDS);
      M3RuntimeConfiguration firstActivation = activation("1");
      var initialized = M3TableGroupInitializer.initializeFresh(plan,
          "sha256:" + source.fingerprint(), identity, initializer, firstActivation.pinConfiguration());
      ControlSnapshot preUpgrade = controlSnapshot(plan, initializer, identity);
      assertEquals(CommitState.FORMAT_VERSION, preUpgrade.state().formatVersion());
      long staleWriterNow = System.currentTimeMillis();
      CommitState staleWriterClaim = preUpgrade.state().claimOwner("pre-m4-writer", staleWriterNow,
          Math.addExact(staleWriterNow, 60_000));
      RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
          List.of(new RuntimeConfiguration.Listener("m4-local", "127.0.0.1", 0, source.tableGroupId())),
          initialization.hbaseProfiles(), initialization.saltProfiles(), initialization.tableGroups(), RESOURCES);

      RuntimeServer first = RuntimeServer.startM3(configuration, firstActivation);
      String prefix = run.prefix();
      long explicitId;
      try {
        assertTrue(first.dataReady());
        assertOldWriterCannotPublish(plan, initializer, identity, preUpgrade, staleWriterClaim);
        runTargetDriver(run, layout, first.boundListeners().getFirst().port());
        assertEquals(1, assertControlAndTableAreAuxiliary(plan, initializer, identity));
        explicitId = leaseIdForPrefix(prefix, "explicit");
        assertTrue(explicitId > 0);
      } finally {
        first.close();
      }

      M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
      if (layout.equals("HISTORY")) {
        assertM4CannotBeDisabledAfterFormatUpgrade(configuration, activation("3", false));
      }
      M3RuntimeConfiguration secondActivation = activation("2");
      try (RuntimeServer restarted = RuntimeServer.startM3(configuration, secondActivation)) {
        assertTrue(restarted.dataReady());
        Rpc.LeaseTimeToLiveResponse ttl = timeToLive(restarted, explicitId, true);
        assertEquals(explicitId, ttl.getID());
        assertTrue(ttl.getTTL() > 0);
        assertEquals(30, ttl.getGrantedTTL());
        assertEquals(0, ttl.getKeysCount());
        assertEquals(initialized.clusterId(), ttl.getHeader().getClusterId());
        assertEquals(1, ttl.getHeader().getRevision());
        Rpc.LeaseLeasesResponse leases = leases(restarted);
        assertTrue(leases.getLeasesList().stream().anyMatch(value -> value.getID() == explicitId));
        assertEquals(initialized.clusterId(), leases.getHeader().getClusterId());
        assertEquals(1, leases.getHeader().getRevision());
        assertEquals(2, leases.getHeader().getMemberId());
      }
    }
  }

  private static void runBindingLayout(TemporaryHBaseClusterProcess cluster, int clientPort,
      String layoutFile, String layout, RunContext run) throws Exception {
    InitializationConfiguration initialization = initialization(layoutFile, clientPort);
    var source = TableGroupPlanFactory.create(initialization, "local", null);
    M3TableGroupPlan plan = M3TableGroupPlan.from(source);
    var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
        source.tableGroupId(), source.hbaseProfile().id());
    try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
            cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            1, 1, Duration.ofSeconds(30));
         var credentials = new io.etcdhbase.spi.identity.BackendCredentials("m4-initializer", new char[0])) {
      var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
          .get(30, TimeUnit.SECONDS);
      M3RuntimeConfiguration firstActivation = activation("1");
      var initialized = M3TableGroupInitializer.initializeFresh(plan,
          "sha256:" + source.fingerprint(), identity, initializer, firstActivation.pinConfiguration());
      RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
          List.of(new RuntimeConfiguration.Listener("m4-local", "127.0.0.1", 0, source.tableGroupId())),
          initialization.hbaseProfiles(), initialization.saltProfiles(), initialization.tableGroups(), RESOURCES);

      long publishedRevision;
      RuntimeServer first = RuntimeServer.startM3(configuration, firstActivation);
      try {
        assertTrue(first.dataReady());
        runBindingTargetDriver(run, layout, first.boundListeners().getFirst().port());
        publishedRevision = assertControlAndTableAreAuxiliary(plan, initializer, identity);
        assertTrue(publishedRevision > 1, "binding scenario should advance the KV revision");
      } finally {
        first.close();
      }

      M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
      long leaseB = leaseIdForPrefix(run.prefix(), "binding-b");
      try (RuntimeServer restarted = RuntimeServer.startM3(configuration, activation("2"))) {
        assertTrue(restarted.dataReady());
        Rpc.LeaseTimeToLiveResponse ttl = timeToLive(restarted, leaseB, true);
        assertEquals(leaseB, ttl.getID());
        assertTrue(ttl.getTTL() > 0);
        assertEquals(60, ttl.getGrantedTTL());
        assertEquals(2, ttl.getKeysCount());
        assertEquals(Set.of(
            ByteString.copyFrom(run.prefix() + "survivor/registered", StandardCharsets.UTF_8),
            ByteString.copyFrom(run.prefix() + "atomic/unselected-else", StandardCharsets.UTF_8)),
            Set.copyOf(ttl.getKeysList()));
        assertEquals(initialized.clusterId(), ttl.getHeader().getClusterId());
        assertEquals(publishedRevision, ttl.getHeader().getRevision());
      }
    }
  }

  private static ControlSnapshot controlSnapshot(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity) throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
      return HBaseCommitControlStore.open(provider, identity, plan.system(), scope).join()
          .read(scope).join().orElseThrow();
    }
  }

  private static void assertOldWriterCannotPublish(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      ControlSnapshot preUpgrade, CommitState staleWriterClaim) throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
      var control = HBaseCommitControlStore.open(provider, identity, plan.system(), scope).join();
      assertFalse(control.compareAndSet(preUpgrade, staleWriterClaim, scope).join());
      assertTrue(control.read(scope).join().orElseThrow().state().supportsAuxiliaryRecords());
    }
  }

  private static void assertM4CannotBeDisabledAfterFormatUpgrade(
      RuntimeConfiguration configuration, M3RuntimeConfiguration disabledActivation) {
    IOException failure = assertThrows(IOException.class,
        () -> RuntimeServer.startM3(configuration, disabledActivation));
    assertTrue(causeChainContains(failure,
        "M4 Lease activation must remain enabled for a table group with auxiliary state"));
  }

  private static boolean causeChainContains(Throwable failure, String expected) {
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (current.getMessage() != null && current.getMessage().contains(expected)) return true;
    }
    return false;
  }

  private static long assertControlAndTableAreAuxiliary(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity) throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
      var control = HBaseCommitControlStore.open(provider, identity, plan.system(), scope).join();
      var state = control.read(scope).join().orElseThrow().state();
      assertTrue(state.supportsAuxiliaryRecords());
      assertTrue(state.supportsLeaseBindings());
      assertTrue(state.publishedSequence() > 0);
      var connection = provider.connectionFor(identity).toCompletableFuture().join();
      var descriptor = connection.getAdmin().getDescriptor(plan.system().table()).join();
      assertTrue(plan.system().supportsAuxiliary(descriptor));
      assertTrue(plan.system().supportsLeaseBindings(descriptor));
      return state.publishedRevision();
    }
  }

  private static Rpc.LeaseTimeToLiveResponse timeToLive(RuntimeServer server, long id,
      boolean includeKeys) throws Exception {
    var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.boundListeners().getFirst().port())
        .usePlaintext().build();
    try {
      return LeaseGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
          .leaseTimeToLive(Rpc.LeaseTimeToLiveRequest.newBuilder().setID(id).setKeys(includeKeys).build());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static Rpc.LeaseLeasesResponse leases(RuntimeServer server) throws Exception {
    var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.boundListeners().getFirst().port())
        .usePlaintext().build();
    try {
      return LeaseGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
          .leaseLeases(Rpc.LeaseLeasesRequest.getDefaultInstance());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static InitializationConfiguration initialization(String file, int clientPort) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Path path = WireProcesses.repositoryRoot().resolve("config/examples/j05").resolve(file);
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(path));
    ObjectNode profile = (ObjectNode) tree.withArray("hbase_profiles").get(0);
    profile.put("namespace", "m4_" + UUID.randomUUID().toString().replace("-", ""))
        .put("quorum", "127.0.0.1").put("client_port", clientPort);
    ObjectNode system = (ObjectNode) tree.withArray("table_groups").get(0).get("system_table");
    boolean hasResultCategory = false;
    for (var category : system.withArray("record_categories")) {
      if (category.asText().equals("RESULT")) {
        hasResultCategory = true;
        break;
      }
    }
    if (!hasResultCategory) {
      system.withArray("record_categories").add("RESULT");
    }
    return new InitializationConfigurationLoader()
        .load(new ByteArrayInputStream(mapper.writeValueAsBytes(tree)));
  }

  private static M3RuntimeConfiguration activation(String memberId) throws Exception {
    return activation(memberId, true);
  }

  private static M3RuntimeConfiguration activation(String memberId, boolean enableLeaseApi)
      throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Path path = WireProcesses.repositoryRoot().resolve("config/examples/m4/activation.json");
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(path));
    // MiniCluster admin procedures can exceed a 400 ms heartbeat timeout under Maven load.
    tree.put("member_id", memberId).put("owner_lease_ms", TEST_OWNER_LEASE_MILLIS)
        .put("heartbeat_ms", TEST_HEARTBEAT_MILLIS)
        .put("m4_lease_api_enabled", enableLeaseApi);
    return new M3RuntimeConfigurationLoader()
        .load(new ByteArrayInputStream(mapper.writeValueAsBytes(tree)));
  }

  private static long leaseIdForPrefix(String prefix, String purpose) throws Exception {
    byte[] digest = MessageDigest.getInstance("SHA-256")
        .digest((prefix + ":" + purpose).getBytes(StandardCharsets.UTF_8));
    long id = ByteBuffer.wrap(digest).getLong() & Long.MAX_VALUE;
    return id == 0 ? 1 : id;
  }

  private static RunContext prepareRun() throws IOException {
    Path root = WireProcesses.repositoryRoot();
    String runId = System.getenv().getOrDefault("M4_RUN_ID", "rpc-" + UUID.randomUUID());
    if (!runId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException("invalid M4 fixture run ID");
    }
    String configuredRoot = System.getenv("M4_ARTIFACT_ROOT");
    Path artifacts = configuredRoot == null
        ? root.resolve(".artifacts/m4").resolve(runId) : Path.of(configuredRoot).toAbsolutePath();
    if (!artifacts.normalize().startsWith(root.resolve(".artifacts/m4").toAbsolutePath().normalize())
        || !artifacts.getFileName().toString().equals(runId)) {
      throw new IllegalArgumentException("M4 fixture output must be under its run directory");
    }
    Files.createDirectories(artifacts);
    return new RunContext(root, artifacts, runId,
        "etcd-hbase-m4-test/" + runId + "/", artifacts.resolve("native-trace.jsonl"));
  }

  private static RunContext prepareBindingRun() throws IOException {
    RunContext parent = prepareRun();
    Path artifacts = parent.artifacts().resolve("j41");
    Files.createDirectories(artifacts);
    return new RunContext(parent.root(), artifacts, parent.runId(),
        parent.prefix() + "bindings/", artifacts.resolve("native-trace.jsonl"));
  }

  private static void prepareNativeEvidence(RunContext run) throws Exception {
    if (!Files.exists(run.nativeTrace())) {
      runDriver(run, run.artifacts().resolve("native.log"), "run",
          "--native-data-dir", run.artifacts().resolve("native-data").toString(),
          "--scope", "native-etcd", "--prefix", run.prefix(),
          "--trace", run.nativeTrace().toString(),
          "--metadata", run.artifacts().resolve("native-metadata.json").toString());
    }
  }

  private static void prepareNativeBindingEvidence(RunContext run) throws Exception {
    if (!Files.exists(run.nativeTrace())) {
      runBindingDriver(run, run.artifacts().resolve("native.log"), "run",
          "--native-data-dir", run.artifacts().resolve("native-data").toString(),
          "--scope", "native-etcd", "--prefix", run.prefix(),
          "--trace", run.nativeTrace().toString(),
          "--metadata", run.artifacts().resolve("native-metadata.json").toString());
    }
  }

  private static void runTargetDriver(RunContext run, String layout, int port) throws Exception {
    String suffix = layout.toLowerCase(java.util.Locale.ROOT);
    runDriver(run, run.artifacts().resolve("target-" + suffix + ".log"), "run",
        "--endpoint", "127.0.0.1:" + port, "--scope", "target-hbase", "--layout", layout,
        "--prefix", run.prefix(), "--trace", run.artifacts().resolve("target-" + suffix + "-trace.jsonl").toString(),
        "--metadata", run.artifacts().resolve("target-" + suffix + "-metadata.json").toString());
  }

  private static void runBindingTargetDriver(RunContext run, String layout, int port) throws Exception {
    String suffix = layout.toLowerCase(java.util.Locale.ROOT);
    runBindingDriver(run, run.artifacts().resolve("target-" + suffix + ".log"), "run",
        "--endpoint", "127.0.0.1:" + port, "--scope", "target-hbase", "--layout", layout,
        "--prefix", run.prefix(), "--trace", run.artifacts().resolve("target-" + suffix + "-trace.jsonl").toString(),
        "--metadata", run.artifacts().resolve("target-" + suffix + "-metadata.json").toString());
  }

  private static void compare(RunContext run, String layout) throws Exception {
    String suffix = layout.toLowerCase(java.util.Locale.ROOT);
    runDriver(run, run.artifacts().resolve("compare-" + suffix + ".log"), "compare",
        "--left", run.nativeTrace().toString(),
        "--right", run.artifacts().resolve("target-" + suffix + "-trace.jsonl").toString());
  }

  private static void compareBindings(RunContext run, String layout) throws Exception {
    String suffix = layout.toLowerCase(java.util.Locale.ROOT);
    runBindingDriver(run, run.artifacts().resolve("compare-" + suffix + ".log"), "compare-bindings",
        "--left", run.nativeTrace().toString(),
        "--right", run.artifacts().resolve("target-" + suffix + "-trace.jsonl").toString());
  }

  private static void runDriver(RunContext run, Path log, String... arguments) throws Exception {
    runDriverWithScenario(run, log, "m4-lease.yaml", arguments);
  }

  private static void runBindingDriver(RunContext run, Path log, String... arguments) throws Exception {
    runDriverWithScenario(run, log, "m4-lease-bindings.yaml", arguments);
  }

  private static void runDriverWithScenario(RunContext run, Path log, String scenarioFile,
      String... arguments) throws Exception {
    var command = new java.util.ArrayList<String>();
    command.add(run.root().resolve("scripts/dev/go-program.sh").toString());
    command.add("m4driver");
    if (arguments.length > 0 && arguments[0].equals("run")) {
      command.addAll(List.of("run", "--scenario",
          run.root().resolve("testkit/scenarios").resolve(scenarioFile).toString()));
      command.addAll(List.of(arguments).subList(1, arguments.length));
    } else {
      command.addAll(List.of(arguments));
    }
    ProcessBuilder builder = new ProcessBuilder(command).directory(run.root().toFile())
        .redirectErrorStream(true).redirectOutput(log.toFile());
    builder.environment().put("M4_RUN_ID", run.runId());
    String source = System.getenv("M4_SOURCE_COMMIT");
    if (source != null) builder.environment().put("M4_SOURCE_COMMIT", source);
    Process process = builder.start();
    try {
      if (!process.waitFor(180, TimeUnit.SECONDS)) {
        throw new IllegalStateException("M4 driver timed out; inspect the local run log");
      }
      int exitCode = process.exitValue();
      if (exitCode != 0) {
        assertEquals(0, exitCode, "M4 Go driver failed; last log bytes:\n" + tail(log));
      }
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      }
    }
  }

  private static String tail(Path log) {
    try {
      long size = Files.size(log);
      long offset = Math.max(0, size - MAX_DRIVER_DIAGNOSTIC_BYTES);
      try (var input = Files.newInputStream(log)) {
        input.skipNBytes(offset);
        byte[] bytes = input.readNBytes(MAX_DRIVER_DIAGNOSTIC_BYTES);
        return (offset == 0 ? "" : "[earlier log output omitted]\n")
            + new String(bytes, StandardCharsets.UTF_8);
      }
    } catch (IOException failure) {
      return "driver log unavailable (" + failure.getClass().getSimpleName() + ")";
    }
  }

  private record RunContext(Path root, Path artifacts, String runId, String prefix, Path nativeTrace) {}
}
