package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import etcdserverpb.AuthGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.core.auth.*;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.admin.TableGroupPlanFactory;
import io.etcdhbase.grpc.service.*;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.storage.AuthControlBinding;
import io.etcdhbase.hbase.storage.HBaseAuthSessionStore;
import io.etcdhbase.server.auth.*;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfigurationLoader;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.M3RuntimeConfigurationLoader;
import io.etcdhbase.server.commit.M3TableGroupRuntime;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AuthRegistryAdminCommandTest {
  @TempDir Path temporary;
  private final ObjectMapper json = new ObjectMapper();

  @Test
  void productionLeaseRequiresVersionedSecureIdentityAndIsolatedRecovery() throws Exception {
    Path configFile = configuration("local", 60_000);
    M3RuntimeConfiguration activation;
    try (var input = Files.newInputStream(WireProcesses.repositoryRoot()
        .resolve("config/examples/m4/activation.json"))) {
      activation = new M3RuntimeConfigurationLoader().load(input);
    }
    RuntimeConfiguration secure;
    try (var input = Files.newInputStream(configFile)) {
      secure = new RuntimeConfigurationLoader().load(input);
    }
    assertDoesNotThrow(() -> M3RuntimeGroups.requireProductionLeaseCapabilities(secure, activation));
    var withoutAuth = new RuntimeConfiguration(secure.schemaVersion(), secure.listeners(),
        secure.hbaseProfiles(), secure.saltProfiles(), secure.tableGroups(), secure.resources(), null);
    assertThrows(M3RuntimeConfiguration.LeaseActivationUnavailable.class,
        () -> M3RuntimeGroups.requireProductionLeaseCapabilities(withoutAuth, activation));
    var tree = (ObjectNode) json.readTree(Files.readAllBytes(configFile));
    var client = (ObjectNode) tree.path("authentication").path("client_profiles").get(0);
    client.remove("backend_identity_domain");
    Files.write(configFile, json.writeValueAsBytes(tree));
    try (var input = Files.newInputStream(configFile)) {
      var missingDomain = new RuntimeConfigurationLoader().load(input);
      assertThrows(M3RuntimeConfiguration.LeaseActivationUnavailable.class,
          () -> M3RuntimeGroups.requireProductionLeaseCapabilities(missingDomain, activation));
    }
    client.put("backend_identity_domain", "realm").remove("credential_versions");
    Files.write(configFile, json.writeValueAsBytes(tree));
    try (var input = Files.newInputStream(configFile)) {
      var unversioned = new RuntimeConfigurationLoader().load(input);
      assertThrows(M3RuntimeConfiguration.LeaseActivationUnavailable.class,
          () -> M3RuntimeGroups.requireProductionLeaseCapabilities(unversioned, activation));
    }
    try (var input = Files.newInputStream(WireProcesses.repositoryRoot()
            .resolve("config/examples/m3/server.json"));
        var scope = RequestScope.withTimeout(Duration.ofSeconds(3))) {
      var development = new RuntimeConfigurationLoader().load(input);
      var plan = M3TableGroupPlan.from(TableGroupPlanFactory.create(development.initialization(), "local", null));
      assertThrows(M3RuntimeConfiguration.LeaseActivationUnavailable.class, () -> M3TableGroupRuntime.open(
          plan, activation, development.resources(), mock(HBaseConnectionFactory.class),
          mock(AuthorizedBackendIdentity.class), mock(BudgetManager.class), mock(ScanScheduler.class), scope));
    }
  }

  @Test
  void disabledGroupCanRecoverLaterConfigurationUpdates() throws Exception {
    Path oldConfig = configuration("local", 60_000);
    Path newConfig = configuration("local", 120_000);
    Path oldInventory = singleDisabledInventory(oldConfig);
    Path newInventory = singleDisabledInventory(newConfig);
    Path activation = WireProcesses.repositoryRoot().resolve("config/examples/m3/activation.json");
    var backend = new MockControl();
    var connector = backend.connector();
    assertEquals(0, command(connector, "initialize", oldConfig, activation, oldInventory, null).code());
    var resources = new M3RuntimeGroups.GroupResources();
    try {
      var sessions = service(backend.store, backend.registry().groups().getFirst(), oldConfig,
          new MockCommitControl(), resources);
      assertFalse(authStatus(sessions).getEnabled());
      resources.startRecovery(3_000);
      resources.initialRecoveryReadiness().get(5, TimeUnit.SECONDS);
      assertEquals(0, command(connector, "update", oldConfig, activation, newInventory, "1").code());
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (backend.registry().configurationGeneration() == 1 && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
      assertEquals(2, backend.registry().configurationGeneration());
    } finally {
      resources.stopRecovery();
      resources.recoveryDrained().get(5, TimeUnit.SECONDS);
      resources.close();
    }
  }

  @Test
  void timedOutWriteKeepsControlResourcesUntilItsPhysicalProducerDrains() throws Exception {
    Path first = configuration("local", 60_000);
    Path second = configuration("other", 60_000);
    var activationJson = (ObjectNode) json.readTree(Files.readAllBytes(WireProcesses.repositoryRoot()
        .resolve("config/examples/m3/activation.json")));
    activationJson.put("startup_timeout_ms", 100).put("shutdown_timeout_ms", 1_000);
    Path activation = temporary.resolve("activation.json");
    Files.write(activation, json.writeValueAsBytes(activationJson));
    var producer = new CompletableFuture<Boolean>();
    var closed = new CompletableFuture<Void>();
    var store = mock(AuthDeploymentRegistryStore.class);
    doAnswer(invocation -> {
      RequestScope scope = invocation.getArgument(1);
      scope.trackPhysicalProducer(producer);
      return producer;
    }).when(store).createRegistryIfAbsent(any(), any());
    AuthRegistryAdminCommand.Connector connector = (configuration, runtime, scope) ->
        new AuthRegistryAdminCommand.Handle() {
          @Override public AuthDeploymentRegistryStore store() { return store; }
          @Override public void close() { closed.complete(null); }
        };

    Result result = command(connector, "initialize", first, activation, inventory(first, second), null);
    assertEquals(2, result.code());
    assertTrue(result.error().contains("outcome may be unknown"));
    assertFalse(closed.isDone());
    producer.complete(true);
    closed.get(5, TimeUnit.SECONDS);
  }

  @Test
  void inventoryRejectsDifferentControlBindingsAndDuplicateGroupsBeforeBackendAccess() throws Exception {
    Path first = configuration("local", 60_000);
    Path second = configuration("other", 60_000);
    var modified = (ObjectNode) json.readTree(Files.readAllBytes(second));
    ((ObjectNode) modified.path("authentication").path("auth_control")).put("table_name", "different_control");
    Files.write(second, json.writeValueAsBytes(modified));
    Path activation = WireProcesses.repositoryRoot().resolve("config/examples/m3/activation.json");
    AtomicBoolean contacted = new AtomicBoolean();
    AuthRegistryAdminCommand.Connector connector = (config, runtime, scope) -> {
      contacted.set(true);
      throw new AssertionError("must not connect for invalid inventory");
    };
    assertEquals(2, command(connector, "initialize", first, activation, inventory(first, second), null).code());
    assertFalse(contacted.get());
    var duplicate = json.createObjectNode();
    var groups = duplicate.putArray("groups");
    for (int index = 0; index < 2; index++) {
      groups.addObject().put("configuration_file", first.toString()).put("table_group_id", "local")
          .put("cluster_id", "42").put("enabled", true);
    }
    Path duplicateFile = temporary.resolve("duplicate.json");
    Files.write(duplicateFile, json.writeValueAsBytes(duplicate));
    assertEquals(2, command(connector, "initialize", first, activation, duplicateFile, null).code());
    assertFalse(contacted.get());
  }

  @Test
  void commandPersistsInventoryAndWorkersRecoverAnUnknownUpdateBeforeStatusChanges() throws Exception {
    Path oldA = configuration("local", 60_000);
    Path oldB = configuration("other", 60_000);
    Path newA = configuration("local", 120_000);
    Path initial = inventory(oldA, oldB);
    Path replacement = inventory(newA, oldB);
    Path activation = WireProcesses.repositoryRoot().resolve("config/examples/m3/activation.json");
    var backend = new MockControl();
    var connector = backend.connector();
    assertEquals(0, command(connector, "initialize", oldA, activation, initial, null).code());
    assertEquals(3, command(connector, "initialize", oldA, activation, initial, null).code());
    var status = command(connector, "status", oldA, activation, null, null);
    assertEquals(1, json.readTree(status.output()).path("configuration_generation").asLong());
    assertEquals(2, json.readTree(status.output()).path("groups").size());

    var oldGroups = backend.registry().groups();
    var oldLocal = oldGroups.stream().filter(group -> group.tableGroupId().equals("local")).findFirst().orElseThrow();
    var other = oldGroups.stream().filter(group -> group.tableGroupId().equals("other")).findFirst().orElseThrow();
    var localControl = new MockCommitControl();
    var remoteControl = new MockCommitControl();
    remoteControl.reject.set(true);
    var localResources = new M3RuntimeGroups.GroupResources();
    var remoteResources = new M3RuntimeGroups.GroupResources();
    var nextResources = new M3RuntimeGroups.GroupResources();
    try {
      var local = service(backend.store, oldLocal, oldA, localControl, localResources);
      service(backend.store, other, oldB, remoteControl, remoteResources);
      assertEquals(1, authStatus(local).getAuthRevision());

      backend.unknownNextWrite = true;
      var update = command(connector, "update", oldA, activation, replacement, "1");
      assertEquals(2, update.code());
      assertTrue(update.error().contains("outcome may be unknown"));
      status = command(connector, "status", oldA, activation, null, null);
      assertTrue(json.readTree(status.output()).has("configuration_update"));
      assertEquals(Status.Code.UNAVAILABLE, assertThrows(StatusRuntimeException.class,
          () -> authStatus(local)).getStatus().getCode());

      localResources.startRecovery(3_000);
      remoteResources.startRecovery(3_000);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (System.nanoTime() < deadline) {
        var operation = backend.registry().configurationUpdate();
        if (remoteControl.attempted.get() && operation != null && operation.targets().getFirst().installed()) break;
        Thread.sleep(10);
      }
      assertTrue(remoteControl.attempted.get());
      assertTrue(backend.registry().configurationUpdate().targets().getFirst().installed());
      assertFalse(remoteResources.recoveryReady());
      remoteControl.reject.set(false);
      remoteResources.initialRecoveryReadiness().get(5, TimeUnit.SECONDS);
      assertEquals(1, localControl.current.get().state().authorizationBarrierGeneration());
      assertEquals(1, remoteControl.current.get().state().authorizationBarrierGeneration());
      status = command(connector, "status", oldA, activation, null, null);
      assertEquals(2, json.readTree(status.output()).path("configuration_generation").asLong());
      assertFalse(json.readTree(status.output()).has("configuration_update"));
      assertEquals(Status.Code.UNAVAILABLE, assertThrows(StatusRuntimeException.class,
          () -> authStatus(local)).getStatus().getCode());
      var currentLocal = backend.registry().groups().stream()
          .filter(group -> group.tableGroupId().equals("local")).findFirst().orElseThrow();
      var next = service(backend.store, currentLocal, newA, localControl, nextResources);
      nextResources.startRecovery(3_000);
      nextResources.initialRecoveryReadiness().get(5, TimeUnit.SECONDS);
      var response = authStatus(next);
      assertTrue(response.getEnabled());
      assertEquals(2, response.getAuthRevision());
    } finally {
      for (var resources : java.util.List.of(localResources, remoteResources, nextResources)) {
        resources.stopRecovery();
        resources.recoveryDrained().get(5, TimeUnit.SECONDS);
        resources.close();
      }
    }
  }

  private AuthSessionService service(HBaseAuthSessionStore store, AuthDeploymentRegistry.Group group,
      Path configuration, MockCommitControl control, M3RuntimeGroups.GroupResources resources) throws Exception {
    try (var input = Files.newInputStream(configuration)) {
      var auth = new RuntimeConfigurationLoader().load(input).authentication();
      return M3RuntimeGroups.createAuthSessions(store, mock(CredentialEnvelopeProtector.class),
          mock(SessionIdentityManager.class), auth,
          new AuthenticationScope(group.tableGroupId(), group.connectionProfileId()), group.providerId(),
          group.backendIdentityDomain(), group.datasetDigest(), control, resources);
    }
  }

  private Rpc.AuthStatusResponse authStatus(AuthSessionService sessions) throws Exception {
    var listener = new RuntimeConfiguration.Listener("test", "127.0.0.1", 0, "local");
    var budgets = new RuntimeConfiguration.ResourceBudgets(65_536, 2, 3_000, 1_048_576, 2_097_152, 4, 0);
    var handler = new AuthStatusRpcHandler(sessions::readAuthConfiguration,
        scope -> CompletableFuture.completedFuture(Rpc.ResponseHeader.getDefaultInstance()));
    try (var server = Bootstrap.startConfigured(listener, budgets, new KvService(), new LeaseService(),
        new WatchService(), new AuthService((request, observer) -> {}, handler), new RequestMetrics())) {
      var channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.boundListener().port())
          .usePlaintext().build();
      try {
        return AuthGrpc.newBlockingStub(channel).withDeadlineAfter(2, TimeUnit.SECONDS)
            .authStatus(Rpc.AuthStatusRequest.getDefaultInstance());
      } finally {
        channel.shutdownNow();
        assertTrue(channel.awaitTermination(2, TimeUnit.SECONDS));
      }
    }
  }

  private Result command(AuthRegistryAdminCommand.Connector connector, String action, Path config,
      Path activation, Path inventory, String generation) {
    var args = new java.util.ArrayList<>(java.util.List.of(action, "--config", config.toString(),
        "--m3-config", activation.toString()));
    if (inventory != null) args.addAll(java.util.List.of("--inventory", inventory.toString()));
    if (generation != null) args.addAll(java.util.List.of("--expected-generation", generation));
    var output = new ByteArrayOutputStream();
    var error = new ByteArrayOutputStream();
    int code = AuthRegistryAdminCommand.run(args.toArray(String[]::new),
        new PrintStream(output), new PrintStream(error), connector);
    return new Result(code, output.toString(), error.toString());
  }

  private Path inventory(Path first, Path second) throws Exception {
    var root = json.createObjectNode();
    var groups = root.putArray("groups");
    groups.addObject().put("configuration_file", first.toString()).put("table_group_id", "local")
        .put("cluster_id", "42").put("enabled", true);
    groups.addObject().put("configuration_file", second.toString()).put("table_group_id", "other")
        .put("cluster_id", "43").put("enabled", true);
    Path path = Files.createTempFile(temporary, "inventory", ".json");
    Files.write(path, json.writeValueAsBytes(root));
    return path;
  }

  private Path singleDisabledInventory(Path configuration) throws Exception {
    var root = json.createObjectNode();
    root.putArray("groups").addObject().put("configuration_file", configuration.toString())
        .put("table_group_id", "local").put("cluster_id", "42").put("enabled", false);
    Path path = Files.createTempFile(temporary, "disabled-inventory", ".json");
    Files.write(path, json.writeValueAsBytes(root));
    return path;
  }

  private Path configuration(String group, long ttl) throws Exception {
    var root = (ObjectNode) json.readTree(Files.readAllBytes(
        WireProcesses.repositoryRoot().resolve("config/examples/m3/server.json")));
    ((ObjectNode) root.withArray("hbase_profiles").get(0)).put("authentication_mode", "CREDENTIAL_REFERENCE")
        .put("credential_reference", "env://CONTROL");
    ((ObjectNode) root.withArray("table_groups").get(0)).put("id", group).put("identity_provider_id", "provider");
    var listener = (ObjectNode) root.withArray("listeners").get(0);
    listener.put("table_group_id", group).putObject("tls")
        .put("certificate_file", "/cert").put("private_key_file", "/key");
    var auth = root.putObject("authentication").put("trust_domain", "trust").put("session_ttl_millis", ttl)
        .put("session_slots", 4).put("maximum_identity_containers", 4).put("secret_directory", "/secrets");
    var control = auth.putObject("auth_control").put("hbase_profile_id", "local-hbase")
        .put("salt_profile_id", "crc32-sixteen-buckets").put("table_name", "auth_control")
        .put("provider_id", "control").put("client_profile_id", "client");
    credential(control.putObject("identity"));
    var client = auth.putArray("client_profiles").addObject().put("id", "client").put("hbase_profile_id", "local-hbase")
        .put("authenticator_id", "kerberos-password").put("hbase_site_file", "/site")
        .put("backend_identity_domain", "realm");
    client.putArray("credential_versions").addObject().put("principal", "alice@EXAMPLE")
        .put("generation", 1).put("credential_reference", "env://ALICE");
    auth.putArray("endpoint_bindings").addObject().put("table_group_id", group)
        .put("provider_id", "provider").put("client_profile_id", "client");
    var background = auth.putArray("background_identities").addObject().put("table_group_id", group)
        .put("provider_id", "provider").put("client_profile_id", "client");
    credential(background.putObject("identity"));
    background.putArray("allowed_table_group_ids").add(group);
    auth.putObject("credential_key_ring").put("active_key_id", "key").putArray("keys")
        .addObject().put("key_id", "key").put("secret_reference", "secret://key");
    Path path = Files.createTempFile(temporary, "runtime", ".json");
    Files.write(path, json.writeValueAsBytes(root));
    return path;
  }

  private void credential(ObjectNode identity) {
    identity.put("source", "CREDENTIAL_REFERENCE").put("credential_reference", "env://CONTROL");
  }

  private record Result(int code, String output, String error) {}

  private static final class MockCommitControl implements CommitControlStore {
    private final AtomicBoolean reject = new AtomicBoolean();
    private final AtomicBoolean attempted = new AtomicBoolean();
    private final AtomicReference<ControlSnapshot> current = new AtomicReference<>(snapshot(
        CommitState.initial(ByteKey.copyOf(new byte[32]), 1)
            .claimOwner("owner", 1, System.currentTimeMillis() + 60_000)
            .enableAuthorizationBarrier("owner", 1, 2)));

    @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(current.get()));
    }
    @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState initial, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<Boolean> compareAndSet(ControlSnapshot old, CommitState replacement,
        RequestScope scope) {
      attempted.set(true);
      if (reject.get()) return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
      return CompletableFuture.completedFuture(current.compareAndSet(old, snapshot(replacement)));
    }
    private static ControlSnapshot snapshot(CommitState state) {
      return new ControlSnapshot(state, java.nio.ByteBuffer.allocate(Long.BYTES).putLong(state.stateVersion()).array());
    }
  }

  private static final class MockControl {
    private final AsyncConnection connection = mock(AsyncConnection.class);
    private final AsyncTable<?> table = mock(AsyncTable.class);
    private final AuthControlBinding binding = new AuthControlBinding(TableName.valueOf("etcd_candidate", "auth_control"),
        "trust", new HashSaltStrategy(new SaltProfile("crc32-bucket-v1", 1, 16, 2)));
    private byte[] value;
    private boolean unknownNextWrite;
    private HBaseAuthSessionStore store;

    @SuppressWarnings({"unchecked", "rawtypes"})
    MockControl() {
      var builder = mock(AsyncTableBuilder.class, RETURNS_SELF);
      var admin = mock(AsyncAdmin.class);
      doReturn(builder).when(connection).getTableBuilder(binding.table());
      doReturn(table).when(builder).build();
      doReturn(admin).when(connection).getAdmin();
      doReturn(CompletableFuture.completedFuture(binding.descriptor())).when(admin).getDescriptor(binding.table());
      doAnswer(invocation -> {
        Get get = invocation.getArgument(0);
        synchronized (this) {
          var result = value == null ? org.apache.hadoop.hbase.client.Result.EMPTY_RESULT
              : org.apache.hadoop.hbase.client.Result.create(new org.apache.hadoop.hbase.Cell[] {
                  new KeyValue(get.getRow(), binding.family(), new byte[] {'r'}, value)});
          return CompletableFuture.completedFuture(result);
        }
      }).when(table).get(any(Get.class));
      doAnswer(invocation -> {
        CheckAndMutate mutation = invocation.getArgument(0);
        synchronized (this) {
          if (!Arrays.equals(value, mutation.getValue())) {
            return CompletableFuture.completedFuture(new CheckAndMutateResult(false, null));
          }
          Put put = (Put) mutation.getAction();
          value = CellUtil.cloneValue(put.getFamilyCellMap().firstEntry().getValue().getFirst());
          if (unknownNextWrite) {
            unknownNextWrite = false;
            return CompletableFuture.failedFuture(new java.io.IOException("simulated lost response"));
          }
          return CompletableFuture.completedFuture(new CheckAndMutateResult(true, null));
        }
      }).when(table).checkAndMutate(any(CheckAndMutate.class));
    }

    AuthRegistryAdminCommand.Connector connector() {
      var factory = mock(HBaseConnectionFactory.class);
      doReturn(CompletableFuture.completedFuture(connection)).when(factory).connectionFor(any());
      var identity = mock(AuthorizedBackendIdentity.class);
      return (configuration, activation, scope) -> {
        store = HBaseAuthSessionStore.open(factory, identity, binding, scope).get();
        return new AuthRegistryAdminCommand.Handle() {
          @Override public AuthDeploymentRegistryStore store() { return store; }
          @Override public void close() {}
        };
      };
    }

    AuthDeploymentRegistry registry() {
      try (var scope = RequestScope.withoutDeadline()) {
        return store.readRegistry(scope).toCompletableFuture().join().orElseThrow();
      }
    }
  }
}
