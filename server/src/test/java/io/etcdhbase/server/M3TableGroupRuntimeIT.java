package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.admin.M3TableGroupInitializer;
import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.admin.TableGroupPlanFactory;
import io.etcdhbase.admin.config.InitializationConfigurationLoader;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.server.commit.M3TableGroupRuntime;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Real HBase and Java RPC coverage for one explicitly local owner runtime. */
@Timeout(300)
class M3TableGroupRuntimeIT {
  private static final SnapshotPinConfiguration PINS =
      new SnapshotPinConfiguration(2, 16, Duration.ofSeconds(60));
  private static final RuntimeConfiguration.ResourceBudgets RESOURCES =
      new RuntimeConfiguration.ResourceBudgets(65_536, 2, 30_000, 4 << 20, 4 << 20, 4, 0);

  @Test
  void realRuntimeRecoversAndRetainsHeaderIdentityOnBothLayouts() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      for (String file : List.of("history-table.json", "native-cell-versions.json")) {
        var plan = plan(file, cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1));
        var scope = new AuthenticationScope(plan.source().tableGroupId(), plan.source().hbaseProfile().id());
        try (var provider = new HBaseBackendIdentityProvider(plan.source().identityProviderId(), scope,
                cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
                (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
                1, 1, Duration.ofSeconds(30));
             var credentials = new BackendCredentials("m3-runtime-fixture", new char[0])) {
          var identity = provider.authenticate(credentials, scope).toCompletableFuture().get(30, TimeUnit.SECONDS);
          var initialized = M3TableGroupInitializer.initializeFresh(plan,
              "sha256:" + plan.source().fingerprint(), identity, provider, PINS);
          assertInsufficientRecoveryBudgetRollsBack(plan, provider, identity);
          var limits = new BudgetManager.Limits(6, 0, 8, 4, 8, 0, 512L << 20);
          var budgets = new BudgetManager(limits, Map.of(plan.source().tableGroupId(), limits), limits);
          var first = write(plan, provider, identity, budgets, activation("9223372036854775808"), "before");
          assertEquals(initialized.clusterId(), first.getHeader().getClusterId());
          assertEquals(Long.MIN_VALUE, first.getHeader().getMemberId());
          assertEquals(2, first.getHeader().getRevision());
          waitForLeaseExpiry(plan, provider, identity);
          var second = write(plan, provider, identity, budgets, activation("2"), "after");
          assertEquals(first.getHeader().getClusterId(), second.getHeader().getClusterId());
          assertEquals(2, second.getHeader().getMemberId());
          assertEquals(3, second.getHeader().getRevision());
          assertTrue(second.hasPrevKv());
          assertEquals(ByteString.copyFromUtf8("before"), second.getPrevKv().getValue());
          assertEquals(0, budgets.snapshot().reservedBytes());
          assertEquals(0, budgets.snapshot().allocatedBytes());
        }
      }
    }
  }

  @Test
  void realRuntimeExecutesFlatTransactionsOnBothLayouts() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      for (String file : List.of("history-table.json", "native-cell-versions.json")) {
        runTransactionLayout(cluster, file);
      }
    }
  }

  private static void runTransactionLayout(TemporaryHBaseClusterProcess cluster, String file) throws Exception {
    var plan = plan(file, cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1));
    var authentication = new AuthenticationScope(plan.source().tableGroupId(), plan.source().hbaseProfile().id());
    try (var provider = new HBaseBackendIdentityProvider(plan.source().identityProviderId(), authentication,
            cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            1, 1, Duration.ofSeconds(30));
         var credentials = new BackendCredentials("m3-runtime-fixture", new char[0])) {
      var identity = provider.authenticate(credentials, authentication).toCompletableFuture().get(30, TimeUnit.SECONDS);
      M3TableGroupInitializer.initializeFresh(plan, "sha256:" + plan.source().fingerprint(), identity, provider, PINS);
      var limits = new BudgetManager.Limits(6, 6, 8, 4, 8, 1L << 20, 512L << 20);
      var budgets = new BudgetManager(limits, Map.of(plan.source().tableGroupId(), limits), limits);
      try (var scans = new ScanScheduler(budgets, Duration.ofSeconds(30))) {
        M3TableGroupRuntime runtime;
        try (var startup = RequestScope.withTimeout(Duration.ofSeconds(30))) {
          runtime = M3TableGroupRuntime.open(plan, activation("1"), RESOURCES, provider, identity, budgets, scans, startup);
        }
        try {
          runTransactionProbe(runtime, plan);
        } finally {
          runtime.close();
          runtime.drained().get(30, TimeUnit.SECONDS);
        }
        var metrics = runtime.metrics();
        assertEquals(2, metrics.txnStages().get(RequestMetrics.TxnStage.QUEUE).count());
        assertEquals(4, metrics.txnStages().get(RequestMetrics.TxnStage.COMPARE_AND_PREPARE).count());
        assertEquals(1, metrics.txnStages().get(RequestMetrics.TxnStage.PUBLISH_AND_FINALIZE).count());
        assertEquals(2, metrics.txnStages().get(RequestMetrics.TxnStage.RESULT_RESOLUTION).count());
        assertEquals(4, metrics.txnStages().get(RequestMetrics.TxnStage.RESPONSE).count());
        assertTrue(metrics.txnOutputBytes() > 0);
        assertEquals(1, metrics.putStages().get(RequestMetrics.PutStage.QUEUE).count());
      }
      assertEquals(0, budgets.snapshot().reservedBytes());
      assertEquals(0, budgets.snapshot().allocatedBytes());
    }
  }

  private static void runTransactionProbe(M3TableGroupRuntime runtime, M3TableGroupPlan plan) throws Exception {
    try (Bootstrap server = Bootstrap.startConfigured(new RuntimeConfiguration.Listener(
        "m3-txn", "127.0.0.1", 0, plan.source().tableGroupId()), RESOURCES, runtime.kvService())) {
      var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.boundListener().port())
          .usePlaintext().build();
      try {
        assertFlatTransactionScenarios(channel);
      } finally {
        channel.shutdownNow();
        assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
      }
    }
  }

  private static void assertFlatTransactionScenarios(io.grpc.ManagedChannel channel) {
    var stub = KVGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
    var key = ByteString.copyFromUtf8("txn-key");
    var range = Rpc.RequestOp.newBuilder().setRequestRange(Rpc.RangeRequest.newBuilder().setKey(key)).build();
    var put = Rpc.RequestOp.newBuilder().setRequestPut(Rpc.PutRequest.newBuilder().setKey(key)
        .setValue(ByteString.copyFromUtf8("value"))).build();
    var absent = Rpc.Compare.newBuilder().setKey(key).setTarget(Rpc.Compare.CompareTarget.VERSION)
        .setResult(Rpc.Compare.CompareResult.EQUAL).setVersion(0).build();
    var first = stub.txn(Rpc.TxnRequest.newBuilder().addCompare(absent)
        .addSuccess(range).addSuccess(put).addSuccess(range).build());
    assertTrue(first.getSucceeded());
    assertEquals(2, first.getHeader().getRevision());
    assertEquals(0, first.getResponses(0).getResponseRange().getKvsCount());
    assertEquals(ByteString.copyFromUtf8("value"),
        first.getResponses(2).getResponseRange().getKvs(0).getValue());
    var unchanged = stub.txn(Rpc.TxnRequest.newBuilder().addCompare(absent)
        .addSuccess(put).addFailure(range).build());
    assertFalse(unchanged.getSucceeded());
    assertEquals(2, unchanged.getHeader().getRevision());
    assertEquals(ByteString.copyFromUtf8("value"),
        unchanged.getResponses(0).getResponseRange().getKvs(0).getValue());
    var readOnly = stub.txn(Rpc.TxnRequest.newBuilder().addSuccess(range).build());
    assertEquals(2, readOnly.getHeader().getRevision());
    assertEquals(ByteString.copyFromUtf8("value"),
        readOnly.getResponses(0).getResponseRange().getKvs(0).getValue());
    var rangeCompare = Rpc.Compare.newBuilder().setKey(ByteString.copyFromUtf8("txn"))
        .setRangeEnd(ByteString.copyFromUtf8("txo")).setTarget(Rpc.Compare.CompareTarget.VERSION)
        .setResult(Rpc.Compare.CompareResult.GREATER).setVersion(0).build();
    assertTrue(stub.txn(Rpc.TxnRequest.newBuilder().addCompare(rangeCompare)
        .addSuccess(range).build()).getSucceeded());
    assertEquals(3, stub.put(Rpc.PutRequest.newBuilder().setKey(key)
        .setValue(ByteString.copyFromUtf8("next")).build()).getHeader().getRevision());
  }

  private static void assertInsufficientRecoveryBudgetRollsBack(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity) throws Exception {
    var tooSmall = new BudgetManager.Limits(6, 0, 8, 4, 8, 0, 16L << 20);
    var budgets = new BudgetManager(tooSmall, Map.of(plan.source().tableGroupId(), tooSmall), tooSmall);
    try (var scans = new ScanScheduler(budgets, Duration.ofSeconds(30));
         var startup = RequestScope.withTimeout(Duration.ofSeconds(30))) {
      var failure = assertThrows(M3TableGroupRuntime.StartupException.class, () ->
          M3TableGroupRuntime.open(plan, activation("1"), RESOURCES, provider, identity, budgets, scans, startup));
      failure.cleanup().toCompletableFuture().get(30, TimeUnit.SECONDS);
      assertEquals(0, budgets.snapshot().reservedBytes());
      assertEquals(0, budgets.snapshot().allocatedBytes());
      try (var check = RequestScope.withTimeout(Duration.ofSeconds(10))) {
        var control = HBaseCommitControlStore.open(provider, identity, plan.system(), check).join();
        assertEquals(0, control.read(check).join().orElseThrow().state().ownerEpoch());
      }
    }
  }

  private static Rpc.PutResponse write(M3TableGroupPlan plan, HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity, BudgetManager budgets,
      M3RuntimeConfiguration activation, String value) throws Exception {
    try (var scans = new ScanScheduler(budgets, Duration.ofSeconds(30))) {
      M3TableGroupRuntime runtime;
      try (var startup = RequestScope.withTimeout(Duration.ofSeconds(30))) {
        runtime = M3TableGroupRuntime.open(plan, activation, RESOURCES, provider, identity, budgets, scans, startup);
      }
      try {
        try (Bootstrap server = Bootstrap.startConfigured(new RuntimeConfiguration.Listener(
            "m3-local", "127.0.0.1", 0, plan.source().tableGroupId()), RESOURCES, runtime.kvService())) {
          var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.boundListener().port())
              .usePlaintext().build();
          try {
            return KVGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS).put(
                Rpc.PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("m3-runtime-key"))
                    .setValue(ByteString.copyFromUtf8(value)).setPrevKv(true).build());
          } finally {
            channel.shutdownNow();
            assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
          }
        }
      } finally {
        runtime.close();
        runtime.drained().get(30, TimeUnit.SECONDS);
      }
    }
  }

  static void waitForLeaseExpiry(M3TableGroupPlan plan, HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity) throws Exception {
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      var control = HBaseCommitControlStore.open(provider, identity, plan.system(), scope).join();
      long deadline = control.read(scope).join().orElseThrow().state().ownerLeaseDeadlineEpochMillis();
      long remaining;
      while ((remaining = deadline - System.currentTimeMillis()) > 0) {
        Thread.sleep(Math.min(remaining, 100));
      }
    }
  }

  static M3RuntimeConfiguration activation(String memberId) {
    return new M3RuntimeConfiguration(memberId, 1500, 200, 30_000, 30_000,
        512L << 20, 1_572_864, 2, 16, 60_000);
  }

  static M3TableGroupPlan plan(String file, int port) throws Exception {
    return plan(file, port, 16);
  }

  static M3TableGroupPlan plan(String file, int port, int buckets) throws Exception {
    return plan(file, port, "m3_runtime_" + UUID.randomUUID().toString().replace("-", ""), buckets);
  }

  static M3TableGroupPlan plan(String file, int port, String namespace) throws Exception {
    return plan(file, port, namespace, 16);
  }

  static M3TableGroupPlan plan(String file, int port, String namespace, int buckets) throws Exception {
    var mapper = new ObjectMapper();
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(
        Path.of("..", "config", "examples", "j05", file)));
    ObjectNode backend = (ObjectNode) tree.withArray("hbase_profiles").get(0);
    backend.put("namespace", namespace).put("quorum", "127.0.0.1").put("client_port", port);
    ObjectNode salt = (ObjectNode) tree.withArray("salt_profiles").get(0);
    String saltId = buckets == 16 ? "crc32-sixteen-buckets" : "crc32-" + buckets + "-buckets";
    salt.put("id", saltId).put("bucket_count", buckets);
    var splits = salt.putArray("split_keys_hex");
    for (int bucket = 1; bucket < buckets; bucket++) splits.add(String.format("%04x", bucket));
    ObjectNode group = (ObjectNode) tree.withArray("table_groups").get(0);
    group.put("salt_profile_id", saltId);
    ((ObjectNode) group.get("system_table")).set("split_keys_hex", splits.deepCopy());
    for (var candidate : group.withArray("history_candidates")) {
      for (var table : candidate.withArray("tables")) {
        ((ObjectNode) table).set("split_keys_hex", splits.deepCopy());
      }
    }
    ((ObjectNode) group.get("system_table")).withArray("record_categories").add("RESULT");
    var config = new InitializationConfigurationLoader()
        .load(new ByteArrayInputStream(mapper.writeValueAsBytes(tree)));
    return M3TableGroupPlan.from(TableGroupPlanFactory.create(config, "local", null));
  }
}
