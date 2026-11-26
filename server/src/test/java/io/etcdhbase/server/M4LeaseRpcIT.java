package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import etcdserverpb.KVGrpc;
import etcdserverpb.LeaseGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.admin.M3TableGroupInitializer;
import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.admin.TableGroupPlanFactory;
import io.etcdhbase.admin.config.InitializationConfiguration;
import io.etcdhbase.admin.config.InitializationConfigurationLoader;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.lease.LeaseCostAdmission;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.SharedEventTailer;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.lease.LeaseExpiryCheckpoint;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.grpc.internal.v1.ForwardMutationRequest;
import io.etcdhbase.grpc.internal.v1.ForwardMutationResponse;
import io.etcdhbase.grpc.internal.v1.InternalOwnerServiceGrpc;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.codec.LeaseStateMessages;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.hbase.storage.HBaseCommitDecisionRepository;
import io.etcdhbase.hbase.storage.HBaseEventRevisionIndexRepository;
import io.etcdhbase.hbase.storage.HBaseInvocationRepository;
import io.etcdhbase.hbase.storage.HBaseManifestRepository;
import io.etcdhbase.hbase.storage.HBaseSystemRecordStore;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.M3RuntimeConfigurationLoader;
import io.etcdhbase.server.config.M3MemoryReservations;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.commit.ServiceAuthorizationContextReference;
import io.etcdhbase.server.commit.M3TableGroupRuntime;
import io.etcdhbase.server.commit.LeaseBudgetProfile;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Verifies M4 Lease RPCs against native etcd, a disposable HBase cluster, and runtime restart. */
@Timeout(600)
class M4LeaseRpcIT {
  private static final int MAX_DRIVER_DIAGNOSTIC_BYTES = 8 * 1024;
  private static final int MIXED_LOAD_KEEPALIVE_RESPONSE_TIMEOUT_SECONDS = 60;
  private static final int MIXED_LOAD_WORKER_BASE_TIMEOUT_SECONDS = 30;
  private static final int MIXED_LOAD_WORKER_TIMEOUT_PER_ITERATION_SECONDS = 5;
  private static final int MIXED_LOAD_WORKER_MAX_TIMEOUT_SECONDS = 180;
  private static final int TEST_OWNER_LEASE_MILLIS = 6_000;
  private static final int TEST_HEARTBEAT_MILLIS = 1_000;
  private static final int EVENT_INDEX_OWNER_LEASE_MILLIS = 15_000;
  private static final int EVENT_INDEX_HEARTBEAT_MILLIS = 2_500;
  private static final List<String> LAYOUT_FILES =
      List.of("history-table.json", "native-cell-versions.json");
  private static final List<String> MIXED_EVIDENCE_SOURCE_FILES = List.of(
      "core/src/main/java/io/etcdhbase/core/lease/LeaseExpiryCheckpoint.java",
      "server/src/main/java/io/etcdhbase/server/commit/LeaseExpiryWorker.java",
      "server/src/main/java/io/etcdhbase/server/commit/LeaseRevocationExecutor.java",
      "server/src/main/java/io/etcdhbase/server/observability/RequestMetrics.java",
      "server/src/test/java/io/etcdhbase/server/M4LeaseRpcIT.java",
      "server/src/test/java/io/etcdhbase/server/M4MixedLeaseBenchmarkIT.java",
      "scripts/verify/m4-report.py",
      "testkit/cases/m4-cases.json");
  private static final RuntimeConfiguration.ResourceBudgets RESOURCES =
      new RuntimeConfiguration.ResourceBudgets(65_536, 4, 30_000,
          4 << 20, 4 << 20, 16, 0);
  // Four KeepAlive streams and their independent Put RPCs must fit at the listener.
  private static final RuntimeConfiguration.ResourceBudgets MIXED_LOAD_RESOURCES =
      new RuntimeConfiguration.ResourceBudgets(65_536, 8, 30_000,
          4 << 20, 4 << 20, 16, 0);
  @TempDir Path testDirectory;

  @Test
  void runtimeFencesEventIndexFormatAndPersistsSelectedPutAcrossLayouts() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      for (String layoutFile : LAYOUT_FILES) {
        InitializationConfiguration initialization = initialization(layoutFile, clientPort);
        var source = TableGroupPlanFactory.create(initialization, "local", null);
        M3TableGroupPlan plan = M3TableGroupPlan.from(source);
        var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
            source.tableGroupId(), source.hbaseProfile().id());
        try (var provider = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
                cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
                (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
                1, 1, Duration.ofSeconds(30));
             var credentials = new io.etcdhbase.spi.identity.BackendCredentials("j44-test", new char[0])) {
          var identity = authenticate(provider, credentials, authScope);
          var activation = activation("1");
          M3TableGroupInitializer.initializeFresh(plan, "sha256:" + source.fingerprint(),
              identity, provider, activation.pinConfiguration());
          var configuration = new RuntimeConfiguration(initialization.schemaVersion(),
              List.of(new RuntimeConfiguration.Listener("j44", "127.0.0.1", 0, source.tableGroupId())),
              initialization.hbaseProfiles(), initialization.saltProfiles(), initialization.tableGroups(), RESOURCES);
          long legacyRevision;
          try (var legacy = RuntimeServer.startM3(configuration, activation("1", false))) {
            legacyRevision = putThrough(legacy, "j44-legacy-key", 1).getHeader().getRevision();
          }
          M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, provider, identity);
          long revision;
          io.etcdhbase.core.bytes.ByteKey registryNamespace;
          try (var runtime = RuntimeServer.startM3(configuration,
              activation("2", true, EVENT_INDEX_OWNER_LEASE_MILLIS, EVENT_INDEX_HEARTBEAT_MILLIS))) {
            assertTrue(runtime.dataReady());
            assertEquals(1, rangeKey(runtime, "j44-legacy-key".getBytes(StandardCharsets.UTF_8)).getKvsCount());
            var response = putAndReadSharedTailer(runtime, plan);
            revision = response.getHeader().getRevision();
            assertEquals(legacyRevision + 1, revision);
            try (var scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
              var control = HBaseCommitControlStore.open(provider, identity, plan.system(), scope).join();
              assertTrue(control.supportsEventRevisionIndex());
              var state = control.read(scope).join().orElseThrow().state();
              assertTrue(state.supportsEventRevisionIndex());
              registryNamespace = state.eventRegistryNamespace().orElseThrow();
              var records = HBaseSystemRecordStore.open(provider, identity, plan.system(), scope).join();
              var legacyIndex = awaitBackfilledIndex(records, legacyRevision, scope);
              var legacyDecision = new HBaseCommitDecisionRepository(records)
                  .read(legacyIndex.decision().sequence(), scope).join().orElseThrow();
              var legacyRoot = new HBaseManifestRepository(records)
                  .readRoot(legacyDecision.manifestRoot(), scope).join().orElseThrow();
              legacyIndex.verifySelected(legacyDecision, legacyRoot);
              var index = new HBaseEventRevisionIndexRepository(records)
                  .read(revision, scope).join().orElseThrow();
              var decision = new HBaseCommitDecisionRepository(records)
                  .read(index.decision().sequence(), scope).join().orElseThrow();
              var root = new HBaseManifestRepository(records)
                  .readRoot(decision.manifestRoot(), scope).join().orElseThrow();
              index.verifySelected(decision, root);
            }
          }
          M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, provider, identity);
          try (var restarted = RuntimeServer.startM3(configuration,
                   activation("3", true, EVENT_INDEX_OWNER_LEASE_MILLIS, EVENT_INDEX_HEARTBEAT_MILLIS));
               var scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
            assertTrue(restarted.dataReady());
            assertEquals(1, rangeKey(restarted, "j44-legacy-key".getBytes(StandardCharsets.UTF_8)).getKvsCount());
            var records = HBaseSystemRecordStore.open(provider, identity, plan.system(), scope).join();
            var control = HBaseCommitControlStore.open(provider, identity, plan.system(), scope).join();
            assertEquals(registryNamespace,
                control.read(scope).join().orElseThrow().state().eventRegistryNamespace().orElseThrow());
            var index = new HBaseEventRevisionIndexRepository(records)
                .read(revision, scope).join().orElseThrow();
            var decision = new HBaseCommitDecisionRepository(records)
                .read(index.decision().sequence(), scope).join().orElseThrow();
            var root = new HBaseManifestRepository(records)
                .readRoot(decision.manifestRoot(), scope).join().orElseThrow();
            index.verifySelected(decision, root);
          }
        }
      }
    }
  }

  private static Rpc.PutResponse putAndReadSharedTailer(RuntimeServer runtime,
      M3TableGroupPlan plan) throws Exception {
    ByteKey key = ByteKey.copyOf("j44-index-key".getBytes(StandardCharsets.UTF_8));
    var limits = new BudgetManager.Limits(8, 8, 8, 8, 8, 1L << 20, 1L << 30);
    var budgets = new BudgetManager(limits, Map.of("events", limits), limits);
    var retained = new BudgetManager.Reservation(0, 1, 1, 1L << 20, 0);
    M3TableGroupRuntime groupRuntime = runtime.tableGroupRuntime(plan.source().tableGroupId());
    SharedEventTailer tailer;
    try {
      tailer = groupRuntime.eventTailerReady().get(60, TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException timeout) {
      throw new AssertionError("shared event tailer did not become ready: backfill="
          + groupRuntime.metrics().eventBackfill() + ", historyFailures="
          + groupRuntime.metrics().eventHistoryFailures() + ", tailer="
          + groupRuntime.metrics().eventTailer(), timeout);
    }
    try (var exactScope = RequestScope.withTimeout(Duration.ofSeconds(30));
         var intervalScope = RequestScope.withTimeout(Duration.ofSeconds(30));
         var exactMemory = budgets.admit("events", exactScope, retained);
         var intervalMemory = budgets.admit("events", intervalScope, retained);
         var exact = tailer.subscribe(KeyRange.point(key), exactMemory, 64, 1L << 20);
         var interval = tailer.subscribe(KeyRange.all(), intervalMemory, 64, 1L << 20)) {
        var metrics = groupRuntime.metrics();
        var metricsBefore = metrics.eventTailer();
        Rpc.PutResponse response = putThrough(runtime, "j44-index-key", 2);
        try {
          try (var pointEvent = awaitDelivery(exact, Duration.ofSeconds(30));
               var rangeEvent = awaitDelivery(interval, Duration.ofSeconds(30))) {
            assertEquals(key, pointEvent.event().current().key());
            assertEquals(key, rangeEvent.event().current().key());
            assertEquals(response.getHeader().getRevision(), pointEvent.revision());
            assertEquals(pointEvent.sequence(), rangeEvent.sequence());
          }
        } catch (AssertionError failure) {
          throw new AssertionError("eventTailer=" + groupRuntime.metrics().eventTailer()
              + ", capturedClosed=" + tailer.isClosed()
              + ", currentInstalled=" + groupRuntime.eventTailer().isPresent()
              + ", exactTermination=" + exact.termination()
              + ", intervalTermination=" + interval.termination()
              + ", cursor=" + tailer.completedSequence() + "/" + tailer.completedRevision()
              + ", observedSequence=" + tailer.observedPublishedSequence(), failure);
        }
        assertTrue(exact.poll().isEmpty());
        assertTrue(interval.poll().isEmpty());
        awaitEventTailerMetrics(metrics, metricsBefore, response.getHeader().getRevision(),
            Duration.ofSeconds(30));
        return response;
    }
  }

  private static SharedEventTailer.Delivery awaitDelivery(SharedEventTailer.Subscription subscription,
      Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      var delivery = subscription.poll();
      if (delivery.isPresent()) return delivery.orElseThrow();
      if (subscription.termination().isPresent()) {
        throw new AssertionError("runtime event tailer stopped subscription: " + subscription.termination());
      }
      Thread.sleep(10);
    }
    throw new AssertionError("runtime event tailer did not deliver the committed event");
  }

  private static io.etcdhbase.server.observability.RequestMetrics.EventTailerSnapshot awaitEventTailerMetrics(
      io.etcdhbase.server.observability.RequestMetrics metrics,
      io.etcdhbase.server.observability.RequestMetrics.EventTailerSnapshot before,
      long expectedRevision, Duration timeout) throws InterruptedException {
    long timeoutNanos = timeout.toNanos();
    long startedAtNanos = System.nanoTime();
    io.etcdhbase.server.observability.RequestMetrics.EventTailerSnapshot observed;
    while (System.nanoTime() - startedAtNanos < timeoutNanos) {
      observed = metrics.eventTailer();
      if (observed.events() > before.events() && observed.revision() >= expectedRevision) return observed;
      TimeUnit.MILLISECONDS.sleep(25);
    }
    observed = metrics.eventTailer();
    throw new AssertionError("event tailer metrics did not record the delivered event revision "
        + expectedRevision + ": before=" + before + ", observed=" + observed);
  }

  private static io.etcdhbase.core.commit.EventRevisionIndex awaitBackfilledIndex(
      HBaseSystemRecordStore records, long revision, RequestScope scope) throws Exception {
    var indexes = new HBaseEventRevisionIndexRepository(records);
    while (!scope.isDeadlineExceeded()) {
      var found = indexes.read(revision, scope).join();
      if (found.isPresent()) return found.orElseThrow();
      Thread.sleep(50);
    }
    throw new AssertionError("legacy event index was not backfilled before the deadline");
  }

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

 @Test
  void minimumMemoryPreservesLeaseLifecycleAcrossClockFailures() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      InitializationConfiguration initialization = initialization("history-table.json", clientPort);
      var source = TableGroupPlanFactory.create(initialization, "local", null);
      var plan = M3TableGroupPlan.from(source);
      var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
          source.tableGroupId(), source.hbaseProfile().id());
      try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
              cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
              (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
              1, 1, Duration.ofSeconds(30));
           var credentials = new io.etcdhbase.spi.identity.BackendCredentials("m4-initializer", new char[0])) {
        var identity = authenticate(initializer, credentials, authScope);
        var activation = minimumActivation();
        initializeM4Layout(plan, source.fingerprint(), identity, initializer, activation);
        var configuration = new RuntimeConfiguration(initialization.schemaVersion(),
            List.of(new RuntimeConfiguration.Listener("m4-local", "127.0.0.1", 0, source.tableGroupId())),
            initialization.hbaseProfiles(), initialization.saltProfiles(), initialization.tableGroups(), RESOURCES);
        exerciseLeaseLifecycleAtMinimum(plan, initializer, identity, configuration, activation);
      }
    }
  }


  private static void exerciseLeaseLifecycleAtMinimum(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider initializer, io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      RuntimeConfiguration configuration, M3RuntimeConfiguration activation) throws Exception {
    var now = new AtomicLong(10_000);
    var healthy = new AtomicBoolean(true);
    var highestObservedTime = new AtomicLong(10_000);
    var armed = new AtomicBoolean(false);
    var rewind = new AtomicBoolean(false);
    LeaseTimeSource time = () -> {
      if (armed.get()) {
        if (rewind.get()) now.set(11_000);
        else healthy.set(false);
      }
      long observedNow = now.get();
      long previousHighWater = highestObservedTime.getAndAccumulate(observedNow, Math::max);
      boolean monotonic = observedNow >= previousHighWater;
      return new LeaseTimeObservation(observedNow, observedNow, healthy.get() && monotonic);
    };
    try (RuntimeServer runtime = RuntimeServer.startM3WithLeaseTimeSource(configuration, activation, ignored -> time)) {
      var channel = NettyChannelBuilder.forAddress("127.0.0.1", runtime.boundListeners().getFirst().port())
          .usePlaintext().build();
      try {
        var client = LeaseGrpc.newBlockingStub(channel).withDeadlineAfter(120, TimeUnit.SECONDS);
        long leaseId = 12345;
        var request = Rpc.LeaseGrantRequest.newBuilder().setID(leaseId).setTTL(5).build();
        assertEquals(leaseId, client.leaseGrant(request).getID());
        // Keep the Lease below its deadline while asserting that unhealthy and rewound clock
        // observations do not select an expiry mutation in the background.
        now.set(12_000);
        assertTrue(client.leaseLeases(Rpc.LeaseLeasesRequest.getDefaultInstance()).getLeasesList().stream()
            .anyMatch(lease -> lease.getID() == leaseId));
        assertEquals(Status.Code.FAILED_PRECONDITION,
            assertThrows(StatusRuntimeException.class, () -> client.leaseGrant(request)).getStatus().getCode());
        long revision = controlSnapshot(plan, initializer, identity).state().publishedRevision();
        armed.set(true);
        var ttlRequest = Rpc.LeaseTimeToLiveRequest.newBuilder().setID(leaseId).setKeys(true).build();
        assertEquals(Status.Code.UNAVAILABLE,
            assertThrows(StatusRuntimeException.class, () -> client.leaseTimeToLive(ttlRequest)).getStatus().getCode());
        assertEquals(revision, controlSnapshot(plan, initializer, identity).state().publishedRevision());
        armed.set(false);
        healthy.set(true);
        assertTrue(client.leaseLeases(Rpc.LeaseLeasesRequest.getDefaultInstance()).getLeasesList().stream()
            .anyMatch(lease -> lease.getID() == leaseId));
        rewind.set(true);
        armed.set(true);
        assertEquals(Status.Code.UNAVAILABLE,
            assertThrows(StatusRuntimeException.class, () -> client.leaseTimeToLive(ttlRequest)).getStatus().getCode());
        assertEquals(revision, controlSnapshot(plan, initializer, identity).state().publishedRevision());
        assertTrue(client.leaseLeases(Rpc.LeaseLeasesRequest.getDefaultInstance()).getLeasesList().stream()
            .anyMatch(lease -> lease.getID() == leaseId),
            "an unhealthy or rewound clock must not revoke a live Lease");
        armed.set(false);
        now.set(20_000);
        long expiredTtl = client.leaseTimeToLive(ttlRequest).getTTL();
        assertTrue(expiredTtl == 0 || expiredTtl == -1,
            "the lease is either awaiting expiry work or already revoked");
        if (client.leaseLeases(Rpc.LeaseLeasesRequest.getDefaultInstance()).getLeasesList().stream()
            .anyMatch(lease -> lease.getID() == leaseId)) {
          client.leaseRevoke(Rpc.LeaseRevokeRequest.newBuilder().setID(leaseId).build());
        }
        assertEquals(-1, client.leaseTimeToLive(ttlRequest).getTTL());
        assertFalse(client.leaseLeases(Rpc.LeaseLeasesRequest.getDefaultInstance()).getLeasesList().stream()
            .anyMatch(lease -> lease.getID() == leaseId));
        assertEquals(leaseId, client.leaseGrant(request).getID());
      } finally {
        channel.shutdownNow();
        assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
      }
    }
  }


  private static M3RuntimeConfiguration minimumActivation() {
    int maxRequest = 1_572_864;
    long reservedMemory = LeaseBudgetProfile.perGroupCapacity(maxRequest).memoryBytes();
    long recoveryMemory = LeaseBudgetProfile.RECOVERY_MEMORY.total();
    long startupMemory = Math.addExact(recoveryMemory, LeaseBudgetProfile.startupLeaseScanMemory().total());
    long regularMemory = Math.max(M3MemoryReservations.minimumLeaseMemory(1, maxRequest), startupMemory);
    long minimumMemory = Math.addExact(reservedMemory, regularMemory);
    return new M3RuntimeConfiguration("1", 6_000, 1_000, 60_000, 30_000,
        minimumMemory, maxRequest,
        2, 16, 60_000, false, true, 5, 1_000, 1_000,
        LeaseCostAdmission.DEFAULT_LIMITS.maximumBindings(),
        LeaseCostAdmission.DEFAULT_LIMITS.maximumKeyBytes(),
        LeaseCostAdmission.DEFAULT_LIMITS.maximumRevokeCostBytes(), null, null,
        M3RuntimeConfiguration.ActivationProfile.PUBLIC_TEST);
  }


  static OwnerTakeoverEvidence verifyDualRuntimeForwardsKeepAliveOverMtlsAndFencesTheOldOwner(
      Path testDirectory) throws Exception {
    RunContext run = prepareRun();
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      InitializationConfiguration initialization = initialization("native-cell-versions.json", clientPort);
      var source = TableGroupPlanFactory.create(initialization, "local", null);
      M3TableGroupPlan plan = M3TableGroupPlan.from(source);
      var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
          source.tableGroupId(), source.hbaseProfile().id());
      var runtimeForwardingIdentity = runtimeForwardingIdentity(source.identityProviderId(), authScope);
      try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
              cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
              (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
              1, 1, Duration.ofSeconds(30));
           var credentials = new io.etcdhbase.spi.identity.BackendCredentials("m4-owner-forwarding", new char[0])) {
        var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
            .get(30, TimeUnit.SECONDS);
        var initialized = M3TableGroupInitializer.initializeFresh(plan,
            "sha256:" + source.fingerprint(), identity, initializer, activation("1").pinConfiguration());
        RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
            List.of(new RuntimeConfiguration.Listener("m4-owner-forwarding", "127.0.0.1", 0,
                source.tableGroupId())), initialization.hbaseProfiles(), initialization.saltProfiles(),
            initialization.tableGroups(), RESOURCES);

        try (TestTlsCertificate ownerACertificate = TestTlsCertificate.create(testDirectory, "owner-a");
             TestTlsCertificate ownerBCertificate = TestTlsCertificate.create(testDirectory, "owner-b");
             TestTlsCertificate unregisteredPeerCertificate =
                 TestTlsCertificate.create(testDirectory, "unregistered-peer")) {
            Path trustedPeers = testDirectory.resolve("trusted-owner-peers.pem");
            Files.writeString(trustedPeers,
                Files.readString(ownerACertificate.certificate())
                    + System.lineSeparator()
                    + Files.readString(ownerBCertificate.certificate())
                    + System.lineSeparator()
                    + Files.readString(unregisteredPeerCertificate.certificate()));
            int ownerAPort = availablePort();
            int ownerBPort = availablePort();
            var peers = List.of(
                new M3RuntimeConfiguration.OwnerPeer("owner-a", "127.0.0.1", ownerAPort, "owner-a"),
                new M3RuntimeConfiguration.OwnerPeer("owner-b", "127.0.0.1", ownerBPort, "owner-b"));
            var ownerAForwarding = new M3RuntimeConfiguration.OwnerForwardingSettings(
                "127.0.0.1", ownerAPort, ownerACertificate.certificate().toString(),
                ownerACertificate.privateKey().toString(), trustedPeers.toString(), peers);
            var ownerBForwarding = new M3RuntimeConfiguration.OwnerForwardingSettings(
                "127.0.0.1", ownerBPort, ownerBCertificate.certificate().toString(),
                ownerBCertificate.privateKey().toString(), trustedPeers.toString(), peers);

            RuntimeServer ownerA = RuntimeServer.startM3(configuration,
                ownerActivation("1", "owner-a", ownerAForwarding));
            RuntimeServer ownerB = null;
            try {
              ownerB = RuntimeServer.startM3(configuration,
                  ownerActivation("2", "owner-b", ownerBForwarding));
              assertTrue(ownerA.dataReady());
              assertTrue(ownerB.dataReady());
              assertPeerCertificateWithoutConfiguredIdentityIsRejected(
                  ownerAPort, "owner-a", trustedPeers, unregisteredPeerCertificate);
              CommitState activeOwner = controlSnapshot(plan, initializer, identity).state();
              assertTrue(isOwnerForPeer(activeOwner, "owner-a"));
              long scopeProbeLease = leaseIdForPrefix(run.prefix(), "forwarding-context-probe");
              InvocationId invalidContextInvocation = InvocationId.create();
              var invalidContextRequest = keepAliveForwardRequest(source.tableGroupId(),
                  runtimeForwardingIdentity, Rpc.LeaseKeepAliveRequest.newBuilder()
                      .setID(scopeProbeLease).build(), invalidContextInvocation,
                  activeOwner.ownerEpoch())
                  .toBuilder()
                  .setAuthorizationContextReference(ByteString.copyFromUtf8("unresolved-context"))
                  .build();
              ForwardMutationResponse invalidContextReply = forwardMutation(ownerAPort, "owner-a",
                  trustedPeers, ownerBCertificate, invalidContextRequest);
              assertEquals(ForwardMutationResponse.Outcome.UNKNOWN, invalidContextReply.getOutcome());
              assertEquals("PERMISSION", invalidContextReply.getFailureCode());
              CommitState afterInvalidContext = controlSnapshot(plan, initializer, identity).state();
              assertEquals(activeOwner.publishedRevision(), afterInvalidContext.publishedRevision(),
                  "an unresolved forwarded service context must not change the KV revision");
              try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
                var records = HBaseSystemRecordStore.open(initializer, identity, plan.system(), scope)
                    .join();
                assertTrue(new HBaseInvocationRepository(records)
                    .read(invalidContextInvocation, scope).join().isEmpty(),
                    "an unresolved forwarded service context must be rejected before invocation binding");
              }

              Rpc.PutResponse forwardedPut = putThrough(ownerB, "forwarded-before-takeover", 2);
              assertEquals(initialized.clusterId(), forwardedPut.getHeader().getClusterId());
              assertEquals(2, forwardedPut.getHeader().getMemberId());
              assertForwardedPutsRespectZeroQueue(ownerB);

              long leaseA = leaseIdForPrefix(run.prefix(), "keepalive-a");
              long leaseB = leaseIdForPrefix(run.prefix(), "keepalive-b");
              long missingLease = leaseIdForPrefix(run.prefix(), "keepalive-missing");
              grantThrough(ownerB, leaseA, 30);
              grantThrough(ownerB, leaseB, 60);
              CommitState beforeRenewal = controlSnapshot(plan, initializer, identity).state();
              LeaseState leaseABeforeRenewal = leaseStateAt(plan, initializer, identity, leaseA,
                  beforeRenewal.finalizedSequence());
              LeaseState leaseBBeforeRenewal = leaseStateAt(plan, initializer, identity, leaseB,
                  beforeRenewal.finalizedSequence());
              List<Rpc.LeaseKeepAliveResponse> renewals = keepAliveThrough(
                  ownerB, leaseA, leaseB, missingLease);
              assertEquals(List.of(leaseA, leaseB, missingLease),
                  renewals.stream().map(Rpc.LeaseKeepAliveResponse::getID).toList());
              assertTrue(renewals.get(0).getTTL() > 0);
              assertTrue(renewals.get(1).getTTL() > 0);
              assertEquals(0, renewals.get(2).getTTL());
              for (var renewal : renewals) {
                assertEquals(beforeRenewal.publishedRevision(), renewal.getHeader().getRevision());
                assertEquals(initialized.clusterId(), renewal.getHeader().getClusterId());
                assertEquals(2, renewal.getHeader().getMemberId());
              }
              CommitState afterRenewal = controlSnapshot(plan, initializer, identity).state();
              assertEquals(beforeRenewal.publishedRevision(), afterRenewal.publishedRevision());
              assertTrue(afterRenewal.publishedSequence() >= beforeRenewal.publishedSequence() + 2,
                  "both successful KeepAlive requests must select an auxiliary commit; expiry "
                      + "checkpoint maintenance may also advance the metadata-only sequence");
              LeaseState leaseAAfterRenewal = leaseStateAt(plan, initializer, identity, leaseA,
                  afterRenewal.finalizedSequence());
              LeaseState leaseBAfterRenewal = leaseStateAt(plan, initializer, identity, leaseB,
                  afterRenewal.finalizedSequence());
              assertEquals(leaseABeforeRenewal.renewalGeneration() + 1,
                  leaseAAfterRenewal.renewalGeneration());
              assertEquals(leaseBBeforeRenewal.renewalGeneration() + 1,
                  leaseBAfterRenewal.renewalGeneration());

              InvocationId missingInvocation = InvocationId.create();
              var missingKeepAlive = Rpc.LeaseKeepAliveRequest.newBuilder().setID(missingLease).build();
              ForwardMutationRequest missingAttempt = keepAliveForwardRequest(
                  source.tableGroupId(), runtimeForwardingIdentity, missingKeepAlive, missingInvocation,
                  afterRenewal.ownerEpoch());
              ForwardMutationResponse firstMissingReply = forwardMutation(
                  ownerAPort, "owner-a", trustedPeers, ownerBCertificate, missingAttempt);
              assertEquals(ForwardMutationResponse.Outcome.COMPLETED, firstMissingReply.getOutcome(),
                  () -> "missing-Lease forwarding failed: " + firstMissingReply.getFailureCode());
              assertTrue(java.util.Arrays.equals(missingInvocation.bytes(),
                  firstMissingReply.getOperationResult().toByteArray()));
              CommitState afterMissingReply = controlSnapshot(plan, initializer, identity).state();
              assertEquals(afterRenewal.publishedRevision(), afterMissingReply.publishedRevision());
              assertEquals(leaseAAfterRenewal,
                  leaseStateAt(plan, initializer, identity, leaseA, afterMissingReply.finalizedSequence()),
                  "an unknown Lease KeepAlive must not change another Lease");
              assertEquals(leaseBAfterRenewal,
                  leaseStateAt(plan, initializer, identity, leaseB, afterMissingReply.finalizedSequence()),
                  "an unknown Lease KeepAlive must not change another Lease");

              ForwardMutationResponse replayedMissingReply = forwardMutation(
                  ownerAPort, "owner-a", trustedPeers, ownerBCertificate, missingAttempt);
              assertEquals(ForwardMutationResponse.Outcome.COMPLETED, replayedMissingReply.getOutcome());
              assertTrue(java.util.Arrays.equals(missingInvocation.bytes(),
                  replayedMissingReply.getOperationResult().toByteArray()));
              CommitState afterMissingReplay = controlSnapshot(plan, initializer, identity).state();
              assertEquals(afterMissingReply.publishedRevision(), afterMissingReplay.publishedRevision());
              assertEquals(leaseAAfterRenewal,
                  leaseStateAt(plan, initializer, identity, leaseA, afterMissingReplay.finalizedSequence()),
                  "replaying an unknown Lease result must remain side-effect free");
              assertEquals(leaseBAfterRenewal,
                  leaseStateAt(plan, initializer, identity, leaseB, afterMissingReplay.finalizedSequence()),
                  "replaying an unknown Lease result must remain side-effect free");

              long oldEpoch = afterMissingReplay.ownerEpoch();
              long takeoverStartedNanos = System.nanoTime();
              ownerA.close();
              M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
              CommitState newOwner = awaitOwner(plan, initializer, identity, "owner-b", oldEpoch);
              long takeoverElapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                  System.nanoTime() - takeoverStartedNanos);
              assertTrue(newOwner.ownerEpoch() > oldEpoch);
              assertEquals(2, putThrough(ownerB, "forwarded-after-takeover", 2)
                  .getHeader().getMemberId());
              CommitState beforeStaleRenewal = controlSnapshot(plan, initializer, identity).state();
              LeaseState leaseBeforeStaleRenewal = leaseStateAt(plan, initializer, identity, leaseA,
                  beforeStaleRenewal.finalizedSequence());
              assertRejectedStaleOwner(ownerBForwarding, trustedPeers, ownerBPort, ownerACertificate,
                  source.tableGroupId(), identity, leaseA, oldEpoch);
              CommitState afterStaleRenewal = controlSnapshot(plan, initializer, identity).state();
              assertEquals(leaseBeforeStaleRenewal, leaseStateAt(plan, initializer, identity, leaseA,
                  afterStaleRenewal.finalizedSequence()),
                  "a stale-owner request must not change the authoritative Lease version");
              assertEquals(beforeStaleRenewal.publishedRevision(), afterStaleRenewal.publishedRevision());
              return new OwnerTakeoverEvidence(oldEpoch, newOwner.ownerEpoch(),
                  takeoverElapsedMillis, newOwner.ownerInstance().orElseThrow());
            } finally {
              if (ownerB != null) ownerB.close();
              ownerA.close();
            }
        }
      }
    }
  }

  static void verifyDualProcessKeepAliveSurvivesOwnerFailureAndFencesStaleEpoch(
      Path testDirectory) throws Exception {
    RunContext beforeFailover = prepareKeepAliveProcessRun("before-failover");
    RunContext afterFailover = prepareKeepAliveProcessRun("after-failover");
    prepareNativeKeepAliveEvidence(beforeFailover);
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration()
          .getInt("hbase.zookeeper.property.clientPort", -1);
      InitializationConfiguration initialization = initialization("native-cell-versions.json", clientPort);
      var source = TableGroupPlanFactory.create(initialization, "local", null);
      M3TableGroupPlan plan = M3TableGroupPlan.from(source);
      var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
          source.tableGroupId(), source.hbaseProfile().id());
      var runtimeForwardingIdentity = runtimeForwardingIdentity(source.identityProviderId(), authScope);
      try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
              cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
              (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
              1, 1, Duration.ofSeconds(30));
           var credentials = new io.etcdhbase.spi.identity.BackendCredentials(
               "m4-dual-process", new char[0])) {
        var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
            .get(30, TimeUnit.SECONDS);
        M3RuntimeConfiguration baseActivation = activation("1");
        var initialized = M3TableGroupInitializer.initializeFresh(plan,
            "sha256:" + source.fingerprint(), identity, initializer,
            baseActivation.pinConfiguration());

        List<Integer> ports = availablePorts(4);
        int ownerAClientPort = ports.get(0);
        int ownerAPeerPort = ports.get(1);
        int ownerBClientPort = ports.get(2);
        int ownerBPeerPort = ports.get(3);
        try (TestTlsCertificate ownerACertificate = TestTlsCertificate.create(testDirectory, "owner-a");
             TestTlsCertificate ownerBCertificate = TestTlsCertificate.create(testDirectory, "owner-b")) {
          Path trustedPeers = testDirectory.resolve("dual-process-owner-peers.pem");
          Files.writeString(trustedPeers,
              Files.readString(ownerACertificate.certificate())
                  + System.lineSeparator()
                  + Files.readString(ownerBCertificate.certificate()));
          var peers = List.of(
              new M3RuntimeConfiguration.OwnerPeer("owner-a", "127.0.0.1", ownerAPeerPort, "owner-a"),
              new M3RuntimeConfiguration.OwnerPeer("owner-b", "127.0.0.1", ownerBPeerPort, "owner-b"));
          var ownerAForwarding = new M3RuntimeConfiguration.OwnerForwardingSettings(
              "127.0.0.1", ownerAPeerPort, ownerACertificate.certificate().toString(),
              ownerACertificate.privateKey().toString(), trustedPeers.toString(), peers);
          var ownerBForwarding = new M3RuntimeConfiguration.OwnerForwardingSettings(
              "127.0.0.1", ownerBPeerPort, ownerBCertificate.certificate().toString(),
              ownerBCertificate.privateKey().toString(), trustedPeers.toString(), peers);

          Path ownerARuntime = writeOwnerProcessRuntimeConfiguration(
              beforeFailover, initialization, ownerAClientPort, "owner-a");
          Path ownerBRuntime = writeOwnerProcessRuntimeConfiguration(
              beforeFailover, initialization, ownerBClientPort, "owner-b");
          Path ownerAActivation = writeOwnerProcessActivation(
              beforeFailover.root(), testDirectory, "1", "owner-a", ownerAForwarding);
          Path ownerBActivation = writeOwnerProcessActivation(
              beforeFailover.root(), testDirectory, "2", "owner-b", ownerBForwarding);

          Process ownerA = null;
          Process ownerB = null;
          ProcessTermination ownerAStop = null;
          ProcessTermination ownerBStop = null;
          long oldEpoch = 0;
          long newEpoch = 0;
          long keepAliveClientPid = 0;
          int keepAliveClientExitCode = -1;
          long keepAliveClientTtl = 0;
          long keepAliveClientRevision = -1;
          Throwable primaryFailure = null;
          try {
            ownerA = startOwnerProcess(ownerARuntime, ownerAActivation,
                beforeFailover.artifacts().resolve("owner-a.log"));
            ownerB = startOwnerProcess(ownerBRuntime, ownerBActivation,
                beforeFailover.artifacts().resolve("owner-b.log"));
            assertTrue(ownerA.pid() != ProcessHandle.current().pid());
            assertTrue(ownerB.pid() != ProcessHandle.current().pid());
            assertTrue(ownerA.pid() != ownerB.pid());

            CommitState active = controlSnapshot(plan, initializer, identity).state();
            assertTrue(isOwnerForPeer(active, "owner-a"));
            oldEpoch = active.ownerEpoch();

            runKeepAliveTargetDriver(beforeFailover, "NATIVE", ownerBClientPort);
            compareKeepAlive(beforeFailover, "NATIVE");
            CommitState afterForwardedKeepAlive =
                controlSnapshot(plan, initializer, identity).state();
            assertEquals(1, afterForwardedKeepAlive.publishedRevision(),
                "the Go KeepAlive stream routed through the standby must not create KV revisions");
            assertTrue(afterForwardedKeepAlive.publishedSequence() > active.publishedSequence(),
                "forwarded KeepAlive must be durably selected before the client receives success");
            long renewedLease = leaseIdForPrefix(beforeFailover.prefix(), "keepalive-a");
            var forwardedReply = keepAliveThrough(ownerBClientPort, renewedLease).getFirst();
            assertTrue(forwardedReply.getTTL() > 0);
            assertEquals(initialized.clusterId(), forwardedReply.getHeader().getClusterId());
            assertEquals(2, forwardedReply.getHeader().getMemberId());

            long recoveryLease = leaseIdForPrefix(
                beforeFailover.prefix(), "keepalive-owner-recovery");
            Rpc.LeaseGrantResponse recoveryLeaseGrant = grantThrough(
                ownerBClientPort, recoveryLease, 120);
            assertEquals(recoveryLease, recoveryLeaseGrant.getID());
            assertEquals(120, recoveryLeaseGrant.getTTL(),
                "the failover probe needs a live Lease while the new owner is elected");

            ownerAStop = terminateOwnerProcess(ownerA, true);
            CommitState recovered = awaitOwner(plan, initializer, identity, "owner-b", oldEpoch);
            newEpoch = recovered.ownerEpoch();
            assertTrue(newEpoch > oldEpoch);
            assertRejectedStaleOwner(ownerBForwarding, trustedPeers, ownerBPeerPort,
                ownerACertificate, source.tableGroupId(), runtimeForwardingIdentity,
                recoveryLease, oldEpoch);

            LeaseState beforeRecoveryKeepAlive = leaseStateAt(
                plan, initializer, identity, recoveryLease, recovered.publishedSequence());
            KeepAliveOnceObservation keepAliveOnce = runKeepAliveOnceAfterRecovery(
                afterFailover, ownerBClientPort, recoveryLease);
            keepAliveClientPid = keepAliveOnce.processId();
            keepAliveClientExitCode = keepAliveOnce.exitCode();
            keepAliveClientTtl = keepAliveOnce.ttl();
            keepAliveClientRevision = keepAliveOnce.revision();
            CommitState afterRecoveryKeepAlive =
                controlSnapshot(plan, initializer, identity).state();
            assertTrue(isOwnerForPeer(afterRecoveryKeepAlive, "owner-b"));
            assertEquals(recovered.publishedRevision(), keepAliveOnce.revision(),
                "the original Go client must observe the unchanged KV revision after owner failover");
            assertEquals(recovered.publishedRevision(), afterRecoveryKeepAlive.publishedRevision(),
                "KeepAliveOnce must not create a KV revision");
            assertTrue(afterRecoveryKeepAlive.publishedSequence() > recovered.publishedSequence(),
                "KeepAliveOnce must persist its auxiliary result before returning");
            LeaseState afterRecoveryLease = leaseStateAt(plan, initializer, identity, recoveryLease,
                afterRecoveryKeepAlive.publishedSequence());
            assertEquals(beforeRecoveryKeepAlive.renewalGeneration() + 1,
                afterRecoveryLease.renewalGeneration());
            assertTrue(afterRecoveryLease.sequence() > beforeRecoveryKeepAlive.sequence());
            assertTrue(afterRecoveryLease.deadlineEpochMillis()
                > beforeRecoveryKeepAlive.deadlineEpochMillis());
          } catch (Exception | Error failure) {
            primaryFailure = failure;
            throw failure;
          } finally {
            List<Throwable> cleanupFailures = new ArrayList<>();
            OwnerProcessCleanup ownerACleanup = ownerAStop == null
                ? stopOwnerProcessForEvidence(ownerA, true, cleanupFailures)
                : new OwnerProcessCleanup(ownerAStop, false);
            ownerAStop = ownerACleanup.termination();
            OwnerProcessCleanup ownerBCleanup = stopOwnerProcessForEvidence(ownerB, false, cleanupFailures);
            ownerBStop = ownerBCleanup.termination();
            try {
              writeOwnerProcessEvidence(beforeFailover.artifacts().resolve("dual-process.json"),
                  ownerA, ownerB, ownerAStop, ownerBStop, oldEpoch, newEpoch,
                  keepAliveClientPid, keepAliveClientExitCode,
                  keepAliveClientTtl, keepAliveClientRevision);
            } catch (IOException failure) {
              cleanupFailures.add(failure);
            }
            if (ownerACleanup.interrupted() || ownerBCleanup.interrupted()) {
              Thread.currentThread().interrupt();
            }
            if (!cleanupFailures.isEmpty()) {
              IOException cleanupFailure = new IOException("dual-process test cleanup was incomplete");
              cleanupFailures.forEach(cleanupFailure::addSuppressed);
              if (primaryFailure != null) primaryFailure.addSuppressed(cleanupFailure);
              else throw cleanupFailure;
            }
          }
        }
      }
    }
  }

  static boolean verifyPersistedKeepAliveSurvivesLostOwnerReplyAndRestart(
      Path testDirectory) throws Exception {
    RunContext run = prepareRun();
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      InitializationConfiguration initialization = initialization("native-cell-versions.json", clientPort);
      var source = TableGroupPlanFactory.create(initialization, "local", null);
      M3TableGroupPlan plan = M3TableGroupPlan.from(source);
      var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
          source.tableGroupId(), source.hbaseProfile().id());
      var runtimeForwardingIdentity = runtimeForwardingIdentity(source.identityProviderId(), authScope);
      try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
              cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
              (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
              1, 1, Duration.ofSeconds(30));
           var credentials = new io.etcdhbase.spi.identity.BackendCredentials("m4-lost-reply", new char[0])) {
        var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
            .get(30, TimeUnit.SECONDS);
        M3RuntimeConfiguration firstActivation = activation("1");
        var initialized = M3TableGroupInitializer.initializeFresh(plan,
            "sha256:" + source.fingerprint(), identity, initializer, firstActivation.pinConfiguration());
        RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
            List.of(new RuntimeConfiguration.Listener("m4-lost-reply", "127.0.0.1", 0,
                source.tableGroupId())), initialization.hbaseProfiles(), initialization.saltProfiles(),
            initialization.tableGroups(), RESOURCES);

        TestTlsCertificate ownerACertificate = TestTlsCertificate.create(testDirectory, "owner-a");
        try {
          TestTlsCertificate ownerBCertificate = TestTlsCertificate.create(testDirectory, "owner-b");
          try {
            Path trustedPeers = testDirectory.resolve("trusted-lost-reply-peers.pem");
            Files.writeString(trustedPeers,
                Files.readString(ownerACertificate.certificate())
                    + System.lineSeparator()
                    + Files.readString(ownerBCertificate.certificate()));
            int ownerAPort = availablePort();
            int ownerBPort = availablePort();
            var peers = List.of(
                new M3RuntimeConfiguration.OwnerPeer("owner-a", "127.0.0.1", ownerAPort, "owner-a"),
                new M3RuntimeConfiguration.OwnerPeer("owner-b", "127.0.0.1", ownerBPort, "owner-b"));
            var ownerAForwarding = new M3RuntimeConfiguration.OwnerForwardingSettings(
                "127.0.0.1", ownerAPort, ownerACertificate.certificate().toString(),
                ownerACertificate.privateKey().toString(), trustedPeers.toString(), peers);
            var ownerBForwarding = new M3RuntimeConfiguration.OwnerForwardingSettings(
                "127.0.0.1", ownerBPort, ownerBCertificate.certificate().toString(),
                ownerBCertificate.privateKey().toString(), trustedPeers.toString(), peers);

            RuntimeServer ownerA = RuntimeServer.startM3(configuration,
                ownerActivation("1", "owner-a", ownerAForwarding));
            RuntimeServer ownerB = null;
            try {
              ownerB = RuntimeServer.startM3(configuration,
                  ownerActivation("2", "owner-b", ownerBForwarding));
              assertTrue(ownerA.dataReady());
              assertTrue(ownerB.dataReady());

              long leaseId = leaseIdForPrefix(run.prefix(), "keepalive-lost-reply");
              grantThrough(ownerB, leaseId, 30);
              CommitState beforeRenewal = controlSnapshot(plan, initializer, identity).state();
              LeaseState leaseBeforeRenewal = leaseStateAt(plan, initializer, identity, leaseId,
                  beforeRenewal.publishedSequence());
              long originalOwnerEpoch = beforeRenewal.ownerEpoch();
              Rpc.LeaseKeepAliveRequest keepAlive = Rpc.LeaseKeepAliveRequest.newBuilder()
                  .setID(leaseId).build();
              InvocationId invocation = InvocationId.create();
              ForwardMutationRequest firstAttempt = keepAliveForwardRequest(
                  source.tableGroupId(), runtimeForwardingIdentity, keepAlive, invocation, originalOwnerEpoch);
              ForwardMutationResponse droppedReply = forwardMutationAndDropReply(
                  ownerAPort, "owner-a", trustedPeers, ownerBCertificate, firstAttempt);
              assertEquals(ForwardMutationResponse.Outcome.COMPLETED, droppedReply.getOutcome(),
                  () -> "lost-reply forwarding failed: " + droppedReply.getFailureCode());
              assertTrue(java.util.Arrays.equals(invocation.bytes(), droppedReply.getOperationResult().toByteArray()));

              CommitState selected = controlSnapshot(plan, initializer, identity).state();
              assertEquals(beforeRenewal.publishedRevision(), selected.publishedRevision(),
                  "KeepAlive must not publish a KV revision");
              LeaseState leaseAfterRenewal = leaseStateAt(plan, initializer, identity, leaseId,
                  selected.publishedSequence());
              assertEquals(leaseBeforeRenewal.renewalGeneration() + 1,
                  leaseAfterRenewal.renewalGeneration(),
                  "the selected renewal must persist its generation, not only advance control state");
              assertTrue(leaseAfterRenewal.deadlineEpochMillis()
                  > leaseBeforeRenewal.deadlineEpochMillis(),
                  "the selected renewal must persist a later deadline");
              assertTrue(leaseAfterRenewal.sequence() > beforeRenewal.publishedSequence(),
                  "the durable Lease version must follow the pre-renewal view");
              long selectedOwnerEpoch = selected.ownerEpoch();

              ownerA.close();
              M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
              CommitState newOwner = awaitOwner(plan, initializer, identity, "owner-b", selectedOwnerEpoch);
              ForwardMutationRequest retry = keepAliveForwardRequest(
                  source.tableGroupId(), runtimeForwardingIdentity, keepAlive, invocation, newOwner.ownerEpoch());
              ForwardMutationResponse recoveredReply = forwardMutation(
                  ownerBPort, "owner-b", trustedPeers, ownerACertificate, retry);
              assertEquals(ForwardMutationResponse.Outcome.COMPLETED, recoveredReply.getOutcome());
              assertTrue(java.util.Arrays.equals(invocation.bytes(), recoveredReply.getOperationResult().toByteArray()));

              CommitState afterRetry = controlSnapshot(plan, initializer, identity).state();
              assertTrue(afterRetry.publishedSequence() >= selected.publishedSequence(),
                  "replay cannot move the durable sequence backwards");
              assertEquals(selected.publishedRevision(), afterRetry.publishedRevision());
              assertEquals(leaseAfterRenewal, leaseStateAt(plan, initializer, identity, leaseId,
                  afterRetry.finalizedSequence()),
                  "replay after owner takeover must preserve the exact durable Lease version");
              Rpc.LeaseTimeToLiveResponse ttl = timeToLive(ownerB, leaseId, true);
              assertEquals(leaseId, ttl.getID());
              assertTrue(ttl.getTTL() > 0);
              assertEquals(0, ttl.getKeysCount());
              assertEquals(initialized.clusterId(), ttl.getHeader().getClusterId());
              assertEquals(selected.publishedRevision(), ttl.getHeader().getRevision());
              return true;
            } finally {
              if (ownerB != null) ownerB.close();
              ownerA.close();
            }
          } finally {
            ownerBCertificate.delete();
          }
        } finally {
          ownerACertificate.delete();
        }
      }
    }
  }

  static void verifyPersistedKeepAliveSurvivesClientCancellationBeforeResponse() throws Exception {
    RunContext run = prepareRun();
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      InitializationConfiguration initialization = initialization("native-cell-versions.json", clientPort);
      var source = TableGroupPlanFactory.create(initialization, "local", null);
      M3TableGroupPlan plan = M3TableGroupPlan.from(source);
      var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
          source.tableGroupId(), source.hbaseProfile().id());
      try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
              cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
              (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
              1, 1, Duration.ofSeconds(30));
           var credentials = new io.etcdhbase.spi.identity.BackendCredentials(
               "m4-client-cancel", new char[0])) {
        var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
            .get(30, TimeUnit.SECONDS);
        M3RuntimeConfiguration firstActivation = activation("1");
        var initialized = M3TableGroupInitializer.initializeFresh(plan,
            "sha256:" + source.fingerprint(), identity, initializer, firstActivation.pinConfiguration());
        RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
            List.of(new RuntimeConfiguration.Listener("m4-client-cancel", "127.0.0.1", 0,
                source.tableGroupId())), initialization.hbaseProfiles(), initialization.saltProfiles(),
            initialization.tableGroups(), RESOURCES);

        long leaseId = leaseIdForPrefix(run.prefix(), "client-cancel");
        CommitState renewed;
        LeaseState renewedLease;
        long checkpointBeforeRestart;
        try (RuntimeServer server = RuntimeServer.startM3(configuration, firstActivation)) {
          assertTrue(server.dataReady());
          Rpc.LeaseGrantResponse granted = grantThrough(server, leaseId, 180);
          assertEquals(initialized.clusterId(), granted.getHeader().getClusterId());
          CommitState before = controlSnapshot(plan, initializer, identity).state();
          LeaseState leaseBeforeRenewal = leaseStateAt(plan, initializer, identity, leaseId,
              before.publishedSequence());
          var channel = NettyChannelBuilder.forAddress(
              "127.0.0.1", server.boundListeners().getFirst().port()).usePlaintext().build();
          var responseReceived = new AtomicBoolean();
          var closed = new CountDownLatch(1);
          var closeStatus = new AtomicReference<Status>();
          try {
            // Force unrelated durable progress after the baseline, before the renewal is sent.
            awaitExpiryCheckpoint(plan, initializer, identity,
                before.finalizedSequence() + 1);
            ClientCall<Rpc.LeaseKeepAliveRequest, Rpc.LeaseKeepAliveResponse> call = channel.newCall(
                LeaseGrpc.getLeaseKeepAliveMethod(),
                CallOptions.DEFAULT.withDeadlineAfter(90, TimeUnit.SECONDS));
            call.start(new ClientCall.Listener<>() {
              @Override public void onMessage(Rpc.LeaseKeepAliveResponse response) {
                responseReceived.set(true);
              }
              @Override public void onClose(Status status, Metadata trailers) {
                closeStatus.set(status);
                closed.countDown();
              }
            }, new Metadata());
            call.sendMessage(Rpc.LeaseKeepAliveRequest.newBuilder().setID(leaseId).build());

            RenewalObservation observation = awaitRenewedLease(plan, initializer, identity,
                leaseId, leaseBeforeRenewal, Duration.ofSeconds(30));
            renewed = observation.control();
            renewedLease = observation.lease();
            assertEquals(before.publishedRevision(), renewed.publishedRevision(),
                "KeepAlive must not change the KV revision");
            assertEquals(leaseBeforeRenewal.renewalGeneration() + 1,
                renewedLease.renewalGeneration(),
                "the durable renewal must advance the persisted generation before response delivery");
            assertTrue(renewedLease.deadlineEpochMillis()
                > leaseBeforeRenewal.deadlineEpochMillis(),
                "the durable renewal must persist a later deadline before response delivery");
            assertTrue(renewedLease.sequence() <= renewed.finalizedSequence());
            LeaseExpiryCheckpoint beforeCancellationCheckpoint = awaitExpiryCheckpoint(
                plan, initializer, identity, renewed.finalizedSequence() + 1);

            // Manual ClientCall flow control is intentionally left at zero demand. The server may
            // persist and queue its response, but the client cancels before consuming that response.
            call.cancel("cancel after durable renewal", null);
            assertTrue(closed.await(10, TimeUnit.SECONDS), "client cancellation did not close the RPC");
            assertEquals(Status.Code.CANCELLED, closeStatus.get().getCode());
            assertFalse(responseReceived.get(), "the cancelled client must not consume a success response");
            LeaseExpiryCheckpoint afterCancellationCheckpoint = awaitExpiryCheckpoint(
                plan, initializer, identity, beforeCancellationCheckpoint.sequence() + 1);
            CommitState afterCancel = controlSnapshot(plan, initializer, identity).state();
            assertEquals(renewed.publishedRevision(), afterCancel.publishedRevision());
            assertEquals(renewedLease, leaseStateAt(plan, initializer, identity, leaseId,
                afterCancel.finalizedSequence()),
                "cancellation must not undo or duplicate the selected renewal");
            checkpointBeforeRestart = afterCancellationCheckpoint.sequence();
          } finally {
            channel.shutdownNow();
            assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
          }
        }

        M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
        try (RuntimeServer restarted = RuntimeServer.startM3(configuration, activation("2"))) {
          assertTrue(restarted.dataReady());
          awaitExpiryCheckpoint(plan, initializer, identity, checkpointBeforeRestart + 1);
          CommitState recovered = controlSnapshot(plan, initializer, identity).state();
          assertEquals(renewed.publishedRevision(), recovered.publishedRevision());
          assertEquals(renewedLease,
              leaseStateAt(plan, initializer, identity, leaseId, recovered.finalizedSequence()),
              "restart must retain the exact renewed Lease version selected before cancellation");
          Rpc.LeaseTimeToLiveResponse ttl = timeToLive(restarted, leaseId, true);
          assertEquals(leaseId, ttl.getID());
          assertTrue(ttl.getTTL() > 0, "the durable renewal must survive client cancellation and restart");
          assertEquals(180, ttl.getGrantedTTL());
          assertEquals(0, ttl.getKeysCount());
          assertEquals(initialized.clusterId(), ttl.getHeader().getClusterId());
          assertEquals(renewed.publishedRevision(), ttl.getHeader().getRevision());
        }
      }
    }
  }

  private record RenewalObservation(CommitState control, LeaseState lease) {}

  private static RenewalObservation awaitRenewedLease(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      long leaseId, LeaseState before, Duration timeout) throws Exception {
    long deadline = Math.addExact(System.nanoTime(), timeout.toNanos());
    while (System.nanoTime() < deadline) {
      CommitState state = controlSnapshot(plan, provider, identity).state();
      LeaseState lease = leaseStateAt(plan, provider, identity, leaseId, state.finalizedSequence());
      if (lease.renewalGeneration() == before.renewalGeneration() + 1) {
        return new RenewalObservation(state, lease);
      }
      if (lease.renewalGeneration() > before.renewalGeneration() + 1) {
        throw new AssertionError("more than one KeepAlive decision was published before cancellation");
      }
      TimeUnit.MILLISECONDS.sleep(25);
    }
    throw new AssertionError("KeepAlive decision was not finalized before the client deadline");
  }

  private static LeaseState leaseStateAt(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      long leaseId, long sequence) throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
      var records = HBaseSystemRecordStore.open(provider, identity, plan.system(), scope).join();
      var entry = records.latestAtOrBefore(SystemRecordType.LEASE_STATE,
          LeaseRecordIdentity.routePrefix(leaseId), sequence, scope).join().orElseThrow();
      assertEquals(leaseId, LeaseRecordIdentity.leaseId(entry.identity()));
      long versionSequence = LeaseRecordIdentity.sequence(entry.identity());
      assertTrue(versionSequence <= sequence,
          "the visible Lease version cannot come from a future M2 sequence");
      LeaseState state = LeaseStateMessages.decode(entry.value());
      assertEquals(leaseId, state.identity().leaseId());
      assertEquals(versionSequence, state.sequence(),
          "the Lease payload sequence must match its versioned record identity");
      return state;
    }
  }

  private static LeaseState latestLeaseStateAtOrBefore(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      long leaseId, long sequence) throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
      var records = HBaseSystemRecordStore.open(provider, identity, plan.system(), scope).join();
      var entry = records.latestAtOrBefore(SystemRecordType.LEASE_STATE,
          LeaseRecordIdentity.routePrefix(leaseId), sequence, scope).join().orElseThrow();
      assertEquals(leaseId, LeaseRecordIdentity.leaseId(entry.identity()));
      return LeaseStateMessages.decode(entry.value());
    }
  }

  private static CommitState awaitLeaseRevoked(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      long leaseId, long afterSequence, Duration timeout) throws Exception {
    return awaitLeaseRevoked(plan, provider, identity, leaseId, afterSequence, timeout, null);
  }

  private static CommitState awaitLeaseRevoked(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      long leaseId, long afterSequence, Duration timeout,
      AtomicReference<Throwable> abortOnFailure) throws Exception {
    long deadline = Math.addExact(System.nanoTime(), timeout.toNanos());
    while (System.nanoTime() < deadline) {
      if (abortOnFailure != null) {
        Throwable workerFailure = abortOnFailure.get();
        if (workerFailure != null) {
          throw new AssertionError("mixed-load worker failed before Lease expiry completed",
              workerFailure);
        }
      }
      CommitState state = controlSnapshot(plan, provider, identity).state();
      if (state.finalizedSequence() > afterSequence) {
        LeaseState lease = latestLeaseStateAtOrBefore(plan, provider, identity, leaseId,
            state.finalizedSequence());
        if (lease.status() == LeaseState.Status.REVOKED) return state;
      }
      TimeUnit.MILLISECONDS.sleep(50);
    }
    throw new AssertionError("ExpiryWorker did not durably revoke the expired Lease before timeout");
  }

  private static LeaseExpiryCheckpoint awaitExpiryCheckpoint(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      long minimumSequence) throws Exception {
    long deadline = Math.addExact(System.nanoTime(), Duration.ofSeconds(30).toNanos());
    while (System.nanoTime() < deadline) {
      CommitState state = controlSnapshot(plan, provider, identity).state();
      if (state.finalizedSequence() >= minimumSequence) {
        Optional<LeaseExpiryCheckpoint> checkpoint = expiryCheckpointAtOrBefore(
            plan, provider, identity, state.finalizedSequence());
        if (checkpoint.isPresent() && checkpoint.orElseThrow().sequence() >= minimumSequence) {
          return checkpoint.orElseThrow();
        }
      }
      TimeUnit.MILLISECONDS.sleep(50);
    }
    throw new AssertionError("ExpiryWorker did not persist the required checkpoint before timeout");
  }

  private static LeaseExpiryCheckpoint latestExpiryCheckpoint(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      long sequence) throws Exception {
    return expiryCheckpointAtOrBefore(plan, provider, identity, sequence)
        .orElseThrow(() -> new AssertionError("no durable Lease expiry checkpoint was found"));
  }

  private static Optional<LeaseExpiryCheckpoint> expiryCheckpointAtOrBefore(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      long sequence) throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
      var records = HBaseSystemRecordStore.open(provider, identity, plan.system(), scope).join();
      var entry = records.latestAtOrBefore(SystemRecordType.LEASE_EXPIRY_CHECKPOINT,
          LeaseExpiryCheckpoint.routePrefix(), sequence, scope).join();
      if (entry.isEmpty()) return Optional.empty();
      var checkpointEntry = entry.orElseThrow();
      LeaseExpiryCheckpoint checkpoint = LeaseExpiryCheckpoint.decode(checkpointEntry.value());
      assertEquals(LeaseExpiryCheckpoint.sequence(checkpointEntry.identity()), checkpoint.sequence());
      return Optional.of(checkpoint);
    }
  }

  private static Rpc.PutResponse putLeaseBoundKey(RuntimeServer server, byte[] key, long leaseId)
      throws Exception {
    var channel = NettyChannelBuilder.forAddress(
        "127.0.0.1", server.boundListeners().getFirst().port()).usePlaintext().build();
    try {
      Rpc.PutRequest request = Rpc.PutRequest.newBuilder()
          .setKey(ByteString.copyFrom(key))
          .setValue(ByteString.copyFromUtf8("expiry-checkpoint"))
          .setLease(leaseId)
          .build();
      return KVGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS).put(request);
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static void awaitKeysAbsent(RuntimeServer server, byte[]... keys) throws Exception {
    var channel = NettyChannelBuilder.forAddress(
        "127.0.0.1", server.boundListeners().getFirst().port()).usePlaintext().build();
    try {
      var stub = KVGrpc.newBlockingStub(channel);
      long deadline = Math.addExact(System.nanoTime(), Duration.ofSeconds(30).toNanos());
      while (System.nanoTime() < deadline) {
        boolean allAbsent = true;
        for (byte[] key : keys) {
          Rpc.RangeResponse response = stub.withDeadlineAfter(5, TimeUnit.SECONDS)
              .range(Rpc.RangeRequest.newBuilder().setKey(ByteString.copyFrom(key)).build());
          allAbsent &= response.getKvsCount() == 0;
        }
        if (allAbsent) return;
        TimeUnit.MILLISECONDS.sleep(50);
      }
      throw new AssertionError("Lease expiry did not remove every bound key before timeout");
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static Rpc.RangeResponse rangeKey(RuntimeServer server, byte[] key) throws Exception {
    var channel = NettyChannelBuilder.forAddress(
        "127.0.0.1", server.boundListeners().getFirst().port()).usePlaintext().build();
    try {
      return KVGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
          .range(Rpc.RangeRequest.newBuilder().setKey(ByteString.copyFrom(key)).build());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static void deleteKey(RuntimeServer server, byte[] key) throws Exception {
    var channel = NettyChannelBuilder.forAddress(
        "127.0.0.1", server.boundListeners().getFirst().port()).usePlaintext().build();
    try {
      KVGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
          .deleteRange(Rpc.DeleteRangeRequest.newBuilder().setKey(ByteString.copyFrom(key)).build());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static void putUnleasedKey(RuntimeServer server, byte[] key, String value) throws Exception {
    var channel = NettyChannelBuilder.forAddress(
        "127.0.0.1", server.boundListeners().getFirst().port()).usePlaintext().build();
    try {
      KVGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
          .put(Rpc.PutRequest.newBuilder().setKey(ByteString.copyFrom(key))
              .setValue(ByteString.copyFromUtf8(value)).build());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
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

  static void verifyGoLeaseKeepAliveMatchesNativeAcrossLayouts() throws Exception {
    RunContext run = prepareKeepAliveRun();
    prepareNativeKeepAliveEvidence(run);
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      for (int index = 0; index < LAYOUT_FILES.size(); index++) {
        String layoutFile = LAYOUT_FILES.get(index);
        String layout = index == 0 ? "HISTORY" : "NATIVE";
        runKeepAliveLayout(cluster, clientPort, layoutFile, layout, run);
      }
    }
    compareKeepAlive(run, "HISTORY");
    compareKeepAlive(run, "NATIVE");
  }

  static void verifyGoLeaseRevokeMatchesNativeAcrossLayouts() throws Exception {
    RunContext run = prepareRevokeRun();
    prepareNativeRevokeEvidence(run);
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      for (int index = 0; index < LAYOUT_FILES.size(); index++) {
        String layoutFile = LAYOUT_FILES.get(index);
        String layout = index == 0 ? "HISTORY" : "NATIVE";
        runRevokeLayout(cluster, clientPort, layoutFile, layout, run);
      }
    }
    compareRevoke(run, "HISTORY");
    compareRevoke(run, "NATIVE");
  }

  static void verifyExpiryCheckpointResumesAfterRuntimeRestart() throws Exception {
    RunContext parent = prepareRun();
    Path artifacts = parent.artifacts().resolve("j43/checkpoint-restart");
    Files.createDirectories(artifacts);
    RunContext run = new RunContext(parent.root(), artifacts, parent.runId(),
        parent.prefix() + "checkpoint-restart/", artifacts.resolve("unused-trace.jsonl"));

    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      for (int index = 0; index < LAYOUT_FILES.size(); index++) {
        String layoutFile = LAYOUT_FILES.get(index);
        String layout = index == 0 ? "HISTORY" : "NATIVE";
        verifyExpiryCheckpointResumesInLayout(cluster, clientPort, layoutFile, layout, run);
      }
    }
  }

  private static void verifyKeepAliveMetrics(
      io.etcdhbase.server.observability.RequestMetrics.LeaseRenewalSnapshot metrics) {
    assertTrue(metrics.requests() >= 8,
        "KeepAlive metrics must count the four raw and four managed renewals, not Grant calls");
    assertTrue(metrics.responses() > 0);
    assertTrue(metrics.selected() > 0);
    assertTrue(metrics.auxiliaryRecords() > 0);
    assertTrue(metrics.auxiliaryPayloadBytes() > 0);
    assertTrue(metrics.hbaseClientOperations() > 0,
        "KeepAlive metrics must count owner-side HBase client operations");
    assertTrue(metrics.logicalHBaseRequestPayloadBytes() > 0,
        "KeepAlive metrics must count encoded row and cell payload bytes");
    assertTrue(metrics.requestBytes() > 0);
    assertTrue(metrics.responseBytes() > 0);
    for (var stage : io.etcdhbase.server.observability.RequestMetrics.LeaseRenewalStage.values()) {
      assertTrue(metrics.stages().get(stage).count() > 0,
          "missing KeepAlive metric stage " + stage);
    }
    reportKeepAliveMetrics(metrics);
  }

  private static void reportKeepAliveMetrics(
      io.etcdhbase.server.observability.RequestMetrics.LeaseRenewalSnapshot metrics) {
    StringBuilder report = new StringBuilder("M4_KEEPALIVE_METRICS_V1");
    appendMetric(report, "sampling_method", "per-runtime-cumulative-counter-snapshot");
    appendMetric(report, "requests", metrics.requests());
    appendMetric(report, "responses", metrics.responses());
    appendMetric(report, "selected", metrics.selected());
    appendMetric(report, "unknown_outcomes", metrics.unknownOutcomes());
    appendMetric(report, "request_bytes", metrics.requestBytes());
    appendMetric(report, "response_bytes", metrics.responseBytes());
    appendMetric(report, "auxiliary_records", metrics.auxiliaryRecords());
    appendMetric(report, "auxiliary_payload_bytes", metrics.auxiliaryPayloadBytes());
    appendMetric(report, "hbase_client_operations", metrics.hbaseClientOperations());
    appendMetric(report, "logical_hbase_request_payload_bytes",
        metrics.logicalHBaseRequestPayloadBytes());
    for (var stage : io.etcdhbase.server.observability.RequestMetrics.LeaseRenewalStage.values()) {
      var sample = metrics.stages().get(stage);
      String stageName = stage.name().toLowerCase(java.util.Locale.ROOT);
      appendMetric(report, stageName + "_samples", sample.count());
      appendMetric(report, stageName + "_total_nanos", sample.totalNanos());
    }
    System.out.println(report);
  }

  private static void appendMetric(StringBuilder report, String name, String value) {
    report.append(' ').append(name).append('=').append(value);
  }

  private static void appendMetric(StringBuilder report, String name, long value) {
    report.append(' ').append(name).append('=').append(value);
  }

  static void verifyExpiredLeaseIsDurablyRevokedBeforeKeepAliveRetry() throws Exception {
    RunContext run = prepareRun();
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      InitializationConfiguration initialization = initialization("native-cell-versions.json", clientPort);
      var source = TableGroupPlanFactory.create(initialization, "local", null);
      M3TableGroupPlan plan = M3TableGroupPlan.from(source);
      var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
          source.tableGroupId(), source.hbaseProfile().id());
      try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
              cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
              (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
              1, 1, Duration.ofSeconds(30));
           var credentials = new io.etcdhbase.spi.identity.BackendCredentials(
               "m4-expired-keepalive", new char[0])) {
        var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
            .get(30, TimeUnit.SECONDS);
        M3RuntimeConfiguration activation = activation("1");
        var initialized = M3TableGroupInitializer.initializeFresh(plan,
            "sha256:" + source.fingerprint(), identity, initializer, activation.pinConfiguration());
        RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
            List.of(new RuntimeConfiguration.Listener("m4-expired-keepalive", "127.0.0.1", 0,
                source.tableGroupId())), initialization.hbaseProfiles(), initialization.saltProfiles(),
            initialization.tableGroups(), RESOURCES);

        try (RuntimeServer server = RuntimeServer.startM3(configuration, activation)) {
          assertTrue(server.dataReady());
          long leaseId = leaseIdForPrefix(run.prefix(), "expired-active");
          Rpc.LeaseGrantResponse granted = grantThrough(server, leaseId, 5);
          assertEquals(initialized.clusterId(), granted.getHeader().getClusterId());
          byte[] boundKey = (run.prefix() + "expired-active/role").getBytes(StandardCharsets.UTF_8);
          putLeaseBoundKey(server, boundKey, leaseId);
          CommitState before = controlSnapshot(plan, initializer, identity).state();
          LeaseState active = leaseStateAt(plan, initializer, identity, leaseId,
              before.finalizedSequence());
          long lowerBoundExpiry = Math.addExact(active.deadlineEpochMillis(),
              activation.leaseClockUncertaintyMs());
          long remainingMillis = lowerBoundExpiry - System.currentTimeMillis() + 10;
          if (remainingMillis > 0) TimeUnit.MILLISECONDS.sleep(remainingMillis);

          List<Rpc.LeaseKeepAliveResponse> expiredResponses = keepAliveThrough(server, leaseId);
          assertEquals(1, expiredResponses.size());
          Rpc.LeaseKeepAliveResponse expiredResponse = expiredResponses.getFirst();
          assertEquals(leaseId, expiredResponse.getID());
          assertEquals(0, expiredResponse.getTTL(),
              "an expired Lease must not be extended before its durable revoke is selected");

          CommitState after = awaitLeaseRevoked(plan, initializer, identity, leaseId,
              before.publishedSequence(), Duration.ofSeconds(30));
          assertEquals(before.publishedRevision() + 1, after.publishedRevision(),
              "revoking the bound key must advance the KV revision exactly once");
          awaitKeysAbsent(server, boundKey);
          LeaseState revoked = leaseStateAt(plan, initializer, identity, leaseId,
              after.finalizedSequence());
          assertEquals(LeaseState.Status.REVOKED, revoked.status(),
              "expiry must persist the authoritative terminal Lease state");
          assertEquals(-1, timeToLive(server, leaseId, true).getTTL(),
              "TimeToLive must report the durably expired Lease as missing");
          assertEquals(after.publishedRevision(), expiredResponse.getHeader().getRevision());

          List<Rpc.LeaseKeepAliveResponse> retryResponses = keepAliveThrough(server, leaseId);
          assertEquals(1, retryResponses.size());
          assertEquals(0, retryResponses.getFirst().getTTL(),
              "KeepAlive for an already revoked Lease must use the native TTL-zero response");
          CommitState afterRetry = controlSnapshot(plan, initializer, identity).state();
          assertEquals(after.publishedRevision(), afterRetry.publishedRevision(),
              "a repeated KeepAlive must not change the KV revision");
        }
      }
    }
  }

  static void verifyRevokeAndKeepAliveRaceHasOneDurableTerminalOutcome() throws Exception {
    RunContext run = prepareRevokeRun();
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      for (int index = 0; index < LAYOUT_FILES.size(); index++) {
        String layoutFile = LAYOUT_FILES.get(index);
        String layout = index == 0 ? "HISTORY" : "NATIVE";
        withRunningLayout(cluster, clientPort, layoutFile, "m4-revoke-race-" + layout,
            (plan, initializer, identity, server) -> {
              long leaseId = leaseIdForPrefix(run.prefix(), "revoke-race-" + layout);
              byte[] key = (run.prefix() + layout.toLowerCase(java.util.Locale.ROOT)
                  + "/race/role").getBytes(StandardCharsets.UTF_8);
              grantThrough(server, leaseId, 30);
              putLeaseBoundKey(server, key, leaseId);
              CommitState before = controlSnapshot(plan, initializer, identity).state();

              var ready = new CountDownLatch(2);
              var start = new CountDownLatch(1);
              var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
              try {
                var revoke = executor.submit(() -> {
                  ready.countDown();
                  if (!start.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("Revoke race barrier did not open");
                  }
                  return revokeThrough(server, leaseId);
                });
                var keepAlive = executor.submit(() -> {
                  ready.countDown();
                  if (!start.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("KeepAlive race barrier did not open");
                  }
                  return keepAliveThrough(server, leaseId);
                });
                assertTrue(ready.await(10, TimeUnit.SECONDS), "both Lease operations must reach the barrier");
                start.countDown();

                Rpc.LeaseRevokeResponse revokeResponse = revoke.get(60, TimeUnit.SECONDS);
                List<Rpc.LeaseKeepAliveResponse> responses = keepAlive.get(60, TimeUnit.SECONDS);
                assertEquals(1, responses.size());
                assertEquals(leaseId, responses.getFirst().getID());
                assertTrue(responses.getFirst().getTTL() >= 0,
                    "KeepAlive may win before Revoke or observe the committed revoke, but cannot fail as an unknown outcome");

                CommitState after = awaitLeaseRevoked(plan, initializer, identity, leaseId,
                    before.finalizedSequence(), Duration.ofSeconds(30));
                assertEquals(before.publishedRevision() + 1, after.publishedRevision(),
                    "the serialized race must publish exactly the one key deletion revision");
                assertEquals(after.publishedRevision(), revokeResponse.getHeader().getRevision(),
                    "Revoke must return the revision selected for its deletion");
                awaitKeysAbsent(server, key);
                assertEquals(LeaseState.Status.REVOKED,
                    leaseStateAt(plan, initializer, identity, leaseId, after.finalizedSequence()).status());
              } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS),
                    "Lease race workers must be terminated before the table group closes");
              }
            });
      }
    }
  }

  static void verifyStaleExpiryCandidatesPreserveReboundAndUnleasedRecreatedKeys() throws Exception {
    RunContext run = prepareRevokeRun();
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      for (int index = 0; index < LAYOUT_FILES.size(); index++) {
        String layoutFile = LAYOUT_FILES.get(index);
        String layout = index == 0 ? "HISTORY" : "NATIVE";
        withRunningLayout(cluster, clientPort, layoutFile, "m4-stale-expiry-" + layout,
            (plan, initializer, identity, server) -> {
              long oldLeaseId = leaseIdForPrefix(run.prefix(), "rebind-old-" + layout);
              long newLeaseId = leaseIdForPrefix(run.prefix(), "rebind-new-" + layout);
              byte[] reboundKey = (run.prefix() + layout.toLowerCase(java.util.Locale.ROOT)
                  + "/rebound/role").getBytes(StandardCharsets.UTF_8);
              grantThrough(server, oldLeaseId, 5);
              grantThrough(server, newLeaseId, 30);
              putLeaseBoundKey(server, reboundKey, oldLeaseId);
              putLeaseBoundKey(server, reboundKey, newLeaseId);
              CommitState beforeOldExpiry = controlSnapshot(plan, initializer, identity).state();
              CommitState oldLeaseRevoked = awaitLeaseRevoked(plan, initializer, identity, oldLeaseId,
                  beforeOldExpiry.finalizedSequence(), Duration.ofSeconds(30));
              assertEquals(beforeOldExpiry.publishedRevision(), oldLeaseRevoked.publishedRevision(),
                  "an empty old Lease revoke must not publish a KV revision");
              Rpc.RangeResponse rebound = rangeKey(server, reboundKey);
              assertEquals(1, rebound.getKvsCount(), "the rebound key must survive its old expiry candidate");
              assertTrue(timeToLive(server, newLeaseId, true).getKeysList()
                  .contains(ByteString.copyFrom(reboundKey)),
                  "the surviving key must remain bound to the new Lease");

              long deletedLeaseId = leaseIdForPrefix(run.prefix(), "delete-recreate-" + layout);
              byte[] recreatedKey = (run.prefix() + layout.toLowerCase(java.util.Locale.ROOT)
                  + "/recreated/role").getBytes(StandardCharsets.UTF_8);
              grantThrough(server, deletedLeaseId, 5);
              putLeaseBoundKey(server, recreatedKey, deletedLeaseId);
              deleteKey(server, recreatedKey);
              putUnleasedKey(server, recreatedKey, "recreated-without-lease");
              CommitState beforeDeletedExpiry = controlSnapshot(plan, initializer, identity).state();
              CommitState deletedLeaseRevoked = awaitLeaseRevoked(plan, initializer, identity,
                  deletedLeaseId, beforeDeletedExpiry.finalizedSequence(), Duration.ofSeconds(30));
              assertEquals(beforeDeletedExpiry.publishedRevision(), deletedLeaseRevoked.publishedRevision(),
                  "expiry of an already unbound Lease must not mutate the recreated KV");
              var recreated = rangeKey(server, recreatedKey).getKvs(0);
              assertEquals(ByteString.copyFromUtf8("recreated-without-lease"), recreated.getValue());
              assertEquals(0, recreated.getLease(),
                  "a stale candidate must not attach or remove the unleased replacement");
            });
      }
    }
  }

  static void verifyLargeLeaseRevokeDeletesEveryBindingInOneRevision() throws Exception {
    RunContext run = prepareRevokeRun();
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      for (int index = 0; index < LAYOUT_FILES.size(); index++) {
        String layoutFile = LAYOUT_FILES.get(index);
        String layout = index == 0 ? "HISTORY" : "NATIVE";
        withRunningLayout(cluster, clientPort, layoutFile, "m4-large-revoke-" + layout,
            (plan, initializer, identity, server) -> {
              long leaseId = leaseIdForPrefix(run.prefix(), "large-revoke-" + layout);
              grantThrough(server, leaseId, 30);
              List<byte[]> keys = new ArrayList<>(128);
              for (int indexInLease = 0; indexInLease < 128; indexInLease++) {
                keys.add((run.prefix() + layout.toLowerCase(java.util.Locale.ROOT)
                    + "/large/role/" + indexInLease).getBytes(StandardCharsets.UTF_8));
              }
              int registrationBatchSize = 8;
              for (int offset = 0; offset < keys.size(); offset += registrationBatchSize) {
                int end = Math.min(keys.size(), offset + registrationBatchSize);
                Rpc.TxnResponse registered = putLeaseBoundKeysInOneTransaction(
                    server, leaseId, keys.subList(offset, end));
                assertTrue(registered.getSucceeded(),
                    "each bounded registration batch must bind every requested key");
              }
              CommitState before = controlSnapshot(plan, initializer, identity).state();
              revokeThrough(server, leaseId);
              CommitState after = awaitLeaseRevoked(plan, initializer, identity, leaseId,
                  before.finalizedSequence(), Duration.ofSeconds(30));
              assertEquals(before.publishedRevision() + 1, after.publishedRevision(),
                  "a large Lease revoke must publish one revision for the complete binding set");
              awaitKeysAbsent(server, keys.toArray(byte[][]::new));
              LeaseState revoked = leaseStateAt(plan, initializer, identity, leaseId,
                  after.finalizedSequence());
              assertEquals(LeaseState.Status.REVOKED, revoked.status());

              long largeKeyLeaseId = leaseIdForPrefix(run.prefix(), "large-key-bytes-" + layout);
              grantThrough(server, largeKeyLeaseId, 30);
              List<byte[]> largeKeys = new ArrayList<>(64);
              for (int indexInLease = 0; indexInLease < 64; indexInLease++) {
                byte[] prefix = (run.prefix() + layout.toLowerCase(java.util.Locale.ROOT)
                    + "/large/key-bytes/" + indexInLease + "/").getBytes(StandardCharsets.UTF_8);
                byte[] key = new byte[30 * 1024];
                System.arraycopy(prefix, 0, key, 0, prefix.length);
                java.util.Arrays.fill(key, prefix.length, key.length, (byte) 'x');
                largeKeys.add(key);
              }
              for (int offset = 0; offset < largeKeys.size(); offset += registrationBatchSize) {
                int end = Math.min(largeKeys.size(), offset + registrationBatchSize);
                Rpc.TxnResponse registered = putLeaseBoundKeysInOneTransaction(
                    server, largeKeyLeaseId, largeKeys.subList(offset, end));
                assertTrue(registered.getSucceeded(),
                    "each bounded batch of large keys must bind successfully");
              }
              assertTrue(largeKeys.stream().mapToInt(key -> key.length).sum()
                      > activation("1").maxEtcdRequestBytes(),
                  "the total binding key bytes must exceed the per-request size limit");
              StatusRuntimeException ttlFailure = assertThrows(StatusRuntimeException.class,
                  () -> timeToLive(server, largeKeyLeaseId, true));
              assertEquals(Status.Code.RESOURCE_EXHAUSTED, ttlFailure.getStatus().getCode(),
                  "the oversized TimeToLive response must respect the RPC output limit");

              CommitState beforeLargeRevoke = controlSnapshot(plan, initializer, identity).state();
              revokeThrough(server, largeKeyLeaseId);
              CommitState afterLargeRevoke = awaitLeaseRevoked(plan, initializer, identity,
                  largeKeyLeaseId, beforeLargeRevoke.finalizedSequence(), Duration.ofSeconds(30));
              assertEquals(beforeLargeRevoke.publishedRevision() + 1,
                  afterLargeRevoke.publishedRevision(),
                  "an admitted Lease must be revocable even when its total key bytes exceed one request");
              awaitKeysAbsent(server, largeKeys.toArray(byte[][]::new));
              assertEquals(LeaseState.Status.REVOKED,
                  leaseStateAt(plan, initializer, identity, largeKeyLeaseId,
                      afterLargeRevoke.finalizedSequence()).status());
            });
      }
    }
  }

  static void verifyMixedLeaseLoadEvidence(Path takeoverTestDirectory) throws Exception {
    RunContext run = prepareRun();
    Path evidencePath = run.artifacts().resolve("j43/mixed-lease-load.json");
    Files.createDirectories(evidencePath.getParent());
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode evidence = mapper.createObjectNode();
    evidence.put("schema", "etcd-hbase-m4-j29.4/v1");
    evidence.put("status", "passed");
    evidence.put("run_id", run.runId());
    evidence.put("source_commit", gitHead(run.root()));
    ObjectNode sourceFiles = sourceFileHashes(mapper, run.root());
    evidence.set("source_files", sourceFiles);
    evidence.put("source_sha256", sourceFilesSha256(sourceFiles));
    evidence.put("scope", "public-local-minicluster");
    evidence.put("protocol", "etcd-v3.5.23");
    evidence.put("target_capacity_accepted", false);
    evidence.put("physical_hbase_rpc_bytes_collected", false);
    evidence.put("recovery_reserve_usage_measured", false);
    evidence.put("listener_max_concurrent_requests", MIXED_LOAD_RESOURCES.maxConcurrentRequests());
    evidence.put("listener_max_queued_requests", MIXED_LOAD_RESOURCES.maxQueuedRequests());
    evidence.put("latency_sampling", "successful-client-rpcs");
    evidence.set("recovery_reserve", recoveryReserveEvidence(mapper,
        activation("1").maxEtcdRequestBytes()));

    ArrayNode layouts = evidence.putArray("layouts");
    try (var cluster = TemporaryHBaseClusterProcess.start()) {
      int clientPort = cluster.clientConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);
      for (int index = 0; index < LAYOUT_FILES.size(); index++) {
        String layoutFile = LAYOUT_FILES.get(index);
        String layout = index == 0 ? "HISTORY" : "NATIVE";
        ObjectNode layoutEvidence = mapper.createObjectNode();
        layoutEvidence.put("layout", layout);
        ArrayNode concurrencyRows = layoutEvidence.putArray("concurrency");
        withRunningLayout(cluster, clientPort, layoutFile,
            "m4-j29-mixed-" + layout.toLowerCase(java.util.Locale.ROOT), MIXED_LOAD_RESOURCES,
            (plan, initializer, identity, server) -> {
              concurrencyRows.add(runFixedMixedLoad(mapper, run, layout, 1, plan,
                  initializer, identity, server));
              concurrencyRows.add(runFixedMixedLoad(mapper, run, layout, 2, plan,
                  initializer, identity, server));
              concurrencyRows.add(runExpiryMixedLoad(mapper, run, layout, 4, plan,
                  initializer, identity, server));
            });
        layouts.add(layoutEvidence);
      }
    }

    Files.createDirectories(takeoverTestDirectory);
    OwnerTakeoverEvidence takeover = verifyDualRuntimeForwardsKeepAliveOverMtlsAndFencesTheOldOwner(
        takeoverTestDirectory.resolve("owner-takeover"));
    ObjectNode takeoverEvidence = evidence.putObject("owner_takeover");
    takeoverEvidence.put("scenario", "two-runtime-mtls-owner-takeover");
    takeoverEvidence.put("old_owner_epoch", takeover.oldOwnerEpoch());
    takeoverEvidence.put("new_owner_epoch", takeover.newOwnerEpoch());
    takeoverEvidence.put("new_owner", "owner-b");
    takeoverEvidence.put("new_owner_instance", takeover.newOwner());
    takeoverEvidence.put("elapsed_millis", takeover.elapsedMillis());
    mapper.writerWithDefaultPrettyPrinter().writeValue(evidencePath.toFile(), evidence);
  }

  private static ObjectNode runFixedMixedLoad(ObjectMapper mapper, RunContext run, String layout,
      int concurrency, M3TableGroupPlan plan, HBaseBackendIdentityProvider initializer,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity, RuntimeServer server)
      throws Exception {
    List<Long> leaseIds = prepareMixedLoadLeases(server, run, layout, concurrency, "fixed");
    var metrics = server.metrics().getFirst();
    var before = metrics.leaseRenewals();
    MixedLoadResult workload = runMixedLoadWorkers(server, run, layout, concurrency, 24,
        leaseIds, 0, 0, plan, initializer, identity);
    var after = metrics.leaseRenewals();
    return mixedLoadRow(mapper, layout, concurrency, "fixed-iterations", 24,
        workload, before, after);
  }

  private static ObjectNode runExpiryMixedLoad(ObjectMapper mapper, RunContext run, String layout,
      int concurrency, M3TableGroupPlan plan, HBaseBackendIdentityProvider initializer,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity, RuntimeServer server)
      throws Exception {
    List<Long> leaseIds = prepareMixedLoadLeases(server, run, layout, concurrency, "expiry-overlap");
    var metrics = server.metrics().getFirst();
    var expiryBefore = metrics.leaseExpiry();
    long expiringLeaseId = leaseIdForPrefix(run.prefix(), "mixed-expiry-" + layout);
    grantThrough(server, expiringLeaseId, 5);
    byte[] boundKey = (run.prefix() + "mixed/" + layout.toLowerCase(java.util.Locale.ROOT)
        + "/expiry/role").getBytes(StandardCharsets.UTF_8);
    putLeaseBoundKey(server, boundKey, expiringLeaseId);
    CommitState beforeRenewal = controlSnapshot(plan, initializer, identity).state();
    LeaseState initialLease = leaseStateAt(plan, initializer, identity, expiringLeaseId,
        beforeRenewal.finalizedSequence());
    List<Rpc.LeaseKeepAliveResponse> initialRenewal = keepAliveThrough(server, expiringLeaseId);
    assertEquals(1, initialRenewal.size());
    assertTrue(initialRenewal.getFirst().getTTL() > 0,
        "the renewal that creates a stale expiry candidate must succeed");
    CommitState afterRenewal = controlSnapshot(plan, initializer, identity).state();
    LeaseState renewedLease = leaseStateAt(plan, initializer, identity, expiringLeaseId,
        afterRenewal.finalizedSequence());
    assertTrue(renewedLease.deadlineEpochMillis() > initialLease.deadlineEpochMillis(),
        "the first expiry candidate must be older than the authoritative renewed deadline");

    var renewalBefore = metrics.leaseRenewals();
    MixedLoadResult workload = runMixedLoadWorkers(server, run, layout, concurrency, 0,
        leaseIds, expiringLeaseId, afterRenewal.finalizedSequence(), plan, initializer, identity);
    var renewalAfter = metrics.leaseRenewals();
    var expiryAfter = awaitExpiryMetrics(metrics, expiryBefore, Duration.ofSeconds(10));
    CommitState revokedState = workload.expiryState();
    LeaseState revokedLease = leaseStateAt(plan, initializer, identity, expiringLeaseId,
        revokedState.finalizedSequence());
    assertEquals(LeaseState.Status.REVOKED, revokedLease.status());
    long observedDelayMillis = Math.max(0,
        workload.expiryObservedEpochMillis() - revokedLease.deadlineEpochMillis());

    ObjectNode row = mixedLoadRow(mapper, layout, concurrency, "until-expiry-revocation", 0,
        workload, renewalBefore, renewalAfter);
    ObjectNode expiry = row.putObject("expiry");
    long revocationCount = expiryAfter.revocations() - expiryBefore.revocations();
    long staleCandidateCount = expiryAfter.staleCandidates() - expiryBefore.staleCandidates();
    long missingDelaySamples = expiryAfter.missingDelaySamples() - expiryBefore.missingDelaySamples();
    assertTrue(workload.activeWorkersAtExpiry() > 0,
        "the expiry must be selected while Lease renewals and business puts are still running");
    assertTrue(revocationCount > 0, "the mixed workload must include a committed expiry");
    assertTrue(staleCandidateCount > 0, "the renewed Lease must leave a measured stale candidate");
    assertTrue(expiryAfter.scanRows() > expiryBefore.scanRows(),
        "expiry evidence must include bounded system-index scan rows");
    assertTrue(expiryAfter.checkpointSequence() > 0,
        "expiry evidence must include a durable checkpoint sequence");
    expiry.put("expired_during_mixed_load", true);
    expiry.put("active_workers_at_revoke", workload.activeWorkersAtExpiry());
    expiry.put("revocations", revocationCount);
    expiry.put("stale_candidates", staleCandidateCount);
    expiry.put("candidates", expiryAfter.candidates() - expiryBefore.candidates());
    expiry.put("scan_rows", expiryAfter.scanRows() - expiryBefore.scanRows());
    expiry.put("scan_bytes", expiryAfter.scanBytes() - expiryBefore.scanBytes());
    expiry.put("retries", expiryAfter.retries() - expiryBefore.retries());
    expiry.put("trusted_delay_samples", revocationCount - missingDelaySamples);
    expiry.put("missing_trusted_delay_samples", missingDelaySamples);
    expiry.put("trusted_delay_total_millis",
        expiryAfter.totalDelayMillis() - expiryBefore.totalDelayMillis());
    expiry.put("observed_deadline_to_revoke_millis", observedDelayMillis);
    expiry.put("owner_epoch", expiryAfter.ownerEpoch());
    expiry.put("checkpoint_sequence", expiryAfter.checkpointSequence());
    expiry.put("window_stop_exclusive", expiryAfter.windowStopExclusive());
    return row;
  }

  private static List<Long> prepareMixedLoadLeases(RuntimeServer server, RunContext run,
      String layout, int concurrency, String phase) throws Exception {
    List<Long> leaseIds = new ArrayList<>(concurrency);
    for (int worker = 0; worker < concurrency; worker++) {
      long leaseId = leaseIdForPrefix(run.prefix(),
          "mixed-" + layout + "-" + phase + "-concurrency-" + concurrency + "-worker-" + worker);
      grantThrough(server, leaseId, 120);
      byte[] key = (run.prefix() + "mixed/" + layout.toLowerCase(java.util.Locale.ROOT)
          + "/" + phase + "/concurrency/" + concurrency + "/role/" + worker)
          .getBytes(StandardCharsets.UTF_8);
      putLeaseBoundKey(server, key, leaseId);
      leaseIds.add(leaseId);
    }
    return List.copyOf(leaseIds);
  }

  private static MixedLoadResult runMixedLoadWorkers(RuntimeServer server, RunContext run,
      String layout, int concurrency, int fixedIterations, List<Long> leaseIds,
      long expiringLeaseId, long afterExpirySequence, M3TableGroupPlan plan,
      HBaseBackendIdentityProvider initializer,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity) throws Exception {
    boolean runUntilExpiry = expiringLeaseId != 0;
    CountDownLatch clientsReady = new CountDownLatch(concurrency);
    CountDownLatch startGate = new CountDownLatch(1);
    CountDownLatch workersStarted = new CountDownLatch(concurrency);
    AtomicBoolean keepRunning = new AtomicBoolean(true);
    AtomicInteger activeWorkers = new AtomicInteger();
    AtomicLong putSequence = new AtomicLong();
    AtomicLong keepAliveAttempts = new AtomicLong();
    AtomicLong keepAliveResourceExhausted = new AtomicLong();
    AtomicLong putAttempts = new AtomicLong();
    AtomicLong putResourceExhausted = new AtomicLong();
    AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    ConcurrentLinkedQueue<Long> keepAliveLatencies = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<Long> putLatencies = new ConcurrentLinkedQueue<>();
    ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    List<Future<?>> workers = new ArrayList<>(concurrency);
    long startedNanos = 0;
    long expiryObservedEpochMillis = 0;
    int activeWorkersAtExpiry = 0;
    CommitState expiryState = null;

    try {
      for (int worker = 0; worker < concurrency; worker++) {
        int workerIndex = worker;
        workers.add(executor.submit(() -> {
          boolean signaledReady = false;
          boolean enteredLoad = false;
          String currentOperation = "connect";
          int iteration = 0;
          try (MixedLoadClient client = new MixedLoadClient(server)) {
            clientsReady.countDown();
            signaledReady = true;
            if (!startGate.await(30, TimeUnit.SECONDS)) {
              throw new AssertionError("mixed-load start gate did not open");
            }
            activeWorkers.incrementAndGet();
            enteredLoad = true;
            workersStarted.countDown();
            while (keepRunning.get() && (runUntilExpiry || iteration < fixedIterations)) {
              currentOperation = "KeepAlive";
              long keepAliveStarted = System.nanoTime();
              keepAliveAttempts.incrementAndGet();
              try {
                Rpc.LeaseKeepAliveResponse renewal = client.keepAlive(leaseIds.get(workerIndex));
                keepAliveLatencies.add(System.nanoTime() - keepAliveStarted);
                if (renewal.getTTL() <= 0) {
                  throw new AssertionError("active mixed-load Lease returned a zero TTL");
                }
              } catch (StatusRuntimeException failure) {
                if (failure.getStatus().getCode() != Status.Code.RESOURCE_EXHAUSTED) throw failure;
                keepAliveResourceExhausted.incrementAndGet();
                client.reopenKeepAliveAfterResourceExhaustion();
              }

              currentOperation = "business Put";
              long sequence = putSequence.incrementAndGet();
              byte[] key = (run.prefix() + "mixed/" + layout.toLowerCase(java.util.Locale.ROOT)
                  + "/c" + concurrency + "/worker/" + workerIndex + "/" + sequence)
                  .getBytes(StandardCharsets.UTF_8);
              byte[] value = ("mixed-value-" + sequence).getBytes(StandardCharsets.UTF_8);
              long putStarted = System.nanoTime();
              putAttempts.incrementAndGet();
              try {
                client.put(key, value);
                putLatencies.add(System.nanoTime() - putStarted);
              } catch (StatusRuntimeException failure) {
                if (failure.getStatus().getCode() != Status.Code.RESOURCE_EXHAUSTED) throw failure;
                putResourceExhausted.incrementAndGet();
              }
              iteration++;
            }
          } catch (Throwable failure) {
            firstFailure.compareAndSet(null, failure);
            keepRunning.set(false);
            throw new RuntimeException("mixed Lease and business workload failed for worker "
                + workerIndex + " at iteration " + iteration + " during " + currentOperation
                + "; runtime_ready=" + server.dataReady()
                + "; keepalive_metrics=" + server.metrics().getFirst().leaseRenewals()
                + "; expiry_metrics=" + server.metrics().getFirst().leaseExpiry(), failure);
          } finally {
            if (!signaledReady) clientsReady.countDown();
            if (enteredLoad) activeWorkers.decrementAndGet();
          }
        }));
      }

      assertTrue(clientsReady.await(60, TimeUnit.SECONDS),
          "all mixed-load gRPC clients must be ready before the run starts");
      startedNanos = System.nanoTime();
      startGate.countDown();
      if (runUntilExpiry) {
        assertTrue(workersStarted.await(30, TimeUnit.SECONDS),
            "all mixed-load workers must start before waiting for expiry");
        try {
          expiryState = awaitLeaseRevoked(plan, initializer, identity, expiringLeaseId,
              afterExpirySequence, Duration.ofSeconds(45), firstFailure);
        } catch (AssertionError timeoutOrFailure) {
          Throwable workerFailure = firstFailure.get();
          if (workerFailure != null) {
            throw new AssertionError("mixed-load worker failed before expiry completed", workerFailure);
          }
          throw timeoutOrFailure;
        }
        expiryObservedEpochMillis = System.currentTimeMillis();
        activeWorkersAtExpiry = activeWorkers.get();
        assertTrue(activeWorkersAtExpiry > 0,
            "the expiry must finish before the mixed-load workers are stopped");
        keepRunning.set(false);
      }

      long workerTimeoutSeconds = Math.min(MIXED_LOAD_WORKER_MAX_TIMEOUT_SECONDS,
          Math.max(MIXED_LOAD_WORKER_BASE_TIMEOUT_SECONDS
                  + MIXED_LOAD_WORKER_TIMEOUT_PER_ITERATION_SECONDS * fixedIterations,
              MIXED_LOAD_KEEPALIVE_RESPONSE_TIMEOUT_SECONDS + 15L));
      for (Future<?> worker : workers) worker.get(workerTimeoutSeconds, TimeUnit.SECONDS);
      keepRunning.set(false);
      Throwable workerFailure = firstFailure.get();
      if (workerFailure != null) throw new AssertionError("mixed-load worker failed", workerFailure);
      return new MixedLoadResult(System.nanoTime() - startedNanos,
          List.copyOf(keepAliveLatencies), List.copyOf(putLatencies),
          keepAliveAttempts.get(), keepAliveResourceExhausted.get(),
          putAttempts.get(), putResourceExhausted.get(), activeWorkersAtExpiry,
          expiryObservedEpochMillis, expiryState);
    } finally {
      keepRunning.set(false);
      startGate.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS),
          "mixed-load worker threads must finish before the runtime closes");
    }
  }

  private static ObjectNode mixedLoadRow(ObjectMapper mapper, String layout, int concurrency,
      String runMode, int iterationsPerWorker, MixedLoadResult workload,
      io.etcdhbase.server.observability.RequestMetrics.LeaseRenewalSnapshot before,
      io.etcdhbase.server.observability.RequestMetrics.LeaseRenewalSnapshot after) {
    long requests = after.requests() - before.requests();
    long responses = after.responses() - before.responses();
    long selected = after.selected() - before.selected();
    long hbaseOperations = after.hbaseClientOperations() - before.hbaseClientOperations();
    long payloadBytes = after.logicalHBaseRequestPayloadBytes()
        - before.logicalHBaseRequestPayloadBytes();
    var queueBefore = before.stages().get(
        io.etcdhbase.server.observability.RequestMetrics.LeaseRenewalStage.QUEUE);
    var queueAfter = after.stages().get(
        io.etcdhbase.server.observability.RequestMetrics.LeaseRenewalStage.QUEUE);
    long queueSamples = queueAfter.count() - queueBefore.count();
    long queueTotalNanos = queueAfter.totalNanos() - queueBefore.totalNanos();
    assertTrue(requests > 0, "the mixed phase must issue real KeepAlive RPCs");
    assertTrue(responses > 0 && selected >= responses && selected <= requests);
    assertTrue(queueSamples > 0 && queueSamples <= requests);
    assertEquals(responses, workload.keepAliveLatencies().size());
    assertEquals(workload.keepAliveAttempts(), responses + workload.keepAliveResourceExhausted());
    assertEquals(workload.putAttempts(),
        workload.putLatencies().size() + workload.putResourceExhausted());
    assertEquals(workload.keepAliveAttempts(), workload.putAttempts());
    assertTrue(requests <= workload.keepAliveAttempts());
    if (runMode.equals("fixed-iterations")) {
      assertEquals(concurrency * iterationsPerWorker, workload.putAttempts(),
          "fixed load phases must attempt the same operation count per worker");
    }
    assertTrue(hbaseOperations > 0, "KeepAlive must account owner-side logical HBase operations");
    assertTrue(payloadBytes > 0, "KeepAlive must account encoded HBase request payload bytes");

    ObjectNode row = mapper.createObjectNode();
    row.put("layout", layout);
    row.put("concurrency", concurrency);
    row.put("run_mode", runMode);
    row.put("iterations_per_worker", iterationsPerWorker);
    row.put("elapsed_nanos", workload.elapsedNanos());
    row.put("keepalive_requests", requests);
    row.put("keepalive_client_attempts", workload.keepAliveAttempts());
    row.put("keepalive_client_resource_exhausted", workload.keepAliveResourceExhausted());
    row.put("keepalive_responses", responses);
    row.put("selected_renewals", selected);
    row.put("business_puts", workload.putLatencies().size());
    row.put("business_put_attempts", workload.putAttempts());
    row.put("business_put_resource_exhausted", workload.putResourceExhausted());
    row.put("combined_operations", responses + workload.putLatencies().size());
    row.put("keepalive_client_rpc_p99_samples", workload.keepAliveLatencies().size());
    row.set("keepalive_client_rpc_latency_nanos",
        latencySummary(mapper, workload.keepAliveLatencies()));
    row.set("business_put_client_rpc_latency_nanos", latencySummary(mapper, workload.putLatencies()));
    row.put("owner_queue_wait_samples", queueSamples);
    row.put("owner_queue_wait_total_nanos", queueTotalNanos);
    row.put("owner_queue_wait_mean_nanos", queueTotalNanos / queueSamples);
    row.put("renewal_logical_hbase_client_operations", hbaseOperations);
    row.put("renewal_logical_hbase_request_payload_bytes", payloadBytes);
    row.put("logical_hbase_operations_per_server_keepalive_request", (double) hbaseOperations / requests);
    row.put("logical_hbase_request_payload_bytes_per_server_keepalive_request", (double) payloadBytes / requests);
    row.put("unknown_outcomes", after.unknownOutcomes() - before.unknownOutcomes());
    row.put("request_proto_bytes", after.requestBytes() - before.requestBytes());
    row.put("response_proto_bytes", after.responseBytes() - before.responseBytes());
    return row;
  }

  private static ObjectNode latencySummary(ObjectMapper mapper, List<Long> samples) {
    if (samples.isEmpty()) throw new AssertionError("latency evidence requires at least one sample");
    List<Long> sorted = new ArrayList<>(samples);
    Collections.sort(sorted);
    ObjectNode summary = mapper.createObjectNode();
    summary.put("samples", sorted.size());
    summary.put("p50_nanos", percentile(sorted, 50));
    summary.put("p95_nanos", percentile(sorted, 95));
    summary.put("p99_nanos", percentile(sorted, 99));
    return summary;
  }

  private static long percentile(List<Long> sorted, int percentile) {
    int index = Math.max(0, (int) Math.ceil(percentile * sorted.size() / 100.0) - 1);
    return sorted.get(index);
  }

  private static io.etcdhbase.server.observability.RequestMetrics.LeaseExpirySnapshot awaitExpiryMetrics(
      io.etcdhbase.server.observability.RequestMetrics metrics,
      io.etcdhbase.server.observability.RequestMetrics.LeaseExpirySnapshot before,
      Duration timeout) throws InterruptedException {
    long deadline = Math.addExact(System.nanoTime(), timeout.toNanos());
    io.etcdhbase.server.observability.RequestMetrics.LeaseExpirySnapshot observed;
    while (System.nanoTime() < deadline) {
      observed = metrics.leaseExpiry();
      if (observed.revocations() > before.revocations()
          && observed.staleCandidates() > before.staleCandidates()
          && observed.checkpointSequence() > 0) {
        return observed;
      }
      TimeUnit.MILLISECONDS.sleep(25);
    }
    observed = metrics.leaseExpiry();
    throw new AssertionError("expiry metrics did not record a stale candidate, revocation, and checkpoint: "
        + observed);
  }

  private static ObjectNode recoveryReserveEvidence(ObjectMapper mapper,
      int maximumEtcdRequestBytes) {
    var capacity = LeaseBudgetProfile.perGroupCapacity(maximumEtcdRequestBytes);
    var mutationReservation = LeaseBudgetProfile.ownerMutationReservation(maximumEtcdRequestBytes);
    ObjectNode reserve = mapper.createObjectNode();
    reserve.put("scope", "configured-per-table-group");
    reserve.put("requests", capacity.requests());
    reserve.put("queued", capacity.queued());
    reserve.put("operations", capacity.operations());
    reserve.put("scanners", capacity.scanners());
    reserve.put("cleanup", capacity.cleanup());
    reserve.put("queued_bytes", capacity.queuedBytes());
    reserve.put("memory_bytes", capacity.memoryBytes());
    reserve.put("lease_owner_mutation_page_bytes", mutationReservation.page());
    reserve.put("expiry_worker_memory_bytes", LeaseBudgetProfile.EXPIRY_WORKER_MEMORY.total());
    reserve.put("expiry_worker_read_physical_rows",
        LeaseBudgetProfile.EXPIRY_WORKER_READ_LIMITS.physicalRows());
    reserve.put("expiry_worker_read_cell_bytes",
        LeaseBudgetProfile.EXPIRY_WORKER_READ_LIMITS.cellBytes());
    reserve.put("runtime_usage_peak_measured", false);
    return reserve;
  }

  private static ObjectNode sourceFileHashes(ObjectMapper mapper, Path repositoryRoot) throws Exception {
    ObjectNode hashes = mapper.createObjectNode();
    for (String relativePath : MIXED_EVIDENCE_SOURCE_FILES) {
      hashes.put(relativePath, java.util.HexFormat.of().formatHex(
          MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(repositoryRoot.resolve(relativePath)))));
    }
    return hashes;
  }

  private static String sourceFilesSha256(ObjectNode sourceFiles) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    for (String relativePath : MIXED_EVIDENCE_SOURCE_FILES) {
      digest.update(relativePath.getBytes(StandardCharsets.UTF_8));
      digest.update((byte) 0);
      digest.update(sourceFiles.get(relativePath).asText().getBytes(StandardCharsets.US_ASCII));
      digest.update((byte) 0);
    }
    return java.util.HexFormat.of().formatHex(digest.digest());
  }

  private static String gitHead(Path repositoryRoot) throws Exception {
    Process process = new ProcessBuilder("git", "-C", repositoryRoot.toString(), "rev-parse", "HEAD")
        .redirectErrorStream(true).start();
    String head = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
    if (!process.waitFor(10, TimeUnit.SECONDS) || process.exitValue() != 0
        || !head.matches("[0-9a-f]{40}")) {
      process.destroyForcibly();
      throw new IOException("could not bind mixed-load evidence to the source commit");
    }
    return head;
  }

  private static void withRunningLayout(TemporaryHBaseClusterProcess cluster, int clientPort,
      String layoutFile, String listenerName, M4RuntimeAction action) throws Exception {
    withRunningLayout(cluster, clientPort, layoutFile, listenerName, RESOURCES, action);
  }

  private static void withRunningLayout(TemporaryHBaseClusterProcess cluster, int clientPort,
      String layoutFile, String listenerName, RuntimeConfiguration.ResourceBudgets resources,
      M4RuntimeAction action) throws Exception {
    InitializationConfiguration initialization = initialization(layoutFile, clientPort);
    var source = TableGroupPlanFactory.create(initialization, "local", null);
    M3TableGroupPlan plan = M3TableGroupPlan.from(source);
    var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
        source.tableGroupId(), source.hbaseProfile().id());
    try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
            cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            1, 1, Duration.ofSeconds(30));
         var credentials = new io.etcdhbase.spi.identity.BackendCredentials(listenerName, new char[0])) {
      var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
          .get(30, TimeUnit.SECONDS);
      M3RuntimeConfiguration activation = activation("1");
      M3TableGroupInitializer.initializeFresh(plan,
          "sha256:" + source.fingerprint(), identity, initializer, activation.pinConfiguration());
      RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
          List.of(new RuntimeConfiguration.Listener(listenerName, "127.0.0.1", 0,
              source.tableGroupId())), initialization.hbaseProfiles(), initialization.saltProfiles(),
          initialization.tableGroups(), resources);
      try (RuntimeServer server = RuntimeServer.startM3(configuration, activation)) {
        assertTrue(server.dataReady(), "M4 Lease test runtime must be ready before requests are sent");
        action.run(plan, initializer, identity, server);
      }
    }
  }

  @FunctionalInterface
  private interface M4RuntimeAction {
    void run(M3TableGroupPlan plan, HBaseBackendIdentityProvider initializer,
        io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity, RuntimeServer server)
        throws Exception;
  }

  private static void runKeepAliveLayout(TemporaryHBaseClusterProcess cluster, int clientPort,
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
          List.of(new RuntimeConfiguration.Listener("m4-keepalive", "127.0.0.1", 0,
              source.tableGroupId())), initialization.hbaseProfiles(), initialization.saltProfiles(),
          initialization.tableGroups(), RESOURCES);

      RuntimeServer first = RuntimeServer.startM3(configuration, firstActivation);
      CommitState selected;
      try {
        assertTrue(first.dataReady());
        runKeepAliveTargetDriver(run, layout, first.boundListeners().getFirst().port());
        verifyKeepAliveMetrics(first.metrics().getFirst().leaseRenewals());
        selected = controlSnapshot(plan, initializer, identity).state();
        assertEquals(1, selected.publishedRevision(),
            "Grant and KeepAlive must not change the KV revision");
        assertTrue(selected.publishedSequence() > 1,
            "Grant and KeepAlive auxiliary state must be durably selected");
      } finally {
        first.close();
      }

      M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
      long leaseA = leaseIdForPrefix(run.prefix(), "keepalive-a");
      long leaseB = leaseIdForPrefix(run.prefix(), "keepalive-b");
      try (RuntimeServer restarted = RuntimeServer.startM3(configuration, activation("2"))) {
        assertTrue(restarted.dataReady());
        for (long leaseId : List.of(leaseA, leaseB)) {
          Rpc.LeaseTimeToLiveResponse ttl = timeToLive(restarted, leaseId, true);
          assertEquals(leaseId, ttl.getID());
          assertTrue(ttl.getTTL() > 0, "renewed Lease must survive an owner restart");
          assertEquals(30, ttl.getGrantedTTL());
          assertEquals(0, ttl.getKeysCount());
          assertEquals(initialized.clusterId(), ttl.getHeader().getClusterId());
          assertEquals(1, ttl.getHeader().getRevision());
        }
      }
      verifyClockReadiness(plan, initializer, identity, configuration, activation("3"));
    }
  }

  private static void runRevokeLayout(TemporaryHBaseClusterProcess cluster, int clientPort,
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
         var credentials = new io.etcdhbase.spi.identity.BackendCredentials("m4-revoke", new char[0])) {
      var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
          .get(30, TimeUnit.SECONDS);
      M3RuntimeConfiguration firstActivation = activation("1");
      var initialized = M3TableGroupInitializer.initializeFresh(plan,
          "sha256:" + source.fingerprint(), identity, initializer, firstActivation.pinConfiguration());
      RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
          List.of(new RuntimeConfiguration.Listener("m4-revoke", "127.0.0.1", 0,
              source.tableGroupId())), initialization.hbaseProfiles(), initialization.saltProfiles(),
          initialization.tableGroups(), RESOURCES);

      try (RuntimeServer server = RuntimeServer.startM3(configuration, firstActivation)) {
        assertTrue(server.dataReady());
        runRevokeTargetDriver(run, layout, server.boundListeners().getFirst().port());
        CommitState selected = controlSnapshot(plan, initializer, identity).state();
        assertEquals(7, selected.publishedRevision(),
            "two bound-key Revoke transactions must each use one KV revision");
        assertTrue(selected.supportsLeaseExpiryCheckpoints());
        var expiry = server.metrics().getFirst().leaseExpiry();
        assertTrue(expiry.revocations() > 0, "automatic expiry must commit through the worker");
        assertTrue(expiry.scanRows() > 0, "expiry must scan the bounded system index");
        assertTrue(expiry.checkpointSequence() > 0, "expiry progress must be durable");
        assertEquals(initialized.clusterId(), timeToLive(server,
            leaseIdForPrefix(run.prefix(), "expiry-bound"), false).getHeader().getClusterId());
      }
    }
  }

  private static void verifyExpiryCheckpointResumesInLayout(TemporaryHBaseClusterProcess cluster,
      int clientPort, String layoutFile, String layout, RunContext run) throws Exception {
    InitializationConfiguration initialization = initialization(layoutFile, clientPort);
    var source = TableGroupPlanFactory.create(initialization, "local", null);
    M3TableGroupPlan plan = M3TableGroupPlan.from(source);
    var authScope = new io.etcdhbase.spi.identity.AuthenticationScope(
        source.tableGroupId(), source.hbaseProfile().id());
    try (var initializer = new HBaseBackendIdentityProvider(source.identityProviderId(), authScope,
            cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            1, 1, Duration.ofSeconds(30));
         var credentials = new io.etcdhbase.spi.identity.BackendCredentials("m4-expiry-restart", new char[0])) {
      var identity = initializer.authenticate(credentials, authScope).toCompletableFuture()
          .get(30, TimeUnit.SECONDS);
      RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
          List.of(new RuntimeConfiguration.Listener("m4-expiry-restart", "127.0.0.1", 0,
              source.tableGroupId())), initialization.hbaseProfiles(), initialization.saltProfiles(),
          initialization.tableGroups(), RESOURCES);
      M3RuntimeConfiguration firstActivation = activation("1");
      var initialized = M3TableGroupInitializer.initializeFresh(plan,
          "sha256:" + source.fingerprint(), identity, initializer, firstActivation.pinConfiguration());

      long leaseId = leaseIdForPrefix(run.prefix(), "restart-" + layout);
      byte[] firstKey = (run.prefix() + layout.toLowerCase(java.util.Locale.ROOT) + "/role/a")
          .getBytes(StandardCharsets.UTF_8);
      byte[] secondKey = (run.prefix() + layout.toLowerCase(java.util.Locale.ROOT) + "/role/b")
          .getBytes(StandardCharsets.UTF_8);
      CommitState beforeRestart;
      RuntimeServer first = RuntimeServer.startM3(configuration, firstActivation);
      try {
        assertTrue(first.dataReady());
        LeaseExpiryCheckpoint initialCheckpoint = awaitExpiryCheckpoint(
            plan, initializer, identity, 1);
        assertTrue(initialCheckpoint.sequence() > 0);
        assertEquals(initialized.clusterId(), grantThrough(first, leaseId, 5).getHeader().getClusterId());
        putLeaseBoundKey(first, firstKey, leaseId);
        putLeaseBoundKey(first, secondKey, leaseId);
        beforeRestart = controlSnapshot(plan, initializer, identity).state();
        awaitExpiryCheckpoint(plan, initializer, identity, beforeRestart.finalizedSequence());
      } finally {
        first.close();
      }

      M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
      M3RuntimeConfiguration secondActivation = activation("2");
      try (RuntimeServer restarted = RuntimeServer.startM3(configuration, secondActivation)) {
        assertTrue(restarted.dataReady());
        awaitKeysAbsent(restarted, firstKey, secondKey);
        CommitState afterExpiry = controlSnapshot(plan, initializer, identity).state();
        assertEquals(beforeRestart.publishedRevision() + 1, afterExpiry.publishedRevision(),
            "a resumed expiry scan must atomically delete the full Lease binding set");
        assertTrue(afterExpiry.publishedSequence() > beforeRestart.publishedSequence());
        assertEquals(-1, timeToLive(restarted, leaseId, true).getTTL());
        LeaseExpiryCheckpoint checkpoint = latestExpiryCheckpoint(plan, initializer, identity,
            afterExpiry.finalizedSequence());
        assertTrue(checkpoint.sequence() <= afterExpiry.finalizedSequence());
        assertTrue(checkpoint.ownerEpoch() > 0);
        assertTrue((layout.equals("HISTORY") && layoutFile.equals("history-table.json"))
            || (layout.equals("NATIVE") && layoutFile.equals("native-cell-versions.json")),
            "checkpoint recovery must exercise the named HBase history layout");
      }
    }
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
      var identity = authenticate(initializer, credentials, authScope);
      M3RuntimeConfiguration firstActivation = activation("1");
      LayoutBaseline baseline = initializeM4Layout(
          plan, source.fingerprint(), identity, initializer, firstActivation);
      RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
          List.of(new RuntimeConfiguration.Listener("m4-local", "127.0.0.1", 0, source.tableGroupId())),
          initialization.hbaseProfiles(), initialization.saltProfiles(), initialization.tableGroups(), RESOURCES);
      long explicitId = runFirstActivation(
          plan, initializer, identity, configuration, firstActivation, baseline, layout, run);
      verifyRestartedLeaseState(plan, initializer, identity, configuration, baseline.clusterId(),
          explicitId, layout);
      verifyClockReadiness(plan, initializer, identity, configuration, activation("3"));
    }
  }

  private static io.etcdhbase.spi.identity.AuthorizedBackendIdentity authenticate(
      HBaseBackendIdentityProvider initializer,
      io.etcdhbase.spi.identity.BackendCredentials credentials,
      io.etcdhbase.spi.identity.AuthenticationScope authScope) throws Exception {
    return initializer.authenticate(credentials, authScope).toCompletableFuture()
        .get(30, TimeUnit.SECONDS);
  }

  private static LayoutBaseline initializeM4Layout(M3TableGroupPlan plan, String fingerprint,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      HBaseBackendIdentityProvider initializer, M3RuntimeConfiguration activation) throws Exception {
    var initialized = M3TableGroupInitializer.initializeFresh(
        plan, "sha256:" + fingerprint, identity, initializer, activation.pinConfiguration());
    ControlSnapshot preUpgrade = controlSnapshot(plan, initializer, identity);
    assertEquals(CommitState.FORMAT_VERSION, preUpgrade.state().formatVersion());
    long staleWriterNow = System.currentTimeMillis();
    CommitState staleWriterClaim = preUpgrade.state().claimOwner("pre-m4-writer", staleWriterNow,
        Math.addExact(staleWriterNow, 60_000));
    return new LayoutBaseline(initialized.clusterId(), preUpgrade, staleWriterClaim);
  }

  private static long runFirstActivation(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider initializer,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      RuntimeConfiguration configuration, M3RuntimeConfiguration activation,
      LayoutBaseline baseline, String layout, RunContext run) throws Exception {
    RuntimeServer first = RuntimeServer.startM3(configuration, activation);
    try {
      assertTrue(first.dataReady());
      assertOldWriterCannotPublish(plan, initializer, identity,
          baseline.controlSnapshot(), baseline.staleWriterClaim());
      runTargetDriver(run, layout, first.boundListeners().getFirst().port());
      assertControlAndTableAreAuxiliary(plan, initializer, identity);
    } finally {
      first.close();
    }
    long explicitId = leaseIdForPrefix(run.prefix(), "explicit");
    assertTrue(explicitId > 0);
    return explicitId;
  }

  private static void verifyRestartedLeaseState(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider initializer,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      RuntimeConfiguration configuration, long clusterId, long explicitId, String layout)
      throws Exception {
    M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
    if (layout.equals("HISTORY")) {
      assertM4CannotBeDisabledAfterFormatUpgrade(configuration, activation("3", false));
    }
    try (RuntimeServer restarted = RuntimeServer.startM3(configuration, activation("2"))) {
      assertTrue(restarted.dataReady());
      Rpc.LeaseTimeToLiveResponse ttl = timeToLive(restarted, explicitId, true);
      assertEquals(explicitId, ttl.getID());
      assertTrue(ttl.getTTL() > 0);
      assertEquals(30, ttl.getGrantedTTL());
      assertEquals(0, ttl.getKeysCount());
      assertEquals(clusterId, ttl.getHeader().getClusterId());
      assertEquals(1, ttl.getHeader().getRevision());
      Rpc.LeaseLeasesResponse leases = leases(restarted);
      assertTrue(leases.getLeasesList().stream().anyMatch(value -> value.getID() == explicitId));
      assertEquals(clusterId, leases.getHeader().getClusterId());
      assertEquals(1, leases.getHeader().getRevision());
      assertEquals(2, leases.getHeader().getMemberId());
    }
  }

  private static void verifyClockReadiness(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider initializer,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      RuntimeConfiguration configuration, M3RuntimeConfiguration activation) throws Exception {
    M3TableGroupRuntimeIT.waitForLeaseExpiry(plan, initializer, identity);
    var clockHealthy = new AtomicBoolean(true);
    LeaseTimeSource timeSource = () -> {
      if (!clockHealthy.get()) return new LeaseTimeObservation(0, 0, false);
      long currentTimeMillis = System.currentTimeMillis();
      return new LeaseTimeObservation(currentTimeMillis,
          Math.addExact(currentTimeMillis, 2_000), true);
    };
    LongFunction<LeaseTimeSource> factory = ignored -> timeSource;

    try (RuntimeServer runtime = RuntimeServer.startM3WithLeaseTimeSource(
        configuration, activation, factory)) {
      assertTrue(runtime.dataReady());
      clockHealthy.set(false);
      assertFalse(runtime.dataReady());
    }
  }

  private record LayoutBaseline(long clusterId, ControlSnapshot controlSnapshot,
      CommitState staleWriterClaim) {}


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
      var identity = authenticate(initializer, credentials, authScope);
      M3RuntimeConfiguration firstActivation = activation("1");
      LayoutBaseline baseline = initializeM4Layout(
          plan, source.fingerprint(), identity, initializer, firstActivation);
      RuntimeConfiguration configuration = new RuntimeConfiguration(initialization.schemaVersion(),
          List.of(new RuntimeConfiguration.Listener("m4-local", "127.0.0.1", 0, source.tableGroupId())),
          initialization.hbaseProfiles(), initialization.saltProfiles(), initialization.tableGroups(), RESOURCES);

      long publishedRevision;
      RuntimeServer first = RuntimeServer.startM3(configuration, firstActivation);
      try {
        assertTrue(first.dataReady());
        assertOldWriterCannotPublish(plan, initializer, identity,
            baseline.controlSnapshot(), baseline.staleWriterClaim());
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
        assertEquals(baseline.clusterId(), ttl.getHeader().getClusterId());
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

  private static int availablePort() throws IOException {
    try (var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }

  private static List<Integer> availablePorts(int count) throws IOException {
    var sockets = new ArrayList<ServerSocket>(count);
    try {
      for (int index = 0; index < count; index++) {
        sockets.add(new ServerSocket(0, 1, InetAddress.getLoopbackAddress()));
      }
      return sockets.stream().map(ServerSocket::getLocalPort).toList();
    } finally {
      for (ServerSocket socket : sockets) socket.close();
    }
  }

  private static Path writeOwnerProcessRuntimeConfiguration(RunContext run,
      InitializationConfiguration initialization, int clientPort, String ownerName) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    Path template = run.root().resolve("config/examples/m3/server.json");
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(template));
    var profile = initialization.hbaseProfiles().getFirst();
    ((ObjectNode) tree.withArray("hbase_profiles").get(0))
        .put("namespace", profile.namespace()).put("client_port", profile.clientPort());
    ((ObjectNode) tree.withArray("listeners").get(0)).put("port", clientPort)
        .put("table_group_id", initialization.tableGroups().getFirst().id());
    Path output = run.artifacts().resolve("runtime-" + ownerName + ".json");
    Files.createDirectories(output.getParent());
    Files.write(output, mapper.writeValueAsBytes(tree));
    return output;
  }

  private static Path writeOwnerProcessActivation(Path repositoryRoot, Path directory,
      String memberId, String instanceId,
      M3RuntimeConfiguration.OwnerForwardingSettings forwarding) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    Path template = repositoryRoot.resolve("config/examples/m4/activation.json");
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(template));
    tree.put("member_id", memberId)
        .put("owner_lease_ms", TEST_OWNER_LEASE_MILLIS)
        .put("heartbeat_ms", TEST_HEARTBEAT_MILLIS)
        .put("m4_lease_api_enabled", true)
        .put("owner_instance_id", instanceId);
    ObjectNode owner = mapper.createObjectNode()
        .put("bind_host", forwarding.bindHost())
        .put("port", forwarding.port())
        .put("certificate_file", forwarding.certificateFile())
        .put("private_key_file", forwarding.privateKeyFile())
        .put("trusted_ca_file", forwarding.trustedCaFile());
    var peers = owner.putArray("peers");
    for (var peer : forwarding.peers()) {
      peers.addObject().put("instance_id", peer.instanceId())
          .put("host", peer.host()).put("port", peer.port())
          .put("peer_identity", peer.peerIdentity());
    }
    tree.set("owner_forwarding", owner);
    Path output = directory.resolve("activation-" + instanceId + ".json");
    Files.write(output, mapper.writeValueAsBytes(tree));
    return output;
  }

  private static Process startOwnerProcess(Path runtime, Path activation, Path log) throws Exception {
    Files.createDirectories(log.getParent());
    var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-cp", System.getProperty("java.class.path"), M4LeaseRuntimeProcess.class.getName(),
        "--config", runtime.toString(), "--m3-config", activation.toString())
        .directory(runtime.getParent().toFile())
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
    try {
      while (System.nanoTime() < deadline) {
        if (!process.isAlive()) {
          throw new AssertionError("runtime process exited before readiness; log:\n" + tail(log));
        }
        if (Files.exists(log) && Files.readString(log).contains("data_readiness=READY")) return process;
        TimeUnit.MILLISECONDS.sleep(100);
      }
      throw new AssertionError("runtime process did not become ready; log:\n" + tail(log));
    } catch (Exception | Error failure) {
      terminateOwnerProcess(process, true);
      throw failure;
    }
  }

  private static ProcessTermination terminateOwnerProcess(Process process, boolean force)
      throws InterruptedException {
    boolean forced = false;
    if (process.isAlive()) {
      if (force) {
        process.destroyForcibly();
        forced = true;
      } else {
        process.destroy();
      }
      if (!process.waitFor(10, TimeUnit.SECONDS)) {
        forced = true;
        process.destroyForcibly();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS), "runtime process did not terminate");
      }
    }
    return new ProcessTermination(process.exitValue(), forced);
  }

  private static OwnerProcessCleanup stopOwnerProcessForEvidence(Process process, boolean force,
      List<Throwable> cleanupFailures) {
    if (process == null) return new OwnerProcessCleanup(null, false);
    try {
      return new OwnerProcessCleanup(terminateOwnerProcess(process, force), false);
    } catch (InterruptedException | RuntimeException | Error failure) {
      cleanupFailures.add(failure);
      return new OwnerProcessCleanup(null, failure instanceof InterruptedException);
    }
  }

  private static void writeOwnerProcessEvidence(Path output, Process ownerA, Process ownerB,
      ProcessTermination ownerAStop, ProcessTermination ownerBStop, long oldEpoch, long newEpoch,
      long keepAliveClientPid, int keepAliveClientExitCode, long keepAliveClientTtl,
      long keepAliveClientRevision) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode evidence = mapper.createObjectNode()
        .put("kind", "two-independent-runtime-processes")
        .put("caller_pid", ProcessHandle.current().pid())
        .put("owner_a_pid", ownerA == null ? 0 : ownerA.pid())
        .put("owner_b_pid", ownerB == null ? 0 : ownerB.pid())
        .put("owner_a_exit_code", ownerAStop == null ? -1 : ownerAStop.exitCode())
        .put("owner_a_forced_termination", ownerAStop != null && ownerAStop.forced())
        .put("owner_b_exit_code", ownerBStop == null ? -1 : ownerBStop.exitCode())
        .put("owner_b_forced_termination", ownerBStop != null && ownerBStop.forced())
        .put("keepalive_client_pid", keepAliveClientPid)
        .put("keepalive_client_exit_code", keepAliveClientExitCode)
        .put("keepalive_response_ttl", keepAliveClientTtl)
        .put("keepalive_response_revision", keepAliveClientRevision)
        .put("previous_owner_epoch", oldEpoch)
        .put("recovered_owner_epoch", newEpoch);
    Files.createDirectories(output.getParent());
    Files.writeString(output, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(evidence)
        + System.lineSeparator());
  }

  private static M3RuntimeConfiguration ownerActivation(String memberId, String instanceId,
      M3RuntimeConfiguration.OwnerForwardingSettings forwarding) throws Exception {
    M3RuntimeConfiguration base = activation(memberId);
    return new M3RuntimeConfiguration(base.memberId(), base.ownerLeaseMs(), base.heartbeatMs(),
        base.startupTimeoutMs(), base.shutdownTimeoutMs(), base.memoryBytes(),
        base.maxEtcdRequestBytes(), base.pinShards(), base.pinSlotsPerShard(), base.pinLeaseMs(),
        base.readCacheEnabled(), base.m4LeaseApiEnabled(), base.leaseMinTtlSeconds(),
        base.leaseClockUncertaintyMs(), base.leaseClockMaxDriftPpm(), base.leaseMaximumBindings(),
        base.leaseMaximumKeyBytes(), base.leaseMaximumRevokeCostBytes(), instanceId, forwarding,
        base.activationProfile());
  }

  private static Rpc.PutResponse putThrough(RuntimeServer server, String key, long expectedMemberId)
      throws Exception {
    var channel = NettyChannelBuilder.forAddress(
            "127.0.0.1", server.boundListeners().getFirst().port())
        .usePlaintext().build();
    try {
      Rpc.PutResponse response = KVGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
          .put(Rpc.PutRequest.newBuilder()
              .setKey(ByteString.copyFrom(key, StandardCharsets.UTF_8))
              .setValue(ByteString.copyFromUtf8("m4-owner-forwarding"))
              .build());
      assertEquals(expectedMemberId, response.getHeader().getMemberId());
      return response;
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static void assertForwardedPutsRespectZeroQueue(RuntimeServer standby) throws Exception {
    var revisions = new java.util.HashSet<Long>();
    for (int index = 0; index < 4; index++) {
      Rpc.PutResponse response = putThrough(standby, "forwarded-sequential-" + index, 2);
      revisions.add(response.getHeader().getRevision());
    }
    assertEquals(4, revisions.size(),
        "each forwarded write must publish a revision without relying on request queueing");
  }

  private static Rpc.LeaseGrantResponse grantThrough(RuntimeServer server, long leaseId, int ttl)
      throws Exception {
    return grantThrough(server.boundListeners().getFirst().port(), leaseId, ttl);
  }

  private static Rpc.LeaseGrantResponse grantThrough(int port, long leaseId, int ttl)
      throws Exception {
    var channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
        .usePlaintext().build();
    try {
      return LeaseGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
          .leaseGrant(Rpc.LeaseGrantRequest.newBuilder().setID(leaseId).setTTL(ttl).build());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static Rpc.LeaseRevokeResponse revokeThrough(RuntimeServer server, long leaseId)
      throws Exception {
    var channel = NettyChannelBuilder.forAddress(
        "127.0.0.1", server.boundListeners().getFirst().port()).usePlaintext().build();
    try {
      return LeaseGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
          .leaseRevoke(Rpc.LeaseRevokeRequest.newBuilder().setID(leaseId).build());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static Rpc.TxnResponse putLeaseBoundKeysInOneTransaction(
      RuntimeServer server, long leaseId, List<byte[]> keys) throws Exception {
    var channel = NettyChannelBuilder.forAddress(
        "127.0.0.1", server.boundListeners().getFirst().port()).usePlaintext().build();
    try {
      Rpc.TxnRequest.Builder request = Rpc.TxnRequest.newBuilder();
      for (byte[] key : keys) {
        Rpc.PutRequest put = Rpc.PutRequest.newBuilder().setKey(ByteString.copyFrom(key))
            .setValue(ByteString.copyFromUtf8("large-lease-value")).setLease(leaseId).build();
        request.addSuccess(Rpc.RequestOp.newBuilder().setRequestPut(put));
      }
      return KVGrpc.newBlockingStub(channel).withDeadlineAfter(60, TimeUnit.SECONDS)
          .txn(request.build());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static List<Rpc.LeaseKeepAliveResponse> keepAliveThrough(
      RuntimeServer server, long... leaseIds) throws Exception {
    return keepAliveThrough(server.boundListeners().getFirst().port(), leaseIds);
  }

  private static List<Rpc.LeaseKeepAliveResponse> keepAliveThrough(int port, long... leaseIds)
      throws Exception {
    var channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
        .usePlaintext().build();
    var responses = new CopyOnWriteArrayList<Rpc.LeaseKeepAliveResponse>();
    var failure = new AtomicReference<Throwable>();
    var terminated = new CountDownLatch(1);
    try {
      StreamObserver<Rpc.LeaseKeepAliveResponse> receiver = new StreamObserver<>() {
        @Override public void onNext(Rpc.LeaseKeepAliveResponse response) { responses.add(response); }
        @Override public void onError(Throwable cause) {
          failure.set(cause);
          terminated.countDown();
        }
        @Override public void onCompleted() { terminated.countDown(); }
      };
      StreamObserver<Rpc.LeaseKeepAliveRequest> sender = LeaseGrpc.newStub(channel)
          .withDeadlineAfter(60, TimeUnit.SECONDS).leaseKeepAlive(receiver);
      for (long leaseId : leaseIds) {
        sender.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(leaseId).build());
      }
      sender.onCompleted();
      assertTrue(terminated.await(60, TimeUnit.SECONDS), "KeepAlive stream did not terminate");
      if (failure.get() != null) throw new AssertionError("KeepAlive stream failed", failure.get());
      return List.copyOf(responses);
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static boolean isOwnerForPeer(CommitState state, String peerId) throws Exception {
    byte[] digest = MessageDigest.getInstance("SHA-256")
        .digest(peerId.getBytes(StandardCharsets.UTF_8));
    String prefix = "~" + java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(digest) + ":";
    return state.ownerInstance().filter(instance -> instance.startsWith(prefix)).isPresent();
  }

  private static CommitState awaitOwner(M3TableGroupPlan plan,
      HBaseBackendIdentityProvider provider,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      String expectedOwner, long previousEpoch) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    CommitState observed = null;
    while (System.nanoTime() < deadline) {
      observed = controlSnapshot(plan, provider, identity).state();
      if (isOwnerForPeer(observed, expectedOwner)
          && observed.ownerEpoch() > previousEpoch) {
        return observed;
      }
      TimeUnit.MILLISECONDS.sleep(50);
    }
    throw new AssertionError("owner did not advance to " + expectedOwner
        + " with a new epoch; last state=" + observed);
  }

  private static void assertRejectedStaleOwner(
      M3RuntimeConfiguration.OwnerForwardingSettings forwarding, Path trustedPeers, int port,
      TestTlsCertificate staleOwnerCertificate, String tableGroup,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity, long leaseId, long staleEpoch)
      throws Exception {
    Rpc.LeaseKeepAliveRequest keepAlive = Rpc.LeaseKeepAliveRequest.newBuilder().setID(leaseId).build();
    ByteKey operation = MutationOperationEnvelope.leaseKeepAlive(keepAlive.toByteArray());
    ByteKey digest = MutationForwardRequest.digest(tableGroup, operation);
    ByteKey authorization = authorizationDigest(identity);
    var request = ForwardMutationRequest.newBuilder()
        .setTableGroupId(tableGroup)
        .setInvocationId(ByteString.copyFrom(InvocationId.create().bytes()))
        .setRequestDigest(ByteString.copyFrom(digest.bytes()))
        .setAuthorizationScopeDigest(ByteString.copyFrom(authorization.bytes()))
        .setAuthorizationContextReference(ByteString.copyFrom(
            ServiceAuthorizationContextReference.create(identity, authorization).bytes()))
        .setExpectedOwnerEpoch(staleEpoch)
        .setRemainingTimeoutNanos(TimeUnit.SECONDS.toNanos(30))
        .setHopCount(MutationForwardRequest.MAX_HOPS)
        .setOperationBytes(ByteString.copyFrom(operation.bytes()))
        .build();
    SslContext clientTls = GrpcSslContexts.configure(SslContextBuilder.forClient()
        .keyManager(staleOwnerCertificate.certificate().toFile(), staleOwnerCertificate.privateKey().toFile())
        .trustManager(trustedPeers.toFile())).build();
    var channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
        .sslContext(clientTls).overrideAuthority(forwarding.peers().stream()
            .filter(peer -> peer.port() == port).findFirst().orElseThrow().peerIdentity())
        .disableRetry().build();
    try {
      ForwardMutationResponse response = InternalOwnerServiceGrpc.newBlockingStub(channel)
          .withDeadlineAfter(30, TimeUnit.SECONDS).forwardMutation(request);
      assertEquals(ForwardMutationResponse.Outcome.UNKNOWN, response.getOutcome());
      assertEquals("UNAVAILABLE", response.getFailureCode());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static void assertPeerCertificateWithoutConfiguredIdentityIsRejected(
      int port, String authority, Path trustedPeers, TestTlsCertificate unregisteredPeerCertificate)
      throws Exception {
    SslContext clientTls = GrpcSslContexts.configure(SslContextBuilder.forClient()
        .keyManager(unregisteredPeerCertificate.certificate().toFile(),
            unregisteredPeerCertificate.privateKey().toFile())
        .trustManager(trustedPeers.toFile())).build();
    ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
        .sslContext(clientTls).overrideAuthority(authority).disableRetry().build();
    try {
      StatusRuntimeException failure = assertThrows(StatusRuntimeException.class,
          () -> InternalOwnerServiceGrpc.newBlockingStub(channel)
              .withDeadlineAfter(30, TimeUnit.SECONDS)
              .forwardMutation(ForwardMutationRequest.getDefaultInstance()));
      assertEquals(Status.Code.PERMISSION_DENIED, failure.getStatus().getCode());
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static ForwardMutationRequest keepAliveForwardRequest(String tableGroup,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity,
      Rpc.LeaseKeepAliveRequest keepAlive, InvocationId invocation, long expectedOwnerEpoch)
      throws Exception {
    ByteKey operation = MutationOperationEnvelope.leaseKeepAlive(keepAlive.toByteArray());
    ByteKey digest = MutationForwardRequest.digest(tableGroup, operation);
    ByteKey authorization = authorizationDigest(identity);
    return ForwardMutationRequest.newBuilder()
        .setTableGroupId(tableGroup)
        .setInvocationId(ByteString.copyFrom(invocation.bytes()))
        .setRequestDigest(ByteString.copyFrom(digest.bytes()))
        .setAuthorizationScopeDigest(ByteString.copyFrom(authorization.bytes()))
        .setAuthorizationContextReference(ByteString.copyFrom(
            ServiceAuthorizationContextReference.create(identity, authorization).bytes()))
        .setExpectedOwnerEpoch(expectedOwnerEpoch)
        .setRemainingTimeoutNanos(TimeUnit.SECONDS.toNanos(30))
        .setHopCount(MutationForwardRequest.MAX_HOPS)
        .setOperationBytes(ByteString.copyFrom(operation.bytes()))
        .build();
  }

  private static io.etcdhbase.spi.identity.AuthorizedBackendIdentity runtimeForwardingIdentity(
      String providerId, io.etcdhbase.spi.identity.AuthenticationScope scope) throws IOException {
    String subject = org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getShortUserName();
    // SIMPLE cannot attest a credential generation; match the runtime's provider-issued handle.
    return io.etcdhbase.spi.identity.AuthorizedBackendIdentity.withUnknownCredentialGeneration(
        providerId, subject, "m4-test-forwarding-context", scope, Set.of(scope.tableGroupId()));
  }

  private static ForwardMutationResponse forwardMutationAndDropReply(int port, String authority,
      Path trustedPeers, TestTlsCertificate clientCertificate,
      ForwardMutationRequest request) throws Exception {
    SslContext clientTls = GrpcSslContexts.configure(SslContextBuilder.forClient()
        .keyManager(clientCertificate.certificate().toFile(), clientCertificate.privateKey().toFile())
        .trustManager(trustedPeers.toFile())).build();
    ManagedChannel transport = NettyChannelBuilder.forAddress("127.0.0.1", port)
        .sslContext(clientTls).overrideAuthority(authority).disableRetry().build();
    var receivedReply = new CountDownLatch(1);
    var droppedResponse = new AtomicReference<ForwardMutationResponse>();
    ClientInterceptor dropReply = new ClientInterceptor() {
      @Override
      public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> interceptCall(
          MethodDescriptor<RequestT, ResponseT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
          @Override
          public void start(Listener<ResponseT> responseListener, Metadata headers) {
            ClientCall<RequestT, ResponseT> interceptedCall = this;
            super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
              private final AtomicBoolean terminal = new AtomicBoolean();

              @Override
              public void onMessage(ResponseT message) {
                if (message instanceof ForwardMutationResponse response) droppedResponse.set(response);
                receivedReply.countDown();
                if (terminal.compareAndSet(false, true)) {
                  responseListener.onClose(
                      Status.UNAVAILABLE.withDescription("simulated lost owner reply"), new Metadata());
                  interceptedCall.cancel("simulated lost owner reply", null);
                }
              }

              @Override
              public void onClose(Status status, Metadata trailers) {
                if (terminal.compareAndSet(false, true)) super.onClose(status, trailers);
              }
            }, headers);
          }
        };
      }
    };
    Channel dropAtClient = ClientInterceptors.intercept(transport, dropReply);
    try {
      StatusRuntimeException failure = assertThrows(StatusRuntimeException.class,
          () -> InternalOwnerServiceGrpc.newBlockingStub(dropAtClient)
              .withDeadlineAfter(30, TimeUnit.SECONDS).forwardMutation(request));
      assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
      assertTrue(receivedReply.await(30, TimeUnit.SECONDS),
          "the owner must finish the durable invocation before the test drops its reply");
      return java.util.Objects.requireNonNull(droppedResponse.get(),
          "the owner did not produce a response before the simulated transport loss");
    } finally {
      transport.shutdownNow();
      assertTrue(transport.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static ForwardMutationResponse forwardMutation(int port, String authority,
      Path trustedPeers, TestTlsCertificate clientCertificate,
      ForwardMutationRequest request) throws Exception {
    SslContext clientTls = GrpcSslContexts.configure(SslContextBuilder.forClient()
        .keyManager(clientCertificate.certificate().toFile(), clientCertificate.privateKey().toFile())
        .trustManager(trustedPeers.toFile())).build();
    ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
        .sslContext(clientTls).overrideAuthority(authority).disableRetry().build();
    try {
      return InternalOwnerServiceGrpc.newBlockingStub(channel)
          .withDeadlineAfter(30, TimeUnit.SECONDS).forwardMutation(request);
    } finally {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static ByteKey authorizationDigest(
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (var output = new DataOutputStream(bytes)) {
      output.writeInt(1);
      output.writeUTF(identity.providerId());
      output.writeUTF(identity.subject());
      output.writeUTF(identity.scope().tableGroupId());
      output.writeUTF(identity.scope().connectionProfileId());
      output.writeLong(identity.credentialGeneration());
      output.writeInt(identity.allowedTableGroupIds().size());
      for (String group : identity.allowedTableGroupIds().stream().sorted().toList()) {
        output.writeUTF(group);
      }
    }
    return ByteKey.copyOf(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
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
    return activation(memberId, enableLeaseApi, TEST_OWNER_LEASE_MILLIS, TEST_HEARTBEAT_MILLIS);
  }

  private static M3RuntimeConfiguration activation(String memberId, boolean enableLeaseApi,
      int ownerLeaseMillis, int heartbeatMillis) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Path path = WireProcesses.repositoryRoot().resolve("config/examples/m4/activation.json");
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(path));
    // MiniCluster admin procedures can exceed a 400 ms heartbeat timeout under Maven load.
    tree.put("member_id", memberId).put("owner_lease_ms", ownerLeaseMillis)
        .put("heartbeat_ms", heartbeatMillis)
        .put("m4_lease_api_enabled", enableLeaseApi);
    return new M3RuntimeConfigurationLoader(M3RuntimeConfiguration.ActivationProfile.PUBLIC_TEST)
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

  private static RunContext prepareKeepAliveRun() throws IOException {
    RunContext parent = prepareRun();
    Path artifacts = parent.artifacts().resolve("j42");
    Files.createDirectories(artifacts);
    return new RunContext(parent.root(), artifacts, parent.runId(),
        parent.prefix() + "keepalive/", artifacts.resolve("native-trace.jsonl"));
  }

  private static RunContext prepareKeepAliveProcessRun(String phase) throws IOException {
    RunContext parent = prepareRun();
    Path artifacts = parent.artifacts().resolve("j42-process").resolve(phase);
    Files.createDirectories(artifacts);
    return new RunContext(parent.root(), artifacts, parent.runId(),
        parent.prefix() + "dual-process/" + phase + "/", artifacts.resolve("native-trace.jsonl"));
  }

  private static RunContext prepareRevokeRun() throws IOException {
    RunContext parent = prepareRun();
    Path artifacts = parent.artifacts().resolve("j43");
    Files.createDirectories(artifacts);
    return new RunContext(parent.root(), artifacts, parent.runId(),
        parent.prefix() + "revoke/", artifacts.resolve("native-trace.jsonl"));
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

  private static void prepareNativeKeepAliveEvidence(RunContext run) throws Exception {
    if (!Files.exists(run.nativeTrace())) {
      runDriverWithScenario(run, run.artifacts().resolve("native.log"),
          "m4-lease-keepalive.yaml", "run", "--native-data-dir",
          run.artifacts().resolve("native-data").toString(), "--scope", "native-etcd",
          "--prefix", run.prefix(), "--trace", run.nativeTrace().toString(), "--metadata",
          run.artifacts().resolve("native-metadata.json").toString());
    }
  }

  private static void prepareNativeRevokeEvidence(RunContext run) throws Exception {
    if (!Files.exists(run.nativeTrace())) {
      runDriverWithScenario(run, run.artifacts().resolve("native.log"),
          "m4-lease-revoke.yaml", "run", "--native-data-dir",
          run.artifacts().resolve("native-data").toString(), "--scope", "native-etcd",
          "--prefix", run.prefix(), "--trace", run.nativeTrace().toString(), "--metadata",
          run.artifacts().resolve("native-metadata.json").toString());
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

  private static void runKeepAliveTargetDriver(RunContext run, String layout, int port)
      throws Exception {
    String suffix = layout.toLowerCase(java.util.Locale.ROOT);
    runDriverWithScenario(run, run.artifacts().resolve("target-" + suffix + ".log"),
        "m4-lease-keepalive.yaml", "run", "--endpoint", "127.0.0.1:" + port,
        "--scope", "target-hbase", "--layout", layout, "--prefix", run.prefix(), "--trace",
        run.artifacts().resolve("target-" + suffix + "-trace.jsonl").toString(), "--metadata",
        run.artifacts().resolve("target-" + suffix + "-metadata.json").toString());
  }

  private static KeepAliveOnceObservation runKeepAliveOnceAfterRecovery(
      RunContext run, int port, long leaseId) throws Exception {
    Path log = run.artifacts().resolve("keepalive-once-after-recovery.log");
    List<String> command = List.of(run.root().resolve("scripts/dev/go-program.sh").toString(),
        "m4driver", "keepalive-once", "--endpoint", "127.0.0.1:" + port,
        "--lease-id", Long.toString(leaseId), "--timeout", "30s");
    ProcessBuilder builder = new ProcessBuilder(command).directory(run.root().toFile())
        .redirectErrorStream(true).redirectOutput(log.toFile());
    builder.environment().put("M4_RUN_ID", run.runId());
    String source = System.getenv("M4_SOURCE_COMMIT");
    if (source != null) builder.environment().put("M4_SOURCE_COMMIT", source);
    Process process = builder.start();
    try {
      if (!process.waitFor(90, TimeUnit.SECONDS)) {
        throw new AssertionError("recovery KeepAliveOnce Go client timed out; log:\n" + tail(log));
      }
      int exitCode = process.exitValue();
      assertEquals(0, exitCode, "recovery KeepAliveOnce Go client failed; log:\n" + tail(log));
      var response = new ObjectMapper().readTree(Files.readString(log));
      long ttl = response.path("ttl").asLong(0);
      long revision = response.path("revision").asLong(-1);
      assertTrue(ttl > 0, "the original Go client must receive a positive TTL after owner recovery");
      assertTrue(revision > 0, "the Go KeepAliveOnce response must contain a real revision");
      return new KeepAliveOnceObservation(process.pid(), exitCode, ttl, revision);
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS), "recovery KeepAliveOnce client did not stop");
      }
    }
  }

  private static void runRevokeTargetDriver(RunContext run, String layout, int port)
      throws Exception {
    String suffix = layout.toLowerCase(java.util.Locale.ROOT);
    runDriverWithScenario(run, run.artifacts().resolve("target-" + suffix + ".log"),
        "m4-lease-revoke.yaml", "run", "--endpoint", "127.0.0.1:" + port,
        "--scope", "target-hbase", "--layout", layout, "--prefix", run.prefix(), "--trace",
        run.artifacts().resolve("target-" + suffix + "-trace.jsonl").toString(), "--metadata",
        run.artifacts().resolve("target-" + suffix + "-metadata.json").toString());
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

  private static void compareKeepAlive(RunContext run, String layout) throws Exception {
    String suffix = layout.toLowerCase(java.util.Locale.ROOT);
    runDriverWithScenario(run, run.artifacts().resolve("keepalive-" + suffix + ".log"),
        "m4-lease-keepalive.yaml", "compare-keepalive", "--left", run.nativeTrace().toString(),
        "--right", run.artifacts().resolve("target-" + suffix + "-trace.jsonl").toString());
  }

  private static void compareRevoke(RunContext run, String layout) throws Exception {
    String suffix = layout.toLowerCase(java.util.Locale.ROOT);
    runDriverWithScenario(run, run.artifacts().resolve("revoke-" + suffix + ".log"),
        "m4-lease-revoke.yaml", "compare-revoke", "--left", run.nativeTrace().toString(),
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

  private static final class MixedLoadClient implements AutoCloseable {
    private final ManagedChannel channel;
    private final KVGrpc.KVBlockingStub keyValue;
    private MixedKeepAliveStream keepAlive;

    private MixedLoadClient(RuntimeServer server) {
      channel = NettyChannelBuilder.forAddress(
              "127.0.0.1", server.boundListeners().getFirst().port())
          .usePlaintext().disableRetry().build();
      keyValue = KVGrpc.newBlockingStub(channel);
      keepAlive = new MixedKeepAliveStream(channel);
    }

    private Rpc.LeaseKeepAliveResponse keepAlive(long leaseId) throws Exception {
      return keepAlive.send(leaseId);
    }

    private void reopenKeepAliveAfterResourceExhaustion() throws Exception {
      keepAlive.close(true);
      keepAlive = new MixedKeepAliveStream(channel);
    }

    private Rpc.PutResponse put(byte[] key, byte[] value) {
      return keyValue.withDeadlineAfter(60, TimeUnit.SECONDS).put(Rpc.PutRequest.newBuilder()
          .setKey(ByteString.copyFrom(key)).setValue(ByteString.copyFrom(value)).build());
    }

    @Override
    public void close() throws Exception {
      try {
        keepAlive.close(false);
      } finally {
        channel.shutdownNow();
        assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS),
            "benchmark gRPC channel must terminate after the worker finishes");
      }
    }
  }

  private static final class MixedKeepAliveStream {
    private final ArrayBlockingQueue<Rpc.LeaseKeepAliveResponse> responses = new ArrayBlockingQueue<>(1);
    private final AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    private final CountDownLatch streamClosed = new CountDownLatch(1);
    private final StreamObserver<Rpc.LeaseKeepAliveRequest> keepAliveRequests;

    private MixedKeepAliveStream(ManagedChannel channel) {
      keepAliveRequests = LeaseGrpc.newStub(channel).withDeadlineAfter(300, TimeUnit.SECONDS)
          .leaseKeepAlive(new StreamObserver<>() {
            @Override
            public void onNext(Rpc.LeaseKeepAliveResponse response) {
              if (!responses.offer(response)) {
                streamFailure.compareAndSet(null,
                    new IllegalStateException("KeepAlive response queue exceeded its one-item bound"));
              }
            }

            @Override
            public void onError(Throwable failure) {
              streamFailure.compareAndSet(null, failure);
              streamClosed.countDown();
            }

            @Override
            public void onCompleted() {
              streamClosed.countDown();
            }
          });
    }

    private Rpc.LeaseKeepAliveResponse send(long leaseId) throws Exception {
      checkStreamState();
      keepAliveRequests.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(leaseId).build());
      long deadlineNanos = System.nanoTime()
          + TimeUnit.SECONDS.toNanos(MIXED_LOAD_KEEPALIVE_RESPONSE_TIMEOUT_SECONDS);
      while (true) {
        checkStreamState();
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
          throw new AssertionError("KeepAlive response was not received before the benchmark deadline");
        }
        Rpc.LeaseKeepAliveResponse response = responses.poll(
            Math.min(remainingNanos, TimeUnit.MILLISECONDS.toNanos(100)), TimeUnit.NANOSECONDS);
        if (response != null) {
          if (response.getID() != leaseId) {
            throw new AssertionError("KeepAlive response ID differed from the requested Lease");
          }
          return response;
        }
      }
    }

    private void checkStreamState() {
      Throwable failure = streamFailure.get();
      if (failure instanceof StatusRuntimeException statusFailure) throw statusFailure;
      if (failure != null) throw new AssertionError("KeepAlive stream failed", failure);
      if (streamClosed.getCount() == 0) {
        throw new AssertionError("KeepAlive stream closed before returning a response");
      }
    }

    private void close(boolean resourceExhaustionExpected) throws InterruptedException {
      if (streamClosed.getCount() != 0) keepAliveRequests.onCompleted();
      if (!streamClosed.await(10, TimeUnit.SECONDS)) {
        throw new AssertionError("KeepAlive benchmark stream did not close after client completion");
      }
      Throwable failure = streamFailure.get();
      if (failure != null && !(resourceExhaustionExpected
          && Status.fromThrowable(failure).getCode() == Status.Code.RESOURCE_EXHAUSTED)) {
        throw new AssertionError("KeepAlive benchmark stream failed", failure);
      }
    }
  }

  private record MixedLoadResult(long elapsedNanos, List<Long> keepAliveLatencies,
      List<Long> putLatencies, long keepAliveAttempts, long keepAliveResourceExhausted,
      long putAttempts, long putResourceExhausted, int activeWorkersAtExpiry,
      long expiryObservedEpochMillis, CommitState expiryState) {
    private MixedLoadResult {
      keepAliveLatencies = List.copyOf(keepAliveLatencies);
      putLatencies = List.copyOf(putLatencies);
    }
  }

  record OwnerTakeoverEvidence(long oldOwnerEpoch, long newOwnerEpoch,
      long elapsedMillis, String newOwner) {}

  private record RunContext(Path root, Path artifacts, String runId, String prefix, Path nativeTrace) {}

  private record ProcessTermination(int exitCode, boolean forced) {}

  private record KeepAliveOnceObservation(long processId, int exitCode, long ttl, long revision) {}

  private record OwnerProcessCleanup(ProcessTermination termination, boolean interrupted) {}
}
