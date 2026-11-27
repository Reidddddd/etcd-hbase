package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.ByteString;
import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.admin.M3TableGroupInitializer;
import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.HistoryGcCheckpointStore;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.hbase.codec.RowFenceMessages;
import io.etcdhbase.hbase.codec.RevisionTimestamp;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.identity.KerberosUserContext;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.hbase.storage.HBaseManifestRepository;
import io.etcdhbase.hbase.storage.HBaseSystemRecordStore;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.server.commit.M3TableGroupRuntime;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.io.TempDir;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Production RPC, real authenticated ADMIN checks, durable pins and both physical layouts. */
@Timeout(600)
class J52PhysicalCompactIT {
  private static final SnapshotPinConfiguration PINS =
      new SnapshotPinConfiguration(2, 16, Duration.ofSeconds(60));
  private static final RuntimeConfiguration.ResourceBudgets RESOURCES =
      new RuntimeConfiguration.ResourceBudgets(65_536, 8, 120_000, 4 << 20, 4 << 20, 8, 0);
  private static final ByteString KEY = ByteString.copyFromUtf8("physical-key");
  @TempDir(cleanup = org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS) Path artifacts;

  @Test
  void zeroRevisionOnANonemptyTablePersistsCompletionWithoutBusinessScansInBothLayouts() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.startSecure()) {
      for (String layout : List.of("history-table.json", "native-cell-versions.json")) {
        for (boolean physical : List.of(false, true)) verifyZeroRevision(cluster, layout, physical);
      }
    }
  }

  private static void verifyZeroRevision(TemporaryHBaseClusterProcess cluster,
      String layout, boolean physical) throws Exception {
    var businessScans = new java.util.concurrent.atomic.AtomicInteger();
    var backend = cluster.clientConfiguration();
    var plan = M3TableGroupRuntimeIT.plan(layout,
        backend.getInt("hbase.zookeeper.property.clientPort", -1));
    try (var fixture = Fixture.open(backend, plan, true, "1", businessScans)) {
      fixture.put(KEY, "old");
      fixture.put(KEY, "new");
      var other = ByteString.copyFromUtf8("another-key");
      long revision = fixture.put(other, "retained");
      businessScans.set(0);
      assertEquals(revision, fixture.client().compact(Rpc.CompactionRequest.newBuilder()
          .setRevision(0).setPhysical(physical).build()).getHeader().getRevision());
      fixture.awaitCompletion(0, revision - 1, true);
      assertEquals(0, businessScans.get(), "zero revision has no business history to enumerate");
      assertEquals(ByteString.copyFromUtf8("new"), fixture.client().range(
          Rpc.RangeRequest.newBuilder().setKey(KEY).build()).getKvs(0).getValue());
      assertEquals(ByteString.copyFromUtf8("retained"), fixture.client().range(
          Rpc.RangeRequest.newBuilder().setKey(other).build()).getKvs(0).getValue());
      System.out.printf("j52-zero-rpc layout=%s physical=%s durable=true businessScans=0%n",
          plan.primary().layout(), physical);
      fixture.client().compact(Rpc.CompactionRequest.newBuilder()
          .setRevision(revision).setPhysical(true).build());
      assertTrue(businessScans.get() > 0, "the observer must see scans for a positive target");
    }
  }

  @Test
  void physicalRpcWaitsForAnOldPinThenConfirmsEveryEligibleVersionInBothLayouts() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.startSecure()) {
      for (String layout : List.of("history-table.json", "native-cell-versions.json")) {
        try (var fixture = Fixture.open(cluster, layout)) { verifyPinnedRpc(fixture); }
      }
    }
  }

  private static void verifyPinnedRpc(Fixture fixture) throws Exception {
    try (var readerScope = RequestScope.withTimeout(Duration.ofMinutes(2))) {
      assertEquals(2, fixture.put(KEY, "version-2"));
      var oldRead = fixture.views.open(OptionalLong.of(2), ByteKey.copyOf(new byte[32]), readerScope)
          .get(30, TimeUnit.SECONDS);
      try {
        var history = populateHistory(fixture);
        var waiting = KVGrpc.newFutureStub(fixture.channel).withDeadlineAfter(60, TimeUnit.SECONDS)
            .compact(Rpc.CompactionRequest.newBuilder().setRevision(history.target()).setPhysical(true).build());
        fixture.awaitLowerCompletion(2);
        assertFalse(waiting.isDone(), "a completed sweep at pin revision 2 cannot complete the higher target");
        assertEquals(history.target() + 1, fixture.put(ByteString.copyFromUtf8("concurrent-key"), "still-writable"),
            "physical waiting must release the owner mutation lane");
        assertCompacted(fixture, history.target());
        assertFalse(waiting.isDone(), "duplicate ErrCompacted must not discard the original durable task");
        release(oldRead);
        assertEquals(history.target(), waiting.get(60, TimeUnit.SECONDS).getHeader().getRevision());
        assertRetainedHistory(fixture, history);
        System.out.printf("j52-physical-rpc layout=%s oldPinBlocked=true ownerLaneFree=true "
            + "duplicateCompacted=true confirmedDeletes=72 realAdmin=true%n", fixture.plan.primary().layout());
      } finally { release(oldRead); }
    }
  }

  private record HistoryTarget(long target, List<ByteString> keys, long baselineA, long baselineB) {}

  private static HistoryTarget populateHistory(Fixture fixture) {
    var inverse = invertedBucketKeys(fixture.plan);
    fixture.put(inverse.getFirst(), "old-a");
    long baselineA = fixture.put(inverse.getFirst(), "new-a");
    fixture.put(inverse.getLast(), "old-b");
    long baselineB = fixture.put(inverse.getLast(), "new-b");
    return new HistoryTarget(writeRemainingVersions(fixture), inverse, baselineA, baselineB);
  }

  private static long writeRemainingVersions(Fixture fixture) {
    long target = 2;
    for (int version = 3; version <= 72; version++) target = fixture.put(KEY, "version-" + version);
    return target;
  }

  private static void assertRetainedHistory(Fixture fixture, HistoryTarget history) throws Exception {
    fixture.assertExactRetainedBaseline(KEY, history.target(), 71);
    fixture.assertExactRetainedBaseline(history.keys().getFirst(), history.baselineA(), 2);
    fixture.assertExactRetainedBaseline(history.keys().getLast(), history.baselineB(), 2);
    assertEquals(ByteString.copyFromUtf8("version-72"), fixture.client().range(
        Rpc.RangeRequest.newBuilder().setKey(KEY).build()).getKvs(0).getValue());
    assertEquals(ByteString.copyFromUtf8("still-writable"), fixture.client().range(
        Rpc.RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("concurrent-key")).build())
        .getKvs(0).getValue());
  }

  @Test
  void cancelledPhysicalRpcResumesAfterOwnerProcessDeathAndLogicalOnlyRegistration() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.startSecure()) {
      for (String layout : List.of("history-table.json", "native-cell-versions.json")) {
        try (var fixture = Fixture.open(cluster, layout)) { verifyProcessRestart(fixture, cluster, layout); }
      }
    }
  }

  private void verifyProcessRestart(Fixture fixture, TemporaryHBaseClusterProcess cluster,
      String layout) throws Exception {
    try (var readerScope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      fixture.put(KEY, "version-2");
      var oldRead = fixture.views.open(OptionalLong.of(2), ByteKey.copyOf(new byte[32]), readerScope)
          .get(30, TimeUnit.SECONDS);
      try {
        long target = writeRemainingVersions(fixture);
        fixture.stopRuntime();
        M3TableGroupRuntimeIT.waitForLeaseExpiry(fixture.plan, fixture.provider, fixture.identity);
        var run = Files.createDirectory(artifacts.resolve(layout));
        var configuration = run.resolve("hbase-site.xml");
        try (var output = Files.newOutputStream(configuration)) { cluster.clientConfiguration().writeXml(output); }
        var selected = selectCancelAndKill(fixture, cluster.clientConfiguration(), layout, configuration, run, target);
        M3TableGroupRuntimeIT.waitForLeaseExpiry(fixture.plan, fixture.provider, fixture.identity);
        try (var second = Child.start(fixture, cluster.clientConfiguration(), layout, configuration, run, "3")) {
          fixture.connect(second.port);
          assertCompacted(fixture, target);
          assertEquals(selected.operation(), fixture.selectedOperation());
          release(oldRead);
          fixture.awaitFullCompletion(target);
          assertTrue(fixture.keyCheckpoint().ownerEpoch() > selected.ownerEpoch());
          fixture.assertExactRetainedBaseline(KEY, target, 71);
          verifyLogicalOnlyResume(fixture);
          System.out.printf("j52-physical-restart layout=%s cancelled=true processKilled=true "
              + "newOwner=true originalOperation=true logicalOnlyResumed=true%n", fixture.plan.primary().layout());
        }
      } finally { release(oldRead); }
    }
  }

  private record SelectedTask(io.etcdhbase.core.commit.InvocationId operation, long ownerEpoch) {}

  private static SelectedTask selectCancelAndKill(Fixture fixture, Configuration backend, String layout,
      Path configuration, Path run, long target) throws Exception {
    try (var first = Child.start(fixture, backend, layout, configuration, run, "2")) {
      fixture.connect(first.port);
      var waiting = KVGrpc.newFutureStub(fixture.channel).withDeadlineAfter(60, TimeUnit.SECONDS)
          .compact(Rpc.CompactionRequest.newBuilder().setRevision(target).setPhysical(true).build());
      fixture.awaitLowerCompletion(2);
      assertFalse(waiting.isDone());
      var selected = new SelectedTask(fixture.selectedOperation(), fixture.keyCheckpoint().ownerEpoch());
      assertTrue(waiting.cancel(true));
      first.kill();
      return selected;
    }
  }

  private static void verifyLogicalOnlyResume(Fixture fixture) throws Exception {
    long next = fixture.put(KEY, "logical-only");
    assertEquals(next, fixture.client().compact(Rpc.CompactionRequest.newBuilder()
        .setRevision(next).setPhysical(false).build()).getHeader().getRevision());
    fixture.awaitFullCompletion(next);
    fixture.assertExactRetainedBaseline(KEY, next, 72);
  }

  private static void assertCompacted(Fixture fixture, long revision) {
    var duplicate = assertThrows(StatusRuntimeException.class, () -> fixture.client().compact(
        Rpc.CompactionRequest.newBuilder().setRevision(revision).setPhysical(true).build()));
    assertEquals(Status.Code.OUT_OF_RANGE, duplicate.getStatus().getCode());
  }

  private static void release(io.etcdhbase.core.commit.PinnedReadView view) throws Exception {
    view.close();
    view.termination().toCompletableFuture().get(30, TimeUnit.SECONDS);
  }

  private static List<ByteString> invertedBucketKeys(M3TableGroupPlan plan) {
    var candidates = java.util.stream.IntStream.range(1, 256)
        .mapToObj(value -> ByteKey.copyOf(new byte[] {(byte) value})).toList();
    int lastBucket = plan.primary().codec().saltProfile().bucketCount() - 1;
    var first = candidates.stream().filter(key -> plan.primary().codec().bucket(key) == 0)
        .reduce((left, right) -> right).orElseThrow();
    var last = candidates.stream().filter(key -> plan.primary().codec().bucket(key) == lastBucket)
        .findFirst().orElseThrow();
    assertTrue(first.compareTo(last) > 0);
    return List.of(ByteString.copyFrom(first.bytes()), ByteString.copyFrom(last.bytes()));
  }

  private static final class Child implements AutoCloseable {
    private final Process process;
    private final Path log;
    private int port;
    private boolean killed;

    private Child(Process process, Path log) { this.process = process; this.log = log; }

    static Child start(Fixture fixture, Configuration backend, String layout, Path configuration,
        Path run, String member) throws Exception {
      Path ready = run.resolve("ready-" + member);
      Path log = run.resolve("runtime-" + member + ".log");
      Path hosts = run.resolve("hosts");
      Files.writeString(hosts, "127.0.0.1 localhost\n::1 localhost\n");
      var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
          "-Djdk.net.hosts.file=" + hosts.toAbsolutePath(),
          "-Djava.security.krb5.conf=" + backend.get("etcdhbase.test.security.krb5"),
          "-cp", System.getProperty("java.class.path"), J52PhysicalRuntimeProcess.class.getName(),
          configuration.toString(), layout,
          Integer.toString(backend.getInt("hbase.zookeeper.property.clientPort", -1)),
          fixture.plan.primary().table().getNamespaceAsString(), member, ready.toString())
          .redirectErrorStream(true).redirectOutput(log.toFile()).start();
      var child = new Child(process, log);
      try {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!Files.isRegularFile(ready)) {
          assertTrue(process.isAlive(), "runtime process exited before readiness; log: " + log);
          assertTrue(System.nanoTime() < deadline, "runtime readiness timed out; log: " + log);
          Thread.sleep(20);
        }
        child.port = Integer.parseInt(Files.readString(ready));
        return child;
      } catch (Exception | Error failure) {
        process.destroyForcibly();
        if (!process.waitFor(10, TimeUnit.SECONDS)) failure.addSuppressed(new IllegalStateException("runtime did not stop"));
        throw failure;
      }
    }

    void kill() throws Exception {
      assertTrue(process.isAlive());
      killed = true;
      process.destroyForcibly();
      assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      assertNotEquals(0, process.exitValue());
    }

    @Override public void close() throws Exception {
      try {
        if (process.isAlive()) {
          process.getOutputStream().write('\n');
          process.getOutputStream().flush();
          process.getOutputStream().close();
          assertTrue(process.waitFor(40, TimeUnit.SECONDS), "runtime did not drain; log: " + log);
        }
        if (!killed) assertEquals(0, process.exitValue(), "runtime failed; log: " + log);
      } finally {
        if (process.isAlive()) {
          process.destroyForcibly();
          assertTrue(process.waitFor(10, TimeUnit.SECONDS));
        }
      }
    }
  }

  static final class Fixture implements AutoCloseable {
    private final M3TableGroupPlan plan;
    private final HBaseBackendIdentityProvider provider;
    private final BudgetManager budgets;
    private final ScanScheduler scans;
    private io.etcdhbase.hbase.api.HBaseConnectionFactory runtimeConnections;
    private AuthorizedBackendIdentity identity;
    private AsyncConnection connection;
    private HistoryGcCheckpointStore checkpoints;
    private ReadViewService views;
    private HBaseCommitControlStore control;
    private M3TableGroupRuntime runtime;
    private Bootstrap server;
    private ManagedChannel channel;

    private Fixture(M3TableGroupPlan plan, HBaseBackendIdentityProvider provider) {
      this.plan = plan;
      this.provider = provider;
      runtimeConnections = provider;
      var limits = new BudgetManager.Limits(16, 16, 32, 16, 32, 8L << 20, 512L << 20);
      budgets = new BudgetManager(limits, Map.of(plan.source().tableGroupId(), limits), limits);
      scans = new ScanScheduler(budgets, Duration.ofSeconds(30));
    }

    static Fixture open(TemporaryHBaseClusterProcess cluster, String layout) throws Exception {
      var configuration = cluster.clientConfiguration();
      var plan = M3TableGroupRuntimeIT.plan(layout,
          configuration.getInt("hbase.zookeeper.property.clientPort", -1));
      return open(configuration, plan, true, "1");
    }

    static Fixture openExisting(Configuration configuration, String layout, int port,
        String namespace, String member) throws Exception {
      return open(configuration, M3TableGroupRuntimeIT.plan(layout, port, namespace), false, member);
    }

    private static Fixture open(Configuration configuration, M3TableGroupPlan plan,
        boolean fresh, String member) throws Exception {
      return open(configuration, plan, fresh, member, null);
    }

    private static Fixture open(Configuration configuration, M3TableGroupPlan plan,
        boolean fresh, String member, java.util.concurrent.atomic.AtomicInteger businessScans) throws Exception {
      var authentication = new AuthenticationScope(plan.source().tableGroupId(), plan.source().hbaseProfile().id());
      var provider = new HBaseBackendIdentityProvider(plan.source().identityProviderId(), authentication,
          configuration, HBaseAuthenticators.kerberosPassword(),
          (backend, ignored) -> backend.getAdmin().listNamespaces().thenApply(names -> null),
          1, 1, Duration.ofSeconds(30));
      var fixture = new Fixture(plan, provider);
      try {
        String principal = "admin@" + configuration.get("etcdhbase.test.security.realm");
        var keytab = Path.of(configuration.get("etcdhbase.test.security.admin.keytab"));
        fixture.identity = provider.activateTrusted(() -> KerberosUserContext.fromKeytab(principal, keytab))
            .toCompletableFuture().get(30, TimeUnit.SECONDS);
        if (fresh) fixture.initialize();
        else fixture.bind();
        if (businessScans != null) fixture.observeBusinessScans(businessScans);
        fixture.start(member);
        return fixture;
      } catch (Exception | Error failure) {
        try { fixture.close(); } catch (Exception | Error cleanup) { failure.addSuppressed(cleanup); }
        throw failure;
      }
    }

    /** Transparent counters delegate every operation, including real ADMIN checks, to HBase. */
    private void observeBusinessScans(java.util.concurrent.atomic.AtomicInteger businessScans) {
      runtimeConnections = new io.etcdhbase.hbase.api.HBaseConnectionFactory() {
        @Override public java.util.concurrent.CompletionStage<AsyncConnection> connectionFor(
            AuthorizedBackendIdentity requested) {
          return provider.connectionFor(requested).thenApply(actual -> {
            return org.mockito.Mockito.mock(AsyncConnection.class, invocation -> {
              Object result = org.mockito.AdditionalAnswers.delegatesTo(actual).answer(invocation);
              String method = invocation.getMethod().getName();
              if ((method.equals("getTableBuilder") || method.equals("getTable"))
                  && plan.primary().table().equals(invocation.getArgument(0))) {
                return method.equals("getTableBuilder") ? observedBuilder(result, businessScans)
                    : observedTable(result, businessScans);
              }
              return result;
            });
          });
        }
        @Override public void close() { /* The real provider retains connection ownership. */ }
      };
    }

    private static Object observedBuilder(Object actual, java.util.concurrent.atomic.AtomicInteger scans) {
      return org.mockito.Mockito.mock(org.apache.hadoop.hbase.client.AsyncTableBuilder.class, invocation -> {
        Object result = org.mockito.AdditionalAnswers.delegatesTo(actual).answer(invocation);
        if (invocation.getMethod().getName().equals("build")) return observedTable(result, scans);
        return result == actual ? invocation.getMock() : result;
      });
    }

    private static Object observedTable(Object actual, java.util.concurrent.atomic.AtomicInteger scans) {
      return org.mockito.Mockito.mock(org.apache.hadoop.hbase.client.AsyncTable.class, invocation -> {
        if (invocation.getMethod().getName().startsWith("scan")) scans.incrementAndGet();
        return org.mockito.AdditionalAnswers.delegatesTo(actual).answer(invocation);
      });
    }

    private void initialize() throws Exception {
      M3TableGroupInitializer.initializeFresh(plan, "sha256:" + plan.source().fingerprint(), identity, provider, PINS);
      connection = provider.connectionFor(identity).toCompletableFuture().get(30, TimeUnit.SECONDS);
      var admin = connection.getAdmin();
      var system = admin.getDescriptor(plan.system().table()).get(30, TimeUnit.SECONDS);
      admin.modifyTable(plan.system().withCompactionCapability(system)).get(30, TimeUnit.SECONDS);
      if (plan.primary().layout() == StorageTableBinding.Layout.NATIVE) {
        var descriptor = admin.getDescriptor(plan.primary().table()).get(30, TimeUnit.SECONDS);
        var family = ColumnFamilyDescriptorBuilder.newBuilder(descriptor.getColumnFamily(plan.primary().family()))
            .setMaxVersions(128).build();
        admin.modifyColumnFamily(plan.primary().table(), family)
            .get(30, TimeUnit.SECONDS);
      }
      bind();
    }

    private void bind() throws Exception {
      connection = provider.connectionFor(identity).toCompletableFuture().get(30, TimeUnit.SECONDS);
      try (var scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
        var records = HBaseSystemRecordStore.open(provider, identity, plan.system(), scope).get(30, TimeUnit.SECONDS);
        control = HBaseCommitControlStore.open(provider, identity, plan.system(), scope).get(30, TimeUnit.SECONDS);
        var pins = SnapshotPinManager.open(records, PINS, scope).get(30, TimeUnit.SECONDS);
        checkpoints = new HistoryGcCheckpointStore(records, plan.source().tableGroupId(),
            plan.system().profileFingerprint(), pins.registryIdentity(), plan.primary().codec().saltProfile().bucketCount(),
            plan.primary().codec()::bucket);
        views = new ReadViewService(control, new ManifestCatalog(new HBaseManifestRepository(records)), pins,
            plan.system().profileFingerprint());
      }
    }

    private void start(String member) throws Exception {
      try (var startup = RequestScope.withTimeout(Duration.ofSeconds(30))) {
        runtime = M3TableGroupRuntime.open(plan, M3TableGroupRuntimeIT.activation(member), RESOURCES,
            runtimeConnections, identity, budgets, scans, startup);
      } catch (M3TableGroupRuntime.StartupException failure) {
        try { failure.cleanup().toCompletableFuture().get(30, TimeUnit.SECONDS); }
        catch (Exception cleanup) { failure.addSuppressed(cleanup); }
        throw failure;
      }
      server = Bootstrap.startConfigured(new RuntimeConfiguration.Listener("j52-physical", "127.0.0.1", 0,
          plan.source().tableGroupId()), RESOURCES, runtime.kvService());
      connect(server.boundListener().port());
    }

    int port() { return server.boundListener().port(); }

    private void connect(int port) throws Exception {
      if (channel != null) {
        channel.shutdownNow();
        assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
      }
      channel = NettyChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build();
    }

    private io.etcdhbase.core.commit.InvocationId selectedOperation() throws Exception {
      try (var scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
        return control.read(scope).get(30, TimeUnit.SECONDS).orElseThrow().state().compactOperationId().orElseThrow();
      }
    }

    private io.etcdhbase.core.commit.HistoryGcCheckpoint keyCheckpoint() throws Exception {
      try (var scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
        return checkpoints.read(plan.primary().codec().bucket(ByteKey.copyOf(KEY.toByteArray())), scope)
            .get(30, TimeUnit.SECONDS).orElseThrow();
      }
    }

    private KVGrpc.KVBlockingStub client() {
      return KVGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
    }

    private long put(ByteString key, String value) {
      return client().put(Rpc.PutRequest.newBuilder().setKey(key).setValue(ByteString.copyFromUtf8(value)).build())
          .getHeader().getRevision();
    }

    private void awaitLowerCompletion(long revision) throws Exception {
      awaitCompletion(revision, 0, true);
    }

    private void awaitFullCompletion(long revision) throws Exception {
      awaitCompletion(revision, revision - 1, false);
    }

    private void awaitCompletion(long revision, long sequence, boolean exact) throws Exception {
      try (var scope = RequestScope.withTimeout(Duration.ofSeconds(20))) {
        int buckets = plan.primary().codec().saltProfile().bucketCount();
        for (int bucket = 0; bucket < buckets; bucket++) {
          while (true) {
            var checkpoint = checkpoints.read(bucket, scope).get(20, TimeUnit.SECONDS);
            if (checkpoint.filter(value -> value.completedGeneration() > 0
                && (exact ? value.completedRevision() == revision : value.completedRevision() >= revision)
                && value.completedSequence() >= sequence).isPresent()) break;
            assertFalse(scope.isDeadlineExceeded(), "background did not finish the conservative pin-bound sweep");
            Thread.sleep(20);
          }
        }
      }
    }

    private void assertExactRetainedBaseline(ByteString businessKey, long revision, int historyRows) throws Exception {
      var binding = plan.primary();
      var key = ByteKey.copyOf(businessKey.toByteArray());
      var table = connection.getTable(binding.table());
      if (binding.layout() == StorageTableBinding.Layout.NATIVE) {
        var row = table.get(new Get(binding.codec().encode(key, 1)).addFamily(binding.family()).readVersions(128))
            .get(30, TimeUnit.SECONDS);
        assertEquals(3, row.size(), "all obsolete m/v/f timestamps must be absent");
        for (var cell : row.rawCells()) assertEquals(RevisionTimestamp.timestamp(revision), cell.getTimestamp());
      } else {
        var range = binding.codec().physicalRange(binding.codec().bucket(key), KeyRange.point(key));
        var scan = new Scan().withStartRow(range.lowerInclusive().orElseThrow().bytes(), true)
            .withStopRow(range.upperExclusive().orElseThrow().bytes(), false).addFamily(binding.family());
        var rows = table.scanAll(scan).get(30, TimeUnit.SECONDS);
        assertEquals(historyRows, rows.size(), "all obsolete guards must remain while their m/v cells are absent");
        for (var row : rows) {
          long version = binding.codec().decode(row.getRow()).revision().orElseThrow();
          if (version == revision) {
            assertNotNull(row.getValue(binding.family(), new byte[] {'m'}));
            assertNotNull(row.getValue(binding.family(), new byte[] {'v'}));
          } else {
            assertTrue(version < revision);
            assertEquals(RowFenceMessages.State.RETIRED,
                RowFenceMessages.decode(row.getValue(binding.family(), new byte[] {'g'})).state());
            assertNull(row.getValue(binding.family(), new byte[] {'m'}));
            assertNull(row.getValue(binding.family(), new byte[] {'v'}));
          }
        }
      }
    }

    private void stopRuntime() throws Exception {
      Exception failure = null;
      if (channel != null) {
        channel.shutdownNow();
        if (!channel.awaitTermination(10, TimeUnit.SECONDS)) failure = new IllegalStateException("RPC channel did not stop");
      }
      for (AutoCloseable resource : new AutoCloseable[] {server, runtime == null ? null : () -> {
        runtime.close();
        runtime.drained().get(30, TimeUnit.SECONDS);
      }}) {
        if (resource == null) continue;
        try { resource.close(); }
        catch (Exception problem) { if (failure == null) failure = problem; else failure.addSuppressed(problem); }
      }
      if (failure != null) throw failure;
      channel = null;
      server = null;
      runtime = null;
      assertEquals(0, budgets.snapshot().requests());
    }

    @Override public void close() throws Exception {
      Exception failure = null;
      try { stopRuntime(); } catch (Exception problem) { failure = problem; }
      for (AutoCloseable resource : new AutoCloseable[] {scans, provider}) {
        try { resource.close(); }
        catch (Exception problem) { if (failure == null) failure = problem; else failure.addSuppressed(problem); }
      }
      if (failure != null) throw failure;
    }
  }
}
