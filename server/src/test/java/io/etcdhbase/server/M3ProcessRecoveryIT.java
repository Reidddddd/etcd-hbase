package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.RecoveryManager;
import io.etcdhbase.core.commit.ReadViewRecordStore;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.commit.TxnReadSession;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.kv.CompareReadExecutor;
import io.etcdhbase.core.kv.ResultNode;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.kv.TxnEvaluator;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.DirectRecordReader;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.hbase.codec.ResultMessages;
import io.etcdhbase.hbase.storage.M3TestEnvironment;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.server.commit.PutCommitPreparation;
import io.etcdhbase.server.commit.TableGroupStores;
import io.etcdhbase.server.commit.TxnCommitPreparation;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/** Process death after the real control CAS, with HBase kept alive for a fresh recovery owner. */
class M3ProcessRecoveryIT {
  private static final Duration TIMEOUT = Duration.ofMinutes(2);
  private static final SnapshotPinConfiguration PIN_CONFIGURATION =
      new SnapshotPinConfiguration(2, 16, Duration.ofSeconds(60));

  @TempDir(cleanup = CleanupMode.ON_SUCCESS) Path temporaryDirectory;

  @Test
  void selectedPutIsRecoveredAfterPublisherProcessExits() throws Exception {
    for (StorageTableBinding.Layout layout : List.of(
        StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (M3TestEnvironment fixture = M3TestEnvironment.start(layout)) {
        var priorValue = new byte[] {1, 0};
        try (var engine = new M3PutCommitIT.Engine(fixture, Duration.ofSeconds(15))) {
          assertEquals(2, engine.put(priorValue, true).result().revision());
        }
        assertRevokedReadCannotSeal(fixture);
        Prepared prepared = prepareNextPut(fixture);
        waitForOldOwnerExpiry(fixture);
        runPublisherUntilCrash(fixture, prepared.id(), layout);
        assertPendingPhysicalState(fixture, priorValue);
        waitForOldOwnerExpiry(fixture);
        recoverAndVerify(fixture, prepared, priorValue);
      }
    }
  }

  @Test
  void selectedDeleteIsRecoveredAfterPublisherProcessExits() throws Exception {
    for (StorageTableBinding.Layout layout : List.of(
        StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (M3TestEnvironment fixture = M3TestEnvironment.start(layout)) {
        var priorValue = new byte[] {1, 0};
        try (var engine = new M3PutCommitIT.Engine(fixture, Duration.ofSeconds(15))) {
          assertEquals(2, engine.put(priorValue, true).result().revision());
        }
        Prepared prepared = prepareNextDelete(fixture);
        waitForOldOwnerExpiry(fixture);
        runPublisherUntilCrash(fixture, prepared.id(), layout);
        assertPendingPhysicalState(fixture, priorValue);
        waitForOldOwnerExpiry(fixture);
        recoverDeleteAndVerify(fixture, prepared, priorValue);
      }
    }
  }

  private static Prepared prepareNextDelete(M3TestEnvironment fixture) throws Exception {
    var budgets = budgets(fixture);
    RequestScope scope = RequestScope.withTimeout(TIMEOUT);
    try (var scans = new ScanScheduler(budgets, Duration.ofSeconds(30))) {
      var stores = openStores(fixture, scope);
      var state = stores.control().read(scope).join().orElseThrow().state();
      var request = Rpc.DeleteRangeRequest.newBuilder().setKey(ByteString.copyFrom(M3PutCommitIT.KEY.bytes()))
          .setPrevKv(true).build();
      InvocationId id = InvocationId.create();
      ByteKey digest = MutationForwardRequest.digest(fixture.system().tableGroupId(),
          MutationOperationEnvelope.deleteRange(request.toByteArray()));
      var invocations = new InvocationService(stores.invocations());
      var binding = invocations.bindOrJoin(InvocationBinding.begin(id, digest, M3PutCommitIT.AUTHORIZATION,
          TransactionId.create(), Math.addExact(state.ownerEpoch(), 1), state.publishedSequence(),
          state.publishedRevision()), scope).join();
      var account = budgets.admit(fixture.system().tableGroupId(), scope, reservation());
      int buckets = fixture.salt().profile().bucketCount();
      var session = TxnReadSession.open(stores.views(), List.of(), M3PutCommitIT.AUTHORIZATION,
          fixture.openPrimary(scope), stores.manifests(), fixture.primary().codec()::bucket, buckets, scope).join();
      var comparisons = new CompareReadExecutor(session, scans, account, scope,
          new CompareReadExecutor.Limits(buckets, 4096, 64, 5L << 20));
      var evaluator = new TxnEvaluator(session, comparisons, account, scope,
          new TxnEvaluator.Limits(5L << 20, 1L << 20, 8L << 20), fixture.primary().codec()::bucket,
          stores.records(), stores.manifestRepository());
      var command = new TxnCommand(List.of(), List.of(
          new TxnCommand.Delete(KeyRange.point(M3PutCommitIT.KEY), true)), List.of());
      var evaluated = evaluator.evaluate(command, Optional.of(binding)).join();
      var preparation = new TxnCommitPreparation(new ResultPreparation(stores.resultRepository()), stores.results(),
          new ManifestPreparation(stores.manifestRepository(),
              new ManifestPreparation.Limits(1, 4L << 20, 1, 8L << 20)), invocations, 8L << 20);
      var sealed = assertInstanceOf(TxnCommitPreparation.Sealed.class,
          preparation.prepare(evaluated, binding, scope, account).join());
      assertEquals(InvocationBinding.State.SEALED, sealed.invocation().state());
      return new Prepared(id, digest);
    } finally {
      closeAndDrain(scope, budgets);
    }
  }

  private static void recoverDeleteAndVerify(M3TestEnvironment fixture, Prepared prepared, byte[] priorValue)
      throws Exception {
    var budgets = budgets(fixture);
    RequestScope scope = RequestScope.withTimeout(TIMEOUT);
    try {
      var stores = openStores(fixture, scope);
      var recovery = new RecoveryManager(fixture.system().profileFingerprint(), stores.control(),
          new CommitOwnerService(stores.control()), stores.invocations(), stores.outcomes(), stores.finalizer());
      long now = System.currentTimeMillis();
      var recovered = recovery.recover("m3-delete-recovery", now, now + 60_000, scope).join();
      assertEquals(RecoveryManager.State.FINALIZED, recovered.state());
      var account = budgets.admit(fixture.system().tableGroupId(), scope, reservation());
      try (var result = stores.invocationResults().resolveTransaction(prepared.id(), prepared.digest(),
          M3PutCommitIT.AUTHORIZATION, scope, account).join()) {
        var root = assertInstanceOf(ResultNode.Txn.class, ResultMessages.decodeTree(result.payload().buffer()).root());
        var deleted = assertInstanceOf(ResultNode.Delete.class, root.responses().getFirst());
        assertEquals(3, root.header().revision());
        assertEquals(1, deleted.deleted());
        assertArrayEquals(priorValue, deleted.previous().getFirst().value().orElseThrow());
      }
      var physical = fixture.openPrimary(scope);
      var current = physical.get(M3PutCommitIT.KEY, 3, ReadProjection.FULL).join().orElseThrow();
      assertEquals(StorageMetadata.State.TOMBSTONE, current.metadata().state());
      assertArrayEquals(priorValue, physical.get(M3PutCommitIT.KEY, 2, ReadProjection.FULL)
          .join().orElseThrow().value().orElseThrow());
      var state = stores.control().read(scope).join().orElseThrow().state();
      assertFalse(state.publication().isPresent());
      assertEquals(3, state.publishedRevision());
      assertEquals(3, state.finalizedRevision());
      assertEquals(2, state.finalizedSequence());
    } finally {
      closeAndDrain(scope, budgets);
    }
  }

  private static Prepared prepareNextPut(M3TestEnvironment fixture) throws Exception {
    var budgets = budgets(fixture);
    RequestScope scope = RequestScope.withTimeout(TIMEOUT);
    try {
      var stores = openStores(fixture, scope);
      var state = stores.control().read(scope).join().orElseThrow().state();
      assertTrue(state.publication().isEmpty());
      var request = Rpc.PutRequest.newBuilder().setKey(ByteString.copyFrom(M3PutCommitIT.KEY.bytes()))
          .setValue(ByteString.copyFrom(new byte[] {9, (byte) 0xff})).setPrevKv(true).build();
      InvocationId id = InvocationId.create();
      ByteKey digest = MutationForwardRequest.digest(
          fixture.system().tableGroupId(), ByteKey.copyOf(request.toByteArray()));
      InvocationBinding binding = InvocationBinding.begin(id, digest, M3PutCommitIT.AUTHORIZATION,
          TransactionId.create(), Math.addExact(state.ownerEpoch(), 1), state.publishedSequence(),
          state.publishedRevision());
      var invocations = new InvocationService(stores.invocations());
      binding = invocations.bindOrJoin(binding, scope).join();
      var preparation = new PutCommitPreparation(new ResultPreparation(stores.resultRepository()),
          stores.results(), new ManifestPreparation(stores.manifestRepository(),
              new ManifestPreparation.Limits(1, 4L << 20, 1, 8L << 20)), invocations);
      var account = budgets.admit(fixture.system().tableGroupId(), scope, reservation());
      var command = new PutCommand(M3PutCommitIT.KEY, request.getValue().toByteArray(),
          0, true, false, false);
      var view = openReadView(fixture, stores, scope);
      try (var point = new DirectRecordReader(view, account, 5L << 20)
          .readForPreparation(M3PutCommitIT.KEY, state.publishedRevision(), ReadProjection.FULL).join()) {
        var sealed = preparation.prepare(command, binding, point.record(), scope, account,
            view::finishRead).join();
        assertEquals(InvocationBinding.State.SEALED, sealed.invocation().state());
      }
      return new Prepared(id, digest);
    } finally {
      closeAndDrain(scope, budgets);
    }
  }

  private static void assertRevokedReadCannotSeal(M3TestEnvironment fixture) throws Exception {
    var budgets = budgets(fixture);
    RequestScope scope = RequestScope.withTimeout(TIMEOUT);
    try {
      var stores = openStores(fixture, scope);
      var state = stores.control().read(scope).join().orElseThrow().state();
      var account = budgets.admit(fixture.system().tableGroupId(), scope, reservation());
      var view = openReadView(fixture, stores, scope);
      var id = InvocationId.create();
      var transaction = TransactionId.create();
      var binding = InvocationBinding.begin(id, ByteKey.copyOf(new byte[32]),
          M3PutCommitIT.AUTHORIZATION, transaction, Math.addExact(state.ownerEpoch(), 1),
          state.publishedSequence(), state.publishedRevision());
      var invocations = new InvocationService(stores.invocations());
      binding = invocations.bindOrJoin(binding, scope).join();
      var preparation = new PutCommitPreparation(new ResultPreparation(stores.resultRepository()),
          stores.results(), new ManifestPreparation(stores.manifestRepository(),
              new ManifestPreparation.Limits(1, 4L << 20, 1, 8L << 20)), invocations);
      var futureClockPins = SnapshotPinManager.open(stores.records(), PIN_CONFIGURATION,
          Clock.offset(Clock.systemUTC(), Duration.ofMinutes(2)), scope).join();
      var command = new PutCommand(M3PutCommitIT.KEY, new byte[] {8}, 0, true, false, false);
      try (var point = new DirectRecordReader(view, account, 5L << 20)
          .readForPreparation(M3PutCommitIT.KEY, state.publishedRevision(), ReadProjection.FULL).join()) {
        InvocationBinding attempt = binding;
        var failure = assertThrows(CompletionException.class, () -> preparation.prepare(command,
            attempt, point.record(), scope, account, () -> futureClockPins.revokeExpiredPins(scope)
                .thenCompose(ignored -> view.finishRead())).join());
        assertEquals(BackendFailure.Category.CANCELLED,
            assertInstanceOf(BackendFailure.class, failure.getCause()).category());
      }
      assertEquals(InvocationBinding.State.PREPARING,
          stores.invocations().read(id, scope).join().orElseThrow().state());
      var rootIdentity = ManifestRoot.create(transaction, binding.preparedOwnerEpoch(),
          binding.baseSequence(), binding.baseRevision(), 0, 0, Optional.empty()).systemIdentity();
      assertTrue(stores.manifests().readRoot(rootIdentity, scope).join().isEmpty());
      assertEquals(state.publishedRevision(),
          stores.control().read(scope).join().orElseThrow().state().publishedRevision());
    } finally {
      closeAndDrain(scope, budgets);
    }
  }

  private static ReadViewRecordStore openReadView(M3TestEnvironment fixture, TableGroupStores stores,
      RequestScope scope) throws Exception {
    var pinned = stores.views().open(OptionalLong.empty(), M3PutCommitIT.AUTHORIZATION, scope).join();
    return new ReadViewRecordStore(pinned, fixture.openPrimary(scope), stores.manifests(),
        fixture.primary().codec()::bucket, fixture.salt().profile().bucketCount(), scope);
  }

  private void runPublisherUntilCrash(M3TestEnvironment fixture, InvocationId id,
      StorageTableBinding.Layout layout) throws Exception {
    Path run = Files.createDirectory(temporaryDirectory.resolve(layout.name().toLowerCase()));
    Path configuration = run.resolve("hbase-site.xml");
    Path hosts = run.resolve("hosts");
    Path log = run.resolve("publisher.log");
    fixture.writeClientConfiguration(configuration);
    // The disposable cluster uses loopback; do not let ZooKeeper's hostname diagnostics depend on DNS.
    Files.writeString(hosts, "127.0.0.1 localhost\n::1 localhost\n");
    String classpath = System.getProperty("java.class.path");
    String current = fixture.current().map(value -> value.table().getNameAsString()).orElse("-");
    var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-Djdk.net.hosts.file=" + hosts.toAbsolutePath(), "-cp", classpath,
        M3CrashWriterMain.class.getName(), configuration.toString(),
        fixture.system().tableGroupId(), fixture.system().table().getNameAsString(),
        fixture.saltProfileId(), layout.name(), fixture.primary().table().getNameAsString(), current)
        .redirectErrorStream(true).redirectOutput(log.toFile());
    builder.environment().put("M3_FIXTURE_INVOCATION", id.value().toString());
    Process child = builder.start();
    try {
      assertTrue(child.pid() > 0 && child.pid() != ProcessHandle.current().pid());
      assertTrue(child.waitFor(90, TimeUnit.SECONDS), "publisher process did not terminate");
      assertEquals(77, child.exitValue(), "publisher did not halt at the selected publication; log: " + log);
      assertTrue(Files.readString(log).contains(
          "m3-crash-fixture: selected and stopping before materialization"));
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        assertTrue(child.waitFor(10, TimeUnit.SECONDS));
      }
    }
  }

  private static void assertPendingPhysicalState(M3TestEnvironment fixture, byte[] priorValue)
      throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(TIMEOUT)) {
      var state = fixture.control().read(scope).join().orElseThrow().state();
      assertTrue(state.publication().isPresent());
      assertEquals(3, state.publishedRevision());
      assertEquals(2, state.finalizedRevision());
      var physical = fixture.openPrimary(scope).get(M3PutCommitIT.KEY, 3, ReadProjection.FULL)
          .join().orElseThrow();
      assertArrayEquals(priorValue, physical.value().orElseThrow());
    }
  }

  private static void recoverAndVerify(M3TestEnvironment fixture, Prepared prepared, byte[] priorValue)
      throws Exception {
    var budgets = budgets(fixture);
    RequestScope scope = RequestScope.withTimeout(TIMEOUT);
    try {
      var stores = openStores(fixture, scope);
      var recovery = new RecoveryManager(fixture.system().profileFingerprint(), stores.control(),
          new CommitOwnerService(stores.control()), stores.invocations(), stores.outcomes(), stores.finalizer());
      var selected = stores.control().read(scope).join().orElseThrow().state();
      assertEquals(stores.invocations().read(prepared.id(), scope).join().orElseThrow().preparedOwnerEpoch(),
          selected.ownerEpoch());
      long now = System.currentTimeMillis();
      var recovered = recovery.recover("m3-recovery-owner", now, now + 60_000, scope).join();
      assertEquals(RecoveryManager.State.FINALIZED, recovered.state());
      assertEquals(Math.addExact(selected.ownerEpoch(), 1), recovered.owner().orElseThrow().epoch());
      var account = budgets.admit(fixture.system().tableGroupId(), scope, reservation());
      try (var result = stores.invocationResults().resolve(prepared.id(), prepared.digest(),
          M3PutCommitIT.AUTHORIZATION, scope, account).join()) {
        var put = ResultMessages.decodePut(result.payload().buffer());
        assertEquals(3, put.revision());
        assertArrayEquals(priorValue, put.previous().orElseThrow().value().orElseThrow());
        assertEquals(2, put.previous().orElseThrow().metadata().modificationRevision());
      }
      var physical = fixture.openPrimary(scope).get(M3PutCommitIT.KEY, 3, ReadProjection.FULL)
          .join().orElseThrow();
      assertArrayEquals(new byte[] {9, (byte) 0xff}, physical.value().orElseThrow());
      var state = stores.control().read(scope).join().orElseThrow().state();
      assertFalse(state.publication().isPresent());
      assertEquals(3, state.publishedRevision());
      assertEquals(3, state.finalizedRevision());
      assertEquals(2, state.finalizedSequence());
    } finally {
      closeAndDrain(scope, budgets);
    }
  }

  private static TableGroupStores openStores(M3TestEnvironment fixture, RequestScope scope) {
    return TableGroupStores.open(fixture.factory(), fixture.identity(), fixture.system(),
        fixture.primary(), fixture.current(), PIN_CONFIGURATION, scope).join();
  }

  private static void waitForOldOwnerExpiry(M3TestEnvironment fixture) throws Exception {
    long deadline;
    try (RequestScope scope = RequestScope.withTimeout(TIMEOUT)) {
      deadline = fixture.control().read(scope).join().orElseThrow().state().ownerLeaseDeadlineEpochMillis();
    }
    long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
    while (System.currentTimeMillis() <= deadline) {
      if (System.nanoTime() >= waitUntil) throw new IllegalStateException("fixture owner did not expire");
      Thread.sleep(Math.min(200, Math.max(1, deadline - System.currentTimeMillis() + 1)));
    }
  }

  private static BudgetManager budgets(M3TestEnvironment fixture) {
    var limits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1L << 20, 256L << 20);
    return new BudgetManager(limits, Map.of(fixture.system().tableGroupId(), limits), limits);
  }

  private static BudgetManager.Reservation reservation() {
    return new BudgetManager.Reservation(64L << 10, 64L << 10, 2L << 20, 16L << 20, 48L << 20);
  }

  private static void closeAndDrain(RequestScope scope, BudgetManager budgets) throws Exception {
    scope.close();
    scope.cleanupCompletion().toCompletableFuture().get(30, TimeUnit.SECONDS);
    scope.physicalProducersCompletion().toCompletableFuture().get(30, TimeUnit.SECONDS);
    assertEquals(0, budgets.snapshot().allocatedBytes());
    assertEquals(0, budgets.snapshot().reservedBytes());
  }

  private record Prepared(InvocationId id, ByteKey digest) {}
}
