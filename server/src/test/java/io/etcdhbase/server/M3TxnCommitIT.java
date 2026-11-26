package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import etcdserverpb.KVGrpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitFinalizationService;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPlanVerifier;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.MaterializationStore;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.ReadViewPinRegistry;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.commit.TxnReadSession;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.CompareCondition;
import io.etcdhbase.core.kv.CompareReadExecutor;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.kv.RangeCommand;
import io.etcdhbase.core.kv.ResultNode;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.kv.TxnEvaluator;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.DirectRecordReader;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.grpc.protocol.TxnResponseEmitter;
import io.etcdhbase.grpc.service.KvService;
import io.etcdhbase.hbase.codec.ResultMessages;
import io.etcdhbase.hbase.storage.M3TestEnvironment;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.server.commit.BoundedPublicationHandoff;
import io.etcdhbase.server.commit.MutationForwardReceiver;
import io.etcdhbase.server.commit.MutationOwnerLane;
import io.etcdhbase.server.commit.MutationRelay;
import io.etcdhbase.server.commit.SerialMutationQueue;
import io.etcdhbase.server.commit.TableGroupStores;
import io.etcdhbase.server.commit.TxnCommitPreparation;
import io.etcdhbase.server.commit.TxnExecutionLimits;
import io.etcdhbase.server.commit.TxnMutationExecutor;
import io.etcdhbase.server.commit.CommitAuthorizationProofTestSupport;
import io.etcdhbase.server.commit.TxnOwnerProcessor;
import io.etcdhbase.server.commit.TxnReadExecutor;
import io.etcdhbase.server.commit.TxnGateway;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.observability.RequestMetrics;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Real HBase evidence for flat transaction publication and the durable no-change result. */
class M3TxnCommitIT {
  private static final Duration TIMEOUT = Duration.ofMinutes(3);
  private static final ByteKey AUTHORIZATION = digest(9);

  @Test
  void failedFinalReadPinReturnsAnErrorWithoutAResult() throws Exception {
    try (M3TestEnvironment environment = M3TestEnvironment.start(StorageTableBinding.Layout.HISTORY, 1);
         Engine engine = new Engine(environment)) {
      engine.run(new TxnCommand(List.of(), List.of(put("pin-failure", "value")), List.of()), 70);
      engine.assertFailedReadPinReturnsError();
    }
  }

  @Test
  void nestedWorksetExceedsMemoryAndDoesNotReplayAnUncertainPageOnBothLayouts() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (M3TestEnvironment environment = M3TestEnvironment.start(layout, 1);
           Engine engine = new Engine(environment)) {
        byte[] value = new byte[1024];
        var operations = new ArrayList<TxnCommand.Operation>();
        for (int index = 0; index < 80; index++) {
          operations.add(new TxnCommand.Put(new PutCommand(key(String.format("bulk-%03d", index)),
              value, 0, false, false, false)));
        }
        operations.add(new TxnCommand.Nested(new TxnCommand(List.of(), List.of(
            pointRange(key("bulk-000")), pointRange(key("bulk-079"))), List.of())));
        var command = new TxnCommand(List.of(), operations, List.of());
        Receipt result = engine.run(command, 40, 64 * 1024);
        assertTrue(result.changed());
        assertEquals(2, result.revision());
        engine.assertResolvedResponse(result, command, true);
        assertTrue((long) value.length * 80 > 64 * 1024);
        Receipt later = engine.run(new TxnCommand(List.of(), List.of(put("later", "value")), List.of()), 42);
        assertEquals(3, later.revision());
        engine.assertResolvedResponse(result, command, true);
        engine.assertUncertainWorksetCannotReplay();
        assertEquals(4, engine.currentRevision());
        assertEquals(3, engine.currentSequence());
      }
    }
  }

  @Test
  void largeDeleteIsAtomicBudgetedAndReplayableOnBothLayouts() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (M3TestEnvironment environment = M3TestEnvironment.start(layout, 1);
           Engine engine = new Engine(environment)) {
        byte[] value = new byte[1024];
        var writes = new ArrayList<TxnCommand.Operation>();
        for (int index = 0; index < 80; index++) {
          writes.add(new TxnCommand.Put(new PutCommand(key(String.format("delete-%03d", index)),
              value, 0, false, false, false)));
        }
        Receipt seed = engine.run(new TxnCommand(List.of(), writes, List.of()), 50, 64 * 1024);
        assertEquals(2, seed.revision());
        var range = KeyRange.between(key("delete-"), key("delete."));
        var delete = new TxnCommand(List.of(), List.of(new TxnCommand.Delete(range, true)), List.of());
        engine.assertDeleteOutputBudgetRejectsBeforePublication(delete);
        assertEquals(2, engine.currentRevision());
        assertEquals(1, engine.currentSequence());
        Receipt removed = engine.run(delete, 52, 64 * 1024);
        assertTrue(removed.changed());
        assertEquals(3, removed.revision());
        engine.assertDeleteResult(removed, 80, 80);
        engine.assertHistoricalAndDeletedRecord("delete-000", 2, 3, value);
        Receipt empty = engine.run(delete, 53, 64 * 1024);
        assertFalse(empty.changed());
        assertEquals(3, empty.revision());
        engine.assertDeleteResult(empty, 0, 0);
        assertEquals(2, engine.currentSequence());
        Receipt recreated = engine.run(new TxnCommand(List.of(), List.of(put("delete-000", "new")), List.of()), 54);
        assertEquals(4, recreated.revision());
        engine.assertDeleteResult(removed, 80, 80);
        engine.assertValue("delete-000", 4, "new");
        Receipt reseeded = engine.run(new TxnCommand(List.of(), writes, List.of()), 55, 64 * 1024);
        assertEquals(5, reseeded.revision());
        var withoutPrevious = new TxnCommand(List.of(), List.of(new TxnCommand.Delete(range, false)), List.of());
        Receipt removedWithoutPrevious = engine.run(withoutPrevious, 56, 64 * 1024);
        assertEquals(6, removedWithoutPrevious.revision());
        engine.assertDeleteResult(removedWithoutPrevious, 80, 0);
      }
    }
  }

  @Test
  void partialDeleteMaterializationRecoversWithoutPartialReadsOnBothLayouts() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (M3TestEnvironment environment = M3TestEnvironment.start(layout, 1);
           Engine engine = new Engine(environment)) {
        Receipt seed = engine.run(new TxnCommand(List.of(), List.of(put("a", "one"), put("b", "two")), List.of()), 60);
        assertEquals(2, seed.revision());
        engine.assertPartialDeleteMaterializationRecovers();
      }
    }
  }

  @Test
  void publishesMultipleKeysOnceAndReplaysAnEarlierNoChangeResultOnBothLayouts() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (M3TestEnvironment environment = M3TestEnvironment.start(layout, 1);
           Engine engine = new Engine(environment)) {
        assertPublishedAndReplayed(engine);
        assertOwnerTransactions(engine);
        engine.assertFinalizationCapacityIsReservedBeforePreparing();
        engine.assertTaggedOwnerRequest();
        engine.assertValue("tagged", 5, "value");
        engine.assertReadOnlyTransaction();
        engine.assertLostOwnerRepliesAndNoChangeTakeover();
      }
    }
  }

  private static void assertPublishedAndReplayed(Engine engine) throws Exception {
    var firstCommand = new TxnCommand(List.of(), List.of(put("a", "one"), put("b", "two")), List.of());
    Receipt first = engine.run(firstCommand, 1);
    assertTrue(first.changed());
    assertEquals(2, first.revision());
    engine.assertValue("a", 2, "one");
    engine.assertValue("b", 2, "two");
    engine.assertResolvedResponse(first, firstCommand, true);
    engine.assertNettyCancellationWaitsForSerialization(first, firstCommand);

    var noChangeCommand = new TxnCommand(List.of(CompareCondition.numeric(
        KeyRange.point(key("a")), CompareCondition.Target.VERSION,
        CompareCondition.Relation.EQUAL, 0)), List.of(put("c", "unused")), List.of());
    Receipt unchanged = engine.run(noChangeCommand, 2);
    assertFalse(unchanged.changed());
    assertEquals(2, unchanged.revision());
    assertEquals(2, engine.currentRevision());
    assertEquals(1, engine.currentSequence());

    Receipt later = engine.run(new TxnCommand(List.of(), List.of(put("a", "new")), List.of()), 3);
    assertTrue(later.changed());
    assertEquals(3, later.revision());
    ResultNode.Txn replayed = engine.replayNoChanges(unchanged);
    assertFalse(replayed.succeeded());
    assertEquals(2, replayed.header().revision());
    assertTrue(replayed.responses().isEmpty());
    engine.assertResolvedResponse(unchanged, noChangeCommand, false);
    assertEquals(3, engine.currentRevision());
    assertEquals(2, engine.currentSequence());
  }

  private static void assertOwnerTransactions(Engine engine) throws Exception {
    Receipt ownerWrite = engine.runViaExecutor(new TxnCommand(List.of(), List.of(put("d", "owner")),
        List.of()), 4);
    assertTrue(ownerWrite.changed());
    assertEquals(4, ownerWrite.revision());
    engine.assertValue("d", 4, "owner");
    Receipt ownerNoChange = engine.runViaExecutor(new TxnCommand(List.of(CompareCondition.numeric(
        KeyRange.point(key("d")), CompareCondition.Target.VERSION,
        CompareCondition.Relation.EQUAL, 0)), List.of(put("e", "unused")), List.of()), 5);
    assertFalse(ownerNoChange.changed());
    assertEquals(4, engine.replayNoChanges(ownerNoChange).header().revision());
  }

  private record Receipt(InvocationId id, ByteKey requestDigest, long revision, boolean changed) {}

  private static final class Engine implements AutoCloseable {
    private final M3TestEnvironment environment;
    private final TableGroupStores stores;
    private final BudgetManager budgets;
    private final ScanScheduler scans;
    private final CommitOwnerService.OwnerToken owner;
    private final InvocationService invocations;
    private final TxnCommitPreparation preparation;
    private final BoundedPublicationHandoff handoff = new BoundedPublicationHandoff(1, 2, TIMEOUT);
    private final CommitPublisher publisher;
    private final ThreadPoolExecutor worker = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(16), Thread.ofPlatform().daemon().name("m3-txn-test-", 0).factory());
    private final TxnMutationExecutor mutations;
    private final TxnOwnerProcessor processor;
    private final TxnReadExecutor readOnly;

    private record PinFailureHarness(AtomicBoolean blockCompletion, CountDownLatch pinStarted,
        CompletableFuture<Void> pendingPin) {}

    private record CancellationEvidence(AtomicInteger responses, AtomicInteger terminations,
        AtomicReference<Status.Code> terminalCode) {}

    private void assertFailedReadPinReturnsError() throws Exception {
      var evidence = new CancellationEvidence(new AtomicInteger(), new AtomicInteger(), new AtomicReference<>());
      var harness = new PinFailureHarness(new AtomicBoolean(), new CountDownLatch(1), new CompletableFuture<>());
      var resources = new RuntimeConfiguration.ResourceBudgets(65_536, 4, 30_000, 4 << 20, 4 << 20, 4, 0);
      try (var server = Bootstrap.startConfigured(new RuntimeConfiguration.Listener(
          "m3-read-pin-failure", "127.0.0.1", 0, environment.system().tableGroupId()), resources,
          pinFailureService(harness))) {
        var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.boundListener().port()).usePlaintext().build();
        try {
          var range = Rpc.RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("pin-failure"))
              .setKeysOnly(true).build();
          assertPinFailureReturnsUnavailable(channel, range);
          cancelDuringPinFailure(channel, range, harness, evidence);
        } finally {
          channel.shutdownNow();
          assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
        }
      }
      assertEquals(0, evidence.responses().get());
      assertEquals(1, evidence.terminations().get());
      assertEquals(Status.Code.CANCELLED, evidence.terminalCode().get());
      assertEquals(0, budgets.snapshot().allocatedBytes());
      assertEquals(0, budgets.snapshot().reservedBytes());
    }

    private ReadViewPinRegistry failingPins(PinFailureHarness harness) {
      return (view, scope) -> stores.pins().register(view, scope).thenApply(pin ->
          new ReadViewPinRegistry.Pin() {
            @Override public long generation() { return pin.generation(); }
            @Override public CompletableFuture<Void> renew(RequestScope request) { return pin.renew(request); }
            @Override public CompletableFuture<Void> complete(RequestScope request) {
              if (harness.blockCompletion().get()) {
                harness.pinStarted().countDown();
                return harness.pendingPin();
              }
              return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
            }
            @Override public java.util.concurrent.CompletionStage<Void> releaseCompletion() {
              return pin.releaseCompletion();
            }
            @Override public void close() { pin.close(); }
          });
    }

    private KvService pinFailureService(PinFailureHarness harness) {
      var views = new ReadViewService(stores.control(), stores.manifests(), failingPins(harness),
          environment.system().profileFingerprint());
      var reads = new TxnReadExecutor(environment.system().tableGroupId(), views, stores.manifests(),
          environment.factory(), environment.primary(), worker, scans,
          new TxnExecutionLimits(4096, 1L << 20, 8L << 20, 4096, 64));
      var relay = new MutationRelay(stores.control(),
          (group, instance, epoch) -> CompletableFuture.failedFuture(new AssertionError("unexpected mutation")),
          (route, request, scope) -> CompletableFuture.failedFuture(new AssertionError("unexpected mutation")));
      var gateway = new TxnGateway(environment.system().tableGroupId(), environment.identity(), AUTHORIZATION,
          stores.clusterId(), 1, 1_572_864, stores.control(), relay, stores.invocationResults(),
          reads, budgets, reservation(), worker, new RequestMetrics());
      return new KvService((request, observer) -> observer.onError(Status.UNIMPLEMENTED.asRuntimeException()),
          gateway, gateway::deleteRange, gateway::range);
    }

    private static void assertPinFailureReturnsUnavailable(io.grpc.ManagedChannel channel,
        Rpc.RangeRequest range) {
      var stub = KVGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
      var failure = assertThrows(io.grpc.StatusRuntimeException.class, () -> stub.range(range));
      assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
      var txn = Rpc.TxnRequest.newBuilder().addSuccess(Rpc.RequestOp.newBuilder().setRequestRange(range)).build();
      var txnStub = KVGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
      var txnFailure = assertThrows(io.grpc.StatusRuntimeException.class, () -> txnStub.txn(txn));
      assertEquals(Status.Code.UNAVAILABLE, txnFailure.getStatus().getCode());
    }

    private static void cancelDuringPinFailure(io.grpc.ManagedChannel channel,
        Rpc.RangeRequest range, PinFailureHarness harness, CancellationEvidence evidence) throws Exception {
      harness.blockCompletion().set(true);
      var cancelled = new CountDownLatch(1);
      var context = io.grpc.Context.current().withCancellation();
      try {
        context.run(() -> KVGrpc.newStub(channel).range(range, new StreamObserver<>() {
          @Override public void onNext(Rpc.RangeResponse response) { evidence.responses().incrementAndGet(); }
          @Override public void onError(Throwable failure) {
            evidence.terminalCode().set(Status.fromThrowable(failure).getCode());
            evidence.terminations().incrementAndGet();
            cancelled.countDown();
          }
          @Override public void onCompleted() { evidence.terminations().incrementAndGet(); cancelled.countDown(); }
        }));
        assertTrue(harness.pinStarted().await(5, TimeUnit.SECONDS));
        context.cancel(null);
        assertTrue(cancelled.await(5, TimeUnit.SECONDS));
        harness.pendingPin().completeExceptionally(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      } finally {
        context.cancel(null);
        harness.pendingPin().completeExceptionally(new BackendFailure(BackendFailure.Category.CANCELLED));
      }
    }

    private Engine(M3TestEnvironment environment) throws Exception {
      this.environment = environment;
      var limits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1L << 20, 256L << 20);
      budgets = new BudgetManager(limits, Map.of(environment.system().tableGroupId(), limits), limits);
      scans = new ScanScheduler(budgets, Duration.ofSeconds(30));
      try (RequestScope scope = RequestScope.withTimeout(TIMEOUT)) {
        stores = TableGroupStores.open(environment.factory(), environment.identity(), environment.system(),
            environment.primary(), environment.current(),
            new SnapshotPinConfiguration(2, 16, Duration.ofSeconds(60)), scope).join();
        long now = System.currentTimeMillis();
        var claimed = new CommitOwnerService(stores.control()).claim("m3-txn-test-owner", now,
            now + Duration.ofHours(1).toMillis(), scope).join().orElseThrow();
        owner = new io.etcdhbase.core.commit.AuthorizationBarrierUpgrade(stores.control())
            .enable(claimed, now, scope).join();
      }
      invocations = new InvocationService(stores.invocations());
      preparation = new TxnCommitPreparation(new ResultPreparation(stores.resultRepository()),
          stores.results(), new ManifestPreparation(stores.manifestRepository(),
              new ManifestPreparation.Limits(128, 4L << 20, 128, 8L << 20)), invocations, 8L << 20);
      publisher = new CommitPublisher(stores.control(), stores.outcomes(),
          new CommitPlanVerifier(stores.manifests(), stores.materialization(), stores.results()),
          stores.invocations(), handoff, stores.finalizer());
      mutations = new TxnMutationExecutor(environment.system().tableGroupId(),
          environment.system().profileFingerprint(), stores.control(), stores.views(), stores.manifests(),
          stores.records(), stores.manifestRepository(), environment.factory(), environment.primary(), worker,
          scans, preparation, publisher,
          new TxnExecutionLimits(4096, 1L << 20, 8L << 20, 4096, 64));
      processor = new TxnOwnerProcessor(environment.system().tableGroupId(), owner, budgets,
          reservation(), mutations, stores.outcomes(), 1_572_864);
      readOnly = new TxnReadExecutor(environment.system().tableGroupId(), stores.views(), stores.manifests(),
          environment.factory(), environment.primary(), worker, scans,
          new TxnExecutionLimits(4096, 1L << 20, 8L << 20, 4096, 64));
    }

    private Receipt run(TxnCommand command, int requestNumber) throws Exception {
      return run(command, requestNumber, 8L << 20);
    }

    private Receipt run(TxnCommand command, int requestNumber, long workingBytes) throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        BudgetManager.Account account = account(scope);
        var control = stores.control().read(scope).join().orElseThrow().state();
        InvocationId id = InvocationId.create();
        ByteKey requestDigest = digest(requestNumber);
        var proposed = InvocationBinding.begin(id, requestDigest, AUTHORIZATION, TransactionId.create(),
            owner.epoch(), control.publishedSequence(), control.publishedRevision());
        var binding = invocations.bindOrJoin(proposed, scope).join();
        var physical = environment.openPrimary(scope);
        var session = TxnReadSession.open(stores.views(), List.of(), AUTHORIZATION, physical,
            stores.manifests(), environment.primary().codec()::bucket, 1, scope).join();
        var comparisons = new CompareReadExecutor(session, scans, account, scope,
            new CompareReadExecutor.Limits(1, 4096, 64, 4096));
        var evaluator = new TxnEvaluator(session, comparisons, account, scope,
            new TxnEvaluator.Limits(4096, 1L << 20, workingBytes), environment.primary().codec()::bucket,
            stores.records(), stores.manifestRepository());
        var evaluated = evaluator.evaluate(command, Optional.of(binding)).join();
        var ready = preparation.prepare(evaluated, binding, scope, account).join();
        if (ready instanceof TxnCommitPreparation.Sealed sealed) {
          var outcome = publisher.publish(sealed.invocation(), owner, sealed.manifest(), scope,
              CommitAuthorizationProofTestSupport.serviceProof(
                  environment.identity(), AUTHORIZATION, control.authorizationBarrierGeneration())).join();
          assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, outcome.state());
          return new Receipt(id, requestDigest, outcome.decision().orElseThrow().revision(), true);
        }
        var noChanges = assertInstanceOf(TxnCommitPreparation.NoChanges.class, ready);
        assertEquals(InvocationBinding.State.NO_CHANGES, noChanges.invocation().state());
        return new Receipt(id, requestDigest, binding.baseRevision(), false);
      } finally {
        closeScope(scope);
      }
    }

    private void assertDeleteOutputBudgetRejectsBeforePublication(TxnCommand command) throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        BudgetManager.Account account = account(scope);
        var control = stores.control().read(scope).join().orElseThrow().state();
        var binding = invocations.bindOrJoin(InvocationBinding.begin(InvocationId.create(), digest(51),
            AUTHORIZATION, TransactionId.create(), owner.epoch(), control.publishedSequence(),
            control.publishedRevision()), scope).join();
        var physical = environment.openPrimary(scope);
        var session = TxnReadSession.open(stores.views(), List.of(), AUTHORIZATION, physical,
            stores.manifests(), environment.primary().codec()::bucket, 1, scope).join();
        var comparisons = new CompareReadExecutor(session, scans, account, scope,
            new CompareReadExecutor.Limits(1, 4096, 64, 4096));
        var evaluator = new TxnEvaluator(session, comparisons, account, scope,
            new TxnEvaluator.Limits(4096, 256, 64 * 1024), environment.primary().codec()::bucket,
            stores.records(), stores.manifestRepository());
        var failure = assertThrows(CompletionException.class,
            () -> evaluator.evaluate(command, Optional.of(binding)).join());
        assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
            assertInstanceOf(BackendFailure.class, failure.getCause()).category());
      } finally {
        closeScope(scope);
      }
    }

    private void assertPartialDeleteMaterializationRecovers() throws Exception {
      InvocationBinding selected = interruptDeleteMaterialization();
      assertPendingDeleteAndRecover(selected);
    }

    private InvocationBinding interruptDeleteMaterialization() throws Exception {
      InvocationBinding selected;
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        BudgetManager.Account account = account(scope);
        var control = stores.control().read(scope).join().orElseThrow().state();
        var binding = invocations.bindOrJoin(InvocationBinding.begin(InvocationId.create(), digest(61),
            AUTHORIZATION, TransactionId.create(), owner.epoch(), control.publishedSequence(),
            control.publishedRevision()), scope).join();
        var command = new TxnCommand(List.of(), List.of(
            new TxnCommand.Delete(KeyRange.between(key("a"), key("c")), true)), List.of());
        var evaluated = evaluator(scope, account, stores.records()).evaluate(command, Optional.of(binding)).join();
        var sealed = assertInstanceOf(TxnCommitPreparation.Sealed.class,
            preparation.prepare(evaluated, binding, scope, account).join());
        selected = sealed.invocation();
        var interrupted = new FailSecondMaterialization(stores.materialization());
        var finalizer = new CommitFinalizationService(stores.control(), stores.outcomes(), stores.decisions(),
            stores.manifests(), interrupted, stores.results());
        var interruptedPublisher = new CommitPublisher(stores.control(), stores.outcomes(),
            new CommitPlanVerifier(stores.manifests(), stores.materialization(), stores.results()),
            stores.invocations(), handoff, finalizer);
        assertThrows(CompletionException.class,
            () -> interruptedPublisher.publish(sealed.invocation(), owner, sealed.manifest(), scope,
                CommitAuthorizationProofTestSupport.serviceProof(environment.identity(), AUTHORIZATION,
                    control.authorizationBarrierGeneration())).join());
        assertEquals(2, interrupted.applied.get());
        var pending = stores.outcomes().resolve(sealed.invocation(), scope).join();
        assertEquals(CommitOutcomeResolver.State.SELECTED_PENDING, pending.state());
        assertEquals(3, currentRevision());
        assertEquals(1, control().finalizedSequence());
        assertPartialPhysicalState(scope);
      } finally {
        closeScope(scope);
      }
      return selected;
    }

    private void assertPartialPhysicalState(RequestScope scope) throws Exception {
      var physical = environment.openPrimary(scope);
      assertEquals(StorageMetadata.State.TOMBSTONE,
          physical.get(key("a"), 3, ReadProjection.METADATA).join().orElseThrow().metadata().state());
      assertEquals(StorageMetadata.State.LIVE,
          physical.get(key("b"), 3, ReadProjection.METADATA).join().orElseThrow().metadata().state());
    }

    private void assertPendingDeleteAndRecover(InvocationBinding selected) throws Exception {
      RequestScope readScope = RequestScope.withTimeout(TIMEOUT);
      try {
        var account = account(readScope);
        var physical = environment.openPrimary(readScope);
        assertPendingDeleteView(physical, account, readScope);
        finalizeDeleteAndVerifyResult(selected, physical, account, readScope);
      } finally {
        closeScope(readScope);
      }
    }

    private void assertPendingDeleteView(RecordStore physical, BudgetManager.Account account,
        RequestScope readScope) {
      var read = TxnReadSession.open(stores.views(), List.of(), AUTHORIZATION, physical,
          stores.manifests(), environment.primary().codec()::bucket, 1, readScope).join();
      for (String name : List.of("a", "b")) {
        try (var point = new DirectRecordReader(read.reader(0), account, 4096)
            .readForPreparation(key(name), 3, ReadProjection.METADATA).join()) {
          assertEquals(StorageMetadata.State.TOMBSTONE, point.record().orElseThrow().metadata().state());
        }
      }
      read.finish().join();
    }

    private void finalizeDeleteAndVerifyResult(InvocationBinding selected, RecordStore physical,
        BudgetManager.Account account, RequestScope readScope) {
      var pending = stores.outcomes().resolve(selected, readScope).join();
      assertEquals(CommitOutcomeResolver.State.SELECTED_PENDING, pending.state());
      assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED,
          stores.finalizer().finish(selected, pending, readScope).join().state());
      for (String name : List.of("a", "b")) {
        assertEquals(StorageMetadata.State.TOMBSTONE,
            physical.get(key(name), 3, ReadProjection.METADATA).join().orElseThrow().metadata().state());
      }
      assertEquals(2, control().finalizedSequence());
      try (var result = stores.invocationResults().resolveTransaction(selected.invocationId(),
          selected.requestDigest(), AUTHORIZATION, readScope, account).join()) {
        var root = assertInstanceOf(ResultNode.Txn.class, ResultMessages.decodeTree(result.payload().buffer()).root());
        var deleted = assertInstanceOf(ResultNode.Delete.class, root.responses().getFirst());
        assertEquals(2, deleted.deleted());
        assertEquals(2, deleted.previous().size());
      }
    }

    private static final class FailSecondMaterialization implements MaterializationStore {
      private final MaterializationStore delegate;
      private final AtomicInteger applied = new AtomicInteger();

      private FailSecondMaterialization(MaterializationStore delegate) { this.delegate = delegate; }

      @Override public CompletableFuture<Void> preflight(PreparedCandidate candidate, RequestScope scope) {
        return delegate.preflight(candidate, scope);
      }

      @Override public CompletableFuture<ApplyResult> applySelected(
          CommitDecision decision, PreparedCandidate candidate, RequestScope scope) {
        if (applied.incrementAndGet() == 2) {
          return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
        }
        return delegate.applySelected(decision, candidate, scope);
      }
    }

    private void assertDeleteResult(Receipt receipt, long deleted, int previousCount) throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        var result = stores.invocationResults().resolveTransaction(receipt.id(), receipt.requestDigest(),
            AUTHORIZATION, scope, account(scope)).join();
        try (result) {
          var tree = ResultMessages.decodeTree(result.payload().buffer());
          var root = assertInstanceOf(ResultNode.Txn.class, tree.root());
          var response = assertInstanceOf(ResultNode.Delete.class, root.responses().getFirst());
          assertEquals(deleted, response.deleted());
          assertEquals(previousCount, response.previous().size());
          for (int index = 0; index < previousCount; index++) {
            assertEquals(key(String.format("delete-%03d", index)), response.previous().get(index).key());
            assertEquals(1024, response.previous().get(index).value().orElseThrow().length);
          }
        }
      } finally {
        closeScope(scope);
      }
    }

    private void assertHistoricalAndDeletedRecord(String name, long historical, long deletedRevision,
        byte[] expected) throws Exception {
      try (RequestScope scope = RequestScope.withTimeout(TIMEOUT)) {
        var store = environment.openPrimary(scope);
        var before = store.get(key(name), historical, ReadProjection.FULL).join().orElseThrow();
        assertArrayEquals(expected, before.value().orElseThrow());
        var after = store.get(key(name), deletedRevision, ReadProjection.FULL).join().orElseThrow();
        assertEquals(StorageMetadata.State.TOMBSTONE, after.metadata().state());
        assertEquals(deletedRevision, after.metadata().modificationRevision());
      }
    }

    private void assertUncertainWorksetCannotReplay() throws Exception {
      var uncertain = new UncertainPageStore(stores.records());
      var command = new TxnCommand(List.of(), List.of(put("uncertain", "private")), List.of());
      InvocationBinding binding = writeUncertainPage(command, uncertain);
      assertUncertainRetryRejected(command, binding);
      recoverFencedAttempt(command, binding);
    }

    private InvocationBinding writeUncertainPage(TxnCommand command, UncertainPageStore uncertain) throws Exception {
      InvocationBinding binding;
      RequestScope first = RequestScope.withTimeout(TIMEOUT);
      try {
        var account = account(first);
        var control = stores.control().read(first).join().orElseThrow().state();
        binding = invocations.bindOrJoin(InvocationBinding.begin(InvocationId.create(), digest(41),
            AUTHORIZATION, TransactionId.create(), owner.epoch(), control.publishedSequence(),
            control.publishedRevision()), first).join();
        var evaluator = evaluator(first, account, uncertain);
        assertThrows(CompletionException.class, () -> evaluator.evaluate(command, Optional.of(binding)).join());
        assertTrue(uncertain.deliveredPage.get(), "HBase accepted the page before its reply was lost");
      } finally {
        closeScope(first);
      }
      return binding;
    }

    private void assertUncertainRetryRejected(TxnCommand command, InvocationBinding binding) throws Exception {
      SystemRecordStore underlying = stores.records();
      RequestScope retry = RequestScope.withTimeout(TIMEOUT);
      try {
        var evaluator = evaluator(retry, account(retry), underlying);
        var failure = assertThrows(CompletionException.class,
            () -> evaluator.evaluate(command, Optional.of(binding)).join());
        assertEquals(BackendFailure.Category.UNKNOWN, assertInstanceOf(BackendFailure.class, failure.getCause()).category());
        assertTrue(environment.openPrimary(retry).get(key("uncertain"), binding.baseRevision(),
            ReadProjection.FULL).join().isEmpty());
      } finally {
        closeScope(retry);
      }
    }

    private void recoverFencedAttempt(TxnCommand command, InvocationBinding binding) throws Exception {
      SystemRecordStore underlying = stores.records();
      RequestScope recovery = RequestScope.withTimeout(TIMEOUT);
      try {
        long afterExpiry = System.currentTimeMillis() + Duration.ofHours(2).toMillis();
        var nextOwner = new CommitOwnerService(stores.control()).claim("m3-workset-recovery", afterExpiry,
            afterExpiry + Duration.ofHours(1).toMillis(), recovery).join().orElseThrow();
        var outcome = stores.outcomes().resolve(binding, recovery).join();
        assertEquals(CommitOutcomeResolver.State.NOT_SELECTED, outcome.state());
        var fenced = invocations.transition(binding, binding.notSelected(outcome.notSelectedProof().orElseThrow()),
            recovery).join();
        assertEquals(InvocationBinding.State.NOT_SELECTED, fenced.state());
        var account = account(recovery);
        var control = stores.control().read(recovery).join().orElseThrow().state();
        var replacement = invocations.bindOrJoin(InvocationBinding.begin(InvocationId.create(), digest(43),
            AUTHORIZATION, TransactionId.create(), nextOwner.epoch(), control.publishedSequence(),
            control.publishedRevision()), recovery).join();
        var evaluated = evaluator(recovery, account, underlying).evaluate(command, Optional.of(replacement)).join();
        var prepared = assertInstanceOf(TxnCommitPreparation.Sealed.class,
            preparation.prepare(evaluated, replacement, recovery, account).join());
        var published = publisher.publish(prepared.invocation(), nextOwner, prepared.manifest(), recovery,
            CommitAuthorizationProofTestSupport.serviceProof(
                environment.identity(), AUTHORIZATION, control.authorizationBarrierGeneration())).join();
        assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, published.state());
        assertEquals(4, published.decision().orElseThrow().revision());
      } finally {
        closeScope(recovery);
      }
    }

    private static final class UncertainPageStore implements SystemRecordStore {
      private final SystemRecordStore underlying;
      private final AtomicBoolean deliveredPage = new AtomicBoolean();

      private UncertainPageStore(SystemRecordStore underlying) { this.underlying = underlying; }
      @Override public CompletableFuture<Optional<ByteKey>> read(
          SystemRecordType type, ByteKey id, RequestScope scope) { return underlying.read(type, id, scope); }
      @Override public int bucket(SystemRecordType type, ByteKey id) { return underlying.bucket(type, id); }
      @Override public CompletableFuture<CreateResult> createIfAbsent(
          SystemRecordType type, ByteKey id, ByteKey value, RequestScope scope) {
        return underlying.createIfAbsent(type, id, value, scope).thenCompose(result -> lostReply(type, id)
            ? CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN))
            : CompletableFuture.completedFuture(result));
      }
      private boolean lostReply(SystemRecordType type, ByteKey id) {
        return type == SystemRecordType.WORKSET && id.length() == 29 && deliveredPage.compareAndSet(false, true);
      }
      @Override public CompletableFuture<Boolean> compareAndSet(SystemRecordType type, ByteKey id,
          Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) {
        return underlying.compareAndSet(type, id, expected, replacement, scope);
      }
    }

    private TxnEvaluator evaluator(RequestScope scope, BudgetManager.Account account,
        SystemRecordStore worksetRecords) throws Exception {
      var physical = environment.openPrimary(scope);
      var session = TxnReadSession.open(stores.views(), List.of(), AUTHORIZATION, physical,
          stores.manifests(), environment.primary().codec()::bucket, 1, scope).join();
      var compares = new CompareReadExecutor(session, scans, account, scope,
          new CompareReadExecutor.Limits(1, 4096, 64, 4096));
      return new TxnEvaluator(session, compares, account, scope,
          new TxnEvaluator.Limits(4096, 1L << 20, 64 * 1024), environment.primary().codec()::bucket,
          worksetRecords, stores.manifestRepository());
    }

    private ResultNode.Txn replayNoChanges(Receipt receipt) throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        try (var result = stores.invocationResults().resolveNoChanges(receipt.id(), receipt.requestDigest(),
            AUTHORIZATION, scope, account(scope)).join()) {
          return assertInstanceOf(ResultNode.Txn.class, ResultMessages.decodeTree(result.payload().buffer()).root());
        }
      } finally {
        closeScope(scope);
      }
    }

    private void assertResolvedResponse(Receipt receipt, TxnCommand command, boolean changed) throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        BudgetManager.Account account = account(scope);
        var result = stores.invocationResults().resolveTransaction(receipt.id(), receipt.requestDigest(),
            AUTHORIZATION, scope, account).join();
        assertEquals(changed, result.changed());
        var emitted = new AtomicReference<Rpc.TxnResponse>();
        var completed = new AtomicBoolean();
        TxnResponseEmitter.sendResolved(command, result, ResultMessages::decodeTree,
            stores.clusterId(), 1, scope, account, new StreamObserver<>() {
              @Override public void onNext(Rpc.TxnResponse value) { emitted.set(value); }
              @Override public void onError(Throwable failure) { throw new AssertionError(failure); }
              @Override public void onCompleted() { completed.set(true); }
            });
        assertTrue(completed.get());
        assertEquals(receipt.revision(), emitted.get().getHeader().getRevision());
      } finally {
        closeScope(scope);
      }
    }

    private Receipt runViaExecutor(TxnCommand command, int requestNumber) throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        BudgetManager.Account account = account(scope);
        var control = stores.control().read(scope).join().orElseThrow().state();
        InvocationId id = InvocationId.create();
        ByteKey requestDigest = digest(requestNumber);
        var proposed = InvocationBinding.begin(id, requestDigest, AUTHORIZATION, TransactionId.create(),
            owner.epoch(), control.publishedSequence(), control.publishedRevision());
        var binding = invocations.bindOrJoin(proposed, scope).join();
        assertEquals(id, mutations.executeNew(command, binding, owner, environment.identity(),
            CommitAuthorizationProofTestSupport.serviceProof(
                environment.identity(), AUTHORIZATION, control.authorizationBarrierGeneration()),
            scope, account).join());
        var durable = stores.invocations().read(id, scope).join().orElseThrow();
        if (durable.state() == InvocationBinding.State.NO_CHANGES) {
          return new Receipt(id, requestDigest, binding.baseRevision(), false);
        }
        var outcome = stores.outcomes().resolve(durable, scope).join();
        assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, outcome.state());
        return new Receipt(id, requestDigest, outcome.decision().orElseThrow().revision(), true);
      } finally {
        closeScope(scope);
      }
    }

    private void assertFinalizationCapacityIsReservedBeforePreparing() throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        BudgetManager.Account account = account(scope);
        var control = stores.control().read(scope).join().orElseThrow().state();
        var binding = invocations.bindOrJoin(InvocationBinding.begin(InvocationId.create(), digest(6),
            AUTHORIZATION, TransactionId.create(), owner.epoch(), control.publishedSequence(),
            control.publishedRevision()), scope).join();
        try (var held = account.tryAcquire(new BudgetManager.Work(0, 4, 0, 4, 0)).orElseThrow()) {
          var command = new TxnCommand(List.of(), List.of(put("blocked", "x")), List.of());
          var failure = assertThrows(java.util.concurrent.CompletionException.class,
              () -> mutations.executeNew(command, binding, owner, environment.identity(),
                  CommitAuthorizationProofTestSupport.serviceProof(
                      environment.identity(), AUTHORIZATION, control.authorizationBarrierGeneration()),
                  scope, account).join());
          assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
              assertInstanceOf(BackendFailure.class, failure.getCause()).category());
          assertEquals(InvocationBinding.State.PREPARING,
              stores.invocations().read(binding.invocationId(), scope).join().orElseThrow().state());
          assertEquals(control.publishedSequence(), stores.control().read(scope).join()
              .orElseThrow().state().publishedSequence());
        }
      } finally {
        closeScope(scope);
      }
    }

    private void assertTaggedOwnerRequest() {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        var wire = Rpc.TxnRequest.newBuilder().addSuccess(Rpc.RequestOp.newBuilder()
            .setRequestPut(Rpc.PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("tagged"))
                .setValue(ByteString.copyFromUtf8("value")))).build();
        ByteKey operation = MutationOperationEnvelope.transaction(wire.toByteArray());
        ByteKey requestDigest = MutationForwardRequest.digest(environment.system().tableGroupId(), operation);
        var state = stores.control().read(scope).join().orElseThrow().state();
        InvocationId id = InvocationId.create();
        var request = new MutationForwardRequest(environment.system().tableGroupId(), id, requestDigest,
            AUTHORIZATION, ByteKey.copyOf(bytes(environment.identity().contextRef())), owner.epoch(),
            Duration.ofMinutes(1).toNanos(), 1, operation);
        var binding = invocations.bindOrJoin(InvocationBinding.begin(id, requestDigest, AUTHORIZATION,
            TransactionId.create(), owner.epoch(), state.publishedSequence(), state.publishedRevision()), scope).join();
        var authorization = CommitAuthorizationProofTestSupport.verified(
            environment.identity(), AUTHORIZATION, state.authorizationBarrierGeneration());
        var response = assertInstanceOf(OwnerForwarding.Completed.class,
            processor.process(request, binding, authorization, scope).join());
        assertArrayEquals(id.bytes(), response.operationResult());
        var persisted = stores.invocations().read(id, scope).join().orElseThrow();
        var replay = assertInstanceOf(OwnerForwarding.Completed.class,
            processor.process(request, persisted, authorization, scope).join());
        assertArrayEquals(id.bytes(), replay.operationResult());
      } finally {
        closeScope(scope);
      }
    }

    private void assertReadOnlyTransaction() throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        BudgetManager.Account account = account(scope);
        var command = new TxnCommand(List.of(CompareCondition.numeric(KeyRange.point(key("a")),
            CompareCondition.Target.VERSION, CompareCondition.Relation.GREATER, 0)),
            List.of(pointRange(key("a")), pointRange(key("tagged"))), List.of());
        var evaluated = readOnly.execute(command, AUTHORIZATION, environment.identity(), scope, account).join();
        var result = assertInstanceOf(ResultNode.Txn.class, evaluated.result().root());
        assertTrue(result.succeeded());
        assertEquals(5, result.header().revision());
        assertArrayEquals(bytes("new"), assertInstanceOf(ResultNode.Range.class,
            result.responses().getFirst()).records().getFirst().value().orElseThrow());
        assertTrue(stores.pins().readBoundary(scope).join().protections().isEmpty());
        assertEquals(4, stores.control().read(scope).join().orElseThrow().state().publishedSequence());
        var emitted = new AtomicReference<Rpc.TxnResponse>();
        var completed = new AtomicBoolean();
        TxnResponseEmitter.sendReadOnly(command, evaluated, stores.clusterId(), 1, scope, account,
            new StreamObserver<>() {
              @Override public void onNext(Rpc.TxnResponse value) { emitted.set(value); }
              @Override public void onError(Throwable failure) { throw new AssertionError(failure); }
              @Override public void onCompleted() { completed.set(true); }
            });
        assertTrue(completed.get());
        assertEquals(5, emitted.get().getHeader().getRevision());
        assertEquals(2, emitted.get().getResponsesCount());
        assertReadResponseRetainedUntilConsumerDrains(command, evaluated, scope, account);
      } finally {
        closeScope(scope);
      }
    }

    private void assertLostOwnerRepliesAndNoChangeTakeover() throws Exception {
      var queue = new SerialMutationQueue(0, 8L << 20, TIMEOUT);
      var receiver = receiver(owner);
      var lane = new MutationOwnerLane(environment.system().tableGroupId(), queue, receiver);
      var route = new OwnerForwarding.Route(owner.instance(), owner.epoch(), "fixture", "fixture-peer");
      var dropReply = new AtomicBoolean();
      var lastRequest = new AtomicReference<MutationForwardRequest>();
      var relay = droppedReplyRelay(lane, route, dropReply, lastRequest);
      var gateway = new TxnGateway(environment.system().tableGroupId(), environment.identity(), AUTHORIZATION,
          stores.clusterId(), 1, 1_572_864, stores.control(), relay, stores.invocationResults(),
          readOnly, budgets, reservation(), worker, new RequestMetrics());
      var service = new KvService((request, observer) -> observer.onError(Status.UNIMPLEMENTED.asRuntimeException()), gateway);
      var resources = new RuntimeConfiguration.ResourceBudgets(65_536, 4, 30_000, 4 << 20, 4 << 20, 4, 0);
      try {
        runDroppedReplyServer(service, resources, dropReply, lastRequest);
      } finally {
        queue.close();
        queue.drained().get(30, TimeUnit.SECONDS);
        assertTrue(queue.awaitTermination(Duration.ofSeconds(30)));
      }
    }

    private MutationRelay droppedReplyRelay(MutationOwnerLane lane, OwnerForwarding.Route route,
        AtomicBoolean dropReply, AtomicReference<MutationForwardRequest> lastRequest) {
      return new MutationRelay(stores.control(), (group, instance, epoch) ->
          CompletableFuture.completedFuture(route), (selected, request, scope) -> {
            lastRequest.set(request);
            return lane.receive("fixture-peer", request, scope).thenApply(reply -> {
              if (reply instanceof OwnerForwarding.Completed && dropReply.compareAndSet(true, false)) {
                throw new CompletionException(new java.io.IOException("fixture dropped owner reply"));
              }
              return reply;
            });
          });
    }

    private void runDroppedReplyServer(KvService service, RuntimeConfiguration.ResourceBudgets resources,
        AtomicBoolean dropReply, AtomicReference<MutationForwardRequest> lastRequest) throws Exception {
      try (var server = Bootstrap.startConfigured(new RuntimeConfiguration.Listener(
          "m3-txn-fault", "127.0.0.1", 0, environment.system().tableGroupId()), resources, service)) {
        var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.boundListener().port()).usePlaintext().build();
        try {
          assertDroppedRepliesAndTakeover(channel, dropReply, lastRequest);
        } finally {
          channel.shutdownNow();
          assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
        }
      }
    }

    private void assertDroppedRepliesAndTakeover(io.grpc.ManagedChannel channel,
        AtomicBoolean dropReply, AtomicReference<MutationForwardRequest> lastRequest) throws Exception {
      var stub = KVGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
      var key = ByteString.copyFromUtf8("lost-reply");
      var put = Rpc.RequestOp.newBuilder().setRequestPut(Rpc.PutRequest.newBuilder().setKey(key)
          .setValue(ByteString.copyFromUtf8("value"))).build();
      var range = Rpc.RequestOp.newBuilder().setRequestRange(Rpc.RangeRequest.newBuilder().setKey(key)).build();
      var absent = Rpc.Compare.newBuilder().setKey(key).setTarget(Rpc.Compare.CompareTarget.VERSION)
          .setResult(Rpc.Compare.CompareResult.EQUAL).setVersion(0).build();
      dropReply.set(true);
      var changed = stub.txn(Rpc.TxnRequest.newBuilder().addSuccess(put).build());
      assertFalse(dropReply.get());
      assertEquals(6, changed.getHeader().getRevision());
      dropReply.set(true);
      var unchanged = stub.txn(Rpc.TxnRequest.newBuilder().addCompare(absent)
          .addSuccess(put).addFailure(range).build());
      assertFalse(dropReply.get());
      assertFalse(unchanged.getSucceeded());
      assertEquals(6, unchanged.getHeader().getRevision());
      var originalRequest = lastRequest.get();
      assertEquals(ByteString.copyFromUtf8("value"),
          unchanged.getResponses(0).getResponseRange().getKvs(0).getValue());
      var later = stub.txn(Rpc.TxnRequest.newBuilder().addSuccess(Rpc.RequestOp.newBuilder()
          .setRequestPut(Rpc.PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("after-no-change"))
              .setValue(ByteString.copyFromUtf8("later")))).build());
      assertEquals(7, later.getHeader().getRevision());
      assertNoChangeReplayAfterOwnerSwitch(originalRequest, unchanged);
    }

    private void assertNettyCancellationWaitsForSerialization(Receipt receipt, TxnCommand command) throws Exception {
      M3NettyResponseProbe.verifyTxnCancellationDuringSerialization((scope, response) -> {
        var account = account(scope);
        var resolved = stores.invocationResults().resolveTransaction(receipt.id(), receipt.requestDigest(),
            AUTHORIZATION, scope, account).join();
        TxnResponseEmitter.sendResolved(command, resolved, ResultMessages::decodeTree,
            stores.clusterId(), 1, scope, account, response);
      }, () -> budgets.snapshot().allocatedBytes());
      assertEquals(0, budgets.snapshot().allocatedBytes());
      assertEquals(0, budgets.snapshot().reservedBytes());
    }

    private MutationForwardReceiver receiver(CommitOwnerService.OwnerToken token) {
      var ownerProcessor = new TxnOwnerProcessor(environment.system().tableGroupId(), token, budgets,
          reservation(), mutations, stores.outcomes(), 1_572_864);
      return new MutationForwardReceiver(stores.control(), invocations,
          (peer, request, barrierGeneration, scope) -> peer.equals("fixture-peer")
              ? CompletableFuture.completedFuture(new MutationForwardReceiver.VerifiedAuthorization(
                  environment.identity(), AUTHORIZATION,
                  CommitAuthorizationProofTestSupport.serviceProof(
                      environment.identity(), AUTHORIZATION, barrierGeneration)))
              : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION)), ownerProcessor);
    }

    private void assertNoChangeReplayAfterOwnerSwitch(MutationForwardRequest request,
        Rpc.TxnResponse expected) throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        long afterExpiry = System.currentTimeMillis() + Duration.ofHours(2).toMillis();
        var nextOwner = new CommitOwnerService(stores.control()).claim("m3-next-owner", afterExpiry,
            afterExpiry + Duration.ofHours(1).toMillis(), scope).join().orElseThrow();
        var replay = new MutationForwardRequest(request.tableGroupId(), request.invocationId(),
            request.requestDigest(), request.authorizationScopeDigest(), request.authorizationContextReference(),
            nextOwner.epoch(), request.remainingTimeoutNanos(), request.hopCount(), request.operationBytes());
        var receipt = assertInstanceOf(OwnerForwarding.Completed.class,
            receiver(nextOwner).receive("fixture-peer", replay, scope).join());
        assertArrayEquals(request.invocationId().bytes(), receipt.operationResult());
        try (var original = stores.invocationResults().resolveTransaction(request.invocationId(),
            request.requestDigest(), AUTHORIZATION, scope, account(scope)).join()) {
          assertFalse(original.changed());
          assertEquals(expected.getHeader().getRevision(), original.revision());
          var tree = assertInstanceOf(ResultNode.Txn.class, ResultMessages.decodeTree(original.payload().buffer()).root());
          assertFalse(tree.succeeded());
          assertEquals("value", new String(assertInstanceOf(ResultNode.Range.class,
              tree.responses().getFirst()).records().getFirst().value().orElseThrow(), StandardCharsets.UTF_8));
        }
        assertEquals(7, stores.control().read(scope).join().orElseThrow().state().publishedRevision());
      } finally {
        closeScope(scope);
      }
    }

    private void assertReadResponseRetainedUntilConsumerDrains(TxnCommand command,
        TxnEvaluator.ValidatedRead result, RequestScope scope, BudgetManager.Account account) throws Exception {
      var observer = new BlockingResponseObserver();
      Thread sender = Thread.ofVirtual().start(() -> {
        try {
          TxnResponseEmitter.sendReadOnly(command, result, stores.clusterId(), 1, scope, account, observer);
        } catch (RuntimeException | Error error) {
          observer.failure.set(error);
        }
      });
      try {
        assertTrue(observer.entered.await(30, TimeUnit.SECONDS));
        scope.cancelAsync(task -> Thread.ofVirtual().start(task));
        assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
        assertTrue(budgets.snapshot().allocatedBytes() > 0);
      } finally {
        observer.release.countDown();
        sender.join(30_000);
      }
      assertFalse(sender.isAlive());
      assertFalse(observer.completed.get());
      assertTrue(observer.failure.get() == null);
      scope.cleanupCompletion().toCompletableFuture().get(30, TimeUnit.SECONDS);
      assertEquals(0, budgets.snapshot().allocatedBytes());
    }

    private static final class BlockingResponseObserver implements StreamObserver<Rpc.TxnResponse> {
      private final CountDownLatch entered = new CountDownLatch(1);
      private final CountDownLatch release = new CountDownLatch(1);
      private final AtomicBoolean completed = new AtomicBoolean();
      private final AtomicReference<Throwable> failure = new AtomicReference<>();

      @Override public void onNext(Rpc.TxnResponse value) {
        entered.countDown();
        try {
          if (!release.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("consumer did not drain");
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(interrupted);
        }
      }
      @Override public void onError(Throwable error) { failure.set(error); }
      @Override public void onCompleted() { completed.set(true); }
    }

    private void assertValue(String key, long revision, String value) throws Exception {
      try (RequestScope scope = RequestScope.withTimeout(TIMEOUT)) {
        var record = environment.openPrimary(scope).get(key(key), revision, ReadProjection.FULL).join().orElseThrow();
        assertArrayEquals(bytes(value), record.value().orElseThrow());
      }
    }

    private long currentRevision() { return control().publishedRevision(); }
    private long currentSequence() { return control().publishedSequence(); }
    private io.etcdhbase.core.commit.CommitState control() {
      try (RequestScope scope = RequestScope.withTimeout(TIMEOUT)) {
        return stores.control().read(scope).join().orElseThrow().state();
      }
    }

    private BudgetManager.Account account(RequestScope scope) throws Exception {
      return budgets.admit(environment.system().tableGroupId(), scope, reservation());
    }

    private static BudgetManager.Reservation reservation() {
      return new BudgetManager.Reservation(64L << 10, 64L << 10, 2L << 20, 16L << 20, 48L << 20);
    }

    private void closeScope(RequestScope scope) {
      scope.close();
      scope.cleanupCompletion().toCompletableFuture().join();
      scope.physicalProducersCompletion().toCompletableFuture().join();
      assertEquals(0, budgets.snapshot().allocatedBytes());
      assertEquals(0, budgets.snapshot().reservedBytes());
    }

    @Override public void close() throws Exception {
      scans.close();
      scans.termination().toCompletableFuture().get(30, TimeUnit.SECONDS);
      handoff.close();
      handoff.drained().join();
      worker.shutdown();
      if (!worker.awaitTermination(10, TimeUnit.SECONDS)) {
        worker.shutdownNow();
        throw new IllegalStateException("transaction test workers did not terminate");
      }
    }
  }

  private static TxnCommand.Put put(String key, String value) {
    return new TxnCommand.Put(new PutCommand(key(key), bytes(value), 0, false, false, false));
  }
  private static TxnCommand.Range pointRange(ByteKey key) {
    return new TxnCommand.Range(new RangeCommand(KeyRange.point(key), RangeCommand.Context.TRANSACTION,
        0, 0, new RangeCommand.Sort(0, 0), new RangeCommand.RevisionFilter(0, 0, 0, 0),
        false, false, false));
  }

  private static ByteKey key(String value) { return ByteKey.copyOf(bytes(value)); }
  private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
  private static ByteKey digest(int value) {
    byte[] digest = new byte[32];
    java.util.Arrays.fill(digest, (byte) value);
    return ByteKey.copyOf(digest);
  }
}
