package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPlanVerifier;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.ResultCatalog;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.kv.PutResult;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.grpc.protocol.PutResponseEmitter;
import io.etcdhbase.hbase.codec.ResultMessages;
import io.etcdhbase.hbase.storage.HBaseInvocationRepository;
import io.etcdhbase.hbase.storage.M3TestEnvironment;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.server.commit.BoundedPublicationHandoff;
import io.etcdhbase.server.commit.PutCommitPreparation;
import io.etcdhbase.server.commit.MutationForwardReceiver;
import io.etcdhbase.server.commit.MutationOwnerLane;
import io.etcdhbase.server.commit.MutationRelay;
import io.etcdhbase.server.commit.PutMutationExecutor;
import io.etcdhbase.server.commit.CommitAuthorizationProofTestSupport;
import io.etcdhbase.server.commit.PutOwnerProcessor;
import io.etcdhbase.server.commit.PutGateway;
import io.etcdhbase.server.commit.SerialMutationQueue;
import io.etcdhbase.server.commit.TableGroupStores;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** Exercises the internal Put commit path on disposable HBase, without claiming RPC coverage. */
class M3PutCommitIT {
  static final ByteKey KEY = ByteKey.copyOf(new byte[] {0, (byte) 0xff, 1});
  static final ByteKey AUTHORIZATION = ByteKey.copyOf(new byte[32]);
  private static final Duration TIMEOUT = Duration.ofMinutes(2);

  @Test
  void preparesPublishesAndRecoversOriginalResultsOnBothLayouts() throws Exception {
    for (StorageTableBinding.Layout layout : List.of(
        StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (M3TestEnvironment fixture = M3TestEnvironment.start(layout);
           Engine engine = new Engine(fixture)) {
        engine.assertStaleOwnerIsRejected();
        engine.assertResponseBudgetStopsPublication();
        engine.assertMissingKeyProducesSemanticRejection();
        Receipt first = engine.put(new byte[] {1, 0}, true);
        assertEquals(2, first.result().revision());
        assertTrue(first.result().previous().isEmpty());

        Receipt second = engine.put(new byte[] {2, (byte) 0xff}, true);
        assertEquals(3, second.result().revision());
        assertPrevious(second.result(), new byte[] {1, 0}, 2, 2, 1);

        Receipt third = engine.put(new byte[] {3}, true);
        assertEquals(4, third.result().revision());
        assertPrevious(third.result(), new byte[] {2, (byte) 0xff}, 2, 3, 2);
        assertPrevious(engine.recover(second), new byte[] {1, 0}, 2, 2, 1);
        engine.assertCancellationWaitsForSerialization(second);
        engine.assertTerminalFailuresDoNotDoubleComplete(second);
        engine.assertNettyCancellationWaitsForSerialization(second);
        engine.assertPhysicalVersions();
      }
    }
  }

  private static void assertPrevious(PutResult result, byte[] value,
      long createRevision, long modificationRevision, long version) {
    var previous = result.previous().orElseThrow();
    assertArrayEquals(value, previous.value().orElseThrow());
    assertEquals(createRevision, previous.metadata().createRevision());
    assertEquals(modificationRevision, previous.metadata().modificationRevision());
    assertEquals(version, previous.metadata().version());
  }

  record Receipt(InvocationId id, ByteKey requestDigest, PutResult result) {}

  static final class Engine implements AutoCloseable {
    private final M3TestEnvironment fixture;
    private final TableGroupStores stores;
    private final BudgetManager budgets;
    private final CommitOwnerService.OwnerToken owner;
    private final HBaseInvocationRepository invocationStore;
    private final InvocationService invocations;
    private final ManifestCatalog manifests;
    private final ResultCatalog results;
    private final CommitOutcomeResolver outcomes;
    private final PutCommitPreparation preparation;
    private final BoundedPublicationHandoff handoff;
    private final CommitPublisher publisher;
    private final InvocationResultResolver resolver;
    private final ThreadPoolExecutor worker;
    private final ReadViewService views;
    private final PutMutationExecutor mutations;
    private final SerialMutationQueue ownerQueue;
    private final MutationRelay relay;
    private final AtomicBoolean dropOwnerReply = new AtomicBoolean();
    private final AtomicBoolean failOwnerSend = new AtomicBoolean();

    Engine(M3TestEnvironment fixture) throws Exception {
      this(fixture, Duration.ofHours(1));
    }

    Engine(M3TestEnvironment fixture, Duration ownerLease) throws Exception {
      this.fixture = fixture;
      var limits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1L << 20, 256L << 20);
      budgets = new BudgetManager(limits, Map.of(fixture.system().tableGroupId(), limits), limits);
      var opened = openStoresAndClaimOwner(fixture, ownerLease);
      stores = opened.stores();
      owner = opened.owner();
      invocationStore = stores.invocations();
      invocations = new InvocationService(invocationStore);
      manifests = stores.manifests();
      results = stores.results();
      outcomes = stores.outcomes();
      preparation = new PutCommitPreparation(new ResultPreparation(stores.resultRepository()), results,
          new ManifestPreparation(stores.manifestRepository(), new ManifestPreparation.Limits(
              1, 4L << 20, 1, 8L << 20)), invocations);
      handoff = new BoundedPublicationHandoff(1, 2, TIMEOUT);
      publisher = new CommitPublisher(stores.control(), outcomes,
          new CommitPlanVerifier(manifests, stores.materialization(), results),
          invocationStore, handoff, stores.finalizer());
      resolver = stores.invocationResults();
      worker = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(32),
          Thread.ofPlatform().daemon().name("m3-fixture-reader-", 0).factory());
      views = stores.views();
      mutations = new PutMutationExecutor(fixture.system().tableGroupId(),
          fixture.system().profileFingerprint(), stores.control(), views, manifests,
          fixture.factory(), fixture.primary(), worker, preparation, publisher, 5L << 20, 8L << 20);
      ownerQueue = new SerialMutationQueue(4, 64L << 20, TIMEOUT);
      relay = createRelay();
    }

    private record StoreOwner(TableGroupStores stores, CommitOwnerService.OwnerToken owner) {}

    private static StoreOwner openStoresAndClaimOwner(M3TestEnvironment fixture, Duration ownerLease)
        throws Exception {
      try (RequestScope scope = RequestScope.withTimeout(TIMEOUT)) {
        var stores = TableGroupStores.open(fixture.factory(), fixture.identity(), fixture.system(),
            fixture.primary(), fixture.current(), new SnapshotPinConfiguration(
                2, 16, Duration.ofSeconds(60)), scope).join();
        assertEquals(fixture.clusterId(), stores.clusterId());
        long now = System.currentTimeMillis();
        if (ownerLease.isNegative() || ownerLease.isZero()) {
          throw new IllegalArgumentException("fixture owner lease must be positive");
        }
        var owner = new CommitOwnerService(stores.control()).claim("m3-test-owner", now,
            Math.addExact(now, ownerLease.toMillis()), scope).join().orElseThrow();
        owner = new io.etcdhbase.core.commit.AuthorizationBarrierUpgrade(stores.control())
            .enable(owner, now, scope).join();
        return new StoreOwner(stores, owner);
      }
    }

    private MutationRelay createRelay() {
      var processor = new PutOwnerProcessor(fixture.system().tableGroupId(), owner, budgets,
          reservation(), mutations, outcomes);
      var receiver = new MutationForwardReceiver(stores.control(), invocations,
          (peer, request, barrierGeneration, scope) -> peer.equals("fixture-loopback-peer")
              ? java.util.concurrent.CompletableFuture.completedFuture(
                  CommitAuthorizationProofTestSupport.verified(
                      fixture.identity(), AUTHORIZATION, barrierGeneration))
              : java.util.concurrent.CompletableFuture.failedFuture(
                  new BackendFailure(BackendFailure.Category.PERMISSION)), processor);
      var lane = new MutationOwnerLane(fixture.system().tableGroupId(), ownerQueue, receiver);
      var route = new OwnerForwarding.Route(owner.instance(), owner.epoch(),
          "fixture-loopback-endpoint", "fixture-loopback-peer");
      // This is an explicitly restricted in-process channel for the disposable fixture only.
      return new MutationRelay(stores.control(),
          (group, instance, epoch) -> group.equals(fixture.system().tableGroupId())
              && instance.equals(owner.instance()) && epoch == owner.epoch()
                  ? java.util.concurrent.CompletableFuture.completedFuture(route)
                  : java.util.concurrent.CompletableFuture.failedFuture(
                      new BackendFailure(BackendFailure.Category.UNAVAILABLE)),
          (selected, request, scope) -> {
            if (!selected.equals(route)) {
              return java.util.concurrent.CompletableFuture.failedFuture(
                  new BackendFailure(BackendFailure.Category.PERMISSION));
            }
            if (failOwnerSend.compareAndSet(true, false)) {
              return java.util.concurrent.CompletableFuture.failedFuture(
                  new IOException("fixture failed before owner dispatch"));
            }
            return lane.receive("fixture-loopback-peer", request, scope).thenApply(reply -> {
              if (reply instanceof OwnerForwarding.Completed
                  && dropOwnerReply.compareAndSet(true, false)) {
                throw new CompletionException(new IOException("fixture dropped owner reply"));
              }
              return reply;
            });
          });
    }

    void dropNextOwnerReply() {
      if (!dropOwnerReply.compareAndSet(false, true)) {
        throw new IllegalStateException("an owner reply is already scheduled to be dropped");
      }
    }

    void failNextOwnerSend() {
      if (!failOwnerSend.compareAndSet(false, true)) {
        throw new IllegalStateException("an owner send is already scheduled to fail");
      }
    }

    io.etcdhbase.grpc.service.KvService putService() {
      return new io.etcdhbase.grpc.service.KvService(new PutGateway(fixture.system().tableGroupId(),
          fixture.identity(), AUTHORIZATION, stores.clusterId(), 1,
          io.etcdhbase.grpc.protocol.PutProtocolLimit.DEFAULT_ETCD_MAX_REQUEST_BYTES, stores.control(), relay,
          resolver, budgets, reservation(), worker));
    }

    Receipt put(byte[] value, boolean previous) throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        BudgetManager.Account account = account(scope);
        InvocationId id = InvocationId.create();
        var wire = etcdserverpb.Rpc.PutRequest.newBuilder()
            .setKey(com.google.protobuf.ByteString.copyFrom(KEY.bytes()))
            .setValue(com.google.protobuf.ByteString.copyFrom(value)).setPrevKv(previous).build();
        var request = forwardedRequest(id, wire);
        var response = assertInstanceOf(MutationRelay.KnownResult.class, relay.forward(request, scope).join());
        assertArrayEquals(id.bytes(), response.responseBytes());
        ByteKey requestDigest = request.requestDigest();
        var sealed = invocationStore.read(id, scope).join().orElseThrow();
        var manifest = manifests.readRoot(sealed.manifestRoot().orElseThrow(), scope)
            .join().orElseThrow();
        assertEquals(1, manifest.candidateCount());
        assertEquals(1, manifest.eventCount());
        assertTrue(manifest.resultReference().isPresent());
        assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED,
            outcomes.resolve(sealed, scope).join().state());
        PutResult result = resolve(id, requestDigest, scope, account);
        return new Receipt(id, requestDigest, result);
      } finally {
        closeScope(scope);
      }
    }

    private void assertMissingKeyProducesSemanticRejection() throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        var before = fixture.control().read(scope).join().orElseThrow().state();
        var wire = etcdserverpb.Rpc.PutRequest.newBuilder()
            .setKey(com.google.protobuf.ByteString.copyFrom(KEY.bytes())).setIgnoreValue(true).build();
        var result = assertInstanceOf(MutationRelay.SemanticRejection.class,
            relay.forward(forwardedRequest(InvocationId.create(), wire), scope).join());
        assertEquals(KvFailure.Reason.KEY_NOT_FOUND, result.reason());
        assertEquals(before.publishedSequence(),
            fixture.control().read(scope).join().orElseThrow().state().publishedSequence());
      } finally {
        closeScope(scope);
      }
    }

    private MutationForwardRequest forwardedRequest(InvocationId id, etcdserverpb.Rpc.PutRequest request) {
      ByteKey operation = ByteKey.copyOf(request.toByteArray());
      return new MutationForwardRequest(fixture.system().tableGroupId(), id,
          MutationForwardRequest.digest(fixture.system().tableGroupId(), operation), AUTHORIZATION,
          ByteKey.copyOf(fixture.identity().contextRef().getBytes(StandardCharsets.UTF_8)),
          owner.epoch(), TimeUnit.SECONDS.toNanos(60), 0, operation);
    }

    private void assertStaleOwnerIsRejected() throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        BudgetManager.Account account = account(scope);
        var state = fixture.control().read(scope).join().orElseThrow().state();
        var binding = InvocationBinding.begin(InvocationId.create(), ByteKey.copyOf(new byte[32]),
            AUTHORIZATION, TransactionId.create(), owner.epoch(), state.publishedSequence(),
            state.publishedRevision());
        var stale = new CommitOwnerService.OwnerToken("stale-owner", owner.epoch());
        var command = new PutCommand(KEY, new byte[] {9}, 0, false, false, false);
        CompletionException failure = assertThrows(CompletionException.class, () ->
            mutations.executeNew(command, binding, stale, fixture.identity(),
                CommitAuthorizationProofTestSupport.serviceProof(
                    fixture.identity(), AUTHORIZATION, state.authorizationBarrierGeneration()),
                scope, account).join());
        assertEquals(BackendFailure.Category.UNAVAILABLE,
            assertInstanceOf(BackendFailure.class, failure.getCause()).category());
        assertEquals(state.publishedSequence(),
            fixture.control().read(scope).join().orElseThrow().state().publishedSequence());
      } finally {
        closeScope(scope);
      }
    }

    private void assertResponseBudgetStopsPublication() throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        BudgetManager.Account account = account(scope);
        var state = fixture.control().read(scope).join().orElseThrow().state();
        var proposed = InvocationBinding.begin(InvocationId.create(), ByteKey.copyOf(new byte[32]),
            AUTHORIZATION, TransactionId.create(), owner.epoch(), state.publishedSequence(),
            state.publishedRevision());
        var binding = invocations.bindOrJoin(proposed, scope).join();
        var constrained = new PutMutationExecutor(fixture.system().tableGroupId(),
            fixture.system().profileFingerprint(), fixture.control(), views, manifests,
            fixture.factory(), fixture.primary(), worker, preparation, publisher, 5L << 20, 64L << 10);
        var command = new PutCommand(KEY, new byte[] {9}, 0, true, false, false);
        CompletionException failure = assertThrows(CompletionException.class, () ->
            constrained.executeNew(command, binding, owner, fixture.identity(),
                CommitAuthorizationProofTestSupport.serviceProof(
                    fixture.identity(), AUTHORIZATION, state.authorizationBarrierGeneration()),
                scope, account).join());
        assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
            assertInstanceOf(BackendFailure.class, failure.getCause()).category());
        var after = fixture.control().read(scope).join().orElseThrow().state();
        assertEquals(state.publishedSequence(), after.publishedSequence());
        assertEquals(state.publishedRevision(), after.publishedRevision());
        assertTrue(after.publication().isEmpty());
      } finally {
        closeScope(scope);
      }
    }

    private PutResult recover(Receipt receipt) throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      try {
        return resolve(receipt.id(), receipt.requestDigest(), scope, account(scope));
      } finally {
        closeScope(scope);
      }
    }

    private PutResult resolve(InvocationId id, ByteKey requestDigest,
        RequestScope scope, BudgetManager.Account account) {
      try (var resolved = resolver.resolve(id, requestDigest, AUTHORIZATION, scope, account).join()) {
        PutResult result = ResultMessages.decodePut(resolved.payload().buffer());
        assertEquals(resolved.decision().revision(), result.revision());
        return result;
      }
    }

    private void assertCancellationWaitsForSerialization(Receipt receipt) throws Exception {
      RequestScope scope = RequestScope.withTimeout(TIMEOUT);
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      AtomicInteger completed = new AtomicInteger();
      AtomicInteger errors = new AtomicInteger();
      AtomicReference<Throwable> workerFailure = new AtomicReference<>();
      Thread sending = null;
      try {
        BudgetManager.Account account = account(scope);
        var resolved = resolver.resolve(receipt.id(), receipt.requestDigest(), AUTHORIZATION,
            scope, account).join();
        var observer = blockingObserver(receipt, entered, release, completed, errors);
        var command = new PutCommand(KEY, new byte[] {2, (byte) 0xff}, 0, true, false, false);
        sending = Thread.ofPlatform().start(() -> {
          try {
            PutResponseEmitter.send(command, resolved, ResultMessages::decodePut,
                fixture.clusterId(), 1, scope, account, observer);
          } catch (Throwable failure) {
            workerFailure.set(failure);
          }
        });
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        scope.cancel();
        assertTrue(budgets.snapshot().allocatedBytes() > 0);
        assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
        release.countDown();
        sending.join(TimeUnit.SECONDS.toMillis(10));
        assertFalse(sending.isAlive());
        assertNull(workerFailure.get());
        assertEquals(0, completed.get());
        assertEquals(0, errors.get());
      } finally {
        release.countDown();
        if (sending != null) sending.join(TimeUnit.SECONDS.toMillis(10));
        closeScope(scope);
      }
    }

    private io.grpc.stub.StreamObserver<etcdserverpb.Rpc.PutResponse> blockingObserver(
        Receipt receipt, CountDownLatch entered, CountDownLatch release,
        AtomicInteger completed, AtomicInteger errors) {
      return new io.grpc.stub.StreamObserver<etcdserverpb.Rpc.PutResponse>() {
        @Override public void onNext(etcdserverpb.Rpc.PutResponse value) {
          assertEquals(receipt.result().revision(), value.getHeader().getRevision());
          entered.countDown();
          try {
            if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("serializer stalled");
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("serializer interrupted", interrupted);
          }
        }
        @Override public void onError(Throwable failure) { errors.incrementAndGet(); }
        @Override public void onCompleted() { completed.incrementAndGet(); }
      };
    }

    private void assertTerminalFailuresDoNotDoubleComplete(Receipt receipt) throws Exception {
      for (boolean failOnNext : new boolean[] {true, false}) {
        RequestScope scope = RequestScope.withTimeout(TIMEOUT);
        try {
          BudgetManager.Account account = account(scope);
          var resolved = resolver.resolve(receipt.id(), receipt.requestDigest(), AUTHORIZATION,
              scope, account).join();
          AtomicInteger errors = new AtomicInteger();
          AtomicInteger completed = new AtomicInteger();
          var observer = new io.grpc.stub.StreamObserver<etcdserverpb.Rpc.PutResponse>() {
            @Override public void onNext(etcdserverpb.Rpc.PutResponse value) {
              if (failOnNext) throw new IllegalStateException("synthetic send failure");
            }
            @Override public void onError(Throwable failure) { errors.incrementAndGet(); }
            @Override public void onCompleted() {
              completed.incrementAndGet();
              throw new IllegalStateException("synthetic completion failure");
            }
          };
          var command = new PutCommand(KEY, new byte[] {2, (byte) 0xff}, 0, true, false, false);
          Runnable send = () -> PutResponseEmitter.send(command, resolved, ResultMessages::decodePut,
              fixture.clusterId(), 1, scope, account, observer);
          if (failOnNext) {
            send.run();
            assertEquals(1, errors.get());
            assertEquals(0, completed.get());
          } else {
            assertThrows(IllegalStateException.class, send::run);
            assertEquals(0, errors.get());
            assertEquals(1, completed.get());
          }
        } finally {
          closeScope(scope);
        }
      }
    }

    private void assertNettyCancellationWaitsForSerialization(Receipt receipt) throws Exception {
      M3NettyResponseProbe.verifyCancellationDuringSerialization((scope, response) -> {
        BudgetManager.Account account = account(scope);
        var resolved = resolver.resolve(receipt.id(), receipt.requestDigest(), AUTHORIZATION,
            scope, account).join();
        var command = new PutCommand(KEY, new byte[] {2, (byte) 0xff}, 0, true, false, false);
        PutResponseEmitter.send(command, resolved, ResultMessages::decodePut,
            fixture.clusterId(), 1, scope, account, response);
      }, () -> budgets.snapshot().allocatedBytes());
      assertEquals(0, budgets.snapshot().allocatedBytes());
      assertEquals(0, budgets.snapshot().reservedBytes());
    }

    private void assertPhysicalVersions() throws Exception {
      try (RequestScope scope = RequestScope.withTimeout(TIMEOUT)) {
        var store = fixture.openPrimary(scope);
        assertArrayEquals(new byte[] {1, 0}, store.get(KEY, 2, ReadProjection.FULL)
            .join().orElseThrow().value().orElseThrow());
        assertArrayEquals(new byte[] {2, (byte) 0xff}, store.get(KEY, 3, ReadProjection.FULL)
            .join().orElseThrow().value().orElseThrow());
        assertArrayEquals(new byte[] {3}, store.get(KEY, 4, ReadProjection.FULL)
            .join().orElseThrow().value().orElseThrow());
      }
    }

    private BudgetManager.Account account(RequestScope scope) throws Exception {
      return budgets.admit(fixture.system().tableGroupId(), scope, reservation());
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

    @Override
    public void close() {
      try {
        ownerQueue.close();
        ownerQueue.drained().join();
        try {
          if (!ownerQueue.awaitTermination(Duration.ofSeconds(10))) {
            throw new IllegalStateException("fixture owner queue did not terminate");
          }
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("fixture owner queue shutdown interrupted", interrupted);
        }
        handoff.close();
        handoff.drained().join();
      } finally {
        worker.shutdownNow();
        try {
          if (!worker.awaitTermination(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("fixture reader workers did not terminate");
          }
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("fixture reader shutdown interrupted", interrupted);
        }
      }
    }
  }
}
