package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.commit.ResultCatalog;
import io.etcdhbase.core.kv.ResultNode;
import io.etcdhbase.core.kv.ResultTree;
import io.etcdhbase.core.kv.ResultTree;
import io.etcdhbase.core.kv.TxnEvaluator;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.codec.ResultMessages;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class TxnGatewayAuthenticationTest {
  private static final long REVISION = 2;
  private static final String TABLE_GROUP = "auth-group";
  private static final String TOKEN = "opaque-token-from-authenticate";

  @Test
  void unauthenticatedRangeUsesServiceIdentityForOrdinaryProvider() {
    AuthorizedBackendIdentity serviceIdentity = identity("service-user");
    var observedIdentity = new AtomicReference<AuthorizedBackendIdentity>();
    var observerFailure = new AtomicReference<Throwable>();
    TxnReadExecutor reads = mock(TxnReadExecutor.class);
    when(reads.execute(any(), any(), any(), any(), any(), anyBoolean(), any())).thenAnswer(invocation -> {
      observedIdentity.set(invocation.getArgument(2));
      return CompletableFuture.failedFuture(
          new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    });

    var gateway = new TxnGateway(TABLE_GROUP, serviceIdentity, ByteKey.copyOf(new byte[32]),
        1, 1, 1 << 20, mock(io.etcdhbase.core.commit.CommitControlStore.class),
        mock(MutationRelay.class), mock(io.etcdhbase.core.commit.InvocationResultResolver.class),
        reads, budgets(), new BudgetManager.Reservation(1L << 16, 1L << 16, 1L << 16,
            1L << 16, 1L << 16), Runnable::run, new RequestMetrics());

    try (RequestScope scope = RequestScope.withoutDeadline()) {
      gateway.accept(rangeTxn(), Optional.empty(), scope, new StreamObserver<>() {
        @Override public void onNext(Rpc.TxnResponse response) { }
        @Override public void onError(Throwable failure) { observerFailure.set(failure); }
        @Override public void onCompleted() { }
      });

      assertSame(serviceIdentity, observedIdentity.get());
      var projectedFailure = assertInstanceOf(StatusRuntimeException.class, observerFailure.get());
      assertEquals(Status.Code.UNAVAILABLE, projectedFailure.getStatus().getCode());
    }
  }

  @Test
  void rangeUsesResolvedSessionIdentityAndReleasesOnlyAfterPhysicalReadDrains() throws Exception {
    var serviceIdentity = identity("service-user");
    var userIdentity = identity("milvus-user");
    var authorization = ByteKey.copyOf(new byte[32]);
    var requestAuthorization = AuthorizationScopeDigest.fromSessionRead(userIdentity, session());
    AuthSessionService sessions = mock(AuthSessionService.class);
    AuthSessionService.ResolvedIdentity resolved = mock(AuthSessionService.ResolvedIdentity.class);
    AtomicInteger releases = new AtomicInteger();
    when(resolved.identity()).thenReturn(userIdentity);
    when(resolved.session()).thenReturn(session());
    when(resolved.release()).thenAnswer(ignored -> {
      releases.incrementAndGet();
      return CompletableFuture.completedFuture(null);
    });
    when(sessions.resolve(eq(TOKEN), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(resolved));

    TxnReadExecutor reads = mock(TxnReadExecutor.class);
    var readResult = new CompletableFuture<io.etcdhbase.core.kv.TxnEvaluator.ValidatedRead>();
    var physicalRead = new CompletableFuture<Void>();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      when(reads.execute(any(), any(), any(), any(), any(), anyBoolean(), any())).thenAnswer(invocation -> {
        assertSame(userIdentity, invocation.getArgument(2));
        assertEquals(requestAuthorization, invocation.getArgument(1));
        assertSame(scope, invocation.getArgument(3));
        scope.trackPhysicalProducer(physicalRead);
        return readResult;
      });

      var budgets = budgets();
      var reservation = new BudgetManager.Reservation(1L << 16, 1L << 16, 1L << 16,
          1L << 16, 1L << 16);
      var gateway = new TxnGateway(TABLE_GROUP, serviceIdentity, authorization, 1, 1,
          1 << 20, mock(io.etcdhbase.core.commit.CommitControlStore.class),
          mock(MutationRelay.class), mock(io.etcdhbase.core.commit.InvocationResultResolver.class),
          reads, budgets, reservation, Runnable::run, new RequestMetrics(), sessions);

      gateway.accept(rangeTxn(), Optional.of(TOKEN), scope, ignoringObserver());

      verify(sessions).resolve(TOKEN, scope);
      assertEquals(0, releases.get());
      assertFalse(scope.physicalProducersCompletion().toCompletableFuture().isDone());

      scope.cancel();
      assertEquals(0, releases.get(), "cancel must not release the identity while HBase work is active");

      physicalRead.complete(null);
      readResult.completeExceptionally(new CancellationException("test read cancelled"));
      scope.cleanupCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS);
      assertEquals(1, releases.get());
    }
  }

  @Test
  void queuedReadRevalidatesBeforeDeliveryAndRejectsRevokedSession() throws Exception {
    var fixture = new ReadDeliveryFixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      fixture.start(scope);
      fixture.worker.runNext(); // Read completes, but delivery remains queued.
      assertEquals(0, fixture.responses.get());
      fixture.revalidation.completeExceptionally(
          new AuthSessionException(AuthSessionException.Reason.INVALID_TOKEN));
      fixture.worker.runNext();
      assertEquals(0, fixture.responses.get());
      assertEquals(Status.Code.UNAUTHENTICATED,
          assertInstanceOf(StatusRuntimeException.class, fixture.failure.get()).getStatus().getCode());
      verify(fixture.reads).invalidateAuthorization(
          AuthorizationScopeDigest.fromSessionRead(identity("milvus-user"), session()));
      verify(fixture.original).retire();
    }
  }

  @Test
  void queuedReadsOnBothGatewaysRejectSharedSessionRevocation() throws Exception {
    var first = new ReadDeliveryFixture();
    var second = new ReadDeliveryFixture();
    var sharedRevalidation = new CompletableFuture<AuthSessionService.ResolvedIdentity>();
    try (RequestScope firstScope = RequestScope.withTimeout(Duration.ofSeconds(5));
         RequestScope secondScope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      first.start(firstScope);
      second.start(secondScope);
      when(first.sessions.resolveByDigest(any(), any(RequestScope.class))).thenReturn(sharedRevalidation);
      when(second.sessions.resolveByDigest(any(), any(RequestScope.class))).thenReturn(sharedRevalidation);
      first.worker.runNext();
      second.worker.runNext();
      sharedRevalidation.completeExceptionally(
          new AuthSessionException(AuthSessionException.Reason.INVALID_TOKEN));
      first.worker.runNext();
      second.worker.runNext();

      var authorization = AuthorizationScopeDigest.fromSessionRead(identity("milvus-user"), session());
      for (var gateway : List.of(first, second)) {
        assertEquals(0, gateway.responses.get());
        assertEquals(Status.Code.UNAUTHENTICATED,
            assertInstanceOf(StatusRuntimeException.class, gateway.failure.get()).getStatus().getCode());
        verify(gateway.reads).invalidateAuthorization(authorization);
        verify(gateway.original).retire();
      }
    }
  }

  @Test
  void backendPermissionFailureRetiresIdentityWithoutWaitingForBorrowDrain() throws Exception {
    var fixture = new ReadDeliveryFixture();
    fixture.readFailure = new BackendFailure(BackendFailure.Category.PERMISSION);
    fixture.userReadRejection = true;
    var retirement = new CompletableFuture<Void>();
    when(fixture.original.retire()).thenReturn(retirement);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      fixture.start(scope);
      fixture.worker.runNext();
      verify(fixture.original).retire();
      verify(fixture.reads).invalidateAuthorization(
          AuthorizationScopeDigest.fromSessionRead(identity("milvus-user"), session()));
      assertFalse(retirement.isDone());
      fixture.worker.runNext();
      var rejected = assertInstanceOf(StatusRuntimeException.class, fixture.failure.get());
      assertEquals(Status.Code.PERMISSION_DENIED, rejected.getStatus().getCode());
    } finally {
      retirement.complete(null);
    }
  }

  @Test
  void controlPermissionFailureDoesNotRetireUserIdentity() throws Exception {
    var fixture = new ReadDeliveryFixture();
    fixture.readFailure = new BackendFailure(BackendFailure.Category.PERMISSION);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      fixture.start(scope);
      fixture.worker.runNext();
      fixture.worker.runNext();
      verify(fixture.original, never()).retire();
      verify(fixture.reads, never()).invalidateAuthorization(any());
      assertEquals(Status.Code.PERMISSION_DENIED,
          assertInstanceOf(StatusRuntimeException.class, fixture.failure.get()).getStatus().getCode());
    }
  }

  @Test
  void transientReadFailureDoesNotRetireIdentity() throws Exception {
    var fixture = new ReadDeliveryFixture();
    fixture.readFailure = new BackendFailure(BackendFailure.Category.UNAVAILABLE);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      fixture.start(scope);
      fixture.worker.runNext();
      fixture.worker.runNext();
      verify(fixture.original, never()).retire();
      verify(fixture.reads, never()).invalidateAuthorization(any());
      assertEquals(Status.Code.UNAVAILABLE,
          assertInstanceOf(StatusRuntimeException.class, fixture.failure.get()).getStatus().getCode());
    }
  }

  @Test
  void readDeliveryWaitsForRevalidationAndCancellationDrainsBeforeRelease() throws Exception {
    var fixture = new ReadDeliveryFixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      fixture.start(scope);
      fixture.worker.runNext();
      fixture.worker.runNext();
      assertEquals(0, fixture.responses.get());
      assertFalse(scope.physicalProducersCompletion().toCompletableFuture().isDone());
      scope.cancel();
      assertEquals(0, fixture.releases.get());
      fixture.revalidation.complete(fixture.current);
      scope.cleanupCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS);
      assertEquals(0, fixture.responses.get());
      assertEquals(2, fixture.releases.get(), "releases");
    }
  }

  @Test
  void readDeliverySucceedsAfterRevalidation() throws Exception {
    var fixture = new ReadDeliveryFixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      fixture.start(scope);
      fixture.worker.runNext();
      fixture.worker.runNext();
      assertEquals(0, fixture.responses.get());
      fixture.revalidation.complete(fixture.current);
      assertEquals(1, fixture.responses.get());
      assertTrue(fixture.completed.get());
      assertNull(fixture.failure.get());
    }
  }

  @Test
  void readViewIdentityChangesWithDurableAuthorizationGeneration() {
    var user = identity("milvus-user");
    var session = new AtomicReference<>(session(1));
    var resolved = mock(AuthSessionService.ResolvedIdentity.class);
    when(resolved.identity()).thenReturn(user);
    when(resolved.session()).thenAnswer(ignored -> session.get());
    when(resolved.release()).thenReturn(CompletableFuture.completedFuture(null));
    var sessions = mock(AuthSessionService.class);
    when(sessions.resolve(eq(TOKEN), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(resolved));
    var seen = new ArrayList<ByteKey>();
    var reads = mock(TxnReadExecutor.class);
    when(reads.execute(any(), any(), any(), any(), any(), anyBoolean(), any())).thenAnswer(call -> {
      seen.add(call.getArgument(1));
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    });
    var gateway = new TxnGateway(TABLE_GROUP, identity("service-user"), ByteKey.copyOf(new byte[32]),
        1, 1, 1 << 20, mock(io.etcdhbase.core.commit.CommitControlStore.class),
        mock(MutationRelay.class), mock(io.etcdhbase.core.commit.InvocationResultResolver.class), reads,
        budgets(), new BudgetManager.Reservation(1L << 16, 1L << 16, 1L << 16, 1L << 18, 1L << 16),
        Runnable::run, new RequestMetrics(), sessions);
    try (RequestScope first = RequestScope.withoutDeadline()) {
      gateway.accept(rangeTxn(), Optional.of(TOKEN), first, ignoringObserver());
    }
    session.set(session(2));
    try (RequestScope second = RequestScope.withoutDeadline()) {
      gateway.accept(rangeTxn(), Optional.of(TOKEN), second, ignoringObserver());
    }
    assertEquals(2, seen.size());
    assertNotEquals(seen.get(0), seen.get(1), "cache proofs must not cross authorization generations");
  }

  @Test
  void noChangeResolutionRevalidatesBeforeDeliveryAndDoesNotRetryTheTransaction() throws Exception {
    var fixture = new NoChangeResolutionFixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      fixture.start(scope, false);
      fixture.worker.runNext();
      fixture.forwarded.complete(null);
      fixture.worker.runNext();
      assertEquals(1, fixture.forwards.get());
      assertEquals(0, fixture.responses.get());

      fixture.revalidation.completeExceptionally(
          new AuthSessionException(AuthSessionException.Reason.PERMISSION_DENIED));

      assertEquals(1, fixture.forwards.get(), "a resolved durable result is never replayed or recomputed");
      assertEquals(0, fixture.responses.get());
      assertEquals(Status.Code.PERMISSION_DENIED,
          assertInstanceOf(StatusRuntimeException.class, fixture.failure.get()).getStatus().getCode());
      assertTrue(fixture.closed.get(), "rejected delivery releases the retained durable result");
      verify(fixture.reads).invalidateAuthorization(
          AuthorizationScopeDigest.fromSessionRead(identity("milvus-user"), session()));
      scope.cancel();
      scope.cleanupCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS);
      assertEquals(1, fixture.releases.get(),
          "a rejected recheck releases only the request identity");
    }
  }

  @Test
  void unchangedMutationResultStillDeliversAfterRevalidation() throws Exception {
    var fixture = new NoChangeResolutionFixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      fixture.start(scope, false);
      fixture.worker.runNext();
      fixture.forwarded.complete(null);
      fixture.worker.runNext();
      assertEquals(1, fixture.forwards.get());
      assertEquals(0, fixture.responses.get());

      fixture.revalidation.complete(fixture.current);

      assertEquals(1, fixture.forwards.get());
      assertEquals(1, fixture.responses.get(), String.valueOf(fixture.failure.get()));
      assertTrue(fixture.completed.get());
      assertNull(fixture.failure.get());
      assertTrue(fixture.closed.get());
      scope.cancel();
      scope.cleanupCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS);
      assertEquals(2, fixture.releases.get(),
          "the request identity and the identity resolved by the recheck both release after drain");
    }
  }

  @Test
  void changedTransactionResultKeepsItsDecidedAuthorizationWithoutRecheck() {
    var fixture = new NoChangeResolutionFixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      fixture.start(scope, true, true);
      fixture.worker.runNext();
      fixture.forwarded.complete(null);
      fixture.worker.runNext();
      assertEquals(1, fixture.responses.get(), String.valueOf(fixture.failure.get()));
      assertEquals(0, fixture.revalidations.get());
      assertTrue(fixture.closed.get());
    }
  }

  private static final class NoChangeResolutionFixture {
    private final QueueExecutor worker = new QueueExecutor();
    private final AuthSessionService sessions = mock(AuthSessionService.class);
    private final AuthSessionService.ResolvedIdentity original = mock(AuthSessionService.ResolvedIdentity.class);
    private final AuthSessionService.ResolvedIdentity current = mock(AuthSessionService.ResolvedIdentity.class);
    private final InvocationResultResolver resolver = mock(InvocationResultResolver.class);
    private final TxnReadExecutor reads = mock(TxnReadExecutor.class);
    private final CompletableFuture<AuthSessionService.ResolvedIdentity> revalidation = new CompletableFuture<>();
    private final CompletableFuture<Void> forwarded = new CompletableFuture<>();
    private final AtomicInteger forwards = new AtomicInteger();
    private final AtomicInteger revalidations = new AtomicInteger();
    private final AtomicInteger releases = new AtomicInteger();
    private final AtomicInteger responses = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean completed = new AtomicBoolean();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    private void start(RequestScope scope, boolean changed) {
      start(scope, true, changed);
    }

    private void start(RequestScope scope, boolean revalidate, boolean changed) {
      var user = identity("milvus-user");
      for (var resolved : List.of(original, current)) {
        when(resolved.identity()).thenReturn(user);
        when(resolved.session()).thenReturn(session());
        when(resolved.release()).thenAnswer(ignored -> {
          releases.incrementAndGet();
          return CompletableFuture.completedFuture(null);
        });
      }
      when(sessions.resolve(eq(TOKEN), any(RequestScope.class)))
          .thenReturn(CompletableFuture.completedFuture(original));
      when(sessions.resolveByDigest(any(), any(RequestScope.class))).thenAnswer(ignored -> {
        revalidations.incrementAndGet();
        return revalidation;
      });
      var state = CommitState.restore(ByteKey.copyOf(new byte[32]), Optional.of("owner"), 3,
          Long.MAX_VALUE, 2, 2, 2, 2, 2, Optional.empty(), 0);
      var control = mock(CommitControlStore.class);
      when(control.read(any(RequestScope.class)))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1}))));
      MutationOwnerLane lane = mock(MutationOwnerLane.class);
      when(lane.receive(any(), any(), any(RequestScope.class))).thenAnswer(call -> {
        forwards.incrementAndGet();
        byte[] invocation = ((MutationForwardRequest) call.getArgument(1)).invocationId().bytes();
        return forwarded.thenApply(ignored -> new OwnerForwarding.Completed(invocation));
      });
      var relay = new MutationRelay(control,
          (group, instance, epoch) -> CompletableFuture.completedFuture(
              new OwnerForwarding.Route("owner", 3, "fixture", "fixture-peer")),
          (selected, request, requestScope) -> lane.receive("fixture-peer", request, requestScope));
      var encoded = ResultMessages.encode(transactionResult(changed));
      var payload = mock(ResultCatalog.RetainedResult.class);
      when(payload.belongsTo(scope)).thenReturn(true);
      when(payload.retainForConsumer(any())).thenReturn(true);
      when(payload.buffer()).thenReturn(ByteBuffer.wrap(encoded));
      doAnswer(ignored -> {
        closed.set(true);
        return null;
      }).when(payload).close();
      when(resolver.resolveTransaction(any(), any(), any(), any(), any())).thenAnswer(ignored ->
          CompletableFuture.completedFuture(
              new InvocationResultResolver.ResolvedTransaction(REVISION, changed, payload)));
      var gateway = new TxnGateway(TABLE_GROUP, identity("service-user"), ByteKey.copyOf(new byte[32]),
          1, 1, 1 << 20, control, relay, resolver, reads,
          budgets(), new BudgetManager.Reservation(1L << 16, 1L << 16, 1L << 16, 1L << 18, 1L << 16),
          worker, new RequestMetrics(), sessions);
      var key = ByteString.copyFromUtf8("key");
      var put = Rpc.RequestOp.newBuilder().setRequestPut(Rpc.PutRequest.newBuilder()
          .setKey(key).setValue(ByteString.copyFromUtf8("v"))).build();
      var range = Rpc.RequestOp.newBuilder().setRequestRange(Rpc.RangeRequest.newBuilder()
          .setKey(key)).build();
      var absent = Rpc.Compare.newBuilder().setKey(key).setTarget(Rpc.Compare.CompareTarget.VERSION)
          .setResult(Rpc.Compare.CompareResult.EQUAL).setVersion(0).build();
      Rpc.TxnRequest request = changed
          ? Rpc.TxnRequest.newBuilder().addSuccess(put).build()
          : Rpc.TxnRequest.newBuilder().addCompare(absent).addSuccess(put).addFailure(range).build();
      gateway.accept(request, Optional.of(TOKEN), scope, new StreamObserver<>() {
        @Override public void onNext(Rpc.TxnResponse response) { responses.incrementAndGet(); }
        @Override public void onError(Throwable problem) { failure.set(problem); }
        @Override public void onCompleted() { completed.set(true); }
      });
    }
  }

  private static ResultTree transactionResult(boolean changed) {
    var header = new ResultNode.Header(true, REVISION);
    var key = ByteKey.copyOf("key".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    ResultNode child = changed
        ? new ResultNode.Put(0, header, key, Optional.empty())
        : new ResultNode.Range(0, header, List.of(), 0, false);
    return new ResultTree(new ResultNode.Txn(0, header, changed, List.of(child)));
  }

  private static final class ReadDeliveryFixture {
    private final QueueExecutor worker = new QueueExecutor();
    private final AuthSessionService sessions = mock(AuthSessionService.class);
    private final AuthSessionService.ResolvedIdentity original = mock(AuthSessionService.ResolvedIdentity.class);
    private final AuthSessionService.ResolvedIdentity current = mock(AuthSessionService.ResolvedIdentity.class);
    private final TxnReadExecutor reads = mock(TxnReadExecutor.class);
    private final CompletableFuture<AuthSessionService.ResolvedIdentity> revalidation = new CompletableFuture<>();
    private final AtomicInteger releases = new AtomicInteger();
    private final AtomicInteger responses = new AtomicInteger();
    private final AtomicBoolean completed = new AtomicBoolean();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private Throwable readFailure;
    private boolean userReadRejection;

    private void start(RequestScope scope) {
      var user = identity("milvus-user");
      for (var resolved : List.of(original, current)) {
        when(resolved.identity()).thenReturn(user);
        when(resolved.session()).thenReturn(session());
        when(resolved.release()).thenAnswer(ignored -> {
          releases.incrementAndGet();
          return CompletableFuture.completedFuture(null);
        });
      }
      when(sessions.resolve(eq(TOKEN), any(RequestScope.class)))
          .thenReturn(CompletableFuture.completedFuture(original));
      when(sessions.resolveByDigest(any(), any(RequestScope.class))).thenReturn(revalidation);
      var header = new ResultNode.Header(true, 1);
      var tree = new ResultTree(new ResultNode.Txn(0, header, true,
          List.of(new ResultNode.Range(0, header, List.of(), 0, false))));
      var read = mock(TxnEvaluator.ValidatedRead.class);
      when(read.belongsTo(scope)).thenReturn(true);
      when(read.result()).thenReturn(tree);
      when(read.revision()).thenReturn(1L);
      when(reads.execute(any(), any(), any(), any(), any(), anyBoolean(), any()))
          .thenAnswer(call -> {
            if (userReadRejection) ((Runnable) call.getArgument(6)).run();
            return readFailure == null ? CompletableFuture.completedFuture(read)
                : CompletableFuture.failedFuture(readFailure);
          });
      var gateway = new TxnGateway(TABLE_GROUP, identity("service-user"), ByteKey.copyOf(new byte[32]),
          1, 1, 1 << 20, mock(io.etcdhbase.core.commit.CommitControlStore.class),
          mock(MutationRelay.class), mock(io.etcdhbase.core.commit.InvocationResultResolver.class), reads,
          budgets(), new BudgetManager.Reservation(1L << 16, 1L << 16, 1L << 16, 1L << 18, 1L << 16),
          worker, new RequestMetrics(), sessions);
      gateway.accept(rangeTxn(), Optional.of(TOKEN), scope, new StreamObserver<>() {
        @Override public void onNext(Rpc.TxnResponse response) { responses.incrementAndGet(); }
        @Override public void onError(Throwable problem) { failure.set(problem); }
        @Override public void onCompleted() { completed.set(true); }
      });
    }
  }

  private static final class QueueExecutor implements Executor {
    private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
    @Override public void execute(Runnable task) { tasks.addLast(task); }
    private void runNext() { tasks.removeFirst().run(); }
  }

  private static AuthorizedBackendIdentity identity(String subject) {
    var scope = new AuthenticationScope(TABLE_GROUP, "profile");
    return new AuthorizedBackendIdentity("provider", subject, "context-" + subject,
        scope, Set.of(TABLE_GROUP), 1);
  }

  private static AuthSession session() {
    return session(1);
  }

  private static AuthSession session(long authorizationGeneration) {
    var envelope = new CredentialEnvelope("key-1", CredentialEnvelope.CURRENT_AAD_VERSION,
        new byte[CredentialEnvelope.NONCE_BYTES], new byte[CredentialEnvelope.TAG_BYTES]);
    return new AuthSession(ByteKey.copyOf(new byte[16]), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), "milvus", "provider",
        new AuthenticationScope(TABLE_GROUP, "profile"), ByteKey.copyOf(new byte[32]),
        1, authorizationGeneration, 1, 2, 0, 1, AuthSession.Status.ACTIVE, envelope);
  }

  private static Rpc.TxnRequest rangeTxn() {
    var range = Rpc.RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("key")).build();
    return Rpc.TxnRequest.newBuilder()
        .addSuccess(Rpc.RequestOp.newBuilder().setRequestRange(range))
        .build();
  }

  private static BudgetManager budgets() {
    var limits = new BudgetManager.Limits(2, 0, 2, 2, 2, 0, 4L << 20);
    return new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits);
  }

  private static StreamObserver<Rpc.TxnResponse> ignoringObserver() {
    return new StreamObserver<>() {
      @Override public void onNext(Rpc.TxnResponse response) { }
      @Override public void onError(Throwable failure) { }
      @Override public void onCompleted() { }
    };
  }
}
