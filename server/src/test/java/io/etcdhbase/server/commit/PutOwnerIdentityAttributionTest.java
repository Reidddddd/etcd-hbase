package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestRepository;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.commit.PublicationSlot;
import io.etcdhbase.core.commit.ReadViewPinRegistry;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.grpc.protocol.TxnRequestMapper;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.junit.jupiter.api.Test;

class PutOwnerIdentityAttributionTest {
  private static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);
  private static final String GROUP = "group";

  @Test
  void userHistoryReadDenialAfterCancellationRetiresBeforePhysicalDrain() throws Exception {
    try (var fixture = new Fixture()) {
      var backend = new CompletableFuture<List<Result>>();
      doReturn(backend).when(fixture.table).scanAll(any(Scan.class));
      var result = fixture.execute();
      fixture.scope.cancel();
      assertFalse(fixture.scope.physicalProducersCompletion().toCompletableFuture().isDone());
      assertEquals(0, fixture.retirements.get());
      backend.completeExceptionally(new AccessDeniedException("denied"));
      assertThrows(CompletionException.class, result::join);
      assertEquals(1, fixture.retirements.get());
      fixture.scope.cleanupCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS);
      assertTrue(fixture.scope.physicalProducersCompletion().toCompletableFuture().isDone());
    }
  }

  @Test
  void ownerProcessorReturnsUnknownPermissionAndRetiresTheUser() throws Exception {
    try (var fixture = new Fixture()) {
      doReturn(CompletableFuture.failedFuture(new AccessDeniedException("denied")))
          .when(fixture.table).scanAll(any(Scan.class));
      ByteKey operation = ByteKey.copyOf(Rpc.PutRequest.newBuilder()
          .setKey(ByteString.copyFrom(new byte[] {1})).setValue(ByteString.copyFrom(new byte[] {1}))
          .build().toByteArray());
      var invocation = InvocationId.create();
      var request = new MutationForwardRequest(GROUP, invocation,
          MutationForwardRequest.digest(GROUP, operation), DIGEST, ByteKey.copyOf(new byte[] {1}),
          fixture.owner.epoch(), TimeUnit.SECONDS.toNanos(5), MutationForwardRequest.MAX_HOPS, operation);
      var binding = InvocationBinding.begin(invocation, request.requestDigest(), DIGEST,
          TransactionId.create(), fixture.owner.epoch(), 0, 1);
      var authorization = new MutationForwardReceiver.VerifiedAuthorization(fixture.identity, DIGEST,
          mock(CommitAuthorizationProof.class), fixture.retirements::incrementAndGet);
      var processor = new PutOwnerProcessor(GROUP, fixture.owner, fixture.budgets,
          fixture.reservation, fixture.executor, mock(CommitOutcomeResolver.class));
      var unknown = assertInstanceOf(OwnerForwarding.Unknown.class,
          processor.process(request, binding, authorization, fixture.scope).join());
      assertEquals(BackendFailure.Category.PERMISSION, unknown.category());
      assertEquals(1, fixture.retirements.get());
    }
  }

  @Test
  void backendControlPermissionFailureDoesNotRetireTheUser() throws Exception {
    try (var fixture = new Fixture()) {
      doReturn(CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION)))
          .when(fixture.control).read(any(RequestScope.class));
      var failure = assertThrows(CompletionException.class, () -> fixture.execute().join());
      assertEquals(BackendFailure.Category.PERMISSION, ((BackendFailure) failure.getCause()).category());
      assertEquals(0, fixture.retirements.get());
      verify(fixture.factory, never()).connectionFor(any());
    }
  }

  @Test
  void transactionUserDataDenialRetiresTheUser() throws Exception {
    try (var fixture = new Fixture()) {
      doReturn(CompletableFuture.failedFuture(new AccessDeniedException("denied")))
          .when(fixture.table).scanAll(any(Scan.class));
      var records = mock(SystemRecordStore.class);
      doReturn(CompletableFuture.completedFuture(SystemRecordStore.CreateResult.CREATED))
          .when(records).createIfAbsent(any(), any(), any(), any());
      var executor = new TxnMutationExecutor(GROUP, DIGEST, fixture.control, fixture.views,
          fixture.manifests, records, mock(ManifestRepository.class),
          fixture.factory, fixture.primary, Runnable::run, mock(ScanScheduler.class),
          mock(TxnCommitPreparation.class), mock(CommitPublisher.class),
          new TxnExecutionLimits(4096, 1L << 20, 1L << 20, 4096, 64));
      var binding = InvocationBinding.begin(InvocationId.create(), DIGEST, DIGEST,
          TransactionId.create(), fixture.owner.epoch(), 0, 1);
      var put = new PutCommand(ByteKey.copyOf(new byte[] {1}), new byte[] {1}, 0,
          false, false, false);
      var command = new TxnCommand(List.of(), List.of(new TxnCommand.Put(put)), List.of());
      var failure = assertThrows(CompletionException.class, () -> executor.executeNew(
          command, binding, fixture.owner, fixture.identity, mock(CommitAuthorizationProof.class),
          fixture.scope, fixture.account, fixture.retirements::incrementAndGet).join());
      assertEquals(BackendFailure.Category.PERMISSION, ((BackendFailure) failure.getCause()).category());
      assertEquals(1, fixture.retirements.get());
    }
  }

  @Test
  void transactionOwnerReturnsUnknownPermissionAndRetiresTheUser() throws Exception {
    try (var fixture = new Fixture()) {
      doReturn(CompletableFuture.failedFuture(new AccessDeniedException("denied")))
          .when(fixture.table).scanAll(any(Scan.class));
      var records = mock(SystemRecordStore.class);
      doReturn(CompletableFuture.completedFuture(SystemRecordStore.CreateResult.CREATED))
          .when(records).createIfAbsent(any(), any(), any(), any());
      var executor = new TxnMutationExecutor(GROUP, DIGEST, fixture.control, fixture.views,
          fixture.manifests, records, mock(ManifestRepository.class), fixture.factory, fixture.primary,
          Runnable::run, mock(ScanScheduler.class), mock(TxnCommitPreparation.class),
          mock(CommitPublisher.class), new TxnExecutionLimits(4096, 1L << 20, 1L << 20, 4096, 64));
      var put = Rpc.PutRequest.newBuilder().setKey(ByteString.copyFrom(new byte[] {1}))
          .setValue(ByteString.copyFrom(new byte[] {1}));
      var transaction = Rpc.TxnRequest.newBuilder()
          .addSuccess(Rpc.RequestOp.newBuilder().setRequestPut(put)).build();
      ByteKey operation = MutationOperationEnvelope.transaction(transaction.toByteArray());
      var invocation = InvocationId.create();
      var request = new MutationForwardRequest(GROUP, invocation,
          MutationForwardRequest.digest(GROUP, operation), DIGEST, ByteKey.copyOf(new byte[] {1}),
          fixture.owner.epoch(), TimeUnit.SECONDS.toNanos(5), MutationForwardRequest.MAX_HOPS, operation);
      var binding = InvocationBinding.begin(invocation, request.requestDigest(), DIGEST,
          TransactionId.create(), fixture.owner.epoch(), 0, 1);
      var authorization = new MutationForwardReceiver.VerifiedAuthorization(fixture.identity, DIGEST,
          mock(CommitAuthorizationProof.class), fixture.retirements::incrementAndGet);
      var processor = new TxnOwnerProcessor(GROUP, fixture.owner, fixture.budgets, fixture.reservation,
          executor, mock(CommitOutcomeResolver.class), 1_572_864);
      var unknown = assertInstanceOf(OwnerForwarding.Unknown.class,
          processor.process(request, binding, authorization, fixture.scope).join());
      assertEquals(BackendFailure.Category.PERMISSION, unknown.category());
      assertEquals(1, fixture.retirements.get());
    }
  }

  @Test
  void transactionReadUserDenialAfterCancellationRetiresIdentity() throws Exception {
    try (var fixture = new Fixture()) {
      var backend = new CompletableFuture<List<Result>>();
      doReturn(backend).when(fixture.table).scanAll(any(Scan.class));
      var result = fixture.executeRead();
      fixture.scope.cancel();
      assertFalse(fixture.scope.physicalProducersCompletion().toCompletableFuture().isDone());
      backend.completeExceptionally(new AccessDeniedException("denied"));
      assertThrows(CompletionException.class, result::join);
      assertEquals(1, fixture.retirements.get());
      fixture.scope.cleanupCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS);
    }
  }

  @Test
  void transactionReadControlDenialDoesNotRetireUser() throws Exception {
    try (var fixture = new Fixture()) {
      doReturn(CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION)))
          .when(fixture.control).read(any(RequestScope.class));
      var failure = assertThrows(CompletionException.class, () -> fixture.executeRead().join());
      assertEquals(BackendFailure.Category.PERMISSION, ((BackendFailure) failure.getCause()).category());
      assertEquals(0, fixture.retirements.get());
    }
  }

  @Test
  void transactionReadManifestDenialDoesNotRetireUser() throws Exception {
    try (var fixture = new Fixture()) {
      var slot = new PublicationSlot(TransactionId.create(), InvocationId.create(), 1, 0, 1,
          1, 2, true, ByteKey.copyOf(new byte[] {1}), DIGEST);
      var state = CommitState.restore(DIGEST, Optional.of("owner"), 1, Long.MAX_VALUE,
          2, 1, 2, 0, 1, Optional.of(slot), 0);
      doReturn(CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1}))))
          .when(fixture.control).read(any(RequestScope.class));
      doReturn(CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION)))
          .when(fixture.manifests).readRoot(any(), any(RequestScope.class));
      var failure = assertThrows(CompletionException.class, () -> fixture.executeRead().join());
      assertEquals(BackendFailure.Category.PERMISSION, ((BackendFailure) failure.getCause()).category());
      assertEquals(0, fixture.retirements.get());
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final RequestScope scope = RequestScope.withoutDeadline();
    private final AtomicInteger retirements = new AtomicInteger();
    private final CommitControlStore control = mock(CommitControlStore.class);
    private final HBaseConnectionFactory factory = mock(HBaseConnectionFactory.class);
    private final AsyncTable<?> table = mock(AsyncTable.class);
    private final AuthorizedBackendIdentity identity = new AuthorizedBackendIdentity(
        "provider", "subject", "context", new AuthenticationScope(GROUP, "profile"),
        Set.of(GROUP), 0);
    private final CommitOwnerService.OwnerToken owner = new CommitOwnerService.OwnerToken("owner", 1);
    private final StorageTableBinding primary = new StorageTableBinding(
        TableName.valueOf("synthetic"), new byte[] {'d'}, StorageTableBinding.Layout.HISTORY,
        new HashSaltStrategy());
    private final BudgetManager.Account account;
    private final BudgetManager budgets;
    private final BudgetManager.Reservation reservation;
    private final PutMutationExecutor executor;
    private final ManifestCatalog manifests;
    private final ReadViewService views;

    private Fixture() throws Exception {
      var state = CommitState.initial(DIGEST, 1).claimOwner("owner", 0, Long.MAX_VALUE);
      doReturn(CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1}))))
          .when(control).read(any(RequestScope.class));
      manifests = mock(ManifestCatalog.class);
      ReadViewPinRegistry pins = (view, request) -> CompletableFuture.completedFuture(
          new ReadViewPinRegistry.Pin() {
            @Override public long generation() { return 1; }
            @Override public void close() { }
          });
      views = new ReadViewService(control, manifests, pins, DIGEST);
      var connection = mock(AsyncConnection.class);
      AsyncTableBuilder<?> builder = mock(AsyncTableBuilder.class, RETURNS_SELF);
      doReturn(CompletableFuture.completedFuture(connection)).when(factory).connectionFor(identity);
      doReturn(builder).when(connection).getTableBuilder(primary.table());
      doReturn(table).when(builder).build();
      var limits = new BudgetManager.Limits(4, 4, 8, 4, 8, 1L << 20, 32L << 20);
      budgets = new BudgetManager(limits, Map.of(GROUP, limits), limits);
      reservation = new BudgetManager.Reservation(1L << 20, 1L << 20, 1L << 20,
          4L << 20, 1L << 20);
      account = budgets.admit(GROUP, scope, reservation);
      executor = new PutMutationExecutor(GROUP, DIGEST, control, views, manifests, factory,
          primary, Runnable::run, mock(PutCommitPreparation.class), mock(CommitPublisher.class),
          1L << 20, 1L << 20);
    }

    private CompletableFuture<?> executeRead() {
      var range = Rpc.RangeRequest.newBuilder().setKey(ByteString.copyFrom(new byte[] {1}));
      var request = Rpc.TxnRequest.newBuilder()
          .addSuccess(Rpc.RequestOp.newBuilder().setRequestRange(range)).build();
      var readExecutor = new TxnReadExecutor(GROUP, views, manifests, factory, primary,
          Runnable::run, mock(ScanScheduler.class),
          new TxnExecutionLimits(4096, 1L << 20, 1L << 20, 4096, 64));
      return readExecutor.execute(TxnRequestMapper.fromWire(request), DIGEST, identity,
          scope, account, true, retirements::incrementAndGet);
    }

    private CompletableFuture<InvocationId> execute() {
      var binding = InvocationBinding.begin(InvocationId.create(), DIGEST, DIGEST,
          TransactionId.create(), owner.epoch(), 0, 1);
      var command = new PutCommand(ByteKey.copyOf(new byte[] {1}), new byte[] {1}, 0,
          false, false, false);
      return executor.executeNew(command, binding, owner, identity, mock(CommitAuthorizationProof.class),
          scope, account, retirements::incrementAndGet);
    }

    @Override public void close() { scope.close(); }
  }
}
