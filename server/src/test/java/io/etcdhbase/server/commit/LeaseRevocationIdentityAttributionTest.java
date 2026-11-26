package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.LeaseBindingReader;
import io.etcdhbase.core.commit.LeaseReadView;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.commit.PinnedReadView;
import io.etcdhbase.core.commit.ReadView;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseCostAdmission;
import io.etcdhbase.core.lease.LeaseBindingVersion;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.junit.jupiter.api.Test;

class LeaseRevocationIdentityAttributionTest {
  private static final String GROUP = "group";
  private static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);

  @Test
  void ownerRetiresUserOnPhysicalConnectionPermissionFailure() throws Exception {
    try (var fixture = new Fixture()) {
      when(fixture.connections.connectionFor(fixture.identity)).thenReturn(
          CompletableFuture.failedFuture(new AccessDeniedException("denied")));
      var reply = assertInstanceOf(OwnerForwarding.Unknown.class, fixture.execute().join());
      assertEquals(BackendFailure.Category.PERMISSION, reply.category());
      assertEquals(1, fixture.retirements.get());
    }
  }

  @Test
  void ownerRetiresUserOnPhysicalBoundKeyReadPermissionFailure() throws Exception {
    try (var fixture = new Fixture()) {
      var key = ByteKey.copyOf(new byte[] {1});
      var leaseIdentity = new LeaseIdentity(42, 1);
      var costs = new LeaseCostAdmission(LeaseCostAdmission.DEFAULT_LIMITS);
      var lease = LeaseState.grant(leaseIdentity, 30, 100_000, 2)
          .withBindingAccounting(3, 1, key.length(), costs.revokeCostBytes(key, 1));
      var binding = new LeaseBindingVersion(leaseIdentity, key, true, 3, 1, 1);
      when(fixture.pin.belongsTo(fixture.scope)).thenReturn(true);
      when(fixture.pin.protectsAuxiliary()).thenReturn(true);
      when(fixture.pin.isOpen()).thenReturn(true);
      when(fixture.leases.find(eq(fixture.pin), eq(42L), eq(fixture.scope), any()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(lease)));
      when(fixture.bindings.readBindings(eq(fixture.pin), eq(lease), eq(fixture.scope), any(), any()))
          .thenReturn(CompletableFuture.completedFuture(List.of(binding)));
      var connection = mock(AsyncConnection.class);
      AsyncTableBuilder<?> builder = mock(AsyncTableBuilder.class, RETURNS_SELF);
      AsyncTable<?> table = mock(AsyncTable.class);
      when(fixture.connections.connectionFor(fixture.identity)).thenReturn(
          CompletableFuture.completedFuture(connection));
      doReturn(builder).when(connection).getTableBuilder(fixture.primary.table());
      doReturn(table).when(builder).build();
      doReturn(CompletableFuture.failedFuture(new AccessDeniedException("denied")))
          .when(table).scanAll(any(Scan.class));
      var reply = assertInstanceOf(OwnerForwarding.Unknown.class, fixture.execute().join());
      assertEquals(BackendFailure.Category.PERMISSION, reply.category());
      assertEquals(1, fixture.retirements.get());
    }
  }

  @Test
  void controlPermissionFailureDoesNotRetireUser() throws Exception {
    try (var fixture = new Fixture()) {
      when(fixture.control.read(fixture.scope)).thenReturn(
          CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION)));
      var reply = assertInstanceOf(OwnerForwarding.Unknown.class, fixture.execute().join());
      assertEquals(BackendFailure.Category.PERMISSION, reply.category());
      assertEquals(0, fixture.retirements.get());
      verify(fixture.connections, never()).connectionFor(any());
    }
  }

  @Test
  void unavailablePhysicalConnectionDoesNotRetireUser() throws Exception {
    try (var fixture = new Fixture()) {
      when(fixture.connections.connectionFor(fixture.identity)).thenReturn(
          CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE)));
      var reply = assertInstanceOf(OwnerForwarding.Unknown.class, fixture.execute().join());
      assertEquals(BackendFailure.Category.UNAVAILABLE, reply.category());
      assertEquals(0, fixture.retirements.get());
    }
  }

  @Test
  void latePhysicalDenialAfterCancellationRetiresBeforeProducerDrain() throws Exception {
    try (var fixture = new Fixture()) {
      var connection = new CompletableFuture<AsyncConnection>();
      when(fixture.connections.connectionFor(fixture.identity)).thenReturn(connection);
      fixture.execute();
      fixture.scope.cancel();
      assertFalse(fixture.scope.physicalProducersCompletion().toCompletableFuture().isDone());
      connection.completeExceptionally(new AccessDeniedException("denied"));
      fixture.scope.cleanupCompletion().toCompletableFuture().get(2, TimeUnit.SECONDS);
      assertEquals(1, fixture.retirements.get());
    }
  }

  @Test
  void expiredKeepAliveRetiresUserOnPhysicalConnectionPermissionFailure() throws Exception {
    try (var fixture = new Fixture()) {
      when(fixture.connections.connectionFor(fixture.identity)).thenReturn(
          CompletableFuture.failedFuture(new AccessDeniedException("denied")));
      var reply = assertInstanceOf(OwnerForwarding.Unknown.class, fixture.executeExpiredKeepAlive().join());
      assertEquals(BackendFailure.Category.PERMISSION, reply.category());
      assertEquals(1, fixture.retirements.get());
    }
  }

  @Test
  void expiredKeepAliveDoesNotRetireUserForControlPermissionFailure() throws Exception {
    try (var fixture = new Fixture()) {
      when(fixture.control.read(fixture.scope)).thenReturn(
          CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION)));
      var reply = assertInstanceOf(OwnerForwarding.Unknown.class, fixture.executeExpiredKeepAlive().join());
      assertEquals(BackendFailure.Category.PERMISSION, reply.category());
      assertEquals(0, fixture.retirements.get());
      verify(fixture.connections, never()).connectionFor(any());
    }
  }

  @Test
  void expiredKeepAliveRetiresUserWhenCancelledConnectionFailsLate() throws Exception {
    try (var fixture = new Fixture()) {
      var connection = new CompletableFuture<AsyncConnection>();
      when(fixture.connections.connectionFor(fixture.identity)).thenReturn(connection);
      fixture.executeExpiredKeepAlive();
      fixture.scope.cancel();
      assertFalse(fixture.scope.physicalProducersCompletion().toCompletableFuture().isDone());
      connection.completeExceptionally(new AccessDeniedException("denied"));
      fixture.scope.cleanupCompletion().toCompletableFuture().get(2, TimeUnit.SECONDS);
      assertEquals(1, fixture.retirements.get());
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(30));
    private final AtomicInteger retirements = new AtomicInteger();
    private final CommitOwnerService.OwnerToken owner = new CommitOwnerService.OwnerToken("owner", 1);
    private final AuthorizedBackendIdentity identity = new AuthorizedBackendIdentity(
        "provider", "subject", "context", new AuthenticationScope(GROUP, "profile"), Set.of(GROUP), 0);
    private final CommitControlStore control = mock(CommitControlStore.class);
    private final ReadViewService views = mock(ReadViewService.class);
    private final LeaseReadView leases = mock(LeaseReadView.class);
    private final LeaseBindingReader bindings = mock(LeaseBindingReader.class);
    private final PinnedReadView pin = mock(PinnedReadView.class);
    private final HBaseConnectionFactory connections = mock(HBaseConnectionFactory.class);
    private final BudgetManager budgets;
    private final BudgetManager.Reservation reservation =
        new BudgetManager.Reservation(0, 1024, 1L << 20, 10L << 20, 1L << 20);
    private final StorageTableBinding primary = new StorageTableBinding(TableName.valueOf("synthetic"),
        new byte[] {'d'}, StorageTableBinding.Layout.HISTORY, new HashSaltStrategy());
    private final MutationForwardRequest request;
    private final InvocationBinding binding;
    private final MutationForwardReceiver.VerifiedAuthorization authorization;
    private final LeaseRevocationOwnerProcessor processor;
    private final LeaseRevocationExecutor revocations;

    private Fixture() {
      var limits = new BudgetManager.Limits(4, 0, 8, 2, 8, 0, 32L << 20);
      var reserve = new BudgetManager.ReservedCapacity(4, 0, 8, 2, 8, 0, 32L << 20);
      budgets = new BudgetManager(limits, Map.of(GROUP, limits), limits, reserve, Map.of(GROUP, reserve));
      long leaseId = 42;
      ByteKey operation = MutationOperationEnvelope.leaseRevoke(
          Rpc.LeaseRevokeRequest.newBuilder().setID(leaseId).build().toByteArray());
      var invocation = InvocationId.create();
      request = new MutationForwardRequest(GROUP, invocation,
          MutationForwardRequest.digest(GROUP, operation), DIGEST, ByteKey.copyOf(new byte[] {1}),
          owner.epoch(), TimeUnit.SECONDS.toNanos(10), MutationForwardRequest.MAX_HOPS, operation);
      binding = InvocationBinding.begin(invocation, request.requestDigest(), DIGEST,
          TransactionId.create(), owner.epoch(), 3, 9);
      authorization = new MutationForwardReceiver.VerifiedAuthorization(identity, DIGEST,
          mock(CommitAuthorizationProof.class), retirements::incrementAndGet);
      CommitState state = mock(CommitState.class);
      when(state.profileFingerprint()).thenReturn(DIGEST);
      when(state.supportsLeaseExpiryCheckpoints()).thenReturn(true);
      when(state.supportsAuxiliaryRecords()).thenReturn(true);
      when(state.ownerEpoch()).thenReturn(owner.epoch());
      when(state.ownerInstance()).thenReturn(Optional.of(owner.instance()));
      when(state.ownerLeaseExpired(anyLong())).thenReturn(false);
      when(state.publication()).thenReturn(Optional.empty());
      when(state.publishedSequence()).thenReturn(binding.baseSequence());
      when(state.publishedRevision()).thenReturn(binding.baseRevision());
      when(control.read(scope)).thenReturn(CompletableFuture.completedFuture(
          Optional.of(new ControlSnapshot(state, new byte[] {1}))));
      ReadView view = mock(ReadView.class);
      when(view.barrierSequence()).thenReturn(binding.baseSequence());
      when(view.targetRevision()).thenReturn(binding.baseRevision());
      when(view.selectedDecision()).thenReturn(Optional.empty());
      when(pin.view()).thenReturn(view);
      when(views.openAuxiliary(DIGEST, scope)).thenReturn(CompletableFuture.completedFuture(pin));
      when(leases.find(eq(pin), eq(leaseId), eq(scope), any())).thenReturn(
          CompletableFuture.completedFuture(Optional.of(LeaseState.grant(new LeaseIdentity(leaseId, 1),
              30, 100_000, binding.baseSequence()))));
      when(leases.finish(pin, scope)).thenReturn(CompletableFuture.completedFuture(null));
      revocations = new LeaseRevocationExecutor(GROUP, DIGEST, 1, 1, control, views,
          mock(SystemRecordStore.class), leases, bindings,
          mock(LeaseBindingMutationPlanner.class), new LeaseCostAdmission(LeaseCostAdmission.DEFAULT_LIMITS),
          mock(LeaseAuxiliaryMutationPreparation.class), mock(CommitPublisher.class), connections,
          primary, Runnable::run, Clock.systemUTC(), () -> new LeaseTimeObservation(1, 1, true),
          new RequestMetrics());
      processor = new LeaseRevocationOwnerProcessor(GROUP, () -> Optional.of(owner), budgets,
          reservation, revocations, mock(CommitOutcomeResolver.class), mock(InvocationService.class),
          Clock.systemUTC());
    }

    private CompletableFuture<OwnerForwarding.Reply> execute() {
      return processor.process(request, binding, authorization, scope);
    }

    private CompletableFuture<OwnerForwarding.Reply> executeExpiredKeepAlive() {
      long leaseId = 42;
      when(leases.expiryCheckpoint(eq(pin), eq(scope), any()))
          .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
      when(leases.find(eq(pin), eq(leaseId), eq(scope), any())).thenReturn(
          CompletableFuture.completedFuture(Optional.of(LeaseState.grant(
              new LeaseIdentity(leaseId, 1), 30, 999, 3))));
      var executor = new LeaseRenewalExecutor(GROUP, DIGEST, 1, 1, control, views, leases,
          () -> new LeaseTimeObservation(1_000, 1_000, true), Clock.systemUTC(),
          mock(LeaseRenewalPreparation.class), revocations, mock(CommitPublisher.class),
          new RequestMetrics(), 1);
      var renewalProcessor = new LeaseRenewalOwnerProcessor(GROUP, () -> Optional.of(owner), budgets,
          reservation, executor, mock(CommitOutcomeResolver.class), mock(InvocationService.class),
          Clock.systemUTC());
      ByteKey operation = MutationOperationEnvelope.leaseKeepAlive(
          Rpc.LeaseKeepAliveRequest.newBuilder().setID(leaseId).build().toByteArray());
      var invocation = InvocationId.create();
      var renewalRequest = new MutationForwardRequest(GROUP, invocation,
          MutationForwardRequest.digest(GROUP, operation), DIGEST, ByteKey.copyOf(new byte[] {1}),
          owner.epoch(), TimeUnit.SECONDS.toNanos(10), MutationForwardRequest.MAX_HOPS, operation);
      var renewalBinding = InvocationBinding.begin(invocation, renewalRequest.requestDigest(), DIGEST,
          TransactionId.create(), owner.epoch(), 3, 9);
      return renewalProcessor.process(renewalRequest, renewalBinding, authorization, scope);
    }

    @Override public void close() { scope.close(); }
  }
}
