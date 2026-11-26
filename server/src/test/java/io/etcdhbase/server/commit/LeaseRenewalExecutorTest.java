package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.LeaseReadView;
import io.etcdhbase.core.commit.PinnedReadView;
import io.etcdhbase.core.commit.ReadView;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class LeaseRenewalExecutorTest {
  @Test
  void expiredActiveLeaseUsesTheAtomicRevocationResultForKeepAlive() throws BackendFailure {
    try (Fixture fixture = new Fixture()) {
      when(fixture.revocations.executeExpiredKeepAlive(eq(fixture.leaseId),
          eq(fixture.lease), eq(fixture.binding), eq(fixture.owner),
          eq(fixture.identity), eq(fixture.publicationProof), eq(fixture.pin), eq(fixture.observation),
          eq(fixture.scope), eq(fixture.account), any(Runnable.class)))
          .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(
              fixture.binding.invocationId()));

      InvocationId completed = fixture.executor.executeNew(fixture.leaseId, fixture.binding,
          fixture.owner, fixture.identity, fixture.publicationProof,
          fixture.scope, fixture.account).join();

      assertEquals(fixture.binding.invocationId(), completed);
      verify(fixture.revocations).executeExpiredKeepAlive(eq(fixture.leaseId),
          eq(fixture.lease), eq(fixture.binding), eq(fixture.owner), eq(fixture.identity),
          eq(fixture.publicationProof), eq(fixture.pin), eq(fixture.observation), eq(fixture.scope),
          eq(fixture.account), any(Runnable.class));
      verifyNoInteractions(fixture.renewalPreparation, fixture.publisher);
    }
  }

  @Test
  void failedExpiryRevocationCannotTurnIntoASuccessfulKeepAliveResponse() throws BackendFailure {
    try (Fixture fixture = new Fixture()) {
      BackendFailure unknown = new BackendFailure(BackendFailure.Category.UNKNOWN);
      when(fixture.revocations.executeExpiredKeepAlive(anyLong(), any(), any(), any(), any(),
          any(CommitAuthorizationProof.class), any(), any(), any(), any(), any(Runnable.class)))
          .thenReturn(java.util.concurrent.CompletableFuture.failedFuture(unknown));

      CompletionException failure = assertThrows(CompletionException.class,
          () -> fixture.executor.executeNew(fixture.leaseId, fixture.binding,
              fixture.owner, fixture.identity, fixture.publicationProof,
              fixture.scope, fixture.account).join());

      assertSame(unknown, failure.getCause());
      verifyNoInteractions(fixture.renewalPreparation, fixture.publisher);
    }
  }

  private static final class Fixture implements AutoCloseable {
    private static final String TABLE_GROUP = "lease-renewal-test";
    private static final ByteKey PROFILE = ByteKey.copyOf(new byte[32]);
    private static final ByteKey AUTHORIZATION = ByteKey.copyOf(new byte[32]);

    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(30));
    private final BudgetManager budgets;
    private final BudgetManager.Account account;
    private final CommitOwnerService.OwnerToken owner =
        new CommitOwnerService.OwnerToken("owner-a", 7);
    private final long leaseId = 42;
    private final LeaseIdentity leaseIdentity = new LeaseIdentity(leaseId, 1);
    private final LeaseState lease = LeaseState.grant(leaseIdentity, 30, 9_999, 3);
    private final LeaseTimeObservation observation = new LeaseTimeObservation(10_000, 10_000, true);
    private final AuthorizedBackendIdentity identity = new AuthorizedBackendIdentity(
        "test-provider", "test-subject", "fixture-context",
        new AuthenticationScope(TABLE_GROUP, "test-profile"), Set.of(TABLE_GROUP), 0);
    private final CommitAuthorizationProof publicationProof =
        CommitAuthorizationProofTestSupport.alwaysValid(0);
    private final InvocationBinding binding = InvocationBinding.begin(InvocationId.create(),
        ByteKey.copyOf(new byte[32]), AUTHORIZATION,
        io.etcdhbase.core.commit.TransactionId.create(), owner.epoch(), 3, 9);
    private final CommitControlStore control = mock(CommitControlStore.class);
    private final ReadViewService views = mock(ReadViewService.class);
    private final LeaseReadView leases = mock(LeaseReadView.class);
    private final PinnedReadView pin = mock(PinnedReadView.class);
    private final LeaseRenewalPreparation renewalPreparation = mock(LeaseRenewalPreparation.class);
    private final LeaseRevocationExecutor revocations = mock(LeaseRevocationExecutor.class);
    private final CommitPublisher publisher = mock(CommitPublisher.class);
    private final LeaseRenewalExecutor executor;

    private Fixture() throws BackendFailure {
      BudgetManager.Limits limits = new BudgetManager.Limits(
          4, 0, 8, 2, 8, 0, 8L << 20);
      budgets = new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits);
      account = budgets.admit(TABLE_GROUP, scope,
          new BudgetManager.Reservation(0, 1024, 1L << 20, 1024, 1L << 20));

      CommitState state = mock(CommitState.class);
      when(state.profileFingerprint()).thenReturn(PROFILE);
      when(state.supportsAuxiliaryRecords()).thenReturn(true);
      when(state.ownerEpoch()).thenReturn(owner.epoch());
      when(state.ownerInstance()).thenReturn(Optional.of(owner.instance()));
      when(state.ownerLeaseExpired(anyLong())).thenReturn(false);
      when(state.publication()).thenReturn(Optional.empty());
      when(state.publishedSequence()).thenReturn(binding.baseSequence());
      when(state.publishedRevision()).thenReturn(binding.baseRevision());
      when(control.read(scope)).thenReturn(java.util.concurrent.CompletableFuture.completedFuture(
          Optional.of(new ControlSnapshot(state, new byte[] {1}))));

      ReadView readView = mock(ReadView.class);
      when(readView.barrierSequence()).thenReturn(binding.baseSequence());
      when(readView.targetRevision()).thenReturn(binding.baseRevision());
      when(readView.selectedDecision()).thenReturn(Optional.empty());
      when(pin.view()).thenReturn(readView);
      when(views.openAuxiliary(AUTHORIZATION, scope)).thenReturn(
          java.util.concurrent.CompletableFuture.completedFuture(pin));
      when(leases.expiryCheckpoint(eq(pin), eq(scope), any()))
          .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(Optional.empty()));
      when(leases.find(eq(pin), eq(leaseId), eq(scope), any()))
          .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(Optional.of(lease)));

      LeaseTimeSource time = () -> observation;
      executor = new LeaseRenewalExecutor(TABLE_GROUP, PROFILE, 100, 200,
          control, views, leases, time, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC),
          renewalPreparation, revocations, publisher, new RequestMetrics(), 4);
    }

    @Override
    public void close() {
      scope.close();
      account.close();
    }
  }
}
