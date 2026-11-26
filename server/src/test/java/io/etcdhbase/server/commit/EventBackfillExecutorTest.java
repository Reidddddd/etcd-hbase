package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.EventBackfillWindow;
import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class EventBackfillExecutorTest {
  @Test
  void rechecksOwnerAndProtectionBeforePublishingTheSealedCheckpoint() {
    try (var fixture = new Fixture()) {
      when(fixture.control.read(fixture.scope)).thenReturn(fixture.snapshot(fixture.state));
      when(fixture.publisher.publish(eq(fixture.sealed), eq(fixture.owner), eq(fixture.root),
          eq(fixture.scope), eq((CommitAuthorizationProof) fixture.publicationProof)))
          .thenReturn(CompletableFuture.completedFuture(fixture.finalized));
      assertEquals(fixture.finalized, fixture.executor.publish(fixture.invocation, fixture.owner,
          fixture.checkpoint, fixture.window, fixture.publicationProof,
          fixture.scope, fixture.account, fixture.budget).join());
      verify(fixture.control, times(2)).read(fixture.scope);
      verify(fixture.window, times(2)).checkPublication(fixture.checkpoint, 8, 1, fixture.budget);
      verify(fixture.publisher).publish(fixture.sealed, fixture.owner, fixture.root,
          fixture.scope, fixture.publicationProof);
    }
  }

  @Test
  void takeoverAfterSealingPreventsPublication() {
    try (var fixture = new Fixture()) {
      CommitState replacement = fixture.state.claimOwner("replacement", 100, 200);
      when(fixture.control.read(fixture.scope)).thenReturn(
          fixture.snapshot(fixture.state), fixture.snapshot(replacement));
      assertThrows(CompletionException.class, () -> fixture.executor.publish(fixture.invocation, fixture.owner,
          fixture.checkpoint, fixture.window, fixture.publicationProof,
          fixture.scope, fixture.account, fixture.budget).join());
      verify(fixture.publisher, never()).publish(any(), any(), any(), any(),
          any(CommitAuthorizationProof.class));
    }
  }

  @Test
  void lostProtectionAfterSealingPreventsPublication() {
    try (var fixture = new Fixture()) {
      when(fixture.control.read(fixture.scope)).thenReturn(fixture.snapshot(fixture.state));
      when(fixture.window.checkPublication(fixture.checkpoint, 8, 1, fixture.budget))
          .thenReturn(CompletableFuture.completedFuture(null),
              CompletableFuture.failedFuture(new IllegalStateException("protection lost")));
      assertThrows(CompletionException.class, () -> fixture.executor.publish(fixture.invocation, fixture.owner,
          fixture.checkpoint, fixture.window, fixture.publicationProof,
          fixture.scope, fixture.account, fixture.budget).join());
      verify(fixture.publisher, never()).publish(any(), any(), any(), any(),
          any(CommitAuthorizationProof.class));
    }
  }

  @Test
  void cancellationBeforePreparationPreventsPublication() {
    try (var fixture = new Fixture()) {
      fixture.scope.close();
      assertThrows(CompletionException.class, () -> fixture.executor.publish(fixture.invocation, fixture.owner,
          fixture.checkpoint, fixture.window, fixture.publicationProof,
          fixture.scope, fixture.account, fixture.budget).join());
      verify(fixture.control, never()).read(any());
      verify(fixture.publisher, never()).publish(any(), any(), any(), any(),
          any(CommitAuthorizationProof.class));
    }
  }

  @Test
  void mismatchedRegistryIdentityCannotProtectPublishedProgress() {
    try (var fixture = new Fixture()) {
      when(fixture.control.read(fixture.scope)).thenReturn(fixture.snapshot(fixture.state));
      byte[] otherRegistry = new byte[16];
      otherRegistry[0] = 2;
      var checkpoint = new EventIndexBackfillCheckpoint(7, 1, 1, 0, 1, 1, 1, 1, 0,
          ByteKey.copyOf(otherRegistry));
      assertThrows(CompletionException.class, () -> fixture.executor.publish(fixture.invocation, fixture.owner,
          checkpoint, fixture.window, fixture.publicationProof,
          fixture.scope, fixture.account, fixture.budget).join());
      verify(fixture.preparation, never()).prepare(any(), any(), any(), any(), any());
      verify(fixture.publisher, never()).publish(any(), any(), any(), any(),
          any(CommitAuthorizationProof.class));
    }
  }

  @Test
  void unselectedNamespaceCannotProtectPublishedProgress() {
    try (var fixture = new Fixture()) {
      when(fixture.control.read(fixture.scope)).thenReturn(fixture.snapshot(fixture.state));
      byte[] otherNamespace = new byte[16];
      otherNamespace[0] = 1;
      when(fixture.window.registryNamespace()).thenReturn(Optional.of(ByteKey.copyOf(otherNamespace)));
      assertThrows(CompletionException.class, () -> fixture.executor.publish(fixture.invocation, fixture.owner,
          fixture.checkpoint, fixture.window, fixture.publicationProof,
          fixture.scope, fixture.account, fixture.budget).join());
      verify(fixture.preparation, never()).prepare(any(), any(), any(), any(), any());
      verify(fixture.publisher, never()).publish(any(), any(), any(), any(),
          any(CommitAuthorizationProof.class));
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final ByteKey fingerprint = ByteKey.copyOf(new byte[32]);
    private final ByteKey namespace = ByteKey.copyOf(new byte[16]);
    private final ByteKey registry = ByteKey.copyOf(new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    private final ByteKey rootIdentity = ByteKey.copyOf(new byte[] {1});
    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
    private final BudgetManager.Account account = mock(BudgetManager.Account.class);
    private final CommitControlStore control = mock(CommitControlStore.class);
    private final EventBackfillPreparation preparation = mock(EventBackfillPreparation.class);
    private final EventBackfillWindow window = mock(EventBackfillWindow.class);
    private final CommitPublisher publisher = mock(CommitPublisher.class);
    private final CommitOwnerService.OwnerToken owner = new CommitOwnerService.OwnerToken("owner", 8);
    private final CommitState state = CommitState.initial(fingerprint, 1).claimOwner("owner", 10, 100)
        .upgradeAuxiliaryFormat("owner", 1, 11).upgradeLeaseBindingFormat("owner", 2, 12)
        .upgradeLeaseExpiryFormat("owner", 3, 13).upgradeEventIndexFormat("owner", 4, 14)
        .reserveEventRegistry("owner", 5, 15, namespace).upgradeEventRegistryFormat("owner", 6, 16, namespace)
        .enableAuthorizationBarrier("owner", 7, 17);
    private final InvocationBinding invocation = InvocationBinding.begin(InvocationId.create(), fingerprint,
        fingerprint, TransactionId.create(), 8, 0, 1);
    private final InvocationBinding sealed = invocation.seal(rootIdentity);
    private final EventIndexBackfillCheckpoint checkpoint = new EventIndexBackfillCheckpoint(8, 1, 1, 0,
        1, 1, 1, 1, 0, registry);
    private final ManifestRoot root = mock(ManifestRoot.class);
    private final ManifestCatalog.ProofReadBudget budget;
    private final CommitAuthorizationProof publicationProof = CommitAuthorizationProof.capture(
        ByteKey.copyOf(new byte[32]), 0, (identity, generation, scope) ->
            CompletableFuture.completedFuture(null));
    private final CommitOutcomeResolver.Resolution finalized;
    private final EventBackfillExecutor executor;

    private Fixture() {
      when(account.belongsTo(scope)).thenReturn(true);
      when(window.registryNamespace()).thenReturn(Optional.of(namespace));
      when(window.registryIdentity()).thenReturn(checkpoint.registryIdentity());
      budget = new ManifestCatalog.ProofReadBudget(scope, account, 10, 1L << 30);
      when(root.systemIdentity()).thenReturn(rootIdentity);
      var plan = new LeaseAuxiliaryMutationPreparation.Sealed(sealed, root);
      when(window.checkPublication(checkpoint, 8, 1, budget))
          .thenReturn(CompletableFuture.completedFuture(null));
      when(preparation.prepare(eq(invocation), eq(checkpoint), any(), eq(scope), eq(account)))
          .thenAnswer(call -> {
            Supplier<CompletableFuture<Void>> protection = call.getArgument(2);
            return protection.get().thenApply(ignored -> plan);
          });
      var decision = new CommitDecision(invocation.transactionId(), invocation.invocationId(),
          1, 1, 1, rootIdentity, fingerprint, false);
      finalized = new CommitOutcomeResolver.Resolution(CommitOutcomeResolver.State.SELECTED_FINALIZED,
          Optional.of(decision), Optional.empty());
      executor = new EventBackfillExecutor(control, preparation, publisher,
          Clock.fixed(Instant.ofEpochMilli(20), ZoneOffset.UTC));
    }

    private CompletableFuture<Optional<ControlSnapshot>> snapshot(CommitState value) {
      return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(value, new byte[] {1})));
    }

    @Override public void close() { scope.close(); }
  }
}
