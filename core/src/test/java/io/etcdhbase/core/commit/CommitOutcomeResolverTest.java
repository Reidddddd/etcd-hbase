package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class CommitOutcomeResolverTest {
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);
  private static final ByteKey ATTEMPT_ROOT = ByteKey.copyOf(new byte[] {1});
  private static final ByteKey COMPETITOR_ROOT = ByteKey.copyOf(new byte[] {2});
  private static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);
  private static final InvocationId ATTEMPT_INVOCATION = id(1);
  private static final TransactionId ATTEMPT_TRANSACTION = transaction(1);
  private static final InvocationId COMPETITOR_INVOCATION = id(2);
  private static final TransactionId COMPETITOR_TRANSACTION = transaction(2);

  @Test
  void selectedControlSlotProvesTheCompetingSequence() {
    CommitState state = ownedState().publish(slot(1, 0, 1));

    var resolution = resolver(state, Optional.empty())
        .resolve(sealedAttempt(), RequestScope.withoutDeadline()).join();

    assertEquals(CommitOutcomeResolver.State.NOT_SELECTED, resolution.state());
    assertEquals(InvocationBinding.State.NOT_SELECTED,
        sealedAttempt().notSelected(resolution.notSelectedProof().orElseThrow()).state());
  }

  @Test
  void archivedDecisionProvesTheCompetingSequenceAfterTheSlotAdvances() {
    PublicationSlot selected = slot(1, 0, 1);
    CommitState state = ownedState().publish(selected).finalizePublication(
        selected.transactionId()).publish(slot(2, 1, 2));

    var resolution = resolver(state, Optional.of(CommitDecision.from(selected)))
        .resolve(sealedAttempt(), RequestScope.withoutDeadline()).join();

    assertEquals(CommitOutcomeResolver.State.NOT_SELECTED, resolution.state());
    assertEquals(InvocationBinding.State.NOT_SELECTED,
        sealedAttempt().notSelected(resolution.notSelectedProof().orElseThrow()).state());
  }

  @Test
  void selectedSlotStillMustMatchTheInvocationBaseRevision() {
    CommitState state = ownedState().publish(slot(1, 0, 1));
    InvocationBinding mismatchedBase = InvocationBinding.begin(ATTEMPT_INVOCATION, DIGEST, DIGEST,
        ATTEMPT_TRANSACTION, 1, 0, 2).seal(ATTEMPT_ROOT);

    assertThrows(CompletionException.class, () -> resolver(state, Optional.empty())
        .resolve(mismatchedBase, RequestScope.withoutDeadline()).join());
  }

  @Test
  void noChangeTerminalIsNotACommitPublicationOutcome() {
    var preparing = InvocationBinding.begin(ATTEMPT_INVOCATION, DIGEST, DIGEST,
        ATTEMPT_TRANSACTION, 1, 0, 1);
    var noChanges = preparing.completeWithoutChanges(ByteKey.copyOf(new byte[] {7}));
    var outcomeResolver = resolver(ownedState(), Optional.empty());
    var scope = RequestScope.withoutDeadline();
    assertThrows(CompletionException.class, () -> outcomeResolver.resolve(noChanges, scope).join());
    assertThrows(CompletionException.class,
        () -> outcomeResolver.resolveForFinalization(noChanges, scope).join());
    scope.close();
  }

  @Test
  void serializedOwnerCanPersistAProvenPrePreparationRejection() {
    InvocationBinding preparing = InvocationBinding.begin(ATTEMPT_INVOCATION, DIGEST, DIGEST,
        ATTEMPT_TRANSACTION, 1, 0, 1);
    var scope = RequestScope.withoutDeadline();
    var outcomeResolver = resolver(ownedState(), Optional.empty());

    var resolution = outcomeResolver.proveNotSelectedForOwnerLane(preparing,
        new CommitOwnerService.OwnerToken("owner", 1), 100, scope).join();

    assertEquals(CommitOutcomeResolver.State.NOT_SELECTED, resolution.state());
    NotSelectedProof proof = resolution.notSelectedProof().orElseThrow();
    InvocationBinding rejected = preparing.notSelected(proof);
    AtomicReference<InvocationBinding> stored = new AtomicReference<>(preparing);
    InvocationRepository repository = new InvocationRepository() {
      @Override
      public CompletableFuture<Optional<InvocationBinding>> read(InvocationId id, RequestScope ignored) {
        return CompletableFuture.completedFuture(Optional.ofNullable(stored.get()));
      }

      @Override
      public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
          InvocationBinding binding, RequestScope ignored) {
        throw new UnsupportedOperationException();
      }

      @Override
      public CompletableFuture<Boolean> compareAndSet(InvocationBinding expected,
          InvocationBinding replacement, RequestScope ignored) {
        return CompletableFuture.completedFuture(stored.compareAndSet(expected, replacement));
      }
    };

    InvocationBinding persisted = new InvocationService(repository)
        .transition(preparing, rejected, scope).join();

    assertEquals(InvocationBinding.State.NOT_SELECTED, persisted.state());
    assertEquals(InvocationBinding.State.NOT_SELECTED, stored.get().state());
    assertFalse(outcomeResolver.proveNotSelectedForOwnerLane(preparing,
        new CommitOwnerService.OwnerToken("other", 1), 100, scope).join().state()
        == CommitOutcomeResolver.State.NOT_SELECTED);
    scope.close();
  }

  private static CommitOutcomeResolver resolver(
      CommitState state, Optional<CommitDecision> selectedDecision) {
    return new CommitOutcomeResolver(new FixedControlStore(state),
        new FixedDecisionRepository(selectedDecision));
  }

  private static CommitState ownedState() {
    return CommitState.initial(FINGERPRINT, 1).claimOwner("owner", 0, 10_000);
  }

  private static InvocationBinding sealedAttempt() {
    return InvocationBinding.begin(ATTEMPT_INVOCATION, DIGEST, DIGEST,
        ATTEMPT_TRANSACTION, 1, 0, 1).seal(ATTEMPT_ROOT);
  }

  private static PublicationSlot slot(long sequence, long baseSequence, long baseRevision) {
    return new PublicationSlot(
        COMPETITOR_TRANSACTION, COMPETITOR_INVOCATION, 1, baseSequence, baseRevision,
        sequence, baseRevision + 1, true, COMPETITOR_ROOT, DIGEST);
  }

  private static InvocationId id(long value) {
    return new InvocationId(new UUID(0, value));
  }

  private static TransactionId transaction(long value) {
    return new TransactionId(new UUID(0, value));
  }

  private static final class FixedControlStore implements CommitControlStore {
    private final ControlSnapshot snapshot;

    private FixedControlStore(CommitState state) {
      snapshot = new ControlSnapshot(state, new byte[] {1});
    }

    @Override
    public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(snapshot));
    }

    @Override
    public CompletableFuture<Boolean> initializeIfAbsent(
        CommitState initialState, RequestScope scope) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class FixedDecisionRepository implements CommitDecisionRepository {
    private final Optional<CommitDecision> decision;

    private FixedDecisionRepository(Optional<CommitDecision> decision) {
      this.decision = decision;
    }

    @Override
    public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope) {
      return CompletableFuture.completedFuture(decision);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        CommitDecision proposed, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
  }
}
