package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Resolves possible publication outcomes from the control row and durable decision archive. */
public final class CommitOutcomeResolver {
  public enum State {
    SELECTED_PENDING,
    SELECTED_FINALIZED,
    NOT_SELECTED,
    INDETERMINATE,
    WINDOW_CLOSED
  }

  public record Resolution(
      State state, Optional<CommitDecision> decision, Optional<NotSelectedProof> notSelectedProof) {
    public Resolution {
      Objects.requireNonNull(state, "state");
      Objects.requireNonNull(decision, "decision");
      Objects.requireNonNull(notSelectedProof, "notSelectedProof");
      switch (state) {
        case SELECTED_PENDING, SELECTED_FINALIZED -> {
          if (decision.isEmpty() || notSelectedProof.isPresent()) {
            throw new IllegalArgumentException("selected result requires its durable decision");
          }
        }
        case NOT_SELECTED -> {
          if (notSelectedProof.isEmpty()) {
            throw new IllegalArgumentException("not-selected result requires durable proof");
          }
        }
        case INDETERMINATE, WINDOW_CLOSED -> {
          if (decision.isPresent() || notSelectedProof.isPresent()) {
            throw new IllegalArgumentException("unresolved result cannot carry a commit decision");
          }
        }
      }
    }
  }

  private final CommitControlStore control;
  private final CommitDecisionRepository decisions;

  public CommitOutcomeResolver(CommitControlStore control, CommitDecisionRepository decisions) {
    this.control = Objects.requireNonNull(control, "control");
    this.decisions = Objects.requireNonNull(decisions, "decisions");
  }

  public CompletableFuture<Resolution> resolve(InvocationBinding invocation, RequestScope scope) {
    Objects.requireNonNull(invocation, "invocation");
    if (invocation.state() == InvocationBinding.State.WINDOW_CLOSED) {
      return CompletableFuture.completedFuture(new Resolution(
          State.WINDOW_CLOSED, Optional.empty(), Optional.empty()));
    }
    return resolveDurable(invocation, scope);
  }

  /**
   * Proves a pre-preparation rejection while the caller still owns the serialized mutation lane.
   * The owner lane is required so no other request from this owner can select the empty sequence
   * between this control-row read and the caller's durable NOT_SELECTED transition.
   */
  public CompletableFuture<Resolution> proveNotSelectedForOwnerLane(
      InvocationBinding invocation, CommitOwnerService.OwnerToken owner, long nowEpochMillis,
      RequestScope scope) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(scope, "scope");
    if (invocation.state() != InvocationBinding.State.PREPARING
        || invocation.preparedOwnerEpoch() != owner.epoch() || nowEpochMillis < 0) {
      return CompletableFuture.completedFuture(indeterminate());
    }
    return control.read(scope).thenApply(snapshot -> {
      if (snapshot.isEmpty()) return indeterminate();
      CommitState state = snapshot.orElseThrow().state();
      boolean sameActiveOwner = state.ownerEpoch() == owner.epoch()
          && state.ownerInstance().filter(owner.instance()::equals).isPresent()
          && !state.ownerLeaseExpired(nowEpochMillis);
      boolean unchangedEmptySequence = state.publication().isEmpty()
          && state.publishedSequence() == invocation.baseSequence()
          && state.publishedRevision() == invocation.baseRevision();
      if (!sameActiveOwner || !unchangedEmptySequence) return indeterminate();
      var proof = new NotSelectedProof(invocation.preparedOwnerEpoch(), invocation.baseSequence(),
          state.ownerEpoch(), state.publishedSequence(), Optional.empty(),
          NotSelectedProof.Reason.OWNER_LANE_REJECTED_WITH_EMPTY_SLOT);
      return new Resolution(State.NOT_SELECTED, Optional.empty(), Optional.of(proof));
    });
  }

  /** Resolves durable publication state for recovery even after the caller query window closed. */
  CompletableFuture<Resolution> resolveForFinalization(
      InvocationBinding invocation, RequestScope scope) {
    Objects.requireNonNull(invocation, "invocation");
    return resolveDurable(invocation, scope);
  }

  private CompletableFuture<Resolution> resolveDurable(InvocationBinding invocation, RequestScope scope) {
    if (invocation.state() == InvocationBinding.State.NO_CHANGES) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("no-change results have no publication decision"));
    }
    if (invocation.state() == InvocationBinding.State.WINDOW_CLOSED) {
      // A closed result window still retains the immutable binding needed to finish an
      // already-selected publication. The public resolve() method hides it from callers.
      if (invocation.manifestRoot().isEmpty()) {
        return CompletableFuture.completedFuture(new Resolution(
            State.WINDOW_CLOSED, Optional.empty(), Optional.empty()));
      }
    }
    return control.read(scope).thenCompose(snapshot -> {
      if (snapshot.isEmpty()) {
        return CompletableFuture.failedFuture(
            new IllegalStateException("authoritative M2 control state is missing"));
      }
      CommitState state = snapshot.orElseThrow().state();
      long proposedSequence = Math.addExact(invocation.baseSequence(), 1);
      if (state.publication().isPresent()) {
        PublicationSlot slot = state.publication().orElseThrow();
        if (slot.sequence() == proposedSequence) {
          return resolveSlot(invocation, state, slot);
        }
      }
      if (state.publishedSequence() == invocation.baseSequence()) {
        if (state.publication().isPresent()) {
          return CompletableFuture.failedFuture(
              new IllegalStateException("control slot does not match its published sequence"));
        }
        if (state.publishedRevision() != invocation.baseRevision()) {
          return CompletableFuture.failedFuture(
              new IllegalStateException("invocation base revision conflicts with control state"));
        }
        if (state.ownerEpoch() < invocation.preparedOwnerEpoch()) {
          return CompletableFuture.failedFuture(
              new IllegalStateException("invocation was prepared under a future owner epoch"));
        }
        if (state.ownerEpoch() > invocation.preparedOwnerEpoch()) {
          var proof = new NotSelectedProof(invocation.preparedOwnerEpoch(), invocation.baseSequence(),
              state.ownerEpoch(), state.publishedSequence(), Optional.empty(),
              NotSelectedProof.Reason.OLD_OWNER_FENCED_WITH_EMPTY_SLOT);
          return CompletableFuture.completedFuture(new Resolution(
              State.NOT_SELECTED, Optional.empty(), Optional.of(proof)));
        }
        return CompletableFuture.completedFuture(indeterminate());
      }
      if (state.publishedSequence() < invocation.baseSequence()) {
        return CompletableFuture.completedFuture(indeterminate());
      }
      return decisions.read(proposedSequence, scope)
          .thenApply(decision -> resolveArchived(invocation, state, decision));
    });
  }

  private CompletableFuture<Resolution> resolveSlot(
      InvocationBinding invocation, CommitState state, PublicationSlot slot) {
    // The caller selected this slot by proposed sequence, and CommitState validates its sequence
    // and revision against the immutable snapshot. Keep the invocation-specific base revision
    // check because it joins two separately persisted records.
    if (slot.baseRevision() != invocation.baseRevision()) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("selected control slot conflicts with invocation base"));
    }
    if (slot.transactionId().equals(invocation.transactionId())) {
      if (slot.ownerEpoch() != invocation.preparedOwnerEpoch()
          || invocation.manifestRoot().isEmpty()
          || !slot.invocationId().equals(invocation.invocationId())
          || !slot.manifestRoot().equals(invocation.manifestRoot().orElseThrow())) {
        return CompletableFuture.failedFuture(
            new IllegalStateException("selected control slot conflicts with invocation binding"));
      }
      return CompletableFuture.completedFuture(new Resolution(
          State.SELECTED_PENDING, Optional.of(CommitDecision.from(slot)), Optional.empty()));
    }
    if (slot.ownerEpoch() < invocation.preparedOwnerEpoch()
        || slot.invocationId().equals(invocation.invocationId())
        || invocation.manifestRoot().filter(slot.manifestRoot()::equals).isPresent()) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("competing control slot conflicts with invocation identity"));
    }
    return CompletableFuture.completedFuture(notSelected(invocation, state, slot.transactionId()));
  }

  private Resolution resolveArchived(InvocationBinding invocation, CommitState state,
      Optional<CommitDecision> stored) {
    if (stored.isEmpty()) return indeterminate();
    CommitDecision decision = stored.orElseThrow();
    long proposedSequence = Math.addExact(invocation.baseSequence(), 1);
    if (decision.sequence() != proposedSequence
        || decision.baseRevision() != invocation.baseRevision()
        || state.publishedSequence() < decision.sequence()
        || state.publishedRevision() < decision.revision()) {
      throw new IllegalStateException("archived commit decision conflicts with control state");
    }
    if (!decision.transactionId().equals(invocation.transactionId())) {
      if (state.ownerEpoch() < invocation.preparedOwnerEpoch()
          || decision.invocationId().equals(invocation.invocationId())
          || invocation.manifestRoot().filter(decision.manifestRoot()::equals).isPresent()) {
        throw new IllegalStateException("competing decision conflicts with invocation identity");
      }
      var proof = new NotSelectedProof(invocation.preparedOwnerEpoch(), invocation.baseSequence(),
          state.ownerEpoch(), state.publishedSequence(),
          Optional.of(new NotSelectedProof.SelectedDecision(
              decision.sequence(), decision.transactionId())),
          NotSelectedProof.Reason.DIFFERENT_TRANSACTION_SELECTED);
      return new Resolution(State.NOT_SELECTED, Optional.of(decision), Optional.of(proof));
    }
    if (invocation.manifestRoot().isEmpty()
        || !decision.invocationId().equals(invocation.invocationId())
        || !decision.manifestRoot().equals(invocation.manifestRoot().orElseThrow())) {
      throw new IllegalStateException("archived decision conflicts with invocation binding");
    }
    return new Resolution(state.finalizedSequence() >= decision.sequence()
        ? State.SELECTED_FINALIZED : State.SELECTED_PENDING, Optional.of(decision), Optional.empty());
  }

  private static Resolution notSelected(
      InvocationBinding invocation, CommitState state, TransactionId selectedTransaction) {
    var selectedDecision = new NotSelectedProof.SelectedDecision(
        Math.addExact(invocation.baseSequence(), 1), selectedTransaction);
    var proof = new NotSelectedProof(invocation.preparedOwnerEpoch(), invocation.baseSequence(),
        state.ownerEpoch(), state.publishedSequence(), Optional.of(selectedDecision),
        NotSelectedProof.Reason.DIFFERENT_TRANSACTION_SELECTED);
    return new Resolution(State.NOT_SELECTED, Optional.empty(), Optional.of(proof));
  }

  private static Resolution indeterminate() {
    return new Resolution(State.INDETERMINATE, Optional.empty(), Optional.empty());
  }
}
