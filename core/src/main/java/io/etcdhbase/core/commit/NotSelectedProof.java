package io.etcdhbase.core.commit;

import java.util.Objects;
import java.util.Optional;

/** Internal evidence that an attempt can no longer occupy its proposed sequence. */
public final class NotSelectedProof {
  public enum Reason {
    DIFFERENT_TRANSACTION_SELECTED,
    OLD_OWNER_FENCED_WITH_EMPTY_SLOT,
    OWNER_LANE_REJECTED_WITH_EMPTY_SLOT
  }

  record SelectedDecision(long sequence, TransactionId transactionId) {
    SelectedDecision {
      if (sequence <= 0) throw new IllegalArgumentException("selected sequence must be positive");
      Objects.requireNonNull(transactionId, "transactionId");
    }
  }

  private final long preparedOwnerEpoch;
  private final long baseSequence;
  private final Optional<SelectedDecision> selectedDecision;

  NotSelectedProof(long preparedOwnerEpoch, long baseSequence, long observedOwnerEpoch,
      long observedPublishedSequence, Optional<SelectedDecision> selectedDecision, Reason reason) {
    this.preparedOwnerEpoch = preparedOwnerEpoch;
    this.baseSequence = baseSequence;
    this.selectedDecision = Objects.requireNonNull(selectedDecision, "selectedDecision");
    Objects.requireNonNull(reason, "reason");
    if (preparedOwnerEpoch <= 0 || baseSequence < 0 || observedOwnerEpoch < preparedOwnerEpoch
        || observedPublishedSequence < baseSequence) {
      throw new IllegalArgumentException("invalid not-selected evidence");
    }
    if (reason == Reason.DIFFERENT_TRANSACTION_SELECTED
        && (observedPublishedSequence < Math.addExact(baseSequence, 1)
            || selectedDecision.isEmpty()
            || selectedDecision.orElseThrow().sequence() != Math.addExact(baseSequence, 1))) {
      throw new IllegalArgumentException("missing competing selection evidence");
    }
    if (reason == Reason.OLD_OWNER_FENCED_WITH_EMPTY_SLOT
        && (observedPublishedSequence != baseSequence || observedOwnerEpoch <= preparedOwnerEpoch
            || selectedDecision.isPresent())) {
      throw new IllegalArgumentException("old owner was not durably fenced");
    }
    if (reason == Reason.OWNER_LANE_REJECTED_WITH_EMPTY_SLOT
        && (observedPublishedSequence != baseSequence || observedOwnerEpoch != preparedOwnerEpoch
            || selectedDecision.isPresent())) {
      throw new IllegalArgumentException("owner lane did not observe its unchanged empty slot");
    }
  }

  boolean proves(InvocationBinding binding) {
    return preparedOwnerEpoch == binding.preparedOwnerEpoch()
        && baseSequence == binding.baseSequence()
        && (selectedDecision.isEmpty()
            || !selectedDecision.orElseThrow().transactionId().equals(binding.transactionId()));
  }
}
