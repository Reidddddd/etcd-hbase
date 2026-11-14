package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.request.RequestScope;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Persists NOT_SELECTED only after the owner lane proves the proposed sequence is still empty. */
final class LeaseOwnerRejectionRecorder {
  private final CommitOutcomeResolver outcomes;
  private final InvocationService invocations;
  private final CommitOwnerService.OwnerToken owner;
  private final Clock clock;

  LeaseOwnerRejectionRecorder(CommitOutcomeResolver outcomes, InvocationService invocations,
      CommitOwnerService.OwnerToken owner, Clock clock) {
    this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
    this.invocations = Objects.requireNonNull(invocations, "invocations");
    this.owner = Objects.requireNonNull(owner, "owner");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  CompletableFuture<Boolean> persist(InvocationBinding binding, RequestScope scope) {
    return outcomes.proveNotSelectedForOwnerLane(binding, owner, clock.millis(), scope)
        .thenCompose(resolution -> {
          if (resolution.state() != CommitOutcomeResolver.State.NOT_SELECTED) {
            return CompletableFuture.completedFuture(false);
          }
          InvocationBinding rejected = binding.notSelected(
              resolution.notSelectedProof().orElseThrow());
          return invocations.transition(binding, rejected, scope)
              .thenApply(stored -> stored.state() == InvocationBinding.State.NOT_SELECTED);
        });
  }
}
