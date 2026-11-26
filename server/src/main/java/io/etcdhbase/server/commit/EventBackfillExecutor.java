package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.EventBackfillWindow;
import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Publishes a checkpoint while the caller owns the serial owner lane and event window. */
final class EventBackfillExecutor {
  private final CommitControlStore control;
  private final EventBackfillPreparation preparation;
  private final CommitPublisher publisher;
  private final Clock clock;

  EventBackfillExecutor(CommitControlStore control, EventBackfillPreparation preparation,
      CommitPublisher publisher, Clock clock) {
    this.control = Objects.requireNonNull(control, "control");
    this.preparation = Objects.requireNonNull(preparation, "preparation");
    this.publisher = Objects.requireNonNull(publisher, "publisher");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  CompletableFuture<CommitOutcomeResolver.Resolution> publish(InvocationBinding invocation,
      CommitOwnerService.OwnerToken owner, EventIndexBackfillCheckpoint checkpoint,
      EventBackfillWindow window, CommitAuthorizationProof publicationProof,
      RequestScope scope, BudgetManager.Account account,
      ManifestCatalog.ProofReadBudget budget) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(checkpoint, "checkpoint");
    Objects.requireNonNull(window, "window");
    Objects.requireNonNull(publicationProof, "publicationProof");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(budget, "budget");
    if (!account.belongsTo(scope) || !budget.belongsTo(scope)
        || invocation.preparedOwnerEpoch() != owner.epoch()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("backfill execution scope or owner differs"));
    }
    return verifyOwner(invocation, owner, checkpoint, window, scope)
        .thenCompose(ignored -> preparation.prepare(invocation, checkpoint,
            () -> window.checkPublication(checkpoint, owner.epoch(), checkpoint.sequence(), budget), scope, account))
        .thenCompose(sealed -> window.checkPublication(checkpoint, owner.epoch(), checkpoint.sequence(), budget)
            .thenCompose(ignored -> verifyOwner(invocation, owner, checkpoint, window, scope))
            .thenCompose(ignored -> publisher.publish(
                sealed.invocation(), owner, sealed.manifest(), scope, publicationProof)));
  }

  private CompletableFuture<Void> verifyOwner(InvocationBinding invocation,
      CommitOwnerService.OwnerToken owner, EventIndexBackfillCheckpoint checkpoint,
      EventBackfillWindow window, RequestScope scope) {
    if (scope.isClosed() || scope.isDeadlineExceeded()) {
      return CompletableFuture.failedFuture(new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
    }
    return control.read(scope).thenAccept(found -> {
          var state = found.orElseThrow(() -> new IllegalStateException("backfill control row is missing")).state();
          if (!state.supportsEventRevisionIndex()
              || window.registryNamespace().isEmpty()
              || !state.eventRegistryNamespace().equals(window.registryNamespace())
              || !checkpoint.registryIdentity().equals(window.registryIdentity())
              || !state.supportsAuthorizationBarrier()
              || state.ownerEpoch() != owner.epoch()
              || state.ownerInstance().filter(owner.instance()::equals).isEmpty()
              || state.ownerLeaseExpired(clock.millis()) || state.publication().isPresent()
              || state.publishedSequence() != invocation.baseSequence()
              || state.finalizedSequence() != invocation.baseSequence()
              || state.publishedRevision() != invocation.baseRevision()) {
            throw new java.util.concurrent.CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
          }
        });
  }
}
