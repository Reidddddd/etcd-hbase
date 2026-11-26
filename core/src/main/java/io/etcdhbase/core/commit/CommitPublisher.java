package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Publishes a sealed manifest at the control-row CAS linearization point. */
public final class CommitPublisher {
  private static final int MAX_CAS_ATTEMPTS = 32;

  private final CommitControlStore control;
  private final CommitOutcomeResolver outcomes;
  private final CommitPlanVerifier planVerifier;
  private final InvocationRepository invocations;
  private final PublicationHandoff handoff;
  private final CommitFinalizer finalizer;

  public CommitPublisher(
      CommitControlStore control,
      CommitOutcomeResolver outcomes,
      CommitPlanVerifier planVerifier,
      InvocationRepository invocations,
      PublicationHandoff handoff,
      CommitFinalizer finalizer) {
    this.control = Objects.requireNonNull(control, "control");
    this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
    this.planVerifier = Objects.requireNonNull(planVerifier, "planVerifier");
    this.invocations = Objects.requireNonNull(invocations, "invocations");
    this.handoff = Objects.requireNonNull(handoff, "handoff");
    this.finalizer = Objects.requireNonNull(finalizer, "finalizer");
  }

  /**
   * Publishes with a captured proof that is revalidated before every new selection CAS. Returns
   * only an observation: a selected result is not an etcd success until finalization completes.
   */
  public CompletableFuture<CommitOutcomeResolver.Resolution> publish(
      InvocationBinding invocation,
      CommitOwnerService.OwnerToken owner,
      ManifestRoot manifest,
      RequestScope requestScope,
      CommitAuthorizationProof authorization) {
    return publish(invocation, owner, manifest, requestScope, authorization, () -> {});
  }

  /**
   * Runs the local selection condition and revalidates authorization immediately before each new
   * selection attempt. Neither check runs while resolving an already selected invocation.
   */
  public CompletableFuture<CommitOutcomeResolver.Resolution> publish(
      InvocationBinding invocation,
      CommitOwnerService.OwnerToken owner,
      ManifestRoot manifest,
      RequestScope requestScope,
      CommitAuthorizationProof authorization,
      Runnable beforeSelection) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(manifest, "manifest");
    Objects.requireNonNull(requestScope, "requestScope");
    Objects.requireNonNull(authorization, "authorization");
    Objects.requireNonNull(beforeSelection, "beforeSelection");
    if (invocation.preparedOwnerEpoch() != owner.epoch()) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("owner token does not match the sealed invocation"));
    }
    return planVerifier.verifyForPublication(invocation, manifest, requestScope).thenCompose(verified ->
        invocations.read(invocation.invocationId(), requestScope).thenCompose(found -> {
          InvocationBinding durable = found.orElseThrow(() ->
              new IllegalStateException("sealed invocation was not persisted"));
          if (!durable.sameAttempt(invocation) || durable.state() != InvocationBinding.State.SEALED
              || !durable.manifestRoot().equals(invocation.manifestRoot())
              || !durable.resultReference().equals(invocation.resultReference())) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("durable invocation does not own the sealed plan"));
          }
          return handoff.transfer(durable, requestScope, serviceScope ->
              attempt(durable, owner, verified, serviceScope, authorization, beforeSelection, 0)
                  .thenCompose(publication -> finalizer.finish(durable, publication, serviceScope)));
        }))
        .exceptionallyCompose(CommitPublisher::failed);
  }

  private CompletableFuture<CommitOutcomeResolver.Resolution> attempt(
      InvocationBinding invocation,
      CommitOwnerService.OwnerToken owner,
      CommitPlanVerifier.VerifiedPlan verified,
      RequestScope scope,
      CommitAuthorizationProof authorization,
      Runnable beforeSelection,
      int attempt) {
    if (attempt >= MAX_CAS_ATTEMPTS) {
      return outcomes.resolve(invocation, scope);
    }
    return control.read(scope).thenCompose(snapshot -> {
      if (snapshot.isEmpty()) {
        return CompletableFuture.failedFuture(
            new IllegalStateException("authoritative M2 control state is missing"));
      }
      ControlSnapshot observed = snapshot.orElseThrow();
      CommitState state = observed.state();
      ManifestRoot manifest = verified.root();
      if (verified.backfillCompactRevision().isPresent()
          && verified.backfillCompactRevision().getAsLong() != state.compactRevision()) {
        return CompletableFuture.failedFuture(new IllegalStateException(
            "event backfill compact boundary changed before publication"));
      }
      if (manifest.auxiliaryReference().isPresent() && !state.supportsAuxiliaryRecords()) {
        return CompletableFuture.failedFuture(new IllegalStateException(
            "auxiliary publication requires the fenced control format"));
      }
      if (state.ownerEpoch() != owner.epoch()
          || state.ownerInstance().filter(owner.instance()::equals).isEmpty()
          || state.publishedSequence() != invocation.baseSequence()
          || state.publishedRevision() != invocation.baseRevision()
          || state.publication().isPresent()) {
        return outcomes.resolve(invocation, scope);
      }
      if (!state.supportsAuthorizationBarrier()) {
        return CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      if (scope.isDeadlineExceeded() || scope.isClosed()) {
        return CompletableFuture.failedFuture(scopeFailure(scope));
      }
      return authorization.reauthorize(state.authorizationBarrierGeneration(), scope)
          .thenCompose(reauthorized -> {
            if (scope.isDeadlineExceeded() || scope.isClosed()) {
              return CompletableFuture.failedFuture(scopeFailure(scope));
            }
            if (!reauthorized.proofIdentity().equals(authorization.proofIdentity())
                || reauthorized.authorizationBarrierGeneration()
                    != state.authorizationBarrierGeneration()) {
              return CompletableFuture.failedFuture(
                  new IllegalStateException("authorization proof changed during revalidation"));
            }
            return select(invocation, owner, verified, scope, reauthorized, beforeSelection,
                attempt, observed, state);
          });
    });
  }

  private CompletableFuture<CommitOutcomeResolver.Resolution> select(
      InvocationBinding invocation,
      CommitOwnerService.OwnerToken owner,
      CommitPlanVerifier.VerifiedPlan verified,
      RequestScope scope,
      CommitAuthorizationProof authorization,
      Runnable beforeSelection,
      int attempt,
      ControlSnapshot observed,
      CommitState state) {
    if (!state.supportsAuthorizationBarrier()
        || authorization.authorizationBarrierGeneration() != state.authorizationBarrierGeneration()) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    ManifestRoot manifest = verified.root();
    PublicationSlot slot = new PublicationSlot(invocation.transactionId(), invocation.invocationId(),
        owner.epoch(), invocation.baseSequence(), invocation.baseRevision(),
        Math.addExact(invocation.baseSequence(), 1),
        Math.addExact(invocation.baseRevision(), manifest.containsKeyChanges() ? 1 : 0),
        manifest.containsKeyChanges(), manifest.systemIdentity(), manifest.digest());
    CommitState selected = state.publish(slot);
    beforeSelection.run();
    return control.compareAndSet(observed, selected, scope)
        .handle((updated, failure) -> new CasResult(updated, failure))
        .thenCompose(result -> resolveAttempt(invocation, owner, verified, scope, authorization,
            beforeSelection, attempt, result));
  }

  private CompletableFuture<CommitOutcomeResolver.Resolution> resolveAttempt(
      InvocationBinding invocation,
      CommitOwnerService.OwnerToken owner,
      CommitPlanVerifier.VerifiedPlan verified,
      RequestScope scope,
      CommitAuthorizationProof authorization,
      Runnable beforeSelection,
      int attempt,
      CasResult result) {
    if (result.failure() != null) {
      Throwable failure = CompletionFailures.unwrapCompletion(result.failure());
      if (!(failure instanceof BackendFailure backend)
          || !isUnknown(backend.category())) {
        return CompletableFuture.failedFuture(failure);
      }
      return outcomes.resolve(invocation, scope);
    }
    if (result.updated() == null || result.updated()) return outcomes.resolve(invocation, scope);
    return control.read(scope).thenCompose(snapshot -> {
      if (snapshot.isEmpty()) return outcomes.resolve(invocation, scope);
      CommitState state = snapshot.orElseThrow().state();
      // Heartbeats change stateVersion. Reuse the sealed plan only while its logical base and
      // owner epoch are unchanged; every retry still compares a newly observed full state value.
      if (state.ownerEpoch() == owner.epoch()
          && state.ownerInstance().filter(owner.instance()::equals).isPresent()
          && state.publishedSequence() == invocation.baseSequence()
          && state.publishedRevision() == invocation.baseRevision()
          && state.publication().isEmpty()) {
        return attempt(invocation, owner, verified, scope, authorization, beforeSelection, attempt + 1);
      }
      return outcomes.resolve(invocation, scope);
    });
  }

  private static <T> CompletableFuture<T> failed(Throwable failure) {
    return CompletableFuture.failedFuture(CompletionFailures.unwrapCompletion(failure));
  }

  private static BackendFailure scopeFailure(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }

  private static boolean isUnknown(BackendFailure.Category category) {
    return switch (category) {
      case TIMEOUT, CANCELLED, UNAVAILABLE, UNKNOWN -> true;
      default -> false;
    };
  }

  private record CasResult(Boolean updated, Throwable failure) {}
}
