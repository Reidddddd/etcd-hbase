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
   * Returns only a publication observation. A selected result is pending until materialization and
   * finalization complete; it is never an etcd success response by itself.
   */
  public CompletableFuture<CommitOutcomeResolver.Resolution> publish(
      InvocationBinding invocation,
      CommitOwnerService.OwnerToken owner,
      ManifestRoot manifest,
      RequestScope requestScope) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(manifest, "manifest");
    Objects.requireNonNull(requestScope, "requestScope");
    if (invocation.preparedOwnerEpoch() != owner.epoch()) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("owner token does not match the sealed invocation"));
    }
    return planVerifier.verify(invocation, manifest, requestScope).thenCompose(verified ->
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
              attempt(durable, owner, verified, serviceScope, 0)
                  .thenCompose(publication -> finalizer.finish(durable, publication, serviceScope)));
        }))
        .exceptionallyCompose(CommitPublisher::failed);
  }

  private CompletableFuture<CommitOutcomeResolver.Resolution> attempt(
      InvocationBinding invocation,
      CommitOwnerService.OwnerToken owner,
      ManifestRoot manifest,
      RequestScope scope,
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
      PublicationSlot slot = new PublicationSlot(invocation.transactionId(), invocation.invocationId(),
          owner.epoch(), invocation.baseSequence(), invocation.baseRevision(),
          Math.addExact(invocation.baseSequence(), 1),
          Math.addExact(invocation.baseRevision(), manifest.containsKeyChanges() ? 1 : 0),
          manifest.containsKeyChanges(), manifest.systemIdentity(), manifest.digest());
      CommitState selected = state.publish(slot);
      return control.compareAndSet(observed, selected, scope)
          .handle((updated, failure) -> new CasResult(updated, failure))
          .thenCompose(result -> resolveAttempt(invocation, owner, manifest, scope, attempt, result));
    });
  }

  private CompletableFuture<CommitOutcomeResolver.Resolution> resolveAttempt(
      InvocationBinding invocation,
      CommitOwnerService.OwnerToken owner,
      ManifestRoot manifest,
      RequestScope scope,
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
        return attempt(invocation, owner, manifest, scope, attempt + 1);
      }
      return outcomes.resolve(invocation, scope);
    });
  }

  private static <T> CompletableFuture<T> failed(Throwable failure) {
    return CompletableFuture.failedFuture(CompletionFailures.unwrapCompletion(failure));
  }

  private static boolean isUnknown(BackendFailure.Category category) {
    return switch (category) {
      case TIMEOUT, CANCELLED, UNAVAILABLE, UNKNOWN -> true;
      default -> false;
    };
  }

  private record CasResult(Boolean updated, Throwable failure) {}
}
