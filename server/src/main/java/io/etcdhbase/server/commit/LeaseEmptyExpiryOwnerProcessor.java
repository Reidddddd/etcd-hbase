package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

/** Processes the bounded on-demand resolution of an expired empty Lease on the owner lane. */
public final class LeaseEmptyExpiryOwnerProcessor implements MutationForwardReceiver.MutationProcessor {
  private final String tableGroup;
  private final CommitOwnerService.OwnerToken owner;
  private final BudgetManager budgets;
  private final BudgetManager.Reservation reservation;
  private final LeaseEmptyExpiryExecutor executor;
  private final CommitOutcomeResolver outcomes;
  private final LeaseOwnerRejectionRecorder rejections;

  public LeaseEmptyExpiryOwnerProcessor(String tableGroup, CommitOwnerService.OwnerToken owner,
      BudgetManager budgets, BudgetManager.Reservation reservation,
      LeaseEmptyExpiryExecutor executor, CommitOutcomeResolver outcomes,
      InvocationService invocations, Clock clock) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.owner = Objects.requireNonNull(owner, "owner");
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.reservation = Objects.requireNonNull(reservation, "reservation");
    this.executor = Objects.requireNonNull(executor, "executor");
    this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
    this.rejections = new LeaseOwnerRejectionRecorder(outcomes, invocations, owner, clock);
    if (tableGroup.isBlank()) throw new IllegalArgumentException("table group is required");
  }

  @Override
  public CompletableFuture<OwnerForwarding.Reply> process(MutationForwardRequest request,
      InvocationBinding binding, AuthorizedBackendIdentity identity, RequestScope scope) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(binding, "binding");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(scope, "scope");
    if (!tableGroup.equals(request.tableGroupId()) || request.expectedOwnerEpoch() != owner.epoch()
        || !request.invocationId().equals(binding.invocationId())
        || !request.requestDigest().equals(binding.requestDigest())
        || !request.authorizationScopeDigest().equals(binding.authorizationScopeDigest())
        || !identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)) {
      return unknown(BackendFailure.Category.PERMISSION);
    }
    if (scope.isClosed() || scope.isDeadlineExceeded()) {
      return unknown(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
    }
    final ExpiryCandidateIdentity candidate;
    try {
      candidate = ExpiryCandidateIdentity.decode(ByteKey.copyOf(
          MutationOperationEnvelope.leaseExpireEmptyBytes(request.operationBytes())));
    } catch (IllegalArgumentException malformed) {
      return unknown(BackendFailure.Category.UNKNOWN);
    }
    if (binding.state() == InvocationBinding.State.SELECTED
        || binding.state() == InvocationBinding.State.SEALED) {
      return outcomes.resolve(binding, scope).handle((resolution, failure) ->
          failure == null && resolution.state() == CommitOutcomeResolver.State.SELECTED_FINALIZED
              ? receipt(request) : unknownReply(failure));
    }
    if (binding.state() != InvocationBinding.State.PREPARING) {
      return unknown(BackendFailure.Category.UNKNOWN);
    }
    final BudgetManager.Account account;
    try {
      account = budgets.admit(tableGroup, scope, reservation);
    } catch (BackendFailure failure) {
      return unknown(failure.category());
    }
    return executor.executeNew(candidate, binding, owner, identity, scope, account)
        .handle((id, failure) -> handleOutcome(request, binding, id, failure, scope))
        .thenCompose(result -> result);
  }

  private CompletableFuture<OwnerForwarding.Reply> handleOutcome(
      MutationForwardRequest request, InvocationBinding binding, InvocationId id,
      Throwable failure, RequestScope scope) {
    if (failure == null) {
      return CompletableFuture.completedFuture(request.invocationId().equals(id)
          ? receipt(request) : new OwnerForwarding.Unknown(BackendFailure.Category.UNKNOWN));
    }
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    if (cause instanceof LeaseEmptyExpiryExecutor.RejectedBeforePreparation rejected) {
      return rejections.persist(binding, scope).<OwnerForwarding.Reply>thenApply(persisted -> {
        if (!persisted) return new OwnerForwarding.Unknown(BackendFailure.Category.UNKNOWN);
        return rejected.rejection() == LeaseEmptyExpiryExecutor.Rejection.RECHECK
            ? new OwnerForwarding.LeaseRejected(io.etcdhbase.core.lease.LeaseFailure.Reason.NOT_FOUND)
            : new OwnerForwarding.Rejected(BackendFailure.Category.UNAVAILABLE);
      }).exceptionally(LeaseEmptyExpiryOwnerProcessor::unknownReply);
    }
    return CompletableFuture.completedFuture(unknownReply(cause));
  }

  private static OwnerForwarding.Completed receipt(MutationForwardRequest request) {
    return new OwnerForwarding.Completed(request.invocationId().bytes());
  }

  private static CompletableFuture<OwnerForwarding.Reply> unknown(BackendFailure.Category category) {
    return CompletableFuture.completedFuture(new OwnerForwarding.Unknown(category));
  }

  private static OwnerForwarding.Unknown unknownReply(Throwable failure) {
    if (failure == null) return new OwnerForwarding.Unknown(BackendFailure.Category.UNKNOWN);
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    if (cause instanceof BackendFailure backend) return new OwnerForwarding.Unknown(backend.category());
    if (cause instanceof CancellationException) {
      return new OwnerForwarding.Unknown(BackendFailure.Category.CANCELLED);
    }
    return new OwnerForwarding.Unknown(BackendFailure.Category.UNKNOWN);
  }
}
