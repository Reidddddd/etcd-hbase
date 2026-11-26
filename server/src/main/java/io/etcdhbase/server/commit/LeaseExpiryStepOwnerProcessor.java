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
import io.etcdhbase.core.lease.LeaseExpiryStepCommand;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Processes one internal expiry step on the same fenced mutation lane as user writes. */
public final class LeaseExpiryStepOwnerProcessor implements MutationForwardReceiver.MutationProcessor {
  private final String tableGroup;
  private final Supplier<Optional<CommitOwnerService.OwnerToken>> owner;
  private final BudgetManager budgets;
  private final BudgetManager.Reservation reservation;
  private final LeaseExpiryStepExecutor executor;
  private final CommitOutcomeResolver outcomes;
  private final LeaseOwnerRejectionRecorder rejections;

  public LeaseExpiryStepOwnerProcessor(String tableGroup,
      Supplier<Optional<CommitOwnerService.OwnerToken>> owner,
      BudgetManager budgets, BudgetManager.Reservation reservation,
      LeaseExpiryStepExecutor executor, CommitOutcomeResolver outcomes,
      InvocationService invocations, Clock clock) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.owner = Objects.requireNonNull(owner, "owner");
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.reservation = Objects.requireNonNull(reservation, "reservation");
    this.executor = Objects.requireNonNull(executor, "executor");
    this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
    this.rejections = new LeaseOwnerRejectionRecorder(outcomes,
        Objects.requireNonNull(invocations, "invocations"), owner,
        Objects.requireNonNull(clock, "clock"));
    if (tableGroup.isBlank()) throw new IllegalArgumentException("table group is required");
  }

  @Override
  public CompletableFuture<OwnerForwarding.Reply> process(MutationForwardRequest request,
      InvocationBinding binding, MutationForwardReceiver.VerifiedAuthorization authorization,
      RequestScope scope) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(binding, "binding");
    Objects.requireNonNull(authorization, "authorization");
    Objects.requireNonNull(scope, "scope");
    AuthorizedBackendIdentity identity = authorization.identity();
    Optional<CommitOwnerService.OwnerToken> currentOwner = owner.get();
    if (currentOwner.isEmpty()) return unknown(BackendFailure.Category.UNAVAILABLE);
    CommitOwnerService.OwnerToken activeOwner = currentOwner.orElseThrow();
    if (!tableGroup.equals(request.tableGroupId())
        || request.expectedOwnerEpoch() != activeOwner.epoch()
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
    final LeaseExpiryStepCommand command;
    try {
      command = LeaseExpiryStepCommand.decode(ByteKey.copyOf(
          MutationOperationEnvelope.leaseExpiryStepBytes(request.operationBytes())));
    } catch (IllegalArgumentException malformed) {
      return unknown(BackendFailure.Category.UNKNOWN);
    }
    if (binding.state() == InvocationBinding.State.SELECTED
        || binding.state() == InvocationBinding.State.SEALED) {
      return outcomes.resolve(binding, scope).handle((resolution, failure) ->
          failure == null && resolution.state() == CommitOutcomeResolver.State.SELECTED_FINALIZED
              ? receipt(request) : unknownReply(failure));
    }
    if (binding.state() == InvocationBinding.State.NO_CHANGES) {
      return CompletableFuture.completedFuture(receipt(request));
    }
    if (binding.state() == InvocationBinding.State.NOT_SELECTED
        || binding.state() == InvocationBinding.State.WINDOW_CLOSED) {
      return CompletableFuture.completedFuture(
          new OwnerForwarding.Rejected(BackendFailure.Category.UNAVAILABLE));
    }
    if (binding.state() != InvocationBinding.State.PREPARING) {
      return unknown(BackendFailure.Category.UNKNOWN);
    }
    final BudgetManager.Account account;
    try {
      account = budgets.admitReserved(tableGroup, scope, reservation,
          new BudgetManager.ReadLimits(20_000, 64L << 20, 64, 64, 20_000));
    } catch (BackendFailure failure) {
      return unknown(failure.category());
    }
    return executor.executeNew(command, binding, activeOwner, identity,
        authorization.publicationProof(), scope, account)
        .handle((invocation, failure) -> handleOutcome(request, binding, invocation, failure, scope))
        .thenCompose(result -> result);
  }

  private CompletableFuture<OwnerForwarding.Reply> handleOutcome(MutationForwardRequest request,
      InvocationBinding binding, InvocationId invocation, Throwable failure, RequestScope scope) {
    if (failure == null) {
      return CompletableFuture.completedFuture(request.invocationId().equals(invocation)
          ? receipt(request) : new OwnerForwarding.Unknown(BackendFailure.Category.UNKNOWN));
    }
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    if (Boolean.getBoolean("etcdhbase.m4.debug.failures")) {
      System.err.println("[M4 diagnostic] Lease expiry step owner failure: " + cause.getClass().getName());
      cause.printStackTrace(System.err);
    }
    BackendFailure.Category category = cause instanceof BackendFailure backend
        ? backend.category() : BackendFailure.Category.UNAVAILABLE;
    return rejections.persist(binding, scope).<OwnerForwarding.Reply>thenApply(persisted -> persisted
        ? new OwnerForwarding.Rejected(category)
        : new OwnerForwarding.Unknown(category)).exceptionally(ignored -> unknownReply(cause));
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
