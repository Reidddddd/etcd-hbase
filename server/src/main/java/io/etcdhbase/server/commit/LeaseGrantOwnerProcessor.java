package io.etcdhbase.server.commit;

import com.google.protobuf.InvalidProtocolBufferException;
import etcdserverpb.Rpc;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.Objects;
import java.time.Clock;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

/** Processes one Grant on the serialized owner lane or resolves its original decision. */
public final class LeaseGrantOwnerProcessor implements MutationForwardReceiver.MutationProcessor {
  private final String tableGroup;
  private final CommitOwnerService.OwnerToken owner;
  private final BudgetManager budgets;
  private final BudgetManager.Reservation reservation;
  private final LeaseGrantExecutor executor;
  private final CommitOutcomeResolver outcomes;
  private final LeaseOwnerRejectionRecorder rejections;

  public LeaseGrantOwnerProcessor(String tableGroup, CommitOwnerService.OwnerToken owner,
      BudgetManager budgets, BudgetManager.Reservation reservation, LeaseGrantExecutor executor,
      CommitOutcomeResolver outcomes, InvocationService invocations, Clock clock) {
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
    final Rpc.LeaseGrantRequest command;
    try {
      command = Rpc.LeaseGrantRequest.parseFrom(
          MutationOperationEnvelope.leaseGrantBytes(request.operationBytes()));
    } catch (InvalidProtocolBufferException | IllegalArgumentException malformed) {
      return unknown(BackendFailure.Category.UNKNOWN);
    }
    if (binding.state() == InvocationBinding.State.SELECTED
        || binding.state() == InvocationBinding.State.SEALED) {
      return outcomes.resolve(binding, scope).handle((resolution, failure) ->
          failure == null && resolution.state() == CommitOutcomeResolver.State.SELECTED_FINALIZED
              ? receipt(request) : unknownReply(failure));
    }
    if (binding.state() != InvocationBinding.State.PREPARING) return unknown(BackendFailure.Category.UNKNOWN);
    final BudgetManager.Account account;
    try {
      account = budgets.admit(tableGroup, scope, reservation);
    } catch (BackendFailure failure) {
      return unknown(failure.category());
    }
    return executor.executeNew(command.getID(), command.getTTL(), binding, owner, identity, scope, account)
        .handle((id, failure) -> handleGrantOutcome(request, binding, id, failure, scope))
        .thenCompose(result -> result);
  }

  private CompletableFuture<OwnerForwarding.Reply> handleGrantOutcome(
      MutationForwardRequest request, InvocationBinding binding, io.etcdhbase.core.commit.InvocationId id,
      Throwable failure, RequestScope scope) {
    if (failure == null) {
      return CompletableFuture.completedFuture(request.invocationId().equals(id)
          ? receipt(request) : new OwnerForwarding.Unknown(BackendFailure.Category.UNKNOWN));
    }
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    if (cause instanceof LeaseGrantExecutor.RejectedBeforePreparation semantic) {
      return persistSemanticRejection(binding, semantic.reason(), scope);
    }
    return CompletableFuture.completedFuture(unknownReply(cause));
  }

  private CompletableFuture<OwnerForwarding.Reply> persistSemanticRejection(
      InvocationBinding binding, io.etcdhbase.core.lease.LeaseFailure.Reason reason,
      RequestScope scope) {
    return rejections.persist(binding, scope).<OwnerForwarding.Reply>thenApply(persisted -> persisted
        ? new OwnerForwarding.LeaseRejected(reason)
        : new OwnerForwarding.Unknown(BackendFailure.Category.UNKNOWN))
        .exceptionally(failure -> unknownReply(failure));
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
    if (cause instanceof CancellationException) return new OwnerForwarding.Unknown(BackendFailure.Category.CANCELLED);
    return new OwnerForwarding.Unknown(BackendFailure.Category.UNKNOWN);
  }
}
