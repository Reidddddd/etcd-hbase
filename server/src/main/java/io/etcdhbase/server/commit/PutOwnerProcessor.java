package io.etcdhbase.server.commit;

import com.google.protobuf.InvalidProtocolBufferException;
import etcdserverpb.Rpc;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.lease.LeaseFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.grpc.protocol.PutRequestMapper;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

/** Executes an authorized owner-bound Put and returns a small durable-invocation receipt. */
public final class PutOwnerProcessor implements MutationForwardReceiver.MutationProcessor {
  private final String tableGroup;
  private final CommitOwnerService.OwnerToken owner;
  private final BudgetManager budgets;
  private final BudgetManager.Reservation reservation;
  private final PutMutationExecutor executor;
  private final CommitOutcomeResolver outcomes;

  public PutOwnerProcessor(String tableGroup, CommitOwnerService.OwnerToken owner,
      BudgetManager budgets, BudgetManager.Reservation reservation,
      PutMutationExecutor executor, CommitOutcomeResolver outcomes) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.owner = Objects.requireNonNull(owner, "owner");
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.reservation = Objects.requireNonNull(reservation, "reservation");
    this.executor = Objects.requireNonNull(executor, "executor");
    this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
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
        || !request.authorizationScopeDigest().equals(binding.authorizationScopeDigest())) {
      return unknown(BackendFailure.Category.PERMISSION);
    }
    final PutCommand command;
    try {
      command = PutRequestMapper.fromWire(Rpc.PutRequest.parseFrom(request.operationBytes().bytes()));
    } catch (KvFailure semantic) {
      return CompletableFuture.completedFuture(new OwnerForwarding.SemanticRejected(semantic.reason()));
    } catch (InvalidProtocolBufferException malformed) {
      return unknown(BackendFailure.Category.UNKNOWN);
    }
    if (binding.state() == InvocationBinding.State.SELECTED
        || binding.state() == InvocationBinding.State.SEALED) {
      // This path only observes an already-finalized decision; it never republishes a stale base.
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
    return executor.executeNew(command, binding, owner, identity, scope, account)
        .<OwnerForwarding.Reply>handle((id, failure) -> {
          if (failure == null && id.equals(request.invocationId())) return receipt(request);
          if (failure != null) {
            Throwable cause = CompletionFailures.unwrapCompletion(failure);
            // PutPlanner's finite semantic failures occur before private records or publication.
            if (cause instanceof KvFailure semantic) return new OwnerForwarding.SemanticRejected(semantic.reason());
            // Binding validation runs during preparation, before publish() can select a decision.
            // Publication failures remain unknown and are not projected as Lease rejections.
            if (cause instanceof LeaseFailure leaseFailure) {
              return new OwnerForwarding.LeaseRejected(leaseFailure.reason());
            }
          }
          return unknownReply(failure);
        });
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
