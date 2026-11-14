package io.etcdhbase.server.commit;

import com.google.protobuf.InvalidProtocolBufferException;
import etcdserverpb.Rpc;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.lease.LeaseFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.grpc.protocol.TxnProtocolLimit;
import io.etcdhbase.grpc.protocol.TxnRequestMapper;
import io.etcdhbase.grpc.protocol.DeleteProtocolLimit;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

/** Executes or resolves one authenticated owner-forwarded transaction invocation. */
public final class TxnOwnerProcessor implements MutationForwardReceiver.MutationProcessor {
  private final String tableGroup;
  private final CommitOwnerService.OwnerToken owner;
  private final BudgetManager budgets;
  private final BudgetManager.Reservation reservation;
  private final TxnMutationExecutor executor;
  private final CommitOutcomeResolver outcomes;
  private final int maximumEtcdRequestBytes;

  public TxnOwnerProcessor(String tableGroup, CommitOwnerService.OwnerToken owner,
      BudgetManager budgets, BudgetManager.Reservation reservation, TxnMutationExecutor executor,
      CommitOutcomeResolver outcomes, int maximumEtcdRequestBytes) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.owner = Objects.requireNonNull(owner, "owner");
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.reservation = Objects.requireNonNull(reservation, "reservation");
    this.executor = Objects.requireNonNull(executor, "executor");
    this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
    if (tableGroup.isBlank() || maximumEtcdRequestBytes < 1
        || maximumEtcdRequestBytes > MutationForwardRequest.MAX_OPERATION_BYTES) {
      throw new IllegalArgumentException("invalid transaction owner configuration");
    }
    this.maximumEtcdRequestBytes = maximumEtcdRequestBytes;
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
    final TxnCommand command;
    try {
      var kind = MutationOperationEnvelope.kind(request.operationBytes());
      if (kind == MutationOperationEnvelope.Kind.DELETE_RANGE) {
        var delete = Rpc.DeleteRangeRequest.parseFrom(MutationOperationEnvelope.deleteRangeBytes(request.operationBytes()));
        var wire = Rpc.TxnRequest.newBuilder().addSuccess(Rpc.RequestOp.newBuilder().setRequestDeleteRange(delete)).build();
        command = TxnRequestMapper.fromWire(wire);
        DeleteProtocolLimit.requireWithin(delete, request.invocationId().value().getMostSignificantBits(),
            maximumEtcdRequestBytes);
      } else {
        var wire = Rpc.TxnRequest.parseFrom(MutationOperationEnvelope.transactionBytes(request.operationBytes()));
        command = TxnRequestMapper.fromWire(wire);
        TxnProtocolLimit.requireWithin(wire, request.invocationId().value().getMostSignificantBits(),
            maximumEtcdRequestBytes);
      }
      if (!command.mayWrite()) throw new KvFailure(KvFailure.Reason.UNSUPPORTED_OPERATION);
    } catch (KvFailure semantic) {
      return CompletableFuture.completedFuture(new OwnerForwarding.SemanticRejected(semantic.reason()));
    } catch (InvalidProtocolBufferException | IllegalArgumentException malformed) {
      return unknown(BackendFailure.Category.UNKNOWN);
    }
    if (binding.state() == InvocationBinding.State.NO_CHANGES) {
      return CompletableFuture.completedFuture(receipt(request));
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
    return executor.executeNew(command, binding, owner, identity, scope, account)
        .<OwnerForwarding.Reply>handle((id, failure) -> {
          if (failure == null && id.equals(request.invocationId())) return receipt(request);
          if (failure != null) {
            Throwable cause = CompletionFailures.unwrapCompletion(failure);
            if (cause instanceof KvFailure semantic) {
              return new OwnerForwarding.SemanticRejected(semantic.reason());
            }
            // Txn lease checks and auxiliary planning finish before finish() calls publish().
            // A failed or unknown publication is kept on the Unknown path below.
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
