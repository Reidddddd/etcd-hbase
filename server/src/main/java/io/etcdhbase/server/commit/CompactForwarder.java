package io.etcdhbase.server.commit;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CompactionOperation;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Sends one bounded Compact selection to the authoritative owner; waiting stays at ingress. */
final class CompactForwarder {
  private static final BudgetManager.Reservation MEMORY =
      new BudgetManager.Reservation(64L << 10, 1L << 20, 1L << 20, 16L << 10, 16L << 10);
  private final String group;
  private final ByteKey profile;
  private final CompactRequestAuthorization authorizations;
  private final CommitControlStore control;
  private final MutationRelay relay;
  private final BudgetManager budgets;

  CompactForwarder(String group, ByteKey profile, CompactRequestAuthorization authorizations,
      CommitControlStore control, MutationRelay relay, BudgetManager budgets) {
    this.group = group;
    this.profile = profile;
    this.authorizations = authorizations;
    this.control = control;
    this.relay = relay;
    this.budgets = budgets;
  }

  CompletableFuture<CompactionOperation> forward(Rpc.CompactionRequest request,
      Optional<String> token, RequestScope requestScope) {
    var operation = new DrainingRequestScope(requestScope);
    CompletableFuture<CompactionOperation> selected;
    try { selected = send(request, token, operation.scope()); }
    catch (RuntimeException | Error failure) { selected = CompletableFuture.failedFuture(failure); }
    return operation.finish(selected);
  }

  private CompletableFuture<CompactionOperation> send(Rpc.CompactionRequest request,
      Optional<String> token, RequestScope scope) {
    try {
      var account = budgets.admit(group, scope, MEMORY);
      var input = account.tryRetain(BudgetManager.Memory.CANDIDATE, 16L << 10)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      PutCommitPreparation.retainUntilDrain(scope, input);
      ByteKey operation = MutationOperationEnvelope.compact(ByteBuffer.allocate(9)
          .putLong(request.getRevision()).put((byte) (request.getPhysical() ? 1 : 0)).array());
      return authorizations.resolve(token, scope).thenCompose(identity ->
          control.read(scope).thenCompose(found -> {
            var state = found.orElseThrow(() -> new IllegalStateException("missing Compact control")).state();
            long remaining = Math.min(scope.remainingDeadlineNanos(), MutationForwardRequest.MAX_TIMEOUT_NANOS);
            if (remaining <= 0) return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.TIMEOUT));
            if (state.ownerInstance().isEmpty()) {
              return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
            }
            var envelope = new MutationForwardRequest(group, InvocationId.create(),
                MutationForwardRequest.digest(group, operation), identity.authorizationDigest(),
                identity.authorizationReference(), state.ownerEpoch(), remaining, 0, operation);
            return relay.forward(envelope, scope).thenCompose(reply -> resolve(envelope, request.getRevision(), reply));
          }));
    } catch (BackendFailure | RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletableFuture<CompactionOperation> resolve(
      MutationForwardRequest request, long revision, MutationRelay.Result reply) {
    if (reply instanceof MutationRelay.KnownResult known) {
      try {
        var operation = CompactionOperation.decode(ByteKey.copyOf(known.responseBytes()));
        if (!operation.id().equals(request.invocationId())
            || !operation.requestDigest().equals(request.requestDigest())
            || !operation.authorizationDigest().equals(request.authorizationScopeDigest())
            || !operation.profile().equals(profile) || operation.revision() != revision
            || operation.stage() == CompactionOperation.Stage.PREPARED) {
          return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
        }
        return CompletableFuture.completedFuture(operation);
      } catch (IllegalArgumentException malformed) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN, malformed));
      }
    }
    var category = reply instanceof MutationRelay.KnownRejection rejected ? rejected.category()
        : reply instanceof MutationRelay.ResultUnknown unknown ? unknown.category() : BackendFailure.Category.UNKNOWN;
    // A post-send unknown may already have selected the boundary. Never allocate and send a retry.
    return CompletableFuture.failedFuture(new BackendFailure(category));
  }
}
