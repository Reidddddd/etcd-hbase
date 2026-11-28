package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.CompactionService;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;
import java.util.function.Supplier;

/** Serializes only logical Compact selection with publication and revalidates its remote identity. */
final class CompactOwnerLane {
  private final String group;
  private final SerialMutationQueue queue;
  private final CommitControlStore control;
  private final Supplier<Optional<CommitOwnerService.OwnerToken>> owner;
  private final CompactionService compactions;
  private final MutationForwardReceiver.AuthorizationResolver identities;
  private final CompactRequestAuthorization management;

  CompactOwnerLane(String group, SerialMutationQueue queue, CommitControlStore control,
      Supplier<Optional<CommitOwnerService.OwnerToken>> owner, CompactionService compactions,
      MutationForwardReceiver.AuthorizationResolver identities, CompactRequestAuthorization management) {
    this.group = group;
    this.queue = queue;
    this.control = control;
    this.owner = owner;
    this.compactions = compactions;
    this.identities = identities;
    this.management = management;
  }

  CompletableFuture<OwnerForwarding.Reply> receive(
      String peer, MutationForwardRequest request, RequestScope requestScope) {
    if (!group.equals(request.tableGroupId()) || request.hopCount() != MutationForwardRequest.MAX_HOPS) {
      return unknown(BackendFailure.Category.PERMISSION);
    }
    final long revision;
    try {
      revision = ByteBuffer.wrap(MutationOperationEnvelope.compactBytes(request.operationBytes())).getLong();
    } catch (IllegalArgumentException malformed) {
      return unknown(BackendFailure.Category.PERMISSION);
    }
    long entered = System.nanoTime();
    return queue.submit(16L << 10, requestScope, serviceScope -> {
      long elapsed = Math.max(0, System.nanoTime() - entered);
      long remaining = Math.min(request.remainingTimeoutNanos() - elapsed, serviceScope.remainingDeadlineNanos());
      if (remaining <= 0) return unknown(BackendFailure.Category.TIMEOUT);
      var operation = new DrainingRequestScope(serviceScope, remaining);
      return operation.finish(select(peer, request, revision, operation.scope()));
    }).exceptionally(failure -> new OwnerForwarding.Unknown(category(failure)));
  }

  private CompletableFuture<OwnerForwarding.Reply> select(String peer,
      MutationForwardRequest request, long revision, RequestScope scope) {
    return control.read(scope).thenCompose(found -> {
      var token = owner.get();
      if (found.isEmpty() || token.isEmpty()
          || !currentOwner(found.orElseThrow().state(), token.orElseThrow(), request)) {
        return unknown(BackendFailure.Category.UNAVAILABLE);
      }
      var state = found.orElseThrow().state();
      return identities.verifyAndResolve(peer, request, state.authorizationBarrierGeneration(), scope)
          .thenCompose(verified -> {
            if (!verified.identity().scope().tableGroupId().equals(group)
                || !verified.identity().allowedTableGroupIds().contains(group)
                || !verified.authorizationScopeDigest().equals(request.authorizationScopeDigest())
                || verified.publicationProof().authorizationBarrierGeneration()
                    != state.authorizationBarrierGeneration()) return unknown(BackendFailure.Category.PERMISSION);
            var binding = InvocationBinding.begin(request.invocationId(), request.requestDigest(),
                verified.authorizationScopeDigest(), TransactionId.create(), state.ownerEpoch(),
                state.publishedSequence(), state.publishedRevision());
            return compactions.compact(revision, binding, token.orElseThrow(),
                management.captureProof(verified), scope)
                .thenApply(operation -> new OwnerForwarding.Completed(operation.encode().bytes()));
          });
    });
  }

  private static boolean currentOwner(CommitState state, CommitOwnerService.OwnerToken token,
      MutationForwardRequest request) {
    return state.ownerEpoch() == request.expectedOwnerEpoch() && state.ownerEpoch() == token.epoch()
        && state.ownerInstance().filter(token.instance()::equals).isPresent()
        && !state.ownerLeaseExpired(System.currentTimeMillis()) && state.publication().isEmpty();
  }

  private static BackendFailure.Category category(Throwable failure) {
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    if (cause instanceof Error fatal) throw fatal;
    if (cause instanceof CancellationException) return BackendFailure.Category.CANCELLED;
    return cause instanceof BackendFailure backend ? backend.category() : BackendFailure.Category.UNKNOWN;
  }

  private static CompletableFuture<OwnerForwarding.Reply> unknown(BackendFailure.Category category) {
    return CompletableFuture.completedFuture(new OwnerForwarding.Unknown(category));
  }
}
