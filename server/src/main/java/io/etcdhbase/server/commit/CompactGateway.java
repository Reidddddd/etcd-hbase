package io.etcdhbase.server.commit;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.CompactionOperation;
import io.etcdhbase.core.commit.CompactionService;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.protocol.ErrorProjection;
import io.etcdhbase.grpc.protocol.ResponseHeaderFactory;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.grpc.stub.StreamObserver;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Routes {@code KV.Compact} through the existing owner lane so the selection CAS cannot interleave
 * with publication or recovery.
 *
 * <p>The logical boundary is the control-row CAS. {@code physical} additionally waits for the
 * selected target's durable KV-history completion receipt outside the mutation lane. This slice does
 * not confirm internal-record reclamation or the complete J52 physical contract. Maintenance
 * continues independently when the caller stops waiting; HFile release remains asynchronous.
 */
public final class CompactGateway
    implements BiConsumer<Rpc.CompactionRequest, StreamObserver<Rpc.CompactionResponse>> {
  private static final byte[] REQUEST_DOMAIN =
      "etcd-hbase/j52/compact-request/v1".getBytes(StandardCharsets.US_ASCII);

  /** The serialized owner lane; {@link SerialMutationQueue} is the production implementation. */
  @FunctionalInterface
  public interface LaneSubmitter {
    CompletableFuture<CompactionOperation> submit(long requestBytes, RequestScope scope,
        java.util.function.Function<RequestScope, CompletableFuture<CompactionOperation>> work);
  }

  CompactGateway(CompactRequestAuthorization authorizations, long clusterId, long memberId,
      CompactionService compactions,
      LaneSubmitter lane,
      Supplier<java.util.Optional<CommitOwnerService.OwnerToken>> owner,
      java.util.function.Function<RequestScope, CompletableFuture<CommitState>> controlState,
      java.util.Optional<PhysicalCompletion> physicalCompletion) {
    this.authorizations = Objects.requireNonNull(authorizations, "authorizations");
    this.compactions = Objects.requireNonNull(compactions, "compactions");
    this.lane = Objects.requireNonNull(lane, "lane");
    this.owner = Objects.requireNonNull(owner, "owner");
    this.controlState = Objects.requireNonNull(controlState, "controlState");
    this.physicalCompletion = Objects.requireNonNull(physicalCompletion, "physicalCompletion");
    if (clusterId == 0 || memberId == 0) {
      throw new IllegalArgumentException("invalid Compact gateway identity");
    }
    this.clusterId = clusterId;
    this.memberId = memberId;
  }

  private final CompactRequestAuthorization authorizations;
  private final long clusterId;
  private final long memberId;
  private final CompactionService compactions;
  private final LaneSubmitter lane;
  private final Supplier<java.util.Optional<CommitOwnerService.OwnerToken>> owner;
  private final java.util.function.Function<RequestScope, CompletableFuture<CommitState>> controlState;
  private final java.util.Optional<PhysicalCompletion> physicalCompletion;

  @Override
  public void accept(Rpc.CompactionRequest request, StreamObserver<Rpc.CompactionResponse> observer) {
    accept(request, RequestScopeInterceptor.currentAuthToken(), RequestScopeInterceptor.currentScope(), observer);
  }

  void accept(Rpc.CompactionRequest request, Optional<String> token, RequestScope scope,
      StreamObserver<Rpc.CompactionResponse> observer) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(observer, "observer");
    Objects.requireNonNull(token, "token");
    Objects.requireNonNull(scope, "scope");
    var terminal = new AtomicBoolean();
    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) return;
    try {
      // Proto3 omits default fields, so a valid Compact(0) still needs one admission unit.
      lane.submit(Math.max(1, request.getSerializedSize()), scope,
              laneScope -> execute(request, token, laneScope))
          .thenCompose(selected -> awaitPhysical(request.getPhysical(), selected, scope))
          .whenComplete((selected, failure) -> {
            try {
              // The queue receipt includes service-scope cleanup, while delivery belongs to the RPC.
              deliver(observer, terminal, selected, failure, scope);
            } finally {
              drained.complete(null);
            }
          });
    } catch (RuntimeException failure) {
      drained.complete(null);
      signalError(observer, terminal, failure);
    } catch (Error fatal) {
      drained.complete(null);
      signalError(observer, terminal, fatal);
      throw fatal;
    }
  }

  /** The lane retains ownership of accepted work until its service scope has drained. */
  private CompletableFuture<CompactionOperation> execute(
      Rpc.CompactionRequest request, Optional<String> token, RequestScope serviceScope) {
    return authorizations.resolve(token, serviceScope)
        .thenCompose(identity -> select(request, identity, serviceScope));
  }

  private CompletableFuture<CompactionOperation> select(Rpc.CompactionRequest request,
      AuthenticatedRequestIdentityResolver.ResolvedRequestIdentity identity, RequestScope scope) {
    java.util.Optional<CommitOwnerService.OwnerToken> currentOwner = owner.get();
    if (currentOwner.isEmpty()) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    return controlState.apply(scope).thenCompose(state -> {
      InvocationBinding binding = binding(request, state, identity.authorizationDigest());
      var proof = authorizations.captureProof(identity, state.authorizationBarrierGeneration());
      return compactions.compact(request.getRevision(), binding, currentOwner.orElseThrow(), proof, scope);
    });
  }

  private CompletableFuture<CompactionOperation> awaitPhysical(boolean physical,
      CompactionOperation selected, RequestScope scope) {
    if (!physical || selected.stage() != CompactionOperation.Stage.LOGICAL) {
      return CompletableFuture.completedFuture(selected);
    }
    if (physicalCompletion.isEmpty()) {
      // The durable logical selection survives failure to establish its physical waiter.
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    return physicalCompletion.orElseThrow().awaitCompletion(selected, scope).thenApply(ignored -> selected);
  }

  private void deliver(StreamObserver<Rpc.CompactionResponse> observer, AtomicBoolean terminal,
      CompactionOperation selected, Throwable failure, RequestScope scope) {
    Throwable cause = failure == null ? null : CompletionFailures.unwrapCompletion(failure);
    if (cause instanceof Error fatal) throw fatal;
    // A cancelled or expired request must not receive a Compact response for work it abandoned.
    if (cause == null && (scope.isClosed() || scope.isDeadlineExceeded())) {
      if (!terminal.compareAndSet(false, true)) return;
      observer.onError(ErrorProjection.project(new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED)));
      return;
    }
    if (!terminal.compareAndSet(false, true)) return;
    if (cause != null) {
      observer.onError(ErrorProjection.project(cause));
      return;
    }
    if (selected.stage() == CompactionOperation.Stage.COMPACTED) {
      observer.onError(ErrorProjection.project(new KvFailure(KvFailure.Reason.COMPACTED)));
      return;
    }
    if (selected.stage() == CompactionOperation.Stage.FUTURE) {
      observer.onError(ErrorProjection.project(new KvFailure(KvFailure.Reason.FUTURE_REVISION)));
      return;
    }
    if (selected.stage() != CompactionOperation.Stage.LOGICAL) {
      observer.onError(ErrorProjection.project(
          new IllegalStateException("Compact resolved without a logical boundary")));
      return;
    }
    observer.onNext(Rpc.CompactionResponse.newBuilder()
        .setHeader(ResponseHeaderFactory.create(clusterId, memberId, selected.responseRevision(), 0))
        .build());
    observer.onCompleted();
  }

  private InvocationBinding binding(
      Rpc.CompactionRequest request, CommitState state, ByteKey authorization) {
    InvocationId invocation = InvocationId.create();
    return InvocationBinding.begin(invocation,
        requestDigest(request.getRevision(), request.getPhysical()), authorization,
        TransactionId.create(), Math.max(1, state.ownerEpoch()), state.publishedSequence(),
        Math.max(1, state.publishedRevision()));
  }

  /** Waits for durable completion of this exact target; the background worker owns all deletion. */
  @FunctionalInterface
  interface PhysicalCompletion {
    CompletableFuture<Void> awaitCompletion(CompactionOperation target, RequestScope scope);
  }

  private static void signalError(StreamObserver<Rpc.CompactionResponse> observer,
      AtomicBoolean terminal, Throwable failure) {
    if (!terminal.compareAndSet(false, true)) return;
    observer.onError(ErrorProjection.project(CompletionFailures.unwrapCompletion(failure)));
  }

  private static ByteKey requestDigest(long revision, boolean physical) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(REQUEST_DOMAIN);
      digest.update(ByteBuffer.allocate(Long.BYTES * 2)
          .putLong(revision).putLong(physical ? 1 : 0).array());
      return ByteKey.copyOf(digest.digest());
    } catch (NoSuchAlgorithmException unavailable) {
      throw new IllegalStateException("SHA-256 is unavailable", unavailable);
    }
  }
}
