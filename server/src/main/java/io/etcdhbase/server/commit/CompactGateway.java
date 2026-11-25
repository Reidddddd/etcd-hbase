package io.etcdhbase.server.commit;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.CompactionOperation;
import io.etcdhbase.core.commit.CompactionService;
import io.etcdhbase.core.commit.GcCoordinator;
import io.etcdhbase.core.commit.HistoryGcPermit;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.protocol.ErrorProjection;
import io.etcdhbase.grpc.protocol.ResponseHeaderFactory;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.grpc.stub.StreamObserver;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Routes {@code KV.Compact} through the existing owner lane so the selection CAS cannot interleave
 * with publication or recovery.
 *
 * <p>The logical boundary is the control-row CAS. {@code physical} additionally waits for the
 * bounded KV history pages this call deletes. It does not claim HFile release and does not wait for
 * event, decision, or result closure that J52 has not implemented.
 */
public final class CompactGateway
    implements BiConsumer<Rpc.CompactionRequest, StreamObserver<Rpc.CompactionResponse>> {
  private static final byte[] REQUEST_DOMAIN =
      "etcd-hbase/j52/compact-request/v1".getBytes(StandardCharsets.US_ASCII);

  /** The serialized owner lane; {@link SerialMutationQueue} is the production implementation. */
  @FunctionalInterface
  public interface LaneSubmitter {
    CompletableFuture<Void> submit(long requestBytes, RequestScope scope,
        java.util.function.Function<RequestScope, CompletableFuture<Void>> work);
  }

  /** Resolves the current RPC scope; a seam so ordering can be tested without gRPC context. */
  @FunctionalInterface
  public interface RequestScopes {
    RequestScope current();
  }

  public CompactGateway(String tableGroup, AuthorizedBackendIdentity identity, ByteKey authorization,
      long clusterId, long memberId, CompactionService compactions, GcCoordinator gc,
      LaneSubmitter lane, RequestScopes scopes,
      Supplier<java.util.Optional<CommitOwnerService.OwnerToken>> owner,
      java.util.function.Function<RequestScope, CompletableFuture<CommitState>> controlState,
      java.util.Optional<HistoryCollection> collection) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.identity = Objects.requireNonNull(identity, "identity");
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.compactions = Objects.requireNonNull(compactions, "compactions");
    this.gc = Objects.requireNonNull(gc, "gc");
    this.lane = Objects.requireNonNull(lane, "lane");
    this.scopes = Objects.requireNonNull(scopes, "scopes");
    this.owner = Objects.requireNonNull(owner, "owner");
    this.controlState = Objects.requireNonNull(controlState, "controlState");
    this.collection = Objects.requireNonNull(collection, "collection");
    if (tableGroup.isBlank() || !identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)
        || authorization.length() != 32 || clusterId == 0 || memberId == 0) {
      throw new IllegalArgumentException("invalid Compact gateway identity");
    }
    this.clusterId = clusterId;
    this.memberId = memberId;
  }

  private final String tableGroup;
  private final AuthorizedBackendIdentity identity;
  private final ByteKey authorization;
  private final long clusterId;
  private final long memberId;
  private final CompactionService compactions;
  private final GcCoordinator gc;
  private final LaneSubmitter lane;
  private final RequestScopes scopes;
  private final Supplier<java.util.Optional<CommitOwnerService.OwnerToken>> owner;
  private final java.util.function.Function<RequestScope, CompletableFuture<CommitState>> controlState;
  private final java.util.Optional<HistoryCollection> collection;

  @Override
  public void accept(Rpc.CompactionRequest request, StreamObserver<Rpc.CompactionResponse> observer) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(observer, "observer");
    RequestScope scope = scopes.current();
    var terminal = new AtomicBoolean();
    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) return;
    try {
      lane.submit(request.getSerializedSize(), scope,
              laneScope -> execute(request, laneScope, observer, terminal))
          .whenComplete((settled, failure) -> drained.complete(null));
    } catch (RuntimeException failure) {
      drained.complete(null);
      signalError(observer, terminal, failure);
    } catch (Error fatal) {
      drained.complete(null);
      signalError(observer, terminal, fatal);
      throw fatal;
    }
  }

  /** Runs on the serialized owner lane; never blocks that lane on another queue. */
  private CompletableFuture<Void> execute(Rpc.CompactionRequest request, RequestScope scope,
      StreamObserver<Rpc.CompactionResponse> observer, AtomicBoolean terminal) {
    java.util.Optional<CommitOwnerService.OwnerToken> currentOwner = owner.get();
    if (currentOwner.isEmpty()) {
      signalError(observer, terminal, new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      return CompletableFuture.completedFuture(null);
    }
    return controlState.apply(scope).thenCompose(state -> {
      CompactionCommand command = command(request, state);
      CommitAuthorizationProof proof = CommitAuthorizationProof.capture(
          ServiceAuthorizationContextReference.create(identity, authorization),
          state.authorizationBarrierGeneration(),
          (expected, generation, proofScope) ->
              generation != state.authorizationBarrierGeneration()
                  ? CompletableFuture.failedFuture(
                      new BackendFailure(BackendFailure.Category.PERMISSION))
                  : expected.equals(
                      ServiceAuthorizationContextReference.create(identity, authorization))
                      ? CompletableFuture.completedFuture(null)
                      : CompletableFuture.failedFuture(
                          new BackendFailure(BackendFailure.Category.PERMISSION)));
      return compactions.compact(command.revision(), command.binding(), currentOwner.orElseThrow(),
              proof, scope)
          .thenCompose(selected -> collect(command, selected, scope));
    }).handle((selected, failure) -> {
      deliver(observer, terminal, selected, failure, scope);
      return null;
    });
  }

  private CompletableFuture<CompactionOperation> collect(CompactionCommand command,
      CompactionOperation selected, RequestScope scope) {
    if (!command.physical() || selected.stage() != CompactionOperation.Stage.LOGICAL) {
      return CompletableFuture.completedFuture(selected);
    }
    if (collection.isEmpty()) {
      // The logical boundary is already durable; only the physical completion is unavailable.
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    return gc.authorizeHistory(owner.get().orElseThrow(() ->
            new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE))), scope)
        .thenCompose(permit ->
        collection.orElseThrow().collect(permit, scope).thenApply(deleted -> selected));
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

  private CompactionCommand command(Rpc.CompactionRequest request, CommitState state) {
    InvocationId invocation = InvocationId.create();
    InvocationBinding binding = InvocationBinding.begin(invocation,
        requestDigest(request.getRevision(), request.getPhysical()), authorization,
        TransactionId.create(), Math.max(1, state.ownerEpoch()), state.publishedSequence(),
        Math.max(1, state.publishedRevision()));
    return new CompactionCommand(binding, request.getRevision(), request.getPhysical());
  }

  private record CompactionCommand(InvocationBinding binding, long revision, boolean physical) {}

  /**
   * Deletes this call's bounded KV history pages. There is deliberately no no-op implementation:
   * {@code physical=true} without enumeration and durable checkpoints must fail closed rather than
   * report a logical CAS as physical completion.
   */
  @FunctionalInterface
  interface HistoryCollection {
    CompletableFuture<Integer> collect(HistoryGcPermit permit, RequestScope scope);
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
