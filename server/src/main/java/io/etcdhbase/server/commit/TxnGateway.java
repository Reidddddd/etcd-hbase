package io.etcdhbase.server.commit;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.kv.RangeCommand;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.grpc.protocol.DeleteProtocolLimit;
import io.etcdhbase.grpc.protocol.ErrorProjection;
import io.etcdhbase.grpc.protocol.RangeRequestMapper;
import io.etcdhbase.grpc.protocol.TxnProtocolLimit;
import io.etcdhbase.grpc.protocol.TxnRequestMapper;
import io.etcdhbase.grpc.protocol.TxnResponseEmitter;
import io.etcdhbase.hbase.codec.ResultMessages;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** Routes Txn and DeleteRange requests through a fixed view or the authorized mutation owner. */
public final class TxnGateway implements BiConsumer<Rpc.TxnRequest, StreamObserver<Rpc.TxnResponse>> {
  private final String tableGroup;
  private final AuthorizedBackendIdentity identity;
  private final ByteKey authorizationDigest;
  private final long clusterId;
  private final long memberId;
  private final int maximumEtcdRequestBytes;
  private final CommitControlStore control;
  private final MutationRelay relay;
  private final InvocationResultResolver results;
  private final TxnReadExecutor reads;
  private final BudgetManager budgets;
  private final BudgetManager.Reservation reservation;
  private final Executor worker;
  private final RequestMetrics metrics;

  public TxnGateway(String tableGroup, AuthorizedBackendIdentity identity, ByteKey authorizationDigest,
      long clusterId, long memberId, int maximumEtcdRequestBytes, CommitControlStore control,
      MutationRelay relay, InvocationResultResolver results, TxnReadExecutor reads,
      BudgetManager budgets, BudgetManager.Reservation reservation, Executor worker, RequestMetrics metrics) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.identity = Objects.requireNonNull(identity, "identity");
    this.authorizationDigest = Objects.requireNonNull(authorizationDigest, "authorizationDigest");
    this.control = Objects.requireNonNull(control, "control");
    this.relay = Objects.requireNonNull(relay, "relay");
    this.results = Objects.requireNonNull(results, "results");
    this.reads = Objects.requireNonNull(reads, "reads");
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.reservation = Objects.requireNonNull(reservation, "reservation");
    this.worker = Objects.requireNonNull(worker, "worker");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    if (!identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)
        || authorizationDigest.length() != 32 || clusterId == 0 || memberId == 0
        || maximumEtcdRequestBytes < 1 || maximumEtcdRequestBytes > MutationForwardRequest.MAX_OPERATION_BYTES) {
      throw new IllegalArgumentException("invalid transaction gateway identity");
    }
    this.clusterId = clusterId;
    this.memberId = memberId;
    this.maximumEtcdRequestBytes = maximumEtcdRequestBytes;
  }

  @Override
  public void accept(Rpc.TxnRequest request, StreamObserver<Rpc.TxnResponse> observer) {
    accept(request, null, null, observer);
  }

  /** A standalone range shares the read barrier while retaining standalone revision semantics. */
  public void range(Rpc.RangeRequest request, StreamObserver<Rpc.RangeResponse> observer) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(observer, "observer");
    var transaction = Rpc.TxnRequest.newBuilder()
        .addSuccess(Rpc.RequestOp.newBuilder().setRequestRange(request)).build();
    accept(transaction, null, request, new StreamObserver<>() {
      @Override public void onNext(Rpc.TxnResponse response) {
        if (!response.hasHeader() || response.getResponsesCount() != 1
            || !response.getResponses(0).hasResponseRange()) {
          throw new IllegalStateException("standalone range result differs from its request");
        }
        observer.onNext(response.getResponses(0).getResponseRange().toBuilder()
            .setHeader(response.getHeader()).build());
      }
      @Override public void onError(Throwable failure) { observer.onError(failure); }
      @Override public void onCompleted() { observer.onCompleted(); }
    });
  }

  /** A standalone delete uses the same transaction evaluation and durable result path. */
  public void deleteRange(Rpc.DeleteRangeRequest request, StreamObserver<Rpc.DeleteRangeResponse> observer) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(observer, "observer");
    var transaction = Rpc.TxnRequest.newBuilder()
        .addSuccess(Rpc.RequestOp.newBuilder().setRequestDeleteRange(request)).build();
    accept(transaction, request, null, new StreamObserver<>() {
      @Override public void onNext(Rpc.TxnResponse response) {
        if (!response.hasHeader() || response.getResponsesCount() != 1
            || !response.getResponses(0).hasResponseDeleteRange()) {
          throw new IllegalStateException("standalone delete result differs from its request");
        }
        observer.onNext(response.getResponses(0).getResponseDeleteRange().toBuilder()
            .setHeader(response.getHeader()).build());
      }
      @Override public void onError(Throwable failure) { observer.onError(failure); }
      @Override public void onCompleted() { observer.onCompleted(); }
    });
  }

  private void accept(Rpc.TxnRequest request, Rpc.DeleteRangeRequest delete, Rpc.RangeRequest range,
      StreamObserver<Rpc.TxnResponse> observer) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(observer, "observer");
    RequestScope scope = RequestScopeInterceptor.currentScope();
    var terminal = new AtomicBoolean();
    var gatewayDrained = new CompletableFuture<Void>();
    var dispatchState = new AtomicInteger();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(gatewayDrained))) return;
    try {
      BudgetManager.Account account = budgets.admit(tableGroup, scope, reservation);
      long inputBytes = Math.addExact(1024L, Math.multiplyExact(4L, request.getSerializedSize()));
      var input = account.tryRetain(BudgetManager.Memory.CANDIDATE, inputBytes)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      PutCommitPreparation.retainUntilDrain(scope, input);
      worker.execute(() -> {
        if (dispatchState.compareAndSet(0, 1)) begin(request, delete, range, observer, scope, account, terminal, gatewayDrained);
      });
    } catch (BackendFailure | RuntimeException failure) {
      if (dispatchState.compareAndSet(0, 2)) {
        gatewayDrained.complete(null);
        signalError(scope, observer, terminal, failure);
      }
    } catch (Error fatal) {
      if (dispatchState.compareAndSet(0, 2)) {
        gatewayDrained.complete(null);
        signalError(scope, observer, terminal, fatal);
      }
      throw fatal;
    }
  }

  private void begin(Rpc.TxnRequest request, Rpc.DeleteRangeRequest delete, Rpc.RangeRequest range,
      StreamObserver<Rpc.TxnResponse> observer,
      RequestScope scope, BudgetManager.Account account, AtomicBoolean terminal,
      CompletableFuture<Void> gatewayDrained) {
    try {
      TxnCommand command = range == null ? TxnRequestMapper.fromWire(request)
          : new TxnCommand(List.of(), List.of(new TxnCommand.Range(
              RangeRequestMapper.fromWire(range, RangeCommand.Context.STANDALONE))), List.of());
      InvocationId invocation = InvocationId.create();
      if (delete != null) {
        DeleteProtocolLimit.requireWithin(delete, invocation.value().getMostSignificantBits(), maximumEtcdRequestBytes);
      } else if (TxnRequestMapper.requiresNativeRaftLimit(request)) {
        TxnProtocolLimit.requireWithin(request, invocation.value().getMostSignificantBits(), maximumEtcdRequestBytes);
      }
      if (!command.mayWrite()) {
        reads.execute(command, authorizationDigest, identity, scope, account, range == null).whenComplete((result, failure) ->
            dispatchResponse(observer, scope, terminal, gatewayDrained, result, failure, range == null,
                value -> TxnResponseEmitter.sendReadOnly(command, value, clusterId, memberId, scope, account,
                    countOutput(observer, delete == null && range == null)),
                ignored -> { }));
        return;
      }
      if (range != null) throw new IllegalStateException("standalone range cannot mutate");
      ByteKey operation = delete == null ? MutationOperationEnvelope.transaction(request.toByteArray())
          : MutationOperationEnvelope.deleteRange(delete.toByteArray());
      ByteKey digest = MutationForwardRequest.digest(tableGroup, operation);
      control.read(scope).thenCompose(observed -> {
        var state = observed.orElseThrow(() ->
            new IllegalStateException("authoritative commit control state is missing")).state();
        long remaining = Math.min(scope.remainingDeadlineNanos(), MutationForwardRequest.MAX_TIMEOUT_NANOS);
        if (state.ownerInstance().isEmpty() || remaining <= 0) {
          return CompletableFuture.failedFuture(new BackendFailure(remaining <= 0
              ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.UNAVAILABLE));
        }
        var envelope = new MutationForwardRequest(tableGroup, invocation, digest, authorizationDigest,
            ByteKey.copyOf(identity.contextRef().getBytes(StandardCharsets.UTF_8)), state.ownerEpoch(),
            remaining, 0, operation);
        return relay.forward(envelope, scope);
      }).thenCompose(reply -> resolveReply(reply, invocation, digest, scope, account))
          .whenComplete((resolved, failure) -> dispatchResponse(observer, scope, terminal, gatewayDrained,
              resolved, failure, true, value -> TxnResponseEmitter.sendResolved(command, value,
                  ResultMessages::decodeTree, clusterId, memberId, scope, account, countOutput(observer, delete == null)),
              value -> { if (value != null) value.close(); }));
    } catch (RuntimeException failure) {
      gatewayDrained.complete(null);
      signalError(scope, observer, terminal, failure);
    } catch (Error fatal) {
      gatewayDrained.complete(null);
      signalError(scope, observer, terminal, fatal);
      throw fatal;
    }
  }

  private CompletableFuture<InvocationResultResolver.ResolvedTransaction> resolveReply(
      MutationRelay.Result reply, InvocationId invocation, ByteKey digest,
      RequestScope scope, BudgetManager.Account account) {
    if (reply instanceof MutationRelay.KnownResult known) {
      if (!Arrays.equals(known.responseBytes(), invocation.bytes())) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return resolveOriginal(invocation, digest, scope, account);
    }
    if (reply instanceof MutationRelay.SemanticRejection semantic) {
      return CompletableFuture.failedFuture(new KvFailure(semantic.reason()));
    }
    if (reply instanceof MutationRelay.KnownRejection rejected) {
      return CompletableFuture.failedFuture(new BackendFailure(rejected.category()));
    }
    if (!(reply instanceof MutationRelay.ResultUnknown unknown)) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
    }
    return resolveOriginal(invocation, digest, scope, account)
        .exceptionallyCompose(failure -> {
          Throwable cause = CompletionFailures.unwrapCompletion(failure);
          if (cause instanceof Error fatal) return CompletableFuture.failedFuture(fatal);
          return CompletableFuture.failedFuture(new BackendFailure(unknown.category(), cause));
        });
  }

  private CompletableFuture<InvocationResultResolver.ResolvedTransaction> resolveOriginal(
      InvocationId invocation, ByteKey digest, RequestScope scope, BudgetManager.Account account) {
    long started = System.nanoTime();
    return results.resolveTransaction(invocation, digest, authorizationDigest, scope, account)
        .whenComplete((ignored, failure) -> metrics.recordTxnStage(
            RequestMetrics.TxnStage.RESULT_RESOLUTION, Math.max(0, System.nanoTime() - started)));
  }

  private <T> void dispatchResponse(StreamObserver<Rpc.TxnResponse> observer, RequestScope scope,
      AtomicBoolean terminal, CompletableFuture<Void> gatewayDrained, T value, Throwable failure,
      boolean recordTxnMetrics, Consumer<T> send, Consumer<T> discard) {
    var responseDrained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(responseDrained))) {
      try { discard.accept(value); }
      finally {
        try {
          // A failed read may close its scope to release pins before reporting the failure.
          // The transport still needs a terminal error if it has not already closed.
          if (failure != null) signalCompletionError(observer, terminal, failure);
        } finally {
          gatewayDrained.complete(null);
        }
      }
      return;
    }
    var dispatchState = new AtomicInteger();
    try {
      scheduleResponse(observer, scope, terminal, responseDrained, value, failure, recordTxnMetrics, send, discard, dispatchState);
    } finally {
      gatewayDrained.complete(null);
    }
  }

  private <T> void scheduleResponse(StreamObserver<Rpc.TxnResponse> observer, RequestScope scope,
      AtomicBoolean terminal, CompletableFuture<Void> responseDrained, T value, Throwable failure,
      boolean recordTxnMetrics, Consumer<T> send, Consumer<T> discard, AtomicInteger dispatchState) {
    try {
      worker.execute(() -> {
        if (dispatchState.compareAndSet(0, 1)) {
          deliverResponse(observer, scope, terminal, responseDrained, value, failure, recordTxnMetrics, send, discard);
        }
      });
    } catch (RuntimeException rejected) {
      if (dispatchState.compareAndSet(0, 2)) {
        rejectResponse(observer, scope, terminal, responseDrained, value, discard, rejected, true);
      }
    } catch (Error fatal) {
      if (dispatchState.compareAndSet(0, 2)) {
        rejectResponse(observer, scope, terminal, responseDrained, value, discard, fatal, false);
      }
      throw fatal;
    }
  }

  private <T> void deliverResponse(StreamObserver<Rpc.TxnResponse> observer, RequestScope scope,
      AtomicBoolean terminal, CompletableFuture<Void> responseDrained, T value, Throwable failure,
      boolean recordTxnMetrics, Consumer<T> send, Consumer<T> discard) {
    try {
      if (failure != null) signalCompletionError(observer, terminal, failure);
      else if (terminal.compareAndSet(false, true)) {
        long started = System.nanoTime();
        try { send.accept(value); }
        finally { if (recordTxnMetrics) metrics.recordTxnStage(RequestMetrics.TxnStage.RESPONSE, Math.max(0, System.nanoTime() - started)); }
      } else discard.accept(value);
    } finally {
      responseDrained.complete(null);
    }
  }

  private static <T> void rejectResponse(StreamObserver<Rpc.TxnResponse> observer, RequestScope scope,
      AtomicBoolean terminal, CompletableFuture<Void> responseDrained, T value, Consumer<T> discard,
      Throwable failure, boolean rejected) {
    try { discard.accept(value); }
    finally {
      responseDrained.complete(null);
      signalError(scope, observer, terminal, rejected
          ? new BackendFailure(BackendFailure.Category.UNKNOWN, failure) : failure);
    }
  }

  private StreamObserver<Rpc.TxnResponse> countOutput(StreamObserver<Rpc.TxnResponse> observer, boolean txnRpc) {
    return new StreamObserver<>() {
      @Override public void onNext(Rpc.TxnResponse response) {
        observer.onNext(response);
        if (txnRpc) metrics.recordTxnOutputBytes(response.getSerializedSize());
      }
      @Override public void onError(Throwable failure) { observer.onError(failure); }
      @Override public void onCompleted() { observer.onCompleted(); }
    };
  }

  private static void signalError(RequestScope scope, StreamObserver<Rpc.TxnResponse> observer,
      AtomicBoolean terminal, Throwable failure) {
    if (scope.isClosed()) return;
    signalCompletionError(observer, terminal, failure);
  }

  private static void signalCompletionError(StreamObserver<Rpc.TxnResponse> observer,
      AtomicBoolean terminal, Throwable failure) {
    if (!terminal.compareAndSet(false, true)) return;
    observer.onError(ErrorProjection.project(failure));
  }
}
