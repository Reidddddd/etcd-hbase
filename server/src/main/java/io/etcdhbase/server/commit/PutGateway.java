package io.etcdhbase.server.commit;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.grpc.protocol.ErrorProjection;
import io.etcdhbase.grpc.protocol.PutRequestMapper;
import io.etcdhbase.grpc.protocol.PutProtocolLimit;
import io.etcdhbase.grpc.protocol.PutResponseEmitter;
import io.etcdhbase.hbase.codec.ResultMessages;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/** Adapts a Put RPC to an authorized owner route and its original finalized result. */
public final class PutGateway implements BiConsumer<Rpc.PutRequest, StreamObserver<Rpc.PutResponse>> {
  private final String tableGroup;
  private final AuthorizedBackendIdentity identity;
  private final ByteKey authorizationDigest;
  private final long clusterId;
  private final long memberId;
  private final int maximumEtcdRequestBytes;
  private final CommitControlStore control;
  private final MutationRelay relay;
  private final InvocationResultResolver results;
  private final BudgetManager budgets;
  private final BudgetManager.Reservation reservation;
  private final Executor worker;
  private final RequestMetrics metrics;

  /** The caller supplies the actual serving identity and a bounded, lifecycle-owned worker. */
  public PutGateway(String tableGroup, AuthorizedBackendIdentity identity, ByteKey authorizationDigest,
      long clusterId, long memberId, int maximumEtcdRequestBytes,
      CommitControlStore control, MutationRelay relay,
      InvocationResultResolver results, BudgetManager budgets, BudgetManager.Reservation reservation,
      Executor worker) {
    this(tableGroup, identity, authorizationDigest, clusterId, memberId, maximumEtcdRequestBytes,
        control, relay, results, budgets, reservation, worker, new RequestMetrics());
  }

  public PutGateway(String tableGroup, AuthorizedBackendIdentity identity, ByteKey authorizationDigest,
      long clusterId, long memberId, int maximumEtcdRequestBytes,
      CommitControlStore control, MutationRelay relay,
      InvocationResultResolver results, BudgetManager budgets, BudgetManager.Reservation reservation,
      Executor worker, RequestMetrics metrics) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.identity = Objects.requireNonNull(identity, "identity");
    this.authorizationDigest = Objects.requireNonNull(authorizationDigest, "authorizationDigest");
    this.control = Objects.requireNonNull(control, "control");
    this.relay = Objects.requireNonNull(relay, "relay");
    this.results = Objects.requireNonNull(results, "results");
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.reservation = Objects.requireNonNull(reservation, "reservation");
    this.worker = Objects.requireNonNull(worker, "worker");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    if (!identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)
        || authorizationDigest.length() != 32 || clusterId == 0 || memberId == 0
        || maximumEtcdRequestBytes < 1
        || maximumEtcdRequestBytes > MutationForwardRequest.MAX_OPERATION_BYTES) {
      throw new IllegalArgumentException("invalid Put gateway identity");
    }
    this.clusterId = clusterId;
    this.memberId = memberId;
    this.maximumEtcdRequestBytes = maximumEtcdRequestBytes;
  }

  @Override
  public void accept(Rpc.PutRequest request, StreamObserver<Rpc.PutResponse> observer) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(observer, "observer");
    RequestScope scope = RequestScopeInterceptor.currentScope();
    var terminal = new AtomicBoolean();
    var gatewayDrained = new CompletableFuture<Void>();
    var dispatchState = new AtomicInteger();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(gatewayDrained))) return;
    try {
      scheduleRequest(request, observer, scope, terminal, gatewayDrained, dispatchState);
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

  private void scheduleRequest(Rpc.PutRequest request, StreamObserver<Rpc.PutResponse> observer,
      RequestScope scope, AtomicBoolean terminal, CompletableFuture<Void> gatewayDrained,
      AtomicInteger dispatchState) throws BackendFailure {
    BudgetManager.Account account = budgets.admit(tableGroup, scope, reservation);
    long inputBytes = Math.addExact(1024L, Math.multiplyExact(4L, request.getSerializedSize()));
    var inputLease = account.tryRetain(BudgetManager.Memory.CANDIDATE, inputBytes)
        .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    PutCommitPreparation.retainUntilDrain(scope, inputLease);
    worker.execute(() -> {
      if (dispatchState.compareAndSet(0, 1)) {
        begin(request, observer, scope, account, terminal, gatewayDrained);
      }
    });
  }

  private void begin(Rpc.PutRequest request, StreamObserver<Rpc.PutResponse> observer,
      RequestScope scope, BudgetManager.Account account, AtomicBoolean terminal,
      CompletableFuture<Void> gatewayDrained) {
    try {
      PutCommand command = PutRequestMapper.fromWire(request);
      InvocationId invocation = InvocationId.create();
      PutProtocolLimit.requireWithin(request, invocation.value().getMostSignificantBits(),
          maximumEtcdRequestBytes);
      ByteKey operation = ByteKey.copyOf(request.toByteArray());
      if (operation.length() > MutationForwardRequest.MAX_OPERATION_BYTES) {
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
      ByteKey digest = MutationForwardRequest.digest(tableGroup, operation);
      CompletableFuture<InvocationResultResolver.ResolvedResult> result = control.read(scope)
          .thenCompose(observed -> {
            var state = observed.orElseThrow(() ->
                new IllegalStateException("authoritative commit control state is missing")).state();
            long remaining = Math.min(scope.remainingDeadlineNanos(), MutationForwardRequest.MAX_TIMEOUT_NANOS);
            if (state.ownerInstance().isEmpty() || remaining <= 0) {
              return CompletableFuture.failedFuture(new BackendFailure(remaining <= 0
                  ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.UNAVAILABLE));
            }
            var envelope = new MutationForwardRequest(tableGroup, invocation, digest,
                authorizationDigest, ByteKey.copyOf(identity.contextRef().getBytes(StandardCharsets.UTF_8)),
                state.ownerEpoch(), remaining, 0, operation);
            return relay.forward(envelope, scope);
          }).thenCompose(reply -> resolveReply(reply, invocation, digest, scope, account));
      result.whenComplete((resolved, failure) -> dispatchResponse(command, observer, scope, account,
          terminal, gatewayDrained, resolved, failure));
    } catch (BackendFailure | RuntimeException failure) {
      gatewayDrained.complete(null);
      signalError(scope, observer, terminal, failure);
    } catch (Error fatal) {
      gatewayDrained.complete(null);
      signalError(scope, observer, terminal, fatal);
      throw fatal;
    }
  }

  private CompletableFuture<InvocationResultResolver.ResolvedResult> resolveReply(
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
    if (reply instanceof MutationRelay.KnownRejection rejection) {
      return CompletableFuture.failedFuture(new BackendFailure(rejection.category()));
    }
    if (!(reply instanceof MutationRelay.ResultUnknown unknown)) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
    }
    // A lost owner reply may follow a completed publication. Resolve this same invocation;
    // never submit the mutation again or infer success from an incomplete decision.
    return resolveOriginal(invocation, digest, scope, account)
        .exceptionallyCompose(failure -> {
          Throwable cause = CompletionFailures.unwrapCompletion(failure);
          if (cause instanceof Error fatal) return CompletableFuture.failedFuture(fatal);
          return CompletableFuture.failedFuture(new BackendFailure(unknown.category(), cause));
        });
  }

  private CompletableFuture<InvocationResultResolver.ResolvedResult> resolveOriginal(
      InvocationId invocation, ByteKey digest, RequestScope scope, BudgetManager.Account account) {
    long started = System.nanoTime();
    return results.resolve(invocation, digest, authorizationDigest, scope, account)
        .whenComplete((ignored, failure) -> metrics.recordPutStage(
            RequestMetrics.PutStage.RESULT_RESOLUTION, Math.max(0, System.nanoTime() - started)));
  }

  private void dispatchResponse(PutCommand command, StreamObserver<Rpc.PutResponse> observer,
      RequestScope scope, BudgetManager.Account account, AtomicBoolean terminal,
      CompletableFuture<Void> gatewayDrained, InvocationResultResolver.ResolvedResult resolved,
      Throwable failure) {
    var responseDrained = new CompletableFuture<Void>();
    boolean open = scope.startIfOpen(() -> scope.trackPhysicalProducer(responseDrained));
    if (!open) {
      if (resolved != null) resolved.close();
      gatewayDrained.complete(null);
      return;
    }
    var dispatchState = new AtomicInteger();
    try {
      scheduleResponse(command, observer, scope, account, terminal, responseDrained,
          resolved, failure, dispatchState);
    } finally {
      gatewayDrained.complete(null);
    }
  }

  private void scheduleResponse(PutCommand command, StreamObserver<Rpc.PutResponse> observer,
      RequestScope scope, BudgetManager.Account account, AtomicBoolean terminal,
      CompletableFuture<Void> responseDrained,
      InvocationResultResolver.ResolvedResult resolved, Throwable failure,
      AtomicInteger dispatchState) {
    try {
      worker.execute(() -> {
        if (!dispatchState.compareAndSet(0, 1)) return;
        deliverResponse(command, observer, scope, account, terminal, responseDrained, resolved, failure);
      });
    } catch (RuntimeException rejected) {
      if (dispatchState.compareAndSet(0, 2)) {
        if (resolved != null) resolved.close();
        responseDrained.complete(null);
        signalError(scope, observer, terminal, new BackendFailure(BackendFailure.Category.UNKNOWN, rejected));
      }
    } catch (Error fatal) {
      if (dispatchState.compareAndSet(0, 2)) {
        if (resolved != null) resolved.close();
        responseDrained.complete(null);
        signalError(scope, observer, terminal, fatal);
      }
      throw fatal;
    }
  }

  private void deliverResponse(PutCommand command, StreamObserver<Rpc.PutResponse> observer,
      RequestScope scope, BudgetManager.Account account, AtomicBoolean terminal,
      CompletableFuture<Void> responseDrained,
      InvocationResultResolver.ResolvedResult resolved, Throwable failure) {
    try {
      if (failure != null) {
        signalError(scope, observer, terminal, failure);
      } else if (terminal.compareAndSet(false, true)) {
        long started = System.nanoTime();
        try {
          PutResponseEmitter.send(command, resolved, ResultMessages::decodePut,
              clusterId, memberId, scope, account, countOutput(observer));
        } finally {
          metrics.recordPutStage(RequestMetrics.PutStage.RESPONSE,
              Math.max(0, System.nanoTime() - started));
        }
      } else if (resolved != null) {
        resolved.close();
      }
    } finally {
      responseDrained.complete(null);
    }
  }

  private StreamObserver<Rpc.PutResponse> countOutput(StreamObserver<Rpc.PutResponse> observer) {
    return new StreamObserver<>() {
      @Override public void onNext(Rpc.PutResponse response) {
        observer.onNext(response);
        metrics.recordPutOutputBytes(response.getSerializedSize());
      }
      @Override public void onError(Throwable failure) { observer.onError(failure); }
      @Override public void onCompleted() { observer.onCompleted(); }
    };
  }

  private static void signalError(RequestScope scope, StreamObserver<Rpc.PutResponse> observer,
      AtomicBoolean terminal, Throwable failure) {
    if (scope.isClosed() || !terminal.compareAndSet(false, true)) return;
    observer.onError(ErrorProjection.project(failure));
  }
}
