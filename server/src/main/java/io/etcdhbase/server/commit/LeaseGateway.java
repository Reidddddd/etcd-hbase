package io.etcdhbase.server.commit;

import etcdserverpb.Rpc;
import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.commit.LeaseBindingReader;
import io.etcdhbase.core.commit.LeaseEnumerationReader;
import io.etcdhbase.core.commit.LeaseReadView;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.PinnedReadView;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.lease.LeaseFailure;
import io.etcdhbase.core.lease.LeaseRenewalVersion;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeRules;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.grpc.protocol.ErrorProjection;
import io.etcdhbase.grpc.protocol.LeaseGrantResponseMapper;
import io.etcdhbase.grpc.protocol.LeaseTimeToLiveResponseMapper;
import io.etcdhbase.grpc.protocol.ResponseHeaderFactory;
import io.etcdhbase.server.config.M3MemoryReservations;
import io.etcdhbase.hbase.codec.LeaseGrantResultMessages;
import io.etcdhbase.hbase.codec.LeaseKeepAliveResultMessages;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.server.auth.AuthSessionService;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** Adapts the implemented Lease unary methods to one table group's bounded runtime. */
public final class LeaseGateway {
  private record KeepAliveResolution(
      AuthenticatedRequestIdentityResolver.ResolvedRequestIdentity requestIdentity,
      InvocationResultResolver.ResolvedInvocation result) {}

  private record AuthorizedRead<T>(T response,
      AuthenticatedRequestIdentityResolver.ResolvedRequestIdentity identity) {}

  @FunctionalInterface
  private interface AuthenticatedRead<T> {
    CompletableFuture<T> apply(
        AuthenticatedRequestIdentityResolver.ResolvedRequestIdentity requestIdentity,
        RequestScope scope, BudgetManager.Account account);
  }

  private static final int MAX_EXAMINED_LEASES = 100_000;
  private static final long MAX_METADATA_BYTES = 64L << 20;
  private static final long OUTPUT_OBJECT_ALLOWANCE = 256;
  private static final java.util.concurrent.Executor CLEANUP =
      task -> Thread.ofVirtual().name("m4-keepalive-cleanup").start(task);
  static final int BINDING_PAGE_ENTRIES = M3MemoryReservations.LEASE_BINDING_PAGE_ENTRIES;
  private static final long BINDING_PAGE_OVERHEAD_BYTES = 64L << 10;

  private final String tableGroup;
  private final AuthorizedBackendIdentity identity;
  private final ByteKey authorizationDigest;
  private final long clusterId;
  private final long memberId;
  private final int maximumEtcdRequestBytes;
  private final CommitControlStore control;
  private final MutationRelay relay;
  private final InvocationResultResolver results;
  private final ReadViewService views;
  private final LeaseReadView leaseReads;
  private final LeaseEnumerationReader leaseEnumeration;
  private final LeaseBindingReader leaseBindings;
  private final LeaseTimeSource time;
  private final BudgetManager budgets;
  private final BudgetManager.Reservation reservation;
  private final BudgetManager.Reservation bindingReadReservation;
  private final Executor worker;
  private final ScheduledExecutorService timers;
  private final RequestMetrics metrics;
  private final AuthenticatedRequestIdentityResolver requestIdentities;

  public LeaseGateway(String tableGroup, AuthorizedBackendIdentity identity, ByteKey authorizationDigest,
      long clusterId, long memberId, int maximumEtcdRequestBytes, CommitControlStore control,
      MutationRelay relay, InvocationResultResolver results, ReadViewService views,
      LeaseReadView leaseReads, LeaseEnumerationReader leaseEnumeration,
      LeaseBindingReader leaseBindings, LeaseTimeSource time,
      BudgetManager budgets, BudgetManager.Reservation reservation, Executor worker,
      ScheduledExecutorService timers, RequestMetrics metrics) {
    this(tableGroup, identity, authorizationDigest, clusterId, memberId, maximumEtcdRequestBytes,
        control, relay, results, views, leaseReads, leaseEnumeration, leaseBindings, time,
        budgets, reservation, worker, timers, metrics, null);
  }

  /** Authenticated Lease RPCs resolve an independent session borrow before entering the gateway. */
  public LeaseGateway(String tableGroup, AuthorizedBackendIdentity identity, ByteKey authorizationDigest,
      long clusterId, long memberId, int maximumEtcdRequestBytes, CommitControlStore control,
      MutationRelay relay, InvocationResultResolver results, ReadViewService views,
      LeaseReadView leaseReads, LeaseEnumerationReader leaseEnumeration,
      LeaseBindingReader leaseBindings, LeaseTimeSource time,
      BudgetManager budgets, BudgetManager.Reservation reservation, Executor worker,
      ScheduledExecutorService timers, RequestMetrics metrics, AuthSessionService authSessions) {
    this(tableGroup, identity, authorizationDigest, clusterId, memberId, maximumEtcdRequestBytes,
        control, relay, results, views, leaseReads, leaseEnumeration, leaseBindings, time,
        budgets, reservation, worker, timers, metrics, authSessions, null);
  }

  public LeaseGateway(String tableGroup, AuthorizedBackendIdentity identity, ByteKey authorizationDigest,
      long clusterId, long memberId, int maximumEtcdRequestBytes, CommitControlStore control,
      MutationRelay relay, InvocationResultResolver results, ReadViewService views,
      LeaseReadView leaseReads, LeaseEnumerationReader leaseEnumeration,
      LeaseBindingReader leaseBindings, LeaseTimeSource time,
      BudgetManager budgets, BudgetManager.Reservation reservation, Executor worker,
      ScheduledExecutorService timers, RequestMetrics metrics, AuthSessionService authSessions,
      Consumer<ByteKey> invalidateReadAuthorization) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.identity = Objects.requireNonNull(identity, "identity");
    this.authorizationDigest = Objects.requireNonNull(authorizationDigest, "authorizationDigest");
    this.control = Objects.requireNonNull(control, "control");
    this.relay = Objects.requireNonNull(relay, "relay");
    this.results = Objects.requireNonNull(results, "results");
    this.views = Objects.requireNonNull(views, "views");
    this.leaseReads = Objects.requireNonNull(leaseReads, "leaseReads");
    this.leaseEnumeration = Objects.requireNonNull(leaseEnumeration, "leaseEnumeration");
    this.leaseBindings = Objects.requireNonNull(leaseBindings, "leaseBindings");
    this.time = Objects.requireNonNull(time, "time");
    this.budgets = Objects.requireNonNull(budgets, "budgets");
    this.reservation = Objects.requireNonNull(reservation, "reservation");
    this.worker = Objects.requireNonNull(worker, "worker");
    this.timers = Objects.requireNonNull(timers, "timers");
    if (tableGroup.isBlank() || authorizationDigest.length() != 32 || clusterId == 0 || memberId == 0
        || maximumEtcdRequestBytes < 1
        || maximumEtcdRequestBytes > MutationForwardRequest.MAX_OPERATION_BYTES
        || !identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)) {
      throw new IllegalArgumentException("invalid Lease gateway identity or request limit");
    }
    this.clusterId = clusterId;
    this.memberId = memberId;
    this.maximumEtcdRequestBytes = maximumEtcdRequestBytes;
    this.bindingReadReservation = bindingReadReservation(reservation, maximumEtcdRequestBytes);
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.requestIdentities = new AuthenticatedRequestIdentityResolver(tableGroup, identity,
        authorizationDigest, authSessions, invalidateReadAuthorization);
  }

  static long bindingPageBytes(int maximumEtcdRequestBytes) {
    if (maximumEtcdRequestBytes < 1 || maximumEtcdRequestBytes > MutationForwardRequest.MAX_OPERATION_BYTES) {
      throw new IllegalArgumentException("invalid binding request limit");
    }
    return M3MemoryReservations.leaseBindingPageBytes(maximumEtcdRequestBytes);
  }

  static BudgetManager.Reservation bindingReadReservation(
      BudgetManager.Reservation base, int maximumEtcdRequestBytes) {
    return M3MemoryReservations.leaseBindingRead(base, maximumEtcdRequestBytes);
  }

  public BiConsumer<Rpc.LeaseGrantRequest, StreamObserver<Rpc.LeaseGrantResponse>> grantHandler() {
    return this::grant;
  }

  public BiConsumer<Rpc.LeaseRevokeRequest, StreamObserver<Rpc.LeaseRevokeResponse>> revokeHandler() {
    return this::revoke;
  }

  public BiConsumer<Rpc.LeaseTimeToLiveRequest, StreamObserver<Rpc.LeaseTimeToLiveResponse>>
      timeToLiveHandler() {
    return this::timeToLive;
  }

  public BiConsumer<Rpc.LeaseLeasesRequest, StreamObserver<Rpc.LeaseLeasesResponse>> leasesHandler() {
    return this::leases;
  }

  public Function<StreamObserver<Rpc.LeaseKeepAliveResponse>,
      StreamObserver<Rpc.LeaseKeepAliveRequest>> keepAliveHandler() {
    return observer -> {
      RequestScope streamScope = RequestScopeInterceptor.currentScope();
      var authToken = RequestScopeInterceptor.currentAuthToken();
      return new LeaseKeepAliveStream(observer, streamScope, worker, timers,
          request -> keepAlive(request, streamScope, authToken), metrics);
    };
  }

  private void grant(Rpc.LeaseGrantRequest request, StreamObserver<Rpc.LeaseGrantResponse> observer) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(observer, "observer");
    RequestScope scope = RequestScopeInterceptor.currentScope();
    var authToken = RequestScopeInterceptor.currentAuthToken();
    var terminal = new AtomicBoolean();
    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) return;
    BudgetManager.Account account;
    try {
      account = budgets.admit(tableGroup, scope, reservation);
      retainInput(scope, account, request.getSerializedSize());
      if (request.getSerializedSize() > maximumEtcdRequestBytes) {
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
      worker.execute(() -> beginGrant(request, observer, scope, authToken, account, terminal, drained));
    } catch (BackendFailure | RuntimeException failure) {
      drained.complete(null);
      signalError(scope, observer, terminal, failure);
    } catch (Error fatal) {
      drained.complete(null);
      signalError(scope, observer, terminal, fatal);
      throw fatal;
    }
  }

  private void revoke(Rpc.LeaseRevokeRequest request, StreamObserver<Rpc.LeaseRevokeResponse> observer) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(observer, "observer");
    RequestScope scope = RequestScopeInterceptor.currentScope();
    var authToken = RequestScopeInterceptor.currentAuthToken();
    var terminal = new AtomicBoolean();
    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) return;
    BudgetManager.Account account;
    try {
      account = budgets.admit(tableGroup, scope, reservation);
      retainInput(scope, account, request.getSerializedSize());
      if (request.getSerializedSize() > maximumEtcdRequestBytes) {
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
      worker.execute(() -> beginRevoke(request, observer, scope, authToken, account, terminal, drained));
    } catch (BackendFailure | RuntimeException failure) {
      drained.complete(null);
      signalError(scope, observer, terminal, failure);
    } catch (Error fatal) {
      drained.complete(null);
      signalError(scope, observer, terminal, fatal);
      throw fatal;
    }
  }

  private CompletableFuture<LeaseKeepAliveStream.Delivery> keepAlive(
      Rpc.LeaseKeepAliveRequest request, RequestScope streamScope,
      java.util.Optional<String> authToken) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(streamScope, "streamScope");
    RequestScope operationScope = streamScope.childWithTimeoutNanos(
        MutationForwardRequest.MAX_TIMEOUT_NANOS);
    CompletableFuture<Void> deliveryComplete = new CompletableFuture<>();
    operationScope.trackPhysicalProducer(deliveryComplete);
    CompletableFuture<Void> operationDrained = CompletableFuture.allOf(
        operationScope.cleanupCompletion().toCompletableFuture(),
        operationScope.physicalProducersCompletion().toCompletableFuture());
    var parentRegistration = streamScope.registerScopedCleanup(
        () -> {
          if (streamScope.isCancelled()) operationScope.cancelAsync(CLEANUP);
          else operationScope.closeAsync(CLEANUP);
        }, operationDrained);
    var finished = new AtomicBoolean();
    Runnable finish = () -> {
      if (!finished.compareAndSet(false, true)) return;
      deliveryComplete.complete(null);
      operationScope.closeAsync(CLEANUP);
      try {
        parentRegistration.close();
      } catch (Exception failure) {
        // The child scope owns cleanup; the interceptor retains its drain registration.
      }
    };

    CompletableFuture<LeaseKeepAliveStream.Delivery> result = new CompletableFuture<>();
    BudgetManager.Account account = null;
    try {
      account = budgets.admit(tableGroup, operationScope, reservation);
      retainInput(operationScope, account, request.getSerializedSize());
      if (request.getSerializedSize() > maximumEtcdRequestBytes) {
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
      metrics.recordLeaseRenewalRequest(request.getSerializedSize());
    } catch (BackendFailure | RuntimeException failure) {
      if (account != null) account.close();
      finish.run();
      result.completeExceptionally(failure);
      return result;
    } catch (Error fatal) {
      if (account != null) account.close();
      finish.run();
      throw fatal;
    }

    BudgetManager.Account admitted = account;
    try {
      worker.execute(() -> beginKeepAlive(request, operationScope, authToken, admitted, result, finish));
    } catch (RuntimeException rejected) {
      admitted.close();
      finish.run();
      result.completeExceptionally(
          new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT, rejected));
    } catch (Error fatal) {
      admitted.close();
      finish.run();
      throw fatal;
    }
    return result;
  }

  private void beginKeepAlive(Rpc.LeaseKeepAliveRequest request, RequestScope scope,
      java.util.Optional<String> authToken, BudgetManager.Account account,
      CompletableFuture<LeaseKeepAliveStream.Delivery> result, Runnable finish) {
    InvocationId invocation = InvocationId.create();
    ByteKey operation = MutationOperationEnvelope.leaseKeepAlive(request.toByteArray());
    ByteKey digest = MutationForwardRequest.digest(tableGroup, operation);
    try {
      requestIdentities.resolve(authToken, scope).thenCompose(requestIdentity -> {
        requestIdentity.releaseAfterPhysicalDrain(scope);
        return control.read(scope).thenCompose(snapshot -> {
          var state = snapshot.orElseThrow(() ->
              new IllegalStateException("authoritative commit control state is missing")).state();
          long remaining = Math.min(scope.remainingDeadlineNanos(), MutationForwardRequest.MAX_TIMEOUT_NANOS);
          if (state.ownerInstance().isEmpty() || remaining <= 0) {
            return CompletableFuture.failedFuture(new BackendFailure(remaining <= 0
                ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.UNAVAILABLE));
          }
          var forwarded = new MutationForwardRequest(tableGroup, invocation, digest,
              requestIdentity.authorizationDigest(), requestIdentity.authorizationReference(),
              state.ownerEpoch(), remaining, 0, operation);
          return relay.forward(forwarded, scope).thenCompose(reply -> resolveKeepAliveReply(
              reply, invocation, digest, requestIdentity.authorizationDigest(), scope, account))
              .thenApply(resolved -> new KeepAliveResolution(requestIdentity, resolved));
        });
      })
          .whenComplete((resolved, failure) -> {
            if (!scope.startIfOpen(() -> {})) {
              if (resolved != null) resolved.result().close();
              account.close();
              finish.run();
              result.completeExceptionally(new BackendFailure(scope.isDeadlineExceeded()
                  ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
              return;
            }
            if (failure != null) {
              account.close();
              finish.run();
              result.completeExceptionally(CompletionFailures.unwrapCompletion(failure));
              return;
            }
            projectKeepAliveResponse(request, resolved.requestIdentity(), resolved.result(),
                scope, account, result, finish);
          });
    } catch (RuntimeException | Error failure) {
      account.close();
      finish.run();
      result.completeExceptionally(failure);
      if (failure instanceof Error fatal) throw fatal;
    }
  }

  private CompletableFuture<InvocationResultResolver.ResolvedInvocation> resolveKeepAliveReply(
      MutationRelay.Result reply, InvocationId invocation, ByteKey digest,
      ByteKey requestAuthorizationDigest, RequestScope scope, BudgetManager.Account account) {
    if (reply instanceof MutationRelay.KnownResult known) {
      if (!java.util.Arrays.equals(known.responseBytes(), invocation.bytes())) {
        metrics.recordLeaseRenewalUnknownOutcome();
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return resolveKeepAliveOriginal(invocation, digest, requestAuthorizationDigest,
          scope, account, false);
    }
    if (reply instanceof MutationRelay.LeaseRejection rejection) {
      return CompletableFuture.failedFuture(new LeaseFailure(rejection.reason()));
    }
    if (reply instanceof MutationRelay.KnownRejection rejection) {
      return CompletableFuture.failedFuture(new BackendFailure(rejection.category()));
    }
    if (reply instanceof MutationRelay.SemanticRejection) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
    }
    MutationRelay.ResultUnknown unknown = reply instanceof MutationRelay.ResultUnknown value
        ? value : new MutationRelay.ResultUnknown(BackendFailure.Category.UNKNOWN);
    return resolveKeepAliveOriginal(invocation, digest, requestAuthorizationDigest,
        scope, account, true).exceptionallyCompose(failure ->
        CompletableFuture.failedFuture(new BackendFailure(unknown.category(),
            CompletionFailures.unwrapCompletion(failure))));
  }

  private CompletableFuture<InvocationResultResolver.ResolvedInvocation> resolveKeepAliveOriginal(
      InvocationId invocation, ByteKey digest, ByteKey requestAuthorizationDigest, RequestScope scope,
      BudgetManager.Account account, boolean outcomeUnknown) {
    if (outcomeUnknown) metrics.recordLeaseRenewalUnknownOutcome();
    long started = System.nanoTime();
    return results.resolveInvocation(invocation, digest, requestAuthorizationDigest, scope, account)
        .whenComplete((resolved, failure) -> {
      metrics.recordLeaseRenewalStage(RequestMetrics.LeaseRenewalStage.RESULT_RESOLUTION,
          Math.max(0, System.nanoTime() - started));
      if (failure != null && !outcomeUnknown) metrics.recordLeaseRenewalUnknownOutcome();
    });
  }

  private void projectKeepAliveResponse(Rpc.LeaseKeepAliveRequest request,
      AuthenticatedRequestIdentityResolver.ResolvedRequestIdentity requestIdentity,
      InvocationResultResolver.ResolvedInvocation resolved, RequestScope scope,
      BudgetManager.Account account, CompletableFuture<LeaseKeepAliveStream.Delivery> result,
      Runnable finish) {
    Rpc.LeaseKeepAliveResponse response;
    Optional<LeaseRenewalVersion> renewalVersion;
    try (resolved) {
      LeaseKeepAliveResultMessages.Decoded durableResult =
          LeaseKeepAliveResultMessages.decode(resolved.payload().buffer());
      response = Rpc.LeaseKeepAliveResponse.parseFrom(durableResult.response().bytes());
      renewalVersion = durableResult.renewalVersion();
      // An expired KeepAlive may own the atomic revoke that deletes its bound keys. Only a
      // positive renewal promises to leave the KV revision unchanged.
      boolean kvMutationOnPositiveRenewal = response.getTTL() > 0 && resolved.containsKeyChanges();
      if (!response.hasHeader() || response.getID() != request.getID() || response.getTTL() < 0
          || response.getTTL() > LeaseTimeRules.MAX_TTL_SECONDS
          || response.getHeader().getClusterId() != clusterId
          || response.getHeader().getMemberId() == 0
          || response.getHeader().getRevision() != resolved.revision()
          || kvMutationOnPositiveRenewal
          || (response.getTTL() > 0) != renewalVersion.isPresent()
          || (renewalVersion.isPresent()
              && renewalVersion.orElseThrow().identity().leaseId() != response.getID())) {
        throw new BackendFailure(BackendFailure.Category.UNKNOWN);
      }
      // etcd fills this header at the serving RPC endpoint before renewing through its lessor.
      // The durable owner result supplies the revision, while a forwarded response must identify
      // the member that accepted this public stream.
      response = response.toBuilder().setHeader(response.getHeader().toBuilder()
          .setMemberId(memberId)).build();
    } catch (BackendFailure | InvalidProtocolBufferException | RuntimeException failure) {
      failKeepAliveProjection(failure, account, result, finish);
      return;
    } catch (Error fatal) {
      failKeepAliveProjection(fatal, account, result, finish);
      throw fatal;
    }

    if (response.getTTL() == 0) {
      completeKeepAliveDelivery(response, () -> requestIdentities.revalidate(requestIdentity, scope),
          account, result, finish);
      return;
    }
    Rpc.LeaseKeepAliveResponse resolvedResponse = response;
    LeaseRenewalVersion expectedRenewalVersion = renewalVersion.orElseThrow();
    completeKeepAliveDelivery(resolvedResponse,
        () -> requestIdentities.revalidate(requestIdentity, scope)
            .thenCompose(ignored -> readCurrentKeepAliveLease(request.getID(),
                requestIdentity.authorizationDigest(), scope, account))
            .thenAccept(state -> validateKeepAliveLease(state, expectedRenewalVersion,
                resolvedResponse.getTTL(), time.observe())),
        account, result, finish);
  }

  private CompletableFuture<LeaseState> readCurrentKeepAliveLease(long leaseId,
      ByteKey requestAuthorizationDigest, RequestScope scope, BudgetManager.Account account) {
    var proof = new ManifestCatalog.ProofReadBudget(scope, account, 64, MAX_METADATA_BYTES);
    var initialCost = scope.storageCostSnapshot();
    long started = System.nanoTime();
    return views.openAuxiliary(requestAuthorizationDigest, scope).thenCompose(pin ->
        leaseReads.find(pin, leaseId, scope, proof)
            .thenCompose(found -> leaseReads.finish(pin, scope).thenApply(ignored ->
                found.orElseThrow(() -> new CompletionException(
                    new BackendFailure(BackendFailure.Category.UNAVAILABLE))))))
        .whenComplete((ignored, failure) -> {
          metrics.recordLeaseRenewalStage(RequestMetrics.LeaseRenewalStage.DELIVERY_VALIDATION,
              Math.max(0, System.nanoTime() - started));
          metrics.recordLeaseRenewalStorageCost(
              scope.storageCostSnapshot().minus(initialCost));
        });
  }

  static void validateKeepAliveLease(LeaseState state, LeaseRenewalVersion expectedVersion,
      long responseTtlSeconds, LeaseTimeObservation observedTime) {
    if (!expectedVersion.matches(state)
        || state.grantedTtlSeconds() != responseTtlSeconds) {
      throw new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    long remainingMillis = LeaseTimeRules.remainingTtlMillis(state, observedTime);
    if (!LeaseKeepAliveTiming.canSendPositiveResponse(responseTtlSeconds, remainingMillis)) {
      throw new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
  }

  private void completeKeepAliveDelivery(Rpc.LeaseKeepAliveResponse response,
      Supplier<CompletionStage<Void>> validateBeforeSend, BudgetManager.Account account,
      CompletableFuture<LeaseKeepAliveStream.Delivery> result, Runnable finish) {
    try {
      int responseBytes = response.getSerializedSize();
      BudgetManager.Lease output = account.tryRetain(BudgetManager.Memory.OUTPUT, responseBytes)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      var released = new AtomicBoolean();
      Runnable release = () -> {
        if (!released.compareAndSet(false, true)) return;
        output.close();
        account.close();
        finish.run();
      };
      var delivery = new LeaseKeepAliveStream.Delivery(response, release, validateBeforeSend);
      if (!result.complete(delivery)) delivery.release();
    } catch (BackendFailure | RuntimeException failure) {
      failKeepAliveProjection(failure, account, result, finish);
    } catch (Error fatal) {
      failKeepAliveProjection(fatal, account, result, finish);
      throw fatal;
    }
  }

  private void failKeepAliveProjection(Throwable failure, BudgetManager.Account account,
      CompletableFuture<LeaseKeepAliveStream.Delivery> result, Runnable finish) {
    account.close();
    finish.run();
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    result.completeExceptionally(cause);
    if (cause instanceof Error fatal) throw fatal;
  }

  private void beginGrant(Rpc.LeaseGrantRequest request, StreamObserver<Rpc.LeaseGrantResponse> observer,
      RequestScope scope, java.util.Optional<String> authToken, BudgetManager.Account account,
      AtomicBoolean terminal,
      CompletableFuture<Void> gatewayDrained) {
    InvocationId invocation = InvocationId.create();
    ByteKey operation = MutationOperationEnvelope.leaseGrant(request.toByteArray());
    ByteKey digest = MutationForwardRequest.digest(tableGroup, operation);
    try {
      requestIdentities.resolve(authToken, scope).thenCompose(requestIdentity -> {
        requestIdentity.releaseAfterPhysicalDrain(scope);
        return control.read(scope).thenCompose(snapshot -> {
          var state = snapshot.orElseThrow(() ->
              new IllegalStateException("authoritative commit control state is missing")).state();
          long remaining = Math.min(scope.remainingDeadlineNanos(), MutationForwardRequest.MAX_TIMEOUT_NANOS);
          if (state.ownerInstance().isEmpty() || remaining <= 0) {
            return CompletableFuture.failedFuture(new BackendFailure(remaining <= 0
                ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.UNAVAILABLE));
          }
          var forwarded = new MutationForwardRequest(tableGroup, invocation, digest,
              requestIdentity.authorizationDigest(), requestIdentity.authorizationReference(),
              state.ownerEpoch(), remaining, 0, operation);
          return relay.forward(forwarded, scope).thenCompose(reply -> resolveGrantReply(
              reply, request, invocation, digest, requestIdentity.authorizationDigest(), scope, account));
        });
      })
          .whenComplete((resolved, failure) -> {
            if (!scope.startIfOpen(() -> {})) {
              if (resolved != null) resolved.close();
              gatewayDrained.complete(null);
              return;
            }
            dispatchGrantResponse(request, observer, scope, account, terminal, gatewayDrained,
                resolved, failure);
          });
    } catch (RuntimeException | Error failure) {
      gatewayDrained.complete(null);
      signalError(scope, observer, terminal, failure);
      if (failure instanceof Error fatal) throw fatal;
    }
  }

  private void beginRevoke(Rpc.LeaseRevokeRequest request,
      StreamObserver<Rpc.LeaseRevokeResponse> observer, RequestScope scope,
      java.util.Optional<String> authToken, BudgetManager.Account account, AtomicBoolean terminal,
      CompletableFuture<Void> gatewayDrained) {
    InvocationId invocation = InvocationId.create();
    ByteKey operation = MutationOperationEnvelope.leaseRevoke(request.toByteArray());
    ByteKey digest = MutationForwardRequest.digest(tableGroup, operation);
    try {
      requestIdentities.resolve(authToken, scope).thenCompose(requestIdentity -> {
        requestIdentity.releaseAfterPhysicalDrain(scope);
        return control.read(scope).thenCompose(snapshot -> {
          var state = snapshot.orElseThrow(() ->
              new IllegalStateException("authoritative commit control state is missing")).state();
          long remaining = Math.min(scope.remainingDeadlineNanos(), MutationForwardRequest.MAX_TIMEOUT_NANOS);
          if (state.ownerInstance().isEmpty() || remaining <= 0) {
            return CompletableFuture.failedFuture(new BackendFailure(remaining <= 0
                ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.UNAVAILABLE));
          }
          var forwarded = new MutationForwardRequest(tableGroup, invocation, digest,
              requestIdentity.authorizationDigest(), requestIdentity.authorizationReference(),
              state.ownerEpoch(), remaining, 0, operation);
          return relay.forward(forwarded, scope).thenCompose(reply -> resolveRevokeReply(
              reply, invocation, digest, requestIdentity.authorizationDigest(), scope, account));
        });
      })
          .whenComplete((resolved, failure) -> {
            if (!scope.startIfOpen(() -> {})) {
              if (resolved != null) resolved.close();
              gatewayDrained.complete(null);
              return;
            }
            dispatchRevokeResponse(observer, scope, account, terminal, gatewayDrained,
                resolved, failure);
          });
    } catch (RuntimeException | Error failure) {
      gatewayDrained.complete(null);
      signalError(scope, observer, terminal, failure);
      if (failure instanceof Error fatal) throw fatal;
    }
  }

  private CompletableFuture<InvocationResultResolver.ResolvedResult> resolveRevokeReply(
      MutationRelay.Result reply, InvocationId invocation, ByteKey digest,
      ByteKey requestAuthorizationDigest, RequestScope scope, BudgetManager.Account account) {
    if (reply instanceof MutationRelay.KnownResult known) {
      if (!java.util.Arrays.equals(known.responseBytes(), invocation.bytes())) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return resolveOriginal(invocation, digest, requestAuthorizationDigest, scope, account);
    }
    if (reply instanceof MutationRelay.LeaseRejection rejection) {
      return CompletableFuture.failedFuture(new LeaseFailure(rejection.reason()));
    }
    if (reply instanceof MutationRelay.KnownRejection rejection) {
      return CompletableFuture.failedFuture(new BackendFailure(rejection.category()));
    }
    if (reply instanceof MutationRelay.SemanticRejection) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
    }
    MutationRelay.ResultUnknown unknown = reply instanceof MutationRelay.ResultUnknown value
        ? value : new MutationRelay.ResultUnknown(BackendFailure.Category.UNKNOWN);
    return resolveOriginal(invocation, digest, requestAuthorizationDigest, scope, account)
        .exceptionallyCompose(failure ->
        CompletableFuture.failedFuture(new BackendFailure(unknown.category(),
            CompletionFailures.unwrapCompletion(failure))));
  }

  private void dispatchRevokeResponse(StreamObserver<Rpc.LeaseRevokeResponse> observer,
      RequestScope scope, BudgetManager.Account account, AtomicBoolean terminal,
      CompletableFuture<Void> gatewayDrained,
      InvocationResultResolver.ResolvedResult resolved, Throwable failure) {
    try {
      worker.execute(() -> {
        try {
          if (failure != null) {
            signalError(scope, observer, terminal, CompletionFailures.unwrapCompletion(failure));
            return;
          }
          if (resolved == null || scope.isClosed() || scope.isDeadlineExceeded()) {
            if (resolved != null) resolved.close();
            return;
          }
          var buffer = resolved.payload().buffer();
          var bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          Rpc.LeaseRevokeResponse response = Rpc.LeaseRevokeResponse.parseFrom(bytes);
          send(response, response.getSerializedSize(), scope, account, observer, terminal);
        } catch (InvalidProtocolBufferException | RuntimeException | Error projectionFailure) {
          signalError(scope, observer, terminal, projectionFailure);
          if (projectionFailure instanceof Error fatal) throw fatal;
        } finally {
          if (resolved != null) resolved.close();
          gatewayDrained.complete(null);
        }
      });
    } catch (RuntimeException rejected) {
      if (resolved != null) resolved.close();
      gatewayDrained.complete(null);
      signalError(scope, observer, terminal,
          new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT, rejected));
    }
  }

  private CompletableFuture<InvocationResultResolver.ResolvedResult> resolveGrantReply(
      MutationRelay.Result reply, Rpc.LeaseGrantRequest request, InvocationId invocation,
      ByteKey digest, ByteKey requestAuthorizationDigest, RequestScope scope,
      BudgetManager.Account account) {
    if (reply instanceof MutationRelay.KnownResult known) {
      if (!java.util.Arrays.equals(known.responseBytes(), invocation.bytes())) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return resolveOriginal(invocation, digest, requestAuthorizationDigest, scope, account);
    }
    if (reply instanceof MutationRelay.LeaseRejection rejection) {
      return CompletableFuture.failedFuture(new LeaseFailure(rejection.reason()));
    }
    if (reply instanceof MutationRelay.KnownRejection rejection) {
      return CompletableFuture.failedFuture(new BackendFailure(rejection.category()));
    }
    if (reply instanceof MutationRelay.SemanticRejection) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
    }
    MutationRelay.ResultUnknown unknown = reply instanceof MutationRelay.ResultUnknown value
        ? value : new MutationRelay.ResultUnknown(BackendFailure.Category.UNKNOWN);
    return resolveOriginal(invocation, digest, requestAuthorizationDigest, scope, account)
        .exceptionallyCompose(failure -> {
      Throwable cause = CompletionFailures.unwrapCompletion(failure);
      return CompletableFuture.failedFuture(new BackendFailure(unknown.category(), cause));
    });
  }

  private CompletableFuture<InvocationResultResolver.ResolvedResult> resolveOriginal(
      InvocationId invocation, ByteKey digest, ByteKey requestAuthorizationDigest,
      RequestScope scope, BudgetManager.Account account) {
    return results.resolve(invocation, digest, requestAuthorizationDigest, scope, account);
  }

  private void dispatchGrantResponse(Rpc.LeaseGrantRequest request,
      StreamObserver<Rpc.LeaseGrantResponse> observer, RequestScope scope, BudgetManager.Account account,
      AtomicBoolean terminal, CompletableFuture<Void> gatewayDrained,
      InvocationResultResolver.ResolvedResult resolved, Throwable failure) {
    try {
      worker.execute(() -> {
        try {
          if (failure != null) {
            signalError(scope, observer, terminal, CompletionFailures.unwrapCompletion(failure));
            return;
          }
          if (resolved == null || scope.isClosed() || scope.isDeadlineExceeded()) {
            if (resolved != null) resolved.close();
            return;
          }
          var buffer = resolved.payload().buffer();
          var bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          var result = LeaseGrantResultMessages.decode(ByteKey.copyOf(bytes));
          var response = LeaseGrantResponseMapper.toWire(request, result, resolved, clusterId, memberId);
          send(response, response.getSerializedSize(), scope, account, observer, terminal);
        } catch (RuntimeException | Error projectionFailure) {
          signalError(scope, observer, terminal, projectionFailure);
          if (projectionFailure instanceof Error fatal) throw fatal;
        } finally {
          if (resolved != null) resolved.close();
          gatewayDrained.complete(null);
        }
      });
    } catch (RuntimeException rejected) {
      if (resolved != null) resolved.close();
      gatewayDrained.complete(null);
      signalError(scope, observer, terminal,
          new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT, rejected));
    }
  }

  private void timeToLive(Rpc.LeaseTimeToLiveRequest request,
      StreamObserver<Rpc.LeaseTimeToLiveResponse> observer) {
    Objects.requireNonNull(request, "request");
    BudgetManager.Reservation readReservation = request.getKeys() ? bindingReadReservation : reservation;
    startRead(observer, readReservation, (requestIdentity, scope, account) ->
            readTimeToLive(request, requestIdentity.readAuthorizationDigest(), scope, account),
        Rpc.LeaseTimeToLiveResponse::getSerializedSize);
  }

  private void leases(Rpc.LeaseLeasesRequest request,
      StreamObserver<Rpc.LeaseLeasesResponse> observer) {
    Objects.requireNonNull(request, "request");
    startRead(observer, reservation, (requestIdentity, scope, account) ->
        readLeases(requestIdentity.readAuthorizationDigest(), scope, account),
        Rpc.LeaseLeasesResponse::getSerializedSize);
  }

  private <T> void startRead(StreamObserver<T> observer, BudgetManager.Reservation memory,
      AuthenticatedRead<T> operation,
      java.util.function.ToIntFunction<T> responseSize) {
    Objects.requireNonNull(observer, "observer");
    RequestScope scope = RequestScopeInterceptor.currentScope();
    var authToken = RequestScopeInterceptor.currentAuthToken();
    var terminal = new AtomicBoolean();
    var drained = new CompletableFuture<Void>();
    if (!registerReadProducer(scope, drained)) return;
    try {
      BudgetManager.Account account = budgets.admit(tableGroup, scope, memory);
      scheduleRead(scope, authToken, account, operation, responseSize, observer, terminal, drained);
    } catch (BackendFailure | RuntimeException failure) {
      settleReadFailure(scope, observer, terminal, drained, failure);
    } catch (Error fatal) {
      settleReadFailure(scope, observer, terminal, drained, fatal);
      throw fatal;
    }
  }

  private static boolean registerReadProducer(RequestScope scope, CompletableFuture<Void> drained) {
    return scope.startIfOpen(() -> scope.trackPhysicalProducer(drained));
  }

  private <T> void scheduleRead(RequestScope scope, java.util.Optional<String> authToken,
      BudgetManager.Account account, AuthenticatedRead<T> operation,
      java.util.function.ToIntFunction<T> responseSize, StreamObserver<T> observer,
      AtomicBoolean terminal, CompletableFuture<Void> drained) {
    worker.execute(() -> runReadOperation(scope, authToken, account, operation,
        responseSize, observer, terminal, drained));
  }

  private <T> void runReadOperation(RequestScope scope, java.util.Optional<String> authToken,
      BudgetManager.Account account, AuthenticatedRead<T> operation,
      java.util.function.ToIntFunction<T> responseSize, StreamObserver<T> observer,
      AtomicBoolean terminal, CompletableFuture<Void> drained) {
    CompletableFuture<AuthorizedRead<T>> operationResult;
    try {
      operationResult = requestIdentities.resolve(authToken, scope).thenCompose(requestIdentity -> {
        requestIdentity.releaseAfterPhysicalDrain(scope);
        return Objects.requireNonNull(operation.apply(requestIdentity, scope, account),
            "Lease read future").thenApply(response -> new AuthorizedRead<>(response, requestIdentity));
      });
    } catch (RuntimeException | Error failure) {
      settleReadFailure(scope, observer, terminal, drained, failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }
    operationResult.whenComplete((response, failure) -> dispatchReadCompletion(
        response, failure, responseSize, scope, account, observer, terminal, drained));
  }

  private <T> void dispatchReadCompletion(AuthorizedRead<T> result, Throwable failure,
      java.util.function.ToIntFunction<T> responseSize, RequestScope scope,
      BudgetManager.Account account, StreamObserver<T> observer, AtomicBoolean terminal,
      CompletableFuture<Void> drained) {
    var dispatchState = new AtomicInteger();
    try {
      worker.execute(() -> {
        if (dispatchState.compareAndSet(0, 1)) {
          validateReadCompletion(result, failure, responseSize, scope, account,
              observer, terminal, drained);
        }
      });
    } catch (RuntimeException rejected) {
      if (dispatchState.compareAndSet(0, 2)) {
        settleReadFailure(scope, observer, terminal, drained,
            new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT, rejected));
      }
    } catch (Error fatal) {
      if (dispatchState.compareAndSet(0, 2)) {
        settleReadFailure(scope, observer, terminal, drained, fatal);
      }
      throw fatal;
    }
  }

  private <T> void validateReadCompletion(AuthorizedRead<T> result, Throwable failure,
      java.util.function.ToIntFunction<T> responseSize, RequestScope scope,
      BudgetManager.Account account, StreamObserver<T> observer, AtomicBoolean terminal,
      CompletableFuture<Void> drained) {
    if (failure != null || result == null || result.response() == null
        || scope.isClosed() || scope.isDeadlineExceeded()) {
      completeRead(result == null ? null : result.response(), failure, responseSize, scope,
          account, observer, terminal, drained);
      return;
    }
    CompletionStage<Void> validation;
    try {
      validation = Objects.requireNonNull(requestIdentities.revalidate(result.identity(), scope),
          "Lease response validation");
    } catch (RuntimeException | Error problem) {
      completeRead(result.response(), problem, responseSize, scope, account, observer, terminal, drained);
      if (problem instanceof Error fatal) throw fatal;
      return;
    }
    validation.whenComplete((ignored, problem) -> completeRead(result.response(), problem,
        responseSize, scope, account, observer, terminal, drained));
  }

  private <T> void completeRead(T response, Throwable failure,
      java.util.function.ToIntFunction<T> responseSize, RequestScope scope,
      BudgetManager.Account account, StreamObserver<T> observer, AtomicBoolean terminal,
      CompletableFuture<Void> drained) {
    try {
      if (failure != null) {
        signalError(scope, observer, terminal, CompletionFailures.unwrapCompletion(failure));
      } else if (response != null) {
        send(response, responseSize.applyAsInt(response), scope, account, observer, terminal);
      }
    } finally {
      drained.complete(null);
    }
  }

  private static void settleReadFailure(RequestScope scope, StreamObserver<?> observer,
      AtomicBoolean terminal, CompletableFuture<Void> drained, Throwable failure) {
    drained.complete(null);
    signalError(scope, observer, terminal, failure);
  }

  private CompletableFuture<Rpc.LeaseTimeToLiveResponse> readTimeToLive(
      Rpc.LeaseTimeToLiveRequest request, ByteKey requestAuthorizationDigest,
      RequestScope scope, BudgetManager.Account account) {
    if (request.getID() == 0) {
      return views.openAuxiliary(requestAuthorizationDigest, scope).thenCompose(pin ->
          leaseReads.finish(pin, scope).thenApply(ignored ->
              projectTimeToLive(request, Optional.empty(), pin, time.observe(), List.of())));
    }
    var proof = new ManifestCatalog.ProofReadBudget(scope, account, 64, MAX_METADATA_BYTES);
    return views.openAuxiliary(requestAuthorizationDigest, scope).thenCompose(pin ->
        leaseReads.find(pin, request.getID(), scope, proof)
            .thenCompose(found -> {
              var clock = time.observe();
              if (found.isPresent() && found.orElseThrow().status() == LeaseState.Status.ACTIVE
                  && LeaseTimeRules.isExpired(found.orElseThrow(), clock)) {
                LeaseState expired = found.orElseThrow();
                if (expired.boundKeyCount() != 0 || expired.boundKeyBytes() != 0
                    || expired.revokeCostBytes() != 0) {
                  return CompletableFuture.failedFuture(
                      new BackendFailure(BackendFailure.Category.UNAVAILABLE));
                }
              }
              if (!request.getKeys() || found.isEmpty()
                  || found.orElseThrow().status() != LeaseState.Status.ACTIVE) {
                return CompletableFuture.completedFuture(
                    projectTimeToLive(request, found, pin, clock, List.of()));
              }
              return leaseBindings.read(pin, found.orElseThrow(), scope, account, proof)
                  .thenApply(keys -> projectTimeToLive(request, found, pin, clock, keys));
            })
            .thenCompose(response -> leaseReads.finish(pin, scope).thenApply(ignored -> response)));
  }

  private static CompletableFuture<Void> closeResolved(
      CompletableFuture<InvocationResultResolver.ResolvedResult> future) {
    return future.thenAccept(InvocationResultResolver.ResolvedResult::close);
  }

  private Rpc.LeaseTimeToLiveResponse projectTimeToLive(Rpc.LeaseTimeToLiveRequest request,
      Optional<LeaseState> found, PinnedReadView pin,
      io.etcdhbase.core.lease.LeaseTimeObservation clock, List<ByteKey> keys) {
    return LeaseTimeToLiveResponseMapper.toWire(request, found, pin.view().targetRevision(),
        clock, keys, clusterId, memberId);
  }

  private CompletableFuture<Rpc.LeaseLeasesResponse> readLeases(
      ByteKey requestAuthorizationDigest, RequestScope scope, BudgetManager.Account account) {
    var proof = new ManifestCatalog.ProofReadBudget(scope, account, MAX_EXAMINED_LEASES, MAX_METADATA_BYTES);
    return views.openAuxiliary(requestAuthorizationDigest, scope).thenCompose(pin ->
        leaseEnumeration.read(pin, scope, account, proof).thenCompose(states -> {
          List<Rpc.LeaseStatus> leases = states.stream().filter(state -> state.status() == LeaseState.Status.ACTIVE)
              .map(state -> Rpc.LeaseStatus.newBuilder()
                  .setID(state.identity().leaseId())
                  .build())
              .sorted((left, right) -> Long.compareUnsigned(left.getID(), right.getID())).toList();
          var response = Rpc.LeaseLeasesResponse.newBuilder()
              .setHeader(ResponseHeaderFactory.create(clusterId, memberId, pin.view().targetRevision(), 0))
              .addAllLeases(leases).build();
          return leaseReads.finish(pin, scope).thenApply(ignored -> response);
        }));
  }

  private <T> void send(T response, int serializedBytes, RequestScope scope,
      BudgetManager.Account account, StreamObserver<T> observer, AtomicBoolean terminal) {
    if (scope.isClosed() || scope.isDeadlineExceeded()) return;
    BudgetManager.Lease output = null;
    try {
      long estimated = Math.addExact(Math.multiplyExact(2L, serializedBytes), OUTPUT_OBJECT_ALLOWANCE);
      output = account.tryRetain(BudgetManager.Memory.OUTPUT, estimated)
          .orElseThrow(LeaseGateway::resourceLimit);
      if (serializedBytes > maximumEtcdRequestBytes) {
        throw resourceLimit();
      }
      if (scope.isClosed() || scope.isDeadlineExceeded()) return;
      observer.onNext(response);
      if (!scope.isClosed() && !scope.isDeadlineExceeded() && terminal.compareAndSet(false, true)) {
        observer.onCompleted();
      }
    } catch (BackendFailure | RuntimeException failure) {
      signalError(scope, observer, terminal, failure);
    } catch (Error fatal) {
      signalError(scope, observer, terminal, fatal);
      throw fatal;
    } finally {
      if (output != null) output.close();
    }
  }

  private static void retainInput(RequestScope scope, BudgetManager.Account account, int serializedBytes)
      throws BackendFailure {
    long bytes = Math.addExact(1024L, Math.multiplyExact(4L, serializedBytes));
    PutCommitPreparation.retainUntilDrain(scope, account.tryRetain(BudgetManager.Memory.CANDIDATE, bytes)
        .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT)));
  }

  private static CompletionException resourceLimit() {
    return new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
  }

  private static void signalError(RequestScope scope, StreamObserver<?> observer,
      AtomicBoolean terminal, Throwable failure) {
    if (!terminal.compareAndSet(false, true) || scope.isClosed()) return;
    try {
      observer.onError(ErrorProjection.project(failure));
    } catch (RuntimeException | Error terminationFailure) {
      if (failure != terminationFailure) failure.addSuppressed(terminationFailure);
      if (terminationFailure instanceof Error fatal) throw fatal;
    }
  }
}
