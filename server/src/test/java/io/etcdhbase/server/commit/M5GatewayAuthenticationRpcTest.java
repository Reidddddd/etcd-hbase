package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import etcdserverpb.AuthGrpc;
import etcdserverpb.KVGrpc;
import etcdserverpb.LeaseGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.core.auth.AuthToken;
import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.AuthSessionStore;
import io.etcdhbase.core.auth.AuthSlot;
import io.etcdhbase.core.auth.CredentialEnvelopeProtector;
import io.etcdhbase.core.auth.SubjectAuthority;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.LeaseBindingReader;
import io.etcdhbase.core.commit.LeaseEnumerationReader;
import io.etcdhbase.core.commit.LeaseReadView;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.ResultCatalog;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.PutResult;
import io.etcdhbase.core.kv.ResultNode;
import io.etcdhbase.core.kv.ResultTree;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.kv.TxnEvaluator;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.grpc.EtcdAuthTokenMetadata;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.grpc.service.AuthService;
import io.etcdhbase.grpc.service.KvService;
import io.etcdhbase.grpc.service.LeaseService;
import io.etcdhbase.hbase.codec.ResultMessages;
import io.etcdhbase.server.auth.AesGcmCredentialEnvelopeProtector;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.server.auth.AuthDeploymentFixture;
import io.etcdhbase.server.auth.AuthenticateRpcHandler;
import io.etcdhbase.server.auth.CredentialKeyProvider;
import io.etcdhbase.server.auth.SessionIdentityLease;
import io.etcdhbase.server.auth.SessionIdentityManager;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.MetadataUtils;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Exercises the public RPC, non-owner gateway, and owner session revalidation as one path.
 * The plaintext A/B loopback gRPC listeners and gateways are real; the auth-control store,
 * backend identity provider, and owner transport are deterministic substitutes, so this is not
 * TLS, mTLS, or HBase proof.
 */
class M5GatewayAuthenticationRpcTest {
  private static final String TABLE_GROUP = "auth-group";
  private static final String PROVIDER = "hbase-provider";
  private static final String OWNER = "owner-a";
  private static final String NON_OWNER_PEER = "replica-b";
  private static final String PRINCIPAL = "milvus-service";
  private static final AuthenticationScope AUTH_SCOPE =
      new AuthenticationScope(TABLE_GROUP, "profile");
  private static final ByteKey DATASET = ByteKey.copyOf(fill(32, (byte) 0x31));
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(fill(32, (byte) 0x52));

  @Test
  void noTokenPutAndTxnSucceedWithServiceIdentityWhenAuthenticationIsDisabled() throws Exception {
    var serviceIdentity = identity("server-service", "service-context");
    var authorization = AuthorizationScopeDigest.fromIdentity(serviceIdentity);
    var forwarded = new AtomicReference<MutationForwardRequest>();
    var relay = mock(MutationRelay.class);
    when(relay.forward(any(MutationForwardRequest.class), any(RequestScope.class)))
        .thenAnswer(call -> {
          MutationForwardRequest request = call.getArgument(0);
          forwarded.set(request);
          return CompletableFuture.completedFuture(new MutationRelay.KnownResult(request.invocationId().bytes()));
        });

    var payload = mock(ResultCatalog.RetainedResult.class);
    when(payload.belongsTo(any(RequestScope.class))).thenReturn(true);
    when(payload.retainForConsumer(any())).thenReturn(true);
    var key = ByteKey.copyOf("put-key".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    when(payload.buffer()).thenReturn(ByteBuffer.wrap(ResultMessages.encode(
        new PutResult(key, 2, Optional.empty()))));
    var decision = mock(CommitDecision.class);
    when(decision.containsKeyChanges()).thenReturn(true);
    when(decision.revision()).thenReturn(2L);
    var result = mock(InvocationResultResolver.ResolvedResult.class);
    when(result.payload()).thenReturn(payload);
    when(result.decision()).thenReturn(decision);
    var results = mock(InvocationResultResolver.class);
    when(results.resolve(any(), any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(result));

    var observedIdentity = new AtomicReference<AuthorizedBackendIdentity>();
    var reads = mock(TxnReadExecutor.class);
    when(reads.execute(any(), any(), any(), any(), any(), anyBoolean(), any())).thenAnswer(call -> {
      observedIdentity.set(call.getArgument(2));
      var header = new ResultNode.Header(true, 2);
      var tree = new ResultTree(new ResultNode.Txn(0, header, true,
          List.of(new ResultNode.Range(0, header, List.of(), 0, false))));
      var validated = mock(TxnEvaluator.ValidatedRead.class);
      when(validated.belongsTo(any(RequestScope.class))).thenReturn(true);
      when(validated.result()).thenReturn(tree);
      when(validated.revision()).thenReturn(2L);
      return CompletableFuture.completedFuture(validated);
    });

    var control = controlStore();
    var budgets = budgets();
    var responseReservation = new BudgetManager.Reservation(1L << 16, 1L << 16,
        1L << 16, 1L << 18, 1L << 16);
    var txn = new TxnGateway(TABLE_GROUP, serviceIdentity, authorization, 1, 1, 1 << 20,
        control, relay, results, reads, budgets, responseReservation, Runnable::run, new RequestMetrics());
    var put = new PutGateway(TABLE_GROUP, serviceIdentity, authorization, 1, 1, 1 << 20,
        control, relay, results, budgets, responseReservation, Runnable::run, new RequestMetrics());
    try (var server = RpcNode.start(new KvService(put, txn, txn::deleteRange, txn::range),
        new AuthService())) {
      var client = KVGrpc.newBlockingStub(server.channel()).withDeadlineAfter(3, TimeUnit.SECONDS);
      var putResponse = client.put(Rpc.PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("put-key"))
          .setValue(ByteString.copyFromUtf8("value")).build());
      assertEquals(2, putResponse.getHeader().getRevision());
      assertNotNull(forwarded.get());
      assertEquals(authorization, forwarded.get().authorizationScopeDigest());
      assertEquals(ServiceAuthorizationContextReference.create(serviceIdentity, authorization),
          forwarded.get().authorizationContextReference());

      var txnResponse = client.txn(readTransaction());
      assertTrue(txnResponse.getSucceeded());
      assertEquals(1, txnResponse.getResponsesCount());
      assertEquals(serviceIdentity, observedIdentity.get());
    }
  }

  @Test
  void authenticateOnAThenBRangeAndOwnerForwardingUseRevalidatedIdentityUntilPhysicalDrain()
      throws Exception {
    var store = new InMemoryAuthSessionStore();
    var keyProvider = new TestCredentialKeyProvider();
    var protector = new AesGcmCredentialEnvelopeProtector(keyProvider, new SecureRandom());
    var authenticateIdentities = new TestSessionIdentityManager("replica-a", 0);
    var nonOwnerIdentities = new TestSessionIdentityManager("replica-b", 3);
    var ownerIdentities = new TestSessionIdentityManager("owner-a", 2);
    var sessionsA = sessions(store, protector, authenticateIdentities);
    var sessionsB = sessions(store, protector, nonOwnerIdentities);
    var ownerSessions = sessions(store, protector, ownerIdentities);

    var control = controlStore();
    var invocations = mock(InvocationService.class);
    when(invocations.bindOrJoin(any(InvocationBinding.class), any(RequestScope.class)))
        .thenAnswer(call -> CompletableFuture.completedFuture(call.getArgument(0)));
    var results = mock(InvocationResultResolver.class);
    when(results.resolveTransaction(any(InvocationId.class), any(ByteKey.class),
        any(ByteKey.class), any(RequestScope.class), any(BudgetManager.Account.class)))
        .thenReturn(CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE)));

    var ownerPhysicalWorks = new java.util.concurrent.CopyOnWriteArrayList<CompletableFuture<Void>>();
    var ownerResolvedIdentity = new AtomicReference<AuthorizedBackendIdentity>();
    var ownerResolvedIdentities = new java.util.concurrent.CopyOnWriteArrayList<AuthorizedBackendIdentity>();
    var forwardedRequest = new AtomicReference<MutationForwardRequest>();
    var forwardedRequests = new java.util.concurrent.CopyOnWriteArrayList<MutationForwardRequest>();
    var route = new AtomicReference<OwnerForwarding.Route>();
    var ownerReceiver = new MutationForwardReceiver(control, invocations,
        new SessionAuthorizationResolver(TABLE_GROUP, ownerSessions,
            peerIdentity -> NON_OWNER_PEER.equals(peerIdentity)),
        (request, binding, authorization, scope) -> {
          ownerResolvedIdentity.set(authorization.identity());
          ownerResolvedIdentities.add(authorization.identity());
          var physical = new CompletableFuture<Void>();
          ownerPhysicalWorks.add(physical);
          scope.trackPhysicalProducer(physical);
          return CompletableFuture.completedFuture(
              new OwnerForwarding.Unknown(BackendFailure.Category.UNAVAILABLE));
        });
    var relay = new MutationRelay(control,
        (tableGroup, ownerInstance, ownerEpoch) -> {
          var resolvedRoute = new OwnerForwarding.Route(
              ownerInstance, ownerEpoch, "owner-endpoint", "owner-peer");
          route.set(resolvedRoute);
          return CompletableFuture.completedFuture(resolvedRoute);
        },
        (resolvedRoute, request, scope) -> {
          forwardedRequest.set(request);
          forwardedRequests.add(request);
          return ownerReceiver.receive(NON_OWNER_PEER, request, scope);
        });

    var readExecutor = mock(TxnReadExecutor.class);
    var readIdentity = new AtomicReference<AuthorizedBackendIdentity>();
    var readAuthorization = new AtomicReference<ByteKey>();
    when(readExecutor.execute(any(TxnCommand.class), any(ByteKey.class),
        any(AuthorizedBackendIdentity.class), any(RequestScope.class),
        any(BudgetManager.Account.class), anyBoolean(), any()))
        .thenAnswer(call -> {
          readIdentity.set(call.getArgument(2));
          readAuthorization.set(call.getArgument(1));
          return CompletableFuture.failedFuture(
              new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
        });

    var serviceIdentity = identity("server-service", "b-service-context");
    var serviceAuthorization = AuthorizationScopeDigest.fromIdentity(serviceIdentity);
    var budgets = budgets();
    var reservation = reservation();
    var metrics = new RequestMetrics();
    var txnGateway = new TxnGateway(TABLE_GROUP, serviceIdentity, serviceAuthorization,
        1, 1, 1 << 20, control, relay, results, readExecutor, budgets, reservation,
        Runnable::run, metrics, sessionsB);
    var putGateway = new PutGateway(TABLE_GROUP, serviceIdentity, serviceAuthorization,
        1, 1, 1 << 20, control, relay, results, budgets, reservation,
        Runnable::run, metrics, sessionsB);
    when(results.resolve(any(InvocationId.class), any(ByteKey.class), any(ByteKey.class),
        any(RequestScope.class), any(BudgetManager.Account.class)))
        .thenReturn(CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE)));
    var leaseTimers = new ScheduledThreadPoolExecutor(1,
        Thread.ofPlatform().daemon().name("m5-lease-auth-test-", 0).factory());
    var leaseGateway = new LeaseGateway(TABLE_GROUP, serviceIdentity, serviceAuthorization,
        1, 1, 1 << 20, control, relay, results, mock(ReadViewService.class),
        mock(LeaseReadView.class), mock(LeaseEnumerationReader.class), mock(LeaseBindingReader.class),
        mock(LeaseTimeSource.class), budgets, reservation, Runnable::run, leaseTimers, metrics, sessionsB);
    var leaseService = new LeaseService(leaseGateway.grantHandler(),
        leaseGateway.timeToLiveHandler(), leaseGateway.leasesHandler(),
        leaseGateway.keepAliveHandler(), leaseGateway.revokeHandler());
    var kv = new KvService(putGateway, txnGateway, txnGateway::deleteRange, txnGateway::range);

    try (var keys = keyProvider;
         var authenticateReplica = RpcNode.start(new KvService(),
             new AuthService(new AuthenticateRpcHandler(sessionsA)));
         var nonOwnerReplica = RpcNode.start(kv, leaseService,
             new AuthService(new AuthenticateRpcHandler(sessionsB)))) {
      try {
        String token = AuthGrpc.newBlockingStub(authenticateReplica.channel())
            .withDeadlineAfter(3, TimeUnit.SECONDS)
            .authenticate(Rpc.AuthenticateRequest.newBuilder()
                .setName(PRINCIPAL).setPassword("request-secret").build())
            .getToken();
        assertTrue(token.length() >= 32, "Authenticate must return an opaque token");

        var metadata = new Metadata();
        metadata.put(EtcdAuthTokenMetadata.key(), token);
        var authenticatedChannel = ClientInterceptors.intercept(nonOwnerReplica.channel(),
            MetadataUtils.newAttachHeadersInterceptor(metadata));
        var kvClient = KVGrpc.newBlockingStub(authenticatedChannel)
            .withDeadlineAfter(3, TimeUnit.SECONDS);

        var missingTokenClient = KVGrpc.newBlockingStub(nonOwnerReplica.channel())
            .withDeadlineAfter(3, TimeUnit.SECONDS);
        assertInvalidAuthToken(() -> missingTokenClient.txn(readTransaction()));
        assertInvalidAuthToken(() -> missingTokenClient.put(Rpc.PutRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("missing-token-put")).build()));

        var invalidMetadata = new Metadata();
        invalidMetadata.put(EtcdAuthTokenMetadata.key(), "not-a-session-token");
        var invalidTokenChannel = ClientInterceptors.intercept(nonOwnerReplica.channel(),
            MetadataUtils.newAttachHeadersInterceptor(invalidMetadata));
        var invalidTokenClient = KVGrpc.newBlockingStub(invalidTokenChannel)
            .withDeadlineAfter(3, TimeUnit.SECONDS);
        assertInvalidAuthToken(() -> invalidTokenClient.txn(readTransaction()));
        assertInvalidAuthToken(() -> invalidTokenClient.put(Rpc.PutRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("invalid-token-put")).build()));

        StatusRuntimeException rangeFailure = assertThrows(StatusRuntimeException.class,
            () -> kvClient.txn(readTransaction()));
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, rangeFailure.getStatus().getCode());
        AuthorizedBackendIdentity readResolvedIdentity = readIdentity.get();
        assertNotNull(readResolvedIdentity, "the B transaction gateway must resolve the token");
        assertEquals(PRINCIPAL, readResolvedIdentity.subject());
        assertEquals(AuthorizationScopeDigest.fromSessionRead(readResolvedIdentity,
                store.sessions.values().iterator().next()),
            readAuthorization.get());

        StatusRuntimeException mutationFailure = assertThrows(StatusRuntimeException.class,
            () -> kvClient.txn(writeTransaction()));
        assertEquals(Status.Code.UNAVAILABLE, mutationFailure.getStatus().getCode());

        AuthorizedBackendIdentity ownerIdentity = ownerResolvedIdentity.get();
        assertNotNull(ownerIdentity, "the non-owner mutation must reach the owner receiver");
        MutationForwardRequest sentRequest = forwardedRequest.get();
        assertNotNull(sentRequest, "the non-owner gateway must send one owner request");
        OwnerForwarding.Route resolvedRoute = route.get();
        assertNotNull(resolvedRoute, "the relay must resolve the owner from control state");
        assertEquals(PRINCIPAL, ownerIdentity.subject());
        assertEquals("owner-a-context-1", ownerIdentity.contextRef());
        assertEquals(2, nonOwnerIdentities.activatedIdentities().size());
        AuthorizedBackendIdentity nonOwnerIdentity = nonOwnerIdentities.activatedIdentities().get(1);
        assertEquals(PRINCIPAL, nonOwnerIdentity.subject());
        assertNotSame(nonOwnerIdentity, ownerIdentity,
            "the owner must activate a local identity from the durable session reference");
        assertEquals(AuthorizationScopeDigest.fromIdentity(nonOwnerIdentity),
            sentRequest.authorizationScopeDigest());
        assertEquals(AuthorizationScopeDigest.fromIdentity(ownerIdentity),
            sentRequest.authorizationScopeDigest());
        assertEquals(OWNER, resolvedRoute.ownerInstance());
        assertEquals("owner-peer", resolvedRoute.peerIdentity());
        assertEquals(0, ownerIdentities.releaseCount(),
            "owner identity must remain leased while the physical producer is active");

        StatusRuntimeException leaseFailure = assertThrows(StatusRuntimeException.class,
            () -> LeaseGrpc.newBlockingStub(authenticatedChannel)
                .withDeadlineAfter(3, TimeUnit.SECONDS)
                .leaseGrant(Rpc.LeaseGrantRequest.newBuilder().setTTL(30).build()));
        assertEquals(Status.Code.UNAVAILABLE, leaseFailure.getStatus().getCode());
        assertEquals(2, ownerResolvedIdentities.size(),
            "LeaseGrant must use the real owner authorization resolver");
        AuthorizedBackendIdentity ownerLeaseIdentity = ownerResolvedIdentities.get(1);
        assertEquals(PRINCIPAL, ownerLeaseIdentity.subject());
        assertEquals("owner-a-context-2", ownerLeaseIdentity.contextRef());
        MutationForwardRequest leaseForward = forwardedRequests.get(1);
        AuthorizedBackendIdentity nonOwnerLeaseIdentity = nonOwnerIdentities.activatedIdentities().get(2);
        assertEquals(AuthorizationScopeDigest.fromIdentity(nonOwnerLeaseIdentity),
            leaseForward.authorizationScopeDigest());
        assertEquals(AuthorizationScopeDigest.fromIdentity(ownerLeaseIdentity),
            leaseForward.authorizationScopeDigest());
        try (AuthToken parsed = AuthToken.parse(token)) {
          assertEquals(parsed.digest(), SessionAuthorizationContextReference.parse(
              leaseForward.authorizationContextReference()).tokenDigest());
        }
        assertEquals(0, ownerIdentities.releaseCount(),
            "Lease owner identity must remain leased while its producer is active");

        ownerPhysicalWorks.forEach(work -> work.complete(null));
        assertTrue(ownerIdentities.awaitReleases(), "owner identity release must follow physical drain");
        assertTrue(nonOwnerIdentities.awaitReleases(), "non-owner request identities must be released");
        assertEquals(2, ownerIdentities.releaseCount());
        assertEquals(3, nonOwnerIdentities.releaseCount());
      } finally {
        ownerPhysicalWorks.forEach(work -> work.complete(null));
      }
    } finally {
      leaseTimers.shutdownNow();
      authenticateIdentities.close();
      nonOwnerIdentities.close();
      ownerIdentities.close();
    }
  }

  private static AuthSessionService sessions(InMemoryAuthSessionStore store,
      CredentialEnvelopeProtector protector, TestSessionIdentityManager identities) {
    return new AuthSessionService(store, protector, identities, "milvus", PROVIDER, AUTH_SCOPE,
        DATASET, Duration.ofMinutes(5), 8, Clock.systemUTC(), new SecureRandom(),
        store.deployment, store.deployment.barriers(),
        AuthDeploymentFixture.IDENTITY_DOMAIN, store.deployment.configurationDigest());
  }

  private static AuthorizedBackendIdentity identity(String subject, String context) {
    return new AuthorizedBackendIdentity(PROVIDER, subject, context, AUTH_SCOPE,
        Set.of(TABLE_GROUP), 7);
  }

  private static CommitControlStore controlStore() {
    long now = System.currentTimeMillis();
    var state = CommitState.initial(FINGERPRINT, 1)
        .claimOwner(OWNER, now - 1, now + TimeUnit.MINUTES.toMillis(1));
    var snapshot = new ControlSnapshot(state, new byte[] {1});
    var control = mock(CommitControlStore.class);
    when(control.read(any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(snapshot)));
    return control;
  }

  private static BudgetManager budgets() {
    var limits = new BudgetManager.Limits(2, 0, 2, 2, 2, 0, 4L << 20);
    return new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits);
  }

  private static BudgetManager.Reservation reservation() {
    return new BudgetManager.Reservation(1L << 16, 1L << 16, 1L << 16,
        1L << 16, 1L << 16);
  }

  private static Rpc.TxnRequest readTransaction() {
    var range = Rpc.RangeRequest.newBuilder()
        .setKey(ByteString.copyFromUtf8("read-key")).build();
    return Rpc.TxnRequest.newBuilder()
        .addSuccess(Rpc.RequestOp.newBuilder().setRequestRange(range))
        .build();
  }

  private static Rpc.TxnRequest writeTransaction() {
    var put = Rpc.PutRequest.newBuilder()
        .setKey(ByteString.copyFromUtf8("write-key"))
        .setValue(ByteString.copyFromUtf8("write-value"))
        .build();
    return Rpc.TxnRequest.newBuilder()
        .addSuccess(Rpc.RequestOp.newBuilder().setRequestPut(put))
        .build();
  }

  private static byte[] fill(int length, byte value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, value);
    return bytes;
  }

  private static void assertInvalidAuthToken(Runnable rpc) {
    StatusRuntimeException failure = assertThrows(StatusRuntimeException.class, rpc::run);
    assertEquals(Status.Code.UNAUTHENTICATED, failure.getStatus().getCode());
    assertEquals("etcdserver: invalid auth token", failure.getStatus().getDescription());
  }

  private static final class RpcNode implements AutoCloseable {
    private final io.grpc.Server server;
    private final ScheduledThreadPoolExecutor deadlines;
    private final RequestScopeInterceptor requests;
    private final ManagedChannel channel;

    private RpcNode(io.grpc.Server server, ScheduledThreadPoolExecutor deadlines,
        RequestScopeInterceptor requests, ManagedChannel channel) {
      this.server = server;
      this.deadlines = deadlines;
      this.requests = requests;
      this.channel = channel;
    }

    static RpcNode start(KvService kv, AuthService auth) throws Exception {
      return start(kv, new LeaseService(), auth);
    }

    static RpcNode start(KvService kv, LeaseService lease, AuthService auth) throws Exception {
      var deadlines = new ScheduledThreadPoolExecutor(1,
          Thread.ofPlatform().daemon().name("j50-rpc-deadline-", 0).factory());
      deadlines.setRemoveOnCancelPolicy(true);
      deadlines.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
      var requests = new RequestScopeInterceptor(deadlines, 8, Duration.ofSeconds(10),
          1 << 20, new RequestMetrics());
      io.grpc.Server server = NettyServerBuilder
          .forAddress(new InetSocketAddress("127.0.0.1", 0))
          .maxInboundMessageSize(1 << 20)
          .intercept(requests)
          .addService(kv)
          .addService(lease)
          .addService(auth)
          .build()
          .start();
      ManagedChannel channel = ManagedChannelBuilder
          .forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
      return new RpcNode(server, deadlines, requests, channel);
    }

    ManagedChannel channel() { return channel; }

    @Override
    public void close() throws Exception {
      channel.shutdown();
      if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
        channel.shutdownNow();
        if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
          throw new IllegalStateException("test gRPC channel did not terminate");
        }
      }
      server.shutdown();
      if (!server.awaitTermination(3, TimeUnit.SECONDS)) {
        server.shutdownNow();
        if (!server.awaitTermination(3, TimeUnit.SECONDS)) {
          throw new IllegalStateException("test gRPC server did not terminate");
        }
      }
      requests.close();
      deadlines.shutdown();
      if (!deadlines.awaitTermination(3, TimeUnit.SECONDS)) {
        deadlines.shutdownNow();
        if (!deadlines.awaitTermination(3, TimeUnit.SECONDS)) {
          throw new IllegalStateException("test deadline scheduler did not terminate");
        }
      }
    }
  }

  private static final class TestCredentialKeyProvider implements CredentialKeyProvider {
    private final byte[] key = fill(32, (byte) 0x6a);

    @Override public String activeKeyId() { return "test-key"; }

    @Override
    public byte[] keyBytes(String keyId) {
      if (!"test-key".equals(keyId)) throw new IllegalArgumentException("unknown test key");
      return key.clone();
    }

    @Override public void close() { Arrays.fill(key, (byte) 0); }
  }

  private static final class TestSessionIdentityManager implements SessionIdentityManager {
    private final String replica;
    private final CountDownLatch releases;
    private final AtomicInteger releaseCount = new AtomicInteger();
    private final AtomicInteger activationCount = new AtomicInteger();
    private final java.util.List<AuthorizedBackendIdentity> activated =
        new java.util.concurrent.CopyOnWriteArrayList<>();

    private TestSessionIdentityManager(String replica, int expectedReleases) {
      this.replica = replica;
      this.releases = new CountDownLatch(expectedReleases);
    }

    @Override
    public CompletionStage<AuthorizedBackendIdentity> authenticate(
        BackendCredentials credentials, AuthenticationScope scope) {
      return CompletableFuture.completedFuture(new AuthorizedBackendIdentity(PROVIDER,
          credentials.principal(), replica + "-authenticated", scope, Set.of(TABLE_GROUP), 7));
    }

    @Override
    public CompletionStage<Void> verifyRestorable(
        BackendCredentials credentials, AuthorizedBackendIdentity authenticatedIdentity) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<SessionIdentityLease> activate(
        AuthSession session, BackendCredentials credentials) {
      int activation = activationCount.incrementAndGet();
      var identity = new AuthorizedBackendIdentity(PROVIDER, credentials.principal(),
          replica + "-context-" + activation, session.scope(), Set.of(TABLE_GROUP),
          session.credentialGeneration());
      activated.add(identity);
      return CompletableFuture.completedFuture(new SessionIdentityLease(identity, () -> {
        releaseCount.incrementAndGet();
        releases.countDown();
        return CompletableFuture.completedFuture(null);
      }));
    }

    @Override
    public CompletionStage<Void> discardUnpublished(AuthorizedBackendIdentity identity) {
      return CompletableFuture.completedFuture(null);
    }

    java.util.List<AuthorizedBackendIdentity> activatedIdentities() { return activated; }
    int releaseCount() { return releaseCount.get(); }
    boolean awaitReleases() throws InterruptedException {
      return releases.await(2, TimeUnit.SECONDS);
    }

    @Override public void close() { }
  }

  private static final class InMemoryAuthSessionStore implements AuthSessionStore {
    private final AuthDeploymentFixture deployment = new AuthDeploymentFixture(
        this, "milvus", PROVIDER, AUTH_SCOPE, DATASET, Clock.systemUTC());
    private final Map<ByteKey, AuthSession> sessions = new HashMap<>();
    private final Map<Integer, AuthSlot> slots = new HashMap<>();
    private final Map<ByteKey, SubjectAuthority> authorities = new HashMap<>();

    @Override
    public synchronized CompletionStage<Optional<AuthSession>> readSession(
        ByteKey tokenDigest, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(sessions.get(tokenDigest)));
    }

    @Override
    public synchronized CompletionStage<Boolean> createSessionIfAbsent(
        AuthSession session, RequestScope scope) {
      if (sessions.containsKey(session.tokenDigest())) return CompletableFuture.completedFuture(false);
      sessions.put(session.tokenDigest(), session);
      return CompletableFuture.completedFuture(true);
    }

    @Override
    public synchronized CompletionStage<Boolean> deleteSessionIfUnchanged(
        AuthSession observed, RequestScope scope) {
      return CompletableFuture.completedFuture(sessions.remove(observed.tokenDigest(), observed));
    }

    @Override
    public synchronized CompletionStage<Boolean> compareAndSetSession(
        AuthSession observed, AuthSession replacement, RequestScope scope) {
      if (!observed.equals(sessions.get(observed.tokenDigest()))) {
        return CompletableFuture.completedFuture(false);
      }
      sessions.put(observed.tokenDigest(), replacement);
      return CompletableFuture.completedFuture(true);
    }

    @Override
    public synchronized CompletionStage<Optional<AuthSlot>> readSlot(int index, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(slots.get(index)));
    }

    @Override
    public synchronized CompletionStage<Boolean> createSlotIfAbsent(AuthSlot slot, RequestScope scope) {
      if (slots.containsKey(slot.index())) return CompletableFuture.completedFuture(false);
      slots.put(slot.index(), slot);
      return CompletableFuture.completedFuture(true);
    }

    @Override
    public synchronized CompletionStage<Boolean> compareAndSetSlot(
        AuthSlot observed, AuthSlot replacement, RequestScope scope) {
      if (!observed.equals(slots.get(observed.index()))) {
        return CompletableFuture.completedFuture(false);
      }
      slots.put(observed.index(), replacement);
      return CompletableFuture.completedFuture(true);
    }

    @Override
    public synchronized CompletionStage<Optional<SubjectAuthority>> readAuthority(
        ByteKey subjectDigest, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(authorities.get(subjectDigest)));
    }

    @Override
    public synchronized CompletionStage<Boolean> createAuthorityIfAbsent(
        SubjectAuthority authority, RequestScope scope) {
      if (authority.state() != SubjectAuthority.State.ACTIVE
          || !authority.revocationTargets().isEmpty()
          || authorities.containsKey(authority.subjectDigest())) {
        return CompletableFuture.completedFuture(false);
      }
      authorities.put(authority.subjectDigest(), authority);
      return CompletableFuture.completedFuture(true);
    }

    @Override
    public synchronized CompletionStage<Boolean> compareAndSetAuthority(
        SubjectAuthority observed, SubjectAuthority replacement, RequestScope scope) {
      if (!observed.equals(authorities.get(observed.subjectDigest()))
          || !observed.allowsTransitionTo(replacement)) {
        return CompletableFuture.completedFuture(false);
      }
      authorities.put(observed.subjectDigest(), replacement);
      return CompletableFuture.completedFuture(true);
    }
  }
}
