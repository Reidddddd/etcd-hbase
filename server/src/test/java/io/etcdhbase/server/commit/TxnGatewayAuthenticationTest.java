package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TxnGatewayAuthenticationTest {
  private static final String TABLE_GROUP = "auth-group";
  private static final String TOKEN = "opaque-token-from-authenticate";

  @Test
  void rangeUsesResolvedSessionIdentityAndReleasesOnlyAfterPhysicalReadDrains() throws Exception {
    var serviceIdentity = identity("service-user");
    var userIdentity = identity("milvus-user");
    var authorization = ByteKey.copyOf(new byte[32]);
    var requestAuthorization = AuthorizationScopeDigest.fromIdentity(userIdentity);
    AuthSessionService sessions = mock(AuthSessionService.class);
    AuthSessionService.ResolvedIdentity resolved = mock(AuthSessionService.ResolvedIdentity.class);
    AtomicInteger releases = new AtomicInteger();
    when(resolved.identity()).thenReturn(userIdentity);
    when(resolved.session()).thenReturn(session());
    when(resolved.release()).thenAnswer(ignored -> {
      releases.incrementAndGet();
      return CompletableFuture.completedFuture(null);
    });
    when(sessions.resolve(eq(TOKEN), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(resolved));

    TxnReadExecutor reads = mock(TxnReadExecutor.class);
    var readResult = new CompletableFuture<io.etcdhbase.core.kv.TxnEvaluator.ValidatedRead>();
    var physicalRead = new CompletableFuture<Void>();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      when(reads.execute(any(), any(), any(), any(), any(), anyBoolean())).thenAnswer(invocation -> {
        assertSame(userIdentity, invocation.getArgument(2));
        assertEquals(requestAuthorization, invocation.getArgument(1));
        assertSame(scope, invocation.getArgument(3));
        scope.trackPhysicalProducer(physicalRead);
        return readResult;
      });

      var budgets = budgets();
      var reservation = new BudgetManager.Reservation(1L << 16, 1L << 16, 1L << 16,
          1L << 16, 1L << 16);
      var gateway = new TxnGateway(TABLE_GROUP, serviceIdentity, authorization, 1, 1,
          1 << 20, mock(io.etcdhbase.core.commit.CommitControlStore.class),
          mock(MutationRelay.class), mock(io.etcdhbase.core.commit.InvocationResultResolver.class),
          reads, budgets, reservation, Runnable::run, new RequestMetrics(), sessions);

      gateway.accept(rangeTxn(), Optional.of(TOKEN), scope, ignoringObserver());

      verify(sessions).resolve(TOKEN, scope);
      assertEquals(0, releases.get());
      assertFalse(scope.physicalProducersCompletion().toCompletableFuture().isDone());

      scope.cancel();
      assertEquals(0, releases.get(), "cancel must not release the identity while HBase work is active");

      physicalRead.complete(null);
      readResult.completeExceptionally(new CancellationException("test read cancelled"));
      scope.cleanupCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS);
      assertEquals(1, releases.get());
    }
  }

  private static AuthorizedBackendIdentity identity(String subject) {
    var scope = new AuthenticationScope(TABLE_GROUP, "profile");
    return new AuthorizedBackendIdentity("provider", subject, "context-" + subject,
        scope, Set.of(TABLE_GROUP), 1);
  }

  private static AuthSession session() {
    var envelope = new CredentialEnvelope("key-1", CredentialEnvelope.CURRENT_AAD_VERSION,
        new byte[CredentialEnvelope.NONCE_BYTES], new byte[CredentialEnvelope.TAG_BYTES]);
    return new AuthSession(ByteKey.copyOf(new byte[16]), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), "milvus", "provider",
        new AuthenticationScope(TABLE_GROUP, "profile"), ByteKey.copyOf(new byte[32]),
        1, 1, 1, 2, 0, 1, AuthSession.Status.ACTIVE, envelope);
  }

  private static Rpc.TxnRequest rangeTxn() {
    var range = Rpc.RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("key")).build();
    return Rpc.TxnRequest.newBuilder()
        .addSuccess(Rpc.RequestOp.newBuilder().setRequestRange(range))
        .build();
  }

  private static BudgetManager budgets() {
    var limits = new BudgetManager.Limits(2, 0, 2, 2, 2, 0, 4L << 20);
    return new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits);
  }

  private static StreamObserver<Rpc.TxnResponse> ignoringObserver() {
    return new StreamObserver<>() {
      @Override public void onNext(Rpc.TxnResponse response) { }
      @Override public void onError(Throwable failure) { }
      @Override public void onCompleted() { }
    };
  }
}
