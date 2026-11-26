package io.etcdhbase.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import etcdserverpb.Rpc;
import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.commit.LeaseBindingReader;
import io.etcdhbase.core.commit.LeaseEnumerationReader;
import io.etcdhbase.core.commit.LeaseReadView;
import io.etcdhbase.core.commit.PinnedReadView;
import io.etcdhbase.core.commit.ReadView;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.server.commit.LeaseGateway;
import io.etcdhbase.server.commit.MutationRelay;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class LeaseGatewayReadAuthorizationTest {
  private static final String GROUP = "group";
  private static final String TOKEN = "token";

  @Test
  void queuedLeaseReadRechecksSessionAfterRevocation() {
    try (var fixture = new Fixture(); var scope = RequestScope.withoutDeadline()) {
      fixture.start(scope);
      fixture.worker.runNext();
      fixture.revoked.set(true);
      fixture.worker.runNext();
      assertEquals(0, fixture.responses.get());
      assertNotNull(fixture.invalidated.get());
      assertEquals(Status.Code.UNAUTHENTICATED,
          assertInstanceOf(StatusRuntimeException.class, fixture.failure.get()).getStatus().getCode());
    }
  }

  @Test
  void leaseReadWaitsForRevalidationAndDrainsOnCancellation() throws Exception {
    try (var fixture = new Fixture(); var scope = RequestScope.withoutDeadline()) {
      fixture.pending.set(true);
      fixture.start(scope);
      fixture.worker.runNext();
      fixture.worker.runNext();
      assertEquals(0, fixture.responses.get());
      scope.cancel();
      assertFalse(scope.physicalProducersCompletion().toCompletableFuture().isDone());
      assertEquals(0, fixture.releases.get());
      fixture.validation.complete(fixture.current);
      scope.cleanupCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS);
      assertEquals(0, fixture.responses.get());
      assertEquals(2, fixture.releases.get());
    }
  }

  @Test
  void leaseReadSucceedsWhenSessionRemainsValid() {
    try (var fixture = new Fixture(); var scope = RequestScope.withoutDeadline()) {
      fixture.start(scope);
      fixture.worker.runNext();
      fixture.worker.runNext();
      assertEquals(1, fixture.responses.get());
      assertTrue(fixture.completed.get());
      assertNull(fixture.invalidated.get());
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final QueueExecutor worker = new QueueExecutor();
    private final ScheduledThreadPoolExecutor timers = new ScheduledThreadPoolExecutor(1);
    private final AuthSessionService sessions = mock(AuthSessionService.class);
    private final AuthSessionService.ResolvedIdentity original = mock(AuthSessionService.ResolvedIdentity.class);
    private final AuthSessionService.ResolvedIdentity current = mock(AuthSessionService.ResolvedIdentity.class);
    private final CompletableFuture<AuthSessionService.ResolvedIdentity> validation = new CompletableFuture<>();
    private final AtomicBoolean revoked = new AtomicBoolean();
    private final AtomicBoolean pending = new AtomicBoolean();
    private final AtomicBoolean completed = new AtomicBoolean();
    private final AtomicInteger responses = new AtomicInteger();
    private final AtomicInteger releases = new AtomicInteger();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicReference<ByteKey> invalidated = new AtomicReference<>();

    private void start(RequestScope scope) {
      var authScope = new AuthenticationScope(GROUP, "profile");
      var identity = new AuthorizedBackendIdentity("provider", "user", "context",
          authScope, Set.of(GROUP), 1);
      var envelope = new CredentialEnvelope("key", CredentialEnvelope.CURRENT_AAD_VERSION,
          new byte[CredentialEnvelope.NONCE_BYTES], new byte[CredentialEnvelope.TAG_BYTES]);
      var session = new AuthSession(ByteKey.copyOf(new byte[16]), ByteKey.copyOf(new byte[32]),
          ByteKey.copyOf(new byte[32]), "trust", "provider", authScope, ByteKey.copyOf(new byte[32]),
          1, 1, 1, 2, 0, 1, AuthSession.Status.ACTIVE, envelope);
      for (var resolved : new AuthSessionService.ResolvedIdentity[] {original, current}) {
        when(resolved.identity()).thenReturn(identity);
        when(resolved.session()).thenReturn(session);
        when(resolved.release()).thenAnswer(ignored -> {
          releases.incrementAndGet();
          return CompletableFuture.completedFuture(null);
        });
      }
      when(sessions.resolve(any(), any())).thenReturn(CompletableFuture.completedFuture(original));
      when(sessions.resolveByDigest(any(), any())).thenAnswer(ignored -> {
        if (revoked.get()) return CompletableFuture.failedFuture(
            new AuthSessionException(AuthSessionException.Reason.INVALID_TOKEN));
        return pending.get() ? validation : CompletableFuture.completedFuture(current);
      });
      var view = mock(ReadView.class);
      when(view.targetRevision()).thenReturn(1L);
      var pin = mock(PinnedReadView.class);
      when(pin.view()).thenReturn(view);
      var views = mock(ReadViewService.class);
      when(views.openAuxiliary(any(), any())).thenReturn(CompletableFuture.completedFuture(pin));
      var leaseReads = mock(LeaseReadView.class);
      when(leaseReads.finish(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
      var time = mock(LeaseTimeSource.class);
      when(time.observe()).thenReturn(new LeaseTimeObservation(1, 1, true));
      var limits = new BudgetManager.Limits(2, 0, 2, 2, 2, 0, 4L << 20);
      var gateway = new LeaseGateway(GROUP, identity, ByteKey.copyOf(new byte[32]), 1, 1, 1 << 20,
          mock(CommitControlStore.class), mock(MutationRelay.class), mock(InvocationResultResolver.class),
          views, leaseReads, mock(LeaseEnumerationReader.class), mock(LeaseBindingReader.class), time,
          new BudgetManager(limits, Map.of(GROUP, limits), limits),
          new BudgetManager.Reservation(1L << 16, 1L << 16, 1L << 16, 1L << 18, 1L << 16),
          worker, timers, new RequestMetrics(), sessions, invalidated::set);
      var context = Context.current().withValues(RequestScopeInterceptor.SCOPE, scope,
          RequestScopeInterceptor.AUTH_TOKEN, TOKEN);
      var previous = context.attach();
      try {
        gateway.timeToLiveHandler().accept(Rpc.LeaseTimeToLiveRequest.getDefaultInstance(),
            new StreamObserver<>() {
              @Override public void onNext(Rpc.LeaseTimeToLiveResponse response) { responses.incrementAndGet(); }
              @Override public void onError(Throwable problem) { failure.set(problem); }
              @Override public void onCompleted() { completed.set(true); }
            });
      } finally {
        context.detach(previous);
      }
    }

    @Override public void close() { timers.shutdownNow(); }
  }

  private static final class QueueExecutor implements Executor {
    private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
    @Override public void execute(Runnable task) { tasks.addLast(task); }
    private void runNext() { tasks.removeFirst().run(); }
  }
}
