package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseExpiryStepCommand;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class LocalLeaseExpiryAuthorizationResolverTest {
  private static final String GROUP = "group";
  private static final ByteKey AUTHORIZATION = ByteKey.copyOf(new byte[32]);
  private static final AuthorizedBackendIdentity SERVICE = new AuthorizedBackendIdentity(
      "provider", "service", "context", new AuthenticationScope(GROUP, "profile"),
      Set.of(GROUP), 0);
  private static final ByteKey REFERENCE =
      ServiceAuthorizationContextReference.create(SERVICE, AUTHORIZATION);
  private static final ByteKey EXPIRY = MutationOperationEnvelope.leaseExpiryStep(
      new LeaseExpiryStepCommand(0).encode().bytes());

  @Test
  void localCurrentOwnerCanResolveExpiryButCannotAuthorizeOtherMutations() {
    var resolver = new LocalLeaseExpiryAuthorizationResolver(GROUP, SERVICE, AUTHORIZATION,
        "local-owner"::equals);
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      var verified = resolver.verifyAndResolve("local-owner",
          request(GROUP, EXPIRY, AUTHORIZATION, REFERENCE), 7, scope).join();
      assertSame(SERVICE, verified.identity());
      assertEquals(7, verified.publicationProof().authorizationBarrierGeneration());
      verified.publicationProof().reauthorize(7, scope).join();

      assertDenied(resolver, "local-owner", request(GROUP, ByteKey.copyOf(new byte[] {1}),
          AUTHORIZATION, REFERENCE), scope);
      assertDenied(resolver, "old-owner", request(GROUP, EXPIRY, AUTHORIZATION, REFERENCE), scope);
      assertDenied(resolver, "local-owner", request("other-group", EXPIRY, AUTHORIZATION, REFERENCE), scope);
      assertDenied(resolver, "local-owner", request(GROUP, EXPIRY,
          ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32])), scope);
      byte[] changed = new byte[32];
      changed[0] = 1;
      assertDenied(resolver, "local-owner", request(GROUP, EXPIRY,
          ByteKey.copyOf(changed), REFERENCE), scope);
    }
  }

  @Test
  void queuedExpiryIsFencedOnOwnerChangeAndNewOwnerCanContinue() throws Exception {
    long firstDeadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1);
    var state = new AtomicReference<>(CommitState.initial(AUTHORIZATION, 1)
        .claimOwner("owner-a", 0, firstDeadline));
    var current = new AtomicReference<>(Optional.of(new CommitOwnerService.OwnerToken("owner-a", 1)));
    var control = mock(CommitControlStore.class);
    when(control.read(any())).thenAnswer(ignored -> CompletableFuture.completedFuture(
        Optional.of(new ControlSnapshot(state.get(), new byte[] {1}))));
    var owner = mock(M3OwnerLease.class);
    when(owner.currentToken()).thenAnswer(ignored -> current.get());
    var invocations = mock(InvocationService.class);
    when(invocations.bindOrJoin(any(), any())).thenAnswer(call ->
        CompletableFuture.completedFuture(call.getArgument(0)));
    var calls = new AtomicInteger();
    var started = new CountDownLatch(1);
    var held = new CompletableFuture<Void>();
    var queue = new SerialMutationQueue(2, 20_000, 2, 20_000, Duration.ofSeconds(30));
    try (RequestScope holdingScope = RequestScope.withoutDeadline();
         RequestScope firstScope = RequestScope.withoutDeadline();
         RequestScope secondScope = RequestScope.withoutDeadline()) {
      var running = queue.submit(1, holdingScope, ignored -> {
        started.countDown();
        return held;
      });
      assertTrue(started.await(5, TimeUnit.SECONDS));
      var receiver = new MutationForwardReceiver(control, invocations,
          new LocalLeaseExpiryAuthorizationResolver(GROUP, SERVICE, AUTHORIZATION,
              peer -> current.get().map(token -> token.instance().equals(peer)).orElse(false)),
          (request, binding, verified, scope) -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(
                new OwnerForwarding.Completed(request.invocationId().bytes()));
          });
      var lane = new MutationOwnerLane(GROUP, queue, receiver);
      var relay = M3TableGroupRuntime.localOwnerRelay(GROUP, control, owner, lane);
      var oldAttempt = relay.forward(localRequest(1), firstScope);
      assertEquals(1, queue.snapshot().waiting());
      assertFalse(oldAttempt.isDone());

      state.set(state.get().claimOwner("owner-b", firstDeadline, Long.MAX_VALUE));
      current.set(Optional.empty());
      held.complete(null);
      running.get(5, TimeUnit.SECONDS);
      var rejected = assertInstanceOf(MutationRelay.ResultUnknown.class,
          oldAttempt.get(5, TimeUnit.SECONDS));
      assertEquals(BackendFailure.Category.UNAVAILABLE, rejected.category());
      assertEquals(0, calls.get(), "the old owner must not dispatch the queued expiry");

      current.set(Optional.of(new CommitOwnerService.OwnerToken("owner-b", 2)));
      assertInstanceOf(MutationRelay.KnownResult.class,
          relay.forward(localRequest(2), secondScope).get(5, TimeUnit.SECONDS));
      assertEquals(1, calls.get());
    } finally {
      held.complete(null);
      queue.close();
      queue.drained().get(5, TimeUnit.SECONDS);
      assertTrue(queue.awaitTermination(Duration.ofSeconds(5)));
    }
  }

  private static void assertDenied(LocalLeaseExpiryAuthorizationResolver resolver, String peer,
      MutationForwardRequest request, RequestScope scope) {
    var failure = assertThrows(CompletionException.class,
        () -> resolver.verifyAndResolve(peer, request, 7, scope).join());
    assertEquals(BackendFailure.Category.PERMISSION, ((BackendFailure) failure.getCause()).category());
  }

  private static MutationForwardRequest request(String group, ByteKey operation,
      ByteKey authorization, ByteKey reference) {
    return new MutationForwardRequest(group, InvocationId.create(),
        MutationForwardRequest.digest(group, operation), authorization, reference, 1,
        TimeUnit.SECONDS.toNanos(1), MutationForwardRequest.MAX_HOPS, operation);
  }

  private static MutationForwardRequest localRequest(long epoch) {
    return new MutationForwardRequest(GROUP, InvocationId.create(),
        MutationForwardRequest.digest(GROUP, EXPIRY), AUTHORIZATION, REFERENCE, epoch,
        TimeUnit.SECONDS.toNanos(10), 0, EXPIRY);
  }
}
