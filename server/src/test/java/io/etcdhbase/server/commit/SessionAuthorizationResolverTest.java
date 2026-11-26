package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseExpiryStepCommand;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SessionAuthorizationResolverTest {
  private static final String TABLE_GROUP = "auth-group";
  private static final String PEER = "authenticated-owner-peer";
  private static final AuthenticationScope SCOPE = new AuthenticationScope(TABLE_GROUP, "profile");
  private static final long AUTHORIZATION_BARRIER_GENERATION = 19;

  @Test
  void userSessionCannotAuthorizeInternalLeaseExpiryStep() {
    AuthSessionService sessions = mock(AuthSessionService.class);
    AuthSessionService.ResolvedIdentity resolved = mock(AuthSessionService.ResolvedIdentity.class);
    AuthSession session = session(3);
    when(resolved.session()).thenReturn(session);
    when(resolved.identity()).thenReturn(identity(TABLE_GROUP));
    when(resolved.release()).thenReturn(CompletableFuture.completedFuture(null));
    when(sessions.resolveByDigest(eq(session.tokenDigest()), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(resolved));
    ByteKey operation = MutationOperationEnvelope.leaseExpiryStep(
        new LeaseExpiryStepCommand(0).encode().bytes());
    var request = new MutationForwardRequest(TABLE_GROUP, InvocationId.create(),
        MutationForwardRequest.digest(TABLE_GROUP, operation),
        AuthorizationScopeDigest.fromIdentity(identity(TABLE_GROUP)),
        SessionAuthorizationContextReference.fromSession(session).encode(), 1,
        TimeUnit.SECONDS.toNanos(1), MutationForwardRequest.MAX_HOPS, operation);
    var resolver = new SessionAuthorizationResolver(TABLE_GROUP, sessions, PEER::equals);
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(java.util.concurrent.CompletionException.class,
          () -> resolver.verifyAndResolve(PEER, request, AUTHORIZATION_BARRIER_GENERATION, scope).join());
      assertEquals(BackendFailure.Category.PERMISSION,
          ((BackendFailure) failure.getCause()).category());
    }
  }

  @Test
  void ownerResolvesMatchingDurableReferenceAndReleasesAfterPhysicalDrain() throws Exception {
    AuthSessionService sessions = mock(AuthSessionService.class);
    AuthSessionService.ResolvedIdentity resolved = mock(AuthSessionService.ResolvedIdentity.class);
    AuthSession session = session(3);
    AuthorizedBackendIdentity identity = identity(TABLE_GROUP);
    when(resolved.session()).thenReturn(session);
    when(resolved.identity()).thenReturn(identity);
    AtomicInteger releases = new AtomicInteger();
    when(resolved.release()).thenAnswer(ignored -> {
      releases.incrementAndGet();
      return CompletableFuture.completedFuture(null);
    });
    when(sessions.resolveByDigest(eq(session.tokenDigest()), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(resolved));

    var resolver = new SessionAuthorizationResolver(TABLE_GROUP, sessions, PEER::equals);
    RequestScope scope = RequestScope.withoutDeadline();
    var physicalProducer = new CompletableFuture<Void>();
    scope.trackPhysicalProducer(physicalProducer);
    try {
      var verified = resolver.verifyAndResolve(
          PEER, request(session), AUTHORIZATION_BARRIER_GENERATION, scope).join();
      assertSame(identity, verified.identity());
      assertEquals(AuthorizationScopeDigest.fromIdentity(identity),
          verified.authorizationScopeDigest());
      assertEquals(AUTHORIZATION_BARRIER_GENERATION,
          verified.publicationProof().authorizationBarrierGeneration());

      scope.cancel();
      assertEquals(0, releases.get(), "identity must remain borrowed while owner work is active");
      assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());

      physicalProducer.complete(null);
      scope.cleanupCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS);
      assertEquals(1, releases.get());
    } finally {
      physicalProducer.complete(null);
      scope.close();
    }
  }

  @Test
  void ownerRetiresCapturedIdentityWhenPublicationRecheckFindsRevokedSession() throws Exception {
    AuthSessionService sessions = mock(AuthSessionService.class);
    AuthSessionService.ResolvedIdentity held = mock(AuthSessionService.ResolvedIdentity.class);
    AuthSession session = session(3);
    when(held.session()).thenReturn(session);
    when(held.identity()).thenReturn(identity(TABLE_GROUP));
    when(held.retire()).thenReturn(CompletableFuture.completedFuture(null));
    when(held.release()).thenReturn(CompletableFuture.completedFuture(null));
    when(sessions.resolveByDigest(eq(session.tokenDigest()), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(held), CompletableFuture.failedFuture(
            new AuthSessionException(AuthSessionException.Reason.INVALID_TOKEN)));
    var invalidated = new ArrayList<ByteKey>();
    var resolver = new SessionAuthorizationResolver(TABLE_GROUP, sessions, PEER::equals, invalidated::add);
    RequestScope scope = RequestScope.withoutDeadline();
    var physicalProducer = new CompletableFuture<Void>();
    scope.trackPhysicalProducer(physicalProducer);
    try {
      var proof = resolver.verifyAndResolve(
          PEER, request(session), AUTHORIZATION_BARRIER_GENERATION, scope).join().publicationProof();
      var failure = assertThrows(java.util.concurrent.CompletionException.class,
          () -> proof.reauthorize(AUTHORIZATION_BARRIER_GENERATION, scope).join());
      assertEquals(BackendFailure.Category.INVALID_AUTH_TOKEN,
          ((BackendFailure) failure.getCause()).category());
      verify(held).retire();
      assertEquals(List.of(AuthorizationScopeDigest.fromSessionRead(identity(TABLE_GROUP), session)),
          invalidated);
      scope.cancel();
      verify(held, never()).release();
      physicalProducer.complete(null);
      scope.cleanupCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS);
      verify(held).release();
    } finally {
      physicalProducer.complete(null);
      scope.close();
    }
  }

  @Test
  void ownerPhysicalDenialSignalsTheHeldIdentityWithoutWaitingForRetirement() throws Exception {
    AuthSessionService sessions = mock(AuthSessionService.class);
    AuthSessionService.ResolvedIdentity held = mock(AuthSessionService.ResolvedIdentity.class);
    AuthSession session = session(3);
    when(held.session()).thenReturn(session);
    when(held.identity()).thenReturn(identity(TABLE_GROUP));
    var retirement = new CompletableFuture<Void>();
    when(held.retire()).thenReturn(retirement);
    when(held.release()).thenReturn(CompletableFuture.completedFuture(null));
    when(sessions.resolveByDigest(eq(session.tokenDigest()), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(held));
    var invalidated = new ArrayList<ByteKey>();
    var resolver = new SessionAuthorizationResolver(TABLE_GROUP, sessions, PEER::equals, invalidated::add);
    var physicalProducer = new CompletableFuture<Void>();
    RequestScope scope = RequestScope.withoutDeadline();
    scope.trackPhysicalProducer(physicalProducer);
    try {
      var verified = resolver.verifyAndResolve(
          PEER, request(session), AUTHORIZATION_BARRIER_GENERATION, scope).join();
      var denied = new BackendFailure(BackendFailure.Category.PERMISSION);
      var operation = UserIdentityRecordStore.observe(
          CompletableFuture.failedFuture(denied), verified.retireUserIdentity());
      assertSame(denied, assertThrows(java.util.concurrent.CompletionException.class,
          operation::join).getCause());
      verify(held).retire();
      assertEquals(List.of(AuthorizationScopeDigest.fromSessionRead(identity(TABLE_GROUP), session)),
          invalidated);
      assertFalse(retirement.isDone());
      scope.cancel();
      verify(held, never()).release();
      physicalProducer.complete(null);
      scope.cleanupCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS);
      verify(held).release();
    } finally {
      retirement.complete(null);
      physicalProducer.complete(null);
      scope.close();
    }
  }

  @Test
  void changedPublicationSessionInvalidatesHeldIdentityAndReleasesNewBorrow() {
    var sessions = mock(AuthSessionService.class);
    var held = mock(AuthSessionService.ResolvedIdentity.class);
    var current = mock(AuthSessionService.ResolvedIdentity.class);
    var oldSession = session(3);
    when(held.session()).thenReturn(oldSession);
    when(held.identity()).thenReturn(identity(TABLE_GROUP));
    when(held.retire()).thenReturn(CompletableFuture.completedFuture(null));
    when(held.release()).thenReturn(CompletableFuture.completedFuture(null));
    when(current.session()).thenReturn(session(4));
    when(current.identity()).thenReturn(identity(TABLE_GROUP));
    when(current.release()).thenReturn(CompletableFuture.completedFuture(null));
    when(sessions.resolveByDigest(eq(oldSession.tokenDigest()), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(held), CompletableFuture.completedFuture(current));
    var invalidated = new ArrayList<ByteKey>();
    var resolver = new SessionAuthorizationResolver(TABLE_GROUP, sessions, PEER::equals, invalidated::add);
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      var proof = resolver.verifyAndResolve(
          PEER, request(oldSession), AUTHORIZATION_BARRIER_GENERATION, scope).join().publicationProof();
      var failure = assertThrows(java.util.concurrent.CompletionException.class,
          () -> proof.reauthorize(AUTHORIZATION_BARRIER_GENERATION, scope).join());
      assertEquals(BackendFailure.Category.PERMISSION, ((BackendFailure) failure.getCause()).category());
      verify(held).retire();
      verify(current).release();
      assertEquals(List.of(AuthorizationScopeDigest.fromSessionRead(identity(TABLE_GROUP), oldSession)),
          invalidated);
    }
  }

  @Test
  void ownerDoesNotRetireIdentityForUnavailableOrUnattributedBackendFailure() {
    var failures = List.of(
        new AuthSessionException(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE),
        new BackendFailure(BackendFailure.Category.PERMISSION));
    for (var recheckFailure : failures) {
      AuthSessionService sessions = mock(AuthSessionService.class);
      AuthSessionService.ResolvedIdentity held = mock(AuthSessionService.ResolvedIdentity.class);
      AuthSession session = session(3);
      when(held.session()).thenReturn(session);
      when(held.identity()).thenReturn(identity(TABLE_GROUP));
      when(held.release()).thenReturn(CompletableFuture.completedFuture(null));
      when(sessions.resolveByDigest(eq(session.tokenDigest()), any(RequestScope.class)))
          .thenReturn(CompletableFuture.completedFuture(held),
              CompletableFuture.failedFuture(recheckFailure));
      var resolver = new SessionAuthorizationResolver(TABLE_GROUP, sessions, PEER::equals);
      try (RequestScope scope = RequestScope.withoutDeadline()) {
        var proof = resolver.verifyAndResolve(
            PEER, request(session), AUTHORIZATION_BARRIER_GENERATION, scope).join().publicationProof();
        var failure = assertThrows(java.util.concurrent.CompletionException.class,
            () -> proof.reauthorize(AUTHORIZATION_BARRIER_GENERATION, scope).join());
        var expected = recheckFailure instanceof BackendFailure
            ? BackendFailure.Category.PERMISSION : BackendFailure.Category.UNAVAILABLE;
        assertEquals(expected, ((BackendFailure) failure.getCause()).category());
        verify(held, never()).retire();
      }
    }
  }

  @Test
  void ownerRejectsReferenceWithReusedSlotGenerationAndReleasesResolvedIdentity() {
    AuthSessionService sessions = mock(AuthSessionService.class);
    AuthSessionService.ResolvedIdentity resolved = mock(AuthSessionService.ResolvedIdentity.class);
    AuthSession referenced = session(3);
    AuthSession current = session(4);
    when(resolved.session()).thenReturn(current);
    when(resolved.identity()).thenReturn(identity(TABLE_GROUP));
    AtomicInteger releases = new AtomicInteger();
    when(resolved.release()).thenAnswer(ignored -> {
      releases.incrementAndGet();
      return CompletableFuture.completedFuture(null);
    });
    when(sessions.resolveByDigest(eq(referenced.tokenDigest()), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(resolved));

    var resolver = new SessionAuthorizationResolver(TABLE_GROUP, sessions, PEER::equals);
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(java.util.concurrent.CompletionException.class,
          () -> resolver.verifyAndResolve(
              PEER, request(referenced), AUTHORIZATION_BARRIER_GENERATION, scope).join());
      assertEquals(BackendFailure.Category.PERMISSION,
          ((BackendFailure) failure.getCause()).category());
      assertEquals(1, releases.get());
    }
  }

  @Test
  void ownerRejectsIdentityFromAnotherTableGroupAndReleasesIt() {
    AuthSessionService sessions = mock(AuthSessionService.class);
    AuthSessionService.ResolvedIdentity resolved = mock(AuthSessionService.ResolvedIdentity.class);
    AuthSession session = session(3);
    when(resolved.session()).thenReturn(session);
    when(resolved.identity()).thenReturn(identity("other-group"));
    when(resolved.release()).thenReturn(CompletableFuture.completedFuture(null));
    when(sessions.resolveByDigest(eq(session.tokenDigest()), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(resolved));

    var resolver = new SessionAuthorizationResolver(TABLE_GROUP, sessions, PEER::equals);
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(java.util.concurrent.CompletionException.class,
          () -> resolver.verifyAndResolve(
              PEER, request(session), AUTHORIZATION_BARRIER_GENERATION, scope).join());
      assertEquals(BackendFailure.Category.PERMISSION,
          ((BackendFailure) failure.getCause()).category());
      verify(resolved).release();
    }
  }

  @Test
  void ownerPreservesInvalidTokenSemanticsWhenSessionExpiresBeforeForwardedMutation() {
    AuthSessionService sessions = mock(AuthSessionService.class);
    AuthSession referenced = session(3);
    when(sessions.resolveByDigest(eq(referenced.tokenDigest()), any(RequestScope.class)))
        .thenReturn(CompletableFuture.failedFuture(
            new AuthSessionException(AuthSessionException.Reason.INVALID_TOKEN)));

    var resolver = new SessionAuthorizationResolver(TABLE_GROUP, sessions, PEER::equals);
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(java.util.concurrent.CompletionException.class,
          () -> resolver.verifyAndResolve(
              PEER, request(referenced), AUTHORIZATION_BARRIER_GENERATION, scope).join());
      assertEquals(BackendFailure.Category.INVALID_AUTH_TOKEN,
          ((BackendFailure) failure.getCause()).category());
    }
  }

  @Test
  void unauthenticatedPeerCannotResolveSession() {
    AuthSessionService sessions = mock(AuthSessionService.class);
    AuthSession session = session(3);
    var resolver = new SessionAuthorizationResolver(TABLE_GROUP, sessions, PEER::equals);
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(java.util.concurrent.CompletionException.class,
          () -> resolver.verifyAndResolve(
              "untrusted", request(session), AUTHORIZATION_BARRIER_GENERATION, scope).join());
      assertEquals(BackendFailure.Category.PERMISSION,
          ((BackendFailure) failure.getCause()).category());
      verify(sessions, never()).resolveByDigest(any(), any());
    }
  }

  private static MutationForwardRequest request(AuthSession session) {
    ByteKey operation = ByteKey.copyOf(new byte[] {1});
    return new MutationForwardRequest(TABLE_GROUP, InvocationId.create(),
        MutationForwardRequest.digest(TABLE_GROUP, operation),
        AuthorizationScopeDigest.fromIdentity(identity(TABLE_GROUP)),
        SessionAuthorizationContextReference.fromSession(session).encode(), 1,
        TimeUnit.SECONDS.toNanos(1), MutationForwardRequest.MAX_HOPS, operation);
  }

  private static AuthorizedBackendIdentity identity(String group) {
    return new AuthorizedBackendIdentity("provider", "milvus-user", "context",
        new AuthenticationScope(group, "profile"), Set.of(group), 1);
  }

  private static AuthSession session(long slotGeneration) {
    var envelope = new CredentialEnvelope("key-1", CredentialEnvelope.CURRENT_AAD_VERSION,
        new byte[CredentialEnvelope.NONCE_BYTES], new byte[CredentialEnvelope.TAG_BYTES]);
    return new AuthSession(ByteKey.copyOf(new byte[16]), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), "milvus", "provider", SCOPE,
        ByteKey.copyOf(new byte[32]), 1, 1, 1, 1_000, 0, slotGeneration,
        AuthSession.Status.ACTIVE, envelope);
  }
}
