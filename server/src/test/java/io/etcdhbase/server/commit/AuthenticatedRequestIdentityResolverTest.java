package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class AuthenticatedRequestIdentityResolverTest {
  private static final String TABLE_GROUP = "auth-group";
  private static final AuthenticationScope SCOPE = new AuthenticationScope(TABLE_GROUP, "profile");

  @Test
  void missingTokenIsRejectedWhenAuthenticationIsEnabled() {
    var resolver = resolver(identity("hbase-provider"), mock(AuthSessionService.class));
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(java.util.concurrent.CompletionException.class,
          () -> resolver.resolve(Optional.empty(), scope).join());

      assertEquals(AuthSessionException.Reason.INVALID_TOKEN,
          ((AuthSessionException) failure.getCause()).reason());
    }
  }

  @Test
  void developmentIdentityRemainsAvailableWithoutToken() {
    AuthorizedBackendIdentity developmentIdentity =
        identity(BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID);
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      var resolved = resolver(developmentIdentity, null).resolve(Optional.empty(), scope).join();
      assertEquals(ByteKey.copyOf(new byte[32]), resolved.authorizationDigest());
      assertEquals(developmentIdentity, resolved.identity());
    }
  }

  @Test
  void unauthenticatedModeUsesServiceIdentityForOrdinaryProvider() {
    AuthorizedBackendIdentity serviceIdentity = identity("hbase-provider");
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      var resolved = resolver(serviceIdentity, null).resolve(Optional.empty(), scope).join();

      assertEquals(serviceIdentity, resolved.identity());
      assertEquals(ByteKey.copyOf(new byte[32]), resolved.authorizationDigest());
      assertNull(resolved.sessionIdentity());
    }
  }

  @Test
  void suppliedTokenWithoutSessionServiceRemainsUnavailable() {
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(java.util.concurrent.CompletionException.class,
          () -> resolver(identity("hbase-provider"), null)
              .resolve(Optional.of("opaque-token"), scope).join());

      assertEquals(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE,
          ((AuthSessionException) failure.getCause()).reason());
    }
  }

  @Test
  void revalidationRetiresOnlyOnExplicitSessionRejection() {
    var failures = java.util.List.of(
        new AuthSessionException(AuthSessionException.Reason.INVALID_TOKEN),
        new AuthSessionException(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE),
        new BackendFailure(BackendFailure.Category.PERMISSION));
    for (var recheckFailure : failures) {
      var sessions = mock(AuthSessionService.class);
      var held = mock(AuthSessionService.ResolvedIdentity.class);
      var session = mock(AuthSession.class);
      var digest = ByteKey.copyOf(new byte[32]);
      when(session.tokenDigest()).thenReturn(digest);
      when(held.session()).thenReturn(session);
      when(sessions.resolveByDigest(eq(digest), any(RequestScope.class)))
          .thenReturn(CompletableFuture.failedFuture(recheckFailure));
      var identity = identity("hbase-provider");
      var requestIdentity = new AuthenticatedRequestIdentityResolver.ResolvedRequestIdentity(
          identity, digest, digest, held);
      try (RequestScope scope = RequestScope.withoutDeadline()) {
        assertThrows(java.util.concurrent.CompletionException.class,
            () -> resolver(identity, sessions).revalidate(requestIdentity, scope).join());
        if (recheckFailure instanceof AuthSessionException auth
            && auth.reason() == AuthSessionException.Reason.INVALID_TOKEN) {
          verify(held).retire();
        } else {
          verify(held, never()).retire();
        }
      }
    }
  }

  @Test
  void rejectedStreamRevalidationInvalidatesTheCapturedReadAuthorization() {
    var sessions = mock(AuthSessionService.class);
    var held = mock(AuthSessionService.ResolvedIdentity.class);
    var envelope = new CredentialEnvelope("key-1", CredentialEnvelope.CURRENT_AAD_VERSION,
        new byte[CredentialEnvelope.NONCE_BYTES], new byte[CredentialEnvelope.TAG_BYTES]);
    var session = new AuthSession(ByteKey.copyOf(new byte[16]), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), "domain", "hbase-provider", SCOPE,
        ByteKey.copyOf(new byte[32]), 0, 1, 1, 2, 0, 1, AuthSession.Status.ACTIVE, envelope);
    var identity = identity("hbase-provider");
    var invalidated = new AtomicReference<ByteKey>();
    when(held.session()).thenReturn(session);
    when(sessions.resolveByDigest(eq(session.tokenDigest()), any(RequestScope.class)))
        .thenReturn(CompletableFuture.failedFuture(
            new AuthSessionException(AuthSessionException.Reason.INVALID_TOKEN)));
    var resolver = new AuthenticatedRequestIdentityResolver(TABLE_GROUP, identity,
        ByteKey.copyOf(new byte[32]), sessions, invalidated::set);
    var captured = new AuthenticatedRequestIdentityResolver.ResolvedRequestIdentity(identity,
        AuthorizationScopeDigest.fromIdentity(identity), ByteKey.copyOf(new byte[32]), held);
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      assertThrows(java.util.concurrent.CompletionException.class,
          () -> resolver.revalidate(captured, scope).join());
      assertEquals(captured.readAuthorizationDigest(), invalidated.get());
      verify(held).retire();
    }
  }

  @Test
  void unavailableRevalidationDoesNotInvalidateOrRetireAnIdentity() {
    var sessions = mock(AuthSessionService.class);
    var held = mock(AuthSessionService.ResolvedIdentity.class);
    var session = mock(AuthSession.class);
    var digest = ByteKey.copyOf(new byte[32]);
    var invalidated = new AtomicReference<ByteKey>();
    when(session.tokenDigest()).thenReturn(digest);
    when(held.session()).thenReturn(session);
    when(sessions.resolveByDigest(eq(digest), any(RequestScope.class)))
        .thenReturn(CompletableFuture.failedFuture(
            new AuthSessionException(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE)));
    var identity = identity("hbase-provider");
    var resolver = new AuthenticatedRequestIdentityResolver(TABLE_GROUP, identity, digest,
        sessions, invalidated::set);
    var captured = new AuthenticatedRequestIdentityResolver.ResolvedRequestIdentity(
        identity, digest, digest, held);
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      assertThrows(java.util.concurrent.CompletionException.class,
          () -> resolver.revalidate(captured, scope).join());
      assertNull(invalidated.get());
      verify(held, never()).retire();
    }
  }

  private static AuthenticatedRequestIdentityResolver resolver(
      AuthorizedBackendIdentity identity, io.etcdhbase.server.auth.AuthSessionService sessions) {
    return new AuthenticatedRequestIdentityResolver(TABLE_GROUP, identity,
        ByteKey.copyOf(new byte[32]), sessions);
  }

  private static AuthorizedBackendIdentity identity(String provider) {
    return new AuthorizedBackendIdentity(provider, "service", "context", SCOPE,
        Set.of(TABLE_GROUP), 0);
  }
}
