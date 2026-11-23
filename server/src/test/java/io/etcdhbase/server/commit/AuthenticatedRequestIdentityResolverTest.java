package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class AuthenticatedRequestIdentityResolverTest {
  private static final String TABLE_GROUP = "auth-group";
  private static final AuthenticationScope SCOPE = new AuthenticationScope(TABLE_GROUP, "profile");

  @Test
  void missingTokenIsRejectedOutsideExplicitDevelopmentIdentity() {
    var resolver = resolver(identity("hbase-provider"), null);
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
