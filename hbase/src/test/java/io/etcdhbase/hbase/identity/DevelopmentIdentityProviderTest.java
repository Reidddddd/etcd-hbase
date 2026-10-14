package io.etcdhbase.hbase.identity;

import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DevelopmentIdentityProviderTest {
  private final DevelopmentIdentityProvider provider = new DevelopmentIdentityProvider();

  @Test
  void returnsOnlyAnExplicitSyntheticDevelopmentIdentity() {
    try (BackendCredentials credentials = new BackendCredentials("developer", new char[] {'x'})) {
      var identity =
          provider.authenticate(credentials, new AuthenticationScope("local", "local-hbase"))
              .toCompletableFuture()
              .join();

      assertEquals(BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID, identity.providerId());
      assertEquals("developer", identity.subject());
      assertEquals(Set.of("local"), identity.allowedTableGroupIds());
      assertEquals("local", identity.scope().tableGroupId());
      assertEquals(0, identity.credentialGeneration());
      assertFalse(identity.toString().contains("developer"));
    }
  }

  @Test
  void rejectsUnboundedOrControlCharacterPrincipals() {
    try (BackendCredentials credentials =
        new BackendCredentials("developer\nforged-log-entry", new char[0])) {
      CompletionException completionFailure = assertThrows(
          CompletionException.class,
          () -> provider.authenticate(credentials, new AuthenticationScope("local", "local-hbase"))
              .toCompletableFuture().join());
      BackendIdentityException failure =
          assertInstanceOf(BackendIdentityException.class, completionFailure.getCause());
      assertEquals(BackendIdentityException.Reason.INVALID_CREDENTIALS, failure.reason());
      assertFalse(failure.getMessage().contains("forged-log-entry"));
    }
  }
}
