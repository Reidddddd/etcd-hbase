package io.etcdhbase.server.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.etcdhbase.hbase.api.HBaseCredentialAuthenticator;
import io.etcdhbase.hbase.api.HBaseUserContext;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.CredentialVersion;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.security.User;
import org.junit.jupiter.api.Test;

class VersionedHBaseAuthenticatorTest {
  private static final AuthenticationScope SCOPE = new AuthenticationScope("group", "profile");

  @Test
  void configuredCredentialIsBoundToItsVersionAndDelegatesCleanup() throws Exception {
    var closed = new AtomicBoolean();
    var context = context("alice", closed);
    HBaseCredentialAuthenticator backend = (credentials, scope) -> context;
    var authenticator = new VersionedHBaseAuthenticator(backend,
        List.of(new CredentialVersion("alice", 4, "env://ALICE_V4")), null,
        Map.of("ALICE_V4", "alice\nnew-password"));
    try (var credentials = new BackendCredentials("alice", "new-password".toCharArray())) {
      var authenticated = authenticator.authenticate(credentials, SCOPE);
      assertEquals(OptionalLong.of(4), authenticated.credentialGeneration());
      assertEquals("alice", authenticated.user().getName());
      authenticated.close();
      assertTrue(closed.get());
    }
  }

  @Test
  void oldSecretCannotBePromotedToTheNewGeneration() {
    var called = new AtomicBoolean();
    HBaseCredentialAuthenticator backend = (credentials, scope) -> {
      called.set(true);
      return context("alice", new AtomicBoolean());
    };
    var authenticator = new VersionedHBaseAuthenticator(backend,
        List.of(new CredentialVersion("alice", 5, "env://ALICE_V5")), null,
        Map.of("ALICE_V5", "alice\nnew-password"));
    try (var credentials = new BackendCredentials("alice", "old-password".toCharArray())) {
      assertThrows(IllegalArgumentException.class, () -> authenticator.authenticate(credentials, SCOPE));
      assertFalse(called.get());
    }
  }

  @Test
  void unconfiguredOrMissingVersionDoesNotBecomeAClaimedGeneration() throws Exception {
    HBaseCredentialAuthenticator backend = (credentials, scope) -> context("alice", new AtomicBoolean());
    var absent = new VersionedHBaseAuthenticator(backend, List.of(), null, Map.of());
    try (var credentials = new BackendCredentials("alice", "secret".toCharArray())) {
      try (var user = absent.authenticate(credentials, SCOPE)) {
        assertTrue(user.credentialGeneration().isEmpty());
      }
      var unavailable = new VersionedHBaseAuthenticator(backend,
          List.of(new CredentialVersion("alice", 2, "env://MISSING")), null, Map.of());
      assertThrows(IllegalStateException.class, () -> unavailable.authenticate(credentials, SCOPE));
    }
  }

  @Test
  void mismatchedBackendUserIsClosedWithoutAttestingAVersion() {
    var closed = new AtomicBoolean();
    HBaseCredentialAuthenticator backend = (credentials, scope) -> context("other", closed);
    var authenticator = new VersionedHBaseAuthenticator(backend,
        List.of(new CredentialVersion("alice", 3, "env://ALICE")), null,
        Map.of("ALICE", "alice\nsecret"));
    try (var credentials = new BackendCredentials("alice", "secret".toCharArray())) {
      assertThrows(IllegalArgumentException.class, () -> authenticator.authenticate(credentials, SCOPE));
      assertTrue(closed.get());
    }
  }

  private static HBaseUserContext context(String principal, AtomicBoolean closed) {
    User user = mock(User.class);
    when(user.getName()).thenReturn(principal);
    return new HBaseUserContext() {
      @Override public User user() { return user; }
      @Override public void close() { closed.set(true); }
    };
  }
}
