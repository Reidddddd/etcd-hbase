package io.etcdhbase.hbase.identity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.etcdhbase.hbase.api.HBaseCredentialAuthenticator;
import io.etcdhbase.hbase.api.HBaseUserContext;
import io.etcdhbase.server.auth.VersionedHBaseAuthenticator;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.CredentialVersion;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.security.User;
import org.junit.jupiter.api.Test;

class VersionedHBaseProviderTest {
  @Test
  void rotationBindsEachReplicaToItsOwnCredentialAndVersion() throws Exception {
    var scope = new AuthenticationScope("group", "profile");
    User user = mock(User.class);
    when(user.getName()).thenReturn("alice");
    var calls = new AtomicInteger();
    HBaseCredentialAuthenticator backend = (credentials, requested) -> {
      calls.incrementAndGet();
      return new HBaseUserContext() {
        @Override public User user() { return user; }
        @Override public void close() { }
      };
    };
    Configuration configuration = new Configuration(false);
    configuration.set("hbase.security.authentication", "kerberos");
    var oldAuthenticator = new VersionedHBaseAuthenticator(backend,
        List.of(new CredentialVersion("alice", 1, "env://ALICE_V1")), null,
        Map.of("ALICE_V1", "alice\nold-secret"));
    var newAuthenticator = new VersionedHBaseAuthenticator(backend,
        List.of(new CredentialVersion("alice", 2, "env://ALICE_V2")), null,
        Map.of("ALICE_V2", "alice\nnew-secret"));
    try (var oldProvider = provider(scope, configuration, oldAuthenticator);
        var newProvider = provider(scope, configuration, newAuthenticator);
        var oldCredentials = new BackendCredentials("alice", "old-secret".toCharArray());
        var newCredentials = new BackendCredentials("alice", "new-secret".toCharArray())) {
      assertEquals(1, oldProvider.authenticate(oldCredentials, scope).toCompletableFuture()
          .get(5, TimeUnit.SECONDS).credentialGeneration());
      assertEquals(2, newProvider.authenticate(newCredentials, scope).toCompletableFuture()
          .get(5, TimeUnit.SECONDS).credentialGeneration());
      var oldOnNew = assertThrows(ExecutionException.class,
          () -> newProvider.authenticate(oldCredentials, scope).toCompletableFuture()
              .get(5, TimeUnit.SECONDS));
      var newOnOld = assertThrows(ExecutionException.class,
          () -> oldProvider.authenticate(newCredentials, scope).toCompletableFuture()
              .get(5, TimeUnit.SECONDS));
      assertEquals(BackendIdentityException.Reason.INVALID_CREDENTIALS,
          assertInstanceOf(BackendIdentityException.class, oldOnNew.getCause()).reason());
      assertEquals(BackendIdentityException.Reason.INVALID_CREDENTIALS,
          assertInstanceOf(BackendIdentityException.class, newOnOld.getCause()).reason());
      assertEquals(2, calls.get());
    }
  }

  @Test
  void unavailableCredentialReferenceIsNotReportedAsInvalidUserPassword() throws Exception {
    var scope = new AuthenticationScope("group", "profile");
    var backendCalls = new AtomicInteger();
    HBaseCredentialAuthenticator backend = (credentials, requested) -> {
      backendCalls.incrementAndGet();
      throw new AssertionError("backend must not be called without configured material");
    };
    var authenticator = new VersionedHBaseAuthenticator(backend,
        List.of(new CredentialVersion("alice", 2, "env://MISSING")), null, Map.of());
    Configuration configuration = new Configuration(false);
    configuration.set("hbase.security.authentication", "kerberos");
    try (var provider = provider(scope, configuration, authenticator);
        var credentials = new BackendCredentials("alice", "secret".toCharArray())) {
      var failure = assertThrows(ExecutionException.class,
          () -> provider.authenticate(credentials, scope).toCompletableFuture()
              .get(5, TimeUnit.SECONDS));
      assertEquals(BackendIdentityException.Reason.BACKEND_UNAVAILABLE,
          assertInstanceOf(BackendIdentityException.class, failure.getCause()).reason());
      assertEquals(0, backendCalls.get());
    }
  }

  @Test
  void credentialReferenceForTheWrongPrincipalIsAConfigurationFailure() throws Exception {
    var scope = new AuthenticationScope("group", "profile");
    var backendCalls = new AtomicInteger();
    HBaseCredentialAuthenticator backend = (credentials, requested) -> {
      backendCalls.incrementAndGet();
      throw new AssertionError("backend must not receive mismatched configured material");
    };
    var authenticator = new VersionedHBaseAuthenticator(backend,
        List.of(new CredentialVersion("alice", 2, "env://ALICE")), null,
        Map.of("ALICE", "bob\nsecret"));
    Configuration configuration = new Configuration(false);
    configuration.set("hbase.security.authentication", "kerberos");
    try (var provider = provider(scope, configuration, authenticator);
        var credentials = new BackendCredentials("alice", "secret".toCharArray())) {
      var failure = assertThrows(ExecutionException.class,
          () -> provider.authenticate(credentials, scope).toCompletableFuture()
              .get(5, TimeUnit.SECONDS));
      assertEquals(BackendIdentityException.Reason.BACKEND_UNAVAILABLE,
          assertInstanceOf(BackendIdentityException.class, failure.getCause()).reason());
      assertEquals(0, backendCalls.get());
    }
  }

  private static HBaseBackendIdentityProvider provider(AuthenticationScope scope,
      Configuration configuration, HBaseCredentialAuthenticator authenticator) {
    return new HBaseBackendIdentityProvider("provider", scope, configuration, authenticator,
        (connection, requested) -> CompletableFuture.completedFuture(null),
        2, 2, Duration.ofSeconds(5),
        (conf, identity) -> CompletableFuture.completedFuture(mock(AsyncConnection.class)));
  }

  @Test
  void authenticatedVersionComesFromTheExactConfiguredCredential() throws Exception {
    var scope = new AuthenticationScope("group", "profile");
    User user = mock(User.class);
    when(user.getName()).thenReturn("alice");
    HBaseCredentialAuthenticator backend = (credentials, requested) -> new HBaseUserContext() {
      @Override public User user() { return user; }
      @Override public void close() { }
    };
    var versioned = new VersionedHBaseAuthenticator(backend,
        List.of(new CredentialVersion("alice", 2, "env://ALICE_V2")), null,
        Map.of("ALICE_V2", "alice\nnew-secret"));
    Configuration configuration = new Configuration(false);
    configuration.set("hbase.security.authentication", "kerberos");
    try (var provider = new HBaseBackendIdentityProvider("provider", scope, configuration,
        versioned, (connection, requested) -> CompletableFuture.completedFuture(null),
        2, 2, Duration.ofSeconds(5),
        (conf, identity) -> CompletableFuture.completedFuture(mock(AsyncConnection.class)))) {
      try (var credentials = new BackendCredentials("alice", "new-secret".toCharArray())) {
        var identity = provider.authenticate(credentials, scope).toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
        assertEquals(2, identity.credentialGeneration());
      }
      try (var credentials = new BackendCredentials("alice", "old-secret".toCharArray())) {
        var failure = assertThrows(ExecutionException.class, () -> provider.authenticate(credentials, scope)
            .toCompletableFuture().get(5, TimeUnit.SECONDS));
        assertEquals(BackendIdentityException.Reason.INVALID_CREDENTIALS,
            assertInstanceOf(BackendIdentityException.class, failure.getCause()).reason());
      }
    }
  }
}
