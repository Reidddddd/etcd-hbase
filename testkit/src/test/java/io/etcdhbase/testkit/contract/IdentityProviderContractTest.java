package io.etcdhbase.testkit.contract;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.identity.BackendIdentityException.Reason;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

/** A consumer outside spi compiles against JDK-only public types; this does not contact HBase. */
class IdentityProviderContractTest {
  private static final AuthenticationScope SCOPE = new AuthenticationScope("group", "profile");

  @Test
  void extensionUsesDefensiveCredentialsAndScopedImmutableIdentity() {
    char[] supplied = "fixture-secret".toCharArray();
    try (BackendIdentityProvider provider = new ExtensionProvider(null);
        var credentials = new BackendCredentials("fixture-user", supplied)) {
      Arrays.fill(supplied, '\0');
      var result = provider.authenticate(credentials, SCOPE).toCompletableFuture();
      assertTrue(result.isDone(), "bounded CPU-only fixture must not schedule hidden I/O");
      AuthorizedBackendIdentity identity = result.join();
      assertEquals(provider.providerId(), identity.providerId());
      assertEquals(SCOPE, identity.scope());
      assertEquals(Set.of("group"), identity.allowedTableGroupIds());
      assertThrows(
          UnsupportedOperationException.class, () -> identity.allowedTableGroupIds().clear());
      assertFalse(identity.toString().contains("fixture-user"));
      assertFalse(identity.toString().contains("fixture-context"));
      assertFalse(credentials.isClosed(), "provider must not close caller-owned credentials");
      assertTrue(((ExtensionProvider) provider).secretCleared());
    }
  }

  @Test
  void failuresHaveStableReasonsAndNoCredentialText() {
    for (Reason reason : Reason.values()) {
      try (var provider = new ExtensionProvider(reason);
          var credentials =
              new BackendCredentials("fixture-user", "fixture-secret".toCharArray())) {
        var failure =
            assertThrows(
                CompletionException.class,
                () -> provider.authenticate(credentials, SCOPE).toCompletableFuture().join());
        var classified = (BackendIdentityException) failure.getCause();
        assertEquals(reason, classified.reason());
        assertFalse(classified.getMessage().contains("fixture-user"));
        assertFalse(classified.getMessage().contains("fixture-secret"));
        assertTrue(provider.secretCleared());
      }
    }
  }

  @Test
  void closeIsIdempotentAndSubsequentAuthenticationFails() {
    var provider = new ExtensionProvider(null);
    provider.close();
    provider.close();
    try (var credentials = new BackendCredentials("fixture-user", new char[0])) {
      var failure =
          assertThrows(
              CompletionException.class,
              () -> provider.authenticate(credentials, SCOPE).toCompletableFuture().join());
      assertEquals(
          Reason.BACKEND_UNAVAILABLE, ((BackendIdentityException) failure.getCause()).reason());
    }
    assertEquals(1, provider.closes);
  }

  @Test
  void legacyProviderDefaultsDoNotClaimBorrowOrRetirementSucceeded() {
    var provider = new ExtensionProvider(null);
    AuthorizedBackendIdentity identity;
    try (var credentials = new BackendCredentials("fixture-user", "fixture-secret".toCharArray())) {
      identity = provider.authenticate(credentials, SCOPE).toCompletableFuture().join();
    }

    assertThrows(UnsupportedOperationException.class, () -> provider.borrow(identity));
    var retirement = assertThrows(
        CompletionException.class, () -> provider.retire(identity).toCompletableFuture().join());
    assertTrue(retirement.getCause() instanceof UnsupportedOperationException);
    provider.close();
  }

  @Test
  void wrongScopeCannotBecomeAnAuthorizedIdentity() {
    try (var provider = new ExtensionProvider(null);
        var credentials = new BackendCredentials("fixture-user", "fixture-secret".toCharArray())) {
      var wrongScope = new AuthenticationScope("other", "profile");
      var failure =
          assertThrows(
              CompletionException.class,
              () -> provider.authenticate(credentials, wrongScope).toCompletableFuture().join());
      assertEquals(
          Reason.INSUFFICIENT_PERMISSION, ((BackendIdentityException) failure.getCause()).reason());
    }
  }

  @Test
  void invalidAndClosedCredentialsStillCompleteWithTheStableFailureType() {
    try (var provider = new ExtensionProvider(null);
        var closed = new BackendCredentials("fixture-user", new char[0]);
        var oversized = new BackendCredentials("fixture-user", new char[65]);
        var wrong = new BackendCredentials("wrong-user", "fixture-secret".toCharArray())) {
      closed.close();
      for (var credentials : java.util.List.of(closed, oversized, wrong)) {
        var failure =
            assertThrows(
                CompletionException.class,
                () -> provider.authenticate(credentials, SCOPE).toCompletableFuture().join());
        assertEquals(
            Reason.INVALID_CREDENTIALS, ((BackendIdentityException) failure.getCause()).reason());
      }
    }
  }

  /**
   * A synchronous, bounded, CPU-only provider fixture. Real HBase authentication and its async
   * ownership checks remain in hbase; this fixture proves use of the public contract only.
   */
  private static final class ExtensionProvider implements BackendIdentityProvider {
    private final Reason forcedFailure;
    private boolean closed;
    private int closes;
    private char[] lastSecret = new char[0];

    private ExtensionProvider(Reason forcedFailure) {
      this.forcedFailure = forcedFailure;
    }

    public String providerId() {
      return "extension-fixture";
    }

    public synchronized CompletionStage<AuthorizedBackendIdentity> authenticate(
        BackendCredentials credentials, AuthenticationScope scope) {
      if (closed) return failed(Reason.BACKEND_UNAVAILABLE);
      if (!SCOPE.equals(scope)) return failed(Reason.INSUFFICIENT_PERMISSION);
      if (!credentials.principal().equals("fixture-user"))
        return failed(Reason.INVALID_CREDENTIALS);
      try {
        lastSecret = credentials.copySecret(64);
      } catch (IllegalArgumentException | IllegalStateException rejected) {
        return failed(Reason.INVALID_CREDENTIALS);
      }
      try {
        if (forcedFailure != null) return failed(forcedFailure);
        if (!Arrays.equals(lastSecret, "fixture-secret".toCharArray())) {
          return failed(Reason.INVALID_CREDENTIALS);
        }
        return CompletableFuture.completedFuture(
            new AuthorizedBackendIdentity(
                providerId(),
                credentials.principal(),
                "fixture-context",
                scope,
                Set.of("group"),
                0));
      } finally {
        Arrays.fill(lastSecret, '\0');
      }
    }

    private static CompletionStage<AuthorizedBackendIdentity> failed(Reason reason) {
      return CompletableFuture.failedFuture(new BackendIdentityException(reason));
    }

    private synchronized boolean secretCleared() {
      char[] zeros = new char[lastSecret.length];
      return Arrays.equals(zeros, lastSecret);
    }

    public synchronized void close() {
      if (closed) return;
      closed = true;
      closes++;
      Arrays.fill(lastSecret, '\0');
    }
  }
}
