package io.etcdhbase.hbase.identity;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.etcdhbase.hbase.api.HBaseAccessVerifier;
import io.etcdhbase.hbase.api.HBaseCredentialAuthenticator;
import io.etcdhbase.hbase.api.HBaseUserContext;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.identity.BackendIdentityException.Reason;
import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.OptionalLong;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.sasl.SaslException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

class HBaseBackendIdentityProviderTest {
  private static final AuthenticationScope SCOPE = new AuthenticationScope("group", "backend");
  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  @Test
  void reportsOnlyTheGenerationAttestedByTheAuthenticatedContext() throws Exception {
    var connection = mock(AsyncConnection.class);
    var context = context("alice");
    when(context.credentialGeneration()).thenReturn(OptionalLong.of(12));
    try (var provider = provider((credentials, scope) -> context,
        (backend, scope) -> CompletableFuture.completedFuture(null),
        (configuration, user) -> CompletableFuture.completedFuture(connection), TIMEOUT)) {
      var identity = authenticate(provider).get(5, TimeUnit.SECONDS);
      assertTrue(identity.hasKnownCredentialGeneration());
      assertEquals(12, identity.credentialGeneration());
    }
  }

  @Test
  void missingGenerationIsExplicitlyUnknown() throws Exception {
    var context = context("alice");
    when(context.credentialGeneration()).thenReturn(OptionalLong.empty());
    try (var provider = provider((credentials, scope) -> context,
        (backend, scope) -> CompletableFuture.completedFuture(null),
        (configuration, user) -> CompletableFuture.completedFuture(mock(AsyncConnection.class)), TIMEOUT)) {
      var identity = authenticate(provider).get(5, TimeUnit.SECONDS);
      assertFalse(identity.hasKnownCredentialGeneration());
      assertTrue(identity.toString().contains("unknown"));
    }
  }

  @Test
  void accessRecheckUsesTheRealConnectionAndRetiresOnPermissionLoss() throws Exception {
    var context = context("alice");
    var connection = mock(AsyncConnection.class);
    var calls = new AtomicInteger();
    try (var provider = provider(
        (credentials, scope) -> context,
        (backend, scope) -> {
          assertSame(connection, backend);
          return calls.getAndIncrement() == 0 ? CompletableFuture.completedFuture(null)
              : CompletableFuture.failedFuture(new BackendIdentityException(Reason.INSUFFICIENT_PERMISSION));
        },
        (configuration, user) -> CompletableFuture.completedFuture(connection), TIMEOUT)) {
      AuthorizedBackendIdentity identity = authenticate(provider).get(5, TimeUnit.SECONDS);
      var existing = provider.borrow(identity);
      assertReason(Reason.INSUFFICIENT_PERMISSION,
          provider.verifyCurrentAccess(identity).toCompletableFuture());
      assertThrows(BackendIdentityException.class, () -> provider.borrow(identity));
      verify(connection, never()).close();
      existing.release().toCompletableFuture().get(5, TimeUnit.SECONDS);
      provider.retire(identity).toCompletableFuture().get(5, TimeUnit.SECONDS);
      verify(connection).close();
      verify(context).close();
    }
  }

  @Test
  void timedOutAccessRecheckKeepsConnectionUntilActualVerifierWorkDrains() throws Exception {
    var context = context("alice");
    var connection = mock(AsyncConnection.class);
    var pending = new CompletableFuture<Void>();
    var calls = new AtomicInteger();
    try (var provider = provider(
        (credentials, scope) -> context,
        (backend, scope) -> calls.getAndIncrement() == 0
            ? CompletableFuture.completedFuture(null) : pending,
        (configuration, user) -> CompletableFuture.completedFuture(connection), Duration.ofSeconds(1))) {
      AuthorizedBackendIdentity identity = authenticate(provider).get(5, TimeUnit.SECONDS);
      assertReason(Reason.TIMEOUT, provider.verifyCurrentAccess(identity).toCompletableFuture());
      assertThrows(BackendIdentityException.class, () -> provider.borrow(identity));
      verify(connection, never()).close();
      var retirement = provider.retire(identity).toCompletableFuture();
      assertFalse(retirement.isDone());
      pending.complete(null);
      retirement.get(5, TimeUnit.SECONDS);
      verify(connection).close();
      verify(context).close();
    } finally {
      pending.complete(null);
    }
  }

  @Test
  void accessRecheckKeepsCapacityWhileItsResultCallbackIsBlocked() throws Exception {
    var pending = new CompletableFuture<Void>();
    var callbackEntered = new CountDownLatch(1);
    var releaseCallback = new CountDownLatch(1);
    var calls = new AtomicInteger();
    try (var provider = provider(
        (credentials, scope) -> context("alice"),
        (backend, scope) -> calls.getAndIncrement() == 0
            ? CompletableFuture.completedFuture(null) : pending,
        (configuration, user) -> CompletableFuture.completedFuture(mock(AsyncConnection.class)),
        TIMEOUT, 1)) {
      AuthorizedBackendIdentity identity = authenticate(provider).get(5, TimeUnit.SECONDS);
      var first = provider.verifyCurrentAccess(identity).toCompletableFuture();
      var callback = first.thenRun(() -> blockCallback(callbackEntered, releaseCallback));
      pending.complete(null);
      assertTrue(callbackEntered.await(5, TimeUnit.SECONDS));
      assertReason(Reason.RESOURCE_LIMIT, provider.verifyCurrentAccess(identity).toCompletableFuture());
      var notificationDrain = provider.accessChecksDrained().toCompletableFuture();
      assertFalse(notificationDrain.isDone());
      provider.retire(identity).toCompletableFuture().get(5, TimeUnit.SECONDS);
      releaseCallback.countDown();
      callback.get(5, TimeUnit.SECONDS);
      notificationDrain.get(5, TimeUnit.SECONDS);
    } finally {
      pending.complete(null);
      releaseCallback.countDown();
    }
  }

  @Test
  void verifiesBackendBeforePublicationAndReusesOnlyTheIssuedIdentity() throws Exception {
    var context = context("alice");
    var connection = mock(AsyncConnection.class);
    var verification = new CompletableFuture<Void>();
    var verified = new CountDownLatch(1);
    var copiedCredentials = new AtomicReference<BackendCredentials>();
    HBaseCredentialAuthenticator authenticator =
        (credentials, scope) -> {
          copiedCredentials.set(credentials);
          assertArrayEquals("password".toCharArray(), credentials.copySecret());
          return context;
        };
    try (var provider =
        provider(
            authenticator,
            (opened, scope) -> {
              assertSame(connection, opened);
              verified.countDown();
              return verification;
            },
            (configuration, user) -> {
              assertFalse(
                  configuration.getBoolean(HBaseCredentialAuthenticator.SIMPLE_FALLBACK, true));
              assertEquals("alice", user.getName());
              return CompletableFuture.completedFuture(connection);
            },
            TIMEOUT)) {
      CompletableFuture<AuthorizedBackendIdentity> result;
      try (var credentials = new BackendCredentials("alice", "password".toCharArray())) {
        result = provider.authenticate(credentials, SCOPE).toCompletableFuture();
      }
      assertTrue(verified.await(5, TimeUnit.SECONDS));
      assertFalse(result.isDone(), "creating a User/connection must not publish an identity");
      assertTrue(copiedCredentials.get().isClosed());
      verification.complete(null);
      AuthorizedBackendIdentity identity = result.get(5, TimeUnit.SECONDS);
      assertEquals("backend-auth", identity.providerId());
      assertEquals("alice", identity.subject());
      assertEquals(SCOPE, identity.scope());
      assertEquals(Set.of("group"), identity.allowedTableGroupIds());
      assertSame(connection, provider.connectionFor(identity).toCompletableFuture().get());
      assertSame(connection, provider.connectionFor(identity).toCompletableFuture().get());
      var forged =
          new AuthorizedBackendIdentity(
              identity.providerId(),
              identity.subject(),
              identity.contextRef(),
              identity.scope(),
              Set.of("group"),
              identity.credentialGeneration());
      assertReason(
          Reason.INSUFFICIENT_PERMISSION, provider.connectionFor(forged).toCompletableFuture());
      assertReason(Reason.RESOURCE_LIMIT, authenticate(provider));
    }
    verify(connection).close();
    verify(context).close();
  }

  @Test
  void discardingUnpublishedIdentityReleasesCapacityAndRejectsFurtherBorrowing() throws Exception {
    var verification = new CompletableFuture<Void>();
    var callbackEntered = new CountDownLatch(1);
    var releaseCallback = new CountDownLatch(1);
    var issuedContext = new AtomicReference<HBaseUserContext>();
    var openedConnection = new AtomicReference<AsyncConnection>();
    try (var provider = provider(
        (credentials, scope) -> {
          HBaseUserContext context = context(credentials.principal());
          issuedContext.set(context);
          return context;
        },
        (backend, scope) -> verification,
        (configuration, user) -> {
          AsyncConnection connection = mock(AsyncConnection.class);
          openedConnection.set(connection);
          return CompletableFuture.completedFuture(connection);
        },
        TIMEOUT,
        1)) {
      var authentication = authenticate(provider);
      var callback = authentication.thenRun(() -> blockCallback(callbackEntered, releaseCallback));
      verification.complete(null);
      assertTrue(callbackEntered.await(5, TimeUnit.SECONDS));
      AuthorizedBackendIdentity first = authentication.get(5, TimeUnit.SECONDS);
      HBaseUserContext firstContext = issuedContext.get();
      AsyncConnection firstConnection = openedConnection.get();

      provider.discardUnpublished(first).toCompletableFuture().get(5, TimeUnit.SECONDS);
      verify(firstConnection).close();
      verify(firstContext).close();
      assertReason(Reason.INSUFFICIENT_PERMISSION, provider.connectionFor(first).toCompletableFuture());

      // Closing the identity does not release the notification slot while its callback runs.
      assertReason(Reason.RESOURCE_LIMIT, authenticate(provider));
      releaseCallback.countDown();
      callback.get(5, TimeUnit.SECONDS);
      AuthorizedBackendIdentity next = authenticateAfterCleanup(provider);
      assertNotEquals(first.contextRef(), next.contextRef());
    } finally {
      releaseCallback.countDown();
    }
  }

  @Test
  void releasingOnePublishedIdentityDoesNotCloseProviderOrAnotherIdentity() throws Exception {
    var openedConnections = new java.util.concurrent.CopyOnWriteArrayList<AsyncConnection>();
    var openedContexts = new java.util.concurrent.CopyOnWriteArrayList<HBaseUserContext>();
    try (var provider = provider(
        (credentials, scope) -> {
          HBaseUserContext context = context(credentials.principal());
          openedContexts.add(context);
          return context;
        },
        (backend, scope) -> CompletableFuture.completedFuture(null),
        (configuration, user) -> {
          AsyncConnection connection = mock(AsyncConnection.class);
          openedConnections.add(connection);
          return CompletableFuture.completedFuture(connection);
        },
        TIMEOUT,
        2)) {
      AuthorizedBackendIdentity first = authenticate(provider, "alice").get(5, TimeUnit.SECONDS);
      AuthorizedBackendIdentity second = authenticate(provider, "bob").get(5, TimeUnit.SECONDS);
      AsyncConnection firstConnection = openedConnections.get(0);
      AsyncConnection secondConnection = openedConnections.get(1);
      HBaseUserContext firstContext = openedContexts.get(0);
      HBaseUserContext secondContext = openedContexts.get(1);

      provider.release(first).toCompletableFuture().get(5, TimeUnit.SECONDS);
      provider.release(first).toCompletableFuture().get(5, TimeUnit.SECONDS);

      verify(firstConnection, times(1)).close();
      verify(firstContext, times(1)).close();
      verify(secondConnection, never()).close();
      verify(secondContext, never()).close();
      assertReason(Reason.INSUFFICIENT_PERMISSION,
          provider.connectionFor(first).toCompletableFuture());
      assertSame(secondConnection,
          provider.connectionFor(second).toCompletableFuture().get(5, TimeUnit.SECONDS));

      AuthorizedBackendIdentity third = authenticateAfterCleanup(provider, "carol");
      assertNotEquals(first.contextRef(), third.contextRef());
    }
  }

  @Test
  void retirementStopsNewBorrowsAndWaitsForExistingBorrowToDrain() throws Exception {
    var context = context("alice");
    var connection = mock(AsyncConnection.class);
    try (var provider = provider(
        (credentials, scope) -> context,
        (backend, scope) -> CompletableFuture.completedFuture(null),
        (configuration, user) -> CompletableFuture.completedFuture(connection),
        TIMEOUT)) {
      AuthorizedBackendIdentity identity = authenticate(provider).get(5, TimeUnit.SECONDS);
      var borrow = provider.borrow(identity);
      var retirement = provider.retire(identity).toCompletableFuture();

      assertFalse(retirement.isDone(), "retirement waits until request work releases its borrow");
      assertThrows(BackendIdentityException.class, () -> provider.borrow(identity));
      assertReason(Reason.INSUFFICIENT_PERMISSION, provider.connectionFor(identity).toCompletableFuture());

      borrow.release().toCompletableFuture().get(5, TimeUnit.SECONDS);
      borrow.release().toCompletableFuture().get(5, TimeUnit.SECONDS);
      retirement.get(5, TimeUnit.SECONDS);

      var order = inOrder(connection, context);
      order.verify(connection).close();
      order.verify(context).close();
    }
  }

  @Test
  void identityCleanupFailureKeepsCapacityAndCanBeRetried() throws Exception {
    var context = context("alice");
    var connection = mock(AsyncConnection.class);
    doThrow(new IOException("fixture-private-close-diagnostic")).doNothing()
        .when(connection).close();
    try (var provider = provider(
        (credentials, scope) -> context,
        (backend, scope) -> CompletableFuture.completedFuture(null),
        (configuration, user) -> CompletableFuture.completedFuture(connection),
        TIMEOUT,
        1)) {
      AuthorizedBackendIdentity identity = authenticate(provider).get(5, TimeUnit.SECONDS);

      var failure = assertThrows(ExecutionException.class,
          () -> provider.retire(identity).toCompletableFuture().get(5, TimeUnit.SECONDS));
      assertFalse(failure.getCause().getMessage().contains("fixture-private"));
      verify(context, never()).close();
      assertReason(Reason.RESOURCE_LIMIT, authenticate(provider));

      provider.retire(identity).toCompletableFuture().get(5, TimeUnit.SECONDS);
      verify(connection, times(2)).close();
      verify(context).close();
      // Resource retirement can complete before the authentication notification releases capacity.
      awaitFullyDrained(provider);
      authenticate(provider).get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void closeRetainsBorrowedIdentityUntilReleaseAndCleanupRetry() throws Exception {
    var context = context("alice");
    var connection = mock(AsyncConnection.class);
    doThrow(new IOException("fixture-private-close-diagnostic")).doNothing()
        .when(connection).close();
    try (var provider = provider(
        (credentials, scope) -> context,
        (backend, scope) -> CompletableFuture.completedFuture(null),
        (configuration, user) -> CompletableFuture.completedFuture(connection),
        TIMEOUT)) {
      AuthorizedBackendIdentity identity = authenticate(provider).get(5, TimeUnit.SECONDS);
      var borrow = provider.borrow(identity);

      provider.close();
      assertFalse(provider.isFullyDrained());
      verify(connection, never()).close();

      borrow.release().toCompletableFuture().get(5, TimeUnit.SECONDS);
      verify(connection).close();
      verify(context, never()).close();

      provider.close();
      verify(connection, times(2)).close();
      verify(context).close();
      assertTrue(provider.isFullyDrained());
    }
  }

  @Test
  void closedProviderRetriesCleanupForLateConnection() throws Exception {
    var context = context("alice");
    var rawConnection = new CompletableFuture<AsyncConnection>();
    var opened = new CountDownLatch(1);
    var retryStarted = new CountDownLatch(1);
    var allowRetryClose = new CountDownLatch(1);
    var retryClosed = new CountDownLatch(1);
    var closeAttempts = new AtomicInteger();
    var connection = mock(AsyncConnection.class);
    doAnswer(invocation -> {
      if (closeAttempts.incrementAndGet() == 1) {
        throw new IOException("fixture close failure");
      }
      retryStarted.countDown();
      if (!allowRetryClose.await(5, TimeUnit.SECONDS)) {
        throw new IOException("fixture retry was not released");
      }
      retryClosed.countDown();
      return null;
    }).when(connection).close();
    try (var provider = provider(
        (credentials, scope) -> context,
        (backend, scope) -> CompletableFuture.completedFuture(null),
        (configuration, user) -> {
          opened.countDown();
          return rawConnection;
        },
        TIMEOUT)) {
      var result = authenticate(provider);
      assertTrue(opened.await(5, TimeUnit.SECONDS));

      provider.close();
      assertFalse(provider.isFullyDrained());
      assertFalse(rawConnection.isCancelled());

      rawConnection.complete(connection);
      assertReason(Reason.CANCELLED, result);
      try {
        assertTrue(retryStarted.await(5, TimeUnit.SECONDS));
        assertEquals(2, closeAttempts.get());
        assertFalse(provider.isFullyDrained(), "failed cleanup must retain provider ownership");
        verify(context, never()).close();
      } finally {
        allowRetryClose.countDown();
      }
      assertTrue(retryClosed.await(5, TimeUnit.SECONDS));
      verify(connection, times(2)).close();
      verify(context).close();
      awaitFullyDrained(provider);
    }
  }

  /** Resource cleanup and notification completion must both drain before capacity is reusable. */
  private static void awaitFullyDrained(HBaseBackendIdentityProvider provider) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!provider.isFullyDrained()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("provider did not drain every accepted attempt");
      }
      Thread.sleep(5);
    }
  }

  @Test
  void rejectsWrongScopeWithoutOpeningCredentialsOrConnections() throws Exception {
    var authenticator = mock(HBaseCredentialAuthenticator.class);
    try (var provider = provider(authenticator, (c, s) -> null, (c, u) -> null, TIMEOUT);
        var credentials = new BackendCredentials("alice", new char[0])) {
      assertReason(
          Reason.INSUFFICIENT_PERMISSION,
          provider
              .authenticate(credentials, new AuthenticationScope("other", "backend"))
              .toCompletableFuture());
      verify(authenticator, never()).authenticate(any(), any());
    }
  }

  @Test
  void authenticationFailureDoesNotFallBackToAnotherMechanism() throws Exception {
    var opens = new java.util.concurrent.atomic.AtomicInteger();
    try (var provider =
        provider(
            (credentials, scope) -> {
              throw new SaslException("fixture rejected password");
            },
            (c, s) -> CompletableFuture.completedFuture(null),
            (c, u) -> {
              opens.incrementAndGet();
              return null;
            },
            TIMEOUT)) {
      assertReason(Reason.INVALID_CREDENTIALS, authenticate(provider));
      assertEquals(0, opens.get());
    }
  }

  @Test
  void permissionDenialClosesConnectionBeforeCredentials() throws Exception {
    var context = context("alice");
    var connection = mock(AsyncConnection.class);
    var closed = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              closed.countDown();
              return null;
            })
        .when(context)
        .close();
    try (var provider =
        provider(
            (credentials, scope) -> context,
            (c, s) ->
                CompletableFuture.failedFuture(
                    new BackendIdentityException(Reason.INSUFFICIENT_PERMISSION)),
            (c, u) -> CompletableFuture.completedFuture(connection),
            TIMEOUT)) {
      assertReason(Reason.INSUFFICIENT_PERMISSION, authenticate(provider));
      assertTrue(closed.await(5, TimeUnit.SECONDS));
      var order = inOrder(connection, context);
      order.verify(connection).close();
      order.verify(context).close();
    }
  }

  @Test
  void timeoutKeepsCapacityUntilLateConnectionAndCredentialsHaveBeenClosed() throws Exception {
    var context = context("alice");
    var rawConnection = new CompletableFuture<AsyncConnection>();
    var connectionOpened = new CountDownLatch(1);
    var closed = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              closed.countDown();
              return null;
            })
        .when(context)
        .close();
    var connection = mock(AsyncConnection.class);
    try (var provider =
        provider(
            (credentials, scope) -> context,
            (c, s) -> CompletableFuture.completedFuture(null),
            (c, u) -> {
              connectionOpened.countDown();
              return rawConnection;
            },
            Duration.ofSeconds(1))) {
      var result = authenticate(provider);
      assertTrue(connectionOpened.await(5, TimeUnit.SECONDS));
      assertReason(Reason.TIMEOUT, result);
      assertFalse(rawConnection.isCancelled());
      assertReason(Reason.RESOURCE_LIMIT, authenticate(provider));
      rawConnection.complete(connection);
      assertTrue(closed.await(5, TimeUnit.SECONDS));
      verify(connection).close();
    }
  }

  @Test
  void cancellationAndShutdownCleanLateConnectionsWithoutCancellingTheResourceFuture()
      throws Exception {
    var context = context("alice");
    var rawConnection = new CompletableFuture<AsyncConnection>();
    var opened = new CountDownLatch(1);
    var closed = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              closed.countDown();
              return null;
            })
        .when(context)
        .close();
    var connection = mock(AsyncConnection.class);
    var provider =
        provider(
            (credentials, scope) -> context,
            (c, s) -> CompletableFuture.completedFuture(null),
            (c, u) -> {
              opened.countDown();
              return rawConnection;
            },
            TIMEOUT);
    try {
      var result = authenticate(provider);
      assertTrue(opened.await(5, TimeUnit.SECONDS));
      assertTrue(result.cancel(true));
      assertReason(Reason.RESOURCE_LIMIT, authenticate(provider));
      provider.close();
      assertFalse(rawConnection.isCancelled());
      rawConnection.complete(connection);
      assertTrue(closed.await(5, TimeUnit.SECONDS));
      assertReason(Reason.BACKEND_UNAVAILABLE, authenticate(provider));
      verify(connection).close();
    } finally {
      provider.close();
    }
  }

  @Test
  void closeFailureRetainsCredentialsAndIsRetriedAtShutdown() throws Exception {
    var context = context("alice");
    var connection = mock(AsyncConnection.class);
    doThrow(new IOException("fixture close failure")).doNothing().when(connection).close();
    var provider =
        provider(
            (credentials, scope) -> context,
            (c, s) -> CompletableFuture.completedFuture(null),
            (c, u) -> CompletableFuture.completedFuture(connection),
            TIMEOUT);
    authenticate(provider).get(5, TimeUnit.SECONDS);
    assertThrows(IllegalStateException.class, provider::close);
    verify(context, never()).close();
    provider.close();
    provider.close();
    verify(connection, times(2)).close();
    verify(context).close();
  }

  @Test
  void trustedActivationUsesTheSameBackendVerificationWithoutPretendingToVerifyClientCredentials()
      throws Exception {
    var authenticator = mock(HBaseCredentialAuthenticator.class);
    var context = context("operator");
    var connection = mock(AsyncConnection.class);
    var verified = new java.util.concurrent.atomic.AtomicBoolean();
    try (var provider =
        provider(
            authenticator,
            (c, s) -> {
              verified.set(true);
              return CompletableFuture.completedFuture(null);
            },
            (c, u) -> CompletableFuture.completedFuture(connection),
            TIMEOUT)) {
      var identity =
          provider.activateTrusted(() -> context).toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertTrue(verified.get());
      assertEquals("operator", identity.subject());
      assertSame(connection, provider.connectionFor(identity).toCompletableFuture().get());
      verify(authenticator, never()).authenticate(any(), any());
    }
    verify(connection).close();
    verify(context).close();
  }

  @Test
  void cancellationDuringBlockingLoginRetainsCapacityAndClosesTheLateUser() throws Exception {
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var interrupted = new CountDownLatch(1);
    var closed = new CountDownLatch(1);
    var context = context("alice");
    doAnswer(
            invocation -> {
              closed.countDown();
              return null;
            })
        .when(context)
        .close();
    var opens = new java.util.concurrent.atomic.AtomicInteger();
    var provider =
        provider(
            (credentials, scope) -> {
              entered.countDown();
              boolean done = false;
              while (!done) {
                try {
                  done = release.await(5, TimeUnit.SECONDS);
                  if (!done) throw new IOException("fixture login was not released");
                } catch (InterruptedException expected) {
                  interrupted.countDown();
                }
              }
              return context;
            },
            (c, s) -> CompletableFuture.completedFuture(null),
            (c, u) -> {
              opens.incrementAndGet();
              return null;
            },
            TIMEOUT);
    try {
      var result = authenticate(provider);
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      assertTrue(result.cancel(true));
      assertTrue(interrupted.await(5, TimeUnit.SECONDS));
      assertReason(Reason.RESOURCE_LIMIT, authenticate(provider));
      provider.close();
      release.countDown();
      assertTrue(closed.await(5, TimeUnit.SECONDS));
      assertEquals(0, opens.get());
    } finally {
      release.countDown();
      provider.close();
    }
  }

  @Test
  void shutdownDuringPermissionRpcWaitsForCompletionBeforeDestroyingUserCredentials()
      throws Exception {
    var context = context("alice");
    var connection = mock(AsyncConnection.class);
    var verification = new CompletableFuture<Void>();
    var entered = new CountDownLatch(1);
    var cleaned = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              cleaned.countDown();
              return null;
            })
        .when(context)
        .close();
    var provider =
        provider(
            (credentials, scope) -> context,
            (c, s) -> {
              entered.countDown();
              return verification;
            },
            (c, u) -> CompletableFuture.completedFuture(connection),
            TIMEOUT);
    try {
      var result = authenticate(provider);
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      provider.close();
      assertReason(Reason.CANCELLED, result);
      verify(connection, never()).close();
      verify(context, never()).close();
      assertFalse(verification.isCancelled());
      verification.complete(null);
      assertTrue(cleaned.await(5, TimeUnit.SECONDS));
      verify(connection).close();
    } finally {
      verification.complete(null);
      provider.close();
    }
  }

  @Test
  void malformedCustomUserContextCannotLeaveAuthenticationUncompleted() throws Exception {
    var context = context("a".repeat(257));
    var connection = mock(AsyncConnection.class);
    var cleaned = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              cleaned.countDown();
              return null;
            })
        .when(context)
        .close();
    try (var provider =
        provider(
            (credentials, scope) -> context,
            (c, s) -> CompletableFuture.completedFuture(null),
            (c, u) -> CompletableFuture.completedFuture(connection),
            TIMEOUT)) {
      assertReason(Reason.INVALID_CREDENTIALS, authenticate(provider));
      assertTrue(cleaned.await(5, TimeUnit.SECONDS));
      verify(connection).close();
    }
  }

  @Test
  void lateCloseFailureRetainsCapacityAndCredentialsUntilShutdownRetries() throws Exception {
    var context = context("alice");
    var rawConnection = new CompletableFuture<AsyncConnection>();
    var opened = new CountDownLatch(1);
    var failedClose = new CountDownLatch(1);
    var credentialsClosed = new CompletableFuture<Void>();
    var closeAttempts = new AtomicInteger();
    var connection = mock(AsyncConnection.class);
    doAnswer(
            invocation -> {
              if (closeAttempts.incrementAndGet() == 1) {
                failedClose.countDown();
                throw new IOException("fixture-private-close-diagnostic");
              }
              return null;
            })
        .when(connection)
        .close();
    doAnswer(
            invocation -> {
              credentialsClosed.complete(null);
              return null;
            })
        .when(context)
        .close();
    var provider =
        provider(
            (credentials, scope) -> context,
            (backend, scope) -> CompletableFuture.completedFuture(null),
            (configuration, user) -> {
              opened.countDown();
              return rawConnection;
            },
            Duration.ofSeconds(1));
    try {
      var result = authenticate(provider);
      assertTrue(opened.await(5, TimeUnit.SECONDS));
      assertReason(Reason.TIMEOUT, result);
      rawConnection.complete(connection);
      assertTrue(failedClose.await(5, TimeUnit.SECONDS));
      assertReason(Reason.RESOURCE_LIMIT, authenticate(provider));
      verify(context, never()).close();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      do {
        // A close concurrent with an existing cleanup may return while that cleanup still runs.
        provider.close();
        Thread.yield();
      } while (!credentialsClosed.isDone() && System.nanoTime() < deadline);
      credentialsClosed.get(1, TimeUnit.SECONDS);
      assertEquals(2, closeAttempts.get());
      var order = inOrder(connection, context);
      order.verify(connection, times(2)).close();
      order.verify(context).close();
    } finally {
      provider.close();
    }
  }

  @Test
  void oneFailedCloseDoesNotPreventOtherIdentitiesFromBeingReleased() throws Exception {
    var alice = context("alice");
    var bob = context("bob");
    var failingConnection = mock(AsyncConnection.class);
    var healthyConnection = mock(AsyncConnection.class);
    var bobVerification = new CompletableFuture<Void>();
    doThrow(new IOException("fixture-private-close-diagnostic"))
        .doNothing()
        .when(failingConnection)
        .close();
    var provider =
        provider(
            (credentials, scope) -> credentials.principal().equals("alice") ? alice : bob,
            (backend, scope) ->
                backend == healthyConnection
                    ? bobVerification
                    : CompletableFuture.completedFuture(null),
            (configuration, user) ->
                CompletableFuture.completedFuture(
                    user.getName().equals("alice") ? failingConnection : healthyConnection),
            TIMEOUT,
            2);
    try {
      authenticate(provider, "alice").get(5, TimeUnit.SECONDS);
      var bobAuthentication = authenticate(provider, "bob");
      // Close from Bob's result callback after Alice has been verified.
      var closed =
          bobAuthentication.thenApply(
              ignored -> assertThrows(IllegalStateException.class, provider::close));
      bobVerification.complete(null);
      var failure = closed.get(5, TimeUnit.SECONDS);
      assertFalse(failure.getMessage().contains("fixture-private"));
      verify(healthyConnection).close();
      verify(bob).close();
      verify(alice, never()).close();
    } finally {
      bobVerification.complete(null);
      provider.close();
    }
    provider.close();
    verify(failingConnection, times(2)).close();
    verify(healthyConnection).close();
    verify(alice).close();
    verify(bob).close();
  }

  @Test
  void failedOpenersAreClassifiedWithoutDiagnosticsAndRetireBeforeCapacityIsReused()
      throws Exception {
    for (Exception failure :
        new Exception[] {
          new IOException("fixture-private-diagnostic"),
          new TimeoutException("fixture-private-diagnostic"),
          new CancellationException("fixture-private-diagnostic"),
          new IllegalStateException("fixture-private-diagnostic")
        }) {
      Reason expected =
          failure instanceof TimeoutException
              ? Reason.TIMEOUT
              : failure instanceof CancellationException
                  ? Reason.CANCELLED
                  : Reason.BACKEND_UNAVAILABLE;
      var opened = new AtomicInteger();
      var retryConnection = mock(AsyncConnection.class);
      try (var provider =
          provider(
              (credentials, scope) -> context(credentials.principal()),
              (backend, scope) -> CompletableFuture.completedFuture(null),
              (configuration, user) -> {
                if (opened.getAndIncrement() == 0) {
                  if (failure instanceof IllegalStateException)
                    throw (IllegalStateException) failure;
                  return CompletableFuture.failedFuture(failure);
                }
                return CompletableFuture.completedFuture(retryConnection);
              },
              TIMEOUT)) {
        var rejected =
            assertThrows(
                ExecutionException.class, () -> authenticate(provider).get(5, TimeUnit.SECONDS));
        var reason = assertInstanceOf(BackendIdentityException.class, rejected.getCause());
        assertEquals(expected, reason.reason());
        assertFalse(reason.getMessage().contains("fixture-private"));
        var retry = authenticateAfterCleanup(provider);
        assertSame(retryConnection, provider.connectionFor(retry).toCompletableFuture().get());
        assertEquals(2, opened.get());
      }
      verify(retryConnection).close();
    }
  }

  @Test
  void sameNamedIdentitiesAndOtherProviderInstancesCannotBorrowEachOthersConnections()
      throws Exception {
    var opened = new AtomicInteger();
    HBaseBackendIdentityProvider.ConnectionOpener opener =
        (configuration, user) -> {
          opened.incrementAndGet();
          return CompletableFuture.completedFuture(mock(AsyncConnection.class));
        };
    try (var first =
            provider(
                (credentials, scope) -> context("alice"),
                (backend, scope) -> CompletableFuture.completedFuture(null),
                opener,
                TIMEOUT,
                2);
        var other =
            provider(
                (credentials, scope) -> context("alice"),
                (backend, scope) -> CompletableFuture.completedFuture(null),
                opener,
                TIMEOUT)) {
      var original = authenticate(first).get(5, TimeUnit.SECONDS);
      var secondSession = authenticate(first).get(5, TimeUnit.SECONDS);
      var firstConnection = first.connectionFor(original).toCompletableFuture().get();
      var secondConnection = first.connectionFor(secondSession).toCompletableFuture().get();
      assertNotEquals(original.contextRef(), secondSession.contextRef());
      assertNotSame(firstConnection, secondConnection);
      assertReason(
          Reason.INSUFFICIENT_PERMISSION, other.connectionFor(original).toCompletableFuture());
      var borrower = first.connectionFor(original).toCompletableFuture();
      borrower.obtrudeException(new IllegalStateException("caller-owned future only"));
      assertSame(firstConnection, first.connectionFor(original).toCompletableFuture().get());
      assertEquals(2, opened.get());
    }
  }

  @Test
  void blockingResultCallbackDoesNotHoldTheProviderOrIdentityLock() throws Exception {
    var verification = new CompletableFuture<Void>();
    var callbackEntered = new CountDownLatch(1);
    var releaseCallback = new CountDownLatch(1);
    var connection = mock(AsyncConnection.class);
    var executor = Executors.newSingleThreadExecutor();
    try (var provider =
        provider(
            (credentials, scope) -> context("alice"),
            (backend, scope) -> verification,
            (configuration, user) -> CompletableFuture.completedFuture(connection),
            TIMEOUT)) {
      var authentication = authenticate(provider);
      var callback =
          authentication.thenRun(
              () -> {
                callbackEntered.countDown();
                try {
                  if (!releaseCallback.await(5, TimeUnit.SECONDS))
                    throw new IllegalStateException("fixture callback was not released");
                } catch (InterruptedException interrupted) {
                  Thread.currentThread().interrupt();
                  throw new java.util.concurrent.CompletionException(interrupted);
                }
              });
      verification.complete(null);
      assertTrue(callbackEntered.await(5, TimeUnit.SECONDS));
      var identity = authentication.get(5, TimeUnit.SECONDS);
      assertSame(
          connection,
          executor
              .submit(() -> provider.connectionFor(identity).toCompletableFuture().get())
              .get(5, TimeUnit.SECONDS));
      assertReason(
          Reason.RESOURCE_LIMIT,
          executor.submit(() -> authenticate(provider)).get(5, TimeUnit.SECONDS));
      releaseCallback.countDown();
      callback.get(5, TimeUnit.SECONDS);
    } finally {
      releaseCallback.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void blockingTimeoutCallbackDoesNotDelayAnotherAuthenticationDeadline() throws Exception {
    var verification = new CompletableFuture<Void>();
    var verifying = new CountDownLatch(2);
    var callbackEntered = new CountDownLatch(1);
    var releaseCallback = new CountDownLatch(1);
    var provider =
        provider(
            (credentials, scope) -> context(credentials.principal()),
            (backend, scope) -> {
              verifying.countDown();
              return verification;
            },
            (configuration, user) ->
                CompletableFuture.completedFuture(mock(AsyncConnection.class)),
            Duration.ofSeconds(1),
            2);
    try {
      var first = authenticate(provider, "alice");
      first.whenComplete((identity, failure) -> blockCallback(callbackEntered, releaseCallback));
      var second = authenticate(provider, "bob");
      assertTrue(verifying.await(5, TimeUnit.SECONDS));
      assertTrue(callbackEntered.await(5, TimeUnit.SECONDS));
      var failure = assertThrows(ExecutionException.class, () -> second.get(2, TimeUnit.SECONDS));
      assertEquals(
          Reason.TIMEOUT,
          assertInstanceOf(BackendIdentityException.class, failure.getCause()).reason());
    } finally {
      releaseCallback.countDown();
      verification.complete(null);
      provider.close();
    }
  }

  @Test
  void blockingFailureCallbackDoesNotDelayCleanupOrReleaseItsNotificationCapacity()
      throws Exception {
    var verification = new CompletableFuture<Void>();
    var callbackEntered = new CountDownLatch(1);
    var releaseCallback = new CountDownLatch(1);
    var cleaned = new CountDownLatch(1);
    var userContext = context("alice");
    doAnswer(
            invocation -> {
              cleaned.countDown();
              return null;
            })
        .when(userContext)
        .close();
    var verifications = new AtomicInteger();
    var logins = new AtomicInteger();
    var provider =
        provider(
            (credentials, scope) -> logins.getAndIncrement() == 0 ? userContext : context("alice"),
            (backend, scope) ->
                verifications.getAndIncrement() == 0
                    ? verification
                    : CompletableFuture.completedFuture(null),
            (configuration, user) ->
                CompletableFuture.completedFuture(mock(AsyncConnection.class)),
            TIMEOUT);
    try {
      var result = authenticate(provider);
      result.whenComplete((identity, failure) -> blockCallback(callbackEntered, releaseCallback));
      verification.completeExceptionally(
          new BackendIdentityException(Reason.INSUFFICIENT_PERMISSION));
      assertTrue(callbackEntered.await(5, TimeUnit.SECONDS));
      assertTrue(cleaned.await(2, TimeUnit.SECONDS), "result callbacks must not delay backend cleanup");
      assertReason(Reason.INSUFFICIENT_PERMISSION, result);
      assertReason(Reason.RESOURCE_LIMIT, authenticate(provider));
      releaseCallback.countDown();
      authenticateAfterCleanup(provider);
    } finally {
      releaseCallback.countDown();
      verification.complete(null);
      provider.close();
    }
  }

  @Test
  void blockingSuccessCallbackDoesNotDelayAnotherAuthenticationOrShutdown() throws Exception {
    var verification = new CompletableFuture<Void>();
    var callbackEntered = new CountDownLatch(1);
    var releaseCallback = new CountDownLatch(1);
    var verifications = new AtomicInteger();
    var provider =
        provider(
            (credentials, scope) -> context(credentials.principal()),
            (backend, scope) ->
                verifications.getAndIncrement() == 0
                    ? verification
                    : CompletableFuture.completedFuture(null),
            (configuration, user) ->
                CompletableFuture.completedFuture(mock(AsyncConnection.class)),
            TIMEOUT,
            2);
    var executor = Executors.newSingleThreadExecutor();
    try {
      var first = authenticate(provider, "alice");
      first.thenRun(() -> blockCallback(callbackEntered, releaseCallback));
      verification.complete(null);
      assertTrue(callbackEntered.await(5, TimeUnit.SECONDS));
      var second = authenticate(provider, "bob").get(2, TimeUnit.SECONDS);
      provider.connectionFor(second).toCompletableFuture().get(2, TimeUnit.SECONDS);
      executor.submit(provider::close).get(2, TimeUnit.SECONDS);
    } finally {
      releaseCallback.countDown();
      verification.complete(null);
      provider.close();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void blockingBackendCleanupDoesNotDelayFailureNotificationOrOtherDeadlines() throws Exception {
    var verification = new CompletableFuture<Void>();
    var cleanupEntered = new CountDownLatch(1);
    var releaseCleanup = new CountDownLatch(1);
    var connection = mock(AsyncConnection.class);
    doAnswer(
            invocation -> {
              cleanupEntered.countDown();
              releaseCleanup.await();
              return null;
            })
        .when(connection)
        .close();
    var provider =
        provider(
            (credentials, scope) -> context(credentials.principal()),
            (backend, scope) -> verification,
            (configuration, user) ->
                CompletableFuture.completedFuture(
                    user.getName().equals("alice") ? connection : mock(AsyncConnection.class)),
            Duration.ofSeconds(1),
            2);
    try {
      var first = authenticate(provider, "alice");
      verification.completeExceptionally(
          new BackendIdentityException(Reason.INSUFFICIENT_PERMISSION));
      assertTrue(cleanupEntered.await(5, TimeUnit.SECONDS));
      assertReason(Reason.INSUFFICIENT_PERMISSION, first);
      var second = authenticate(provider, "bob");
      var failure = assertThrows(ExecutionException.class, () -> second.get(2, TimeUnit.SECONDS));
      assertEquals(
          Reason.TIMEOUT,
          assertInstanceOf(BackendIdentityException.class, failure.getCause()).reason());
    } finally {
      releaseCleanup.countDown();
      provider.close();
    }
  }

  @Test
  void notificationCapacityRejectsCredentialsAndTrustedActivationBeforeBackendWork()
      throws Exception {
    var verification = new CompletableFuture<Void>();
    var callbackEntered = new CountDownLatch(1);
    var releaseCallback = new CountDownLatch(1);
    var authenticator = mock(HBaseCredentialAuthenticator.class);
    when(authenticator.authenticate(any(), any()))
        .thenAnswer(
            invocation -> {
              BackendCredentials credentials = invocation.getArgument(0);
              return context(credentials.principal());
            });
    var trustedActivations = new AtomicInteger();
    var provider =
        new HBaseBackendIdentityProvider(
            "backend-auth",
            SCOPE,
            secureConfiguration(),
            authenticator,
            (backend, scope) -> verification,
            3,
            1,
            TIMEOUT,
            (configuration, user) ->
                CompletableFuture.completedFuture(mock(AsyncConnection.class)));
    try {
      var first = authenticate(provider);
      var callback = first.thenRun(() -> blockCallback(callbackEntered, releaseCallback));
      verification.complete(null);
      assertTrue(callbackEntered.await(5, TimeUnit.SECONDS));
      assertReason(Reason.RESOURCE_LIMIT, authenticate(provider, "bob"));
      assertReason(
          Reason.RESOURCE_LIMIT,
          provider
              .activateTrusted(
                  () -> {
                    trustedActivations.incrementAndGet();
                    return context("operator");
                  })
              .toCompletableFuture());
      verify(authenticator).authenticate(any(), any());
      assertEquals(0, trustedActivations.get());
      releaseCallback.countDown();
      callback.get(5, TimeUnit.SECONDS);
      authenticateAfterCleanup(provider);
      verify(authenticator, times(2)).authenticate(any(), any());
    } finally {
      releaseCallback.countDown();
      verification.complete(null);
      provider.close();
    }
  }

  @Test
  void simpleDevelopmentRejectsPasswordsAndInvalidPrincipalsWithoutOpeningConnections()
      throws Exception {
    var configuration = new Configuration(false);
    configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
    var opens = new AtomicInteger();
    for (var credentials :
        new BackendCredentials[] {
          new BackendCredentials("developer\nforged-log-entry", new char[0]),
          new BackendCredentials("a".repeat(257), new char[0]),
          new BackendCredentials("developer", new char[] {'x'})
        }) {
      try (credentials;
          var provider =
              new HBaseBackendIdentityProvider(
                  "dev-no-auth",
                  SCOPE,
                  configuration,
                  HBaseAuthenticators.simpleDevelopment(),
                  (backend, scope) -> CompletableFuture.completedFuture(null),
                  1,
                  1,
                  TIMEOUT,
                  (snapshot, user) -> {
                    opens.incrementAndGet();
                    return null;
                  })) {
        var rejected =
            assertThrows(
                ExecutionException.class,
                () ->
                    provider
                        .authenticate(credentials, SCOPE)
                        .toCompletableFuture()
                        .get(5, TimeUnit.SECONDS));
        var failure = assertInstanceOf(BackendIdentityException.class, rejected.getCause());
        assertEquals(Reason.INVALID_CREDENTIALS, failure.reason());
        assertFalse(failure.getMessage().contains("forged-log-entry"));
      }
    }
    assertEquals(0, opens.get());
  }

  @Test
  void secureAdaptersRejectSimpleConfigurationAndExplicitFallback() {
    for (var authenticator :
        new HBaseCredentialAuthenticator[] {
          HBaseAuthenticators.kerberosPassword(),
          HBaseAuthenticators.delegationToken("cluster"),
          HBaseAuthenticators.digestPassword("cluster")
        }) {
      assertThrows(
          IllegalArgumentException.class,
          () -> authenticator.validateConfiguration(new Configuration(false)));
      var configuration = secureConfiguration();
      configuration.setBoolean(HBaseCredentialAuthenticator.SIMPLE_FALLBACK, true);
      assertThrows(
          IllegalArgumentException.class, () -> authenticator.validateConfiguration(configuration));
    }
  }

  private static HBaseBackendIdentityProvider provider(
      HBaseCredentialAuthenticator authenticator,
      HBaseAccessVerifier verifier,
      HBaseBackendIdentityProvider.ConnectionOpener opener,
      Duration timeout) {
    return provider(authenticator, verifier, opener, timeout, 1);
  }

  private static HBaseBackendIdentityProvider provider(
      HBaseCredentialAuthenticator authenticator,
      HBaseAccessVerifier verifier,
      HBaseBackendIdentityProvider.ConnectionOpener opener,
      Duration timeout,
      int maximumContexts) {
    return new HBaseBackendIdentityProvider(
        "backend-auth",
        SCOPE,
        secureConfiguration(),
        authenticator,
        verifier,
        maximumContexts,
        maximumContexts,
        timeout,
        opener);
  }

  private static Configuration secureConfiguration() {
    var configuration = new Configuration(false);
    configuration.set("hbase.security.authentication", "kerberos");
    return configuration;
  }

  private static HBaseUserContext context(String name) {
    var context = mock(HBaseUserContext.class);
    when(context.user()).thenReturn(User.create(UserGroupInformation.createRemoteUser(name)));
    when(context.credentialGeneration()).thenReturn(OptionalLong.of(0));
    return context;
  }

  private static CompletableFuture<AuthorizedBackendIdentity> authenticate(
      HBaseBackendIdentityProvider provider) {
    return authenticate(provider, "alice");
  }

  private static CompletableFuture<AuthorizedBackendIdentity> authenticate(
      HBaseBackendIdentityProvider provider, String principal) {
    try (var credentials = new BackendCredentials(principal, "password".toCharArray())) {
      return provider.authenticate(credentials, SCOPE).toCompletableFuture();
    }
  }

  private static void blockCallback(CountDownLatch entered, CountDownLatch release) {
    entered.countDown();
    try {
      release.await();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new java.util.concurrent.CompletionException(interrupted);
    }
  }

  private static AuthorizedBackendIdentity authenticateAfterCleanup(
      HBaseBackendIdentityProvider provider) throws Exception {
    return authenticateAfterCleanup(provider, "alice");
  }

  private static AuthorizedBackendIdentity authenticateAfterCleanup(
      HBaseBackendIdentityProvider provider, String principal) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (true) {
      try {
        return authenticate(provider, principal).get(5, TimeUnit.SECONDS);
      } catch (ExecutionException failure) {
        if (!(failure.getCause() instanceof BackendIdentityException identityFailure)
            || identityFailure.reason() != Reason.RESOURCE_LIMIT
            || System.nanoTime() >= deadline) {
          throw failure;
        }
        // Wait for asynchronous retirement and notification completion, not a scheduling window.
        Thread.yield();
      }
    }
  }

  private static void assertReason(Reason reason, CompletableFuture<?> future) {
    var failure = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    var identityFailure = assertInstanceOf(BackendIdentityException.class, failure.getCause());
    assertEquals(reason, identityFailure.reason());
  }
}
