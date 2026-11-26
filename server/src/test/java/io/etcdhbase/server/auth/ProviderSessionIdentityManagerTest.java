package io.etcdhbase.server.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.connection.HBaseConnectionRouter;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendIdentityBorrow;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.identity.BackendIdentityException.Reason;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.junit.jupiter.api.Test;

class ProviderSessionIdentityManagerTest {
  private static final AuthenticationScope SCOPE = new AuthenticationScope("group", "profile");

  @Test
  void explicitRetirementStopsNewBorrowsAndWaitsForExistingOnes() throws Exception {
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    var provider = provider();
    var identity = identity("rpc-failure-context");
    var retirement = new CompletableFuture<Void>();
    when(provider.authenticate(any(), eq(SCOPE)))
        .thenReturn(CompletableFuture.completedFuture(identity));
    when(provider.retire(identity)).thenReturn(retirement);
    var manager = new ProviderSessionIdentityManager(() -> provider, router, SCOPE, "provider", 1);
    try (var credentials = new BackendCredentials("milvus", new char[] {'p'})) {
      var first = manager.activate(session(), credentials).toCompletableFuture().get();
      var second = manager.activate(session(), credentials).toCompletableFuture().get();
      var retired = first.retire().toCompletableFuture();
      assertFalse(retired.isDone());
      assertThrows(CompletionException.class,
          () -> manager.activate(session(), credentials).toCompletableFuture().join());
      verify(provider, times(1)).retire(identity);
      first.release().toCompletableFuture().get();
      second.release().toCompletableFuture().get();
      verify(provider, never()).close();
      retirement.complete(null);
      retired.get();
      first.retire().toCompletableFuture().get();
      verify(provider, times(1)).retire(identity);
      verify(provider).close();
    } finally {
      retirement.complete(null);
      manager.close();
      router.close();
    }
  }

  @Test
  void rechecksReusedIdentityAndRejectsNewBorrowsAfterPermissionLoss() throws Exception {
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    var provider = provider();
    var identity = identity("acl-recheck-context");
    var recheck = new CompletableFuture<Void>();
    var retirement = new CompletableFuture<Void>();
    when(provider.authenticate(any(), eq(SCOPE)))
        .thenReturn(CompletableFuture.completedFuture(identity));
    when(provider.verifyCurrentAccess(identity)).thenReturn(CompletableFuture.completedFuture(null), recheck);
    when(provider.retire(identity)).thenReturn(retirement);
    var manager = new ProviderSessionIdentityManager(() -> provider, router, SCOPE, "provider", 1);
    try (var credentials = new BackendCredentials("milvus", new char[] {'p'})) {
      var first = manager.activate(session(), credentials).toCompletableFuture().get();
      var second = manager.activate(session(), credentials).toCompletableFuture();
      assertFalse(second.isDone());
      recheck.completeExceptionally(new BackendIdentityException(Reason.INSUFFICIENT_PERMISSION));
      var failure = assertThrows(java.util.concurrent.ExecutionException.class,
          () -> second.get(5, java.util.concurrent.TimeUnit.SECONDS));
      assertEquals(Reason.INSUFFICIENT_PERMISSION,
          ((BackendIdentityException) failure.getCause()).reason());
      assertThrows(CompletionException.class,
          () -> manager.activate(session(), credentials).toCompletableFuture().join());
      verify(provider, never()).close();
      first.release().toCompletableFuture().get();
      verify(provider, never()).close();
      retirement.complete(null);
      verify(provider).close();
    } finally {
      retirement.complete(null);
      manager.close();
      router.close();
    }
  }

  @Test
  void temporaryRecheckCapacityFailureDoesNotRetireAnotherBorrow() throws Exception {
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    var provider = provider();
    var identity = identity("acl-capacity-context");
    when(provider.authenticate(any(), eq(SCOPE)))
        .thenReturn(CompletableFuture.completedFuture(identity));
    when(provider.verifyCurrentAccess(identity)).thenReturn(
        CompletableFuture.completedFuture(null),
        CompletableFuture.failedFuture(new BackendIdentityException(Reason.RESOURCE_LIMIT)),
        CompletableFuture.completedFuture(null));
    var manager = new ProviderSessionIdentityManager(() -> provider, router, SCOPE, "provider", 1);
    try (var credentials = new BackendCredentials("milvus", new char[] {'p'})) {
      var first = manager.activate(session(), credentials).toCompletableFuture().get();
      var failure = assertThrows(CompletionException.class,
          () -> manager.activate(session(), credentials).toCompletableFuture().join());
      assertEquals(Reason.RESOURCE_LIMIT, ((BackendIdentityException) failure.getCause()).reason());
      verify(provider, never()).retire(identity);
      var next = manager.activate(session(), credentials).toCompletableFuture().get();
      first.release().toCompletableFuture().get();
      next.release().toCompletableFuture().get();
      verify(provider).retire(identity);
    } finally {
      manager.close();
      router.close();
    }
  }

  @Test
  void cancellingARecheckRetainsCapacityUntilItsPhysicalWorkDrains() throws Exception {
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    var provider = provider();
    var identity = identity("acl-cancel-context");
    var recheck = new CompletableFuture<Void>();
    var retirement = new CompletableFuture<Void>();
    when(provider.authenticate(any(), eq(SCOPE)))
        .thenReturn(CompletableFuture.completedFuture(identity));
    when(provider.verifyCurrentAccess(identity)).thenReturn(recheck);
    when(provider.retire(identity)).thenReturn(retirement);
    var manager = new ProviderSessionIdentityManager(() -> provider, router, SCOPE, "provider", 1);
    try (var credentials = new BackendCredentials("milvus", new char[] {'p'})) {
      var request = manager.activate(session(), credentials).toCompletableFuture();
      assertTrue(request.cancel(false));
      assertThrows(CompletionException.class,
          () -> manager.activate(session(), credentials).toCompletableFuture().join());
      verify(provider, never()).close();
      recheck.complete(null);
      verify(provider, never()).close();
      retirement.complete(null);
      verify(provider).close();
    } finally {
      recheck.complete(null);
      retirement.complete(null);
      manager.close();
      router.close();
    }
  }

  @Test
  void closedProviderKeepsContainerCapacityUntilAccessCallbacksDrain() throws Exception {
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    var firstProvider = provider();
    var secondProvider = provider();
    var firstIdentity = identity("acl-callback-first");
    var secondIdentity = identity("acl-callback-second");
    var callbackDrain = new CompletableFuture<Void>();
    when(firstProvider.accessChecksDrained()).thenReturn(callbackDrain);
    when(firstProvider.authenticate(any(), eq(SCOPE)))
        .thenReturn(CompletableFuture.completedFuture(firstIdentity));
    when(secondProvider.authenticate(any(), eq(SCOPE)))
        .thenReturn(CompletableFuture.completedFuture(secondIdentity));
    var calls = new AtomicInteger();
    var manager = new ProviderSessionIdentityManager(
        () -> calls.getAndIncrement() == 0 ? firstProvider : secondProvider,
        router, SCOPE, "provider", 1);
    try (var credentials = new BackendCredentials("milvus", new char[] {'p'})) {
      var first = manager.activate(session(), credentials).toCompletableFuture().get();
      first.release().toCompletableFuture().get();
      verify(firstProvider).close();
      var failure = assertThrows(CompletionException.class,
          () -> manager.activate(sessionWithSubject((byte) 2), credentials).toCompletableFuture().join());
      assertEquals(Reason.RESOURCE_LIMIT, ((BackendIdentityException) failure.getCause()).reason());
      assertEquals(1, calls.get());
      callbackDrain.complete(null);
      var second = manager.activate(sessionWithSubject((byte) 2), credentials).toCompletableFuture().get();
      assertSame(secondIdentity, second.identity());
      second.release().toCompletableFuture().get();
    } finally {
      callbackDrain.complete(null);
      manager.close();
      router.close();
    }
  }

  @Test
  void concurrentBorrowsShareColdActivationAndRetireOnlyAfterTheLastRelease() {
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    var provider = provider();
    var identity = identity("shared-context");
    var authentication = new CompletableFuture<AuthorizedBackendIdentity>();
    when(provider.authenticate(any(), eq(SCOPE))).thenReturn(authentication);
    var providerCreations = new AtomicInteger();
    var manager = new ProviderSessionIdentityManager(() -> {
      providerCreations.incrementAndGet();
      return provider;
    }, router, SCOPE, "provider", 1);

    try (var firstCredentials = new BackendCredentials("milvus", new char[] {'p'});
         var secondCredentials = new BackendCredentials("milvus", new char[] {'p'})) {
      var firstRequest = manager.activate(session(), firstCredentials).toCompletableFuture();
      var secondRequest = manager.activate(session(), secondCredentials).toCompletableFuture();
      verify(provider, times(1)).authenticate(any(), eq(SCOPE));

      authentication.complete(identity);
      var firstBorrow = firstRequest.join();
      var secondBorrow = secondRequest.join();
      assertSame(firstBorrow.identity(), secondBorrow.identity());
      assertEquals(1, providerCreations.get());

      firstBorrow.release().toCompletableFuture().join();
      verify(provider, never()).close();
      secondBorrow.release().toCompletableFuture().join();
      verify(provider).close();
    } finally {
      manager.close();
      router.close();
    }
  }

  @Test
  void credentialCopyFailureCompletesSharedWaitersAndReleasesCapacity() throws Exception {
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    var failedProvider = provider();
    var nextProvider = provider();
    var nextIdentity = identity("after-credential-copy-failure");
    when(nextProvider.authenticate(any(), eq(SCOPE)))
        .thenReturn(CompletableFuture.completedFuture(nextIdentity));
    var creations = new AtomicInteger();
    var activationReserved = new java.util.concurrent.CountDownLatch(1);
    var manager = new ProviderSessionIdentityManager(() -> {
      if (creations.getAndIncrement() == 0) {
        activationReserved.countDown();
        return failedProvider;
      }
      return nextProvider;
    }, router, SCOPE, "provider", 1);

    try (var closedCredentials = new BackendCredentials("milvus", new char[] {'p'});
         var validCredentials = new BackendCredentials("milvus", new char[] {'p'})) {
      closedCredentials.close();
      var first = new CompletableFuture<SessionIdentityLease>();
      var caller = new Thread(() -> {
        try {
          manager.activate(session(), closedCredentials).whenComplete((lease, failure) -> {
            if (failure == null) first.complete(lease);
            else first.completeExceptionally(failure);
          });
        } catch (RuntimeException failure) {
          first.completeExceptionally(failure);
        }
      });
      caller.setDaemon(true);
      CompletableFuture<SessionIdentityLease> shared;
      synchronized (closedCredentials) {
        caller.start();
        assertTrue(activationReserved.await(5, java.util.concurrent.TimeUnit.SECONDS));
        shared = manager.activate(session(), validCredentials).toCompletableFuture();
        assertFalse(shared.isDone());
      }
      var firstFailure = assertThrows(java.util.concurrent.ExecutionException.class,
          () -> first.get(5, java.util.concurrent.TimeUnit.SECONDS));
      var sharedFailure = assertThrows(java.util.concurrent.ExecutionException.class,
          () -> shared.get(5, java.util.concurrent.TimeUnit.SECONDS));
      assertTrue(firstFailure.getCause() instanceof IllegalStateException);
      assertTrue(sharedFailure.getCause() instanceof IllegalStateException);
      verify(failedProvider, never()).authenticate(any(), eq(SCOPE));
      verify(failedProvider).close();

      var next = manager.activate(session(), validCredentials).toCompletableFuture()
          .get(5, java.util.concurrent.TimeUnit.SECONDS);
      assertSame(nextIdentity, next.identity());
      assertEquals(2, creations.get());
      next.release().toCompletableFuture().join();
    } finally {
      manager.close();
      router.close();
    }
  }

  @Test
  void cancellingOneBorrowDoesNotCancelTheSharedColdActivation() {
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    var provider = provider();
    var identity = identity("cancel-safe-context");
    var authentication = new CompletableFuture<AuthorizedBackendIdentity>();
    when(provider.authenticate(any(), eq(SCOPE))).thenReturn(authentication);
    var manager = new ProviderSessionIdentityManager(() -> provider, router, SCOPE, "provider", 1);

    try (var firstCredentials = new BackendCredentials("milvus", new char[] {'p'});
         var secondCredentials = new BackendCredentials("milvus", new char[] {'p'})) {
      var cancelled = manager.activate(session(), firstCredentials).toCompletableFuture();
      var survivor = manager.activate(session(), secondCredentials).toCompletableFuture();
      assertTrue(cancelled.cancel(true));
      assertFalse(authentication.isCancelled());

      authentication.complete(identity);
      var borrow = survivor.join();
      assertSame(identity, borrow.identity());
      borrow.release().toCompletableFuture().join();
      verify(provider).close();
    } finally {
      manager.close();
      router.close();
    }
  }

  @Test
  void lateActivationWithoutBorrowersIsClosedBeforeCapacityIsReused() {
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    var firstProvider = provider();
    var secondProvider = provider();
    var lateAuthentication = new CompletableFuture<AuthorizedBackendIdentity>();
    when(firstProvider.authenticate(any(), eq(SCOPE))).thenReturn(lateAuthentication);
    when(secondProvider.authenticate(any(), eq(SCOPE))).thenReturn(
        CompletableFuture.completedFuture(identity("next-context")));
    var providerCreations = new AtomicInteger();
    var manager = new ProviderSessionIdentityManager(
        () -> providerCreations.getAndIncrement() == 0 ? firstProvider : secondProvider,
        router, SCOPE, "provider", 1);

    try (var firstCredentials = new BackendCredentials("milvus", new char[] {'p'});
         var rejectedCredentials = new BackendCredentials("milvus", new char[] {'p'});
         var nextCredentials = new BackendCredentials("milvus", new char[] {'p'})) {
      var cancelled = manager.activate(session(), firstCredentials).toCompletableFuture();
      assertTrue(cancelled.cancel(true));
      assertFalse(lateAuthentication.isCancelled());
      var full = assertThrows(CompletionException.class,
          () -> manager.activate(sessionWithSubject((byte) 1), rejectedCredentials)
              .toCompletableFuture().join());
      assertEquals(Reason.RESOURCE_LIMIT, ((BackendIdentityException) full.getCause()).reason());

      lateAuthentication.complete(identity("late-context"));
      verify(firstProvider).close();
      var next = manager.activate(sessionWithSubject((byte) 1), nextCredentials)
          .toCompletableFuture().join();
      assertEquals("next-context", next.identity().contextRef());
      assertEquals(2, providerCreations.get());
      next.release().toCompletableFuture().join();
    } finally {
      manager.close();
      router.close();
    }
  }

  @Test
  void deterministicActivationFailureIsBrieflyNegativeCached() {
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    var provider = provider();
    when(provider.authenticate(any(), eq(SCOPE))).thenReturn(CompletableFuture.failedFuture(
        new BackendIdentityException(Reason.INVALID_CREDENTIALS)));
    var providerCreations = new AtomicInteger();
    var manager = new ProviderSessionIdentityManager(() -> {
      providerCreations.incrementAndGet();
      return provider;
    }, router, SCOPE, "provider", 1);

    try (var firstCredentials = new BackendCredentials("milvus", new char[] {'p'});
         var secondCredentials = new BackendCredentials("milvus", new char[] {'p'})) {
      var firstFailure = assertThrows(CompletionException.class,
          () -> manager.activate(session(), firstCredentials).toCompletableFuture().join());
      var cachedFailure = assertThrows(CompletionException.class,
          () -> manager.activate(session(), secondCredentials).toCompletableFuture().join());
      assertEquals(Reason.INVALID_CREDENTIALS,
          ((BackendIdentityException) firstFailure.getCause()).reason());
      assertEquals(Reason.INVALID_CREDENTIALS,
          ((BackendIdentityException) cachedFailure.getCause()).reason());
      assertEquals(1, providerCreations.get());
      verify(provider).authenticate(any(), eq(SCOPE));
    } finally {
      manager.close();
      router.close();
    }
  }

  @Test
  void concurrentColdActivationsRespectTheContainerLimit() {
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    var provider = provider();
    var authentication = new CompletableFuture<AuthorizedBackendIdentity>();
    when(provider.authenticate(any(), eq(SCOPE))).thenReturn(authentication);
    var providerCreations = new AtomicInteger();
    var manager = new ProviderSessionIdentityManager(() -> {
      providerCreations.incrementAndGet();
      return provider;
    }, router, SCOPE, "provider", 1);

    try (var firstCredentials = new BackendCredentials("milvus", new char[] {'p'});
         var secondCredentials = new BackendCredentials("milvus", new char[] {'p'})) {
      var firstRequest = manager.activate(session(), firstCredentials).toCompletableFuture();
      var capacityFailure = assertThrows(CompletionException.class,
          () -> manager.activate(sessionWithSubject((byte) 1), secondCredentials)
              .toCompletableFuture().join());
      assertEquals(Reason.RESOURCE_LIMIT,
          ((BackendIdentityException) capacityFailure.getCause()).reason());
      assertEquals(1, providerCreations.get());

      authentication.complete(identity("bounded-context"));
      firstRequest.join().release().toCompletableFuture().join();
    } finally {
      manager.close();
      router.close();
    }
  }

  @Test
  void retiringOneRequestClosesOnlyItsProviderContainer() {
    var base = mock(HBaseConnectionFactory.class);
    var router = new HBaseConnectionRouter(base);
    var firstProvider = provider();
    var secondProvider = provider();
    var firstIdentity = identity("context-first");
    var secondIdentity = identity("context-second");
    var firstConnection = mock(AsyncConnection.class);
    var secondConnection = mock(AsyncConnection.class);
    when(firstProvider.authenticate(any(), eq(SCOPE)))
        .thenReturn(CompletableFuture.completedFuture(firstIdentity));
    when(secondProvider.authenticate(any(), eq(SCOPE)))
        .thenReturn(CompletableFuture.completedFuture(secondIdentity));
    when(firstProvider.connectionFor(firstIdentity))
        .thenReturn(CompletableFuture.completedFuture(firstConnection));
    when(secondProvider.connectionFor(secondIdentity))
        .thenReturn(CompletableFuture.completedFuture(secondConnection));

    var providers = new java.util.concurrent.atomic.AtomicInteger();
    var manager = new ProviderSessionIdentityManager(
        () -> providers.getAndIncrement() == 0 ? firstProvider : secondProvider,
        router, SCOPE, "provider", 2);
    try (var credentials = new BackendCredentials("milvus", new char[] {'p'})) {
      var first = manager.activate(session(), credentials).toCompletableFuture().join();
      var second = manager.activate(sessionWithSubject((byte) 1), credentials)
          .toCompletableFuture().join();

      assertSame(firstConnection,
          router.connectionFor(first.identity()).toCompletableFuture().join());
      assertSame(secondConnection,
          router.connectionFor(second.identity()).toCompletableFuture().join());

      first.release().toCompletableFuture().join();

      verify(firstProvider).close();
      verify(secondProvider, never()).close();
      assertSame(secondConnection,
          router.connectionFor(second.identity()).toCompletableFuture().join());
    } finally {
      manager.close();
      router.close();
    }
    verify(secondProvider).close();
  }

  @Test
  void failedIdentityRetirementRetainsCapacityUntilBoundedRetry() throws Exception {
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    var provider = provider();
    var identity = identity("context-retry");
    when(provider.authenticate(any(), eq(SCOPE)))
        .thenReturn(CompletableFuture.completedFuture(identity));
    var firstRetirement = new CompletableFuture<Void>();
    when(provider.retire(identity)).thenReturn(firstRetirement,
        CompletableFuture.failedFuture(new IllegalStateException("fixture retry failure")),
        CompletableFuture.completedFuture(null));
    var manager = new ProviderSessionIdentityManager(() -> provider, router, SCOPE, "provider", 1);
    try (var credentials = new BackendCredentials("milvus", new char[] {'p'})) {
      var lease = manager.activate(session(), credentials).toCompletableFuture().join();
      var release = lease.release().toCompletableFuture();
      assertFalse(release.isDone());
      var capacityFailure = assertThrows(CompletionException.class,
          () -> manager.activate(sessionWithSubject((byte) 1), credentials)
              .toCompletableFuture().join());
      assertEquals(Reason.RESOURCE_LIMIT,
          ((BackendIdentityException) capacityFailure.getCause()).reason());

      firstRetirement.completeExceptionally(new IllegalStateException("fixture cleanup failure"));
      release.get(5, java.util.concurrent.TimeUnit.SECONDS);
      verify(provider, times(3)).retire(identity);
      verify(provider).close();

      manager.close();
      router.close();
    }
  }

  @Test
  void failedProviderCloseRemainsOwnedUntilManagerRetriesCleanup() throws Exception {
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    var provider = provider();
    var identity = identity("provider-close-retry");
    when(provider.authenticate(any(), eq(SCOPE)))
        .thenReturn(CompletableFuture.completedFuture(identity));
    org.mockito.Mockito.doThrow(new IllegalStateException("fixture close failure"))
        .doThrow(new IllegalStateException("fixture close failure"))
        .doThrow(new IllegalStateException("fixture close failure"))
        .doNothing().when(provider).close();
    var manager = new ProviderSessionIdentityManager(() -> provider, router, SCOPE, "provider", 1);
    try (var credentials = new BackendCredentials("milvus", new char[] {'p'})) {
      var lease = manager.activate(session(), credentials).toCompletableFuture().get();
      assertThrows(java.util.concurrent.ExecutionException.class,
          () -> lease.release().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS));
      var capacityFailure = assertThrows(CompletionException.class,
          () -> manager.activate(sessionWithSubject((byte) 1), credentials)
              .toCompletableFuture().join());
      assertEquals(Reason.RESOURCE_LIMIT,
          ((BackendIdentityException) capacityFailure.getCause()).reason());

      manager.close();
      verify(provider, times(4)).close();
    } finally {
      manager.close();
      router.close();
    }
  }

  @Test
  void managerCloseRetainsBorrowedContainerForBoundedRetryAfterRelease() {
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    var provider = provider();
    var identity = identity("context-close-retry");
    when(provider.authenticate(any(), eq(SCOPE)))
        .thenReturn(CompletableFuture.completedFuture(identity));
    when(provider.isFullyDrained()).thenReturn(false, false, false, true);
    var manager = new ProviderSessionIdentityManager(() -> provider, router, SCOPE, "provider", 1);
    try (var credentials = new BackendCredentials("milvus", new char[] {'p'})) {
      var lease = manager.activate(session(), credentials).toCompletableFuture().join();

      assertThrows(IllegalStateException.class, manager::close);
      verify(provider, times(3)).close();

      lease.release().toCompletableFuture().join();
      verify(provider).retire(identity);
      verify(provider, times(4)).close();
    } finally {
      manager.close();
      router.close();
    }
  }

  @Test
  void rejectsAnIdentityFromAnotherProviderAndClosesItsContainer() {
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    var provider = provider();
    var wrong = new AuthorizedBackendIdentity("other-provider", "milvus",
        "wrong-provider-context", SCOPE, Set.of("group"), 1);
    when(provider.authenticate(any(), eq(SCOPE)))
        .thenReturn(CompletableFuture.completedFuture(wrong));
    var manager = new ProviderSessionIdentityManager(() -> provider, router, SCOPE, "provider", 1);
    try (var credentials = new BackendCredentials("milvus", new char[] {'p'})) {
      var failure = assertThrows(java.util.concurrent.CompletionException.class,
          () -> manager.activate(session(), credentials).toCompletableFuture().join());
      assertEquals(Reason.INSUFFICIENT_PERMISSION,
          ((BackendIdentityException) failure.getCause()).reason());
      manager.close();
      router.close();
    }
    verify(provider).close();
  }

  private static HBaseBackendIdentityProvider provider() {
    var provider = mock(HBaseBackendIdentityProvider.class);
    when(provider.providerId()).thenReturn("provider");
    try {
      when(provider.borrow(any())).thenAnswer(invocation -> borrow(invocation.getArgument(0)));
    } catch (BackendIdentityException impossible) {
      throw new AssertionError(impossible);
    }
    when(provider.retire(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(provider.verifyCurrentAccess(any())).thenReturn(CompletableFuture.completedFuture(null));
    when(provider.accessChecksDrained()).thenReturn(CompletableFuture.completedFuture(null));
    when(provider.isFullyDrained()).thenReturn(true);
    return provider;
  }

  private static BackendIdentityBorrow borrow(AuthorizedBackendIdentity identity) {
    return new BackendIdentityBorrow() {
      @Override
      public AuthorizedBackendIdentity identity() {
        return identity;
      }

      @Override
      public CompletionStage<Void> release() {
        return CompletableFuture.completedFuture(null);
      }
    };
  }

  private static AuthorizedBackendIdentity identity(String context) {
    return new AuthorizedBackendIdentity("provider", "milvus", context, SCOPE,
        Set.of("group"), 1);
  }

  private static AuthSession session() {
    return sessionWithSubject((byte) 0);
  }

  private static AuthSession sessionWithSubject(byte marker) {
    byte[] subjectDigest = new byte[32];
    subjectDigest[0] = marker;
    var envelope = new CredentialEnvelope("key-1", CredentialEnvelope.CURRENT_AAD_VERSION,
        new byte[CredentialEnvelope.NONCE_BYTES], new byte[CredentialEnvelope.TAG_BYTES]);
    return new AuthSession(ByteKey.copyOf(new byte[16]), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(subjectDigest), "milvus", "provider", SCOPE,
        ByteKey.copyOf(new byte[32]),
        1, 1, 1, 2, 0, 1, AuthSession.Status.ACTIVE, envelope);
  }
}
