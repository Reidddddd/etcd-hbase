package io.etcdhbase.hbase.identity;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.hbase.connection.OfficialHBaseConnectionFactory;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class IdentityIsolationTest {
  @Test
  void coalescesConcurrentRequestsForTheSameAuthorizedIdentity() throws Exception {
    AtomicInteger creations = new AtomicInteger();
    AsyncConnection connection = mock(AsyncConnection.class);
    var factory =
        new OfficialHBaseConnectionFactory(
            2,
            ignored -> {
              creations.incrementAndGet();
              return CompletableFuture.completedFuture(connection);
            });

    var first = factory.connectionFor(identity("alice", "ctx-a", "local", "hbase", 1));
    var second = factory.connectionFor(identity("alice", "ctx-a", "local", "hbase", 1));

    assertSame(first.toCompletableFuture().join(), second.toCompletableFuture().join());
    assertEquals(1, creations.get());
    factory.close();
    verify(connection).close();
  }

  @Test
  void keepsConnectionsSeparateAcrossSubjectContextGroupProfileAndCredentialGeneration() {
    AtomicInteger creations = new AtomicInteger();
    var factory =
        new OfficialHBaseConnectionFactory(
            8,
            ignored -> {
              creations.incrementAndGet();
              return CompletableFuture.completedFuture(mock(AsyncConnection.class));
            });

    var identities =
        Set.of(
            identity("alice", "ctx-a", "local", "hbase", 1),
            identity("bob", "ctx-a", "local", "hbase", 1),
            identity("alice", "ctx-b", "local", "hbase", 1),
            identity("alice", "ctx-a", "other", "hbase", 1),
            identity("alice", "ctx-a", "local", "other-profile", 1),
            identity("alice", "ctx-a", "local", "hbase", 2));

    var connections =
        identities.stream()
            .map(factory::connectionFor)
            .map(stage -> stage.toCompletableFuture().join())
            .toList();

    assertEquals(6, creations.get());
    for (int left = 0; left < connections.size(); left++) {
      for (int right = left + 1; right < connections.size(); right++) {
        assertNotSame(connections.get(left), connections.get(right));
      }
    }
    factory.close();
  }

  @Test
  void callerCancellationDoesNotCancelSharedConnectionCreation() throws Exception {
    CompletableFuture<AsyncConnection> creation = new CompletableFuture<>();
    AsyncConnection connection = mock(AsyncConnection.class);
    var factory = new OfficialHBaseConnectionFactory(1, ignored -> creation);
    AuthorizedBackendIdentity identity = identity("alice", "ctx-a", "local", "hbase", 1);
    var firstCaller = factory.connectionFor(identity).toCompletableFuture();

    assertTrue(firstCaller.cancel(true));
    var secondCaller = factory.connectionFor(identity).toCompletableFuture();
    assertFalse(creation.isCancelled());

    creation.complete(connection);

    assertSame(connection, secondCaller.join());
    factory.close();
    verify(connection).close();
  }

  @Test
  void successfulConnectionCallbacksDoNotHoldTheManagerLock() throws Exception {
    CompletableFuture<AsyncConnection> firstCreation = new CompletableFuture<>();
    AsyncConnection firstConnection = mock(AsyncConnection.class);
    var factory =
        new OfficialHBaseConnectionFactory(
            2,
            identity ->
                identity.subject().equals("alice")
                    ? firstCreation
                    : CompletableFuture.completedFuture(mock(AsyncConnection.class)));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch callbackStarted = new CountDownLatch(1);
    CountDownLatch allowCallbackToFinish = new CountDownLatch(1);

    try {
      CompletableFuture<AsyncConnection> first =
          factory
              .connectionFor(identity("alice", "ctx-a", "local", "hbase", 1))
              .toCompletableFuture();
      first.thenRun(
          () -> {
            callbackStarted.countDown();
            try {
              if (!allowCallbackToFinish.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("test callback timed out");
              }
            } catch (InterruptedException interrupted) {
              Thread.currentThread().interrupt();
              throw new CompletionException(interrupted);
            }
          });

      Future<Boolean> completion = executor.submit(() -> firstCreation.complete(firstConnection));
      assertTrue(callbackStarted.await(5, TimeUnit.SECONDS));
      Future<AsyncConnection> secondConnection =
          executor.submit(
              () ->
                  factory
                      .connectionFor(identity("bob", "ctx-b", "local", "hbase", 1))
                      .toCompletableFuture()
                      .join());
      AsyncConnection completedSecondConnection;
      try {
        completedSecondConnection = secondConnection.get(5, TimeUnit.SECONDS);
      } finally {
        allowCallbackToFinish.countDown();
      }

      assertTrue(completion.get(5, TimeUnit.SECONDS));
      assertSame(firstConnection, first.join());
      assertNotSame(firstConnection, completedSecondConnection);
    } finally {
      allowCallbackToFinish.countDown();
      factory.close();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void removesFailedCreationsForRetryAndKeepsFailureMessagesSafe() {
    AtomicInteger attempts = new AtomicInteger();
    AsyncConnection connection = mock(AsyncConnection.class);
    var factory =
        new OfficialHBaseConnectionFactory(
            1,
            ignored ->
                attempts.incrementAndGet() == 1
                    ? CompletableFuture.failedFuture(new IOException("secret diagnostic"))
                    : CompletableFuture.completedFuture(connection));
    AuthorizedBackendIdentity identity = identity("alice", "ctx-a", "local", "hbase", 1);

    CompletionException creationFailure =
        assertThrows(
            CompletionException.class,
            () -> factory.connectionFor(identity).toCompletableFuture().join());
    BackendFailure safeFailure =
        assertInstanceOf(BackendFailure.class, creationFailure.getCause());
    assertEquals(BackendFailure.Category.UNAVAILABLE, safeFailure.category());
    assertFalse(safeFailure.getMessage().contains("secret diagnostic"));

    assertSame(connection, factory.connectionFor(identity).toCompletableFuture().join());
    assertEquals(2, attempts.get());
    factory.close();
  }

  @Test
  void preservesAuthenticationAndPermissionFailureCategories() {
    var factory =
        new OfficialHBaseConnectionFactory(
            2,
            identity ->
                CompletableFuture.failedFuture(
                    new BackendIdentityException(
                        identity.subject().equals("alice")
                            ? BackendIdentityException.Reason.INVALID_CREDENTIALS
                            : BackendIdentityException.Reason.INSUFFICIENT_PERMISSION)));

    BackendFailure authenticationFailure =
        connectionFailure(factory, identity("alice", "ctx-a", "local", "hbase", 1));
    BackendFailure permissionFailure =
        connectionFailure(factory, identity("bob", "ctx-b", "local", "hbase", 1));

    assertEquals(BackendFailure.Category.AUTHENTICATION, authenticationFailure.category());
    assertEquals(BackendFailure.Category.PERMISSION, permissionFailure.category());
    assertEquals("backend authentication failed", authenticationFailure.getMessage());
    assertEquals("backend permission was denied", permissionFailure.getMessage());
    factory.close();
  }

  @Test
  void capacityCountsInFlightIdentities() throws Exception {
    CompletableFuture<AsyncConnection> creation = new CompletableFuture<>();
    var factory = new OfficialHBaseConnectionFactory(1, ignored -> creation);
    factory.connectionFor(identity("alice", "ctx-a", "local", "hbase", 1));

    CompletionException failure =
        assertThrows(
            CompletionException.class,
            () ->
                factory
                    .connectionFor(identity("bob", "ctx-b", "local", "hbase", 1))
                    .toCompletableFuture()
                    .join());

    BackendFailure resourceLimit = assertInstanceOf(BackendFailure.class, failure.getCause());
    assertEquals(BackendFailure.Category.RESOURCE_LIMIT, resourceLimit.category());
    factory.close();
    AsyncConnection lateConnection = mock(AsyncConnection.class);
    creation.complete(lateConnection);
    verify(lateConnection).close();
  }

  @Test
  void shutdownClosesAllReadyConnectionsAndIsIdempotent() throws Exception {
    AsyncConnection first = mock(AsyncConnection.class);
    AsyncConnection second = mock(AsyncConnection.class);
    var factory =
        new OfficialHBaseConnectionFactory(
            2,
            identity ->
                CompletableFuture.completedFuture(
                    identity.subject().equals("alice") ? first : second));
    factory
        .connectionFor(identity("alice", "ctx-a", "local", "hbase", 1))
        .toCompletableFuture()
        .join();
    factory
        .connectionFor(identity("bob", "ctx-b", "local", "hbase", 1))
        .toCompletableFuture()
        .join();

    factory.close();
    factory.close();

    verify(first).close();
    verify(second).close();
    CompletionException rejected =
        assertThrows(
            CompletionException.class,
            () ->
                factory
                    .connectionFor(identity("new", "ctx-c", "local", "hbase", 1))
                    .toCompletableFuture()
                    .join());
    BackendFailure unavailable = assertInstanceOf(BackendFailure.class, rejected.getCause());
    assertEquals(BackendFailure.Category.UNAVAILABLE, unavailable.category());
  }

  @Test
  void shutdownAttemptsEveryConnectionWithoutExposingCloseFailureMessages() throws Exception {
    AsyncConnection failing = mock(AsyncConnection.class);
    AsyncConnection healthy = mock(AsyncConnection.class);
    doThrow(new IOException("credential-like diagnostic")).when(failing).close();
    var factory =
        new OfficialHBaseConnectionFactory(
            2,
            identity ->
                CompletableFuture.completedFuture(
                    identity.subject().equals("alice") ? failing : healthy));
    factory
        .connectionFor(identity("alice", "ctx-a", "local", "hbase", 1))
        .toCompletableFuture()
        .join();
    factory
        .connectionFor(identity("bob", "ctx-b", "local", "hbase", 1))
        .toCompletableFuture()
        .join();

    IllegalStateException closeFailure = assertThrows(IllegalStateException.class, factory::close);

    assertEquals("failed to close 1 managed HBase connection(s)", closeFailure.getMessage());
    verify(failing).close();
    verify(healthy).close();
    factory.close();
    verify(failing, times(1)).close();
    verify(healthy, times(1)).close();
  }

  @Test
  void defaultOfficialClientCreatorRejectsTargetIdentitiesUntilTheirMechanismIsConfigured() {
    var factory =
        new OfficialHBaseConnectionFactory(
            new org.apache.hadoop.conf.Configuration(false), "hbase", 1);

    CompletionException failure =
        assertThrows(
            CompletionException.class,
            () ->
                factory
                    .connectionFor(identity("alice", "ctx-a", "local", "hbase", 1, "target"))
                    .toCompletableFuture()
                    .join());
    BackendFailure authenticationFailure =
        assertInstanceOf(BackendFailure.class, failure.getCause());
    assertEquals(BackendFailure.Category.AUTHENTICATION, authenticationFailure.category());
    factory.close();
  }

  private static AuthorizedBackendIdentity identity(
      String subject,
      String contextRef,
      String tableGroupId,
      String connectionProfileId,
      long generation) {
    return identity(
        subject,
        contextRef,
        tableGroupId,
        connectionProfileId,
        generation,
        BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID);
  }

  private static AuthorizedBackendIdentity identity(
      String subject,
      String contextRef,
      String tableGroupId,
      String connectionProfileId,
      long generation,
      String providerId) {
    return new AuthorizedBackendIdentity(
        providerId,
        subject,
        contextRef,
        new AuthenticationScope(tableGroupId, connectionProfileId),
        Set.of(tableGroupId),
        generation);
  }

  private static BackendFailure connectionFailure(
      OfficialHBaseConnectionFactory factory, AuthorizedBackendIdentity identity) {
    CompletionException failure =
        assertThrows(
            CompletionException.class,
            () -> factory.connectionFor(identity).toCompletableFuture().join());
    return assertInstanceOf(BackendFailure.class, failure.getCause());
  }
}
