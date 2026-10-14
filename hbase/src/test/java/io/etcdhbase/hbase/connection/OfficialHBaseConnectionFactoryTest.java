package io.etcdhbase.hbase.connection;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OfficialHBaseConnectionFactoryTest {
  @Test
  void classifiesConnectionCreationTimeoutsAndCancellation() {
    TimeoutException timeoutCause = new TimeoutException("sensitive timeout detail");
    var timeoutFactory =
        new OfficialHBaseConnectionFactory(
            1, ignored -> CompletableFuture.failedFuture(timeoutCause));

    CompletionException timeoutFailure =
        assertThrows(
            CompletionException.class,
            () -> timeoutFactory.connectionFor(identity()).toCompletableFuture().join());
    BackendFailure timeout = assertInstanceOf(BackendFailure.class, timeoutFailure.getCause());
    assertEquals(BackendFailure.Category.TIMEOUT, timeout.category());
    assertEquals("backend operation timed out", timeout.getMessage());
    assertSame(timeoutCause, timeout.getCause());
    timeoutFactory.close();

    CancellationException cancellationCause =
        new CancellationException("sensitive cancellation detail");
    var cancellationFactory =
        new OfficialHBaseConnectionFactory(
            1, ignored -> CompletableFuture.failedFuture(cancellationCause));

    CompletionException cancellationFailure =
        assertThrows(
            CompletionException.class,
            () -> cancellationFactory.connectionFor(identity()).toCompletableFuture().join());
    BackendFailure cancelled =
        assertInstanceOf(BackendFailure.class, cancellationFailure.getCause());
    assertEquals(BackendFailure.Category.CANCELLED, cancelled.category());
    assertEquals("backend operation was cancelled", cancelled.getMessage());
    assertSame(cancellationCause, cancelled.getCause());
    cancellationFactory.close();
  }

  @Test
  void shutdownWaitsForAnAcceptedConnectionToBePublished() throws Exception {
    CompletableFuture<AsyncConnection> creation = new CompletableFuture<>();
    BlockingConnectionFuture publication = new BlockingConnectionFuture();
    AsyncConnection connection = mock(AsyncConnection.class);
    OfficialHBaseConnectionFactory factory =
        new OfficialHBaseConnectionFactory(
            1, ignored -> creation, () -> publication);
    var result = factory.connectionFor(identity());
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch shutdownStarted = new CountDownLatch(1);
    AtomicReference<Thread> shutdownThread = new AtomicReference<>();

    try {
      Future<Boolean> creationCompletion = executor.submit(() -> creation.complete(connection));
      assertTrue(publication.completionStarted.await(5, TimeUnit.SECONDS));

      Future<?> shutdown =
          executor.submit(
              () -> {
                shutdownThread.set(Thread.currentThread());
                shutdownStarted.countDown();
                factory.close();
              });
      assertTrue(shutdownStarted.await(5, TimeUnit.SECONDS));
      awaitBlocked(shutdownThread.get());
      assertThrows(TimeoutException.class, () -> shutdown.get(100, TimeUnit.MILLISECONDS));

      publication.allowCompletion.countDown();
      assertTrue(creationCompletion.get(5, TimeUnit.SECONDS));
      shutdown.get(5, TimeUnit.SECONDS);

      assertTrue(result.toCompletableFuture().isDone());
      assertSame(connection, result.toCompletableFuture().join());
      verify(connection).close();
    } finally {
      publication.allowCompletion.countDown();
      factory.close();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static void awaitBlocked(Thread thread) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (thread.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
      TimeUnit.MILLISECONDS.sleep(1);
    }
    assertEquals(Thread.State.BLOCKED, thread.getState());
  }

  private static AuthorizedBackendIdentity identity() {
    return new AuthorizedBackendIdentity(
        BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID,
        "alice",
        "ctx-a",
        new AuthenticationScope("local", "hbase"),
        Set.of("local"),
        1);
  }

  private static final class BlockingConnectionFuture extends CompletableFuture<AsyncConnection> {
    private final CountDownLatch completionStarted = new CountDownLatch(1);
    private final CountDownLatch allowCompletion = new CountDownLatch(1);

    @Override
    public boolean complete(AsyncConnection connection) {
      completionStarted.countDown();
      try {
        if (!allowCompletion.await(5, TimeUnit.SECONDS)) {
          throw new IllegalStateException("connection publication was not released");
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("connection publication was interrupted", interrupted);
      }
      return super.complete(connection);
    }
  }
}
