package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.CompactionOperation;
import io.etcdhbase.core.commit.CompactionService;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class CompactGatewayTest {
  private static final String GROUP = "compact-group";
  private static final long CLUSTER = 7;
  private static final long MEMBER = 9;
  private static final AuthorizedBackendIdentity IDENTITY = new AuthorizedBackendIdentity(
      "provider", "service-user", "context", new AuthenticationScope(GROUP, "profile"),
      Set.of(GROUP), 1);
  private static final ByteKey AUTHORIZATION = ByteKey.copyOf(new byte[32]);
  private static final CommitOwnerService.OwnerToken OWNER =
      new CommitOwnerService.OwnerToken("owner-instance", 4);

  @Test
  void emptyEncodedZeroRequestCanEnterTheRealOwnerLane() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    var queue = queue(fixture);
    try (var scope = fixture.scope()) {
      var observer = new RecordingObserver();
      assertEquals(0, compact(0, false).getSerializedSize());
      fixture.gateway().accept(compact(0, false), java.util.Optional.empty(), scope, observer);
      observer.terminal.get(5, TimeUnit.SECONDS);
      assertEquals(1, observer.responses.get());
      assertNull(observer.failure.get());
    } finally {
      closeQueue(queue);
    }
  }

  @Test
  void realLaneRejectionReturnsAnErrorInsteadOfHanging() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    var queue = queue(fixture);
    try (var scope = fixture.scope()) {
      queue.close();
      var observer = new RecordingObserver();
      fixture.gateway().accept(compact(5, false), java.util.Optional.empty(), scope, observer);
      observer.terminal.get(5, TimeUnit.SECONDS);
      assertEquals(0, observer.responses.get());
      assertEquals(Status.Code.UNAVAILABLE,
          assertInstanceOf(StatusRuntimeException.class, observer.failure.get()).getStatus().getCode());
    } finally {
      closeQueue(queue);
    }
  }

  @Test
  void dispatchedServiceScopeCannotSendSuccessAfterTheRpcScopeIsCancelled() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    var collection = new CompletableFuture<Void>();
    var started = new CountDownLatch(1);
    fixture.completion = (target, serviceScope) -> {
      started.countDown();
      return collection;
    };
    var queue = queue(fixture);
    try (var scope = fixture.scope()) {
      var observer = new RecordingObserver();
      fixture.gateway().accept(compact(5, true), java.util.Optional.empty(), scope, observer);
      assertTrue(started.await(5, TimeUnit.SECONDS));
      scope.cancel();
      collection.complete(null);
      queue.close();
      queue.drained().get(5, TimeUnit.SECONDS);
      observer.terminal.get(5, TimeUnit.SECONDS);
      assertEquals(0, observer.responses.get());
      assertEquals(Status.Code.CANCELLED,
          assertInstanceOf(StatusRuntimeException.class, observer.failure.get()).getStatus().getCode());
    } finally {
      collection.complete(null);
      closeQueue(queue);
    }
  }

  @Test
  void realLaneSuccessWaitsForServiceCleanup() throws Exception {
    assertResponseWaitsForCleanup(false);
  }

  @Test
  void realLaneCleanupFailureCannotFollowAnAlreadySentSuccess() throws Exception {
    assertResponseWaitsForCleanup(true);
  }

  private static void assertResponseWaitsForCleanup(boolean failCleanup) throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    var cleanup = new CompletableFuture<Void>();
    var cleanupStarted = new CountDownLatch(1);
    doAnswer(call -> {
      RequestScope serviceScope = call.getArgument(4);
      serviceScope.registerScopedCleanup(cleanupStarted::countDown, cleanup);
      return CompletableFuture.completedFuture(fixture.operation);
    }).when(fixture.compactions).compact(anyLong(), any(), any(), any(), any());
    var queue = queue(fixture);
    try (var scope = fixture.scope()) {
      var observer = new RecordingObserver();
      fixture.gateway().accept(compact(5, true), java.util.Optional.empty(), scope, observer);
      assertTrue(cleanupStarted.await(5, TimeUnit.SECONDS));
      assertEquals(0, observer.responses.get(), "queue-owned cleanup must settle before success");
      assertFalse(observer.terminal.isDone());
      if (failCleanup) cleanup.completeExceptionally(new IllegalStateException("fixture cleanup failed"));
      else cleanup.complete(null);
      observer.terminal.get(5, TimeUnit.SECONDS);
      if (failCleanup) {
        assertEquals(0, observer.responses.get());
        assertEquals(Status.Code.INTERNAL,
            assertInstanceOf(StatusRuntimeException.class, observer.failure.get()).getStatus().getCode());
      } else {
        assertEquals(1, observer.responses.get());
        assertNull(observer.failure.get());
      }
    } finally {
      cleanup.complete(null);
      closeQueue(queue);
    }
  }

  @Test
  void logicalSelectionReturnsTheResponseRevisionAndItsServingHeader() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    var observer = new RecordingObserver();

    try (var scope = fixture.scope()) {
      fixture.gateway().accept(compact(5, false), java.util.Optional.empty(), scope, observer);
    }

    assertEquals(1, observer.responses.get(), describe(observer.failure.get()));
    assertEquals(12, observer.last.get().getHeader().getRevision());
    assertEquals(CLUSTER, observer.last.get().getHeader().getClusterId());
    assertEquals(MEMBER, observer.last.get().getHeader().getMemberId());
    assertTrue(observer.completed.get());
    assertNull(observer.failure.get());
    assertEquals(0, fixture.completionWaits.get(), "logical-only Compact performs no history deletion");
  }

  @Test
  void physicalWaitDoesNotHoldTheOwnerLane() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    var completed = new CompletableFuture<Void>();
    var laneReleased = new AtomicBoolean();
    fixture.lane = (bytes, scope, work) -> work.apply(scope)
        .whenComplete((receipt, failure) -> laneReleased.set(true));
    fixture.completion = (target, scope) -> completed;
    var observer = new RecordingObserver();
    try (var scope = fixture.scope()) {
      try {
        fixture.gateway().accept(compact(5, true), java.util.Optional.empty(), scope, observer);
        assertTrue(laneReleased.get(), "physical completion must wait outside the mutation owner lane");
        assertEquals(0, observer.responses.get());
      } finally {
        completed.complete(null);
      }
    }
  }

  @Test
  void physicalCompactWaitsForItsOwnBoundedHistoryDeletion() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    var collect = new CompletableFuture<Void>();
    fixture.completion = (target, scope) -> {
      fixture.completionWaits.incrementAndGet();
      return collect;
    };
    var observer = new RecordingObserver();

    try (var scope = fixture.scope()) {
      fixture.gateway().accept(compact(5, true), java.util.Optional.empty(), scope, observer);
      assertEquals(0, observer.responses.get(), "physical must not answer before its pages finish");
      collect.complete(null);
    }

    assertEquals(1, observer.responses.get(),
        "collect=" + fixture.completionWaits.get() + " " + describe(observer.failure.get()));
    assertEquals(1, fixture.completionWaits.get());
  }

  @Test
  void physicalCompactReportsFailureWhenHistoryDeletionFails() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    fixture.completion = (target, scope) -> CompletableFuture.failedFuture(
        new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    var observer = new RecordingObserver();

    try (var scope = fixture.scope()) {
      fixture.gateway().accept(compact(5, true), java.util.Optional.empty(), scope, observer);
    }

    assertEquals(0, observer.responses.get());
    assertEquals(Status.Code.UNAVAILABLE,
        assertInstanceOf(StatusRuntimeException.class, observer.failure.get()).getStatus().getCode());
  }

  @Test
  void physicalCompactFailsClosedWhenNoHistoryCollectorIsWired() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    fixture.collectorWired = false;
    var observer = new RecordingObserver();

    try (var scope = fixture.scope()) {
      fixture.gateway().accept(compact(5, true), java.util.Optional.empty(), scope, observer);
    }

    assertEquals(0, observer.responses.get(),
        "a logical CAS is never reported as physical completion without deletion");
    assertEquals(Status.Code.UNAVAILABLE,
        assertInstanceOf(StatusRuntimeException.class, observer.failure.get()).getStatus().getCode());
  }

  @Test
  void logicalCompactStillSucceedsWithoutAHistoryCollector() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    fixture.collectorWired = false;
    var observer = new RecordingObserver();

    try (var scope = fixture.scope()) {
      fixture.gateway().accept(compact(5, false), java.util.Optional.empty(), scope, observer);
    }

    assertEquals(1, observer.responses.get());
  }

  @Test
  void repeatedCompactIsReportedAsCompactedRatherThanSucceeding() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.COMPACTED, 12);
    var observer = new RecordingObserver();

    try (var scope = fixture.scope()) {
      fixture.gateway().accept(compact(5, false), java.util.Optional.empty(), scope, observer);
    }

    assertEquals(0, observer.responses.get());
    assertEquals(Status.Code.OUT_OF_RANGE,
        assertInstanceOf(StatusRuntimeException.class, observer.failure.get()).getStatus().getCode());
    assertTrue(observer.failure.get().getMessage().contains("compacted"));
  }

  @Test
  void futureCompactIsRejectedWithoutAdvancingTheBoundary() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.FUTURE, 12);
    var observer = new RecordingObserver();

    try (var scope = fixture.scope()) {
      fixture.gateway().accept(compact(99, false), java.util.Optional.empty(), scope, observer);
    }

    assertEquals(0, observer.responses.get());
    assertEquals(Status.Code.OUT_OF_RANGE,
        assertInstanceOf(StatusRuntimeException.class, observer.failure.get()).getStatus().getCode());
    assertTrue(observer.failure.get().getMessage().contains("future revision"));
  }

  @Test
  void missingOwnerFailsClosedWithoutAttemptingAControlWrite() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    fixture.ownerPresent = false;
    var observer = new RecordingObserver();

    try (var scope = fixture.scope()) {
      fixture.gateway().accept(compact(5, false), java.util.Optional.empty(), scope, observer);
    }

    assertEquals(0, observer.responses.get());
    assertEquals(Status.Code.UNAVAILABLE,
        assertInstanceOf(StatusRuntimeException.class, observer.failure.get()).getStatus().getCode());
    verify(fixture.compactions, never()).compact(anyLong(), any(), any(), any(), any());
  }

  @Test
  void cancelledRequestDoesNotSendAResponse() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    var collect = new CompletableFuture<Void>();
    fixture.completion = (target, scope) -> collect;
    var observer = new RecordingObserver();

    try (var scope = fixture.scope()) {
      fixture.gateway().accept(compact(5, true), java.util.Optional.empty(), scope, observer);
      scope.cancel();
      collect.complete(null);
    }

    assertEquals(0, observer.responses.get(), "a cancelled request never receives Compact success");
  }

  private static String describe(Throwable failure) {
    var text = new StringBuilder(String.valueOf(failure));
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      text.append(" <- ").append(cause.getClass().getSimpleName()).append(':').append(cause.getMessage());
      if (cause.getCause() == cause) break;
    }
    return text.toString();
  }

  private static void closeQueue(SerialMutationQueue queue) throws Exception {
    queue.close();
    queue.drained().get(5, TimeUnit.SECONDS);
    assertTrue(queue.awaitTermination(Duration.ofSeconds(5)));
  }

  private static SerialMutationQueue queue(Fixture fixture) {
    var queue = new SerialMutationQueue(0, 1024, Duration.ofSeconds(5));
    fixture.lane = queue::submit;
    return queue;
  }

  private static Rpc.CompactionRequest compact(long revision, boolean physical) {
    return Rpc.CompactionRequest.newBuilder().setRevision(revision).setPhysical(physical).build();
  }

  private static final class Fixture {
    private final CompactionService compactions = mock(CompactionService.class);
    private final CompactionOperation operation = mock(CompactionOperation.class);
    private final AtomicInteger completionWaits = new AtomicInteger();
    private volatile CompactGateway.PhysicalCompletion completion = (target, scope) -> {
      completionWaits.incrementAndGet();
      return CompletableFuture.completedFuture(null);
    };
    private volatile boolean collectorWired = true;
    private volatile boolean ownerPresent = true;
    private CompactGateway.LaneSubmitter lane = (bytes, scope, work) -> work.apply(scope);

    private Fixture(CompactionOperation.Stage stage, long responseRevision) {
      when(operation.stage()).thenReturn(stage);
      when(operation.responseRevision()).thenReturn(responseRevision);
      when(compactions.compact(anyLong(), any(), any(), any(), any())).thenAnswer(call ->
          ((RequestScope) call.getArgument(4)).startIfOpen(() -> { })
              ? CompletableFuture.completedFuture(operation)
              : CompletableFuture.failedFuture(
                  new BackendFailure(BackendFailure.Category.CANCELLED)));

    }

    private RequestScope scope() {
      return RequestScope.withTimeout(Duration.ofSeconds(5));
    }

    private CompactGateway gateway() {
      var authorization = new CompactRequestAuthorization(GROUP, IDENTITY, AUTHORIZATION, null,
          mock(io.etcdhbase.hbase.api.HBaseConnectionFactory.class),
          java.util.List.of(org.apache.hadoop.hbase.TableName.valueOf("compact-test")));
      return new CompactGateway(authorization, CLUSTER, MEMBER, compactions,
          lane, () -> {
            return ownerPresent ? java.util.Optional.of(OWNER) : java.util.Optional.empty();
          },
          scope -> CompletableFuture.completedFuture(
              CommitState.restore(ByteKey.copyOf(new byte[32]), java.util.Optional.of("owner-instance"),
                  4, Long.MAX_VALUE, 2, 2, 12, 2, 12, java.util.Optional.empty(), 0)),
          collectorWired
              ? java.util.Optional.of((target, scope) -> completion.awaitCompletion(target, scope))
              : java.util.Optional.empty());
    }
  }

  private static final class RecordingObserver implements StreamObserver<Rpc.CompactionResponse> {
    private final AtomicInteger responses = new AtomicInteger();
    private final AtomicReference<Rpc.CompactionResponse> last = new AtomicReference<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicBoolean completed = new AtomicBoolean();
    private final CompletableFuture<Void> terminal = new CompletableFuture<>();

    @Override public void onNext(Rpc.CompactionResponse response) {
      responses.incrementAndGet();
      last.set(response);
    }
    @Override public void onError(Throwable cause) { failure.set(cause); terminal.complete(null); }
    @Override public void onCompleted() { completed.set(true); terminal.complete(null); }
  }
}
