package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
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
import io.etcdhbase.core.commit.GcCoordinator;
import io.etcdhbase.core.commit.HistoryGcPermit;
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
  void logicalSelectionReturnsTheResponseRevisionAndItsServingHeader() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    var observer = new RecordingObserver();

    try (var scope = fixture.scope()) {
      fixture.gateway().accept(compact(5, false), observer);
    }

    assertEquals(1, observer.responses.get(), describe(observer.failure.get()));
    assertEquals(12, observer.last.get().getHeader().getRevision());
    assertEquals(CLUSTER, observer.last.get().getHeader().getClusterId());
    assertEquals(MEMBER, observer.last.get().getHeader().getMemberId());
    assertTrue(observer.completed.get());
    assertNull(observer.failure.get());
    assertEquals(0, fixture.collections.get(), "logical-only Compact performs no history deletion");
  }

  @Test
  void physicalCompactWaitsForItsOwnBoundedHistoryDeletion() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    var collect = new CompletableFuture<Integer>();
    fixture.collection = (permit, scope) -> {
      fixture.collections.incrementAndGet();
      return collect;
    };
    var observer = new RecordingObserver();

    try (var scope = fixture.scope()) {
      fixture.gateway().accept(compact(5, true), observer);
      assertEquals(0, observer.responses.get(), "physical must not answer before its pages finish");
      collect.complete(3);
    }

    assertEquals(1, observer.responses.get(),
        "collect=" + fixture.collections.get() + " " + describe(observer.failure.get()));
    assertEquals(1, fixture.collections.get());
  }

  @Test
  void physicalCompactReportsFailureWhenHistoryDeletionFails() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    fixture.collection = (permit, scope) -> CompletableFuture.failedFuture(
        new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    var observer = new RecordingObserver();

    try (var scope = fixture.scope()) {
      fixture.gateway().accept(compact(5, true), observer);
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
      fixture.gateway().accept(compact(5, true), observer);
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
      fixture.gateway().accept(compact(5, false), observer);
    }

    assertEquals(1, observer.responses.get());
  }

  @Test
  void repeatedCompactIsReportedAsCompactedRatherThanSucceeding() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.COMPACTED, 12);
    var observer = new RecordingObserver();

    try (var scope = fixture.scope()) {
      fixture.gateway().accept(compact(5, false), observer);
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
      fixture.gateway().accept(compact(99, false), observer);
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
      fixture.gateway().accept(compact(5, false), observer);
    }

    assertEquals(0, observer.responses.get());
    assertEquals(Status.Code.UNAVAILABLE,
        assertInstanceOf(StatusRuntimeException.class, observer.failure.get()).getStatus().getCode());
    verify(fixture.compactions, never()).compact(anyLong(), any(), any(), any(), any());
  }

  @Test
  void cancelledRequestDoesNotSendAResponse() throws Exception {
    var fixture = new Fixture(CompactionOperation.Stage.LOGICAL, 12);
    var collect = new CompletableFuture<Integer>();
    fixture.collection = (permit, scope) -> collect;
    var observer = new RecordingObserver();

    try (var scope = fixture.scope()) {
      fixture.gateway().accept(compact(5, true), observer);
      scope.cancel();
      collect.complete(1);
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

  private static Rpc.CompactionRequest compact(long revision, boolean physical) {
    return Rpc.CompactionRequest.newBuilder().setRevision(revision).setPhysical(physical).build();
  }

  private static final class Fixture {
    private final CompactionService compactions = mock(CompactionService.class);
    private final GcCoordinator gc = mock(GcCoordinator.class);
    private final AtomicInteger collections = new AtomicInteger();
    private volatile CompactGateway.HistoryCollection collection = (permit, scope) -> {
      collections.incrementAndGet();
      return CompletableFuture.completedFuture(0);
    };
    private volatile boolean collectorWired = true;
    private volatile boolean ownerPresent = true;
    private final AtomicReference<RequestScope> current = new AtomicReference<>();

    private Fixture(CompactionOperation.Stage stage, long responseRevision) {
      var operation = mock(CompactionOperation.class);
      when(operation.stage()).thenReturn(stage);
      when(operation.responseRevision()).thenReturn(responseRevision);
      when(compactions.compact(anyLong(), any(), any(), any(), any())).thenAnswer(call ->
          ((RequestScope) call.getArgument(4)).startIfOpen(() -> { })
              ? CompletableFuture.completedFuture(operation)
              : CompletableFuture.failedFuture(
                  new BackendFailure(BackendFailure.Category.CANCELLED)));
      var permit = mock(HistoryGcPermit.class);
      when(gc.authorizeHistory(any(), any())).thenReturn(CompletableFuture.completedFuture(permit));
    }

    private RequestScope scope() {
      RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
      current.set(scope);
      return scope;
    }

    private CompactGateway gateway() {
      return new CompactGateway(GROUP, IDENTITY, AUTHORIZATION, CLUSTER, MEMBER, compactions, gc,
          (bytes, scope, work) -> work.apply(scope), current::get, () -> {
            return ownerPresent ? java.util.Optional.of(OWNER) : java.util.Optional.empty();
          },
          scope -> CompletableFuture.completedFuture(
              CommitState.restore(ByteKey.copyOf(new byte[32]), java.util.Optional.of("owner-instance"),
                  4, Long.MAX_VALUE, 2, 2, 12, 2, 12, java.util.Optional.empty(), 0)),
          collectorWired
              ? java.util.Optional.of((permit, scope) -> collection.collect(permit, scope))
              : java.util.Optional.empty());
    }
  }

  private static final class RecordingObserver implements StreamObserver<Rpc.CompactionResponse> {
    private final AtomicInteger responses = new AtomicInteger();
    private final AtomicReference<Rpc.CompactionResponse> last = new AtomicReference<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicBoolean completed = new AtomicBoolean();

    @Override public void onNext(Rpc.CompactionResponse response) {
      responses.incrementAndGet();
      last.set(response);
    }
    @Override public void onError(Throwable cause) { failure.set(cause); }
    @Override public void onCompleted() { completed.set(true); }
  }
}
