package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import etcdserverpb.Rpc;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseRenewalVersion;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.observability.RequestMetrics;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class LeaseKeepAliveStreamTest {
  @Test
  void serializesResponsesAndRequestsOneInboundMessageAtATime() {
    var observer = new RecordingObserver();
    var writes = new AtomicInteger();
    var releases = new AtomicInteger();
    var metrics = new RequestMetrics();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers, request -> {
        writes.incrementAndGet();
        return CompletableFuture.completedFuture(delivery(request.getID(), releases));
      }, metrics);

      assertEquals(1, observer.requests);
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(11).build());

      assertEquals(1, writes.get());
      assertEquals(1, observer.responses.size());
      assertEquals(11, observer.responses.getFirst().getID());
      assertEquals(1, releases.get());
      assertEquals(2, observer.requests,
          "the next request is admitted only after the previous response leaves the stream");
      assertEquals(1, metrics.leaseRenewals().responses());
      assertEquals(observer.responses.getFirst().getSerializedSize(),
          metrics.leaseRenewals().responseBytes());
      assertEquals(1, metrics.leaseRenewals().stages()
          .get(RequestMetrics.LeaseRenewalStage.OUTBOUND_READY_WAIT).count());
      stream.onCompleted();
      assertTrue(observer.completed);
    }
  }

  @Test
  void doesNotSendUntilReadyAndReleasesPendingDeliveryOnCancellation() {
    var observer = new RecordingObserver();
    observer.ready = false;
    var releases = new AtomicInteger();
    var metrics = new RequestMetrics();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers,
          request -> CompletableFuture.completedFuture(delivery(request.getID(), releases)), metrics);
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(21).build());

      assertTrue(observer.responses.isEmpty());
      assertEquals(0, releases.get());

      observer.ready = true;
      observer.readyHandler.run();
      assertEquals(1, observer.responses.size());
      assertEquals(1, releases.get());
      assertEquals(2, observer.requests);
      assertEquals(1, metrics.leaseRenewals().responses());
      assertEquals(1, metrics.leaseRenewals().stages()
          .get(RequestMetrics.LeaseRenewalStage.OUTBOUND_READY_WAIT).count());

      observer.ready = false;
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(22).build());
      assertEquals(1, observer.responses.size());
      assertEquals(1, releases.get());
      observer.cancel();

      assertEquals(2, releases.get(), "cancellation releases a response waiting for transport readiness");
      assertEquals(1, observer.responses.size());
      assertTrue(scope.isCancelled());
    }
  }

  @Test
  void sendsZeroTtlOnlyAfterIdentityRevalidationSucceeds() {
    var observer = new RecordingObserver();
    observer.ready = false;
    var validation = new CompletableFuture<Void>();
    var validationCalls = new AtomicInteger();
    var releases = new AtomicInteger();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers,
          request -> CompletableFuture.completedFuture(new LeaseKeepAliveStream.Delivery(
              Rpc.LeaseKeepAliveResponse.newBuilder().setID(request.getID()).setTTL(0).build(),
              releases::incrementAndGet, () -> {
                validationCalls.incrementAndGet();
                return validation;
              })), new RequestMetrics());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(23).build());
      assertEquals(0, validationCalls.get());
      assertTrue(observer.responses.isEmpty());

      observer.ready = true;
      observer.readyHandler.run();
      assertEquals(1, validationCalls.get());
      assertTrue(observer.responses.isEmpty());
      assertEquals(0, releases.get());

      validation.complete(null);
      assertEquals(1, observer.responses.size());
      assertEquals(23, observer.responses.getFirst().getID());
      assertEquals(0, observer.responses.getFirst().getTTL());
      assertEquals(1, releases.get());
      stream.onCompleted();
      assertTrue(observer.completed);
      assertEquals(1, releases.get());
    }
  }

  @Test
  void successfulAuthorizationHandsOffWriterWithoutRequeueingResponse() {
    var work = new ArrayDeque<Runnable>();
    var observer = new RecordingObserver();
    var validation = new CompletableFuture<Void>();
    var releases = new AtomicInteger();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, work::addLast, timers,
          request -> CompletableFuture.completedFuture(new LeaseKeepAliveStream.Delivery(
              Rpc.LeaseKeepAliveResponse.newBuilder().setID(request.getID()).setTTL(10).build(),
              releases::incrementAndGet, () -> validation)), new RequestMetrics());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(26).build());
      while (!work.isEmpty()) work.removeFirst().run();
      assertTrue(observer.responses.isEmpty());
      assertEquals(0, releases.get());

      validation.complete(null);

      assertEquals(1, observer.responses.size(),
          "fresh authorization and delivery must not be separated by another worker queue");
      assertEquals(1, releases.get());
      assertEquals(2, observer.requests);
      stream.onCompleted();
      while (!work.isEmpty()) work.removeFirst().run();
      assertTrue(observer.completed);
    }
  }

  @Test
  void timeoutDuringAuthorizationTerminatesWithoutFakingProducerDrain() {
    var observer = new RecordingObserver();
    var validation = new CompletableFuture<Void>();
    var timeout = new AtomicReference<Runnable>();
    var releases = new AtomicInteger();
    var timers = mock(ScheduledThreadPoolExecutor.class);
    when(timers.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS)))
        .thenAnswer(invocation -> {
          timeout.set(invocation.getArgument(0));
          return mock(ScheduledFuture.class);
        });
    try (var scope = RequestScope.withoutDeadline()) {
      assertTrue(scope.startIfOpen(() -> scope.trackPhysicalProducer(validation)));
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers,
          request -> CompletableFuture.completedFuture(new LeaseKeepAliveStream.Delivery(
              Rpc.LeaseKeepAliveResponse.newBuilder().setID(request.getID()).setTTL(10).build(),
              releases::incrementAndGet, () -> validation)), new RequestMetrics());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(27).build());
      assertTrue(observer.responses.isEmpty());
      assertEquals(0, releases.get());

      timeout.get().run();

      assertEquals(0, observer.failed.getCount());
      assertEquals(io.grpc.Status.Code.RESOURCE_EXHAUSTED,
          io.grpc.Status.fromThrowable(observer.failure).getCode());
      assertEquals(1, releases.get());
      assertTrue(scope.isCancelled());
      assertFalse(scope.physicalProducersCompletion().toCompletableFuture().isDone(),
          "terminal stream delivery is not proof that the backend producer has drained");

      validation.complete(null);

      assertTrue(scope.physicalProducersCompletion().toCompletableFuture().isDone());
      assertTrue(observer.responses.isEmpty(), "late authorization cannot resurrect the response");
      assertEquals(1, releases.get());
      observer.cancel();
      assertEquals(1, releases.get());
    }
  }

  @Test
  void rejectsZeroTtlWhenIdentityRevalidationFails() throws Exception {
    var observer = new RecordingObserver();
    observer.ready = false;
    var validation = new CompletableFuture<Void>();
    var validationCalls = new AtomicInteger();
    var releases = new AtomicInteger();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers,
          request -> CompletableFuture.completedFuture(new LeaseKeepAliveStream.Delivery(
              Rpc.LeaseKeepAliveResponse.newBuilder().setID(request.getID()).setTTL(0).build(),
              releases::incrementAndGet, () -> {
                validationCalls.incrementAndGet();
                return validation;
              })), new RequestMetrics());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(24).build());
      assertEquals(0, validationCalls.get());
      assertTrue(observer.responses.isEmpty());

      observer.ready = true;
      observer.readyHandler.run();
      assertEquals(1, validationCalls.get());
      assertTrue(observer.responses.isEmpty());
      assertEquals(0, releases.get());

      validation.completeExceptionally(new BackendFailure(BackendFailure.Category.INVALID_AUTH_TOKEN));
      assertTrue(observer.failed.await(2, TimeUnit.SECONDS));
      assertTrue(observer.responses.isEmpty());
      assertEquals(io.grpc.Status.Code.UNAUTHENTICATED,
          ((io.grpc.StatusRuntimeException) observer.failure).getStatus().getCode());
      assertTrue(scope.isCancelled());
      assertEquals(1, releases.get());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(25).build());
      observer.cancel();
      assertEquals(1, releases.get());
      assertTrue(observer.responses.isEmpty());
    }
  }

  @Test
  void preservesGrantedKeepAliveTtlWhenTheNextRenewalWindowStillFits() {
    var observer = new RecordingObserver();
    observer.ready = false;
    var releases = new AtomicInteger();
    var remainingTtl = new AtomicLong(8_000);
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers,
          request -> CompletableFuture.completedFuture(new LeaseKeepAliveStream.Delivery(
              Rpc.LeaseKeepAliveResponse.newBuilder().setID(request.getID()).setTTL(15).build(),
              releases::incrementAndGet, () -> {
                if (!LeaseKeepAliveTiming.canSendPositiveResponse(15, remainingTtl.get())) {
                  throw new CompletionException(
                    new BackendFailure(BackendFailure.Category.UNAVAILABLE));
                }
              })), new RequestMetrics());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(25).build());

      observer.ready = true;
      observer.readyHandler.run();

      assertEquals(1, observer.responses.size());
      assertEquals(15, observer.responses.getFirst().getTTL(),
          "the KeepAlive response preserves the granted TTL while rechecking that the lease is active");
      assertEquals(1, releases.get());
    }
  }

  @Test
  void terminatesInsteadOfPromisingAFullTtlWhenTheClientCannotRenewBeforeExpiry() throws Exception {
    var observer = new RecordingObserver();
    observer.ready = false;
    var releases = new AtomicInteger();
    var remainingTtl = new AtomicLong(4_000);
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers,
          request -> CompletableFuture.completedFuture(new LeaseKeepAliveStream.Delivery(
              Rpc.LeaseKeepAliveResponse.newBuilder().setID(request.getID()).setTTL(15).build(),
              releases::incrementAndGet, () -> {
                if (!LeaseKeepAliveTiming.canSendPositiveResponse(15, remainingTtl.get())) {
                  throw new CompletionException(
                      new BackendFailure(BackendFailure.Category.UNAVAILABLE));
                }
              })), new RequestMetrics());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(27).build());

      observer.ready = true;
      observer.readyHandler.run();

      assertTrue(observer.failed.await(2, TimeUnit.SECONDS));
      assertTrue(observer.responses.isEmpty());
      assertEquals(io.grpc.Status.Code.UNAVAILABLE,
          ((io.grpc.StatusRuntimeException) observer.failure).getStatus().getCode());
      assertEquals(1, releases.get());
      assertTrue(scope.isCancelled());
    }
  }

  @Test
  void terminatesInsteadOfSendingKeepAliveAfterItsDeadline() throws Exception {
    var observer = new RecordingObserver();
    observer.ready = false;
    var releases = new AtomicInteger();
    var remainingTtl = new AtomicLong(2);
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers,
          request -> CompletableFuture.completedFuture(new LeaseKeepAliveStream.Delivery(
              Rpc.LeaseKeepAliveResponse.newBuilder().setID(request.getID()).setTTL(6).build(),
              releases::incrementAndGet, () -> {
                if (remainingTtl.get() <= 0) throw new CompletionException(
                    new BackendFailure(BackendFailure.Category.UNAVAILABLE));
              })), new RequestMetrics());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(26).build());

      remainingTtl.set(0);
      observer.ready = true;
      observer.readyHandler.run();

      assertTrue(observer.responses.isEmpty(),
          "a positive KeepAlive response must not be sent after its lease deadline");
      assertTrue(observer.failed.await(2, TimeUnit.SECONDS));
      assertTrue(observer.failure instanceof io.grpc.StatusRuntimeException);
      assertEquals(io.grpc.Status.Code.UNAVAILABLE,
          ((io.grpc.StatusRuntimeException) observer.failure).getStatus().getCode());
      assertEquals(1, releases.get());
      assertTrue(scope.isCancelled());
    }
  }

  @Test
  void rejectsBackpressuredResponseAfterLeaseIdIsRevokedAndRegrantedWithSameTtl()
      throws Exception {
    var observer = new RecordingObserver();
    observer.ready = false;
    long nowEpochMillis = 50_000;
    long ttlSeconds = 15;
    var originalRenewal = LeaseState.grant(
        new LeaseIdentity(28, 1), ttlSeconds, nowEpochMillis + 15_000, 1)
        .renew(nowEpochMillis + 15_000, 2);
    var revoked = originalRenewal.revoke(3);
    var regranted = LeaseState.grant(
        new LeaseIdentity(28, 2), ttlSeconds, nowEpochMillis + 15_000, 4)
        .renew(nowEpochMillis + 15_000, 5);
    var currentLease = new AtomicReference<>(originalRenewal);
    var responseVersion = LeaseRenewalVersion.from(originalRenewal);
    var validationCalls = new AtomicInteger();
    var releases = new AtomicInteger();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers,
          request -> CompletableFuture.completedFuture(new LeaseKeepAliveStream.Delivery(
              Rpc.LeaseKeepAliveResponse.newBuilder().setID(request.getID())
                  .setTTL(ttlSeconds).build(), releases::incrementAndGet, () -> {
                    validationCalls.incrementAndGet();
                    LeaseGateway.validateKeepAliveLease(currentLease.get(), responseVersion,
                        ttlSeconds,
                        new LeaseTimeObservation(nowEpochMillis, nowEpochMillis, true));
                  })), new RequestMetrics());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(28).build());

      assertEquals(0, validationCalls.get(),
          "the authoritative state check must wait until the response can be sent");
      assertTrue(observer.responses.isEmpty());

      currentLease.set(revoked);
      currentLease.set(regranted);
      observer.ready = true;
      observer.readyHandler.run();

      assertTrue(observer.failed.await(2, TimeUnit.SECONDS));
      assertEquals(1, validationCalls.get());
      assertTrue(observer.responses.isEmpty(),
          "the previous Lease incarnation must not receive a positive TTL after ID reuse");
      assertEquals(io.grpc.Status.Code.UNAVAILABLE,
          ((io.grpc.StatusRuntimeException) observer.failure).getStatus().getCode());
      assertEquals(1, releases.get());
      assertTrue(scope.isCancelled());
    }
  }

  @Test
  void discardsValidationThatSpansAnOutboundNotReadyTransition() throws Exception {
    var observer = new RecordingObserver();
    var firstValidation = new CompletableFuture<Void>();
    var leaseActive = new AtomicBoolean(true);
    var validationCalls = new AtomicInteger();
    var releases = new AtomicInteger();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers,
          request -> CompletableFuture.completedFuture(new LeaseKeepAliveStream.Delivery(
              Rpc.LeaseKeepAliveResponse.newBuilder().setID(request.getID()).setTTL(15).build(),
              releases::incrementAndGet, () -> {
                if (validationCalls.incrementAndGet() == 1) return firstValidation;
                if (!leaseActive.get()) {
                  return CompletableFuture.failedFuture(
                      new BackendFailure(BackendFailure.Category.UNAVAILABLE));
                }
                return CompletableFuture.completedFuture(null);
              })), new RequestMetrics());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(30).build());

      assertEquals(1, validationCalls.get());
      observer.ready = false;
      leaseActive.set(false);
      observer.ready = true;
      observer.readyHandler.run();
      firstValidation.complete(null);

      assertTrue(observer.failed.await(2, TimeUnit.SECONDS));
      assertEquals(2, validationCalls.get(),
          "a state read spanning an outbound readiness change must be discarded and retried");
      assertTrue(observer.responses.isEmpty());
      assertEquals(1, releases.get());
    }
  }

  @Test
  void releasesDeliveryOnceWhenCancellationWinsDuringAsynchronousValidation() {
    var observer = new RecordingObserver();
    var validation = new CompletableFuture<Void>();
    var validationCalls = new AtomicInteger();
    var releases = new AtomicInteger();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers,
          request -> CompletableFuture.completedFuture(new LeaseKeepAliveStream.Delivery(
              Rpc.LeaseKeepAliveResponse.newBuilder().setID(request.getID()).setTTL(15).build(),
              releases::incrementAndGet, () -> {
                validationCalls.incrementAndGet();
                return validation;
              })), new RequestMetrics());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(29).build());

      assertEquals(1, validationCalls.get());
      assertTrue(observer.responses.isEmpty());
      assertEquals(0, releases.get());

      observer.cancel();
      validation.complete(null);

      assertTrue(observer.responses.isEmpty());
      assertEquals(1, releases.get(), "cancellation and a late validation callback must release once");
      assertTrue(scope.isCancelled());
    }
  }

  @Test
  void halfCloseWaitsForCurrentOperationBeforeCompleting() {
    var observer = new RecordingObserver();
    CompletableFuture<LeaseKeepAliveStream.Delivery> operation = new CompletableFuture<>();
    var releases = new AtomicInteger();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers, request -> operation,
          new RequestMetrics());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(31).build());
      stream.onCompleted();

      assertFalse(observer.completed, "the response for an accepted request must precede half-close");
      operation.complete(delivery(31, releases));

      assertEquals(1, observer.responses.size());
      assertEquals(31, observer.responses.getFirst().getID());
      assertTrue(observer.completed);
      assertEquals(1, releases.get());
    }
  }

  @Test
  void observerFailureReleasesDeliveryAndStopsFurtherInput() {
    var observer = new RecordingObserver();
    observer.failOnNext = true;
    var handled = new AtomicInteger();
    var releases = new AtomicInteger();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers, request -> {
        handled.incrementAndGet();
        return CompletableFuture.completedFuture(delivery(request.getID(), releases));
      }, new RequestMetrics());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(41).build());
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(42).build());

      assertEquals(1, handled.get());
      assertEquals(1, releases.get());
      assertEquals(1, observer.requests);
      assertTrue(scope.isCancelled());
    }
  }

  @Test
  void timesOutAStalledOutboundResponseAndReleasesItsDelivery() throws Exception {
    var observer = new RecordingObserver();
    observer.ready = false;
    var releases = new AtomicInteger();
    var metrics = new RequestMetrics();
    try (var scope = RequestScope.withoutDeadline(); var timers = timers()) {
      var stream = new LeaseKeepAliveStream(observer, scope, Runnable::run, timers,
          request -> CompletableFuture.completedFuture(new LeaseKeepAliveStream.Delivery(
              Rpc.LeaseKeepAliveResponse.newBuilder().setID(request.getID()).setTTL(1).build(),
              releases::incrementAndGet)), metrics);
      stream.onNext(Rpc.LeaseKeepAliveRequest.newBuilder().setID(51).build());

      assertTrue(observer.failed.await(2, TimeUnit.SECONDS), "stalled response was not terminated");
      assertTrue(observer.failure instanceof io.grpc.StatusRuntimeException);
      assertEquals(io.grpc.Status.Code.RESOURCE_EXHAUSTED,
          ((io.grpc.StatusRuntimeException) observer.failure).getStatus().getCode());
      assertTrue(observer.responses.isEmpty(), "an expired positive TTL response was sent");
      assertEquals(1, releases.get(), "the timed-out response budget must be released once");
      assertTrue(scope.isCancelled(), "the failed stream must stop accepting new work");
      assertEquals(1, metrics.leaseRenewals().stages()
          .get(RequestMetrics.LeaseRenewalStage.OUTBOUND_READY_WAIT).count());
    }
  }

  private static ScheduledThreadPoolExecutor timers() {
    var timers = new ScheduledThreadPoolExecutor(1);
    timers.setRemoveOnCancelPolicy(true);
    timers.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    return timers;
  }

  private static LeaseKeepAliveStream.Delivery delivery(long id, AtomicInteger releases) {
    return new LeaseKeepAliveStream.Delivery(
        Rpc.LeaseKeepAliveResponse.newBuilder().setID(id).setTTL(30).build(), releases::incrementAndGet);
  }

  private static final class RecordingObserver
      extends ServerCallStreamObserver<Rpc.LeaseKeepAliveResponse> {
    private final List<Rpc.LeaseKeepAliveResponse> responses = new ArrayList<>();
    private Runnable readyHandler;
    private Runnable cancelHandler;
    private boolean ready = true;
    private boolean cancelled;
    private boolean completed;
    private boolean failOnNext;
    private int requests;
    private final CountDownLatch failed = new CountDownLatch(1);
    private Throwable failure;

    @Override public boolean isCancelled() { return cancelled; }
    @Override public void setOnCancelHandler(Runnable handler) { cancelHandler = handler; }
    @Override public void setCompression(String compression) {}
    @Override public void setOnReadyHandler(Runnable handler) { readyHandler = handler; }
    @Override public boolean isReady() { return ready; }
    @Override public void request(int count) { requests += count; }
    @Override public void disableAutoInboundFlowControl() {}
    @Override public void setMessageCompression(boolean enabled) {}

    @Override public void onNext(Rpc.LeaseKeepAliveResponse response) {
      if (failOnNext) throw new IllegalStateException("simulated observer failure");
      responses.add(response);
    }

    @Override public void onError(Throwable cause) { failure = cause; failed.countDown(); }
    @Override public void onCompleted() { completed = true; }

    private void cancel() {
      cancelled = true;
      cancelHandler.run();
    }
  }
}
