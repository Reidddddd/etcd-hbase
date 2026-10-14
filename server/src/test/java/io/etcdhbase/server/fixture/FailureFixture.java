package io.etcdhbase.server.fixture;

import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Test-only error fixture; every request terminates with an error or caller cancellation. */
public final class FailureFixture extends KVGrpc.KVImplBase {
  private static final long EXPECTED_REVISION = 1L << 54;
  private static final int EXPECTED_KEY_LENGTH = 3;
  private static final Duration CANCELLATION_WAIT = Duration.ofSeconds(2);

  private final AtomicInteger rawUnavailableRequests = new AtomicInteger();
  private final AtomicInteger clientV3RetryRequests = new AtomicInteger();
  private final CountDownLatch deadlineScopeClosed = new CountDownLatch(1);

  @Override
  public void range(Rpc.RangeRequest request, StreamObserver<Rpc.RangeResponse> responseObserver) {
    RequestScope requestScope = RequestScopeInterceptor.currentScope();
    if (!requestScope.hasDeadline()
        || Context.current().getDeadline() == null
        || request.getRevision() != EXPECTED_REVISION
        || request.getKey().size() != EXPECTED_KEY_LENGTH
        || request.getKey().byteAt(1) != 0
        || request.getKey().byteAt(2) != (byte) 0xff) {
      fail(
          responseObserver,
          Status.INVALID_ARGUMENT.withDescription(
              "fixture request did not preserve wire fields and deadline"));
      return;
    }

    switch (request.getKey().byteAt(0)) {
      case 'a' ->
          fail(
              responseObserver,
              Status.INVALID_ARGUMENT.withDescription(
                  "etcdserver: authentication failed, invalid user ID or password"));
      case 'p' ->
          fail(
              responseObserver,
              Status.PERMISSION_DENIED.withDescription("etcdserver: permission denied"));
      case 'u' -> {
        rawUnavailableRequests.incrementAndGet();
        fail(responseObserver, Status.UNAVAILABLE.withDescription("etcdserver: no leader"));
      }
      case 'r' -> {
        clientV3RetryRequests.incrementAndGet();
        fail(responseObserver, Status.UNAVAILABLE.withDescription("etcdserver: no leader"));
      }
      case 'd' -> waitForCallerCancellation(requestScope, responseObserver);
      default ->
          fail(responseObserver, Status.INVALID_ARGUMENT.withDescription("unknown fixture case"));
    }
  }

  public int rawUnavailableRequestCount() {
    return rawUnavailableRequests.get();
  }

  public int clientV3RetryRequestCount() {
    return clientV3RetryRequests.get();
  }

  public boolean awaitDeadlineScopeCleanup(Duration timeout) throws InterruptedException {
    return deadlineScopeClosed.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
  }

  private void waitForCallerCancellation(
      RequestScope requestScope, StreamObserver<Rpc.RangeResponse> responseObserver) {
    requestScope.registerCleanup(deadlineScopeClosed::countDown);
    try {
      if (!deadlineScopeClosed.await(CANCELLATION_WAIT.toNanos(), TimeUnit.NANOSECONDS)) {
        fail(
            responseObserver,
            Status.DEADLINE_EXCEEDED.withDescription("etcdserver: context deadline exceeded"));
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      fail(responseObserver, Status.CANCELLED.withDescription("etcdserver: request canceled"));
    }
  }

  private static void fail(StreamObserver<Rpc.RangeResponse> responseObserver, Status status) {
    responseObserver.onError(status.asRuntimeException());
  }
}
