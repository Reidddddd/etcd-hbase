package io.etcdhbase.server.fixture;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import etcdserverpb.WatchGrpc;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Test-only Watch endpoint that stays open until the caller deadline cancels its request scope. */
public final class WatchCancellationFixture extends WatchGrpc.WatchImplBase {
  private static final long EXPECTED_REVISION = 1L << 54;
  private static final ByteString EXPECTED_KEY =
      ByteString.copyFrom(new byte[] {0, 0, (byte) 0xff});

  private final CountDownLatch requestReceived = new CountDownLatch(1);
  private final CountDownLatch scopeClosed = new CountDownLatch(1);
  private final AtomicBoolean requestValid = new AtomicBoolean();

  @Override
  public StreamObserver<Rpc.WatchRequest> watch(
      StreamObserver<Rpc.WatchResponse> responseObserver) {
    RequestScope scope = RequestScopeInterceptor.currentScope();
    scope.registerCleanup(scopeClosed::countDown);
    return new StreamObserver<>() {
      @Override
      public void onNext(Rpc.WatchRequest request) {
        requestValid.set(
            request.hasCreateRequest()
                && EXPECTED_KEY.equals(request.getCreateRequest().getKey())
                && request.getCreateRequest().getStartRevision() == EXPECTED_REVISION);
        requestReceived.countDown();
      }

      @Override
      public void onError(Throwable failure) {
        // The client cancellation is observed by the request-scope interceptor.
      }

      @Override
      public void onCompleted() {
        // Keep the server stream open until the bounded caller deadline fires.
      }
    };
  }

  public boolean awaitRequestReceived(Duration timeout) throws InterruptedException {
    return requestReceived.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
  }

  public boolean requestWasValid() {
    return requestValid.get();
  }

  public boolean awaitScopeClosed(Duration timeout) throws InterruptedException {
    return scopeClosed.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
  }
}
