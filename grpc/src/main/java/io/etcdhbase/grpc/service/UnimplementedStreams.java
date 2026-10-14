package io.etcdhbase.grpc.service;

import io.etcdhbase.grpc.protocol.ErrorProjection;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Shared terminal handling for service methods that are not implemented in the M0 skeleton. */
final class UnimplementedStreams {
  private UnimplementedStreams() {}

  static void failUnary(StreamObserver<?> responseObserver, String method) {
    Objects.requireNonNull(responseObserver, "responseObserver")
        .onError(ErrorProjection.notImplemented(method));
  }

  static <RequestT, ResponseT> StreamObserver<RequestT> failAfterRequest(
      StreamObserver<ResponseT> responseObserver, String method) {
    Objects.requireNonNull(responseObserver, "responseObserver");
    AtomicBoolean terminal = new AtomicBoolean();
    AtomicBoolean cancelled = new AtomicBoolean();
    if (responseObserver instanceof ServerCallStreamObserver<?> serverObserver) {
      serverObserver.setOnCancelHandler(
          () -> {
            cancelled.set(true);
            terminal.set(true);
          });
    }

    return new StreamObserver<>() {
      @Override
      public void onNext(RequestT request) {
        fail();
      }

      @Override
      public void onError(Throwable failure) {
        cancelled.set(true);
        terminal.set(true);
      }

      @Override
      public void onCompleted() {
        fail();
      }

      private void fail() {
        if (terminal.compareAndSet(false, true) && !cancelled.get()) {
          responseObserver.onError(ErrorProjection.notImplemented(method));
        }
      }
    };
  }
}
