package io.etcdhbase.grpc.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import etcdserverpb.Rpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class SkeletonServiceTest {
  @Test
  void defaultRangeRemainsUnimplementedUntilAHandlerIsAssembled() {
    AtomicReference<Throwable> failure = new AtomicReference<>();
    StreamObserver<Rpc.RangeResponse> observer = new StreamObserver<>() {
      @Override public void onNext(Rpc.RangeResponse value) { throw new AssertionError("unexpected success"); }
      @Override public void onError(Throwable value) { failure.set(value); }
      @Override public void onCompleted() { throw new AssertionError("unexpected completion"); }
    };
    var request = Rpc.RangeRequest.newBuilder().setKey(com.google.protobuf.ByteString.copyFromUtf8("k")).build();
    new KvService().range(request, observer);
    assertEquals(Status.Code.UNIMPLEMENTED, Status.fromThrowable(failure.get()).getCode());
    AtomicReference<Rpc.RangeRequest> forwarded = new AtomicReference<>();
    AtomicReference<StreamObserver<Rpc.RangeResponse>> destination = new AtomicReference<>();
    new KvService((value, response) -> { }, (value, response) -> { }, (value, response) -> { },
        (value, response) -> { forwarded.set(value); destination.set(response); }).range(request, observer);
    assertSame(request, forwarded.get());
    assertSame(observer, destination.get());
  }

  @Test
  void defaultPutRemainsUnimplementedAndAnAssembledHandlerReceivesTheOriginalRequest() {
    AtomicReference<Throwable> failure = new AtomicReference<>();
    StreamObserver<Rpc.PutResponse> observer = new StreamObserver<>() {
      @Override public void onNext(Rpc.PutResponse value) { throw new AssertionError("unexpected success"); }
      @Override public void onError(Throwable value) { failure.set(value); }
      @Override public void onCompleted() { throw new AssertionError("unexpected completion"); }
    };
    var request = Rpc.PutRequest.newBuilder().setKey(com.google.protobuf.ByteString.copyFromUtf8("k")).build();
    new KvService().put(request, observer);
    assertEquals(Status.Code.UNIMPLEMENTED, Status.fromThrowable(failure.get()).getCode());
    AtomicReference<Rpc.PutRequest> forwarded = new AtomicReference<>();
    AtomicReference<StreamObserver<Rpc.PutResponse>> destination = new AtomicReference<>();
    new KvService((value, response) -> { forwarded.set(value); destination.set(response); })
        .put(request, observer);
    assertSame(request, forwarded.get());
    assertSame(observer, destination.get());
  }

  @Test
  void defaultTxnRemainsUnimplementedUntilAHandlerIsAssembled() {
    AtomicReference<Throwable> failure = new AtomicReference<>();
    StreamObserver<Rpc.TxnResponse> observer = new StreamObserver<>() {
      @Override public void onNext(Rpc.TxnResponse value) { throw new AssertionError("unexpected success"); }
      @Override public void onError(Throwable value) { failure.set(value); }
      @Override public void onCompleted() { throw new AssertionError("unexpected completion"); }
    };
    var request = Rpc.TxnRequest.getDefaultInstance();
    new KvService().txn(request, observer);
    assertEquals(Status.Code.UNIMPLEMENTED, Status.fromThrowable(failure.get()).getCode());
    AtomicReference<Rpc.TxnRequest> forwarded = new AtomicReference<>();
    AtomicReference<StreamObserver<Rpc.TxnResponse>> destination = new AtomicReference<>();
    new KvService((value, response) -> { }, (value, response) -> {
      forwarded.set(value);
      destination.set(response);
    }).txn(request, observer);
    assertSame(request, forwarded.get());
    assertSame(observer, destination.get());
  }

  @Test
  void authenticateRemainsUnimplementedUntilItsSessionHandlerIsAssembled() {
    AtomicReference<Throwable> failure = new AtomicReference<>();
    StreamObserver<Rpc.AuthenticateResponse> observer = new StreamObserver<>() {
      @Override public void onNext(Rpc.AuthenticateResponse value) {
        throw new AssertionError("unexpected authentication response");
      }
      @Override public void onError(Throwable value) { failure.set(value); }
      @Override public void onCompleted() { throw new AssertionError("unexpected completion"); }
    };
    Rpc.AuthenticateRequest request = Rpc.AuthenticateRequest.newBuilder()
        .setName("milvus-service").setPassword("test-secret").build();
    new AuthService().authenticate(request, observer);
    assertEquals(Status.Code.UNIMPLEMENTED, Status.fromThrowable(failure.get()).getCode());

    AtomicReference<Rpc.AuthenticateRequest> forwarded = new AtomicReference<>();
    AtomicReference<StreamObserver<Rpc.AuthenticateResponse>> destination = new AtomicReference<>();
    new AuthService((value, response) -> {
      forwarded.set(value);
      destination.set(response);
    }).authenticate(request, observer);
    assertSame(request, forwarded.get());
    assertSame(observer, destination.get());
  }

  @Test
  void emptyHalfClosedStreamFailsOnceWithoutReturningAResponse() {
    AtomicInteger terminalCount = new AtomicInteger();
    StreamObserver<Rpc.WatchRequest> requests =
        new WatchService().watch(errorOnlyObserver(terminalCount));
    requests.onCompleted();
    requests.onCompleted();
    assertEquals(1, terminalCount.get());
  }

  @Test
  void clientFailureStopsTheStreamWithoutSendingAnotherTerminalResponse() {
    AtomicInteger terminalCount = new AtomicInteger();
    StreamObserver<Rpc.WatchRequest> requests =
        new WatchService().watch(errorOnlyObserver(terminalCount));
    requests.onError(new java.util.concurrent.CancellationException());
    requests.onNext(Rpc.WatchRequest.getDefaultInstance());
    requests.onCompleted();
    assertEquals(0, terminalCount.get());
  }

  private static StreamObserver<Rpc.WatchResponse> errorOnlyObserver(AtomicInteger terminalCount) {
    return new StreamObserver<>() {
      @Override
      public void onNext(Rpc.WatchResponse response) {
        throw new AssertionError("unimplemented streams must not return a response");
      }

      @Override
      public void onError(Throwable failure) {
        assertEquals(Status.Code.UNIMPLEMENTED, Status.fromThrowable(failure).getCode());
        terminalCount.incrementAndGet();
      }

      @Override
      public void onCompleted() {
        throw new AssertionError("unimplemented streams must not complete successfully");
      }
    };
  }

  @Test
  void streamTerminatesOnceAfterAValidRequestAndIgnoresFurtherTerminalSignals() {
    AtomicInteger terminalCount = new AtomicInteger();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    StreamObserver<Rpc.WatchResponse> responseObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(Rpc.WatchResponse value) {
            throw new AssertionError("unimplemented streams must not produce a response");
          }

          @Override
          public void onError(Throwable value) {
            terminalCount.incrementAndGet();
            failure.set(value);
          }

          @Override
          public void onCompleted() {
            terminalCount.incrementAndGet();
            throw new AssertionError("unimplemented streams must not complete successfully");
          }
        };

    StreamObserver<Rpc.WatchRequest> requestObserver = new WatchService().watch(responseObserver);
    requestObserver.onNext(Rpc.WatchRequest.getDefaultInstance());
    requestObserver.onCompleted();
    requestObserver.onNext(Rpc.WatchRequest.getDefaultInstance());

    assertEquals(1, terminalCount.get());
    assertEquals(Status.Code.UNIMPLEMENTED, Status.fromThrowable(failure.get()).getCode());
    assertEquals(
        "etcd-hbase M0: Watch.Watch is not implemented",
        Status.fromThrowable(failure.get()).getDescription());
  }

  @Test
  void assembledWatchServiceDelegatesTheResponseObserverAndReturnsItsRequestHandler() {
    StreamObserver<Rpc.WatchResponse> responseObserver = new StreamObserver<>() {
      @Override public void onNext(Rpc.WatchResponse value) {}
      @Override public void onError(Throwable value) {}
      @Override public void onCompleted() {}
    };
    StreamObserver<Rpc.WatchRequest> requestObserver = new StreamObserver<>() {
      @Override public void onNext(Rpc.WatchRequest value) {}
      @Override public void onError(Throwable value) {}
      @Override public void onCompleted() {}
    };
    AtomicReference<StreamObserver<Rpc.WatchResponse>> forwarded = new AtomicReference<>();

    StreamObserver<Rpc.WatchRequest> returned =
        new WatchService(observer -> { forwarded.set(observer); return requestObserver; })
            .watch(responseObserver);

    assertSame(responseObserver, forwarded.get());
    assertSame(requestObserver, returned);
  }
}
