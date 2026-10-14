package io.etcdhbase.grpc.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import etcdserverpb.Rpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class SkeletonServiceTest {
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
}
