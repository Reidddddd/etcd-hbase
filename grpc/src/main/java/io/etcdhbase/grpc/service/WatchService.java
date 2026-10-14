package io.etcdhbase.grpc.service;

import etcdserverpb.Rpc;
import etcdserverpb.WatchGrpc;
import io.grpc.stub.StreamObserver;

/** M0 Watch service surface; operations remain explicitly unimplemented. */
public final class WatchService extends WatchGrpc.WatchImplBase {
  @Override
  public StreamObserver<Rpc.WatchRequest> watch(
      StreamObserver<Rpc.WatchResponse> responseObserver) {
    return UnimplementedStreams.failAfterRequest(responseObserver, "Watch.Watch");
  }
}
