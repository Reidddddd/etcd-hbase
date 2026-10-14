package io.etcdhbase.grpc.service;

import etcdserverpb.LeaseGrpc;
import etcdserverpb.Rpc;
import io.grpc.stub.StreamObserver;

/** M0 Lease service surface; operations remain explicitly unimplemented. */
public final class LeaseService extends LeaseGrpc.LeaseImplBase {
  @Override
  public void leaseGrant(
      Rpc.LeaseGrantRequest request, StreamObserver<Rpc.LeaseGrantResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Lease.LeaseGrant");
  }

  @Override
  public void leaseRevoke(
      Rpc.LeaseRevokeRequest request, StreamObserver<Rpc.LeaseRevokeResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Lease.LeaseRevoke");
  }

  @Override
  public StreamObserver<Rpc.LeaseKeepAliveRequest> leaseKeepAlive(
      StreamObserver<Rpc.LeaseKeepAliveResponse> responseObserver) {
    return UnimplementedStreams.failAfterRequest(responseObserver, "Lease.LeaseKeepAlive");
  }

  @Override
  public void leaseTimeToLive(
      Rpc.LeaseTimeToLiveRequest request,
      StreamObserver<Rpc.LeaseTimeToLiveResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Lease.LeaseTimeToLive");
  }

  @Override
  public void leaseLeases(
      Rpc.LeaseLeasesRequest request, StreamObserver<Rpc.LeaseLeasesResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Lease.LeaseLeases");
  }
}
