package io.etcdhbase.grpc.service;

import etcdserverpb.ClusterGrpc;
import etcdserverpb.Rpc;
import io.grpc.stub.StreamObserver;

/** M0 Cluster service surface; operations remain explicitly unimplemented. */
public final class ClusterService extends ClusterGrpc.ClusterImplBase {
  @Override
  public void memberAdd(
      Rpc.MemberAddRequest request, StreamObserver<Rpc.MemberAddResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Cluster.MemberAdd");
  }

  @Override
  public void memberRemove(
      Rpc.MemberRemoveRequest request, StreamObserver<Rpc.MemberRemoveResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Cluster.MemberRemove");
  }

  @Override
  public void memberUpdate(
      Rpc.MemberUpdateRequest request, StreamObserver<Rpc.MemberUpdateResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Cluster.MemberUpdate");
  }

  @Override
  public void memberList(
      Rpc.MemberListRequest request, StreamObserver<Rpc.MemberListResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Cluster.MemberList");
  }

  @Override
  public void memberPromote(
      Rpc.MemberPromoteRequest request,
      StreamObserver<Rpc.MemberPromoteResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Cluster.MemberPromote");
  }
}
