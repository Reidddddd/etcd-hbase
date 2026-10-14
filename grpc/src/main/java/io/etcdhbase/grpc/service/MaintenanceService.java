package io.etcdhbase.grpc.service;

import etcdserverpb.MaintenanceGrpc;
import etcdserverpb.Rpc;
import io.grpc.stub.StreamObserver;

/** M0 Maintenance service surface; operations remain explicitly unimplemented. */
public final class MaintenanceService extends MaintenanceGrpc.MaintenanceImplBase {
  @Override
  public void alarm(Rpc.AlarmRequest request, StreamObserver<Rpc.AlarmResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Maintenance.Alarm");
  }

  @Override
  public void status(
      Rpc.StatusRequest request, StreamObserver<Rpc.StatusResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Maintenance.Status");
  }

  @Override
  public void defragment(
      Rpc.DefragmentRequest request, StreamObserver<Rpc.DefragmentResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Maintenance.Defragment");
  }

  @Override
  public void hash(Rpc.HashRequest request, StreamObserver<Rpc.HashResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Maintenance.Hash");
  }

  @Override
  public void hashKV(
      Rpc.HashKVRequest request, StreamObserver<Rpc.HashKVResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Maintenance.HashKV");
  }

  @Override
  public void snapshot(
      Rpc.SnapshotRequest request, StreamObserver<Rpc.SnapshotResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Maintenance.Snapshot");
  }

  @Override
  public void moveLeader(
      Rpc.MoveLeaderRequest request, StreamObserver<Rpc.MoveLeaderResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Maintenance.MoveLeader");
  }

  @Override
  public void downgrade(
      Rpc.DowngradeRequest request, StreamObserver<Rpc.DowngradeResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Maintenance.Downgrade");
  }
}
