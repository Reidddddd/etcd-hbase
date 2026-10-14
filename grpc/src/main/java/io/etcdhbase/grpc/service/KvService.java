package io.etcdhbase.grpc.service;

import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.grpc.stub.StreamObserver;

/** M0 KV service surface; operations remain explicitly unimplemented. */
public final class KvService extends KVGrpc.KVImplBase {
  @Override
  public void range(Rpc.RangeRequest request, StreamObserver<Rpc.RangeResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "KV.Range");
  }

  @Override
  public void put(Rpc.PutRequest request, StreamObserver<Rpc.PutResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "KV.Put");
  }

  @Override
  public void deleteRange(
      Rpc.DeleteRangeRequest request, StreamObserver<Rpc.DeleteRangeResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "KV.DeleteRange");
  }

  @Override
  public void txn(Rpc.TxnRequest request, StreamObserver<Rpc.TxnResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "KV.Txn");
  }

  @Override
  public void compact(
      Rpc.CompactionRequest request, StreamObserver<Rpc.CompactionResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "KV.Compact");
  }
}
