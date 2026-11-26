package io.etcdhbase.grpc.service;

import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.function.BiConsumer;

/** KV service surface with explicitly installed handlers and unimplemented remaining methods. */
public final class KvService extends KVGrpc.KVImplBase {
  private final BiConsumer<Rpc.PutRequest, StreamObserver<Rpc.PutResponse>> putHandler;
  private final BiConsumer<Rpc.TxnRequest, StreamObserver<Rpc.TxnResponse>> txnHandler;
  private final BiConsumer<Rpc.DeleteRangeRequest, StreamObserver<Rpc.DeleteRangeResponse>> deleteHandler;
  private final BiConsumer<Rpc.RangeRequest, StreamObserver<Rpc.RangeResponse>> rangeHandler;
  private final BiConsumer<Rpc.CompactionRequest, StreamObserver<Rpc.CompactionResponse>> compactHandler;

  /** Keeps the protocol-only skeleton explicitly unimplemented. */
  public KvService() {
    this.putHandler = null;
    this.txnHandler = null;
    this.deleteHandler = null;
    this.rangeHandler = null;
    this.compactHandler = null;
  }

  /** Installs a fully assembled Put handler; other KV methods remain unimplemented. */
  public KvService(BiConsumer<Rpc.PutRequest, StreamObserver<Rpc.PutResponse>> putHandler) {
    this.putHandler = Objects.requireNonNull(putHandler, "putHandler");
    this.txnHandler = null;
    this.deleteHandler = null;
    this.rangeHandler = null;
    this.compactHandler = null;
  }

  /** Installs both fully assembled handlers. */
  public KvService(BiConsumer<Rpc.PutRequest, StreamObserver<Rpc.PutResponse>> putHandler,
      BiConsumer<Rpc.TxnRequest, StreamObserver<Rpc.TxnResponse>> txnHandler) {
    this.putHandler = Objects.requireNonNull(putHandler, "putHandler");
    this.txnHandler = Objects.requireNonNull(txnHandler, "txnHandler");
    this.deleteHandler = null;
    this.rangeHandler = null;
    this.compactHandler = null;
  }

  /** Installs Put, Txn and DeleteRange handlers backed by the same table-group runtime. */
  public KvService(BiConsumer<Rpc.PutRequest, StreamObserver<Rpc.PutResponse>> putHandler,
      BiConsumer<Rpc.TxnRequest, StreamObserver<Rpc.TxnResponse>> txnHandler,
      BiConsumer<Rpc.DeleteRangeRequest, StreamObserver<Rpc.DeleteRangeResponse>> deleteHandler) {
    this.putHandler = Objects.requireNonNull(putHandler, "putHandler");
    this.txnHandler = Objects.requireNonNull(txnHandler, "txnHandler");
    this.deleteHandler = Objects.requireNonNull(deleteHandler, "deleteHandler");
    this.rangeHandler = null;
    this.compactHandler = null;
  }

  /** Installs the Range, Put, Txn and DeleteRange handlers for one table group. */
  public KvService(BiConsumer<Rpc.PutRequest, StreamObserver<Rpc.PutResponse>> putHandler,
      BiConsumer<Rpc.TxnRequest, StreamObserver<Rpc.TxnResponse>> txnHandler,
      BiConsumer<Rpc.DeleteRangeRequest, StreamObserver<Rpc.DeleteRangeResponse>> deleteHandler,
      BiConsumer<Rpc.RangeRequest, StreamObserver<Rpc.RangeResponse>> rangeHandler) {
    this.putHandler = Objects.requireNonNull(putHandler, "putHandler");
    this.txnHandler = Objects.requireNonNull(txnHandler, "txnHandler");
    this.deleteHandler = Objects.requireNonNull(deleteHandler, "deleteHandler");
    this.rangeHandler = Objects.requireNonNull(rangeHandler, "rangeHandler");
    this.compactHandler = null;
  }

  @Override
  public void range(Rpc.RangeRequest request, StreamObserver<Rpc.RangeResponse> responseObserver) {
    if (rangeHandler == null) UnimplementedStreams.failUnary(responseObserver, "KV.Range");
    else rangeHandler.accept(request, responseObserver);
  }

  @Override
  public void put(Rpc.PutRequest request, StreamObserver<Rpc.PutResponse> responseObserver) {
    if (putHandler == null) UnimplementedStreams.failUnary(responseObserver, "KV.Put");
    else putHandler.accept(request, responseObserver);
  }

  @Override
  public void deleteRange(
      Rpc.DeleteRangeRequest request, StreamObserver<Rpc.DeleteRangeResponse> responseObserver) {
    if (deleteHandler == null) UnimplementedStreams.failUnary(responseObserver, "KV.DeleteRange");
    else deleteHandler.accept(request, responseObserver);
  }

  @Override
  public void txn(Rpc.TxnRequest request, StreamObserver<Rpc.TxnResponse> responseObserver) {
    if (txnHandler == null) UnimplementedStreams.failUnary(responseObserver, "KV.Txn");
    else txnHandler.accept(request, responseObserver);
  }

  /** Installs the assembled Compact handler; the logical boundary still comes from its own CAS. */
  public KvService withCompact(
      BiConsumer<Rpc.CompactionRequest, StreamObserver<Rpc.CompactionResponse>> compactHandler) {
    return new KvService(putHandler, txnHandler, deleteHandler, rangeHandler,
        Objects.requireNonNull(compactHandler, "compactHandler"));
  }

  private KvService(
      BiConsumer<Rpc.PutRequest, StreamObserver<Rpc.PutResponse>> putHandler,
      BiConsumer<Rpc.TxnRequest, StreamObserver<Rpc.TxnResponse>> txnHandler,
      BiConsumer<Rpc.DeleteRangeRequest, StreamObserver<Rpc.DeleteRangeResponse>> deleteHandler,
      BiConsumer<Rpc.RangeRequest, StreamObserver<Rpc.RangeResponse>> rangeHandler,
      BiConsumer<Rpc.CompactionRequest, StreamObserver<Rpc.CompactionResponse>> compactHandler) {
    this.putHandler = putHandler;
    this.txnHandler = txnHandler;
    this.deleteHandler = deleteHandler;
    this.rangeHandler = rangeHandler;
    this.compactHandler = compactHandler;
  }

  @Override
  public void compact(
      Rpc.CompactionRequest request, StreamObserver<Rpc.CompactionResponse> responseObserver) {
    if (compactHandler == null) UnimplementedStreams.failUnary(responseObserver, "KV.Compact");
    else compactHandler.accept(request, responseObserver);
  }
}
