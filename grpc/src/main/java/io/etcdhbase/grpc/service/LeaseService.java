package io.etcdhbase.grpc.service;

import etcdserverpb.LeaseGrpc;
import etcdserverpb.Rpc;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.function.BiConsumer;

/** Lease RPC surface whose unary handlers are installed by a capable table-group runtime. */
public final class LeaseService extends LeaseGrpc.LeaseImplBase {
  private final BiConsumer<Rpc.LeaseGrantRequest, StreamObserver<Rpc.LeaseGrantResponse>> grant;
  private final BiConsumer<Rpc.LeaseTimeToLiveRequest, StreamObserver<Rpc.LeaseTimeToLiveResponse>> ttl;
  private final BiConsumer<Rpc.LeaseLeasesRequest, StreamObserver<Rpc.LeaseLeasesResponse>> list;

  /** Protocol-only mode retains explicit UNIMPLEMENTED responses. */
  public LeaseService() {
    grant = null;
    ttl = null;
    list = null;
  }

  public LeaseService(
      BiConsumer<Rpc.LeaseGrantRequest, StreamObserver<Rpc.LeaseGrantResponse>> grant,
      BiConsumer<Rpc.LeaseTimeToLiveRequest, StreamObserver<Rpc.LeaseTimeToLiveResponse>> ttl,
      BiConsumer<Rpc.LeaseLeasesRequest, StreamObserver<Rpc.LeaseLeasesResponse>> list) {
    this.grant = Objects.requireNonNull(grant, "grant");
    this.ttl = Objects.requireNonNull(ttl, "ttl");
    this.list = Objects.requireNonNull(list, "list");
  }

  @Override
  public void leaseGrant(
      Rpc.LeaseGrantRequest request, StreamObserver<Rpc.LeaseGrantResponse> responseObserver) {
    if (grant == null) UnimplementedStreams.failUnary(responseObserver, "Lease.LeaseGrant");
    else grant.accept(request, responseObserver);
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
    if (ttl == null) UnimplementedStreams.failUnary(responseObserver, "Lease.LeaseTimeToLive");
    else ttl.accept(request, responseObserver);
  }

  @Override
  public void leaseLeases(
      Rpc.LeaseLeasesRequest request, StreamObserver<Rpc.LeaseLeasesResponse> responseObserver) {
    if (list == null) UnimplementedStreams.failUnary(responseObserver, "Lease.LeaseLeases");
    else list.accept(request, responseObserver);
  }
}
