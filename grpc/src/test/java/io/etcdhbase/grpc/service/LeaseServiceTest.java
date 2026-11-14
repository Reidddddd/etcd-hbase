package io.etcdhbase.grpc.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import etcdserverpb.Rpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class LeaseServiceTest {
  @Test
  void routesOnlyInstalledUnaryOperationsAndLeavesRevokeUnimplemented() {
    var grantRequest = Rpc.LeaseGrantRequest.newBuilder().setID(-7).setTTL(10).build();
    var ttlRequest = Rpc.LeaseTimeToLiveRequest.newBuilder().setID(-7).setKeys(true).build();
    var listRequest = Rpc.LeaseLeasesRequest.getDefaultInstance();
    var grantSeen = new AtomicReference<Rpc.LeaseGrantRequest>();
    var ttlSeen = new AtomicReference<Rpc.LeaseTimeToLiveRequest>();
    var listSeen = new AtomicReference<Rpc.LeaseLeasesRequest>();
    var service = new LeaseService((request, observer) -> grantSeen.set(request),
        (request, observer) -> ttlSeen.set(request), (request, observer) -> listSeen.set(request));
    service.leaseGrant(grantRequest, new Observer<>());
    service.leaseTimeToLive(ttlRequest, new Observer<>());
    service.leaseLeases(listRequest, new Observer<>());
    assertSame(grantRequest, grantSeen.get());
    assertSame(ttlRequest, ttlSeen.get());
    assertSame(listRequest, listSeen.get());

    var revoke = new Observer<Rpc.LeaseRevokeResponse>();
    service.leaseRevoke(Rpc.LeaseRevokeRequest.getDefaultInstance(), revoke);
    assertEquals(Status.Code.UNIMPLEMENTED, Status.fromThrowable(revoke.failure.get()).getCode());
  }

  @Test
  void defaultUnaryHandlersRemainUnimplemented() {
    var service = new LeaseService();
    var grant = new Observer<Rpc.LeaseGrantResponse>();
    var ttl = new Observer<Rpc.LeaseTimeToLiveResponse>();
    var list = new Observer<Rpc.LeaseLeasesResponse>();
    service.leaseGrant(Rpc.LeaseGrantRequest.getDefaultInstance(), grant);
    service.leaseTimeToLive(Rpc.LeaseTimeToLiveRequest.getDefaultInstance(), ttl);
    service.leaseLeases(Rpc.LeaseLeasesRequest.getDefaultInstance(), list);
    assertEquals(Status.Code.UNIMPLEMENTED, Status.fromThrowable(grant.failure.get()).getCode());
    assertEquals(Status.Code.UNIMPLEMENTED, Status.fromThrowable(ttl.failure.get()).getCode());
    assertEquals(Status.Code.UNIMPLEMENTED, Status.fromThrowable(list.failure.get()).getCode());
  }

  private static final class Observer<T> implements StreamObserver<T> {
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    @Override public void onNext(T value) {}
    @Override public void onError(Throwable error) { failure.set(error); }
    @Override public void onCompleted() {}
  }
}
