package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import etcdserverpb.AuthGrpc;
import etcdserverpb.ClusterGrpc;
import etcdserverpb.KVGrpc;
import etcdserverpb.LeaseGrpc;
import etcdserverpb.MaintenanceGrpc;
import etcdserverpb.Rpc;
import etcdserverpb.WatchGrpc;
import io.etcdhbase.server.config.RuntimeConfigurationException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BootstrapTest {
  private Bootstrap bootstrap;
  private ManagedChannel channel;

  @AfterEach
  void closeResources() throws InterruptedException {
    if (channel != null) {
      channel.shutdownNow();
      channel.awaitTermination(2, TimeUnit.SECONDS);
    }
    if (bootstrap != null) {
      bootstrap.close();
    }
  }

  @Test
  void startsLoopbackPortZeroAndReturnsUnimplementedAcrossEveryService() throws Exception {
    startFromExample();
    Bootstrap.BoundListener listener = bootstrap.boundListeners().getFirst();
    channel =
        ManagedChannelBuilder.forAddress(listener.bindHost(), listener.port())
            .usePlaintext()
            .build();

    assertEquals("local", listener.tableGroupId());
    assertEquals("127.0.0.1", listener.bindHost());
    assertNotEquals(0, listener.port());
    assertUnimplemented(
        () ->
            KVGrpc.newBlockingStub(channel)
                .range(
                    Rpc.RangeRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8("binary-key"))
                        .build()),
        "KV.Range");
    assertUnimplemented(
        () ->
            LeaseGrpc.newBlockingStub(channel)
                .leaseGrant(Rpc.LeaseGrantRequest.getDefaultInstance()),
        "Lease.LeaseGrant");
    assertUnimplemented(
        () ->
            AuthGrpc.newBlockingStub(channel)
                .authStatus(Rpc.AuthStatusRequest.getDefaultInstance()),
        "Auth.AuthStatus");
    assertUnimplemented(
        () ->
            ClusterGrpc.newBlockingStub(channel)
                .memberList(Rpc.MemberListRequest.getDefaultInstance()),
        "Cluster.MemberList");
    assertUnimplemented(
        () ->
            MaintenanceGrpc.newBlockingStub(channel).status(Rpc.StatusRequest.getDefaultInstance()),
        "Maintenance.Status");
    assertWatchAndKeepAliveTerminateAsUnimplemented();
  }

  @Test
  void cancelsBidirectionalStreamAndShutdownReleasesTheListener() throws Exception {
    startFromExample();
    Bootstrap.BoundListener listener = bootstrap.boundListeners().getFirst();
    channel =
        ManagedChannelBuilder.forAddress(listener.bindHost(), listener.port())
            .usePlaintext()
            .build();
    CountDownLatch terminal = new CountDownLatch(1);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    AtomicReference<ClientCallStreamObserver<Rpc.WatchRequest>> requests = new AtomicReference<>();
    WatchGrpc.newStub(channel)
        .watch(
            new ClientResponseObserver<Rpc.WatchRequest, Rpc.WatchResponse>() {
              @Override
              public void beforeStart(ClientCallStreamObserver<Rpc.WatchRequest> requestStream) {
                requests.set(requestStream);
              }

              @Override
              public void onNext(Rpc.WatchResponse value) {
                throw new AssertionError("cancelled stream must not produce data");
              }

              @Override
              public void onError(Throwable failure) {
                streamFailure.set(failure);
                terminal.countDown();
              }

              @Override
              public void onCompleted() {
                terminal.countDown();
              }
            });
    requests.get().cancel("test cancellation", null);
    assertTrue(terminal.await(2, TimeUnit.SECONDS));
    assertEquals(Status.Code.CANCELLED, Status.fromThrowable(streamFailure.get()).getCode());

    int boundPort = listener.port();
    channel.shutdownNow();
    assertTrue(channel.awaitTermination(2, TimeUnit.SECONDS));
    bootstrap.close();
    assertTrue(bootstrap.awaitTermination(Duration.ofSeconds(1)));
    try (Socket socket = new Socket()) {
      assertThrows(
          ConnectException.class,
          () -> socket.connect(new InetSocketAddress("127.0.0.1", boundPort), 1_000));
    }
  }

  @Test
  void rejectsInvalidConfigurationBeforeOpeningAListener() throws IOException {
    Path invalidConfiguration = Files.createTempFile("m0-server-invalid-", ".yaml");
    try {
      Files.writeString(invalidConfiguration, "schema_version: 99\n");
      assertThrows(
          RuntimeConfigurationException.class, () -> Bootstrap.start(invalidConfiguration));
    } finally {
      Files.deleteIfExists(invalidConfiguration);
    }
  }

  @Test
  void foregroundWaitReturnsWhenAnyListenerTerminates() throws Exception {
    Server first =
        NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0)).build().start();
    Server second =
        NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0)).build().start();
    CountDownLatch returned = new CountDownLatch(1);
    AtomicReference<Throwable> waitFailure = new AtomicReference<>();
    Thread waiter =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    Bootstrap.awaitAnyTermination(List.of(first, second));
                  } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    waitFailure.set(failure);
                  } finally {
                    returned.countDown();
                  }
                });
    try {
      second.shutdown();

      assertTrue(returned.await(2, TimeUnit.SECONDS));
      assertNull(waitFailure.get());
      assertFalse(first.isTerminated());
    } finally {
      first.shutdownNow();
      second.shutdownNow();
      waiter.join(TimeUnit.SECONDS.toMillis(2));
      assertTrue(first.awaitTermination(2, TimeUnit.SECONDS));
      assertTrue(second.awaitTermination(2, TimeUnit.SECONDS));
    }
  }

  @Test
  void stopsEarlierListenersWhenALaterConfiguredListenerCannotBind() throws Exception {
    Path example =
        Path.of(BootstrapTest.class.getResource("/m0-examples/server-skeleton.yaml").toURI());
    Path config = Files.createTempFile("m0-server-partial-start-", ".yaml");
    try (ServerSocket firstPort =
            new ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"));
        ServerSocket occupiedPort =
            new ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"))) {
      int releasedPort = firstPort.getLocalPort();
      int blockedPort = occupiedPort.getLocalPort();
      String yaml =
          Files.readString(example)
              .replaceFirst("(?m)^    port: 0$", "    port: " + releasedPort)
              .replace(
                  "    table_group_id: local\nhbase_profiles:",
                  "    table_group_id: local\n"
                      + "  - id: blocked-listener\n"
                      + "    bind_host: 127.0.0.1\n"
                      + "    port: "
                      + blockedPort
                      + "\n"
                      + "    table_group_id: local\nhbase_profiles:");
      Files.writeString(config, yaml);
      firstPort.close();

      assertThrows(IOException.class, () -> Bootstrap.start(config));
      try (ServerSocket rebound = new ServerSocket()) {
        rebound.bind(new InetSocketAddress("127.0.0.1", releasedPort));
      }
    } finally {
      Files.deleteIfExists(config);
    }
  }

  private void startFromExample() throws Exception {
    Path config =
        Path.of(BootstrapTest.class.getResource("/m0-examples/server-skeleton.yaml").toURI());
    bootstrap = Bootstrap.start(config);
  }

  private void assertWatchAndKeepAliveTerminateAsUnimplemented() throws Exception {
    this.<Rpc.WatchRequest, Rpc.WatchResponse>assertStreamUnimplemented(
        observer -> WatchGrpc.newStub(channel).watch(observer),
        Rpc.WatchRequest.getDefaultInstance(),
        "Watch.Watch");
    this.<Rpc.LeaseKeepAliveRequest, Rpc.LeaseKeepAliveResponse>assertStreamUnimplemented(
        observer -> LeaseGrpc.newStub(channel).leaseKeepAlive(observer),
        Rpc.LeaseKeepAliveRequest.getDefaultInstance(),
        "Lease.LeaseKeepAlive");
  }

  private static <RequestT, ResponseT> void assertStreamUnimplemented(
      StreamStarter<RequestT, ResponseT> starter, RequestT request, String method)
      throws InterruptedException {
    CountDownLatch terminal = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    StreamObserver<RequestT> requests =
        starter.start(
            new StreamObserver<>() {
              @Override
              public void onNext(ResponseT value) {
                throw new AssertionError("unimplemented stream must not produce data");
              }

              @Override
              public void onError(Throwable value) {
                failure.set(value);
                terminal.countDown();
              }

              @Override
              public void onCompleted() {
                terminal.countDown();
              }
            });
    requests.onNext(request);
    assertTrue(terminal.await(2, TimeUnit.SECONDS), method);
    assertEquals(Status.Code.UNIMPLEMENTED, Status.fromThrowable(failure.get()).getCode(), method);
    assertEquals(
        "etcd-hbase M0: " + method + " is not implemented",
        Status.fromThrowable(failure.get()).getDescription(),
        method);
  }

  private static void assertUnimplemented(Runnable request, String method) {
    StatusRuntimeException failure =
        assertThrows(StatusRuntimeException.class, request::run, method);
    assertEquals(Status.Code.UNIMPLEMENTED, failure.getStatus().getCode(), method);
    assertEquals(
        "etcd-hbase M0: " + method + " is not implemented",
        failure.getStatus().getDescription(),
        method);
  }

  @FunctionalInterface
  private interface StreamStarter<RequestT, ResponseT> {
    StreamObserver<RequestT> start(StreamObserver<ResponseT> responseObserver);
  }
}
