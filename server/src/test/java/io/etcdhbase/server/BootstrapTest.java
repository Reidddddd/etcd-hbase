package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.grpc.service.AuthService;
import io.etcdhbase.grpc.service.ClusterService;
import io.etcdhbase.grpc.service.KvService;
import io.etcdhbase.grpc.service.LeaseService;
import io.etcdhbase.grpc.service.MaintenanceService;
import io.etcdhbase.grpc.service.WatchService;
import io.etcdhbase.server.observability.RequestMetrics;
import etcdserverpb.AuthGrpc;
import etcdserverpb.Rpc;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BootstrapTest {
  @Test
  void bindsTheAuthenticateHandlerSuppliedByTheTableGroupRuntime() throws Exception {
    var listener = new io.etcdhbase.server.config.RuntimeConfiguration.Listener(
        "j50-auth", "127.0.0.1", 0, "group");
    var resources = new io.etcdhbase.server.config.RuntimeConfiguration.ResourceBudgets(
        65_536, 2, 3_000, 1_048_576, 2_097_152, 4, 0);
    var auth = new AuthService((request, observer) -> {
      observer.onNext(Rpc.AuthenticateResponse.newBuilder().setToken("opaque-token").build());
      observer.onCompleted();
    });
    try (Bootstrap bootstrap = Bootstrap.startConfigured(listener, resources, new KvService(),
        new LeaseService(), new WatchService(), auth, new RequestMetrics())) {
      ManagedChannel channel = ManagedChannelBuilder
          .forAddress("127.0.0.1", bootstrap.boundListener().port())
          .usePlaintext().build();
      try {
        Rpc.AuthenticateResponse response = AuthGrpc.newBlockingStub(channel)
            .withDeadlineAfter(2, TimeUnit.SECONDS)
            .authenticate(Rpc.AuthenticateRequest.newBuilder()
                .setName("milvus-service").setPassword("request-secret").build());
        assertEquals("opaque-token", response.getToken());
      } finally {
        channel.shutdownNow();
        assertTrue(channel.awaitTermination(2, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  void servesAuthenticateHandlerOverConfiguredTls(@TempDir java.nio.file.Path directory)
      throws Exception {
    try (TestTlsCertificate certificate = TestTlsCertificate.createWithDnsSans(
             directory, "localhost", List.of("localhost"))) {
      var listener = new io.etcdhbase.server.config.RuntimeConfiguration.Listener(
          "secure", "127.0.0.1", 0, "secure-group",
          new io.etcdhbase.server.config.RuntimeConfiguration.Tls(
              certificate.certificate().toString(), certificate.privateKey().toString()));
      var resources = new io.etcdhbase.server.config.RuntimeConfiguration.ResourceBudgets(
          65_536, 2, 3_000, 1_048_576, 2_097_152, 4, 0);
      var auth = new AuthService((request, observer) -> {
        observer.onNext(Rpc.AuthenticateResponse.newBuilder().setToken("tls-token").build());
        observer.onCompleted();
      });
      try (Bootstrap bootstrap = Bootstrap.startConfigured(listener, resources, new KvService(),
          new LeaseService(), new WatchService(), auth, new RequestMetrics())) {
        SslContext clientTls = GrpcSslContexts.configure(SslContextBuilder.forClient()
            .trustManager(certificate.certificate().toFile())).build();
        ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1",
                bootstrap.boundListener().port())
            .sslContext(clientTls)
            .overrideAuthority("localhost")
            .build();
        try {
          Rpc.AuthenticateResponse response = AuthGrpc.newBlockingStub(channel)
              .withDeadlineAfter(3, TimeUnit.SECONDS)
              .authenticate(Rpc.AuthenticateRequest.newBuilder()
                  .setName("milvus-service").setPassword("request-secret").build());
          assertEquals("tls-token", response.getToken());
        } finally {
          channel.shutdownNow();
          assertTrue(channel.awaitTermination(2, TimeUnit.SECONDS));
        }
      }
    }
  }

  @Test
  void registersAllFortyOneMethodsAndNeverReturnsBusinessSuccess() throws Exception {
    try (Bootstrap bootstrap =
        Bootstrap.start(new Bootstrap.ListenerBinding("127.0.0.1", 0, "j03-test"))) {
      ManagedChannel channel =
          ManagedChannelBuilder.forAddress("127.0.0.1", bootstrap.boundListener().port())
              .usePlaintext()
              .build();
      try {
        List<ServerServiceDefinition> services =
            List.of(
                new KvService().bindService(),
                new WatchService().bindService(),
                new LeaseService().bindService(),
                new AuthService().bindService(),
                new ClusterService().bindService(),
                new MaintenanceService().bindService());
        int methodCount = 0;
        for (ServerServiceDefinition service : services) {
          for (ServerMethodDefinition<?, ?> method : service.getMethods()) {
            assertUnimplemented(channel, method.getMethodDescriptor());
            methodCount++;
          }
        }
        assertEquals(41, methodCount);
      } finally {
        channel.shutdownNow();
        assertTrue(channel.awaitTermination(2, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  void closesIdempotentlyAndReleasesTheActualPort() throws Exception {
    Bootstrap bootstrap =
        Bootstrap.start(new Bootstrap.ListenerBinding("localhost", 0, "j03-close"));
    int port = bootstrap.boundListener().port();
    try {
      assertTrue(port > 0);
      assertEquals("127.0.0.1", bootstrap.boundListener().bindHost());
      assertEquals("j03-close", bootstrap.boundListener().tableGroupId());
      assertFalse(bootstrap.awaitTermination(Duration.ZERO));
    } finally {
      bootstrap.close();
    }
    bootstrap.close();
    assertTrue(bootstrap.awaitTermination(Duration.ofSeconds(2)));
    try (ServerSocket rebound = new ServerSocket()) {
      rebound.bind(new InetSocketAddress("127.0.0.1", port));
    }
  }

  @Test
  void failsWhenTheRequestedPortIsAlreadyBound() throws IOException {
    try (ServerSocket held = new ServerSocket()) {
      held.bind(new InetSocketAddress("127.0.0.1", 0));
      assertThrows(
          IOException.class,
          () ->
              Bootstrap.start(
                  new Bootstrap.ListenerBinding("127.0.0.1", held.getLocalPort(), "j03-busy")));
    }
  }

  @Test
  void rejectsNonLoopbackBindingsAndInvalidIdentifiersBeforeStarting() {
    for (String host : List.of("0.0.0.0", "::", "example.com", "*", "127.0.0.2")) {
      assertThrows(
          IllegalArgumentException.class,
          () -> new Bootstrap.ListenerBinding(host, 0, "j03-invalid"));
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> new Bootstrap.ListenerBinding(null, 0, "j03-invalid"));
    for (int port : List.of(-1, 65536)) {
      assertThrows(
          IllegalArgumentException.class,
          () -> new Bootstrap.ListenerBinding("127.0.0.1", port, "j03-invalid"));
    }
    for (String id : List.of("", "group\nforged-log", "x".repeat(65))) {
      assertThrows(
          IllegalArgumentException.class,
          () -> new Bootstrap.ListenerBinding("127.0.0.1", 0, id));
    }
  }

  @Test
  void parsesOnlyTheThreeDeclaredOptionsAndRejectsDuplicates() {
    assertEquals(
        new Bootstrap.ListenerBinding("127.0.0.1", 0, "j03-cli"),
        Bootstrap.parseArguments(
            new String[] {"--port", "0", "--table-group", "j03-cli", "--bind-host", "localhost"}));
    assertEquals(2, Bootstrap.run(new String[] {"--config", "later-j06.yaml"}));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            Bootstrap.parseArguments(
                new String[] {"--port", "0", "--port", "1", "--table-group", "j03-cli"}));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            Bootstrap.parseArguments(
                new String[] {
                  "--port", "invalid", "--bind-host", "localhost", "--table-group", "j03-cli"
                }));
  }

  @SuppressWarnings("unchecked")
  private static void assertUnimplemented(
      ManagedChannel channel, MethodDescriptor<?, ?> descriptor) throws Exception {
    MethodDescriptor<Object, Object> method = (MethodDescriptor<Object, Object>) descriptor;
    Object request = method.getRequestMarshaller().parse(new ByteArrayInputStream(new byte[0]));
    CallOptions options = CallOptions.DEFAULT.withDeadlineAfter(2, TimeUnit.SECONDS);
    Throwable failure;
    if (method.getType() == MethodDescriptor.MethodType.UNARY) {
      failure =
          assertThrows(
              StatusRuntimeException.class,
              () -> ClientCalls.blockingUnaryCall(channel, method, options, request));
    } else if (method.getType() == MethodDescriptor.MethodType.SERVER_STREAMING) {
      failure =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  ClientCalls.blockingServerStreamingCall(channel, method, options, request)
                      .hasNext());
    } else {
      CountDownLatch terminal = new CountDownLatch(1);
      AtomicReference<Throwable> error = new AtomicReference<>();
      AtomicInteger responses = new AtomicInteger();
      StreamObserver<Object> requests =
          ClientCalls.asyncBidiStreamingCall(
              channel.newCall(method, options),
              new StreamObserver<>() {
                @Override
                public void onNext(Object response) {
                  responses.incrementAndGet();
                }

                @Override
                public void onError(Throwable cause) {
                  error.set(cause);
                  terminal.countDown();
                }

                @Override
                public void onCompleted() {
                  terminal.countDown();
                }
              });
      requests.onNext(request);
      assertTrue(terminal.await(3, TimeUnit.SECONDS), method.getFullMethodName());
      assertEquals(0, responses.get());
      failure = error.get();
    }
    assertNotNull(failure, method.getFullMethodName());
    Status status = Status.fromThrowable(failure);
    assertEquals(Status.Code.UNIMPLEMENTED, status.getCode(), method.getFullMethodName());
    assertNotNull(status.getDescription());
    assertTrue(
        status.getDescription().startsWith("etcd-hbase M0: "),
        "a generated UNIMPLEMENTED for an unregistered method must not satisfy this test: "
            + method.getFullMethodName());
  }
}
