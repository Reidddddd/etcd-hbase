package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.server.fixture.FailureFixture;
import io.etcdhbase.server.fixture.WatchCancellationFixture;
import io.grpc.CallOptions;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class GoInteropIT {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Duration DRIVER_TIMEOUT = Duration.ofSeconds(30);
  private static final long EXPECTED_REVISION = 1L << 54;
  private static final MethodDescriptor.Marshaller<byte[]> RAW_BYTES =
      new MethodDescriptor.Marshaller<>() {
        @Override
        public InputStream stream(byte[] value) {
          return new ByteArrayInputStream(value);
        }

        @Override
        public byte[] parse(InputStream stream) {
          try {
            return stream.readAllBytes();
          } catch (IOException failure) {
            throw new UncheckedIOException(failure);
          }
        }
      };

  @Test
  void goClientsReachJavaServicesAndObserveBoundedErrorsAndStreams() throws Exception {
    Path repositoryRoot = findRepositoryRoot();
    Path cases = resolveCases(repositoryRoot);
    verifyWireManifest(cases);

    Bootstrap bootstrap = null;
    Server fixtureServer = null;
    Process driverProcess = null;
    try {
      Path config =
          Path.of(GoInteropIT.class.getResource("/m0-examples/server-skeleton.yaml").toURI());
      bootstrap = Bootstrap.start(config);
      Bootstrap.BoundListener skeleton = bootstrap.boundListeners().getFirst();

      FailureFixture fixture = new FailureFixture();
      WatchCancellationFixture watchFixture = new WatchCancellationFixture();
      fixtureServer =
          NettyServerBuilder.forAddress(
                  new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0))
              .intercept(new RequestScopeInterceptor())
              .addService(fixture)
              .addService(watchFixture)
              .build()
              .start();

      Path artifactDirectory = resolveArtifactDirectory(repositoryRoot);
      Files.createDirectories(artifactDirectory);
      Path trace = resolveTracePath(artifactDirectory);
      Path driverLog = artifactDirectory.resolve("m0driver.log");
      Path driver = resolveDriver(repositoryRoot, artifactDirectory);
      driverProcess =
          new ProcessBuilder(
                  driver.toString(),
                  "wire",
                  "--skeleton-endpoint",
                  skeleton.bindHost() + ":" + skeleton.port(),
                  "--fixture-endpoint",
                  "127.0.0.1:" + fixtureServer.getPort(),
                  "--trace",
                  trace.toString(),
                  "--timeout",
                  "3s")
              .directory(repositoryRoot.toFile())
              .redirectErrorStream(true)
              .redirectOutput(driverLog.toFile())
              .start();

      assertTrue(
          driverProcess.waitFor(DRIVER_TIMEOUT.toSeconds(), TimeUnit.SECONDS),
          "Go driver exceeded its outer process deadline");
      assertFalse(driverProcess.isAlive(), "Go driver process remained alive after completion");
      assertEquals(0, driverProcess.exitValue(), readOutput(driverLog));
      assertTrue(
          fixture.awaitDeadlineScopeCleanup(Duration.ofSeconds(2)),
          "caller deadline did not cancel and clean the Java request scope");
      assertTrue(
          watchFixture.awaitRequestReceived(Duration.ofSeconds(2)),
          "Java did not receive the bounded Watch cancellation request");
      assertTrue(watchFixture.requestWasValid(), "Java received the wrong Watch request fields");
      assertTrue(
          watchFixture.awaitScopeClosed(Duration.ofSeconds(2)),
          "caller deadline did not close the Watch request scope");
      assertTrue(
          fixture.rawUnavailableRequestCount() >= 1,
          "raw apiv3 UNAVAILABLE request did not reach the fixture");
      assertTrue(
          fixture.clientV3RetryRequestCount() >= 2,
          "clientv3 did not retry its UNAVAILABLE request before the caller deadline");
      assertWireTrace(trace);
    } finally {
      stopProcess(driverProcess);
      if (fixtureServer != null) {
        fixtureServer.shutdownNow();
        assertTrue(fixtureServer.awaitTermination(2, TimeUnit.SECONDS));
      }
      if (bootstrap != null) {
        bootstrap.close();
      }
    }
  }

  @Test
  void rejectsEveryUnsupportedExternalLockAndElectionMethod() throws Exception {
    Path config =
        Path.of(GoInteropIT.class.getResource("/m0-examples/server-skeleton.yaml").toURI());
    Bootstrap bootstrap = Bootstrap.start(config);
    try {
      Bootstrap.BoundListener listener = bootstrap.boundListeners().getFirst();
      var channel =
          ManagedChannelBuilder.forAddress(listener.bindHost(), listener.port())
              .usePlaintext()
              .build();
      try {
        for (String method :
            List.of(
                "v3lockpb.Lock/Lock",
                "v3lockpb.Lock/Unlock",
                "v3electionpb.Election/Campaign",
                "v3electionpb.Election/Proclaim",
                "v3electionpb.Election/Leader",
                "v3electionpb.Election/Resign")) {
          MethodDescriptor<byte[], byte[]> descriptor =
              rawMethod(method, MethodDescriptor.MethodType.UNARY);
          StatusRuntimeException failure =
              assertThrows(
                  StatusRuntimeException.class,
                  () ->
                      ClientCalls.blockingUnaryCall(
                          channel, descriptor, CallOptions.DEFAULT, new byte[0]),
                  method);
          assertEquals(Status.Code.UNIMPLEMENTED, failure.getStatus().getCode(), method);
        }

        MethodDescriptor<byte[], byte[]> observe =
            rawMethod("v3electionpb.Election/Observe", MethodDescriptor.MethodType.BIDI_STREAMING);
        CountDownLatch terminal = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<byte[]> response = new AtomicReference<>();
        ClientCalls.asyncBidiStreamingCall(
            channel.newCall(observe, CallOptions.DEFAULT),
            new StreamObserver<>() {
              @Override
              public void onNext(byte[] value) {
                response.set(value);
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
        assertTrue(terminal.await(2, TimeUnit.SECONDS), "Election.Observe did not terminate");
        assertNull(response.get(), "Election.Observe unexpectedly produced a response");
        assertTrue(failure.get() instanceof StatusRuntimeException, "Election.Observe did not fail");
        assertEquals(
            Status.Code.UNIMPLEMENTED,
            Status.fromThrowable(failure.get()).getCode(),
            "Election.Observe");
      } finally {
        channel.shutdownNow();
        assertTrue(channel.awaitTermination(2, TimeUnit.SECONDS));
      }
    } finally {
      bootstrap.close();
    }
  }

  private static MethodDescriptor<byte[], byte[]> rawMethod(
      String fullMethodName, MethodDescriptor.MethodType methodType) {
    return MethodDescriptor.<byte[], byte[]>newBuilder()
        .setType(methodType)
        .setFullMethodName(fullMethodName)
        .setRequestMarshaller(RAW_BYTES)
        .setResponseMarshaller(RAW_BYTES)
        .build();
  }

  private static void assertWireTrace(Path tracePath) throws IOException {
    List<JsonNode> records = new ArrayList<>();
    String serializedTrace;
    try (var lines = Files.lines(tracePath)) {
      serializedTrace = String.join("\n", lines.toList());
    }
    for (String line : serializedTrace.lines().toList()) {
      if (!line.isBlank()) {
        records.add(JSON.readTree(line));
      }
    }

    Map<String, JsonNode> byCase = new HashMap<>();
    for (JsonNode record : records) {
      byCase.put(record.path("case_id").asText(), record);
    }
    assertEquals(10, records.size(), "wire probe omitted an expected request/stream case");

    assertRecord(byCase, "wire-skeleton-clientv3", "Unimplemented", "transport");
    assertRecord(byCase, "wire-skeleton-raw-range", "Unimplemented", "transport");
    assertRecord(byCase, "wire-skeleton-watch", "Unimplemented", "transport");
    assertRecord(byCase, "wire-skeleton-lease-keepalive", "Unimplemented", "transport");
    assertRecord(byCase, "wire-fixture-watch-cancel", "DeadlineExceeded", "transport-fixture");
    assertRecord(byCase, "wire-fixture-clientv3-auth", "InvalidArgument", "transport-fixture");
    assertRecord(byCase, "wire-fixture-raw-permission", "PermissionDenied", "transport-fixture");
    assertRecord(byCase, "wire-fixture-raw-unavailable", "Unavailable", "transport-fixture");
    assertRecord(
        byCase, "wire-fixture-clientv3-retry-deadline", "DeadlineExceeded", "transport-fixture");
    assertRecord(
        byCase, "wire-fixture-raw-deadline-cancel", "DeadlineExceeded", "transport-fixture");

    for (String caseId :
        List.of(
            "wire-skeleton-clientv3",
            "wire-skeleton-raw-range",
            "wire-fixture-clientv3-auth",
            "wire-fixture-raw-permission",
            "wire-fixture-raw-unavailable",
            "wire-fixture-clientv3-retry-deadline",
            "wire-fixture-raw-deadline-cancel")) {
      JsonNode fields = byCase.get(caseId).path("request").path("fields");
      assertEquals("18014398509481984", fields.path("revision").asText(), caseId);
      assertTrue(fields.path("key").isTextual(), caseId + " did not preserve the byte key");
      assertEquals("etcdserverpb.KV/Range", byCase.get(caseId).path("method").asText(), caseId);
    }

    JsonNode watch = byCase.get("wire-skeleton-watch");
    JsonNode watchRequest =
        watch.path("request").path("fields").path("createRequest").path("fields");
    assertEquals("etcdserverpb.Watch/Watch", watch.path("method").asText());
    assertEquals("AAD/", watchRequest.path("key").asText());
    assertEquals(Long.toString(EXPECTED_REVISION), watchRequest.path("startRevision").asText());
    assertEquals("grpc-status", watch.path("stream_terminal").path("kind").asText());
    assertEquals("Unimplemented", watch.path("stream_terminal").path("grpc_code").asText());
    JsonNode keepAlive = byCase.get("wire-skeleton-lease-keepalive");
    assertEquals("etcdserverpb.Lease/LeaseKeepAlive", keepAlive.path("method").asText());
    assertEquals(
        Long.toString(EXPECTED_REVISION),
        keepAlive.path("request").path("fields").path("ID").asText());
    assertEquals("grpc-status", keepAlive.path("stream_terminal").path("kind").asText());
    assertEquals("Unimplemented", keepAlive.path("stream_terminal").path("grpc_code").asText());
    JsonNode cancelledWatch = byCase.get("wire-fixture-watch-cancel");
    JsonNode cancelledWatchRequest =
        cancelledWatch.path("request").path("fields").path("createRequest").path("fields");
    assertEquals("etcdserverpb.Watch/Watch", cancelledWatch.path("method").asText());
    assertEquals("AAD/", cancelledWatchRequest.path("key").asText());
    assertEquals(
        Long.toString(EXPECTED_REVISION), cancelledWatchRequest.path("startRevision").asText());
    assertEquals("context-deadline", cancelledWatch.path("stream_terminal").path("kind").asText());
    assertFalse(
        serializedTrace.contains("password")
            || serializedTrace.contains("token")
            || serializedTrace.contains("user-secret"),
        "wire trace contains credential-like content");
  }

  private static void assertRecord(
      Map<String, JsonNode> records, String caseId, String grpcCode, String scope) {
    JsonNode record = records.get(caseId);
    assertTrue(record != null, "missing trace case " + caseId);
    assertEquals(grpcCode, record.path("error").path("grpc_code").asText(), caseId);
    assertEquals(scope, record.path("evidence_scope").asText(), caseId);
  }

  private static void verifyWireManifest(Path cases) throws IOException {
    JsonNode root = JSON.readTree(cases.toFile());
    assertEquals("etcd-v3.5.23", root.path("protocol").asText());
    JsonNode methods = root.path("descriptor_methods");
    assertMethod(methods, "etcdserverpb.KV/Range", false, false);
    assertMethod(methods, "etcdserverpb.Watch/Watch", true, true);
    assertMethod(methods, "etcdserverpb.Lease/LeaseKeepAlive", true, true);
  }

  private static void assertMethod(
      JsonNode methods, String methodName, boolean clientStreaming, boolean serverStreaming) {
    for (JsonNode method : methods) {
      if (methodName.equals(method.path("method").asText())) {
        assertEquals(clientStreaming, method.path("client_streaming").asBoolean(), methodName);
        assertEquals(serverStreaming, method.path("server_streaming").asBoolean(), methodName);
        assertEquals("UNIMPLEMENTED", method.path("m0_behavior").asText(), methodName);
        return;
      }
    }
    throw new AssertionError("wire case manifest has no method " + methodName);
  }

  private static Path resolveDriver(Path repositoryRoot, Path artifactDirectory)
      throws IOException, InterruptedException {
    String configuredDriver = System.getenv("M0_WIRE_DRIVER");
    if (configuredDriver != null && !configuredDriver.isBlank()) {
      Path driver = Path.of(configuredDriver).toAbsolutePath().normalize();
      if (!Files.isExecutable(driver)) {
        throw new IOException("M0_WIRE_DRIVER is not executable");
      }
      return driver;
    }

    Path driver = artifactDirectory.resolve("m0driver");
    Path buildLog = artifactDirectory.resolve("m0driver-build.log");
    Process build =
        new ProcessBuilder(
                repositoryRoot.resolve("scripts/dev/go-m0.sh").toString(),
                "-C",
                "testkit/go",
                "build",
                "-trimpath",
                "-o",
                driver.toString(),
                "./cmd/m0driver")
            .directory(repositoryRoot.toFile())
            .redirectErrorStream(true)
            .redirectOutput(buildLog.toFile())
            .start();
    if (!build.waitFor(5, TimeUnit.MINUTES)) {
      stopProcess(build);
      throw new IOException("locked Go driver build exceeded five minutes");
    }
    if (build.exitValue() != 0 || !Files.isExecutable(driver)) {
      throw new IOException("locked Go driver build failed: " + readOutput(buildLog));
    }
    return driver;
  }

  private static Path resolveCases(Path repositoryRoot) {
    String configured = System.getenv("M0_WIRE_CASES");
    Path cases =
        configured == null || configured.isBlank()
            ? repositoryRoot.resolve("testkit/cases/m0-wire.json")
            : Path.of(configured);
    return cases.toAbsolutePath().normalize();
  }

  private static Path resolveArtifactDirectory(Path repositoryRoot) {
    String configured = System.getenv("M0_WIRE_ARTIFACT_ROOT");
    if (configured != null && !configured.isBlank()) {
      return Path.of(configured).toAbsolutePath().normalize();
    }
    return repositoryRoot
        .resolve("server/target/m0-wire/")
        .resolve(Long.toString(System.currentTimeMillis()) + "-" + UUID.randomUUID());
  }

  private static Path resolveTracePath(Path artifactDirectory) {
    String configured = System.getenv("M0_WIRE_TRACE");
    if (configured != null && !configured.isBlank()) {
      return Path.of(configured).toAbsolutePath().normalize();
    }
    return artifactDirectory.resolve("transport.jsonl");
  }

  private static Path findRepositoryRoot() throws IOException {
    Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
    while (current != null) {
      if (Files.isRegularFile(current.resolve("scripts/dev/m0-server.sh"))) {
        return current;
      }
      current = current.getParent();
    }
    throw new IOException("could not locate the etcd-hbase repository root");
  }

  private static String readOutput(Path output) throws IOException {
    return Files.exists(output) ? Files.readString(output) : "process output was not captured";
  }

  private static void stopProcess(Process process) throws InterruptedException {
    if (process == null || !process.isAlive()) {
      return;
    }
    process.destroy();
    if (!process.waitFor(1, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      process.waitFor(2, TimeUnit.SECONDS);
    }
  }
}
