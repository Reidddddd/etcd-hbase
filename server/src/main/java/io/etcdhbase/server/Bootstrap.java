package io.etcdhbase.server;

import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.grpc.service.AuthService;
import io.etcdhbase.grpc.service.ClusterService;
import io.etcdhbase.grpc.service.KvService;
import io.etcdhbase.grpc.service.LeaseService;
import io.etcdhbase.grpc.service.MaintenanceService;
import io.etcdhbase.grpc.service.WatchService;
import io.etcdhbase.server.config.ListenerAddress;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.observability.RequestMetrics;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/** Protocol skeleton with bounded request contexts; business handlers remain UNIMPLEMENTED. */
public final class Bootstrap implements AutoCloseable {
  private static final Set<String> OPTIONS = Set.of("--bind-host", "--port", "--table-group");
  private static final String USAGE =
      "usage: j03-server.sh --bind-host <loopback> --port <0..65535> --table-group <id>";

  private final Server server;
  private final java.util.concurrent.ScheduledThreadPoolExecutor deadlines;
  private final RequestScopeInterceptor requests;
  private final RequestMetrics metrics;
  private final BoundListener boundListener;
  private final AtomicBoolean closed = new AtomicBoolean();

  private Bootstrap(
      Server server,
      String host,
      String tableGroup,
      java.util.concurrent.ScheduledThreadPoolExecutor deadlines,
      RequestScopeInterceptor requests,
      RequestMetrics metrics) {
    this.server = server;
    this.boundListener = new BoundListener(host, server.getPort(), tableGroup);
    this.deadlines = deadlines;
    this.requests = requests;
    this.metrics = metrics;
  }

  public static Bootstrap start(ListenerBinding binding) throws IOException {
    Objects.requireNonNull(binding, "binding");
    return start(
        binding.bindHost(),
        binding.port(),
        binding.tableGroupId(),
        new RuntimeConfiguration.ResourceBudgets(65536, 64, 5000, 1048576, 2097152, 16, 0),
        new KvService(), new LeaseService(), new RequestMetrics());
  }

  static Bootstrap startConfigured(
      RuntimeConfiguration.Listener binding, RuntimeConfiguration.ResourceBudgets resources)
      throws IOException {
    return startConfigured(binding, resources, new KvService());
  }

  /** Binds a previously assembled KV service; its backend lifecycle remains with the caller. */
  static Bootstrap startConfigured(
      RuntimeConfiguration.Listener binding, RuntimeConfiguration.ResourceBudgets resources,
      KvService kv) throws IOException {
    return startConfigured(binding, resources, kv, new RequestMetrics());
  }

  static Bootstrap startConfigured(
      RuntimeConfiguration.Listener binding, RuntimeConfiguration.ResourceBudgets resources,
      KvService kv, RequestMetrics metrics) throws IOException {
    return startConfigured(binding, resources, kv, new LeaseService(), metrics);
  }

  /** Binds the Lease surface selected by the activated table-group runtime. */
  static Bootstrap startConfigured(
      RuntimeConfiguration.Listener binding, RuntimeConfiguration.ResourceBudgets resources,
      KvService kv, LeaseService lease, RequestMetrics metrics) throws IOException {
    String host = ListenerAddress.parse(binding.bindHost()).connectionName();
    if (host.equals("localhost")) host = "127.0.0.1";
    return start(host, binding.port(), binding.tableGroupId(), resources, kv, lease, metrics);
  }

  private static Bootstrap start(
      String host, int port, String tableGroup, RuntimeConfiguration.ResourceBudgets resources,
      KvService kv, LeaseService lease, RequestMetrics metrics)
      throws IOException {
    Objects.requireNonNull(kv, "kv");
    Objects.requireNonNull(lease, "lease");
    Objects.requireNonNull(metrics, "metrics");
    var deadlines =
        new java.util.concurrent.ScheduledThreadPoolExecutor(
            1, Thread.ofPlatform().daemon().name("grpc-request-deadline-", 0).factory());
    deadlines.setRemoveOnCancelPolicy(true);
    deadlines.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    var requests =
        new RequestScopeInterceptor(
            deadlines,
            resources.maxConcurrentRequests(),
            Duration.ofMillis(resources.requestDeadlineMs()),
            resources.maxRequestBytes(),
            metrics);
    Server server =
        NettyServerBuilder.forAddress(new InetSocketAddress(host, port))
            .maxInboundMessageSize(resources.maxRpcBytes())
            .intercept(requests)
            .addService(kv)
            .addService(new WatchService())
            .addService(lease)
            .addService(new AuthService())
            .addService(new ClusterService())
            .addService(new MaintenanceService())
            .build();
    try {
      server.start();
      return new Bootstrap(server, host, tableGroup, deadlines, requests, metrics);
    } catch (IOException | RuntimeException | Error failure) {
      server.shutdownNow();
      deadlines.shutdownNow();
      requests.close();
      throw failure;
    }
  }

  public int activeRequests() {
    return requests.activeRequests();
  }

  CompletionStage<Void> requestsDrained() { return requests.drained(); }

  public RequestMetrics metrics() {
    return metrics;
  }

  public BoundListener boundListener() {
    return boundListener;
  }

  public void awaitTermination() throws InterruptedException {
    server.awaitTermination();
  }

  public boolean awaitTermination(Duration timeout) throws InterruptedException {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must not be negative");
    }
    long nanos;
    try {
      nanos = timeout.toNanos();
    } catch (ArithmeticException overflow) {
      nanos = Long.MAX_VALUE;
    }
    return server.awaitTermination(nanos, TimeUnit.NANOSECONDS);
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    server.shutdown();
    try {
      if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
        server.shutdownNow();
        if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
          throw new IllegalStateException("gRPC skeleton did not terminate");
        }
      }
    } catch (InterruptedException interrupted) {
      server.shutdownNow();
      Thread.currentThread().interrupt();
    } finally {
      try {
        requests.close();
      } finally {
        deadlines.shutdownNow();
      }
    }
  }

  static ListenerBinding parseArguments(String[] arguments) {
    if (arguments.length != 6) {
      throw new IllegalArgumentException(USAGE);
    }
    Map<String, String> values = new HashMap<>();
    for (int index = 0; index < arguments.length; index += 2) {
      String option = arguments[index];
      if (!OPTIONS.contains(option) || values.putIfAbsent(option, arguments[index + 1]) != null) {
        throw new IllegalArgumentException(USAGE);
      }
    }
    try {
      return new ListenerBinding(
          values.get("--bind-host"),
          Integer.parseInt(values.get("--port")),
          values.get("--table-group"));
    } catch (NumberFormatException invalidPort) {
      throw new IllegalArgumentException("port must be an integer");
    }
  }

  static int run(String[] arguments) {
    ListenerBinding binding;
    try {
      binding = parseArguments(arguments);
    } catch (IllegalArgumentException invalidArguments) {
      System.err.println(USAGE);
      return 2;
    }
    try (Bootstrap bootstrap = start(binding)) {
      Thread shutdown = new Thread(bootstrap::close, "j03-grpc-shutdown");
      Runtime.getRuntime().addShutdownHook(shutdown);
      try {
        BoundListener listener = bootstrap.boundListener();
        System.out.printf(
            "listener bind=%s port=%d table_group=%s data_readiness=UNIMPLEMENTED%n",
            listener.bindHost(), listener.port(), listener.tableGroupId());
        bootstrap.awaitTermination();
        return 0;
      } finally {
        try {
          Runtime.getRuntime().removeShutdownHook(shutdown);
        } catch (IllegalStateException shuttingDown) {
          // The JVM shutdown hook owns cleanup after shutdown has begun.
        }
      }
    } catch (IOException | RuntimeException | InterruptedException failure) {
      if (failure instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      System.err.println("J03 gRPC skeleton failed: " + failure.getClass().getSimpleName());
      return 1;
    }
  }

  public static void main(String[] arguments) {
    int exitCode = run(arguments);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  /** A declared binding only; it does not assert that a backend table group exists or is ready. */
  public record ListenerBinding(String bindHost, int port, String tableGroupId) {
    public ListenerBinding {
      if (bindHost == null || !Set.of("127.0.0.1", "::1", "localhost").contains(bindHost)) {
        throw new IllegalArgumentException("the J03 skeleton requires a loopback binding");
      }
      if (port < 0 || port > 65535) {
        throw new IllegalArgumentException("port must be between 0 and 65535");
      }
      if (tableGroupId == null || !tableGroupId.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")) {
        throw new IllegalArgumentException("table-group id must be a bounded identifier");
      }
      if ("localhost".equals(bindHost)) {
        bindHost = "127.0.0.1";
      }
    }
  }

  public record BoundListener(String bindHost, int port, String tableGroupId) {}
}
