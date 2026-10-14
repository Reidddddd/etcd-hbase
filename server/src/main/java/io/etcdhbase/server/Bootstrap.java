package io.etcdhbase.server;

import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.grpc.service.AuthService;
import io.etcdhbase.grpc.service.ClusterService;
import io.etcdhbase.grpc.service.KvService;
import io.etcdhbase.grpc.service.LeaseService;
import io.etcdhbase.grpc.service.MaintenanceService;
import io.etcdhbase.grpc.service.WatchService;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration.TableGroup;
import io.etcdhbase.server.config.RuntimeConfigurationLoader;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Starts the M0 gRPC service skeleton from a validated runtime configuration. */
public final class Bootstrap implements AutoCloseable {
  private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);
  private static final String DATA_READINESS = "UNIMPLEMENTED";

  private final List<RunningListener> listeners;
  private final AtomicBoolean closed = new AtomicBoolean();

  private Bootstrap(List<RunningListener> listeners) {
    this.listeners = List.copyOf(listeners);
  }

  /** Loads and validates configuration before opening any listener. */
  public static Bootstrap start(Path configPath) throws IOException {
    RuntimeConfigurationLoader loader =
        new RuntimeConfigurationLoader(
            Set.of(BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID),
            Set.of(HashSaltStrategy.STRATEGY_ID));
    RuntimeConfiguration configuration;
    try (InputStream input = Files.newInputStream(configPath)) {
      configuration = loader.load(input);
    }
    return start(configuration);
  }

  /** Returns the effective address and configured table-group binding for each started listener. */
  public List<BoundListener> boundListeners() {
    return listeners.stream().map(RunningListener::boundListener).toList();
  }

  /** Waits until all listeners terminate or the supplied duration expires. */
  public boolean awaitTermination(Duration timeout) throws InterruptedException {
    if (timeout == null) {
      throw new IllegalArgumentException("timeout is required");
    }
    if (timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must not be negative");
    }
    long timeoutNanos;
    try {
      timeoutNanos = timeout.toNanos();
    } catch (ArithmeticException overflow) {
      timeoutNanos = Long.MAX_VALUE;
    }
    for (RunningListener listener : listeners) {
      long waitStarted = System.nanoTime();
      if (!listener.server().awaitTermination(timeoutNanos, TimeUnit.NANOSECONDS)) {
        return false;
      }
      long elapsed = Math.max(0, System.nanoTime() - waitStarted);
      timeoutNanos = Math.max(0, timeoutNanos - elapsed);
    }
    return true;
  }

  /** Waits for any listener to terminate; used by the foreground process entry point. */
  public void awaitTermination() throws InterruptedException {
    awaitAnyTermination(listeners.stream().map(RunningListener::server).toList());
  }

  static void awaitAnyTermination(List<? extends Server> servers) throws InterruptedException {
    if (servers.isEmpty()) {
      return;
    }
    CountDownLatch terminated = new CountDownLatch(1);
    List<Thread> watchers = new ArrayList<>(servers.size());
    for (Server server : servers) {
      Thread watcher =
          Thread.ofVirtual()
              .name("m0-server-termination-watcher")
              .start(
                  () -> {
                    try {
                      server.awaitTermination();
                    } catch (InterruptedException interrupted) {
                      Thread.currentThread().interrupt();
                    } finally {
                      terminated.countDown();
                    }
                  });
      watchers.add(watcher);
    }
    try {
      terminated.await();
    } catch (InterruptedException interrupted) {
      watchers.forEach(Thread::interrupt);
      throw interrupted;
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    listeners.forEach(listener -> listener.server().shutdown());
    long deadline = System.nanoTime() + SHUTDOWN_TIMEOUT.toNanos();
    boolean interrupted = false;
    for (RunningListener listener : listeners) {
      long remaining = Math.max(0, deadline - System.nanoTime());
      try {
        if (!listener.server().awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
          listener.server().shutdownNow();
        }
      } catch (InterruptedException exception) {
        interrupted = true;
        break;
      }
    }
    if (interrupted) {
      listeners.forEach(listener -> listener.server().shutdownNow());
      Thread.currentThread().interrupt();
    }
  }

  private static Bootstrap start(RuntimeConfiguration configuration) throws IOException {
    List<RunningListener> started = new ArrayList<>();
    RequestMetrics requestMetrics = new RequestMetrics();
    try {
      for (RuntimeConfiguration.Listener listener : configuration.listeners()) {
        TableGroup group =
            configuration.tableGroups().stream()
                .filter(candidate -> candidate.id().equals(listener.tableGroupId()))
                .findFirst()
                .orElseThrow(
                    () -> new IllegalArgumentException("listener table group is unavailable"));
        AuthenticationScope authenticationScope =
            new AuthenticationScope(group.id(), group.hbaseProfileId());
        InetAddress bindAddress = InetAddress.getByName(listener.bindHost());
        Server server =
            NettyServerBuilder.forAddress(new InetSocketAddress(bindAddress, listener.port()))
                .maxInboundMessageSize(Math.toIntExact(configuration.resources().maxRequestBytes()))
                .intercept(new RequestScopeInterceptor(authenticationScope, requestMetrics))
                .addService(new KvService())
                .addService(new WatchService())
                .addService(new LeaseService())
                .addService(new AuthService())
                .addService(new ClusterService())
                .addService(new MaintenanceService())
                .build()
                .start();
        started.add(
            new RunningListener(
                listener.id(),
                listener.bindHost(),
                server.getPort(),
                listener.tableGroupId(),
                server));
      }
      return new Bootstrap(started);
    } catch (IOException | RuntimeException failure) {
      stopStarted(started);
      throw failure;
    }
  }

  private static void stopStarted(List<RunningListener> listeners) {
    listeners.forEach(listener -> listener.server().shutdownNow());
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    for (RunningListener listener : listeners) {
      long remaining = Math.max(0, deadline - System.nanoTime());
      try {
        listener.server().awaitTermination(remaining, TimeUnit.NANOSECONDS);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private static int run(String[] arguments) {
    if (arguments.length != 2 || !"--config".equals(arguments[0])) {
      System.err.println("usage: m0-server.sh --config <runtime-config.yaml>");
      return 2;
    }
    try (Bootstrap bootstrap = start(Path.of(arguments[1]))) {
      for (BoundListener listener : bootstrap.boundListeners()) {
        System.out.printf(
            "listener id=%s bind=%s port=%d table_group=%s data_readiness=%s%n",
            listener.id(),
            listener.bindHost(),
            listener.port(),
            listener.tableGroupId(),
            DATA_READINESS);
      }
      Runtime.getRuntime().addShutdownHook(new Thread(bootstrap::close, "m0-grpc-shutdown"));
      bootstrap.awaitTermination();
      return 0;
    } catch (IOException | RuntimeException | InterruptedException failure) {
      if (failure instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      System.err.println(
          "M0 gRPC skeleton failed: "
              + failure.getClass().getSimpleName()
              + ": "
              + failure.getMessage());
      return 1;
    }
  }

  public static void main(String[] arguments) {
    int exitCode = run(arguments);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  public record BoundListener(String id, String bindHost, int port, String tableGroupId) {}

  private record RunningListener(
      String id, String bindHost, int port, String tableGroupId, Server server) {
    BoundListener boundListener() {
      return new BoundListener(id, bindHost, port, tableGroupId);
    }
  }
}
