package io.etcdhbase.server;

import io.etcdhbase.admin.config.InitializationConfiguration.AuthenticationMode;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.M3RuntimeConfigurationLoader;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfigurationLoader;
import io.etcdhbase.server.config.RuntimeConfigurationValidator;
import io.etcdhbase.server.commit.M3TableGroupRuntime;
import io.etcdhbase.server.observability.RequestMetrics;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongFunction;

/** Owns all configured listeners and rolls back earlier binds when startup partially fails. */
public final class RuntimeServer implements AutoCloseable {
  private final List<Bootstrap> listeners;
  private final M3RuntimeGroups groups;
  private final int shutdownTimeoutMs;
  private final AtomicBoolean closed = new AtomicBoolean();

  private RuntimeServer(List<Bootstrap> listeners) {
    this(listeners, null, 0);
  }

  private RuntimeServer(List<Bootstrap> listeners, M3RuntimeGroups groups, int shutdownTimeoutMs) {
    this.listeners = List.copyOf(listeners);
    this.groups = groups;
    this.shutdownTimeoutMs = shutdownTimeoutMs;
  }

  /** Activates the configured local-development groups only after recovery and resource checks. */
  public static RuntimeServer startM3(RuntimeConfiguration configuration,
      M3RuntimeConfiguration activation) throws IOException {
    return startM3Internal(configuration, activation, Optional.empty());
  }

  static RuntimeServer startM3WithLeaseTimeSource(RuntimeConfiguration configuration,
      M3RuntimeConfiguration activation, LongFunction<LeaseTimeSource> leaseTimeSourceFactory)
      throws IOException {
    return startM3Internal(configuration, activation,
        Optional.of(Objects.requireNonNull(leaseTimeSourceFactory, "leaseTimeSourceFactory")));
  }

  private static RuntimeServer startM3Internal(RuntimeConfiguration configuration,
      M3RuntimeConfiguration activation,
      Optional<LongFunction<LeaseTimeSource>> leaseTimeSourceFactory) throws IOException {
    RuntimeConfigurationValidator.validate(configuration, 1);
    requireRuntimeActivation(configuration, true);
    var groups = leaseTimeSourceFactory.isPresent()
        ? M3RuntimeGroups.openWithLeaseTimeSource(
            configuration, activation, leaseTimeSourceFactory.orElseThrow())
        : M3RuntimeGroups.open(configuration, activation);
    List<Bootstrap> opened = new ArrayList<>();
    try {
      for (var listener : configuration.listeners()) {
        opened.add(Bootstrap.startConfigured(listener, configuration.resources(),
            groups.service(listener.tableGroupId()), groups.leaseService(listener.tableGroupId()),
            groups.watchService(listener.tableGroupId()), groups.authService(listener.tableGroupId()),
            groups.metrics(listener.tableGroupId())));
      }
      if (!groups.isReady()) throw new IllegalStateException("M3 table group lost readiness during startup");
      return new RuntimeServer(opened, groups, activation.shutdownTimeoutMs());
    } catch (IOException | RuntimeException | Error failure) {
      try { closeAll(opened); }
      catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
      try { groups.closeAfterRequests(requestsDrained(opened)); }
      catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
      try { groups.drained().get(activation.shutdownTimeoutMs(), TimeUnit.MILLISECONDS); }
      catch (Exception cleanup) {
        if (cleanup instanceof InterruptedException) Thread.currentThread().interrupt();
        if (cleanup != failure) failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  public static RuntimeServer start(RuntimeConfiguration configuration) throws IOException {
    RuntimeConfigurationValidator.validate(configuration, 1);
    requireRuntimeActivation(configuration, false);
    List<Bootstrap> opened = new ArrayList<>();
    try {
      for (var listener : configuration.listeners()) {
        opened.add(Bootstrap.startConfigured(listener, configuration.resources()));
      }
      return new RuntimeServer(opened);
    } catch (IOException | RuntimeException | Error failure) {
      try {
        closeAll(opened);
      } catch (RuntimeException | Error cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  private static void requireRuntimeActivation(RuntimeConfiguration configuration,
      boolean m3Activated) {
    for (var listener : configuration.listeners()) {
      var group =
          configuration.tableGroups().stream()
              .filter(value -> value.id().equals(listener.tableGroupId()))
              .findFirst()
              .orElseThrow();
      var backend =
          configuration.hbaseProfiles().stream()
              .filter(value -> value.id().equals(group.hbaseProfileId()))
              .findFirst()
              .orElseThrow();
      if (backend.authenticationMode() != AuthenticationMode.DEV_NO_AUTH
          && (!m3Activated || configuration.authentication() == null)) {
        throw new IllegalArgumentException(
            "target identity and TLS activation are not configured");
      }
    }
    if (!m3Activated && configuration.authentication() != null) {
      throw new IllegalArgumentException("M5 authentication requires M3 runtime activation");
    }
  }

  public List<Bootstrap.BoundListener> boundListeners() {
    return listeners.stream().map(Bootstrap::boundListener).toList();
  }

  /** Listeners for the same table group share one bounded metric set. */
  public List<RequestMetrics> metrics() {
    return listeners.stream().map(Bootstrap::metrics).toList();
  }

  /** Current local activation state; a bound protocol skeleton alone is never data-ready. */
  public boolean dataReady() { return !closed.get() && groups != null && groups.isReady(); }

  M3TableGroupRuntime tableGroupRuntime(String tableGroupId) {
    if (groups == null) throw new IllegalStateException("M3 runtime is not active");
    return groups.runtime(tableGroupId);
  }

  public void awaitTermination() throws InterruptedException {
    listeners.getFirst().awaitTermination();
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) return;
    Throwable failure = null;
    try { closeAll(listeners); }
    catch (RuntimeException | Error cleanup) { failure = cleanup; }
    if (groups != null) {
      try {
        groups.closeAfterRequests(requestsDrained(listeners));
        groups.drained().get(shutdownTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (Exception | Error cleanup) {
        if (cleanup instanceof InterruptedException) Thread.currentThread().interrupt();
        if (failure == null) failure = cleanup;
        else if (failure != cleanup) failure.addSuppressed(cleanup);
      }
    }
    if (failure instanceof Error fatal) throw fatal;
    if (failure instanceof RuntimeException problem) throw problem;
    if (failure != null) throw new IllegalStateException("runtime cleanup did not complete", failure);
  }

  private static void closeAll(List<Bootstrap> opened) {
    Throwable first = null;
    for (var listener : opened.reversed()) {
      try {
        listener.close();
      } catch (RuntimeException | Error failure) {
        if (first == null) first = failure;
        else first.addSuppressed(failure);
      }
    }
    if (first instanceof Error fatal) throw fatal;
    if (first instanceof RuntimeException failure) throw failure;
  }

  private static java.util.concurrent.CompletableFuture<Void> requestsDrained(List<Bootstrap> listeners) {
    return java.util.concurrent.CompletableFuture.allOf(listeners.stream()
        .map(Bootstrap::requestsDrained).map(java.util.concurrent.CompletionStage::toCompletableFuture)
        .toArray(java.util.concurrent.CompletableFuture[]::new));
  }

  static int run(String[] arguments) {
    return run(arguments, M3RuntimeConfiguration.ActivationProfile.PRODUCTION);
  }

  static int run(String[] arguments, M3RuntimeConfiguration.ActivationProfile activationProfile) {
    if ((arguments.length != 2 && arguments.length != 4) || !arguments[0].equals("--config")
        || arguments.length == 4 && !arguments[2].equals("--m3-config")) {
      System.err.println("usage: runtime-server --config <json-file> [--m3-config <json-file>]");
      return 2;
    }
    try (var input = Files.newInputStream(Path.of(arguments[1]))) {
      var configuration = new RuntimeConfigurationLoader().load(input);
      RuntimeServer activated;
      if (arguments.length == 4) {
        try (var activationInput = Files.newInputStream(Path.of(arguments[3]))) {
          var loader = new M3RuntimeConfigurationLoader(
              Objects.requireNonNull(activationProfile, "activationProfile"));
          activated = startM3(configuration, loader.load(activationInput));
        }
      } else {
        activated = start(configuration);
      }
      try (RuntimeServer server = activated) {
        Thread hook = new Thread(server::close, "runtime-server-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);
        try {
          System.out.println(
              "listeners=" + server.boundListeners().size()
                  + " data_readiness=" + (server.dataReady() ? "READY" : "UNIMPLEMENTED"));
          server.awaitTermination();
          return 0;
        } finally {
          try {
            Runtime.getRuntime().removeShutdownHook(hook);
          } catch (IllegalStateException shuttingDown) {
            /* The shutdown hook owns cleanup. */
          }
        }
      }
    } catch (M3RuntimeConfiguration.CacheActivationUnavailable unavailable) {
      System.err.println("read cache requires verified identity lifecycle and coverage proofs");
      return 2;
    } catch (IllegalArgumentException failure) {
      System.err.println("runtime configuration or activation is invalid");
      return 2;
    } catch (IOException | InterruptedException | RuntimeException failure) {
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      System.err.println("runtime server failed");
      return 1;
    }
  }

  public static void main(String[] arguments) {
    int result = run(arguments);
    if (result != 0) System.exit(result);
  }
}
