package io.etcdhbase.server;

import static io.etcdhbase.spi.identity.BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID;

import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.admin.TableGroupInitializationPlan;
import io.etcdhbase.admin.TableGroupPlanFactory;
import io.etcdhbase.admin.config.DevelopmentHBaseConfiguration;
import io.etcdhbase.admin.config.InitializationConfiguration.AuthenticationMode;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.grpc.service.KvService;
import io.etcdhbase.grpc.service.LeaseService;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.server.commit.M3TableGroupRuntime;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.security.UserGroupInformation;

/** Owns activated local-development group runtimes and their verified HBase connections. */
final class M3RuntimeGroups implements AutoCloseable {
  private static final Executor CLEANUP = task -> Thread.ofVirtual().name("m3-group-cleanup").start(task);
  private final Map<String, ManagedGroup> groups;
  private final ScanScheduler scans;
  private final AtomicBoolean closing = new AtomicBoolean();
  private final CompletableFuture<Void> drained = new CompletableFuture<>();

  private M3RuntimeGroups(Map<String, ManagedGroup> groups, ScanScheduler scans) {
    this.groups = Map.copyOf(groups);
    this.scans = scans;
  }

  static M3RuntimeGroups open(RuntimeConfiguration configuration, M3RuntimeConfiguration activation)
      throws IOException {
    var budgets = M3RuntimeBudgets.create(configuration, activation);
    var scans = new ScanScheduler(budgets, Duration.ofMillis(configuration.resources().requestDeadlineMs()));
    Map<String, ManagedGroup> opened = new LinkedHashMap<>();
    try {
      for (String group : configuration.listeners().stream()
          .map(RuntimeConfiguration.Listener::tableGroupId).distinct().toList()) {
        var source = TableGroupPlanFactory.create(configuration.initialization(), group, null);
        var plan = M3TableGroupPlan.from(source);
        if (source.hbaseProfile().authenticationMode() != AuthenticationMode.DEV_NO_AUTH
            || !source.identityProviderId().equals(DEVELOPMENT_NO_AUTH_PROVIDER_ID)) {
          throw new IllegalArgumentException("configured target identity activation is not available");
        }
        ManagedGroup managed = openGroup(plan, configuration, activation, budgets, scans);
        try {
          opened.put(group, managed);
        } catch (RuntimeException | Error failure) {
          try { managed.runtime().close(); }
          catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
          awaitCleanup(closeAfter(managed.runtime().drained(), managed.provider()),
              activation.shutdownTimeoutMs(), failure);
          throw failure;
        }
      }
      return new M3RuntimeGroups(opened, scans);
    } catch (Exception | Error failure) {
      var rollback = new M3RuntimeGroups(opened, scans);
      try { rollback.close(); }
      catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
      awaitCleanup(rollback.drained, activation.shutdownTimeoutMs(), failure);
      if (failure instanceof Error fatal) throw fatal;
      if (failure instanceof RuntimeException invalid) throw invalid;
      if (failure instanceof IOException io) throw io;
      throw new IOException("M3 table-group activation failed", failure);
    }
  }

  private static ManagedGroup openGroup(M3TableGroupPlan plan, RuntimeConfiguration configuration,
      M3RuntimeConfiguration activation, BudgetManager budgets, ScanScheduler scans) throws Exception {
    var source = plan.source();
    var provider = createProvider(source, activation);
    boolean providerCloseDeferred = false;
    try {
      M3TableGroupRuntime runtime;
      try (var startup = RequestScope.withTimeout(Duration.ofMillis(activation.startupTimeoutMs()))) {
        AuthorizedBackendIdentity identity = authenticate(provider, source, activation);
        runtime = startRuntime(plan, configuration, activation, budgets, scans, provider, identity, startup);
      } catch (M3TableGroupRuntime.StartupException failure) {
        providerCloseDeferred = true;
        awaitCleanup(closeAfter(failure.cleanup(), provider), activation.shutdownTimeoutMs(), failure);
        throw failure;
      } catch (Error fatal) {
        providerCloseDeferred = awaitFatalStartupCleanup(fatal, provider, activation.shutdownTimeoutMs());
        throw fatal;
      }
      try {
        return new ManagedGroup(runtime, provider);
      } catch (RuntimeException | Error failure) {
        providerCloseDeferred = true;
        closeRuntimeAfterDrain(runtime, provider, activation.shutdownTimeoutMs(), failure);
        throw failure;
      }
    } catch (Exception | Error failure) {
      if (!providerCloseDeferred) closeProvider(provider, failure);
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      throw failure;
    }
  }

  private static HBaseBackendIdentityProvider createProvider(
      TableGroupInitializationPlan source, M3RuntimeConfiguration activation) {
    var scope = new AuthenticationScope(source.tableGroupId(), source.hbaseProfile().id());
    var timeout = Duration.ofMillis(Math.min(activation.startupTimeoutMs(), 300_000));
    return new HBaseBackendIdentityProvider(source.identityProviderId(), scope,
        DevelopmentHBaseConfiguration.create(source.hbaseProfile()), HBaseAuthenticators.simpleDevelopment(),
        (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
        1, 1, timeout);
  }

  private static AuthorizedBackendIdentity authenticate(HBaseBackendIdentityProvider provider,
      TableGroupInitializationPlan source, M3RuntimeConfiguration activation) throws Exception {
    var scope = new AuthenticationScope(source.tableGroupId(), source.hbaseProfile().id());
    String principal = UserGroupInformation.getCurrentUser().getShortUserName();
    try (var credentials = new BackendCredentials(principal, new char[0])) {
      return provider.authenticate(credentials, scope).toCompletableFuture()
          .get(activation.startupTimeoutMs(), TimeUnit.MILLISECONDS);
    }
  }

  private static M3TableGroupRuntime startRuntime(M3TableGroupPlan plan,
      RuntimeConfiguration configuration, M3RuntimeConfiguration activation,
      BudgetManager budgets, ScanScheduler scans, HBaseBackendIdentityProvider provider,
      AuthorizedBackendIdentity identity, RequestScope startup) throws Exception {
    return M3TableGroupRuntime.open(plan, activation, configuration.resources(),
        provider, identity, budgets, scans, startup);
  }

  private static boolean awaitFatalStartupCleanup(Error fatal, HBaseBackendIdentityProvider provider,
      int timeoutMs) {
    for (Throwable suppressed : fatal.getSuppressed()) {
      if (suppressed instanceof M3TableGroupRuntime.StartupCleanup cleanup) {
        awaitCleanup(closeAfter(cleanup.cleanup(), provider), timeoutMs, fatal);
        return true;
      }
    }
    return false;
  }

  private static void closeRuntimeAfterDrain(M3TableGroupRuntime runtime,
      HBaseBackendIdentityProvider provider, int timeoutMs, Throwable failure) {
    try { runtime.close(); }
    catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
    awaitCleanup(closeAfter(runtime.drained(), provider), timeoutMs, failure);
  }

  private static void closeProvider(HBaseBackendIdentityProvider provider, Throwable failure) {
    try { provider.close(); }
    catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
  }

  KvService service(String group) {
    ManagedGroup managed = groups.get(group);
    if (managed == null || closing.get()) throw new IllegalArgumentException("table group is not active");
    return managed.runtime().kvService();
  }

  LeaseService leaseService(String group) {
    ManagedGroup managed = groups.get(group);
    if (managed == null || closing.get()) throw new IllegalArgumentException("table group is not active");
    return managed.runtime().leaseService();
  }

  RequestMetrics metrics(String group) {
    ManagedGroup managed = groups.get(group);
    if (managed == null || closing.get()) throw new IllegalArgumentException("table group is not active");
    return managed.runtime().metrics();
  }

  boolean isReady() {
    return !closing.get() && !groups.isEmpty() && groups.values().stream().allMatch(value -> value.runtime().isReady());
  }

  CompletableFuture<Void> drained() { return drained.copy(); }

  @Override public void close() { closeAfterRequests(CompletableFuture.completedFuture(null)); }

  void closeAfterRequests(CompletionStage<Void> requestsDrained) {
    Objects.requireNonNull(requestsDrained, "requestsDrained");
    if (!closing.compareAndSet(false, true)) return;
    List<CompletableFuture<Void>> cleanup = new ArrayList<>();
    for (ManagedGroup group : groups.values()) {
      try {
        group.runtime().close();
      } catch (RuntimeException | Error failure) {
        cleanup.add(CompletableFuture.failedFuture(failure));
      }
    }
    try { scans.close(); }
    catch (RuntimeException | Error failure) { cleanup.add(CompletableFuture.failedFuture(failure)); }
    var scansDrained = scans.termination().toCompletableFuture();
    cleanup.add(scansDrained);
    for (ManagedGroup group : groups.values()) {
      cleanup.add(closeWhenDrained(group.runtime().drained(), scansDrained, requestsDrained,
          group.provider()::close));
    }
    CompletableFuture.allOf(cleanup.toArray(CompletableFuture[]::new)).whenComplete((ignored, failure) -> {
      if (failure == null) drained.complete(null);
      else drained.completeExceptionally(failure);
    });
  }

  static CompletableFuture<Void> closeWhenDrained(CompletionStage<Void> runtime,
      CompletionStage<Void> scans, CompletionStage<Void> requests, Runnable close) {
    return CompletableFuture.allOf(runtime.toCompletableFuture(), scans.toCompletableFuture(),
        requests.toCompletableFuture()).thenRunAsync(close, CLEANUP);
  }

  private static CompletableFuture<Void> closeAfter(CompletionStage<Void> completion,
      HBaseBackendIdentityProvider provider) {
    return completion.toCompletableFuture().thenRunAsync(provider::close, CLEANUP);
  }

  private static void awaitCleanup(CompletableFuture<Void> cleanup, int timeoutMs, Throwable primary) {
    try {
      cleanup.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception failure) {
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      if (failure != primary) primary.addSuppressed(failure);
    }
  }

  private record ManagedGroup(M3TableGroupRuntime runtime, HBaseBackendIdentityProvider provider) {}
}
