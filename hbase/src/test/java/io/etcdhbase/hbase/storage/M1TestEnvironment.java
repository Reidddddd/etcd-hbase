package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.TableDescriptor;

/** Disposable real backend shared by storage integration tests; never touches a target environment. */
final class M1TestEnvironment implements AutoCloseable {
  private TemporaryHBaseClusterProcess cluster;
  private HBaseBackendIdentityProvider provider;
  private AuthorizedBackendIdentity identity;
  private AsyncConnection connection;
  private ThreadPoolExecutor worker;
  private final List<TableName> tables = new ArrayList<>();

  static M1TestEnvironment start(int servers) throws Exception {
    var environment = new M1TestEnvironment();
    try { environment.open(servers); return environment; }
    catch (Exception | Error failure) {
      try { environment.close(); } catch (Exception | Error cleanup) { failure.addSuppressed(cleanup); }
      throw failure;
    }
  }

  private void open(int servers) throws Exception {
    cluster = TemporaryHBaseClusterProcess.start(servers);
    worker = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(64));
    var scope = new AuthenticationScope("m1", "public");
    provider = new HBaseBackendIdentityProvider("m1-fixture", scope, cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
        (client, ignored) -> client.getAdmin().listNamespaces().thenApply(names -> null), 2, 2, Duration.ofSeconds(30));
    try (var credentials = new BackendCredentials("m1-fixture", new char[0])) {
      identity = await(provider.authenticate(credentials, scope).toCompletableFuture());
    }
    connection = await(provider.connectionFor(identity).toCompletableFuture());
  }

  HBaseRecordStore store(StorageTableBinding binding, RequestScope scope) throws Exception {
    return await(HBaseRecordStore.open(provider, identity, binding, scope, worker));
  }

  HBaseConnectionFactory factory() { return provider; }

  AuthorizedBackendIdentity identity() { return identity; }

  org.apache.hadoop.conf.Configuration clientConfiguration() {
    return cluster.clientConfiguration();
  }

  /** Returns the provider-owned connection for fixture callers that borrow it. */
  AsyncConnection connection() { return connection; }

  java.util.Map<String, Long> activeStoreFiles(StorageTableBinding binding) throws Exception {
    return cluster.activeStoreFiles(binding.table(), binding.family());
  }

  void create(StorageTableBinding binding, byte[][] splits) throws Exception {
    create(binding.table(), binding.descriptor(), splits);
  }

  void create(TableName table, TableDescriptor descriptor, byte[][] splits) throws Exception {
    await(connection.getAdmin().createTable(descriptor, splits));
    tables.add(table);
  }

  static <T> T await(CompletableFuture<T> future) throws Exception { return future.get(60, TimeUnit.SECONDS); }

  @Override
  public void close() throws Exception {
    CleanupFailures failures = new CleanupFailures();
    if (connection != null) {
      for (TableName table : tables) {
        try {
          await(connection.getAdmin().disableTable(table));
          await(connection.getAdmin().deleteTable(table));
        } catch (Exception | Error problem) {
          failures.add(problem);
        }
      }
    }
    try {
      if (worker != null) {
        worker.shutdownNow();
        if (!worker.awaitTermination(20, TimeUnit.SECONDS)) {
          throw new IllegalStateException("fixture scan workers did not terminate");
        }
      }
    } catch (Exception | Error problem) {
      failures.add(problem);
    }
    try {
      if (provider != null) provider.close();
    } catch (RuntimeException | Error problem) {
      failures.add(problem);
    }
    try {
      if (cluster != null) cluster.close();
    } catch (Exception | Error problem) {
      failures.add(problem);
    }
    failures.throwIfPresent();
  }

  private static final class CleanupFailures {
    private Throwable primary;

    void add(Throwable failure) {
      if (primary == null) {
        primary = failure;
      } else {
        primary.addSuppressed(failure);
      }
    }

    void throwIfPresent() throws Exception {
      if (primary instanceof Error fatal) throw fatal;
      if (primary instanceof Exception exception) throw exception;
    }
  }
}
