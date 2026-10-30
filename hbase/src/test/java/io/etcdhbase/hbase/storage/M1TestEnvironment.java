package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.request.RequestScope;
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

/** Disposable real backend shared by M1 fixture callers; does not touch a target environment. */
final class M1TestEnvironment implements AutoCloseable {
  private TemporaryHBaseClusterProcess cluster;
  HBaseBackendIdentityProvider provider;
  AuthorizedBackendIdentity identity;
  AsyncConnection connection;
  ThreadPoolExecutor worker;
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

  void create(StorageTableBinding binding, byte[][] splits) throws Exception {
    await(connection.getAdmin().createTable(binding.descriptor(), splits));
    tables.add(binding.table());
  }

  static <T> T await(CompletableFuture<T> future) throws Exception { return future.get(60, TimeUnit.SECONDS); }

  @Override
  public void close() throws Exception {
    Throwable failure = null;
    if (connection != null) {
      for (TableName table : tables) {
        try { await(connection.getAdmin().disableTable(table)); await(connection.getAdmin().deleteTable(table)); }
        catch (Exception | Error problem) { if (failure == null) failure = problem; else failure.addSuppressed(problem); }
      }
    }
    try {
      if (worker != null) {
        worker.shutdownNow();
        if (!worker.awaitTermination(20, TimeUnit.SECONDS)) throw new IllegalStateException("fixture scan workers did not terminate");
      }
    } catch (Exception | Error problem) { if (failure == null) failure = problem; else failure.addSuppressed(problem); }
    try { if (provider != null) provider.close(); }
    catch (RuntimeException | Error problem) { if (failure == null) failure = problem; else failure.addSuppressed(problem); }
    try { if (cluster != null) cluster.close(); }
    catch (Exception | Error problem) { if (failure == null) failure = problem; else failure.addSuppressed(problem); }
    if (failure instanceof Error fatal) throw fatal;
    if (failure instanceof Exception exception) throw exception;
  }
}
