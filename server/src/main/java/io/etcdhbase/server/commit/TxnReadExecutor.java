package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.TxnReadSession;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.CompareReadExecutor;
import io.etcdhbase.core.kv.RangeReadCache;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.kv.TxnEvaluator;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Executes a provably read-only transaction on one protected, authoritative control barrier. */
public final class TxnReadExecutor {
  private final String tableGroup;
  private final ReadViewService views;
  private final ManifestCatalog manifests;
  private final HBaseConnectionFactory connections;
  private final StorageTableBinding primary;
  private final Executor backendWorker;
  private final ScanScheduler scans;
  private final TxnExecutionLimits limits;
  private final RequestMetrics metrics;
  private final Optional<RangeReadCache> readCache;

  public TxnReadExecutor(String tableGroup, ReadViewService views, ManifestCatalog manifests,
      HBaseConnectionFactory connections, StorageTableBinding primary, Executor backendWorker,
      ScanScheduler scans, TxnExecutionLimits limits) {
    this(tableGroup, views, manifests, connections, primary, backendWorker, scans, limits, new RequestMetrics());
  }

  public TxnReadExecutor(String tableGroup, ReadViewService views, ManifestCatalog manifests,
      HBaseConnectionFactory connections, StorageTableBinding primary, Executor backendWorker,
      ScanScheduler scans, TxnExecutionLimits limits, RequestMetrics metrics) {
    this(tableGroup, views, manifests, connections, primary, backendWorker, scans, limits, metrics, Optional.empty());
  }

  /** The cache is supplied by an authorized service owner; the normal runtime leaves it absent. */
  public TxnReadExecutor(String tableGroup, ReadViewService views, ManifestCatalog manifests,
      HBaseConnectionFactory connections, StorageTableBinding primary, Executor backendWorker,
      ScanScheduler scans, TxnExecutionLimits limits, RequestMetrics metrics,
      Optional<RangeReadCache> readCache) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.views = Objects.requireNonNull(views, "views");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.connections = Objects.requireNonNull(connections, "connections");
    this.primary = Objects.requireNonNull(primary, "primary");
    this.backendWorker = Objects.requireNonNull(backendWorker, "backendWorker");
    this.scans = Objects.requireNonNull(scans, "scans");
    this.limits = Objects.requireNonNull(limits, "limits");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.readCache = Objects.requireNonNull(readCache, "readCache");
    if (tableGroup.isBlank() || primary.layout() == StorageTableBinding.Layout.CURRENT) {
      throw new IllegalArgumentException("invalid transaction read identity");
    }
  }

  /** Returned output is valid only while its scope remains open; caller owns transport drainage. */
  public CompletableFuture<TxnEvaluator.ValidatedRead> execute(TxnCommand command,
      ByteKey authorizationDigest, AuthorizedBackendIdentity identity,
      RequestScope scope, BudgetManager.Account account) {
    return execute(command, authorizationDigest, identity, scope, account, true, () -> { });
  }

  CompletableFuture<TxnEvaluator.ValidatedRead> execute(TxnCommand command,
      ByteKey authorizationDigest, AuthorizedBackendIdentity identity,
      RequestScope scope, BudgetManager.Account account, boolean recordTxnMetrics,
      Runnable onUserBackendRejection) {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(authorizationDigest, "authorizationDigest");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(onUserBackendRejection, "onUserBackendRejection");
    if (command.mayWrite() || authorizationDigest.length() != 32 || !account.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid read-only transaction"));
    }
    if (!identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
    }
    int buckets = primary.codec().saltProfile().bucketCount();
    if (account.reservation().heads() / buckets < limits.pageBytes()
        || limits.resultBytes() > account.reservation().output()
        || limits.pointBytes() > account.reservation().output() - limits.resultBytes()) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    }
    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) {
      return CompletableFuture.failedFuture(inactive(scope));
    }
    CompletableFuture<TxnEvaluator.ValidatedRead> operation;
    try {
      long started = System.nanoTime();
      var cacheGeneration = readCache.map(RangeReadCache::beginRead);
      operation = UserIdentityRecordStore.open(
              connections, identity, primary, scope, backendWorker, onUserBackendRejection)
          .thenCompose(physical -> TxnReadSession.open(views, List.of(), authorizationDigest,
              physical, manifests, primary.codec()::bucket, buckets, scope))
          .thenCompose(session -> {
            var compares = new CompareReadExecutor(session, scans, account, scope,
                new CompareReadExecutor.Limits(buckets, limits.pageBytes(), limits.physicalRowsPerPage(),
                    limits.pointBytes()));
            var evaluator = new TxnEvaluator(session, compares, account, scope,
                new TxnEvaluator.Limits(limits.pointBytes(), limits.resultBytes(), limits.workingBytes()),
                primary.codec()::bucket);
            return readCache.isPresent()
                ? readCache.orElseThrow().read(command, session, evaluator, account, scope,
                    cacheGeneration.orElseThrow())
                : evaluator.evaluate(command, Optional.empty()).thenCompose(TxnEvaluator.Evaluation::finishReadOnly);
          })
          .whenComplete((ignored, failure) -> {
            if (recordTxnMetrics) metrics.recordTxnStage(
                RequestMetrics.TxnStage.COMPARE_AND_PREPARE, Math.max(0, System.nanoTime() - started));
          });
    } catch (RuntimeException failure) {
      drained.complete(null);
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      drained.complete(null);
      throw fatal;
    }
    operation.whenComplete((ignored, failure) -> drained.complete(null));
    return operation.copy();
  }

  void invalidateAuthorization(ByteKey authorization) {
    readCache.ifPresent(cache -> cache.invalidateAuthorization(authorization));
  }

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }
}
