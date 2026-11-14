package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestRepository;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.TxnReadSession;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.kv.CompareReadExecutor;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.kv.TxnEvaluator;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.codec.PhysicalRowLimitException;
import io.etcdhbase.hbase.storage.HBaseRecordStore;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/** Executes an owner-bound transaction against one fixed base and its private workset. */
public final class TxnMutationExecutor {
  private static final BudgetManager.Work FINALIZATION_CAPACITY =
      new BudgetManager.Work(0, 1, 0, 1, 0);
  private final String tableGroup;
  private final ByteKey profileFingerprint;
  private final CommitControlStore control;
  private final ReadViewService views;
  private final ManifestCatalog manifests;
  private final SystemRecordStore worksetRecords;
  private final ManifestRepository worksetManifests;
  private final HBaseConnectionFactory connections;
  private final StorageTableBinding primary;
  private final Executor backendWorker;
  private final ScanScheduler scans;
  private final TxnCommitPreparation preparation;
  private final CommitPublisher publisher;
  private final TxnExecutionLimits limits;
  private final LeaseBindingMutationPlanner leaseBindings;
  private final Clock clock;
  private final RequestMetrics metrics;

  public TxnMutationExecutor(String tableGroup, ByteKey profileFingerprint,
      CommitControlStore control, ReadViewService views, ManifestCatalog manifests,
      SystemRecordStore worksetRecords, ManifestRepository worksetManifests,
      HBaseConnectionFactory connections, StorageTableBinding primary, Executor backendWorker,
      ScanScheduler scans, TxnCommitPreparation preparation, CommitPublisher publisher, TxnExecutionLimits limits) {
    this(tableGroup, profileFingerprint, control, views, manifests, worksetRecords, worksetManifests, connections, primary,
        backendWorker, scans, preparation, publisher, limits, Clock.systemUTC(), new RequestMetrics(), null);
  }

  public TxnMutationExecutor(String tableGroup, ByteKey profileFingerprint,
      CommitControlStore control, ReadViewService views, ManifestCatalog manifests,
      SystemRecordStore worksetRecords, ManifestRepository worksetManifests,
      HBaseConnectionFactory connections, StorageTableBinding primary, Executor backendWorker,
      ScanScheduler scans, TxnCommitPreparation preparation, CommitPublisher publisher,
      TxnExecutionLimits limits, RequestMetrics metrics) {
    this(tableGroup, profileFingerprint, control, views, manifests, worksetRecords, worksetManifests, connections, primary,
        backendWorker, scans, preparation, publisher, limits, Clock.systemUTC(), metrics, null);
  }

  public TxnMutationExecutor(String tableGroup, ByteKey profileFingerprint,
      CommitControlStore control, ReadViewService views, ManifestCatalog manifests,
      SystemRecordStore worksetRecords, ManifestRepository worksetManifests,
      HBaseConnectionFactory connections, StorageTableBinding primary, Executor backendWorker,
      ScanScheduler scans, TxnCommitPreparation preparation, CommitPublisher publisher,
      TxnExecutionLimits limits, LeaseBindingMutationPlanner leaseBindings,
      RequestMetrics metrics) {
    this(tableGroup, profileFingerprint, control, views, manifests, worksetRecords,
        worksetManifests, connections, primary, backendWorker, scans, preparation,
        publisher, limits, Clock.systemUTC(), metrics,
        Objects.requireNonNull(leaseBindings, "leaseBindings"));
  }

  TxnMutationExecutor(String tableGroup, ByteKey profileFingerprint,
      CommitControlStore control, ReadViewService views, ManifestCatalog manifests,
      SystemRecordStore worksetRecords, ManifestRepository worksetManifests,
      HBaseConnectionFactory connections, StorageTableBinding primary, Executor backendWorker,
      ScanScheduler scans, TxnCommitPreparation preparation, CommitPublisher publisher,
      TxnExecutionLimits limits, Clock clock, RequestMetrics metrics) {
    this(tableGroup, profileFingerprint, control, views, manifests, worksetRecords,
        worksetManifests, connections, primary, backendWorker, scans, preparation,
        publisher, limits, clock, metrics, null);
  }

  private TxnMutationExecutor(String tableGroup, ByteKey profileFingerprint,
      CommitControlStore control, ReadViewService views, ManifestCatalog manifests,
      SystemRecordStore worksetRecords, ManifestRepository worksetManifests,
      HBaseConnectionFactory connections, StorageTableBinding primary, Executor backendWorker,
      ScanScheduler scans, TxnCommitPreparation preparation, CommitPublisher publisher,
      TxnExecutionLimits limits, Clock clock, RequestMetrics metrics,
      LeaseBindingMutationPlanner leaseBindings) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.profileFingerprint = Objects.requireNonNull(profileFingerprint, "profileFingerprint");
    this.control = Objects.requireNonNull(control, "control");
    this.views = Objects.requireNonNull(views, "views");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.worksetRecords = Objects.requireNonNull(worksetRecords, "worksetRecords");
    this.worksetManifests = Objects.requireNonNull(worksetManifests, "worksetManifests");
    this.connections = Objects.requireNonNull(connections, "connections");
    this.primary = Objects.requireNonNull(primary, "primary");
    this.backendWorker = Objects.requireNonNull(backendWorker, "backendWorker");
    this.scans = Objects.requireNonNull(scans, "scans");
    this.preparation = Objects.requireNonNull(preparation, "preparation");
    this.publisher = Objects.requireNonNull(publisher, "publisher");
    this.limits = Objects.requireNonNull(limits, "limits");
    this.leaseBindings = leaseBindings;
    this.clock = Objects.requireNonNull(clock, "clock");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    if (tableGroup.isBlank() || profileFingerprint.length() != 32
        || primary.layout() == StorageTableBinding.Layout.CURRENT) {
      throw new IllegalArgumentException("invalid transaction execution identity");
    }
  }

  /** Caller serializes new attempts, verifies forwarding authorization and closes the service scope. */
  public CompletableFuture<InvocationId> executeNew(TxnCommand command, InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, AuthorizedBackendIdentity identity,
      RequestScope scope, BudgetManager.Account account) {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(binding, "binding");
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    if (!command.mayWrite() || binding.state() != InvocationBinding.State.PREPARING
        || binding.preparedOwnerEpoch() != owner.epoch() || !account.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid new transaction attempt"));
    }
    if (!identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
    }
    int buckets = primary.codec().saltProfile().bucketCount();
    if (account.reservation().heads() / buckets < limits.pageBytes()
        || limits.resultBytes() > account.reservation().output()
        || limits.pointBytes() > account.reservation().output() - limits.resultBytes()
        || limits.workingBytes() > account.reservation().candidates()) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    }
    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) {
      return CompletableFuture.failedFuture(inactive(scope));
    }
    CompletableFuture<InvocationId> operation;
    try {
      var finalization = account.tryAcquire(FINALIZATION_CAPACITY)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      PutCommitPreparation.retainUntilDrain(scope, finalization);
      long started = System.nanoTime();
      operation = control.read(scope).thenCompose(observed -> {
        requireCurrentBase(observed.orElseThrow(() ->
            new IllegalStateException("authoritative commit control state is missing")).state(), binding, owner);
        return HBaseRecordStore.open(connections, identity, primary, scope, backendWorker);
      }).thenCompose(physical -> leaseBindings == null
          ? TxnReadSession.open(views, List.of(), binding.authorizationScopeDigest(),
              physical, manifests, primary.codec()::bucket, buckets, scope)
          : TxnReadSession.openAuxiliary(views, List.of(), binding.authorizationScopeDigest(),
              physical, manifests, primary.codec()::bucket, buckets, scope))
          .thenCompose(session -> {
            var compares = new CompareReadExecutor(session, scans, account, scope,
                new CompareReadExecutor.Limits(buckets, limits.pageBytes(), limits.physicalRowsPerPage(),
                    limits.pointBytes()));
            LeaseBindingMutationPlanner.MutationView leaseView = leaseBindings == null ? null
                : leaseBindings.open(session.currentAuxiliaryView(), scope, account);
            var evaluator = new TxnEvaluator(session, compares, account, scope,
                new TxnEvaluator.Limits(limits.pointBytes(), limits.resultBytes(), limits.workingBytes()),
                primary.codec()::bucket, worksetRecords, worksetManifests, leaseView);
            return evaluator.evaluate(command, Optional.of(binding)).thenCompose(evaluated ->
                leaseView == null
                    ? preparation.prepare(evaluated, binding, scope, account)
                    : leaseView.prepare(binding, evaluated.leaseBindingChanges()).thenCompose(auxiliary ->
                        preparation.prepare(evaluated, binding, scope, account, auxiliary)));
          })
          .exceptionallyCompose(failure -> {
            Throwable cause = CompletionFailures.unwrapCompletion(failure);
            return CompletableFuture.failedFuture(cause instanceof PhysicalRowLimitException
                ? new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT, cause) : failure);
          })
          .whenComplete((ignored, failure) -> metrics.recordTxnStage(
              RequestMetrics.TxnStage.COMPARE_AND_PREPARE, Math.max(0, System.nanoTime() - started)))
          .thenCompose(ready -> finish(binding, owner, ready, scope));
    } catch (BackendFailure | RuntimeException failure) {
      drained.complete(null);
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      drained.complete(null);
      throw fatal;
    }
    operation.whenComplete((ignored, failure) -> drained.complete(null));
    return operation.copy();
  }

  private CompletableFuture<InvocationId> finish(InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, TxnCommitPreparation.Prepared ready, RequestScope scope) {
    if (ready instanceof TxnCommitPreparation.NoChanges) {
      return CompletableFuture.completedFuture(binding.invocationId());
    }
    var sealed = (TxnCommitPreparation.Sealed) ready;
    long started = System.nanoTime();
    return control.read(scope).thenCompose(observed -> {
      requireCurrentBase(observed.orElseThrow(() ->
          new IllegalStateException("authoritative commit control state is missing")).state(), binding, owner);
      return publisher.publish(sealed.invocation(), owner, sealed.manifest(), scope);
    }).thenCompose(outcome -> outcome.state() == CommitOutcomeResolver.State.SELECTED_FINALIZED
        ? CompletableFuture.completedFuture(binding.invocationId())
        : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN)))
        .whenComplete((ignored, failure) -> metrics.recordTxnStage(
            RequestMetrics.TxnStage.PUBLISH_AND_FINALIZE, Math.max(0, System.nanoTime() - started)));
  }

  private void requireCurrentBase(CommitState state, InvocationBinding binding,
      CommitOwnerService.OwnerToken owner) {
    if (!state.profileFingerprint().equals(profileFingerprint)) {
      throw new IllegalStateException("transaction control profile differs from its table group");
    }
    if (state.ownerEpoch() != owner.epoch()
        || state.ownerInstance().filter(owner.instance()::equals).isEmpty()
        || state.ownerLeaseExpired(clock.millis()) || state.publication().isPresent()
        || state.publishedSequence() != binding.baseSequence()
        || state.publishedRevision() != binding.baseRevision()) {
      throw new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
  }

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }
}
