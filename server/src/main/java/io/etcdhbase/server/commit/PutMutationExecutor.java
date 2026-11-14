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
import io.etcdhbase.core.commit.PinnedReadView;
import io.etcdhbase.core.commit.ReadViewRecordStore;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.kv.PutPlanner;
import io.etcdhbase.core.kv.ResponseBudget;
import io.etcdhbase.core.lease.LeaseBindingChange;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.DirectRecordReader;
import io.etcdhbase.core.scan.PointResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.codec.PhysicalRowLimitException;
import io.etcdhbase.hbase.storage.HBaseRecordStore;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Executes a new bound Put against one pinned base, then publishes its sealed M2 manifest. */
public final class PutMutationExecutor {
  private static final BudgetManager.Work FINALIZATION_CAPACITY =
      new BudgetManager.Work(0, 1, 0, 1, 0);

  private final String tableGroup;
  private final ByteKey profileFingerprint;
  private final CommitControlStore control;
  private final ReadViewService views;
  private final ManifestCatalog manifests;
  private final HBaseConnectionFactory connections;
  private final StorageTableBinding primary;
  private final Executor backendWorker;
  private final PutCommitPreparation preparation;
  private final CommitPublisher publisher;
  private final LeaseBindingMutationPlanner leaseBindings;
  private final long maximumPointBytes;
  private final long maximumResponseBytes;
  private final Clock clock;
  private final RequestMetrics metrics;

  public PutMutationExecutor(String tableGroup, ByteKey profileFingerprint,
      CommitControlStore control, ReadViewService views, ManifestCatalog manifests,
      HBaseConnectionFactory connections, StorageTableBinding primary, Executor backendWorker,
      PutCommitPreparation preparation, CommitPublisher publisher,
      long maximumPointBytes, long maximumResponseBytes) {
    this(tableGroup, profileFingerprint, control, views, manifests, connections, primary,
        backendWorker, preparation, publisher, maximumPointBytes, maximumResponseBytes,
        Clock.systemUTC(), new RequestMetrics(), null);
  }

  public PutMutationExecutor(String tableGroup, ByteKey profileFingerprint,
      CommitControlStore control, ReadViewService views, ManifestCatalog manifests,
      HBaseConnectionFactory connections, StorageTableBinding primary, Executor backendWorker,
      PutCommitPreparation preparation, CommitPublisher publisher,
      long maximumPointBytes, long maximumResponseBytes, RequestMetrics metrics) {
    this(tableGroup, profileFingerprint, control, views, manifests, connections, primary,
        backendWorker, preparation, publisher, maximumPointBytes, maximumResponseBytes,
        Clock.systemUTC(), metrics, null);
  }

  public PutMutationExecutor(String tableGroup, ByteKey profileFingerprint,
      CommitControlStore control, ReadViewService views, ManifestCatalog manifests,
      HBaseConnectionFactory connections, StorageTableBinding primary, Executor backendWorker,
      PutCommitPreparation preparation, CommitPublisher publisher,
      long maximumPointBytes, long maximumResponseBytes,
      LeaseBindingMutationPlanner leaseBindings, RequestMetrics metrics) {
    this(tableGroup, profileFingerprint, control, views, manifests, connections, primary,
        backendWorker, preparation, publisher, maximumPointBytes, maximumResponseBytes,
        Clock.systemUTC(), metrics, Objects.requireNonNull(leaseBindings, "leaseBindings"));
  }

  PutMutationExecutor(String tableGroup, ByteKey profileFingerprint,
      CommitControlStore control, ReadViewService views, ManifestCatalog manifests,
      HBaseConnectionFactory connections, StorageTableBinding primary, Executor backendWorker,
      PutCommitPreparation preparation, CommitPublisher publisher,
      long maximumPointBytes, long maximumResponseBytes, Clock clock) {
    this(tableGroup, profileFingerprint, control, views, manifests, connections, primary,
        backendWorker, preparation, publisher, maximumPointBytes, maximumResponseBytes,
        clock, new RequestMetrics(), null);
  }

  private PutMutationExecutor(String tableGroup, ByteKey profileFingerprint,
      CommitControlStore control, ReadViewService views, ManifestCatalog manifests,
      HBaseConnectionFactory connections, StorageTableBinding primary, Executor backendWorker,
      PutCommitPreparation preparation, CommitPublisher publisher,
      long maximumPointBytes, long maximumResponseBytes, Clock clock, RequestMetrics metrics,
      LeaseBindingMutationPlanner leaseBindings) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.profileFingerprint = Objects.requireNonNull(profileFingerprint, "profileFingerprint");
    this.control = Objects.requireNonNull(control, "control");
    this.views = Objects.requireNonNull(views, "views");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.connections = Objects.requireNonNull(connections, "connections");
    this.primary = Objects.requireNonNull(primary, "primary");
    this.backendWorker = Objects.requireNonNull(backendWorker, "backendWorker");
    this.preparation = Objects.requireNonNull(preparation, "preparation");
    this.publisher = Objects.requireNonNull(publisher, "publisher");
    this.leaseBindings = leaseBindings;
    this.clock = Objects.requireNonNull(clock, "clock");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    if (tableGroup.isBlank() || profileFingerprint.length() != 32
        || primary.layout() == StorageTableBinding.Layout.CURRENT
        || maximumPointBytes < 1 || maximumResponseBytes < ResponseBudget.FIXED_BYTES) {
      throw new IllegalArgumentException("invalid Put execution configuration");
    }
    this.maximumPointBytes = maximumPointBytes;
    this.maximumResponseBytes = maximumResponseBytes;
  }

  /**
   * The caller serializes attempts before binding their base and verifies the forwarded request's
   * authorization digest. This method handles only a new PREPARING attempt; sealed or selected
   * retries must use the durable outcome/recovery path, never create a replacement attempt.
   * Caller-owned response capacity must also remain reserved until actual transport drain.
   */
  public CompletableFuture<InvocationId> executeNew(PutCommand command, InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, AuthorizedBackendIdentity identity,
      RequestScope scope, BudgetManager.Account account) {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(binding, "binding");
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    if (binding.state() != InvocationBinding.State.PREPARING
        || binding.preparedOwnerEpoch() != owner.epoch() || !account.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid new Put attempt"));
    }
    if (!identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
    }
    if (maximumPointBytes > account.reservation().output()
        || maximumResponseBytes > account.reservation().output() - maximumPointBytes) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    }
    CompletableFuture<Void> executionDrained = new CompletableFuture<>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(executionDrained))) {
      return CompletableFuture.failedFuture(inactive(scope));
    }
    CompletableFuture<InvocationId> operation;
    try {
      PutCommitPreparation.retainUntilDrain(scope,
          account.tryRetain(BudgetManager.Memory.OUTPUT, maximumResponseBytes)
              .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT)));
      PutCommitPreparation.retainUntilDrain(scope,
          account.tryAcquire(FINALIZATION_CAPACITY)
              .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT)));
      operation = control.read(scope).thenCompose(observed -> {
        CommitState state = observed.orElseThrow(() ->
            new IllegalStateException("authoritative commit control state is missing")).state();
        requireCurrentBase(state, binding, owner);
        return readAndPrepare(command, binding, identity, scope, account);
      }).thenCompose(sealed -> publish(binding, owner, sealed, scope));
    } catch (BackendFailure | RuntimeException failure) {
      executionDrained.complete(null);
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      executionDrained.complete(null);
      throw fatal;
    }
    operation.whenComplete((ignored, failure) -> executionDrained.complete(null));
    return operation.copy();
  }

  private CompletableFuture<PutCommitPreparation.Sealed> readAndPrepare(PutCommand command,
      InvocationBinding binding, AuthorizedBackendIdentity identity,
      RequestScope scope, BudgetManager.Account account) {
    long started = System.nanoTime();
    var readView = leaseBindings == null
        ? views.open(OptionalLong.empty(), binding.authorizationScopeDigest(), scope)
        : views.openAuxiliary(binding.authorizationScopeDigest(), scope);
    return readView
        .thenCompose(pinned -> {
          if (pinned.view().barrierSequence() != binding.baseSequence()
              || pinned.view().targetRevision() != binding.baseRevision()
              || pinned.view().selectedDecision().isPresent()) {
            return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
          }
          return HBaseRecordStore.open(connections, identity, primary, scope, backendWorker)
              .thenCompose(physical -> {
                var view = new ReadViewRecordStore(pinned, physical, manifests, primary.codec()::bucket,
                    primary.codec().saltProfile().bucketCount(), scope);
                var reader = new DirectRecordReader(view, account, maximumPointBytes);
                return reader.readForPreparation(command.key(), binding.baseRevision(), ReadProjection.FULL)
                    .thenCompose(point -> prepareFromPoint(
                        command, binding, point, pinned, view, scope, account));
              });
        }).exceptionallyCompose(failure -> {
          Throwable cause = CompletionFailures.unwrapCompletion(failure);
          return CompletableFuture.failedFuture(cause instanceof PhysicalRowLimitException
              ? new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT, cause) : failure);
        }).whenComplete((ignored, failure) -> metrics.recordPutStage(RequestMetrics.PutStage.PREPARE,
            Math.max(0, System.nanoTime() - started)));
  }

  private CompletableFuture<PutCommitPreparation.Sealed> prepareFromPoint(PutCommand command,
      InvocationBinding binding, PointResult point, PinnedReadView pinned, ReadViewRecordStore view,
      RequestScope scope, BudgetManager.Account account) {
    try {
      CompletableFuture<PutCommitPreparation.Sealed> result;
      if (leaseBindings == null) {
        result = preparation.prepare(command, binding, point.record(), scope, account, view::finishRead);
      } else {
        PutPlanner.Plan plan = new PutPlanner().planWithLease(command, binding, point.record());
        List<LeaseBindingChange> changes = LeaseBindingChange.between(
            point.record(), plan.candidate().record()).stream().toList();
        LeaseBindingMutationPlanner.MutationView mutationView =
            leaseBindings.open(pinned, scope, account);
        result = mutationView.prepare(binding, changes).thenCompose(auxiliary ->
            preparation.prepare(plan, command, binding, point.record(), auxiliary,
                scope, account, view::finishRead));
      }
      return result.whenComplete((ignored, failure) -> point.close());
    } catch (RuntimeException | Error failure) {
      try {
        point.close();
      } catch (RuntimeException | Error cleanup) {
        if (failure != cleanup) failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  private CompletableFuture<InvocationId> publish(InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, PutCommitPreparation.Sealed sealed, RequestScope scope) {
    long encoded = sealed.manifest().resultReference().orElseThrow().totalBytes();
    if (ResponseBudget.forEncodedResult(encoded) > maximumResponseBytes) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    }
    long started = System.nanoTime();
    return control.read(scope).thenCompose(observed -> {
      CommitState state = observed.orElseThrow(() ->
          new IllegalStateException("authoritative commit control state is missing")).state();
      requireCurrentBase(state, binding, owner);
      return publisher.publish(sealed.invocation(), owner, sealed.manifest(), scope);
    }).thenCompose(outcome -> outcome.state() == CommitOutcomeResolver.State.SELECTED_FINALIZED
        ? CompletableFuture.completedFuture(binding.invocationId())
        : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN)))
        .whenComplete((ignored, failure) -> metrics.recordPutStage(
            RequestMetrics.PutStage.PUBLISH_AND_FINALIZE, Math.max(0, System.nanoTime() - started)));
  }

  private void requireCurrentBase(CommitState state, InvocationBinding binding,
      CommitOwnerService.OwnerToken owner) {
    if (!state.profileFingerprint().equals(profileFingerprint)) {
      throw new IllegalStateException("Put control profile differs from its table group");
    }
    if (state.ownerEpoch() != owner.epoch()
        || state.ownerInstance().filter(owner.instance()::equals).isEmpty()
        || state.ownerLeaseExpired(clock.millis()) || state.publication().isPresent()
        || state.publishedSequence() != binding.baseSequence()
        || state.publishedRevision() != binding.baseRevision()) {
      throw new java.util.concurrent.CompletionException(
          new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
  }

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }
}
