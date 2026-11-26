package io.etcdhbase.server.commit;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.LeaseBindingReader;
import io.etcdhbase.core.commit.LeaseReadView;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.PinnedReadView;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.lease.LeaseCostAdmission;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.LeaseExpiryCheckpoint;
import io.etcdhbase.core.lease.LeaseFailure;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeRules;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.grpc.protocol.ResponseHeaderFactory;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.codec.LeaseKeepAliveResultMessages;
import io.etcdhbase.hbase.storage.HBaseRecordStore;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;

/** Executes explicit Revoke and expiry-selected deletion through the shared M2 revocation plan. */
public final class LeaseRevocationExecutor {
  public enum CandidateRejection { RECHECK, CLOCK_UNHEALTHY }

  public static final class RejectedBeforePreparation extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final CandidateRejection rejection;

    private RejectedBeforePreparation(CandidateRejection rejection) {
      super("Lease expiry candidate must be rechecked before preparation");
      this.rejection = Objects.requireNonNull(rejection, "rejection");
    }

    public CandidateRejection rejection() { return rejection; }
  }

  private static final BudgetManager.Work FINALIZATION_CAPACITY =
      new BudgetManager.Work(0, 1, 0, 1, 0);
  private static final int MAX_METADATA_RECORDS = 100_000;
  private static final long MAX_METADATA_BYTES = 64L << 20;

  private final String tableGroup;
  private final ByteKey profileFingerprint;
  private final long clusterId;
  private final long memberId;
  private final CommitControlStore control;
  private final ReadViewService views;
  private final LeaseReadView leases;
  private final LeaseRevocationPreparer revocations;
  private final LeaseAuxiliaryMutationPreparation preparation;
  private final CommitPublisher publisher;
  private final HBaseConnectionFactory connections;
  private final StorageTableBinding primary;
  private final Executor backendWorker;
  private final Clock ownerClock;
  private final LeaseTimeSource time;
  private final RequestMetrics metrics;

  public LeaseRevocationExecutor(String tableGroup, ByteKey profileFingerprint,
      long clusterId, long memberId, CommitControlStore control, ReadViewService views,
      SystemRecordStore systemRecords, LeaseReadView leases, LeaseBindingReader bindings,
      LeaseBindingMutationPlanner bindingPlanner, LeaseCostAdmission costs,
      LeaseAuxiliaryMutationPreparation preparation, CommitPublisher publisher,
      HBaseConnectionFactory connections, StorageTableBinding primary, Executor backendWorker,
      Clock ownerClock, LeaseTimeSource time, RequestMetrics metrics) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.profileFingerprint = Objects.requireNonNull(profileFingerprint, "profileFingerprint");
    this.control = Objects.requireNonNull(control, "control");
    this.views = Objects.requireNonNull(views, "views");
    this.leases = Objects.requireNonNull(leases, "leases");
    this.revocations = new LeaseRevocationPreparer(
        leases, bindings, bindingPlanner, costs, systemRecords);
    this.preparation = Objects.requireNonNull(preparation, "preparation");
    this.publisher = Objects.requireNonNull(publisher, "publisher");
    this.connections = Objects.requireNonNull(connections, "connections");
    this.primary = Objects.requireNonNull(primary, "primary");
    this.backendWorker = Objects.requireNonNull(backendWorker, "backendWorker");
    this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
    this.time = Objects.requireNonNull(time, "time");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    if (tableGroup.isBlank() || profileFingerprint.length() != 32 || clusterId == 0 || memberId == 0
        || primary.layout() == StorageTableBinding.Layout.CURRENT) {
      throw new IllegalArgumentException("invalid Lease revocation execution configuration");
    }
    this.clusterId = clusterId;
    this.memberId = memberId;
  }

  /** Caller owns the serialized owner lane and durable invocation binding. */
  public CompletableFuture<InvocationId> executeNew(long leaseId, InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, AuthorizedBackendIdentity identity,
      CommitAuthorizationProof publicationProof, RequestScope scope, BudgetManager.Account account) {
    return executeNew(leaseId, binding, owner, identity, publicationProof, scope, account, () -> { });
  }

  CompletableFuture<InvocationId> executeNew(long leaseId, InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, AuthorizedBackendIdentity identity,
      CommitAuthorizationProof publicationProof, RequestScope scope, BudgetManager.Account account,
      Runnable retireUserIdentity) {
    Objects.requireNonNull(binding, "binding");
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(publicationProof, "publicationProof");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(retireUserIdentity, "retireUserIdentity");
    if (binding.state() != InvocationBinding.State.PREPARING
        || binding.preparedOwnerEpoch() != owner.epoch() || !account.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid new Lease Revoke attempt"));
    }
    if (leaseId == 0) return CompletableFuture.failedFuture(
        new LeaseFailure(LeaseFailure.Reason.NOT_FOUND));
    if (!identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
    }

    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) {
      return CompletableFuture.failedFuture(inactive(scope));
    }
    CompletableFuture<InvocationId> operation;
    try {
      PutCommitPreparation.retainUntilDrain(scope, account.tryAcquire(FINALIZATION_CAPACITY)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT)));
      operation = currentBase(binding, owner, scope)
          .thenCompose(ignored -> views.openAuxiliary(binding.authorizationScopeDigest(), scope))
          .thenCompose(pin -> withManagedRead(pin, scope,
              managed -> prepareExplicit(leaseId, binding, owner, identity, publicationProof,
                  managed, scope, account, retireUserIdentity)));
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

  /**
   * Selects expiry through the current KeepAlive invocation and stores its TTL-zero response as
   * the durable result. The caller already owns the serialized mutation lane, so this method
   * invokes the shared revocation preparation directly instead of relaying into that lane again.
   */
  CompletableFuture<InvocationId> executeExpiredKeepAlive(long leaseId, LeaseState state,
      InvocationBinding binding, CommitOwnerService.OwnerToken owner,
      AuthorizedBackendIdentity backendIdentity, CommitAuthorizationProof publicationProof,
      PinnedReadView pin,
      LeaseTimeObservation observation, RequestScope scope, BudgetManager.Account account,
      Runnable retireUserIdentity) {
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(binding, "binding");
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(backendIdentity, "backendIdentity");
    Objects.requireNonNull(publicationProof, "publicationProof");
    Objects.requireNonNull(pin, "pin");
    Objects.requireNonNull(observation, "observation");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(retireUserIdentity, "retireUserIdentity");
    if (leaseId != state.identity().leaseId() || binding.state() != InvocationBinding.State.PREPARING
        || binding.preparedOwnerEpoch() != owner.epoch() || !account.belongsTo(scope)
        || !observation.healthy()
        || !backendIdentity.scope().tableGroupId().equals(tableGroup)
        || !backendIdentity.allowedTableGroupIds().contains(tableGroup)) {
      return CompletableFuture.failedFuture(
          new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    Function<LeaseRevocationPreparer.Prepared, ByteKey> keepAliveResult = prepared -> {
      long revision = prepared.deletedCount() == 0
          ? binding.baseRevision() : Math.addExact(binding.baseRevision(), 1);
      Rpc.LeaseKeepAliveResponse response = Rpc.LeaseKeepAliveResponse.newBuilder()
          .setHeader(ResponseHeaderFactory.create(clusterId, memberId, revision, 0))
          .setID(leaseId)
          .setTTL(0)
          .build();
      return LeaseKeepAliveResultMessages.encode(
          ByteKey.copyOf(response.toByteArray()), Optional.empty());
    };
    return withManagedRead(pin, scope, managed -> {
      if (!pinnedAtBase(managed.pin(), binding)) {
        return CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      return currentBase(binding, owner, scope).thenCompose(ignored ->
          prepareAndPublish(state, binding, owner, backendIdentity, managed.pin(),
              managed::finish, true, observation, Optional.empty(), publicationProof, scope, account,
              keepAliveResult, Optional.of(retireUserIdentity)));
    });
  }

  /**
   * Applies one already scanned candidate at the same pinned barrier as its checkpoint advance.
   * An empty result means the candidate was stale and the caller may advance only the checkpoint.
   */
  CompletableFuture<java.util.Optional<InvocationId>> revokePinnedCandidate(
      ExpiryCandidateIdentity candidate, long candidateStateSequence,
      InvocationBinding binding, CommitOwnerService.OwnerToken owner,
      AuthorizedBackendIdentity identity, CommitAuthorizationProof publicationProof,
      PinnedReadView pin,
      Supplier<CompletableFuture<Void>> finishPinnedRead,
      LeaseTimeObservation observation, LeaseExpiryCheckpoint.Position nextPosition,
      RequestScope scope, BudgetManager.Account account) {
    Objects.requireNonNull(candidate, "candidate");
    Objects.requireNonNull(publicationProof, "publicationProof");
    Objects.requireNonNull(pin, "pin");
    Objects.requireNonNull(finishPinnedRead, "finishPinnedRead");
    Objects.requireNonNull(observation, "observation");
    Objects.requireNonNull(nextPosition, "nextPosition");
    if (!observation.healthy()) {
      return CompletableFuture.failedFuture(
          new RejectedBeforePreparation(CandidateRejection.CLOCK_UNHEALTHY));
    }
    var proof = new ManifestCatalog.ProofReadBudget(
        scope, account, MAX_METADATA_RECORDS, MAX_METADATA_BYTES);
    CompletableFuture<java.util.Optional<InvocationId>> operation = currentBase(binding, owner, scope).thenCompose(ignored ->
        leases.find(pin, candidate.leaseId(), scope, proof).thenCompose(found -> {
          LeaseState state = found.orElseThrow(() -> new IllegalStateException(
              "expiry candidate references a missing authoritative Lease"));
          CandidateStatus status = candidateStatus(candidate, candidateStateSequence, state);
          if (status == CandidateStatus.STALE) return CompletableFuture.completedFuture(Optional.empty());
          if (!LeaseTimeRules.isExpired(state, observation)) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                "closed expiry window contains a Lease that has not reached its deadline"));
          }
          return prepareAndPublish(state, binding, owner, identity, pin,
              finishPinnedRead, true, observation, Optional.of(nextPosition),
              publicationProof, scope, account)
              .thenApply(Optional::of);
        }));
    return operation.handle((value, failure) -> new ReadOutcome<>(value, failure))
        .thenCompose(outcome -> {
          CompletableFuture<Void> finished;
          try {
            finished = Objects.requireNonNull(finishPinnedRead.get(), "pinned read completion");
          } catch (RuntimeException | Error failure) {
            if (failure instanceof Error fatal) throw fatal;
            finished = CompletableFuture.failedFuture(failure);
          }
          return finished.handle((ignored, finishFailure) -> {
            if (outcome.failure() != null) {
              Throwable cause = CompletionFailures.unwrapCompletion(outcome.failure());
              if (finishFailure != null) {
                Throwable cleanup = CompletionFailures.unwrapCompletion(finishFailure);
                if (cleanup != cause) cause.addSuppressed(cleanup);
              }
              throw new CompletionException(cause);
            }
            if (finishFailure != null) {
              throw new CompletionException(CompletionFailures.unwrapCompletion(finishFailure));
            }
            return outcome.value();
          });
        }).copy();
  }

  enum CandidateStatus { CURRENT, STALE }

  static CandidateStatus candidateStatus(ExpiryCandidateIdentity candidate,
      long candidateStateSequence, LeaseState state) {
    if (state.identity().leaseId() != candidate.leaseId()) {
      throw new IllegalStateException("expiry candidate Lease ID differs from its state row");
    }
    if (state.identity().incarnation() > candidate.incarnation()
        || state.status() == LeaseState.Status.REVOKED
        || state.renewalGeneration() > candidate.renewalGeneration()
        || state.expiryCandidateSequence() > candidateStateSequence) {
      return CandidateStatus.STALE;
    }
    if (state.identity().incarnation() < candidate.incarnation()
        || state.renewalGeneration() < candidate.renewalGeneration()
        || state.expiryCandidateSequence() < candidateStateSequence
        || state.deadlineEpochMillis() != candidate.deadlineEpochMillis()) {
      throw new IllegalStateException("expiry candidate is inconsistent with its authoritative Lease");
    }
    return CandidateStatus.CURRENT;
  }

  static Optional<CandidateRejection> expiryRejectionAtSelection(
      LeaseState state, LeaseTimeObservation observation) {
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(observation, "observation");
    if (!observation.healthy()) return Optional.of(CandidateRejection.CLOCK_UNHEALTHY);
    return LeaseTimeRules.isExpired(state, observation)
        ? Optional.empty() : Optional.of(CandidateRejection.RECHECK);
  }

  private CompletableFuture<InvocationId> prepareExplicit(long leaseId,
      InvocationBinding binding, CommitOwnerService.OwnerToken owner,
      AuthorizedBackendIdentity identity, CommitAuthorizationProof publicationProof,
      ManagedRead managed, RequestScope scope,
      BudgetManager.Account account, Runnable retireUserIdentity) {
    PinnedReadView pin = managed.pin();
    if (!pinnedAtBase(pin, binding)) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    var proof = new ManifestCatalog.ProofReadBudget(
        scope, account, MAX_METADATA_RECORDS, MAX_METADATA_BYTES);
    return leases.find(pin, leaseId, scope, proof).thenCompose(found -> {
      var state = found.orElseThrow(() -> new LeaseFailure(LeaseFailure.Reason.NOT_FOUND));
      if (state.status() != io.etcdhbase.core.lease.LeaseState.Status.ACTIVE) {
        return CompletableFuture.failedFuture(new LeaseFailure(LeaseFailure.Reason.NOT_FOUND));
      }
      return prepareAndPublish(state, binding, owner, identity, managed,
          false, null, Optional.empty(), publicationProof, scope, account, retireUserIdentity);
    });
  }

  private CompletableFuture<InvocationId> prepareAndPublish(LeaseState state,
      InvocationBinding binding, CommitOwnerService.OwnerToken owner,
      AuthorizedBackendIdentity backendIdentity, ManagedRead managed,
      boolean requireExpired, LeaseTimeObservation expiryProof,
      Optional<LeaseExpiryCheckpoint.Position> nextPosition,
      CommitAuthorizationProof publicationProof,
      RequestScope scope, BudgetManager.Account account, Runnable retireUserIdentity) {
    return prepareAndPublish(state, binding, owner, backendIdentity, managed.pin(),
        managed::finish, requireExpired, expiryProof, nextPosition, publicationProof, scope, account,
        prepared -> revokeResultPayload(prepared, binding), Optional.of(retireUserIdentity));
  }

  private CompletableFuture<InvocationId> prepareAndPublish(LeaseState state,
      InvocationBinding binding, CommitOwnerService.OwnerToken owner,
      AuthorizedBackendIdentity backendIdentity, PinnedReadView pin,
      Supplier<CompletableFuture<Void>> finishPinnedRead,
      boolean requireExpired, LeaseTimeObservation expiryProof,
      Optional<LeaseExpiryCheckpoint.Position> nextPosition,
      CommitAuthorizationProof publicationProof,
      RequestScope scope, BudgetManager.Account account) {
    return prepareAndPublish(state, binding, owner, backendIdentity, pin,
        finishPinnedRead, requireExpired, expiryProof, nextPosition, publicationProof, scope, account,
        prepared -> revokeResultPayload(prepared, binding), Optional.empty());
  }

  private CompletableFuture<InvocationId> prepareAndPublish(LeaseState state,
      InvocationBinding binding, CommitOwnerService.OwnerToken owner,
      AuthorizedBackendIdentity backendIdentity, PinnedReadView pin,
      Supplier<CompletableFuture<Void>> finishPinnedRead,
      boolean requireExpired, LeaseTimeObservation expiryProof,
      Optional<LeaseExpiryCheckpoint.Position> nextPosition,
      CommitAuthorizationProof publicationProof,
      RequestScope scope, BudgetManager.Account account,
      Function<LeaseRevocationPreparer.Prepared, ByteKey> resultPayload,
      Optional<Runnable> retireUserIdentity) {
    long started = System.nanoTime();
    CompletableFuture<? extends RecordStore> records = retireUserIdentity.isPresent()
        ? UserIdentityRecordStore.open(connections, backendIdentity, primary, scope, backendWorker,
            retireUserIdentity.orElseThrow())
        : HBaseRecordStore.open(connections, backendIdentity, primary, scope, backendWorker);
    return records
        .thenCompose(currentRecords -> revocations.prepare(state.identity(), binding, requireExpired,
            pin, currentRecords, scope, account,
            new ManifestCatalog.ProofReadBudget(scope, account, MAX_METADATA_RECORDS, MAX_METADATA_BYTES),
            finishPinnedRead, expiryProof, nextPosition))
        .thenCompose(prepared -> sealPrepared(prepared, binding, owner, scope, account,
            resultPayload))
        .whenComplete((ignored, failure) -> metrics.recordLeaseRevocationStage(
            RequestMetrics.LeaseRevocationStage.PREPARE,
            Math.max(0, System.nanoTime() - started)))
        .thenCompose(sealed -> currentBase(binding, owner, scope)
            .thenCompose(ignored -> validateExpiryAtSelection(requireExpired, expiryProof, state)
                .thenCompose(checked -> publisher.publish(
                    sealed.invocation(), owner, sealed.manifest(), scope, publicationProof))))
        .thenCompose(outcome -> {
          metrics.recordLeaseRevocationStage(RequestMetrics.LeaseRevocationStage.PUBLISH_AND_FINALIZE,
              Math.max(0, System.nanoTime() - started));
          return outcome.state() == CommitOutcomeResolver.State.SELECTED_FINALIZED
              ? CompletableFuture.completedFuture(binding.invocationId())
              : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
        });
  }

  private CompletableFuture<Void> validateExpiryAtSelection(boolean requireExpired,
      LeaseTimeObservation expiryProof, LeaseState state) {
    if (!requireExpired) return CompletableFuture.completedFuture(null);
    if (expiryProof == null) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("expiry selection requires a trusted time proof"));
    }
    if (!expiryProof.healthy()) {
      return CompletableFuture.failedFuture(
          new RejectedBeforePreparation(CandidateRejection.CLOCK_UNHEALTHY));
    }
    if (!LeaseTimeRules.isExpired(state, expiryProof)) {
      return CompletableFuture.failedFuture(
          new RejectedBeforePreparation(CandidateRejection.RECHECK));
    }
    LeaseTimeObservation selectionObservation =
        Objects.requireNonNull(time.observe(), "Lease time observation");
    Optional<CandidateRejection> rejection =
        expiryRejectionAtSelection(state, selectionObservation);
    if (rejection.isPresent()) {
      return CompletableFuture.failedFuture(new RejectedBeforePreparation(rejection.orElseThrow()));
    }
    return CompletableFuture.completedFuture(null);
  }

  private CompletableFuture<LeaseAuxiliaryMutationPreparation.Sealed> sealPrepared(
      LeaseRevocationPreparer.Prepared prepared, InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, RequestScope scope, BudgetManager.Account account,
      Function<LeaseRevocationPreparer.Prepared, ByteKey> resultPayload) {
    final ByteKey payload;
    try {
      payload = Objects.requireNonNull(resultPayload.apply(prepared), "Lease Revoke result payload");
    } catch (RuntimeException | Error failure) {
      prepared.close();
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    return preparation.prepareWithMutation(binding, payload,
        prepared.candidates().iterator(), prepared.events().iterator(),
        prepared.auxiliaryRecords().iterator(), () -> CompletableFuture.completedFuture(null),
        scope, account).whenComplete((ignored, failure) -> prepared.close());
  }

  private ByteKey revokeResultPayload(LeaseRevocationPreparer.Prepared prepared,
      InvocationBinding binding) {
    long revision = prepared.deletedCount() == 0
        ? binding.baseRevision() : Math.addExact(binding.baseRevision(), 1);
    Rpc.LeaseRevokeResponse response = Rpc.LeaseRevokeResponse.newBuilder()
        .setHeader(ResponseHeaderFactory.create(clusterId, memberId, revision, 0))
        .build();
    return ByteKey.copyOf(response.toByteArray());
  }

  private CompletableFuture<Void> currentBase(InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, RequestScope scope) {
    return control.read(scope).thenAccept(observed -> {
      CommitState state = observed.orElseThrow(() ->
          new IllegalStateException("authoritative commit control state is missing")).state();
      if (!state.profileFingerprint().equals(profileFingerprint)) {
        throw new IllegalStateException("Lease Revoke control profile differs from its table group");
      }
      if (!state.supportsLeaseExpiryCheckpoints()
          || state.ownerEpoch() != owner.epoch()
          || state.ownerInstance().filter(owner.instance()::equals).isEmpty()
          || state.ownerLeaseExpired(ownerClock.millis()) || state.publication().isPresent()
          || state.publishedSequence() != binding.baseSequence()
          || state.publishedRevision() != binding.baseRevision()) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
    });
  }

  static boolean pinnedAtBase(PinnedReadView pin, InvocationBinding binding) {
    return pin.view().barrierSequence() == binding.baseSequence()
        && pin.view().targetRevision() == binding.baseRevision()
        && pin.view().selectedDecision().isEmpty();
  }

  private <T> CompletableFuture<T> withManagedRead(PinnedReadView pin, RequestScope scope,
      Function<ManagedRead, CompletableFuture<T>> operation) {
    ManagedRead managed = new ManagedRead(pin, scope);
    CompletableFuture<T> result;
    try {
      result = Objects.requireNonNull(operation.apply(managed), "Lease read operation");
    } catch (RuntimeException | Error failure) {
      result = CompletableFuture.failedFuture(failure);
    }
    return result.handle((value, failure) -> new ReadOutcome<>(value, failure))
        .thenCompose(outcome -> {
          if (outcome.failure() == null) return CompletableFuture.completedFuture(outcome.value());
          Throwable cause = CompletionFailures.unwrapCompletion(outcome.failure());
          return managed.finish().handle((ignored, cleanupFailure) -> {
            if (cleanupFailure != null) {
              Throwable cleanupCause = CompletionFailures.unwrapCompletion(cleanupFailure);
              if (cleanupCause != cause) cause.addSuppressed(cleanupCause);
            }
            throw new CompletionException(cause);
          });
        });
  }

  private record ReadOutcome<T>(T value, Throwable failure) {}

  private final class ManagedRead {
    private final PinnedReadView pin;
    private final RequestScope scope;
    private CompletableFuture<Void> completion;

    private ManagedRead(PinnedReadView pin, RequestScope scope) {
      this.pin = Objects.requireNonNull(pin, "pin");
      this.scope = Objects.requireNonNull(scope, "scope");
    }

    private PinnedReadView pin() { return pin; }

    private synchronized CompletableFuture<Void> finish() {
      if (completion == null) {
        try {
          completion = Objects.requireNonNull(leases.finish(pin, scope), "Lease read completion");
        } catch (RuntimeException | Error failure) {
          if (failure instanceof Error fatal) throw fatal;
          completion = CompletableFuture.failedFuture(failure);
        }
      }
      return completion.copy();
    }
  }

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }
}
