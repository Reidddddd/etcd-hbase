package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.LeaseReadView;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.PinnedReadView;
import io.etcdhbase.core.commit.PreparedAuxiliaryRecord;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.SystemRecordEntry;
import io.etcdhbase.core.commit.SystemRecordPage;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.ExpiryCandidateRecord;
import io.etcdhbase.core.lease.LeaseExpiryCheckpoint;
import io.etcdhbase.core.lease.LeaseExpiryStepCommand;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeRules;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.server.observability.RequestMetrics;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.Supplier;

/** Executes one bounded, owner-fenced expiry scan page and atomically persists its progress. */
public final class LeaseExpiryStepExecutor {
  static final int SCAN_PAGE_ENTRIES = 32;
  static final long SCAN_PAGE_BYTES = 32L << 10;
  static final long SCAN_PAGE_MEMORY_BYTES = 48L << 10;
  private static final int MAX_METADATA_RECORDS = 4096;
  private static final long MAX_METADATA_BYTES = 64L << 20;
  private static final BudgetManager.Work FINALIZATION_CAPACITY =
      new BudgetManager.Work(0, 1, 0, 1, 0);
  private static final ByteKey STEP_RESULT = ByteKey.copyOf(new byte[] {'L', 'E', 'X', 1});

  private final String tableGroup;
  private final ByteKey profileFingerprint;
  private final int bucketCount;
  private final CommitControlStore control;
  private final ReadViewService views;
  private final LeaseReadView leases;
  private final SystemRecordStore records;
  private final LeaseTimeSource time;
  private final Clock ownerClock;
  private final LeaseAuxiliaryMutationPreparation preparation;
  private final CommitPublisher publisher;
  private final LeaseRevocationExecutor revocations;
  private final RequestMetrics metrics;

  public LeaseExpiryStepExecutor(String tableGroup, ByteKey profileFingerprint, int bucketCount,
      CommitControlStore control, ReadViewService views, LeaseReadView leases,
      SystemRecordStore records, LeaseTimeSource time, Clock ownerClock,
      LeaseAuxiliaryMutationPreparation preparation, CommitPublisher publisher,
      LeaseRevocationExecutor revocations, RequestMetrics metrics) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.profileFingerprint = Objects.requireNonNull(profileFingerprint, "profileFingerprint");
    this.control = Objects.requireNonNull(control, "control");
    this.views = Objects.requireNonNull(views, "views");
    this.leases = Objects.requireNonNull(leases, "leases");
    this.records = Objects.requireNonNull(records, "records");
    this.time = Objects.requireNonNull(time, "time");
    this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
    this.preparation = Objects.requireNonNull(preparation, "preparation");
    this.publisher = Objects.requireNonNull(publisher, "publisher");
    this.revocations = Objects.requireNonNull(revocations, "revocations");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    if (tableGroup.isBlank() || profileFingerprint.length() != 32 || bucketCount < 1
        || bucketCount > 65_536) {
      throw new IllegalArgumentException("invalid Lease expiry step configuration");
    }
    this.bucketCount = bucketCount;
  }

  /** Caller owns the serialized owner lane and the durable invocation binding. */
  public CompletableFuture<InvocationId> executeNew(LeaseExpiryStepCommand command,
      InvocationBinding binding, CommitOwnerService.OwnerToken owner,
      AuthorizedBackendIdentity identity, CommitAuthorizationProof publicationProof,
      RequestScope scope, BudgetManager.Account account) {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(binding, "binding");
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(publicationProof, "publicationProof");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    if (binding.state() != InvocationBinding.State.PREPARING
        || binding.preparedOwnerEpoch() != owner.epoch() || !account.belongsTo(scope)
        || !identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("invalid Lease expiry step attempt"));
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
          .thenCompose(pin -> withManagedRead(pin, scope, managed -> executePinned(command,
              binding, owner, identity, publicationProof, managed, scope, account)));
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

  private CompletableFuture<InvocationId> executePinned(LeaseExpiryStepCommand command,
      InvocationBinding binding, CommitOwnerService.OwnerToken owner,
      AuthorizedBackendIdentity identity, CommitAuthorizationProof publicationProof,
      ManagedRead managed,
      RequestScope scope, BudgetManager.Account account) {
    PinnedReadView pin = managed.pin();
    if (!LeaseRevocationExecutor.pinnedAtBase(pin, binding)) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    var proof = new ManifestCatalog.ProofReadBudget(
        scope, account, MAX_METADATA_RECORDS, MAX_METADATA_BYTES);
    return leases.expiryCheckpoint(pin, scope, proof).thenCompose(previous -> {
      long actualSequence = previous.map(LeaseExpiryCheckpoint::sequence).orElse(0L);
      if (actualSequence != command.expectedCheckpointSequence()) {
        return completeWithoutChanges(binding, managed, scope, account);
      }
      if (previous.isPresent()) {
        LeaseExpiryCheckpoint checkpoint = previous.orElseThrow();
        if (checkpoint.bucketCount() != bucketCount || checkpoint.ownerEpoch() > owner.epoch()) {
          return CompletableFuture.failedFuture(new IllegalStateException(
              "Lease expiry checkpoint belongs to another storage profile or future owner"));
        }
      }
      LeaseTimeObservation observation = time.observe();
      if (!observation.healthy()) {
        metrics.recordLeaseExpiryRetry();
        return completeWithoutChanges(binding, managed, scope, account);
      }
      if (previous.isPresent()
          && observation.nowLowerEpochMillis() < previous.orElseThrow().trustedTimeLowerEpochMillis()) {
        metrics.recordLeaseExpiryRetry();
        return completeWithoutChanges(binding, managed, scope, account);
      }
      return nextScan(previous, observation, binding, owner, identity, publicationProof, managed,
          scope, account, proof);
    }).copy();
  }

  private CompletableFuture<InvocationId> nextScan(Optional<LeaseExpiryCheckpoint> previous,
      LeaseTimeObservation observation, InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, AuthorizedBackendIdentity identity,
      CommitAuthorizationProof publicationProof,
      ManagedRead managed, RequestScope scope, BudgetManager.Account account,
      ManifestCatalog.ProofReadBudget proof) {
    if (previous.isEmpty()) {
      long stop = LeaseExpiryCheckpoint.closedWindowStopExclusive(observation.nowLowerEpochMillis());
      return persistCheckpoint(binding, owner, Optional.empty(), observation,
          new LeaseExpiryCheckpoint.Position(0, stop, 0, Optional.empty()), managed,
          publicationProof, scope, account);
    }
    LeaseExpiryCheckpoint checkpoint = previous.orElseThrow();
    LeaseExpiryCheckpoint.Position position = checkpoint.position();
    if (position.idle()) {
      long stop = LeaseExpiryCheckpoint.closedWindowStopExclusive(observation.nowLowerEpochMillis());
      if (stop <= position.windowStartInclusive()) {
        return completeWithoutChanges(binding, managed, scope, account);
      }
      return persistCheckpoint(binding, owner, previous, observation,
          new LeaseExpiryCheckpoint.Position(position.windowStartInclusive(), stop, 0,
              Optional.empty()), managed, publicationProof, scope, account);
    }
    if (observation.nowLowerEpochMillis() < checkpoint.trustedTimeLowerEpochMillis()) {
      metrics.recordLeaseExpiryRetry();
      return completeWithoutChanges(binding, managed, scope, account);
    }
    return scanPage(position, checkpoint, observation, binding, owner, identity,
        publicationProof, managed, scope, account, proof);
  }

  private CompletableFuture<InvocationId> scanPage(LeaseExpiryCheckpoint.Position position,
      LeaseExpiryCheckpoint checkpoint, LeaseTimeObservation observation,
      InvocationBinding binding, CommitOwnerService.OwnerToken owner,
      AuthorizedBackendIdentity identity, CommitAuthorizationProof publicationProof,
      ManagedRead managed,
      RequestScope scope, BudgetManager.Account account,
      ManifestCatalog.ProofReadBudget proof) {
    return withExpiryPageMemory(scope, account, () -> records.scanExpiryWindow(position.bucket(),
        position.windowStartInclusive(), position.windowStopExclusive(), position.afterIdentity(),
        SCAN_PAGE_ENTRIES, SCAN_PAGE_BYTES, scope).thenCompose(page -> {
          try {
            account.chargeInternalScanPage(
                page.physicalRows(), page.encodedBytes(), page.scanRpcCalls());
            metrics.recordLeaseExpiryScan(page.physicalRows(), page.encodedBytes());
            return processPage(page, position, checkpoint, observation, binding, owner,
                identity, publicationProof, managed, scope, account, proof);
          } catch (BackendFailure | RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
          }
        }));
  }

  static <T> CompletableFuture<T> withExpiryPageMemory(RequestScope scope,
      BudgetManager.Account account, Supplier<CompletableFuture<T>> operation) {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(operation, "operation");
    final BudgetManager.Lease pageMemory;
    try {
      pageMemory = account.tryRetain(BudgetManager.Memory.PAGE, SCAN_PAGE_MEMORY_BYTES)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      PutCommitPreparation.retainUntilDrain(scope, pageMemory);
    } catch (BackendFailure | RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    try {
      return Objects.requireNonNull(operation.get(), "Lease expiry page operation").copy();
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletableFuture<InvocationId> processPage(SystemRecordPage page,
      LeaseExpiryCheckpoint.Position position, LeaseExpiryCheckpoint checkpoint,
      LeaseTimeObservation observation, InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, AuthorizedBackendIdentity identity,
      CommitAuthorizationProof publicationProof,
      ManagedRead managed, RequestScope scope, BudgetManager.Account account,
      ManifestCatalog.ProofReadBudget proof) {
    CompletableFuture<Optional<LocatedCandidate>> scan = CompletableFuture.completedFuture(Optional.empty());
    for (SystemRecordEntry entry : page.entries()) {
      scan = scan.thenCompose(found -> found.isPresent()
          ? CompletableFuture.completedFuture(found)
          : inspectCandidate(entry, position, binding.baseSequence(), observation,
              managed.pin(), scope, proof));
    }
    return scan.thenCompose(found -> {
      if (found.isPresent()) {
        LocatedCandidate located = found.orElseThrow();
        LeaseExpiryCheckpoint.Position nextPosition = new LeaseExpiryCheckpoint.Position(
            position.windowStartInclusive(), position.windowStopExclusive(), position.bucket(),
            Optional.of(located.identity().encode()));
        return revocations.revokePinnedCandidate(located.identity(), located.record().stateSequence(),
            binding, owner, identity, publicationProof, managed.pin(), managed::finish, observation,
            nextPosition, scope, account).thenCompose(revoked -> {
              if (revoked.isPresent()) {
                recordExpiryDelayAfterCommit(located.identity().deadlineEpochMillis());
                return CompletableFuture.completedFuture(revoked.orElseThrow());
              }
              return persistCheckpoint(binding, owner, Optional.of(checkpoint), observation,
                  nextPosition, managed, publicationProof, scope, account);
            });
      }
      LeaseExpiryCheckpoint.Position nextPosition = nextPositionAfterPage(page, position);
      return persistCheckpoint(binding, owner, Optional.of(checkpoint), observation,
          nextPosition, managed, publicationProof, scope, account);
    });
  }

  private CompletableFuture<Optional<LocatedCandidate>> inspectCandidate(SystemRecordEntry entry,
      LeaseExpiryCheckpoint.Position position, long pinnedSequence,
      LeaseTimeObservation observation, PinnedReadView pin, RequestScope scope,
      ManifestCatalog.ProofReadBudget proof) {
    ExpiryCandidateIdentity candidate = ExpiryCandidateIdentity.decode(entry.identity());
    ExpiryCandidateRecord record = ExpiryCandidateRecord.decode(entry.value());
    if (candidate.deadlineEpochMillis() < position.windowStartInclusive()
        || candidate.deadlineEpochMillis() >= position.windowStopExclusive()
        || !candidateVisibleAtPinnedSequence(record.stateSequence(), pinnedSequence)
        || position.afterIdentity().filter(after -> entry.identity().compareTo(after) <= 0).isPresent()
        || records.bucket(SystemRecordType.EXPIRY_CANDIDATE, entry.identity()) != position.bucket()) {
      return CompletableFuture.failedFuture(new IllegalStateException(
          "expiry scan returned a record outside its closed, protected bucket window"));
    }
    metrics.recordLeaseExpiryCandidate();
    return leases.find(pin, candidate.leaseId(), scope, proof).thenApply(found -> {
      LeaseState state = found.orElseThrow(() -> new IllegalStateException(
          "expiry candidate has no authoritative Lease state"));
      LeaseRevocationExecutor.CandidateStatus status = LeaseRevocationExecutor.candidateStatus(
          candidate, record.stateSequence(), state);
      if (status == LeaseRevocationExecutor.CandidateStatus.STALE) {
        metrics.recordLeaseExpiryStaleCandidate();
        return Optional.empty();
      }
      if (!LeaseTimeRules.isExpired(state, observation)) {
        throw new IllegalStateException("closed expiry window contains a live Lease candidate");
      }
      return Optional.of(new LocatedCandidate(candidate, record));
    });
  }

  static boolean candidateVisibleAtPinnedSequence(long candidateSequence, long pinnedSequence) {
    if (candidateSequence <= 0 || pinnedSequence < 0) {
      throw new IllegalArgumentException("invalid Lease expiry candidate sequence barrier");
    }
    return candidateSequence <= pinnedSequence;
  }

  private LeaseExpiryCheckpoint.Position nextPositionAfterPage(SystemRecordPage page,
      LeaseExpiryCheckpoint.Position before) {
    if (!page.exhausted()) {
      if (page.lastIdentity().isEmpty()) {
        throw new IllegalStateException("non-exhausted expiry page made no cursor progress");
      }
      ByteKey last = page.lastIdentity().orElseThrow();
      if (before.afterIdentity().filter(previous -> last.compareTo(previous) <= 0).isPresent()) {
        throw new IllegalStateException("expiry page cursor did not advance");
      }
      return new LeaseExpiryCheckpoint.Position(before.windowStartInclusive(),
          before.windowStopExclusive(), before.bucket(), Optional.of(last));
    }
    if (before.bucket() + 1 < bucketCount) {
      return new LeaseExpiryCheckpoint.Position(before.windowStartInclusive(),
          before.windowStopExclusive(), before.bucket() + 1, Optional.empty());
    }
    return LeaseExpiryCheckpoint.idleAt(before.windowStopExclusive());
  }

  private CompletableFuture<InvocationId> persistCheckpoint(InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, Optional<LeaseExpiryCheckpoint> previous,
      LeaseTimeObservation observation, LeaseExpiryCheckpoint.Position position,
      ManagedRead managed, CommitAuthorizationProof publicationProof,
      RequestScope scope, BudgetManager.Account account) {
    long sequence;
    LeaseExpiryCheckpoint checkpoint;
    try {
      sequence = Math.addExact(binding.baseSequence(), 1);
      long trustedTime = previous.map(LeaseExpiryCheckpoint::trustedTimeLowerEpochMillis)
          .map(value -> Math.max(value, observation.nowLowerEpochMillis()))
          .orElse(observation.nowLowerEpochMillis());
      checkpoint = new LeaseExpiryCheckpoint(owner.epoch(), sequence, bucketCount,
          binding.baseSequence(), trustedTime, position);
      if (previous.isPresent()) {
        LeaseExpiryCheckpoint.requireForwardProgress(previous.orElseThrow(), checkpoint);
      } else if (position.windowStartInclusive() != 0) {
        throw new IllegalArgumentException("initial Lease expiry window must start at zero");
      }
    } catch (ArithmeticException | IllegalArgumentException invalid) {
      return CompletableFuture.failedFuture(invalid);
    }
    PreparedAuxiliaryRecord record = PreparedAuxiliaryRecord.create(binding.transactionId(),
        binding.preparedOwnerEpoch(), binding.baseSequence(), 0,
        SystemRecordType.LEASE_EXPIRY_CHECKPOINT,
        LeaseExpiryCheckpoint.versionIdentity(sequence), checkpoint.encode());
    return preparation.prepare(binding, STEP_RESULT, List.of(record).iterator(),
        managed::finish, scope, account)
        .thenCompose(sealed -> publish(binding, owner, sealed, publicationProof, scope));
  }

  private void recordExpiryDelayAfterCommit(long deadlineEpochMillis) {
    LeaseTimeObservation completionObservation = null;
    boolean sampleUnavailable = false;
    try {
      completionObservation = Objects.requireNonNull(time.observe(), "Lease time observation");
    } catch (RuntimeException unavailable) {
      sampleUnavailable = true;
    }
    if (sampleUnavailable || completionObservation == null || !completionObservation.healthy()
        || completionObservation.nowLowerEpochMillis() < deadlineEpochMillis) {
      // The revoke is already finalized; an invalid diagnostic sample cannot change its result.
      metrics.recordLeaseExpiryRevocationWithoutDelaySample();
      return;
    }
    metrics.recordLeaseExpiryRevocation(
        completionObservation.nowLowerEpochMillis() - deadlineEpochMillis);
  }

  private CompletableFuture<InvocationId> completeWithoutChanges(InvocationBinding binding,
      ManagedRead managed, RequestScope scope, BudgetManager.Account account) {
    return preparation.completeWithoutChanges(binding, STEP_RESULT, managed::finish, scope, account)
        .thenApply(ignored -> binding.invocationId());
  }

  private CompletableFuture<InvocationId> publish(InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, LeaseAuxiliaryMutationPreparation.Sealed sealed,
      CommitAuthorizationProof publicationProof, RequestScope scope) {
    return currentBase(binding, owner, scope)
        .thenCompose(ignored -> publisher.publish(
            sealed.invocation(), owner, sealed.manifest(), scope, publicationProof))
        .thenCompose(outcome -> outcome.state() == CommitOutcomeResolver.State.SELECTED_FINALIZED
            ? CompletableFuture.completedFuture(binding.invocationId())
            : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN)));
  }

  private CompletableFuture<Void> currentBase(InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, RequestScope scope) {
    return control.read(scope).thenAccept(observed -> {
      CommitState state = observed.orElseThrow(() ->
          new IllegalStateException("authoritative commit control state is missing")).state();
      if (!state.profileFingerprint().equals(profileFingerprint)) {
        throw new IllegalStateException("Lease expiry control profile differs from its table group");
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

  private <T> CompletableFuture<T> withManagedRead(PinnedReadView pin, RequestScope scope,
      Function<ManagedRead, CompletableFuture<T>> operation) {
    ManagedRead managed = new ManagedRead(pin, scope);
    CompletableFuture<T> result;
    try {
      result = Objects.requireNonNull(operation.apply(managed), "expiry scan operation");
    } catch (RuntimeException | Error failure) {
      result = CompletableFuture.failedFuture(failure);
    }
    return result.handle((value, failure) -> new ReadOutcome<>(value, failure))
        .thenCompose(outcome -> managed.finish().handle((ignored, finishFailure) -> {
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
        })).copy();
  }

  private record LocatedCandidate(ExpiryCandidateIdentity identity, ExpiryCandidateRecord record) {}
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
