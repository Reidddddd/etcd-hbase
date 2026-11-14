package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
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
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.LeaseFailure;
import io.etcdhbase.core.lease.LeaseEnumerationPointer;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.lease.LeaseTimeRules;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.codec.LeaseStateMessages;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Resolves an expired empty Lease through the same selected auxiliary commit path as Grant. */
public final class LeaseEmptyExpiryExecutor {
  private static final BudgetManager.Work FINALIZATION_CAPACITY =
      new BudgetManager.Work(0, 1, 0, 1, 0);
  private static final int MAX_METADATA_RECORDS = 1024;
  private static final long MAX_METADATA_BYTES = 64L << 20;

  enum Rejection { RECHECK, UNAVAILABLE }

  static final class RejectedBeforePreparation extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final Rejection rejection;

    private RejectedBeforePreparation(Rejection rejection, Throwable cause) {
      super(cause);
      this.rejection = Objects.requireNonNull(rejection, "rejection");
    }

    Rejection rejection() { return rejection; }
  }

  private final String tableGroup;
  private final ByteKey profileFingerprint;
  private final CommitControlStore control;
  private final ReadViewService views;
  private final LeaseReadView leases;
  private final LeaseTimeSource time;
  private final Clock ownerClock;
  private final LeaseAuxiliaryMutationPreparation preparation;
  private final CommitPublisher publisher;

  public LeaseEmptyExpiryExecutor(String tableGroup, ByteKey profileFingerprint,
      CommitControlStore control, ReadViewService views, LeaseReadView leases,
      LeaseTimeSource time, Clock ownerClock, LeaseAuxiliaryMutationPreparation preparation,
      CommitPublisher publisher) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.profileFingerprint = Objects.requireNonNull(profileFingerprint, "profileFingerprint");
    this.control = Objects.requireNonNull(control, "control");
    this.views = Objects.requireNonNull(views, "views");
    this.leases = Objects.requireNonNull(leases, "leases");
    this.time = Objects.requireNonNull(time, "time");
    this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
    this.preparation = Objects.requireNonNull(preparation, "preparation");
    this.publisher = Objects.requireNonNull(publisher, "publisher");
    if (tableGroup.isBlank() || profileFingerprint.length() != 32) {
      throw new IllegalArgumentException("invalid empty-Lease expiry configuration");
    }
  }

  /** Caller owns the serialized owner lane and resolves any prior invocation first. */
  public CompletableFuture<InvocationId> executeNew(ExpiryCandidateIdentity candidate,
      InvocationBinding binding, CommitOwnerService.OwnerToken owner,
      AuthorizedBackendIdentity identity, RequestScope scope, BudgetManager.Account account) {
    Objects.requireNonNull(candidate, "candidate");
    Objects.requireNonNull(binding, "binding");
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    if (binding.state() != InvocationBinding.State.PREPARING
        || binding.preparedOwnerEpoch() != owner.epoch() || !account.belongsTo(scope)
        || !identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
    }
    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) {
      return CompletableFuture.failedFuture(new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
    }
    CompletableFuture<InvocationId> operation;
    try {
      PutCommitPreparation.retainUntilDrain(scope, account.tryAcquire(FINALIZATION_CAPACITY)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT)));
      operation = currentBase(binding, owner, scope)
          .thenCompose(ignored -> views.openAuxiliary(binding.authorizationScopeDigest(), scope))
          .thenCompose(pin -> prepareExpiry(candidate, binding, pin, scope, account))
          .thenCompose(sealed -> currentBase(binding, owner, scope)
              .thenCompose(ignored -> publisher.publish(sealed.invocation(), owner,
                  sealed.manifest(), scope)))
          .thenCompose(outcome -> outcome.state() == CommitOutcomeResolver.State.SELECTED_FINALIZED
              ? CompletableFuture.completedFuture(binding.invocationId())
              : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN)));
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

  private CompletableFuture<LeaseAuxiliaryMutationPreparation.Sealed> prepareExpiry(
      ExpiryCandidateIdentity candidate, InvocationBinding binding, PinnedReadView pin, RequestScope scope,
      BudgetManager.Account account) {
    if (pin.view().barrierSequence() != binding.baseSequence()
        || pin.view().targetRevision() != binding.baseRevision()
        || pin.view().selectedDecision().isPresent()) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    var proof = new ManifestCatalog.ProofReadBudget(
        scope, account, MAX_METADATA_RECORDS, MAX_METADATA_BYTES);
    return leases.find(pin, candidate.leaseId(), scope, proof).thenCompose(found -> {
      if (found.isEmpty() || !matches(candidate, found.orElseThrow())) {
        return finishAndReject(pin, scope, Rejection.RECHECK, null);
      }
      LeaseState state = found.orElseThrow();
      if (state.status() != LeaseState.Status.ACTIVE) {
        return finishAndReject(pin, scope, Rejection.RECHECK, null);
      }
      if (state.boundKeyCount() != 0 || state.boundKeyBytes() != 0 || state.revokeCostBytes() != 0) {
        return finishAndReject(pin, scope, Rejection.UNAVAILABLE,
            new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      try {
        if (!LeaseTimeRules.isExpired(state, time.observe())) {
          return finishAndReject(pin, scope, Rejection.RECHECK, null);
        }
      } catch (LeaseFailure unhealthyClock) {
        return finishAndReject(pin, scope, Rejection.UNAVAILABLE, unhealthyClock);
      }
      long sequence;
      LeaseState revoked;
      try {
        sequence = Math.addExact(binding.baseSequence(), 1);
        revoked = state.revoke(sequence);
      } catch (ArithmeticException | IllegalStateException invalidState) {
        return finishAndReject(pin, scope, Rejection.UNAVAILABLE, invalidState);
      }
      List<PreparedAuxiliaryRecord> records = revokedRecords(binding, revoked);
      ByteKey result = LeaseStateMessages.encode(revoked);
      return preparation.prepare(binding, result, records.iterator(),
          () -> leases.finish(pin, scope), scope, account);
    });
  }

  private CompletableFuture<LeaseAuxiliaryMutationPreparation.Sealed> finishAndReject(
      PinnedReadView pin, RequestScope scope, Rejection rejection, Throwable cause) {
    return leases.finish(pin, scope).thenCompose(ignored -> CompletableFuture.failedFuture(
        new RejectedBeforePreparation(rejection, cause)));
  }

  private static boolean matches(ExpiryCandidateIdentity candidate, LeaseState state) {
    return candidate.leaseId() == state.identity().leaseId()
        && candidate.incarnation() == state.identity().incarnation()
        && candidate.renewalGeneration() == state.renewalGeneration()
        && candidate.deadlineEpochMillis() == state.deadlineEpochMillis();
  }

  private static List<PreparedAuxiliaryRecord> revokedRecords(
      InvocationBinding binding, LeaseState revoked) {
    long leaseId = revoked.identity().leaseId();
    long sequence = revoked.sequence();
    return List.of(
        PreparedAuxiliaryRecord.create(binding.transactionId(), binding.preparedOwnerEpoch(),
            binding.baseSequence(), 0, SystemRecordType.LEASE_STATE,
            LeaseRecordIdentity.stateVersion(leaseId, sequence), LeaseStateMessages.encode(revoked)),
        PreparedAuxiliaryRecord.create(binding.transactionId(), binding.preparedOwnerEpoch(),
            binding.baseSequence(), 1, SystemRecordType.LEASE_ENUMERATION,
            LeaseRecordIdentity.enumeration(leaseId), new LeaseEnumerationPointer(sequence).encode()));
  }

  private CompletableFuture<Void> currentBase(InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, RequestScope scope) {
    return control.read(scope).thenAccept(observed -> {
      CommitState state = observed.orElseThrow(() ->
          new IllegalStateException("authoritative commit control state is missing")).state();
      if (!state.profileFingerprint().equals(profileFingerprint)) {
        throw new IllegalStateException("Lease expiry control profile differs from its table group");
      }
      if (state.formatVersion() != CommitState.AUXILIARY_FORMAT_VERSION
          || state.ownerEpoch() != owner.epoch()
          || state.ownerInstance().filter(owner.instance()::equals).isEmpty()
          || state.ownerLeaseExpired(ownerClock.millis()) || state.publication().isPresent()
          || state.publishedSequence() != binding.baseSequence()
          || state.publishedRevision() != binding.baseRevision()) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
    });
  }
}
