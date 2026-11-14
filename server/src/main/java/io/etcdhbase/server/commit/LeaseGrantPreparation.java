package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.PreparedAuxiliaryRecord;
import io.etcdhbase.core.commit.ResultCatalog;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.ExpiryCandidateRecord;
import io.etcdhbase.core.lease.LeaseEnumerationPointer;
import io.etcdhbase.core.lease.LeaseGrantResult;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.codec.LeaseGrantResultMessages;
import io.etcdhbase.hbase.codec.LeaseStateMessages;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Persists the original Grant response and state as one private M2 publication plan. */
public final class LeaseGrantPreparation {
  public record Sealed(InvocationBinding invocation, ManifestRoot manifest) {
    public Sealed {
      Objects.requireNonNull(invocation, "invocation");
      Objects.requireNonNull(manifest, "manifest");
      if (invocation.state() != InvocationBinding.State.SEALED
          || invocation.manifestRoot().filter(manifest.systemIdentity()::equals).isEmpty()) {
        throw new IllegalArgumentException("sealed grant invocation and manifest differ");
      }
    }
  }

  private final LeaseAuxiliaryMutationPreparation auxiliaryMutations;

  public LeaseGrantPreparation(ResultPreparation results, ResultCatalog resultCatalog,
      ManifestPreparation manifests, InvocationService invocations) {
    this(new LeaseAuxiliaryMutationPreparation(results, resultCatalog, manifests, invocations));
  }

  public LeaseGrantPreparation(LeaseAuxiliaryMutationPreparation auxiliaryMutations) {
    this.auxiliaryMutations = Objects.requireNonNull(auxiliaryMutations, "auxiliaryMutations");
  }

  LeaseAuxiliaryMutationPreparation auxiliaryMutations() {
    return auxiliaryMutations;
  }

  /** The caller owns the serial lane, fixed view and all service-scope cleanup. */
  public CompletableFuture<Sealed> prepare(InvocationBinding invocation, LeaseState state,
      RequestScope scope, BudgetManager.Account account,
      Supplier<CompletableFuture<Void>> finishPinnedRead) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(finishPinnedRead, "finishPinnedRead");
    if (invocation.state() != InvocationBinding.State.PREPARING || !account.belongsTo(scope)
        || state.status() != LeaseState.Status.ACTIVE || state.renewalGeneration() != 0
        || state.boundKeyCount() != 0 || state.boundKeyBytes() != 0 || state.revokeCostBytes() != 0
        || invocation.baseSequence() == Long.MAX_VALUE
        || state.sequence() != invocation.baseSequence() + 1) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid new Grant preparation"));
    }
    var result = new LeaseGrantResult(state.identity().leaseId(), state.grantedTtlSeconds(),
        invocation.baseRevision());
    return auxiliaryMutations.prepare(invocation,
        LeaseGrantResultMessages.encode(result), auxiliaryRecords(invocation, state).iterator(),
        finishPinnedRead, scope, account)
        .thenApply(sealed -> new Sealed(sealed.invocation(), sealed.manifest()));
  }

  private static List<PreparedAuxiliaryRecord> auxiliaryRecords(
      InvocationBinding invocation, LeaseState state) {
    long leaseId = state.identity().leaseId();
    long sequence = state.sequence();
    var expiry = new ExpiryCandidateIdentity(state.deadlineEpochMillis(), leaseId,
        state.identity().incarnation(), state.renewalGeneration());
    return List.of(
        PreparedAuxiliaryRecord.create(invocation.transactionId(), invocation.preparedOwnerEpoch(),
            invocation.baseSequence(), 0, SystemRecordType.LEASE_STATE,
            LeaseRecordIdentity.stateVersion(leaseId, sequence), LeaseStateMessages.encode(state)),
        PreparedAuxiliaryRecord.create(invocation.transactionId(), invocation.preparedOwnerEpoch(),
            invocation.baseSequence(), 1, SystemRecordType.LEASE_ENUMERATION,
            LeaseRecordIdentity.enumeration(leaseId), new LeaseEnumerationPointer(sequence).encode()),
        PreparedAuxiliaryRecord.create(invocation.transactionId(), invocation.preparedOwnerEpoch(),
            invocation.baseSequence(), 2, SystemRecordType.EXPIRY_CANDIDATE,
            expiry.encode(), new ExpiryCandidateRecord(sequence).encode()));
  }
}
