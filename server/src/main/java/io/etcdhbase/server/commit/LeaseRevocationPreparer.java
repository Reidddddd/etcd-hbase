package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.LeaseBindingReader;
import io.etcdhbase.core.commit.LeaseReadView;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.PinnedReadView;
import io.etcdhbase.core.commit.PreparedAuxiliaryRecord;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.DeletePlanner;
import io.etcdhbase.core.lease.LeaseBindingChange;
import io.etcdhbase.core.lease.LeaseBindingVersion;
import io.etcdhbase.core.lease.LeaseCostAdmission;
import io.etcdhbase.core.lease.LeaseFailure;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.DirectRecordReader;
import io.etcdhbase.core.scan.PointResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/** Builds one immutable KV and binding removal plan for a future Revoke or expiry selector. */
public final class LeaseRevocationPreparer {
  private static final int MAX_BINDINGS = LeaseCostAdmission.MAXIMUM_SUPPORTED_BINDINGS;
  private static final long MAX_POINT_BYTES = LeaseCostAdmission.MAXIMUM_REVOCATION_POINT_BYTES;

  public static final class Prepared implements AutoCloseable {
    private final LeaseIdentity identity;
    private final List<PreparedCandidate> candidates;
    private final List<PreparedEvent> events;
    private final List<LeaseBindingChange> bindingChanges;
    private final List<PreparedAuxiliaryRecord> auxiliaryRecords;
    private final BudgetManager.Lease retainedMemory;

    private Prepared(LeaseIdentity identity, List<PreparedCandidate> candidates,
        List<PreparedEvent> events, List<LeaseBindingChange> bindingChanges,
        List<PreparedAuxiliaryRecord> auxiliaryRecords, BudgetManager.Lease retainedMemory) {
      this.identity = Objects.requireNonNull(identity, "identity");
      this.candidates = List.copyOf(candidates);
      this.events = List.copyOf(events);
      this.bindingChanges = List.copyOf(bindingChanges);
      this.auxiliaryRecords = List.copyOf(auxiliaryRecords);
      this.retainedMemory = Objects.requireNonNull(retainedMemory, "retainedMemory");
      if (this.candidates.size() != this.events.size()
          || this.candidates.size() != this.bindingChanges.size()
          || this.candidates.size() > MAX_BINDINGS) {
        throw new IllegalArgumentException("invalid Lease revocation plan");
      }
    }

    public LeaseIdentity identity() { return identity; }
    public List<PreparedCandidate> candidates() { return candidates; }
    public List<PreparedEvent> events() { return events; }
    public List<LeaseBindingChange> bindingChanges() { return bindingChanges; }
    public List<PreparedAuxiliaryRecord> auxiliaryRecords() { return auxiliaryRecords; }
    public long deletedCount() { return candidates.size(); }

    /** The caller closes this after manifest preparation/publication has physically drained. */
    @Override
    public void close() {
      retainedMemory.close();
    }
  }

  private static final class PlanBuilder {
    private final List<PreparedCandidate> candidates = new ArrayList<>();
    private final List<PreparedEvent> events = new ArrayList<>();
    private final List<LeaseBindingChange> changes = new ArrayList<>();
    private long revokeCostBytes;
  }

  private final LeaseReadView leases;
  private final LeaseBindingReader bindings;
  private final LeaseBindingMutationPlanner bindingPlanner;
  private final LeaseCostAdmission costs;
  private final DeletePlanner deletes = new DeletePlanner();

  public LeaseRevocationPreparer(LeaseReadView leases, LeaseBindingReader bindings,
      LeaseBindingMutationPlanner bindingPlanner, LeaseCostAdmission costs) {
    this.leases = Objects.requireNonNull(leases, "leases");
    this.bindings = Objects.requireNonNull(bindings, "bindings");
    this.bindingPlanner = Objects.requireNonNull(bindingPlanner, "bindingPlanner");
    this.costs = Objects.requireNonNull(costs, "costs");
  }

  /**
   * Captures the Lease and all bound KV rows at one auxiliary-protected M2 barrier. No RPC or
   * publication is started here; the returned candidates, events, and terminal state must be
   * sealed and selected by the caller in one ordinary M2 decision.
   */
  public CompletableFuture<Prepared> prepare(LeaseIdentity identity, InvocationBinding invocation,
      boolean requireExpired, PinnedReadView pinned, RecordStore currentRecords,
      RequestScope scope, BudgetManager.Account account,
      ManifestCatalog.ProofReadBudget metadataBudget,
      Supplier<CompletableFuture<Void>> finishPinnedRead) {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(pinned, "pinned");
    Objects.requireNonNull(currentRecords, "currentRecords");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(metadataBudget, "metadataBudget");
    Objects.requireNonNull(finishPinnedRead, "finishPinnedRead");
    if (identity.leaseId() == 0 || invocation.state() != InvocationBinding.State.PREPARING
        || !pinned.belongsTo(scope) || !pinned.protectsAuxiliary() || !pinned.isOpen()
        || !account.belongsTo(scope) || !metadataBudget.belongsTo(scope)
        || pinned.view().barrierSequence() != invocation.baseSequence()
        || pinned.view().targetRevision() != invocation.baseRevision()
        || pinned.view().selectedDecision().isPresent()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "Lease revocation requires the invocation's unselected auxiliary-protected view"));
    }

    return leases.find(pinned, identity.leaseId(), scope, metadataBudget).thenCompose(found -> {
      LeaseState state = found.orElseThrow(() -> new LeaseFailure(LeaseFailure.Reason.NOT_FOUND));
      if (!state.identity().equals(identity) || state.status() != LeaseState.Status.ACTIVE) {
        return CompletableFuture.failedFuture(new LeaseFailure(LeaseFailure.Reason.NOT_FOUND));
      }
      try {
        costs.requireExistingStateFits(state);
      } catch (BackendFailure invalidState) {
        return CompletableFuture.failedFuture(invalidState);
      }
      return bindings.readBindings(pinned, state, scope, account, metadataBudget)
          .thenCompose(effectiveBindings -> prepareRows(identity, state, invocation,
              requireExpired, pinned, currentRecords, scope, account,
              finishPinnedRead, effectiveBindings));
    }).copy();
  }

  private CompletableFuture<Prepared> prepareRows(LeaseIdentity identity, LeaseState state,
      InvocationBinding invocation, boolean requireExpired, PinnedReadView pinned,
      RecordStore currentRecords, RequestScope scope, BudgetManager.Account account,
      Supplier<CompletableFuture<Void>> finishPinnedRead,
      List<LeaseBindingVersion> effectiveBindings) {
    long reservedUnbindHistory = 0;
    try {
      if (effectiveBindings.size() != state.boundKeyCount()) {
        throw new IllegalStateException("Lease bindings differ from authoritative count");
      }
      for (LeaseBindingVersion binding : effectiveBindings) {
        if (!binding.bound() || !binding.identity().equals(identity)) {
          throw new IllegalStateException("Lease binding differs from the expected incarnation");
        }
        reservedUnbindHistory = Math.addExact(reservedUnbindHistory,
            costs.bindingHistoryRecordBytes(binding.key()));
      }
    } catch (RuntimeException invalidBinding) {
      return CompletableFuture.failedFuture(invalidBinding);
    }

    long retainedBytes;
    try {
      retainedBytes = Math.addExact(state.revokeCostBytes(), reservedUnbindHistory);
    } catch (ArithmeticException overflow) {
      return CompletableFuture.failedFuture(resourceLimit());
    }
    BudgetManager.Lease retainedMemory;
    try {
      retainedMemory = account.tryRetain(BudgetManager.Memory.CANDIDATE, retainedBytes)
          .orElseThrow(LeaseRevocationPreparer::resourceLimit);
    } catch (BackendFailure | RuntimeException unavailable) {
      return CompletableFuture.failedFuture(unavailable);
    }

    PlanBuilder plan = new PlanBuilder();
    DirectRecordReader reader;
    try {
      reader = new DirectRecordReader(currentRecords, account, MAX_POINT_BYTES);
    } catch (RuntimeException invalidReader) {
      retainedMemory.close();
      return CompletableFuture.failedFuture(invalidReader);
    }
    CompletableFuture<Void> bindingReads = CompletableFuture.completedFuture(null);
    for (LeaseBindingVersion binding : effectiveBindings) {
      bindingReads = bindingReads.thenCompose(ignored -> readBinding(binding, identity, invocation,
          reader, plan));
    }
    CompletableFuture<PreparedAuxiliaryPlan> operation = bindingReads.thenCompose(ignored ->
        prepareAuxiliary(identity, state, invocation, requireExpired, pinned, scope, account,
            finishPinnedRead, plan));
    return operation.thenApply(auxiliary -> new Prepared(identity, plan.candidates, plan.events,
        plan.changes, auxiliary.auxiliaryRecords(), retainedMemory))
        .whenComplete((prepared, failure) -> {
          if (failure != null) retainedMemory.close();
        });
  }

  private CompletableFuture<Void> readBinding(LeaseBindingVersion binding, LeaseIdentity identity,
      InvocationBinding invocation, DirectRecordReader reader, PlanBuilder plan) {
    return reader.readForPreparation(binding.key(), invocation.baseRevision(), ReadProjection.FULL)
        .thenAccept(point -> {
          try (point) {
            StoredRecord current = point.record().orElseThrow(() ->
                new IllegalStateException("Lease binding points to a missing KV record"));
            if (current.metadata().state() != StorageMetadata.State.LIVE || !current.hasValue()
                || current.metadata().leaseId() != identity.leaseId()
                || current.metadata().createRevision() != binding.keyCreateRevision()
                || binding.keyModRevision() > current.metadata().modificationRevision()) {
              throw new IllegalStateException("Lease binding and current KV identity differ at the M2 barrier");
            }
            DeletePlanner.Plan deletion = deletes.planForLeaseRevocation(invocation,
                Optional.of(current), plan.events.size()).orElseThrow(() ->
                    new IllegalStateException("live Lease key produced no deletion event"));
            LeaseBindingChange change = LeaseBindingChange.between(
                Optional.of(current), deletion.candidate().record()).orElseThrow(() ->
                    new IllegalStateException("Lease deletion produced no binding change"));
            plan.revokeCostBytes = Math.addExact(plan.revokeCostBytes,
                costs.revokeCostBytes(current.key(), current.value().orElseThrow().length));
            plan.candidates.add(deletion.candidate());
            plan.events.add(deletion.event());
            plan.changes.add(change);
          }
        });
  }

  private CompletableFuture<PreparedAuxiliaryPlan> prepareAuxiliary(LeaseIdentity identity,
      LeaseState state, InvocationBinding invocation, boolean requireExpired, PinnedReadView pinned,
      RequestScope scope, BudgetManager.Account account,
      Supplier<CompletableFuture<Void>> finishPinnedRead, PlanBuilder plan) {
    if (plan.revokeCostBytes != state.revokeCostBytes()) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("Lease revoke costs differ from authoritative accounting"));
    }
    LeaseBindingMutationPlanner.MutationView mutationView;
    try {
      mutationView = bindingPlanner.open(pinned, scope, account);
    } catch (RuntimeException invalidView) {
      return CompletableFuture.failedFuture(invalidView);
    }
    return mutationView.prepareRevocation(invocation, identity, plan.changes, requireExpired)
        .thenCompose(auxiliary -> {
          if (!pinned.isOpen() || scope.isClosed() || scope.isDeadlineExceeded()) {
            return CompletableFuture.failedFuture(inactive(scope));
          }
          CompletableFuture<Void> finish;
          try {
            finish = Objects.requireNonNull(finishPinnedRead.get(), "finished pinned read");
          } catch (RuntimeException | Error failure) {
            return CompletableFuture.failedFuture(failure);
          }
          return finish.thenApply(ignored -> new PreparedAuxiliaryPlan(auxiliary));
        });
  }

  private record PreparedAuxiliaryPlan(List<PreparedAuxiliaryRecord> auxiliaryRecords) {
    private PreparedAuxiliaryPlan {
      auxiliaryRecords = List.copyOf(auxiliaryRecords);
    }
  }

  private static BackendFailure resourceLimit() {
    return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
  }

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }
}
