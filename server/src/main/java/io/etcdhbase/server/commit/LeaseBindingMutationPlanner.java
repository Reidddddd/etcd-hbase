package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.LeaseBindingReadView;
import io.etcdhbase.core.commit.LeaseReadView;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.PinnedReadView;
import io.etcdhbase.core.commit.PreparedAuxiliaryRecord;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.ExpiryCandidateRecord;
import io.etcdhbase.core.lease.LeaseBindingChange;
import io.etcdhbase.core.lease.LeaseBindingIdentity;
import io.etcdhbase.core.lease.LeaseBindingVersion;
import io.etcdhbase.core.lease.LeaseCostAdmission;
import io.etcdhbase.core.lease.LeaseFailure;
import io.etcdhbase.core.lease.LeaseEnumerationPointer;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.lease.LeaseStateLookup;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeRules;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.codec.LeaseBindingMessages;
import io.etcdhbase.hbase.codec.LeaseStateMessages;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletionException;

/** Plans the KV-to-Lease index delta inside the same protected M2 view as the KV mutation. */
public final class LeaseBindingMutationPlanner {
  public static final int MAX_BINDING_CHANGES_PER_MUTATION = LeaseCostAdmission.MAXIMUM_SUPPORTED_BINDINGS;
  public static final int MAX_LEASES_PER_MUTATION = 128;
  public static final int MAX_AUXILIARY_RECORDS_PER_MUTATION =
      2 * MAX_BINDING_CHANGES_PER_MUTATION + 3 * MAX_LEASES_PER_MUTATION;
  private static final int MAX_METADATA_READS = 8_192;
  private static final long MAX_METADATA_BYTES = 64L << 20;

  private final LeaseReadView leases;
  private final LeaseBindingReadView bindings;
  private final LeaseTimeSource time;
  private final LeaseCostAdmission costs;

  public LeaseBindingMutationPlanner(LeaseReadView leases, LeaseBindingReadView bindings,
      LeaseTimeSource time, LeaseCostAdmission costs) {
    this.leases = Objects.requireNonNull(leases, "leases");
    this.bindings = Objects.requireNonNull(bindings, "bindings");
    this.time = Objects.requireNonNull(time, "time");
    this.costs = Objects.requireNonNull(costs, "costs");
  }

  public MutationView open(PinnedReadView pinned, RequestScope scope, BudgetManager.Account account) {
    return new MutationView(pinned, scope, account);
  }

  /** Request-scoped cache shared by Txn preflight and its later binding plan. */
  public final class MutationView implements LeaseStateLookup {
    private final PinnedReadView pinned;
    private final RequestScope scope;
    private final BudgetManager.Account account;
    private final ManifestCatalog.ProofReadBudget metadataBudget;
    private final ConcurrentHashMap<Long, CompletableFuture<Optional<LeaseState>>> states =
        new ConcurrentHashMap<>();

    private MutationView(PinnedReadView pinned, RequestScope scope, BudgetManager.Account account) {
      this.pinned = Objects.requireNonNull(pinned, "pinned");
      this.scope = Objects.requireNonNull(scope, "scope");
      this.account = Objects.requireNonNull(account, "account");
      if (!pinned.belongsTo(scope) || !pinned.protectsAuxiliary() || !account.belongsTo(scope)) {
        throw new IllegalArgumentException("Lease mutation requires its auxiliary-protected request view");
      }
      metadataBudget = new ManifestCatalog.ProofReadBudget(
          scope, account, MAX_METADATA_READS, MAX_METADATA_BYTES);
    }

    @Override
    public CompletableFuture<LeaseState> requireAttachable(long leaseId) {
      if (leaseId == 0) {
        return CompletableFuture.failedFuture(new LeaseFailure(LeaseFailure.Reason.NOT_FOUND));
      }
      return load(leaseId).thenApply(found -> requireAttachable(found.orElseThrow(
          () -> new LeaseFailure(LeaseFailure.Reason.NOT_FOUND)), time.observe())).copy();
    }

    /** Builds immutable state and binding versions for all net KV transitions in one invocation. */
    public CompletableFuture<List<PreparedAuxiliaryRecord>> prepare(
        InvocationBinding invocation, List<LeaseBindingChange> changes) {
      return prepareInternal(invocation, changes, Map.of(), false);
    }

    /**
     * Removes every current binding and selects a terminal Lease version in one auxiliary plan.
     * Expiry workers set {@code requireExpired}; explicit Revoke leaves it false.
     */
    public CompletableFuture<List<PreparedAuxiliaryRecord>> prepareRevocation(
        InvocationBinding invocation, LeaseIdentity identity,
        List<LeaseBindingChange> changes, boolean requireExpired) {
      Objects.requireNonNull(identity, "identity");
      return prepareInternal(invocation, changes, Map.of(identity.leaseId(), identity), requireExpired);
    }

    private CompletableFuture<List<PreparedAuxiliaryRecord>> prepareInternal(
        InvocationBinding invocation, List<LeaseBindingChange> changes,
        Map<Long, LeaseIdentity> revokingLeases, boolean requireExpired) {
      Objects.requireNonNull(invocation, "invocation");
      Objects.requireNonNull(changes, "changes");
      Objects.requireNonNull(revokingLeases, "revokingLeases");
      if (invocation.state() != InvocationBinding.State.PREPARING
          || invocation.baseSequence() == Long.MAX_VALUE
          || !pinned.belongsTo(scope) || !pinned.isOpen() || scope.isClosed() || scope.isDeadlineExceeded()
          || pinned.view().barrierSequence() != invocation.baseSequence()
          || pinned.view().targetRevision() != invocation.baseRevision()
          || pinned.view().selectedDecision().isPresent()) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      if (changes.isEmpty() && revokingLeases.isEmpty()) {
        return CompletableFuture.completedFuture(List.of());
      }
      if (changes.size() > MAX_BINDING_CHANGES_PER_MUTATION) {
        return CompletableFuture.failedFuture(resourceLimit());
      }

      try {
        for (LeaseBindingChange change : changes) costs.requireBindingSupported(change.key());
      } catch (BackendFailure failure) {
        return CompletableFuture.failedFuture(failure);
      }

      List<LeaseBindingChange> ordered = changes.stream()
          .sorted(Comparator.comparing(LeaseBindingChange::key)).toList();
      TreeSet<Long> leaseIds = new TreeSet<>();
      TreeSet<Long> attachingLeaseIds = new TreeSet<>();
      for (LeaseBindingChange change : ordered) {
        if (change.previousLeaseId() != 0) leaseIds.add(change.previousLeaseId());
        if (change.currentLeaseId() != 0) {
          leaseIds.add(change.currentLeaseId());
          attachingLeaseIds.add(change.currentLeaseId());
        }
      }
      for (Map.Entry<Long, LeaseIdentity> revocation : revokingLeases.entrySet()) {
        if (revocation.getKey() == 0 || revocation.getValue().leaseId() != revocation.getKey()
            || attachingLeaseIds.contains(revocation.getKey())) {
          return CompletableFuture.failedFuture(
              new IllegalArgumentException("invalid or simultaneously attached Lease revocation"));
        }
        leaseIds.add(revocation.getKey());
      }
      if (leaseIds.size() > MAX_LEASES_PER_MUTATION) {
        return CompletableFuture.failedFuture(resourceLimit());
      }

      Map<Long, LeaseState> resolved = new HashMap<>();
      CompletableFuture<Void> loading = CompletableFuture.completedFuture(null);
      for (long leaseId : leaseIds) {
        loading = loading.thenCompose(ignored -> load(leaseId).thenAccept(found -> {
          resolved.put(leaseId, found.orElseThrow(() -> attachingLeaseIds.contains(leaseId)
              || revokingLeases.containsKey(leaseId)
                  ? new LeaseFailure(LeaseFailure.Reason.NOT_FOUND)
                  : new IllegalStateException("live KV record refers to an unknown Lease")));
        }));
      }
      return loading.thenCompose(ignored -> {
        LeaseTimeObservation observation = time.observe();
        try {
          for (long leaseId : attachingLeaseIds) {
            requireAttachable(Objects.requireNonNull(resolved.get(leaseId)), observation);
          }
          for (Map.Entry<Long, LeaseIdentity> revocation : revokingLeases.entrySet()) {
            LeaseState state = Objects.requireNonNull(resolved.get(revocation.getKey()));
            if (!state.identity().equals(revocation.getValue())
                || state.status() != LeaseState.Status.ACTIVE) {
              throw new LeaseFailure(LeaseFailure.Reason.NOT_FOUND);
            }
            if (requireExpired && !observation.healthy()) {
              throw new BackendFailure(BackendFailure.Category.UNAVAILABLE);
            }
            if (requireExpired && !LeaseTimeRules.isExpired(state, observation)) {
              throw new LeaseFailure(LeaseFailure.Reason.NOT_FOUND);
            }
          }
        } catch (BackendFailure | RuntimeException invalidState) {
          return CompletableFuture.failedFuture(invalidState);
        }
        return validatePreviousBindings(ordered, resolved).thenApply(validated ->
            createRecords(invocation, ordered, resolved, revokingLeases.keySet()));
      });
    }

    private CompletableFuture<Optional<LeaseState>> load(long leaseId) {
      return states.computeIfAbsent(leaseId,
          id -> leases.find(pinned, id, scope, metadataBudget)).copy();
    }

    private CompletableFuture<Void> validatePreviousBindings(List<LeaseBindingChange> changes,
        Map<Long, LeaseState> resolved) {
      CompletableFuture<Void> validation = CompletableFuture.completedFuture(null);
      for (LeaseBindingChange change : changes) {
        if (change.previousLeaseId() == 0) continue;
        LeaseState lease = Objects.requireNonNull(resolved.get(change.previousLeaseId()),
            "previous Lease state was not resolved");
        if (lease.status() != LeaseState.Status.ACTIVE) {
          return CompletableFuture.failedFuture(
              new IllegalStateException("live KV record refers to a non-active Lease"));
        }
        validation = validation.thenCompose(ignored ->
            bindings.find(pinned, lease.identity(), change.key(), scope, metadataBudget)
                .thenAccept(binding -> requirePreviousBinding(change, lease.identity(), binding)));
      }
      return validation;
    }

    private void requirePreviousBinding(LeaseBindingChange change, LeaseIdentity identity,
        Optional<LeaseBindingVersion> found) {
      LeaseBindingVersion binding = found.orElseThrow(() ->
          new IllegalStateException("KV record has no matching Lease binding version"));
      if (!binding.bound() || !binding.identity().equals(identity) || !binding.key().equals(change.key())
          || binding.keyCreateRevision() != change.previousCreateRevision()
          || binding.keyModRevision() > change.previousModRevision()) {
        throw new IllegalStateException("KV metadata and Lease binding version differ at the M2 barrier");
      }
    }

    private List<PreparedAuxiliaryRecord> createRecords(InvocationBinding invocation,
        List<LeaseBindingChange> changes, Map<Long, LeaseState> resolved, Set<Long> revokingLeaseIds) {
      long sequence = Math.addExact(invocation.baseSequence(), 1);
      Map<Long, Delta> deltas = new HashMap<>();
      for (LeaseBindingChange change : changes) {
        long previous = change.previousLeaseId();
        long current = change.currentLeaseId();
        if (previous != 0) {
          Delta delta = deltas.computeIfAbsent(previous, ignored -> new Delta());
          if (previous != current) delta.bindingCount = Math.subtractExact(delta.bindingCount, 1);
          delta.keyBytes = Math.subtractExact(delta.keyBytes, change.key().length());
          delta.revokeCostBytes = Math.subtractExact(delta.revokeCostBytes,
              costs.revokeCostBytes(change.key(), change.previousValueBytes()));
        }
        if (current != 0) {
          Delta delta = deltas.computeIfAbsent(current, ignored -> new Delta());
          if (previous != current) delta.bindingCount = Math.addExact(delta.bindingCount, 1);
          delta.keyBytes = Math.addExact(delta.keyBytes, change.key().length());
          delta.revokeCostBytes = Math.addExact(delta.revokeCostBytes,
              costs.revokeCostBytes(change.key(), change.currentValueBytes()));
        }
        int newVersions = change.newBindingVersionCount();
        if (newVersions > 0) {
          long historyCost = Math.multiplyExact(costs.bindingHistoryRecordBytes(change.key()), newVersions);
          if (previous != 0) {
            Delta delta = deltas.computeIfAbsent(previous, ignored -> new Delta());
            if (current == previous) {
              delta.bindingHistoryBytes = Math.addExact(delta.bindingHistoryBytes, historyCost);
            } else {
              delta.bindingHistoryBytes = Math.addExact(delta.bindingHistoryBytes,
                  costs.bindingHistoryRecordBytes(change.key()));
            }
          }
          if (current != 0 && current != previous) {
            Delta delta = deltas.computeIfAbsent(current, ignored -> new Delta());
            delta.bindingHistoryBytes = Math.addExact(delta.bindingHistoryBytes,
                costs.bindingHistoryRecordBytes(change.key()));
          } else if (previous == 0 && current != 0) {
            Delta delta = deltas.computeIfAbsent(current, ignored -> new Delta());
            delta.bindingHistoryBytes = Math.addExact(delta.bindingHistoryBytes, historyCost);
          }
        }
      }
      for (long leaseId : revokingLeaseIds) deltas.computeIfAbsent(leaseId, ignored -> new Delta());

      Map<Long, LeaseState> updatedStates = new java.util.TreeMap<>();
      for (Map.Entry<Long, Delta> entry : deltas.entrySet()) {
        LeaseState old = Objects.requireNonNull(resolved.get(entry.getKey()), "Lease state is missing");
        Delta delta = entry.getValue();
        long count = Math.addExact(old.boundKeyCount(), delta.bindingCount);
        long keyBytes = Math.addExact(old.boundKeyBytes(), delta.keyBytes);
        long revokeCostBytes = Math.addExact(old.revokeCostBytes(), delta.revokeCostBytes);
        long bindingHistoryBytes = Math.addExact(old.bindingHistoryBytes(), delta.bindingHistoryBytes);
        try {
          costs.requireWithin(count, keyBytes, revokeCostBytes, bindingHistoryBytes);
        } catch (BackendFailure failure) {
          throw new CompletionException(failure);
        }
        if (revokingLeaseIds.contains(entry.getKey())) {
          if (count != 0 || keyBytes != 0 || revokeCostBytes != 0) {
            throw new IllegalStateException("Lease revocation plan leaves live bindings or reserved cost");
          }
          updatedStates.put(entry.getKey(), new LeaseState(old.identity(), old.grantedTtlSeconds(),
              old.deadlineEpochMillis(), old.renewalGeneration(), LeaseState.Status.REVOKED,
              sequence, old.expiryCandidateSequence(), 0, 0, 0, bindingHistoryBytes));
        } else {
          updatedStates.put(entry.getKey(),
              old.withBindingAccounting(sequence, count, keyBytes, revokeCostBytes, bindingHistoryBytes));
        }
      }

      var records = new ArrayList<PreparedAuxiliaryRecord>(
          Math.addExact(Math.multiplyExact(updatedStates.size(), 3),
              changes.stream().mapToInt(LeaseBindingChange::newBindingVersionCount).sum()));
      long ordinal = 0;
      for (LeaseState state : updatedStates.values()) {
        ordinal = appendLeaseStateRecords(records, invocation, state, ordinal);
      }

      for (LeaseBindingChange change : changes) {
        LeaseState previous = change.previousLeaseId() == 0 ? null
            : Objects.requireNonNull(updatedStates.get(change.previousLeaseId()), "previous Lease update missing");
        LeaseState current = change.currentLeaseId() == 0 ? null
            : Objects.requireNonNull(updatedStates.get(change.currentLeaseId()), "current Lease update missing");
        if (change.previousLeaseId() != 0 && change.previousLeaseId() != change.currentLeaseId()) {
          var unbound = new LeaseBindingVersion(previous.identity(), change.key(), false, sequence,
              change.currentCreateRevision(), change.currentModRevision());
          ordinal = add(records, invocation, ordinal, SystemRecordType.LEASE_BINDING,
              unbound.recordIdentity().encode(), LeaseBindingMessages.encode(unbound));
        }
        if (current != null && change.changesBindingIdentity()) {
          var bound = new LeaseBindingVersion(current.identity(), change.key(), true, sequence,
              change.currentCreateRevision(), change.currentModRevision());
          ordinal = add(records, invocation, ordinal, SystemRecordType.LEASE_BINDING,
              bound.recordIdentity().encode(), LeaseBindingMessages.encode(bound));
        }
      }
      return List.copyOf(records);
    }

    static long appendLeaseStateRecords(List<PreparedAuxiliaryRecord> records,
        InvocationBinding invocation, LeaseState state, long ordinal) {
      Objects.requireNonNull(records, "records");
      Objects.requireNonNull(invocation, "invocation");
      Objects.requireNonNull(state, "state");
      if (state.sequence() != Math.addExact(invocation.baseSequence(), 1)) {
        throw new IllegalArgumentException("Lease state must match the selected sequence");
      }
      long next = add(records, invocation, ordinal, SystemRecordType.LEASE_STATE,
          LeaseRecordIdentity.stateVersion(state.identity().leaseId(), state.sequence()),
          LeaseStateMessages.encode(state));
      next = add(records, invocation, next, SystemRecordType.LEASE_ENUMERATION,
          LeaseRecordIdentity.enumeration(state.identity().leaseId()),
          new LeaseEnumerationPointer(state.sequence()).encode());
      if (state.status() == LeaseState.Status.ACTIVE) {
        ExpiryCandidateIdentity expiry = new ExpiryCandidateIdentity(state.deadlineEpochMillis(),
            state.identity().leaseId(), state.identity().incarnation(), state.renewalGeneration());
        next = add(records, invocation, next, SystemRecordType.EXPIRY_CANDIDATE,
            expiry.encode(), new ExpiryCandidateRecord(state.expiryCandidateSequence()).encode());
      }
      return next;
    }

    private static long add(List<PreparedAuxiliaryRecord> records, InvocationBinding invocation,
        long ordinal, SystemRecordType type, ByteKey identity, ByteKey payload) {
      records.add(PreparedAuxiliaryRecord.create(invocation.transactionId(),
          invocation.preparedOwnerEpoch(), invocation.baseSequence(), ordinal, type, identity, payload));
      return Math.addExact(ordinal, 1);
    }

    private LeaseState requireAttachable(LeaseState state, LeaseTimeObservation observation) {
      if (!observation.healthy()) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      if (state.status() != LeaseState.Status.ACTIVE
          || observation.nowUpperEpochMillis() >= state.deadlineEpochMillis()
          || LeaseTimeRules.isExpired(state, observation)) {
        throw new LeaseFailure(LeaseFailure.Reason.NOT_FOUND);
      }
      return state;
    }
  }

  private static final class Delta {
    private long bindingCount;
    private long keyBytes;
    private long revokeCostBytes;
    private long bindingHistoryBytes;
  }

  private static BackendFailure resourceLimit() {
    return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
  }
}
