package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.ExpiryCandidateRecord;
import io.etcdhbase.core.lease.LeaseBindingIdentity;
import io.etcdhbase.core.lease.LeaseBindingVersion;
import io.etcdhbase.core.lease.LeaseEnumerationPointer;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.lease.LeaseState;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/** Verifies that one auxiliary publication updates each Lease's related indexes together. */
public final class AuxiliaryLeasePlanVerifier {
  private final Function<ByteKey, LeaseState> decodeLeaseState;
  private final Function<ByteKey, LeaseBindingVersion> decodeLeaseBinding;

  public AuxiliaryLeasePlanVerifier(Function<ByteKey, LeaseState> decodeLeaseState,
      Function<ByteKey, LeaseBindingVersion> decodeLeaseBinding) {
    this.decodeLeaseState = Objects.requireNonNull(decodeLeaseState, "decodeLeaseState");
    this.decodeLeaseBinding = Objects.requireNonNull(decodeLeaseBinding, "decodeLeaseBinding");
  }

  /** Starts a fresh bounded check for the sequence selected by a manifest based at {@code base}. */
  public Transaction begin(long baseSequence) {
    if (baseSequence < 0 || baseSequence == Long.MAX_VALUE) {
      throw new IllegalArgumentException("auxiliary base sequence cannot advance");
    }
    return new Transaction(baseSequence + 1);
  }

  public final class Transaction {
    private final long selectedSequence;
    private final Map<Long, LeaseState> states = new HashMap<>();
    private final Set<Long> enumerationIds = new HashSet<>();
    private final Map<Long, ExpiryCandidateIdentity> expiryByLease = new HashMap<>();
    private final Map<BindingKey, LeaseBindingVersion> bindings = new HashMap<>();
    private boolean finished;

    private Transaction(long selectedSequence) {
      this.selectedSequence = selectedSequence;
    }

    public void accept(PreparedAuxiliaryRecord record) {
      Objects.requireNonNull(record, "record");
      if (finished) throw new IllegalStateException("auxiliary lease validation is already finished");
      switch (record.targetType()) {
        case LEASE_STATE -> acceptState(record);
        case LEASE_ENUMERATION -> acceptEnumeration(record);
        case EXPIRY_CANDIDATE -> acceptExpiry(record);
        case LEASE_BINDING -> acceptBinding(record);
        default -> {
          // Other auxiliary domains are validated by their own materialization codecs.
        }
      }
    }

    public void finish() {
      if (finished) throw new IllegalStateException("auxiliary lease validation is already finished");
      finished = true;
      if (!states.keySet().equals(enumerationIds)) {
        throw invalid("Lease state and enumeration pointer must be updated together");
      }
      Set<Long> activeLeases = new HashSet<>();
      for (Map.Entry<Long, LeaseState> entry : states.entrySet()) {
        long leaseId = entry.getKey();
        LeaseState state = entry.getValue();
        ExpiryCandidateIdentity expiry = expiryByLease.get(leaseId);
        if (state.status() == LeaseState.Status.ACTIVE) {
          activeLeases.add(leaseId);
          if (expiry == null || expiry.deadlineEpochMillis() != state.deadlineEpochMillis()
              || expiry.leaseId() != leaseId
              || expiry.incarnation() != state.identity().incarnation()
              || expiry.renewalGeneration() != state.renewalGeneration()) {
            throw invalid("active Lease state and expiry candidate differ");
          }
        } else if (expiry != null) {
          throw invalid("revoked Lease state cannot retain an expiry candidate update");
        }
      }
      if (!activeLeases.equals(expiryByLease.keySet())) {
        throw invalid("expiry candidates must correspond to active Lease state updates");
      }
      for (LeaseBindingVersion binding : bindings.values()) {
        LeaseState state = states.get(binding.identity().leaseId());
        if (state == null || state.identity().incarnation() != binding.identity().incarnation()) {
          throw invalid("Lease binding update has no matching state update");
        }
        if (state.status() == LeaseState.Status.REVOKED && binding.bound()) {
          throw invalid("revoked Lease cannot publish a bound key");
        }
      }
    }

    private void acceptState(PreparedAuxiliaryRecord record) {
      long leaseId = LeaseRecordIdentity.leaseId(record.targetIdentity());
      long sequence = LeaseRecordIdentity.sequence(record.targetIdentity());
      LeaseState state = Objects.requireNonNull(decodeLeaseState.apply(record.payload()),
          "decoded Lease state");
      if (leaseId == 0 || sequence != selectedSequence || state.sequence() != sequence
          || state.identity().leaseId() != leaseId || states.putIfAbsent(leaseId, state) != null) {
        throw invalid("Lease state identity differs from its selected transaction");
      }
    }

    private void acceptEnumeration(PreparedAuxiliaryRecord record) {
      long leaseId = LeaseRecordIdentity.leaseId(record.targetIdentity());
      if (leaseId == 0 || record.targetIdentity().length() != LeaseRecordIdentity.ROUTE_BYTES
          || LeaseEnumerationPointer.decode(record.payload()).sequence() != selectedSequence
          || !enumerationIds.add(leaseId)) {
        throw invalid("Lease enumeration pointer differs from its selected transaction");
      }
    }

    private void acceptExpiry(PreparedAuxiliaryRecord record) {
      ExpiryCandidateIdentity expiry = ExpiryCandidateIdentity.decode(record.targetIdentity());
      if (expiry.leaseId() == 0
          || ExpiryCandidateRecord.decode(record.payload()).stateSequence() != selectedSequence
          || expiryByLease.putIfAbsent(expiry.leaseId(), expiry) != null) {
        throw invalid("Lease expiry candidate differs from its selected transaction");
      }
    }

    private void acceptBinding(PreparedAuxiliaryRecord record) {
      LeaseBindingIdentity.Decoded identity = LeaseBindingIdentity.decode(record.targetIdentity());
      LeaseBindingVersion binding = Objects.requireNonNull(decodeLeaseBinding.apply(record.payload()),
          "decoded Lease binding");
      if (identity.sequence() != selectedSequence || binding.sequence() != selectedSequence
          || binding.identity().leaseId() != identity.leaseId()
          || binding.identity().incarnation() != identity.incarnation()
          || !binding.key().equals(identity.key())
          || bindings.putIfAbsent(new BindingKey(identity.leaseId(), identity.incarnation(), identity.key()),
              binding) != null) {
        throw invalid("Lease binding identity differs from its selected transaction");
      }
    }
  }

  private record BindingKey(long leaseId, long incarnation, ByteKey key) {}

  private static IllegalStateException invalid(String message) {
    return new IllegalStateException(message);
  }
}
