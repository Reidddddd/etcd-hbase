package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.ExpiryCandidateRecord;
import io.etcdhbase.core.lease.LeaseBindingIdentity;
import io.etcdhbase.core.lease.LeaseBindingVersion;
import io.etcdhbase.core.lease.LeaseEnumerationPointer;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.lease.LeaseState;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AuxiliaryLeasePlanVerifierTest {
  private static final long LEASE_ID = -17;
  private static final long BASE_SEQUENCE = 10;
  private static final long SELECTED_SEQUENCE = BASE_SEQUENCE + 1;
  private static final ByteKey STATE_PAYLOAD = ByteKey.copyOf(new byte[] {1});
  private static final ByteKey BINDING_PAYLOAD = ByteKey.copyOf(new byte[] {2});
  private static final LeaseIdentity LEASE_IDENTITY = new LeaseIdentity(LEASE_ID, 3);
  private static final ByteKey KEY = ByteKey.copyOf(new byte[] {0, (byte) 0xff, 1});

  @Test
  void acceptsMatchingStatePointerExpiryAndBindingUpdates() {
    LeaseState state = new LeaseState(LEASE_IDENTITY, 30, 20_000, 4,
        LeaseState.Status.ACTIVE, SELECTED_SEQUENCE, 1, KEY.length(), 512);
    LeaseBindingVersion binding = new LeaseBindingVersion(LEASE_IDENTITY, KEY, true,
        SELECTED_SEQUENCE, 2, 9);
    var verifier = verifier(state, binding);
    var transaction = verifier.begin(BASE_SEQUENCE);
    records(state, binding, new LeaseEnumerationPointer(SELECTED_SEQUENCE),
        new ExpiryCandidateIdentity(state.deadlineEpochMillis(), LEASE_ID,
            state.identity().incarnation(), state.renewalGeneration())).forEach(transaction::accept);

    assertDoesNotThrow(transaction::finish);
  }

  @Test
  void rejectsMissingOrMismatchedEnumerationPointer() {
    LeaseState state = activeState();
    var missing = verifier(state, null).begin(BASE_SEQUENCE);
    records(state, null, null, expiry(state)).forEach(missing::accept);
    assertThrows(IllegalStateException.class, missing::finish);

    var mismatched = verifier(state, null).begin(BASE_SEQUENCE);
    records(state, null, null, expiry(state)).forEach(mismatched::accept);
    PreparedAuxiliaryRecord wrongPointer = record(TransactionId.create(), 0,
        SystemRecordType.LEASE_ENUMERATION, LeaseRecordIdentity.enumeration(LEASE_ID),
        new LeaseEnumerationPointer(SELECTED_SEQUENCE + 1).encode());
    assertThrows(IllegalStateException.class, () -> mismatched.accept(wrongPointer));
  }

  @Test
  void rejectsExpiryCandidateThatDoesNotDescribeItsStateUpdate() {
    LeaseState state = activeState();
    var transaction = verifier(state, null).begin(BASE_SEQUENCE);
    records(state, null, new LeaseEnumerationPointer(SELECTED_SEQUENCE),
        new ExpiryCandidateIdentity(state.deadlineEpochMillis() + 1, LEASE_ID,
            state.identity().incarnation(), state.renewalGeneration())).forEach(transaction::accept);

    assertThrows(IllegalStateException.class, transaction::finish);
  }

  @Test
  void requiresBindingUpdateToMatchTheSameLeaseIncarnationAndSequence() {
    LeaseState state = activeState();
    LeaseBindingVersion wrongIncarnation = new LeaseBindingVersion(
        new LeaseIdentity(LEASE_ID, LEASE_IDENTITY.incarnation() + 1), KEY, true,
        SELECTED_SEQUENCE, 2, 9);
    var transaction = verifier(state, wrongIncarnation).begin(BASE_SEQUENCE);
    records(state, wrongIncarnation, new LeaseEnumerationPointer(SELECTED_SEQUENCE), expiry(state))
        .forEach(transaction::accept);

    assertThrows(IllegalStateException.class, transaction::finish);
  }

  @Test
  void allowsRevokedStateWithoutExpiryCandidate() {
    LeaseState revoked = new LeaseState(LEASE_IDENTITY, 30, 20_000, 4,
        LeaseState.Status.REVOKED, SELECTED_SEQUENCE, 0, 0, 0);
    var transaction = verifier(revoked, null).begin(BASE_SEQUENCE);
    records(revoked, null, new LeaseEnumerationPointer(SELECTED_SEQUENCE), null)
        .forEach(transaction::accept);

    assertDoesNotThrow(transaction::finish);
  }

  private static LeaseState activeState() {
    return new LeaseState(LEASE_IDENTITY, 30, 20_000, 4,
        LeaseState.Status.ACTIVE, SELECTED_SEQUENCE, 0, 0, 0);
  }

  private static ExpiryCandidateIdentity expiry(LeaseState state) {
    return new ExpiryCandidateIdentity(state.deadlineEpochMillis(), state.identity().leaseId(),
        state.identity().incarnation(), state.renewalGeneration());
  }

  private static AuxiliaryLeasePlanVerifier verifier(LeaseState state, LeaseBindingVersion binding) {
    Map<ByteKey, LeaseState> states = Map.of(STATE_PAYLOAD, state);
    Map<ByteKey, LeaseBindingVersion> bindings = binding == null
        ? Map.of() : Map.of(BINDING_PAYLOAD, binding);
    return new AuxiliaryLeasePlanVerifier(
        payload -> required(states, payload), payload -> required(bindings, payload));
  }

  private static List<PreparedAuxiliaryRecord> records(LeaseState state,
      LeaseBindingVersion binding, LeaseEnumerationPointer pointer, ExpiryCandidateIdentity expiry) {
    var transactionId = TransactionId.create();
    var records = new java.util.ArrayList<PreparedAuxiliaryRecord>();
    long ordinal = 0;
    if (state != null) {
      records.add(record(transactionId, ordinal++, SystemRecordType.LEASE_STATE,
          LeaseRecordIdentity.stateVersion(LEASE_ID, SELECTED_SEQUENCE), STATE_PAYLOAD));
    }
    if (pointer != null) {
      records.add(record(transactionId, ordinal++, SystemRecordType.LEASE_ENUMERATION,
          LeaseRecordIdentity.enumeration(LEASE_ID), pointer.encode()));
    }
    if (expiry != null) {
      records.add(record(transactionId, ordinal++, SystemRecordType.EXPIRY_CANDIDATE,
          expiry.encode(), new ExpiryCandidateRecord(SELECTED_SEQUENCE).encode()));
    }
    if (binding != null) {
      records.add(record(transactionId, ordinal, SystemRecordType.LEASE_BINDING,
          binding.recordIdentity().encode(), BINDING_PAYLOAD));
    }
    return List.copyOf(records);
  }

  private static PreparedAuxiliaryRecord record(TransactionId transactionId, long ordinal,
      SystemRecordType type, ByteKey identity, ByteKey payload) {
    return PreparedAuxiliaryRecord.create(transactionId, 1, BASE_SEQUENCE, ordinal,
        type, identity, payload);
  }

  private static <T> T required(Map<ByteKey, T> values, ByteKey key) {
    T value = values.get(key);
    if (value == null) throw new IllegalArgumentException("unknown test auxiliary payload");
    return value;
  }
}
