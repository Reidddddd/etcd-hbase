package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.AuxiliaryLeasePlanVerifier;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.PreparedAuxiliaryRecord;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.hbase.codec.LeaseBindingMessages;
import io.etcdhbase.hbase.codec.LeaseStateMessages;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class LeaseBindingMutationPlannerTest {
  private static final long BASE_SEQUENCE = 10;
  private static final long SELECTED_SEQUENCE = BASE_SEQUENCE + 1;
  private static final LeaseIdentity IDENTITY = new LeaseIdentity(-7, 3);

  @Test
  void revokedStateUpdatesDoNotWriteNewExpiryCandidates() {
    InvocationBinding invocation = invocation();
    LeaseState revoked = new LeaseState(IDENTITY, 30, 20_000, 4,
        LeaseState.Status.REVOKED, SELECTED_SEQUENCE, 1, 0, 0, 0);
    List<PreparedAuxiliaryRecord> records = new ArrayList<>();

    LeaseBindingMutationPlanner.MutationView.appendLeaseStateRecords(records, invocation, revoked, 0);

    assertEquals(List.of(SystemRecordType.LEASE_STATE, SystemRecordType.LEASE_ENUMERATION),
        records.stream().map(PreparedAuxiliaryRecord::targetType).toList());
    assertFalse(records.stream().anyMatch(record -> record.targetType() == SystemRecordType.EXPIRY_CANDIDATE));
    AuxiliaryLeasePlanVerifier.Transaction plan = verifier().begin(BASE_SEQUENCE);
    records.forEach(plan::accept);
    assertDoesNotThrow(plan::finish);
  }

  @Test
  void activeStateUpdateIncludesItsCurrentExpiryCandidate() {
    InvocationBinding invocation = invocation();
    LeaseState active = new LeaseState(IDENTITY, 30, 20_000, 4,
        LeaseState.Status.ACTIVE, SELECTED_SEQUENCE, 3, 0, 0, 0);
    List<PreparedAuxiliaryRecord> records = new ArrayList<>();

    LeaseBindingMutationPlanner.MutationView.appendLeaseStateRecords(records, invocation, active, 0);

    assertEquals(List.of(SystemRecordType.LEASE_STATE, SystemRecordType.LEASE_ENUMERATION,
        SystemRecordType.EXPIRY_CANDIDATE),
        records.stream().map(PreparedAuxiliaryRecord::targetType).toList());
    AuxiliaryLeasePlanVerifier.Transaction plan = verifier().begin(BASE_SEQUENCE);
    records.forEach(plan::accept);
    assertDoesNotThrow(plan::finish);
  }

  private static InvocationBinding invocation() {
    ByteKey digest = ByteKey.copyOf(new byte[32]);
    return InvocationBinding.begin(InvocationId.create(), digest, digest,
        TransactionId.create(), 1, BASE_SEQUENCE, 1);
  }

  private static AuxiliaryLeasePlanVerifier verifier() {
    return new AuxiliaryLeasePlanVerifier(LeaseStateMessages::decode, LeaseBindingMessages::decode);
  }
}
