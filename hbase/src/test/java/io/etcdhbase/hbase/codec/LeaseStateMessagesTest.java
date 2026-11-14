package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import org.junit.jupiter.api.Test;

class LeaseStateMessagesTest {
  @Test
  void roundTripsSignedIdsAndBothStates() {
    var active = new LeaseState(new LeaseIdentity(Long.MIN_VALUE, 2), 30, 4000, 3,
        LeaseState.Status.ACTIVE, 5, 2, 2, 100, 200, 8192);
    var revoked = new LeaseState(active.identity(), 30, 4000, 3,
        LeaseState.Status.REVOKED, 6, 2, 0, 0, 0, 8192);
    assertEquals(active, LeaseStateMessages.decode(LeaseStateMessages.encode(active)));
    assertEquals(revoked, LeaseStateMessages.decode(LeaseStateMessages.encode(revoked)));
  }

  @Test
  void rejectsMissingFieldsUnknownFormatStatusAndInvalidState() throws Exception {
    var state = LeaseState.grant(new LeaseIdentity(-1, 1), 30, 4000, 1);
    var message = CommitProtos.LeaseState.parseFrom(LeaseStateMessages.encode(state).bytes());
    assertInvalid(message.toBuilder().clearBoundKeyCount().build());
    assertInvalid(message.toBuilder().setFormatVersion(4).build());
    assertInvalid(message.toBuilder().clearExpiryCandidateSequence().build());
    assertInvalid(message.toBuilder().clearBindingHistoryBytes().build());
    assertInvalid(message.toBuilder().setStatusValue(42).build());
    assertInvalid(message.toBuilder().setSequence(0).build());
    assertThrows(IllegalArgumentException.class,
        () -> LeaseStateMessages.decode(ByteKey.copyOf(new byte[] {(byte) 0xff})));
  }

  @Test
  void readsLegacyStateWithExpirySequenceEqualToItsStateSequence() {
    var legacy = CommitProtos.LeaseState.newBuilder()
        .setFormatVersion(1)
        .setLeaseId(-7)
        .setIncarnation(1)
        .setGrantedTtlSeconds(30)
        .setDeadlineEpochMillis(40_000)
        .setRenewalGeneration(0)
        .setStatus(CommitProtos.LeaseStatus.LEASE_ACTIVE)
        .setSequence(9)
        .setBoundKeyCount(0)
        .setBoundKeyBytes(0)
        .setRevokeCostBytes(0)
        .build();

    assertEquals(9, LeaseStateMessages.decode(ByteKey.copyOf(legacy.toByteArray()))
        .expiryCandidateSequence());
  }

  @Test
  void readsExpirySequenceFormatWithoutBindingHistoryAccounting() {
    var state = CommitProtos.LeaseState.newBuilder()
        .setFormatVersion(2)
        .setLeaseId(17)
        .setIncarnation(3)
        .setGrantedTtlSeconds(30)
        .setDeadlineEpochMillis(40_000)
        .setRenewalGeneration(1)
        .setStatus(CommitProtos.LeaseStatus.LEASE_ACTIVE)
        .setSequence(11)
        .setExpiryCandidateSequence(9)
        .setBoundKeyCount(0)
        .setBoundKeyBytes(0)
        .setRevokeCostBytes(0)
        .build();

    var decoded = LeaseStateMessages.decode(ByteKey.copyOf(state.toByteArray()));
    assertEquals(9, decoded.expiryCandidateSequence());
    assertEquals(0, decoded.bindingHistoryBytes());
  }

  private static void assertInvalid(CommitProtos.LeaseState message) {
    assertThrows(IllegalArgumentException.class,
        () -> LeaseStateMessages.decode(ByteKey.copyOf(message.toByteArray())));
  }
}
