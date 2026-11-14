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
        LeaseState.Status.ACTIVE, 5, 2, 100, 200);
    var revoked = new LeaseState(active.identity(), 30, 4000, 3,
        LeaseState.Status.REVOKED, 6, 0, 0, 0);
    assertEquals(active, LeaseStateMessages.decode(LeaseStateMessages.encode(active)));
    assertEquals(revoked, LeaseStateMessages.decode(LeaseStateMessages.encode(revoked)));
  }

  @Test
  void rejectsMissingFieldsUnknownFormatStatusAndInvalidState() throws Exception {
    var state = LeaseState.grant(new LeaseIdentity(-1, 1), 30, 4000, 1);
    var message = CommitProtos.LeaseState.parseFrom(LeaseStateMessages.encode(state).bytes());
    assertInvalid(message.toBuilder().clearBoundKeyCount().build());
    assertInvalid(message.toBuilder().setFormatVersion(2).build());
    assertInvalid(message.toBuilder().setStatusValue(42).build());
    assertInvalid(message.toBuilder().setSequence(0).build());
    assertThrows(IllegalArgumentException.class,
        () -> LeaseStateMessages.decode(ByteKey.copyOf(new byte[] {(byte) 0xff})));
  }

  private static void assertInvalid(CommitProtos.LeaseState message) {
    assertThrows(IllegalArgumentException.class,
        () -> LeaseStateMessages.decode(ByteKey.copyOf(message.toByteArray())));
  }
}
