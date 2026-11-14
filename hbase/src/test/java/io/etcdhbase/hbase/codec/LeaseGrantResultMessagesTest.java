package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.LeaseGrantResult;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import org.junit.jupiter.api.Test;

class LeaseGrantResultMessagesTest {
  @Test
  void roundTripsOriginalSignedIdTtlAndRevision() {
    var result = new LeaseGrantResult(Long.MIN_VALUE, 30, 500);
    assertEquals(result, LeaseGrantResultMessages.decode(LeaseGrantResultMessages.encode(result)));
  }

  @Test
  void rejectsMissingFieldsUnknownFormatAndInvalidValues() throws Exception {
    var result = new LeaseGrantResult(-1, 30, 500);
    var message = CommitProtos.LeaseGrantResult.parseFrom(LeaseGrantResultMessages.encode(result).bytes());
    assertInvalid(message.toBuilder().clearRevision().build());
    assertInvalid(message.toBuilder().setFormatVersion(2).build());
    assertInvalid(message.toBuilder().setLeaseId(0).build());
    assertInvalid(message.toBuilder().setGrantedTtlSeconds(0).build());
    assertThrows(IllegalArgumentException.class,
        () -> LeaseGrantResultMessages.decode(ByteKey.copyOf(new byte[] {(byte) 0xff})));
  }

  private static void assertInvalid(CommitProtos.LeaseGrantResult message) {
    assertThrows(IllegalArgumentException.class,
        () -> LeaseGrantResultMessages.decode(ByteKey.copyOf(message.toByteArray())));
  }
}
