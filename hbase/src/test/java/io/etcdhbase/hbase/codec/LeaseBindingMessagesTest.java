package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.LeaseBindingVersion;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import org.junit.jupiter.api.Test;

class LeaseBindingMessagesTest {
  @Test
  void roundTripsActiveAndReleasedBindingsWithBinaryKeys() {
    var active = new LeaseBindingVersion(new LeaseIdentity(Long.MIN_VALUE, 2),
        ByteKey.copyOf(new byte[] {0, 1, (byte) 0xff}), true, 5, 3, 5);
    var released = new LeaseBindingVersion(active.identity(), active.key(), false, 6, 3, 5);

    assertEquals(active, LeaseBindingMessages.decode(LeaseBindingMessages.encode(active)));
    assertEquals(released, LeaseBindingMessages.decode(LeaseBindingMessages.encode(released)));
  }

  @Test
  void rejectsMissingFieldsUnknownFormatAndInvalidMetadata() throws Exception {
    var binding = new LeaseBindingVersion(new LeaseIdentity(7, 1),
        ByteKey.copyOf(new byte[] {1}), true, 2, 1, 2);
    var message = CommitProtos.LeaseBindingPayload.parseFrom(LeaseBindingMessages.encode(binding).bytes());

    assertInvalid(message.toBuilder().clearKey().build());
    assertInvalid(message.toBuilder().setFormatVersion(2).build());
    assertInvalid(message.toBuilder().setSequence(0).build());
    assertInvalid(message.toBuilder().setKeyModRevision(0).build());
    assertThrows(IllegalArgumentException.class,
        () -> LeaseBindingMessages.decode(ByteKey.copyOf(new byte[] {(byte) 0xff})));
  }

  private static void assertInvalid(CommitProtos.LeaseBindingPayload message) {
    assertThrows(IllegalArgumentException.class,
        () -> LeaseBindingMessages.decode(ByteKey.copyOf(message.toByteArray())));
  }
}
