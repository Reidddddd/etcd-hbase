package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.PreparedAuxiliaryRecord;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import org.junit.jupiter.api.Test;

class ManifestMessagesTest {
  @Test
  void roundTripsTheMaximumTargetIdentitySupportedByTheCoreContract() {
    byte[] identityBytes = new byte[PreparedAuxiliaryRecord.MAX_TARGET_IDENTITY_BYTES];
    java.util.Arrays.fill(identityBytes, (byte) 0x5a);
    var identity = ByteKey.copyOf(identityBytes);
    var record = PreparedAuxiliaryRecord.create(TransactionId.create(), 3, 7, 0,
        SystemRecordType.LEASE_STATE, identity, ByteKey.copyOf(new byte[] {1}));

    PreparedAuxiliaryRecord restored = ManifestMessages.decodeAuxiliaryRecord(
        ManifestMessages.encode(record));

    assertEquals(record.transactionId(), restored.transactionId());
    assertEquals(record.ownerEpoch(), restored.ownerEpoch());
    assertEquals(record.baseSequence(), restored.baseSequence());
    assertEquals(record.ordinal(), restored.ordinal());
    assertEquals(record.targetType(), restored.targetType());
    assertEquals(record.targetIdentity(), restored.targetIdentity());
    assertEquals(record.payload(), restored.payload());
    assertEquals(record.digest(), restored.digest());
  }

  @Test
  void rejectsTargetIdentityLargerThanTheCoreContract() {
    var tooLarge = ByteKey.copyOf(new byte[PreparedAuxiliaryRecord.MAX_TARGET_IDENTITY_BYTES + 1]);
    assertThrows(IllegalArgumentException.class,
        () -> PreparedAuxiliaryRecord.create(TransactionId.create(), 3, 7, 0,
            SystemRecordType.LEASE_STATE, tooLarge, ByteKey.copyOf(new byte[] {1})));
  }
}
