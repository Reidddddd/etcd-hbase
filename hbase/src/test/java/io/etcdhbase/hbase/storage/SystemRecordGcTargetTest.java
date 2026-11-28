package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.*;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.SystemRecordEntry;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.hbase.codec.CommitDecisionMessages;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class SystemRecordGcTargetTest {
  @Test
  void metadataOnlyDecisionSharesItsPreparedResultWindowRatherThanBlockingThePreviousRevision() {
    var transaction = TransactionId.create();
    var decision = new CommitDecision(transaction, InvocationId.create(), 11, 9, 9,
        ByteKey.copyOf(new byte[] {1}), ByteKey.copyOf(new byte[32]), false);
    var target = decode(decision);
    assertEquals(SystemRecordGcTarget.prepared(transaction, 9, 10), target);
    assertEquals(10, target.revision());
    assertEquals(11, target.sequence());
  }

  @Test
  void keyChangingAndTerminalDecisionsKeepTheirActualPublicationBounds() {
    var transaction = TransactionId.create();
    var changing = new CommitDecision(transaction, InvocationId.create(), 11, 9, 10,
        ByteKey.copyOf(new byte[] {1}), ByteKey.copyOf(new byte[32]), true);
    assertEquals(new SystemRecordGcTarget(transaction, 10, 11), decode(changing));
    var terminal = new CommitDecision(transaction, InvocationId.create(), Long.MAX_VALUE,
        Long.MAX_VALUE, Long.MAX_VALUE, ByteKey.copyOf(new byte[] {1}), ByteKey.copyOf(new byte[32]), false);
    assertEquals(Long.MAX_VALUE, decode(terminal).revision());
  }

  private static SystemRecordGcTarget decode(CommitDecision decision) {
    return SystemRecordGcTarget.decode(SystemRecordType.DECISION, new SystemRecordEntry(
        ByteKey.copyOf(ByteBuffer.allocate(8).putLong(decision.sequence()).array()),
        ByteKey.copyOf(CommitDecisionMessages.encode(decision))));
  }
}
