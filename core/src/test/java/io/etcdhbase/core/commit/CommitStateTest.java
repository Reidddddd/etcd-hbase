package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CommitStateTest {
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);
  private static final TransactionId TXN =
      new TransactionId(UUID.fromString("00000000-0000-0000-0000-000000000001"));
  private static final InvocationId INVOCATION =
      new InvocationId(UUID.fromString("00000000-0000-0000-0000-000000000002"));

  @Test
  void auxiliaryUpgradeFencesOldEpochAndPreservesWatermarks() {
    var owned = CommitState.initial(FINGERPRINT, 7).claimOwner("owner-a", 10, 100);
    var upgraded = owned.upgradeAuxiliaryFormat("owner-a", 1, 11);
    assertEquals(CommitState.AUXILIARY_FORMAT_VERSION, upgraded.formatVersion());
    assertEquals(2, upgraded.ownerEpoch());
    assertEquals(owned.stateVersion() + 1, upgraded.stateVersion());
    assertEquals(owned.publishedSequence(), upgraded.publishedSequence());
    assertEquals(owned.publishedRevision(), upgraded.publishedRevision());
    assertThrows(IllegalStateException.class, () -> upgraded.heartbeat("owner-a", 1, 200));
    assertEquals(CommitState.AUXILIARY_FORMAT_VERSION,
        upgraded.heartbeat("owner-a", 2, 200).formatVersion());
    assertThrows(IllegalStateException.class, () -> owned.upgradeAuxiliaryFormat("owner-a", 1, 100));
    assertThrows(IllegalStateException.class,
        () -> owned.publish(slot(0, 7, false, 1)).upgradeAuxiliaryFormat("owner-a", 1, 11));
    assertThrows(IllegalStateException.class, () -> upgraded.upgradeAuxiliaryFormat("owner-a", 2, 11));
  }

  @Test
  void metadataOnlyPublicationDoesNotAdvanceRevisionAndKeyChangesAdvanceItOnce() {
    CommitState owned = CommitState.initial(FINGERPRINT, 7).claimOwner("owner-a", 1, 100);

    CommitState metadataOnly = owned.publish(slot(0, 7, false, 1));
    assertEquals(1, metadataOnly.publishedSequence());
    assertEquals(7, metadataOnly.publishedRevision());
    assertEquals(0, metadataOnly.finalizedSequence());
    assertEquals(7, metadataOnly.finalizedRevision());

    CommitState finalizedMetadata = metadataOnly.finalizePublication(TXN);
    assertEquals(1, finalizedMetadata.finalizedSequence());
    assertEquals(7, finalizedMetadata.finalizedRevision());

    // A publication represents any number of changed keys with one commit-level revision bit.
    CommitState keyChanging = finalizedMetadata.publish(slot(1, 7, true, 1));
    assertEquals(2, keyChanging.publishedSequence());
    assertEquals(8, keyChanging.publishedRevision());
    assertEquals(7, keyChanging.finalizedRevision());
  }

  @Test
  void ownerEpochFencesOldOwnersAndExpiredLeaseCanBeClaimed() {
    CommitState firstOwner = CommitState.initial(FINGERPRINT, 1).claimOwner("owner-a", 10, 20);
    assertFalse(firstOwner.ownerLeaseExpired(19));
    assertTrue(firstOwner.ownerLeaseExpired(20));
    assertThrows(IllegalStateException.class, () -> firstOwner.claimOwner("owner-b", 19, 30));

    CommitState secondOwner = firstOwner.claimOwner("owner-b", 20, 40);
    assertEquals(2, secondOwner.ownerEpoch());
    assertThrows(IllegalStateException.class, () -> secondOwner.heartbeat("owner-a", 1, 50));
    assertEquals(50, secondOwner.heartbeat("owner-b", 2, 50).ownerLeaseDeadlineEpochMillis());
  }

  @Test
  void restoreRejectsBrokenOwnerWatermarkAndPublicationInvariants() {
    assertThrows(IllegalArgumentException.class,
        () -> CommitState.restore(ByteKey.copyOf(new byte[31]), Optional.empty(), 0, 0,
            1, 0, 1, 0, 1, Optional.empty(), 0));
    assertThrows(IllegalArgumentException.class,
        () -> CommitState.restore(FINGERPRINT, Optional.empty(), 0, 1, 1,
            0, 1, 0, 1, Optional.empty(), 0));
    assertThrows(IllegalArgumentException.class,
        () -> CommitState.restore(FINGERPRINT, Optional.empty(), 0, 0, 1,
            0, 1, 0, 1, Optional.empty(), 2));
    assertThrows(IllegalArgumentException.class,
        () -> CommitState.restore(FINGERPRINT, Optional.empty(), 0, 0, 1,
            1, 2, 0, 1, Optional.empty(), 0));
    assertThrows(IllegalArgumentException.class,
        () -> CommitState.restore(FINGERPRINT, Optional.of("owner-a"), 1, 0, 1,
            0, 1, 0, 1, Optional.empty(), 0));
  }

  @Test
  void counterAndRevisionExhaustionFailsWithoutWrapping() {
    CommitState exhaustedVersion = CommitState.restore(
        FINGERPRINT, Optional.empty(), 0, 0, Long.MAX_VALUE, 0, 1, 0, 1,
        Optional.empty(), 0);
    assertThrows(ArithmeticException.class, () -> exhaustedVersion.claimOwner("owner-a", 1, 2));

    CommitState exhaustedEpoch = CommitState.restore(
        FINGERPRINT, Optional.of("owner-a"), Long.MAX_VALUE, 1, 1, 0, 1, 0, 1,
        Optional.empty(), 0);
    assertThrows(ArithmeticException.class, () -> exhaustedEpoch.claimOwner("owner-b", 1, 2));

    assertThrows(ArithmeticException.class,
        () -> new PublicationSlot(TXN, INVOCATION, 1, Long.MAX_VALUE, 1,
            0, 1, false, ByteKey.copyOf(new byte[] {1}), ByteKey.copyOf(new byte[32])));
    assertThrows(ArithmeticException.class,
        () -> new PublicationSlot(TXN, INVOCATION, 1, 0, Long.MAX_VALUE,
            1, Long.MAX_VALUE, true, ByteKey.copyOf(new byte[] {1}), ByteKey.copyOf(new byte[32])));

    var lastRepresentablePublication = new PublicationSlot(TXN, INVOCATION, 1,
        Long.MAX_VALUE - 1, Long.MAX_VALUE - 1, Long.MAX_VALUE, Long.MAX_VALUE,
        true, ByteKey.copyOf(new byte[] {1}), ByteKey.copyOf(new byte[32]));
    assertEquals(Long.MAX_VALUE, lastRepresentablePublication.sequence());
    assertEquals(Long.MAX_VALUE, lastRepresentablePublication.revision());
  }

  private static PublicationSlot slot(long baseSequence, long baseRevision,
      boolean containsKeyChanges, long ownerEpoch) {
    return new PublicationSlot(TXN, INVOCATION, ownerEpoch, baseSequence, baseRevision,
        Math.addExact(baseSequence, 1), Math.addExact(baseRevision, containsKeyChanges ? 1 : 0),
        containsKeyChanges, ByteKey.copyOf(new byte[] {1}), ByteKey.copyOf(new byte[32]));
  }
}
