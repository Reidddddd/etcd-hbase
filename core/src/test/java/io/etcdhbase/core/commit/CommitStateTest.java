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
  void leaseBindingUpgradeFencesTheAuxiliaryOwnerBeforeChangingCapabilities() {
    var auxiliary = CommitState.initial(FINGERPRINT, 7).claimOwner("owner-a", 10, 100)
        .upgradeAuxiliaryFormat("owner-a", 1, 11);
    var bindingEnabled = auxiliary.upgradeLeaseBindingFormat("owner-a", 2, 12);

    assertEquals(CommitState.LEASE_BINDING_FORMAT_VERSION, bindingEnabled.formatVersion());
    assertTrue(bindingEnabled.supportsAuxiliaryRecords());
    assertTrue(bindingEnabled.supportsLeaseBindings());
    assertEquals(3, bindingEnabled.ownerEpoch());
    assertEquals(auxiliary.stateVersion() + 1, bindingEnabled.stateVersion());
    assertEquals(auxiliary.publishedSequence(), bindingEnabled.publishedSequence());
    assertEquals(auxiliary.publishedRevision(), bindingEnabled.publishedRevision());
    assertThrows(IllegalStateException.class,
        () -> bindingEnabled.heartbeat("owner-a", 2, 200));
    assertThrows(IllegalStateException.class,
        () -> auxiliary.upgradeLeaseBindingFormat("owner-a", 1, 12));
    assertThrows(IllegalStateException.class,
        () -> auxiliary.publish(slot(0, 7, false, 2))
            .upgradeLeaseBindingFormat("owner-a", 2, 12));
  }

  @Test
  void leaseExpiryUpgradeFencesBindingWritersBeforeEnablingCheckpointProgress() {
    var bindings = CommitState.initial(FINGERPRINT, 7).claimOwner("owner-a", 10, 100)
        .upgradeAuxiliaryFormat("owner-a", 1, 11)
        .upgradeLeaseBindingFormat("owner-a", 2, 12);
    var expiryEnabled = bindings.upgradeLeaseExpiryFormat("owner-a", 3, 13);

    assertEquals(CommitState.LEASE_EXPIRY_FORMAT_VERSION, expiryEnabled.formatVersion());
    assertTrue(expiryEnabled.supportsAuxiliaryRecords());
    assertTrue(expiryEnabled.supportsLeaseBindings());
    assertTrue(expiryEnabled.supportsLeaseExpiryCheckpoints());
    assertEquals(4, expiryEnabled.ownerEpoch());
    assertEquals(bindings.stateVersion() + 1, expiryEnabled.stateVersion());
    assertEquals(bindings.publishedSequence(), expiryEnabled.publishedSequence());
    assertEquals(bindings.publishedRevision(), expiryEnabled.publishedRevision());
    assertThrows(IllegalStateException.class,
        () -> expiryEnabled.heartbeat("owner-a", 3, 200));
    assertThrows(IllegalStateException.class,
        () -> bindings.upgradeLeaseExpiryFormat("owner-a", 2, 13));
    assertThrows(IllegalStateException.class,
        () -> bindings.publish(slot(0, 7, false, 3))
            .upgradeLeaseExpiryFormat("owner-a", 3, 13));
  }

  @Test
  void eventIndexUpgradeFencesOldOwnerAndRejectsPendingOrExpiredWork() {
    var expiry = CommitState.initial(FINGERPRINT, 7).claimOwner("owner-a", 10, 100)
        .upgradeAuxiliaryFormat("owner-a", 1, 11)
        .upgradeLeaseBindingFormat("owner-a", 2, 12)
        .upgradeLeaseExpiryFormat("owner-a", 3, 13);
    var upgraded = expiry.upgradeEventIndexFormat("owner-a", 4, 14);

    assertTrue(upgraded.supportsEventRevisionIndex());
    assertTrue(upgraded.supportsLeaseExpiryCheckpoints());
    assertEquals(5, upgraded.ownerEpoch());
    assertEquals(expiry.publishedSequence(), upgraded.publishedSequence());
    assertEquals(expiry.finalizedRevision(), upgraded.finalizedRevision());
    assertThrows(IllegalStateException.class, () -> upgraded.heartbeat("owner-a", 4, 200));
    assertThrows(IllegalStateException.class, () -> expiry.upgradeEventIndexFormat("owner-a", 3, 14));
    assertThrows(IllegalStateException.class, () -> expiry.upgradeEventIndexFormat("owner-a", 4, 100));
    assertThrows(IllegalStateException.class,
        () -> expiry.publish(slot(0, 7, false, 4)).upgradeEventIndexFormat("owner-a", 4, 14));
  }

  @Test
  void eventRegistrySelectionFencesOldOwnerAndSurvivesControlUpdates() {
    var index = CommitState.initial(FINGERPRINT, 7).claimOwner("owner-a", 10, 100)
        .upgradeAuxiliaryFormat("owner-a", 1, 11)
        .upgradeLeaseBindingFormat("owner-a", 2, 12)
        .upgradeLeaseExpiryFormat("owner-a", 3, 13)
        .upgradeEventIndexFormat("owner-a", 4, 14);
    var namespace = ByteKey.copyOf(new byte[16]);
    var reserved = index.reserveEventRegistry("owner-a", 5, 15, namespace);
    assertEquals(1, reserved.eventRegistryAttempts());
    assertEquals(Optional.of(namespace), reserved.pendingEventRegistryNamespace());
    var selected = reserved.upgradeEventRegistryFormat("owner-a", 6, 16, namespace);
    assertEquals(CommitState.EVENT_REGISTRY_FORMAT_VERSION, selected.formatVersion());
    assertEquals(Optional.of(namespace), selected.eventRegistryNamespace());
    assertEquals(7, selected.ownerEpoch());
    assertEquals(index.publishedSequence(), selected.publishedSequence());
    assertEquals(index.publishedRevision(), selected.publishedRevision());
    assertEquals(Optional.of(namespace), selected.heartbeat("owner-a", 7, 200).eventRegistryNamespace());
    assertThrows(IllegalStateException.class, () -> selected.heartbeat("owner-a", 5, 200));
    assertThrows(IllegalStateException.class, () -> index.reserveEventRegistry("owner-a", 5, 100, namespace));
    assertThrows(IllegalStateException.class, () -> index.reserveEventRegistry("owner-a", 5, 15,
        ByteKey.copyOf(new byte[15])));
    assertThrows(IllegalStateException.class, () -> reserved.upgradeEventRegistryFormat("owner-a", 6, 16,
        ByteKey.copyOf(new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})));
    assertThrows(IllegalStateException.class, () -> index.publish(slot(0, 7, false, 5))
        .reserveEventRegistry("owner-a", 5, 15, namespace));
  }

  @Test
  void authorizationBarrierIsIndependentOfControlFormatAndPreservesOwnerFence() {
    var namespace = ByteKey.copyOf(new byte[16]);
    var formatOne = CommitState.initial(FINGERPRINT, 7).claimOwner("owner-a", 10, 100);
    var barrierOne = formatOne.enableAuthorizationBarrier("owner-a", 1, 11);
    assertEquals(CommitState.FORMAT_VERSION, barrierOne.formatVersion());
    assertTrue(barrierOne.supportsAuthorizationBarrier());
    assertEquals(0, barrierOne.authorizationBarrierGeneration());
    assertEquals(2, barrierOne.ownerEpoch());
    assertEquals(formatOne.stateVersion() + 1, barrierOne.stateVersion());
    assertThrows(IllegalStateException.class,
        () -> barrierOne.heartbeat("owner-a", 1, 200));
    assertThrows(IllegalStateException.class,
        () -> formatOne.enableAuthorizationBarrier("owner-a", 1, 100));
    assertThrows(IllegalStateException.class,
        () -> barrierOne.enableAuthorizationBarrier("owner-a", 2, 20));

    var formatFour = formatOne.upgradeAuxiliaryFormat("owner-a", 1, 11)
        .upgradeLeaseBindingFormat("owner-a", 2, 12)
        .upgradeLeaseExpiryFormat("owner-a", 3, 13);
    var barrierFour = formatFour.enableAuthorizationBarrier("owner-a", 4, 14);
    assertEquals(CommitState.LEASE_EXPIRY_FORMAT_VERSION, barrierFour.formatVersion());
    assertTrue(barrierFour.eventRegistryNamespace().isEmpty());

    var barrierSix = barrierFour.upgradeEventIndexFormat("owner-a", 5, 15)
        .reserveEventRegistry("owner-a", 6, 16, namespace)
        .upgradeEventRegistryFormat("owner-a", 7, 17, namespace);
    assertEquals(CommitState.EVENT_REGISTRY_FORMAT_VERSION, barrierSix.formatVersion());
    assertTrue(barrierSix.supportsAuthorizationBarrier());
    assertEquals(0, barrierSix.authorizationBarrierGeneration());
    assertEquals(Optional.of(namespace), barrierSix.eventRegistryNamespace());
    assertThrows(IllegalStateException.class,
        () -> barrierSix.heartbeat("owner-a", 7, 200));
    assertThrows(IllegalStateException.class,
        () -> barrierSix.enableAuthorizationBarrier("owner-a", 8, 18));
    assertThrows(IllegalStateException.class,
        () -> barrierSix.advanceAuthorizationBarrier("owner-a", 8, 0)
            .enableAuthorizationBarrier("owner-a", 8, 18));
    assertThrows(IllegalStateException.class,
        () -> formatOne.publish(slot(0, 7, false, 7))
            .enableAuthorizationBarrier("owner-a", 1, 17));
  }

  @Test
  void authorizationBarrierAdvancesMonotonicallyWithoutReplacingSelectedPublication() {
    var namespace = ByteKey.copyOf(new byte[16]);
    var enabled = CommitState.initial(FINGERPRINT, 7).claimOwner("owner-a", 10, 100)
        .upgradeAuxiliaryFormat("owner-a", 1, 11)
        .upgradeLeaseBindingFormat("owner-a", 2, 12)
        .upgradeLeaseExpiryFormat("owner-a", 3, 13)
        .upgradeEventIndexFormat("owner-a", 4, 14)
        .reserveEventRegistry("owner-a", 5, 15, namespace)
        .upgradeEventRegistryFormat("owner-a", 6, 16, namespace)
        .enableAuthorizationBarrier("owner-a", 7, 17);
    var advanced = enabled.advanceAuthorizationBarrier("owner-a", 8, 0);

    assertEquals(1, advanced.authorizationBarrierGeneration());
    assertEquals(enabled.ownerEpoch(), advanced.ownerEpoch());
    assertEquals(enabled.stateVersion() + 1, advanced.stateVersion());
    assertThrows(IllegalStateException.class,
        () -> advanced.advanceAuthorizationBarrier("owner-a", 8, 0));
    assertThrows(IllegalStateException.class,
        () -> advanced.advanceAuthorizationBarrier("owner-a", 7, 1));

    var selected = advanced.publish(slot(0, 7, true, 8));
    var advancedWithSelection = selected.advanceAuthorizationBarrier("owner-a", 8, 1);
    assertEquals(2, advancedWithSelection.authorizationBarrierGeneration());
    assertEquals(selected.publication(), advancedWithSelection.publication());
    assertEquals(selected.publishedSequence(), advancedWithSelection.publishedSequence());
    assertEquals(selected.publishedRevision(), advancedWithSelection.publishedRevision());
  }

  @Test
  void authorizationBarrierRestoreRejectsNegativeAndOverflowDoesNotWrap() {
    var namespace = ByteKey.copyOf(new byte[16]);
    assertThrows(IllegalArgumentException.class, () -> CommitState.restore(
        CommitState.EVENT_REGISTRY_FORMAT_VERSION, FINGERPRINT,
        Optional.of("owner-a"), 1, 100, 1, 0, 7, 0, 7, Optional.empty(), 0,
        Optional.of(namespace), Optional.empty(), 1, true, -1));

    var exhausted = CommitState.restore(CommitState.EVENT_REGISTRY_FORMAT_VERSION,
        FINGERPRINT, Optional.of("owner-a"), 1, 100, 1, 0, 7, 0, 7,
        Optional.empty(), 0, Optional.of(namespace), Optional.empty(), 1, true, Long.MAX_VALUE);
    assertThrows(ArithmeticException.class,
        () -> exhausted.advanceAuthorizationBarrier("owner-a", 1, Long.MAX_VALUE));
  }

  @Test
  void eventRegistryAttemptsAreDurablyBounded() {
    var state = CommitState.initial(FINGERPRINT, 7).claimOwner("owner-a", 10, 100)
        .upgradeAuxiliaryFormat("owner-a", 1, 11)
        .upgradeLeaseBindingFormat("owner-a", 2, 12)
        .upgradeLeaseExpiryFormat("owner-a", 3, 13)
        .upgradeEventIndexFormat("owner-a", 4, 14);
    for (int attempt = 0; attempt < CommitState.MAX_EVENT_REGISTRY_ATTEMPTS; attempt++) {
      byte[] namespace = new byte[16];
      namespace[0] = (byte) attempt;
      state = state.reserveEventRegistry("owner-a", state.ownerEpoch(), 15, ByteKey.copyOf(namespace));
    }
    var exhausted = state;
    assertEquals(CommitState.MAX_EVENT_REGISTRY_ATTEMPTS, exhausted.eventRegistryAttempts());
    assertThrows(IllegalStateException.class, () -> exhausted.reserveEventRegistry("owner-a",
        exhausted.ownerEpoch(), 15, ByteKey.copyOf(new byte[16])));
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
