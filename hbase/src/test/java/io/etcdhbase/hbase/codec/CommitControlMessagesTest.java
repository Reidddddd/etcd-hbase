package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.PublicationSlot;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CommitControlMessagesTest {
  private static final int MAX_MANIFEST_ROOT_BYTES = 8192;
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);
  private static final TransactionId TXN =
      new TransactionId(UUID.fromString("00000000-0000-0000-0000-000000000011"));
  private static final InvocationId INVOCATION =
      new InvocationId(UUID.fromString("00000000-0000-0000-0000-000000000012"));

  @Test
  void auxiliaryControlFormatRoundTripsWithoutChangingLegacyBytes() throws Exception {
    var legacy = CommitState.initial(FINGERPRINT, 5).claimOwner("owner-a", 10, 100);
    var upgraded = legacy.upgradeAuxiliaryFormat("owner-a", 1, 11);
    var bindingEnabled = upgraded.upgradeLeaseBindingFormat("owner-a", 2, 12);
    var expiryEnabled = bindingEnabled.upgradeLeaseExpiryFormat("owner-a", 3, 13);
    var eventIndexEnabled = expiryEnabled.upgradeEventIndexFormat("owner-a", 4, 14);
    var namespace = ByteKey.copyOf(new byte[16]);
    var registryReserved = eventIndexEnabled.reserveEventRegistry("owner-a", 5, 15, namespace);
    var registryEnabled = registryReserved.upgradeEventRegistryFormat("owner-a", 6, 16, namespace);
    var barrierEnabled = registryEnabled.enableAuthorizationBarrier("owner-a", 7, 17);
    var oldWire = CommitProtos.CommitControlState.parseFrom(CommitControlMessages.encode(legacy));
    var newWire = CommitProtos.CommitControlState.parseFrom(CommitControlMessages.encode(upgraded));
    var bindingWire = CommitProtos.CommitControlState.parseFrom(CommitControlMessages.encode(bindingEnabled));
    var expiryWire = CommitProtos.CommitControlState.parseFrom(CommitControlMessages.encode(expiryEnabled));
    assertEquals(1, oldWire.getFormatVersion());
    assertEquals(2, newWire.getFormatVersion());
    assertStateEquals(upgraded, roundTrip(upgraded));
    assertEquals(3, bindingWire.getFormatVersion());
    assertStateEquals(bindingEnabled, roundTrip(bindingEnabled));
    assertThrows(IllegalArgumentException.class, () -> CommitControlMessages.decode(
        expiryWire.toBuilder().setFormatVersion(6).build().toByteArray()));
    assertEquals(4, expiryWire.getFormatVersion());
    assertStateEquals(expiryEnabled, roundTrip(expiryEnabled));
    assertStateEquals(eventIndexEnabled, roundTrip(eventIndexEnabled));
    assertStateEquals(registryReserved, roundTrip(registryReserved));
    assertStateEquals(registryEnabled, roundTrip(registryEnabled));
    var barrierWire = CommitProtos.CommitControlState.parseFrom(CommitControlMessages.encode(barrierEnabled));
    assertEquals(CommitState.EVENT_REGISTRY_FORMAT_VERSION, barrierWire.getFormatVersion());
    assertTrue(barrierWire.hasAuthorizationBarrierGeneration());
    assertEquals(0, barrierWire.getAuthorizationBarrierGeneration());
    assertStateEquals(barrierEnabled, roundTrip(barrierEnabled));
    var withoutBarrier = CommitControlMessages.decode(
        barrierWire.toBuilder().clearAuthorizationBarrierGeneration().build().toByteArray());
    assertFalse(withoutBarrier.supportsAuthorizationBarrier());
    assertEquals(0, withoutBarrier.authorizationBarrierGeneration());
    assertThrows(IllegalArgumentException.class, () -> CommitControlMessages.decode(
        barrierWire.toBuilder().setAuthorizationBarrierGeneration(-1).build().toByteArray()));
    var barrierWithoutRegistry = CommitState.initial(FINGERPRINT, 1)
        .claimOwner("owner-a", 10, 100).enableAuthorizationBarrier("owner-a", 1, 11);
    assertEquals(CommitState.FORMAT_VERSION, barrierWithoutRegistry.formatVersion());
    assertTrue(roundTrip(barrierWithoutRegistry).supportsAuthorizationBarrier());
    assertTrue(roundTrip(barrierWithoutRegistry).eventRegistryNamespace().isEmpty());
    assertFalse(oldWire.hasAuthorizationBarrierGeneration());
    var registryWire = CommitProtos.CommitControlState.parseFrom(CommitControlMessages.encode(registryEnabled));
    assertThrows(IllegalArgumentException.class, () -> CommitControlMessages.decode(
        registryWire.toBuilder().clearEventRegistryNamespace().build().toByteArray()));
    assertThrows(IllegalArgumentException.class, () -> CommitControlMessages.decode(
        registryWire.toBuilder().setEventRegistryNamespace(ByteString.copyFrom(new byte[15])).build().toByteArray()));
    assertThrows(IllegalArgumentException.class, () -> CommitControlMessages.decode(
        registryWire.toBuilder().setFormatVersion(5).build().toByteArray()));
  }

  @Test
  void initialAndPendingPublicationStatesRoundTrip() {
    CommitState initial = CommitState.initial(FINGERPRINT, 5);
    assertStateEquals(initial, roundTrip(initial));
    CommitState pending = CommitState.initial(FINGERPRINT, 5)
        .claimOwner("owner-a", 10, 100)
        .publish(slot(0, 5, true, new byte[] {1, 2, 3}));
    assertStateEquals(pending, roundTrip(pending));
  }

  @Test
  void authorizationBarrierPresenceIsIndependentOfControlFormatAndEventRegistry() {
    var formatFour = CommitState.initial(FINGERPRINT, 1).claimOwner("owner-a", 10, 100)
        .upgradeAuxiliaryFormat("owner-a", 1, 11)
        .upgradeLeaseBindingFormat("owner-a", 2, 12)
        .upgradeLeaseExpiryFormat("owner-a", 3, 13)
        .enableAuthorizationBarrier("owner-a", 4, 14);

    var restored = roundTrip(formatFour);

    assertEquals(CommitState.LEASE_EXPIRY_FORMAT_VERSION, restored.formatVersion());
    assertTrue(restored.supportsAuthorizationBarrier());
    assertTrue(restored.eventRegistryNamespace().isEmpty());
    assertStateEquals(formatFour, restored);
  }

  @Test
  void missingRequiredFieldsAndCorruptNestedValuesAreRejected() throws Exception {
    byte[] missingFields = CommitProtos.CommitControlState.newBuilder()
        .setFormatVersion(CommitState.FORMAT_VERSION).build().toByteArray();
    assertThrows(IllegalArgumentException.class, () -> CommitControlMessages.decode(missingFields));

    CommitProtos.CommitControlState valid = CommitProtos.CommitControlState.parseFrom(
        CommitControlMessages.encode(CommitState.initial(FINGERPRINT, 5)));
    byte[] badFingerprint = valid.toBuilder()
        .setProfileFingerprint(ByteString.copyFrom(new byte[31])).build().toByteArray();
    assertThrows(IllegalArgumentException.class,
        () -> CommitControlMessages.decode(badFingerprint));

    byte[] unknownField = valid.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
        .addField(99, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build())
        .build().toByteArray();
    assertThrows(IllegalArgumentException.class, () -> CommitControlMessages.decode(unknownField));

    CommitState pending = CommitState.initial(FINGERPRINT, 5)
        .claimOwner("owner-a", 10, 100)
        .publish(slot(0, 5, true, new byte[] {1}));
    CommitProtos.CommitControlState withBadSlot = CommitProtos.CommitControlState.parseFrom(
        CommitControlMessages.encode(pending));
    CommitProtos.PublicationSlot missingSlotField = withBadSlot.getPublication().toBuilder()
        .clearManifestDigest().build();
    byte[] missingNestedField = withBadSlot.toBuilder().setPublication(missingSlotField)
        .build().toByteArray();
    assertThrows(IllegalArgumentException.class,
        () -> CommitControlMessages.decode(missingNestedField));
  }

  @Test
  void manifestRootAcceptsItsMaximumAndRejectsTheNextByte() throws Exception {
    CommitState maximumRoot = CommitState.initial(FINGERPRINT, 5)
        .claimOwner("owner-a", 10, 100)
        .publish(slot(0, 5, true, new byte[MAX_MANIFEST_ROOT_BYTES]));
    assertStateEquals(maximumRoot, roundTrip(maximumRoot));

    CommitState oversizedRoot = CommitState.initial(FINGERPRINT, 5)
        .claimOwner("owner-a", 10, 100)
        .publish(slot(0, 5, true, new byte[MAX_MANIFEST_ROOT_BYTES + 1]));
    assertThrows(IllegalArgumentException.class, () -> CommitControlMessages.encode(oversizedRoot));

    CommitProtos.CommitControlState valid = CommitProtos.CommitControlState.parseFrom(
        CommitControlMessages.encode(maximumRoot));
    CommitProtos.PublicationSlot oversizedSlot = valid.getPublication().toBuilder()
        .setManifestRoot(ByteString.copyFrom(new byte[MAX_MANIFEST_ROOT_BYTES + 1])).build();
    byte[] oversizedInput = valid.toBuilder().setPublication(oversizedSlot).build().toByteArray();
    assertThrows(IllegalArgumentException.class,
        () -> CommitControlMessages.decode(oversizedInput));
  }

  @Test
  void decoderRejectsControlMessagesAboveTheByteLimit() {
    assertThrows(IllegalArgumentException.class,
        () -> CommitControlMessages.decode(new byte[CommitControlMessages.MAX_CONTROL_BYTES + 1]));
  }

  private static CommitState roundTrip(CommitState expected) {
    return CommitControlMessages.decode(CommitControlMessages.encode(expected));
  }

  private static void assertStateEquals(CommitState expected, CommitState actual) {
    assertEquals(expected.formatVersion(), actual.formatVersion());
    assertEquals(expected.profileFingerprint(), actual.profileFingerprint());
    assertEquals(expected.ownerInstance(), actual.ownerInstance());
    assertEquals(expected.ownerEpoch(), actual.ownerEpoch());
    assertEquals(expected.ownerLeaseDeadlineEpochMillis(), actual.ownerLeaseDeadlineEpochMillis());
    assertEquals(expected.stateVersion(), actual.stateVersion());
    assertEquals(expected.publishedSequence(), actual.publishedSequence());
    assertEquals(expected.publishedRevision(), actual.publishedRevision());
    assertEquals(expected.finalizedSequence(), actual.finalizedSequence());
    assertEquals(expected.finalizedRevision(), actual.finalizedRevision());
    assertEquals(expected.publication(), actual.publication());
    assertEquals(expected.compactRevision(), actual.compactRevision());
    assertEquals(expected.eventRegistryNamespace(), actual.eventRegistryNamespace());
    assertEquals(expected.pendingEventRegistryNamespace(), actual.pendingEventRegistryNamespace());
    assertEquals(expected.eventRegistryAttempts(), actual.eventRegistryAttempts());
    assertEquals(expected.supportsAuthorizationBarrier(), actual.supportsAuthorizationBarrier());
    assertEquals(expected.authorizationBarrierGeneration(), actual.authorizationBarrierGeneration());
  }

  private static PublicationSlot slot(long baseSequence, long baseRevision,
      boolean containsKeyChanges, byte[] manifestRoot) {
    return new PublicationSlot(TXN, INVOCATION, 1, baseSequence, baseRevision,
        baseSequence + 1, baseRevision + (containsKeyChanges ? 1 : 0), containsKeyChanges,
        ByteKey.copyOf(manifestRoot), ByteKey.copyOf(new byte[32]));
  }
}
