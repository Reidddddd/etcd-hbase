package io.etcdhbase.hbase.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.PublicationSlot;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import java.util.Optional;

/** Strict codec for the bounded, single-row M2 commit control state. */
public final class CommitControlMessages {
  public static final int MAX_CONTROL_BYTES = 64 * 1024;
  private static final int FINGERPRINT_BYTES = 32;
  private static final int MAX_MANIFEST_ROOT_BYTES = 8192;

  private CommitControlMessages() {}

  public static byte[] encode(CommitState state) {
    var builder = CommitProtos.CommitControlState.newBuilder()
        .setFormatVersion(state.formatVersion())
        .setProfileFingerprint(ByteString.copyFrom(state.profileFingerprint().bytes()))
        .setOwnerEpoch(state.ownerEpoch())
        .setOwnerLeaseDeadlineEpochMillis(state.ownerLeaseDeadlineEpochMillis())
        .setStateVersion(state.stateVersion())
        .setPublishedSequence(state.publishedSequence())
        .setPublishedRevision(state.publishedRevision())
        .setFinalizedSequence(state.finalizedSequence())
        .setFinalizedRevision(state.finalizedRevision())
        .setCompactRevision(state.compactRevision());
    state.ownerInstance().ifPresent(builder::setOwnerInstance);
    state.publication().ifPresent(slot -> builder.setPublication(encode(slot)));
    state.eventRegistryNamespace().ifPresent(value -> builder.setEventRegistryNamespace(ByteString.copyFrom(value.bytes())));
    state.pendingEventRegistryNamespace().ifPresent(value ->
        builder.setPendingEventRegistryNamespace(ByteString.copyFrom(value.bytes())));
    if (state.eventRegistryAttempts() > 0) builder.setEventRegistryAttempts(state.eventRegistryAttempts());
    if (state.supportsAuthorizationBarrier()) {
      builder.setAuthorizationBarrierGeneration(state.authorizationBarrierGeneration());
    }
    byte[] encoded = builder.build().toByteArray();
    requireLength(encoded.length);
    return encoded;
  }

  public static CommitState decode(byte[] encoded) {
    requireLength(encoded.length);
    try {
      var message = CommitProtos.CommitControlState.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion()
          || message.getFormatVersion() != CommitState.FORMAT_VERSION
              && message.getFormatVersion() != CommitState.AUXILIARY_FORMAT_VERSION
              && message.getFormatVersion() != CommitState.LEASE_BINDING_FORMAT_VERSION
              && message.getFormatVersion() != CommitState.LEASE_EXPIRY_FORMAT_VERSION
              && message.getFormatVersion() != CommitState.EVENT_INDEX_FORMAT_VERSION
              && message.getFormatVersion() != CommitState.EVENT_REGISTRY_FORMAT_VERSION
          || !message.hasProfileFingerprint() || !message.hasOwnerEpoch()
          || !message.hasOwnerLeaseDeadlineEpochMillis() || !message.hasStateVersion()
          || !message.hasPublishedSequence() || !message.hasPublishedRevision()
          || !message.hasFinalizedSequence() || !message.hasFinalizedRevision()
          || message.hasAuthorizationBarrierGeneration()
              && message.getAuthorizationBarrierGeneration() < 0
          || !message.hasCompactRevision()) {
        throw invalid();
      }
      byte[] fingerprint = message.getProfileFingerprint().toByteArray();
      if (fingerprint.length != FINGERPRINT_BYTES) throw invalid();
      long ownerEpoch = message.getOwnerEpoch();
      if ((ownerEpoch == 0) == message.hasOwnerInstance()) throw invalid();
      Optional<String> owner = message.hasOwnerInstance()
          ? Optional.of(message.getOwnerInstance()) : Optional.empty();
      Optional<PublicationSlot> publication = message.hasPublication()
          ? Optional.of(decode(message.getPublication())) : Optional.empty();
      Optional<ByteKey> registryNamespace = message.hasEventRegistryNamespace()
          ? Optional.of(ByteKey.copyOf(message.getEventRegistryNamespace().toByteArray())) : Optional.empty();
      Optional<ByteKey> pendingNamespace = message.hasPendingEventRegistryNamespace()
          ? Optional.of(ByteKey.copyOf(message.getPendingEventRegistryNamespace().toByteArray())) : Optional.empty();
      if (message.hasEventRegistryAttempts() && message.getEventRegistryAttempts() == 0) throw invalid();
      return CommitState.restore(message.getFormatVersion(), ByteKey.copyOf(fingerprint), owner, ownerEpoch,
          message.getOwnerLeaseDeadlineEpochMillis(), message.getStateVersion(),
          message.getPublishedSequence(), message.getPublishedRevision(),
          message.getFinalizedSequence(), message.getFinalizedRevision(), publication,
          message.getCompactRevision(), registryNamespace, pendingNamespace,
          message.getEventRegistryAttempts(), message.hasAuthorizationBarrierGeneration(),
          message.hasAuthorizationBarrierGeneration()
              ? message.getAuthorizationBarrierGeneration() : 0);
    } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
      throw new IllegalArgumentException("invalid persisted commit control state", failure);
    }
  }

  private static CommitProtos.PublicationSlot encode(PublicationSlot slot) {
    validateManifestRootLength(slot.manifestRoot().length());
    return CommitProtos.PublicationSlot.newBuilder()
        .setTransactionId(ByteString.copyFrom(slot.transactionId().bytes()))
        .setInvocationId(ByteString.copyFrom(slot.invocationId().bytes()))
        .setOwnerEpoch(slot.ownerEpoch())
        .setBaseSequence(slot.baseSequence())
        .setBaseRevision(slot.baseRevision())
        .setSequence(slot.sequence())
        .setRevision(slot.revision())
        .setContainsKeyChanges(slot.containsKeyChanges())
        .setManifestRoot(ByteString.copyFrom(slot.manifestRoot().bytes()))
        .setManifestDigest(ByteString.copyFrom(slot.manifestDigest().bytes()))
        .build();
  }

  private static PublicationSlot decode(CommitProtos.PublicationSlot message) {
    if (!message.getUnknownFields().asMap().isEmpty()
        || !message.hasTransactionId() || !message.hasInvocationId()
        || !message.hasOwnerEpoch() || !message.hasBaseSequence() || !message.hasBaseRevision()
        || !message.hasSequence() || !message.hasRevision()
        || !message.hasContainsKeyChanges() || !message.hasManifestRoot()
        || !message.hasManifestDigest()) {
      throw invalid();
    }
    byte[] transactionId = message.getTransactionId().toByteArray();
    byte[] invocationId = message.getInvocationId().toByteArray();
    byte[] root = message.getManifestRoot().toByteArray();
    byte[] digest = message.getManifestDigest().toByteArray();
    if (transactionId.length != 16 || invocationId.length != 16
        || digest.length != FINGERPRINT_BYTES) {
      throw invalid();
    }
    validateManifestRootLength(root.length);
    return new PublicationSlot(TransactionId.fromBytes(transactionId),
        InvocationId.fromBytes(invocationId), message.getOwnerEpoch(), message.getBaseSequence(),
        message.getBaseRevision(), message.getSequence(), message.getRevision(),
        message.getContainsKeyChanges(), ByteKey.copyOf(root), ByteKey.copyOf(digest));
  }

  private static void requireLength(int length) {
    if (length <= 0 || length > MAX_CONTROL_BYTES) throw invalid();
  }

  private static void validateManifestRootLength(int length) {
    if (length <= 0 || length > MAX_MANIFEST_ROOT_BYTES) throw invalid();
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("commit control message violates the storage contract");
  }
}
