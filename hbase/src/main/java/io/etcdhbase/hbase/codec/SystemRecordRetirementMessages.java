package io.etcdhbase.hbase.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.RetirementTombstone;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import java.util.Objects;

/**
 * Strict encoding for the tombstone that replaces a retired system record.
 *
 * <p>Format 1 is deliberately narrow: it records only what is needed to keep a retired identity
 * from being recreated and to explain who retired it. Unknown fields are rejected so a newer writer
 * cannot silently weaken an older reader's retirement decision.
 */
public final class SystemRecordRetirementMessages {
  public static final int FORMAT_VERSION = 1;
  public static final int MAX_BYTES = 4096;

  private SystemRecordRetirementMessages() {}

  public static byte[] encode(RetirementTombstone tombstone) {
    Objects.requireNonNull(tombstone, "tombstone");
    byte[] encoded = CommitProtos.SystemRecordRetirement.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setRecordType(tombstone.type().id())
        .setIdentity(ByteString.copyFrom(tombstone.identity().bytes()))
        .setRetiredValueDigest(ByteString.copyFrom(tombstone.retiredValueDigest().bytes()))
        .setMaintenanceGeneration(tombstone.maintenanceGeneration())
        .setOperationId(ByteString.copyFrom(tombstone.operationId().bytes()))
        .setReason(tombstone.reason().label())
        .setRetiredRevision(tombstone.retiredRevision())
        .setRetiredSequence(tombstone.retiredSequence())
        .build().toByteArray();
    requireEncodedLength(encoded.length);
    return encoded;
  }

  public static RetirementTombstone decode(byte[] encoded) {
    Objects.requireNonNull(encoded, "encoded");
    requireEncodedLength(encoded.length);
    CommitProtos.SystemRecordRetirement message;
    try {
      message = CommitProtos.SystemRecordRetirement.parseFrom(encoded);
    } catch (InvalidProtocolBufferException malformed) {
      throw new IllegalArgumentException("invalid system-record retirement tombstone", malformed);
    }
    if (message.getFormatVersion() != FORMAT_VERSION
        || message.getUnknownFields().asMap().size() != 0) {
      throw new IllegalArgumentException("unsupported system-record retirement tombstone");
    }
    SystemRecordType type = SystemRecordType.fromId(message.getRecordType());
    return new RetirementTombstone(type,
        ByteKey.copyOf(message.getIdentity().toByteArray()),
        ByteKey.copyOf(message.getRetiredValueDigest().toByteArray()),
        message.getMaintenanceGeneration(),
        InvocationId.fromBytes(message.getOperationId().toByteArray()),
        reason(message.getReason()),
        message.getRetiredRevision(),
        message.getRetiredSequence());
  }

  private static SystemRecordRetirement.Reason reason(String label) {
    for (SystemRecordRetirement.Reason reason : SystemRecordRetirement.Reason.values()) {
      if (reason.label().equals(label)) return reason;
    }
    throw new IllegalArgumentException("unknown system-record retirement reason");
  }

  private static void requireEncodedLength(int length) {
    if (length <= 0 || length > MAX_BYTES) {
      throw new IllegalArgumentException("system-record retirement tombstone exceeds its bound");
    }
  }
}
