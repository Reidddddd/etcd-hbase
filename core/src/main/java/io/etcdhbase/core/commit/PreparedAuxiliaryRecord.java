package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.Objects;

/** One private, immutable auxiliary update bound to a specific M2 preparation attempt. */
public final class PreparedAuxiliaryRecord {
  public static final int MAX_PAYLOAD_BYTES = 8 * 1024 * 1024;
  public static final int MAX_TARGET_IDENTITY_BYTES = 32 * 1024;

  private final TransactionId transactionId;
  private final long ownerEpoch;
  private final long baseSequence;
  private final long ordinal;
  private final SystemRecordType targetType;
  private final ByteKey targetIdentity;
  private final ByteKey payload;
  private final ByteKey digest;

  private PreparedAuxiliaryRecord(TransactionId transactionId, long ownerEpoch, long baseSequence,
      long ordinal, SystemRecordType targetType, ByteKey targetIdentity, ByteKey payload,
      ByteKey expectedDigest) {
    this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
    this.targetType = Objects.requireNonNull(targetType, "targetType");
    this.targetIdentity = Objects.requireNonNull(targetIdentity, "targetIdentity");
    this.payload = Objects.requireNonNull(payload, "payload");
    if (ownerEpoch <= 0 || baseSequence < 0 || ordinal < 0
        || !isMaterializedType(targetType)
        || targetIdentity.length() == 0 || targetIdentity.length() > MAX_TARGET_IDENTITY_BYTES
        || payload.length() == 0 || payload.length() > MAX_PAYLOAD_BYTES
        || expectedDigest != null && expectedDigest.length() != 32) {
      throw new IllegalArgumentException("invalid prepared auxiliary record");
    }
    this.ownerEpoch = ownerEpoch;
    this.baseSequence = baseSequence;
    this.ordinal = ordinal;
    this.digest = calculateDigest(transactionId, ownerEpoch, baseSequence, ordinal,
        targetType, targetIdentity, payload);
    if (expectedDigest != null && !this.digest.equals(expectedDigest)) {
      throw new IllegalArgumentException("auxiliary record digest mismatch");
    }
  }

  public static PreparedAuxiliaryRecord create(TransactionId transactionId, long ownerEpoch,
      long baseSequence, long ordinal, SystemRecordType targetType, ByteKey targetIdentity,
      ByteKey payload) {
    return new PreparedAuxiliaryRecord(transactionId, ownerEpoch, baseSequence, ordinal,
        targetType, targetIdentity, payload, null);
  }

  public static PreparedAuxiliaryRecord restore(TransactionId transactionId, long ownerEpoch,
      long baseSequence, long ordinal, SystemRecordType targetType, ByteKey targetIdentity,
      ByteKey payload, ByteKey digest) {
    return new PreparedAuxiliaryRecord(transactionId, ownerEpoch, baseSequence, ordinal,
        targetType, targetIdentity, payload, Objects.requireNonNull(digest, "digest"));
  }

  public TransactionId transactionId() { return transactionId; }
  public long ownerEpoch() { return ownerEpoch; }
  public long baseSequence() { return baseSequence; }
  public long ordinal() { return ordinal; }
  public SystemRecordType targetType() { return targetType; }
  public ByteKey targetIdentity() { return targetIdentity; }
  public ByteKey payload() { return payload; }
  public ByteKey digest() { return digest; }

  public ByteKey systemIdentity() {
    return ByteKey.copyOf(ByteBuffer.allocate(16 + 1 + Long.BYTES)
        .put(transactionId.bytes()).put(ManifestRecordFormat.AUXILIARY_RECORD)
        .putLong(ordinal).array());
  }

  public int encodedBytes() {
    return Math.addExact(96, Math.addExact(targetIdentity.length(), payload.length()));
  }

  static boolean isMaterializedType(SystemRecordType type) {
    return type == SystemRecordType.LEASE_STATE
        || type == SystemRecordType.LEASE_ENUMERATION
        || type == SystemRecordType.EXPIRY_CANDIDATE
        || type == SystemRecordType.LEASE_RESULT
        || type == SystemRecordType.LEASE_BINDING;
  }

  private static ByteKey calculateDigest(TransactionId transactionId, long ownerEpoch,
      long baseSequence, long ordinal, SystemRecordType type, ByteKey identity, ByteKey payload) {
    return ManifestDigest.sha256(output -> {
      output.writeInt(1);
      output.write(transactionId.bytes());
      output.writeLong(ownerEpoch);
      output.writeLong(baseSequence);
      output.writeLong(ordinal);
      output.writeInt(type.id());
      ManifestDigest.writeBytes(output, identity.bytes());
      ManifestDigest.writeBytes(output, payload.bytes());
    });
  }
}
