package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/** Bounded pointer to one immutable auxiliary mutation record. */
public record AuxiliaryRecordReference(long ordinal, ByteKey identity, ByteKey digest,
    int encodedBytes) {
  public AuxiliaryRecordReference {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(digest, "digest");
    if (ordinal < 0 || identity.length() == 0 || digest.length() != 32
        || encodedBytes <= 0 || encodedBytes > PreparedAuxiliaryRecord.MAX_PAYLOAD_BYTES + 16_384) {
      throw new IllegalArgumentException("invalid auxiliary record reference");
    }
  }

  public boolean matches(PreparedAuxiliaryRecord record) {
    Objects.requireNonNull(record, "record");
    return ordinal == record.ordinal() && identity.equals(record.systemIdentity())
        && digest.equals(record.digest()) && encodedBytes == record.encodedBytes();
  }
}
