package io.etcdhbase.hbase.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.storage.proto.StorageProtos;
import java.util.Optional;

/** Validates persisted messages at the storage boundary; malformed data is never an absent key. */
public final class StorageMessages {
  public static final int FORMAT_VERSION = 1;
  public static final int MAX_VALUE_BYTES = 4 * 1024 * 1024;
  private static final int MAX_METADATA_BYTES = 4096;

  private StorageMessages() {}

  /** Checks serialized metadata before an adapter copies its backing storage. */
  public static void requireMetadataLength(int length) {
    if (length < 0 || length > MAX_METADATA_BYTES) throw invalid();
  }

  /** Checks serialized payload bytes, including their protobuf framing allowance. */
  public static void requireValuePayloadLength(int length) {
    if (length < 0 || length > MAX_VALUE_BYTES + 128) throw invalid();
  }

  public static byte[] metadata(StorageMetadata metadata) {
    if (metadata.valueLength() > MAX_VALUE_BYTES) throw invalid();
    var builder = StorageProtos.RecordMetadata.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setCreateRevision(metadata.createRevision())
        .setModificationRevision(metadata.modificationRevision())
        .setVersion(metadata.version()).setLeaseId(metadata.leaseId());
    if (metadata.state() == StorageMetadata.State.TOMBSTONE) builder.setTombstone(true);
    else builder.setLiveValue(StorageProtos.LiveValueRef.newBuilder().setLength(metadata.valueLength()));
    return builder.build().toByteArray();
  }

  public static byte[] value(StoredRecord record) {
    if (record.metadata().valueLength() > MAX_VALUE_BYTES) throw invalid();
    byte[] value = record.value().orElseThrow(StorageMessages::invalid);
    return StorageProtos.ValuePayload.newBuilder().setFormatVersion(FORMAT_VERSION)
        .setModificationRevision(record.metadata().modificationRevision())
        .setValue(ByteString.copyFrom(value)).build().toByteArray();
  }

  public static StoredRecord decode(ByteKey key, byte[] encodedMetadata, Optional<byte[]> encodedValue) {
    StorageMetadata metadata = decodeMetadata(encodedMetadata);
    if (encodedValue.isEmpty()) return StoredRecord.metadataOnly(key, metadata);
    if (metadata.state() == StorageMetadata.State.TOMBSTONE) throw invalid();
    byte[] payload = encodedValue.orElseThrow();
    requireValuePayloadLength(payload.length);
    try {
      var value = StorageProtos.ValuePayload.parseFrom(payload);
      if (!value.hasFormatVersion() || value.getFormatVersion() != FORMAT_VERSION
          || !value.hasModificationRevision() || value.getModificationRevision() != metadata.modificationRevision()
          || !value.hasValue() || value.getValue().size() != metadata.valueLength()) throw invalid();
      return StoredRecord.live(key, metadata, value.getValue().toByteArray());
    } catch (InvalidProtocolBufferException failure) {
      throw new IllegalArgumentException("invalid stored value message", failure);
    }
  }

  public static StorageMetadata decodeMetadata(byte[] encoded) {
    requireMetadataLength(encoded.length);
    try {
      var metadata = StorageProtos.RecordMetadata.parseFrom(encoded);
      if (!metadata.hasFormatVersion() || metadata.getFormatVersion() != FORMAT_VERSION
          || !metadata.hasCreateRevision() || !metadata.hasModificationRevision()
          || !metadata.hasVersion() || !metadata.hasLeaseId()) throw invalid();
      StorageMetadata.State state;
      int valueLength;
      switch (metadata.getStateCase()) {
        case LIVE_VALUE -> {
          if (!metadata.getLiveValue().hasLength()) throw invalid();
          state = StorageMetadata.State.LIVE;
          valueLength = metadata.getLiveValue().getLength();
          if (valueLength < 0 || valueLength > MAX_VALUE_BYTES) throw invalid();
        }
        case TOMBSTONE -> {
          if (!metadata.getTombstone()) throw invalid();
          state = StorageMetadata.State.TOMBSTONE;
          valueLength = 0;
        }
        default -> throw invalid();
      }
      return new StorageMetadata(metadata.getCreateRevision(), metadata.getModificationRevision(),
          metadata.getVersion(), metadata.getLeaseId(), state, valueLength);
    } catch (InvalidProtocolBufferException failure) {
      throw new IllegalArgumentException("invalid stored metadata message", failure);
    }
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("stored message violates the record contract");
  }
}
