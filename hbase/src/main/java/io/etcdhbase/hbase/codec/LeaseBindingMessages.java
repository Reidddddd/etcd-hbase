package io.etcdhbase.hbase.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.LeaseBindingVersion;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import java.util.Objects;

/** Versioned codec for immutable Lease-to-key binding records. */
public final class LeaseBindingMessages {
  private static final int FORMAT_VERSION = 1;
  private static final int MAX_BYTES = 32 * 1024;

  private LeaseBindingMessages() {}

  public static ByteKey encode(LeaseBindingVersion binding) {
    Objects.requireNonNull(binding, "binding");
    var message = CommitProtos.LeaseBindingPayload.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setLeaseId(binding.identity().leaseId())
        .setIncarnation(binding.identity().incarnation())
        .setKey(ByteString.copyFrom(binding.key().bytes()))
        .setBound(binding.bound())
        .setSequence(binding.sequence())
        .setKeyCreateRevision(binding.keyCreateRevision())
        .setKeyModRevision(binding.keyModRevision())
        .build();
    byte[] bytes = message.toByteArray();
    requireLength(bytes.length);
    return ByteKey.copyOf(bytes);
  }

  public static LeaseBindingVersion decode(ByteKey encoded) {
    Objects.requireNonNull(encoded, "encoded");
    requireLength(encoded.length());
    try {
      var message = CommitProtos.LeaseBindingPayload.parseFrom(encoded.bytes());
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != FORMAT_VERSION
          || !message.hasLeaseId() || !message.hasIncarnation() || !message.hasKey()
          || !message.hasBound() || !message.hasSequence()
          || !message.hasKeyCreateRevision() || !message.hasKeyModRevision()) {
        throw invalid();
      }
      return new LeaseBindingVersion(new LeaseIdentity(message.getLeaseId(), message.getIncarnation()),
          ByteKey.copyOf(message.getKey().toByteArray()), message.getBound(), message.getSequence(),
          message.getKeyCreateRevision(), message.getKeyModRevision());
    } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
      throw new IllegalArgumentException("invalid persisted Lease binding", failure);
    }
  }

  private static void requireLength(int length) {
    if (length <= 0 || length > MAX_BYTES) throw invalid();
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("Lease binding violates the storage contract");
  }
}
