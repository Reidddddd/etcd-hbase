package io.etcdhbase.hbase.codec;

import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.LeaseGrantResult;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import java.util.Objects;

/** Encodes the original Grant result for the invocation-bound M2 result repository. */
public final class LeaseGrantResultMessages {
  private static final int FORMAT_VERSION = 1;
  private static final int MAX_BYTES = 128;

  private LeaseGrantResultMessages() {}

  public static ByteKey encode(LeaseGrantResult result) {
    Objects.requireNonNull(result, "result");
    byte[] encoded = CommitProtos.LeaseGrantResult.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setLeaseId(result.leaseId())
        .setGrantedTtlSeconds(result.grantedTtlSeconds())
        .setRevision(result.revision())
        .build().toByteArray();
    requireLength(encoded.length);
    return ByteKey.copyOf(encoded);
  }

  public static LeaseGrantResult decode(ByteKey encoded) {
    Objects.requireNonNull(encoded, "encoded");
    requireLength(encoded.length());
    try {
      var message = CommitProtos.LeaseGrantResult.parseFrom(encoded.bytes());
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != FORMAT_VERSION
          || !message.hasLeaseId() || !message.hasGrantedTtlSeconds() || !message.hasRevision()) {
        throw invalid();
      }
      return new LeaseGrantResult(message.getLeaseId(), message.getGrantedTtlSeconds(),
          message.getRevision());
    } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
      throw new IllegalArgumentException("invalid persisted lease grant result", failure);
    }
  }

  private static void requireLength(int length) {
    if (length <= 0 || length > MAX_BYTES) throw invalid();
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("lease grant result violates the storage contract");
  }
}
