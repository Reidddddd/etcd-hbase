package io.etcdhbase.hbase.codec;

import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import java.util.Objects;

/** Versioned representation of an immutable lease state in the system table. */
public final class LeaseStateMessages {
  private static final int FORMAT_VERSION = 1;
  private static final int MAX_BYTES = 1024;

  private LeaseStateMessages() {}

  public static ByteKey encode(LeaseState state) {
    Objects.requireNonNull(state, "state");
    var message = CommitProtos.LeaseState.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setLeaseId(state.identity().leaseId())
        .setIncarnation(state.identity().incarnation())
        .setGrantedTtlSeconds(state.grantedTtlSeconds())
        .setDeadlineEpochMillis(state.deadlineEpochMillis())
        .setRenewalGeneration(state.renewalGeneration())
        .setStatus(switch (state.status()) {
          case ACTIVE -> CommitProtos.LeaseStatus.LEASE_ACTIVE;
          case REVOKED -> CommitProtos.LeaseStatus.LEASE_REVOKED;
        })
        .setSequence(state.sequence())
        .setBoundKeyCount(state.boundKeyCount())
        .setBoundKeyBytes(state.boundKeyBytes())
        .setRevokeCostBytes(state.revokeCostBytes())
        .build();
    byte[] encoded = message.toByteArray();
    requireLength(encoded.length);
    return ByteKey.copyOf(encoded);
  }

  public static LeaseState decode(ByteKey encoded) {
    Objects.requireNonNull(encoded, "encoded");
    requireLength(encoded.length());
    try {
      var message = CommitProtos.LeaseState.parseFrom(encoded.bytes());
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != FORMAT_VERSION
          || !message.hasLeaseId() || !message.hasIncarnation()
          || !message.hasGrantedTtlSeconds() || !message.hasDeadlineEpochMillis()
          || !message.hasRenewalGeneration() || !message.hasStatus() || !message.hasSequence()
          || !message.hasBoundKeyCount() || !message.hasBoundKeyBytes()
          || !message.hasRevokeCostBytes()) {
        throw invalid();
      }
      LeaseState.Status status = switch (message.getStatus()) {
        case LEASE_ACTIVE -> LeaseState.Status.ACTIVE;
        case LEASE_REVOKED -> LeaseState.Status.REVOKED;
        default -> throw invalid();
      };
      return new LeaseState(new LeaseIdentity(message.getLeaseId(), message.getIncarnation()),
          message.getGrantedTtlSeconds(), message.getDeadlineEpochMillis(),
          message.getRenewalGeneration(), status, message.getSequence(),
          message.getBoundKeyCount(), message.getBoundKeyBytes(), message.getRevokeCostBytes());
    } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
      throw new IllegalArgumentException("invalid persisted lease state", failure);
    }
  }

  private static void requireLength(int length) {
    if (length <= 0 || length > MAX_BYTES) throw invalid();
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("lease state violates the storage contract");
  }
}
