package io.etcdhbase.grpc.protocol;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeRules;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Projects one fixed-view Lease state and its complete binding set to the etcd wire response. */
public final class LeaseTimeToLiveResponseMapper {
  private LeaseTimeToLiveResponseMapper() {}

  public static Rpc.LeaseTimeToLiveResponse toWire(
      Rpc.LeaseTimeToLiveRequest request, Optional<LeaseState> found, long revision,
      LeaseTimeObservation time, List<ByteKey> keys, long clusterId, long memberId) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(found, "found");
    Objects.requireNonNull(time, "time");
    Objects.requireNonNull(keys, "keys");
    if (revision < 0 || clusterId == 0 || memberId == 0) {
      throw new IllegalArgumentException("invalid Lease TTL response header");
    }
    LeaseState state = found.orElse(null);
    if (state != null && state.identity().leaseId() != request.getID()) {
      throw new IllegalArgumentException("Lease TTL state differs from the requested ID");
    }
    if (!request.getKeys() && !keys.isEmpty()) {
      throw new IllegalArgumentException("Lease TTL keys were not requested");
    }

    var builder = Rpc.LeaseTimeToLiveResponse.newBuilder()
        .setHeader(ResponseHeaderFactory.create(clusterId, memberId, revision, 0))
        .setID(request.getID());
    if (state == null || state.status() != LeaseState.Status.ACTIVE) {
      if (!keys.isEmpty()) {
        throw new IllegalArgumentException("inactive Lease TTL response cannot contain keys");
      }
      return builder.setTTL(-1).build();
    }

    builder.setTTL(LeaseTimeRules.remainingTtlSeconds(state, time))
        .setGrantedTTL(state.grantedTtlSeconds());
    keys.forEach(key -> builder.addKeys(ByteString.copyFrom(key.bytes())));
    return builder.build();
  }
}
