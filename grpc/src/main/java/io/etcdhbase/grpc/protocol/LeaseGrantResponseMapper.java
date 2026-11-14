package io.etcdhbase.grpc.protocol;

import etcdserverpb.Rpc;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.lease.LeaseGrantResult;
import java.util.Objects;

/** Projects the original durable Grant result after its commit has been finalized. */
public final class LeaseGrantResponseMapper {
  private LeaseGrantResponseMapper() {}

  public static Rpc.LeaseGrantResponse toWire(Rpc.LeaseGrantRequest request, LeaseGrantResult result,
      InvocationResultResolver.ResolvedResult resolved, long clusterId, long memberId) {
    Objects.requireNonNull(resolved, "resolved");
    return project(request, result, resolved.decision(), clusterId, memberId);
  }

  static Rpc.LeaseGrantResponse project(Rpc.LeaseGrantRequest request, LeaseGrantResult result,
      CommitDecision decision, long clusterId, long memberId) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(result, "result");
    Objects.requireNonNull(decision, "decision");
    if (clusterId == 0 || memberId == 0 || decision.containsKeyChanges()
        || result.revision() != decision.revision()
        || request.getID() == 0 && result.leaseId() < 0
        || request.getID() != 0 && request.getID() != result.leaseId()) {
      throw new IllegalArgumentException("Grant result differs from its request, decision, or serving identity");
    }
    return Rpc.LeaseGrantResponse.newBuilder()
        .setHeader(ResponseHeaderFactory.create(clusterId, memberId, decision.revision(), 0))
        .setID(result.leaseId())
        .setTTL(result.grantedTtlSeconds())
        .build();
  }
}
