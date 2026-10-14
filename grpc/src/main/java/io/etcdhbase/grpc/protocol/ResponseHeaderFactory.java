package io.etcdhbase.grpc.protocol;

import etcdserverpb.Rpc;

/** Builds protocol headers only from an explicit cluster-state snapshot. */
public final class ResponseHeaderFactory {
  private ResponseHeaderFactory() {}

  /**
   * Creates a response header without inventing cluster state. The uint64 fields use the raw
   * 64-bit representation carried by {@code long}; negative Java values represent their unsigned
   * values with the high bit set.
   */
  public static Rpc.ResponseHeader create(
      long clusterId, long memberId, long revision, long raftTerm) {
    return Rpc.ResponseHeader.newBuilder()
        .setClusterId(clusterId)
        .setMemberId(memberId)
        .setRevision(revision)
        .setRaftTerm(raftTerm)
        .build();
  }
}
