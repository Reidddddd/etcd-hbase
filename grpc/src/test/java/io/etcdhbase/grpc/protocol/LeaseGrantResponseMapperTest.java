package io.etcdhbase.grpc.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.lease.LeaseGrantResult;
import org.junit.jupiter.api.Test;

class LeaseGrantResponseMapperTest {
  @Test
  void preservesExplicitNegativeIdAndSelectedRevision() {
    var request = Rpc.LeaseGrantRequest.newBuilder().setID(-7).setTTL(5).build();
    var result = new LeaseGrantResult(-7, 5, 9);
    var response = LeaseGrantResponseMapper.project(request, result, decision(false), 11, 12);

    assertEquals(-7, response.getID());
    assertEquals(5, response.getTTL());
    assertEquals(11, response.getHeader().getClusterId());
    assertEquals(12, response.getHeader().getMemberId());
    assertEquals(9, response.getHeader().getRevision());
  }

  @Test
  void refusesWrongIdRevisionKeyChangesOrMissingServingIdentity() {
    var request = Rpc.LeaseGrantRequest.newBuilder().setID(3).build();
    var good = new LeaseGrantResult(3, 5, 9);
    assertThrows(IllegalArgumentException.class, () -> LeaseGrantResponseMapper.project(
        request, new LeaseGrantResult(4, 5, 9), decision(false), 11, 12));
    assertThrows(IllegalArgumentException.class, () -> LeaseGrantResponseMapper.project(
        request, new LeaseGrantResult(3, 5, 8), decision(false), 11, 12));
    assertThrows(IllegalArgumentException.class, () -> LeaseGrantResponseMapper.project(
        request, good, decision(true), 11, 12));
    assertThrows(IllegalArgumentException.class, () -> LeaseGrantResponseMapper.project(
        request, good, decision(false), 0, 12));
    assertThrows(IllegalArgumentException.class, () -> LeaseGrantResponseMapper.project(
        Rpc.LeaseGrantRequest.getDefaultInstance(), new LeaseGrantResult(-7, 5, 9),
        decision(false), 11, 12));
    assertEquals(3, LeaseGrantResponseMapper.project(
        Rpc.LeaseGrantRequest.getDefaultInstance(), good, decision(false), 11, 12).getID());
  }

  private static CommitDecision decision(boolean changesKeys) {
    return new CommitDecision(TransactionId.create(), InvocationId.create(), 1,
        changesKeys ? 8 : 9, 9, ByteKey.copyOf(new byte[] {1}), ByteKey.copyOf(new byte[32]), changesKeys);
  }
}
