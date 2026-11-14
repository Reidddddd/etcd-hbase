package io.etcdhbase.grpc.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class LeaseTimeToLiveResponseMapperTest {
  @Test
  void includesEveryRequestedKeyAsOriginalBytesWithThePinnedHeader() {
    var request = Rpc.LeaseTimeToLiveRequest.newBuilder().setID(-7).setKeys(true).build();
    var lease = new LeaseState(new LeaseIdentity(-7, 3), 30, 30_000, 0,
        LeaseState.Status.ACTIVE, 1, 2, 3, 0);
    ByteKey binaryKey = ByteKey.copyOf(new byte[] {0, (byte) 0xff, 1});
    ByteKey secondKey = ByteKey.copyOf(new byte[] {'r', 'o', 'l', 'e'});

    Rpc.LeaseTimeToLiveResponse response = LeaseTimeToLiveResponseMapper.toWire(
        request, Optional.of(lease), 12, new LeaseTimeObservation(10_000, 10_000, true),
        List.of(binaryKey, secondKey), 21, 22);

    assertEquals(-7, response.getID());
    assertEquals(20, response.getTTL());
    assertEquals(30, response.getGrantedTTL());
    assertEquals(2, response.getKeysCount());
    assertArrayEquals(binaryKey.bytes(), response.getKeys(0).toByteArray());
    assertEquals(ByteString.copyFrom(secondKey.bytes()), response.getKeys(1));
    assertEquals(21, response.getHeader().getClusterId());
    assertEquals(22, response.getHeader().getMemberId());
    assertEquals(12, response.getHeader().getRevision());
  }

  @Test
  void missingOrRevokedLeaseUsesNativeMissingTtlAndHasNoKeys() {
    var request = Rpc.LeaseTimeToLiveRequest.newBuilder().setID(9).setKeys(true).build();
    var clock = new LeaseTimeObservation(10_000, 10_000, true);

    var missing = LeaseTimeToLiveResponseMapper.toWire(
        request, Optional.empty(), 4, clock, List.of(), 21, 22);
    var revoked = LeaseTimeToLiveResponseMapper.toWire(
        request, Optional.of(new LeaseState(new LeaseIdentity(9, 2), 30, 30_000, 0,
            LeaseState.Status.REVOKED, 1, 0, 0, 0)), 4, clock, List.of(), 21, 22);

    assertEquals(-1, missing.getTTL());
    assertEquals(0, missing.getGrantedTTL());
    assertEquals(0, missing.getKeysCount());
    assertEquals(-1, revoked.getTTL());
    assertEquals(0, revoked.getGrantedTTL());
    assertEquals(0, revoked.getKeysCount());
  }

  @Test
  void rejectsKeysForAnUnrequestedOrDifferentLeaseResponse() {
    var lease = new LeaseState(new LeaseIdentity(9, 1), 30, 30_000, 0,
        LeaseState.Status.ACTIVE, 1, 2, 3, 0);
    var clock = new LeaseTimeObservation(10_000, 10_000, true);
    var key = ByteKey.copyOf(new byte[] {1});

    assertThrows(IllegalArgumentException.class, () -> LeaseTimeToLiveResponseMapper.toWire(
        Rpc.LeaseTimeToLiveRequest.newBuilder().setID(9).build(), Optional.of(lease), 4,
        clock, List.of(key), 21, 22));
    assertThrows(IllegalArgumentException.class, () -> LeaseTimeToLiveResponseMapper.toWire(
        Rpc.LeaseTimeToLiveRequest.newBuilder().setID(8).setKeys(true).build(), Optional.of(lease), 4,
        clock, List.of(), 21, 22));
  }
}
