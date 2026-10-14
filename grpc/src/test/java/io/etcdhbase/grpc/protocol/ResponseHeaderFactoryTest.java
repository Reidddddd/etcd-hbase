package io.etcdhbase.grpc.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import etcdserverpb.Rpc;
import org.junit.jupiter.api.Test;

class ResponseHeaderFactoryTest {
  @Test
  void preservesExplicitUint64BitPatternsAndInt64RevisionBoundaries() {
    Rpc.ResponseHeader header =
        ResponseHeaderFactory.create(Long.MIN_VALUE, -1L, Long.MIN_VALUE, Long.MAX_VALUE);

    assertEquals(Long.MIN_VALUE, header.getClusterId());
    assertEquals(-1L, header.getMemberId());
    assertEquals(Long.MIN_VALUE, header.getRevision());
    assertEquals(Long.MAX_VALUE, header.getRaftTerm());
  }

  @Test
  void doesNotInventHeaderStateForZeroInputs() {
    Rpc.ResponseHeader header = ResponseHeaderFactory.create(0, 0, 0, 0);

    assertEquals(0, header.getClusterId());
    assertEquals(0, header.getMemberId());
    assertEquals(0, header.getRevision());
    assertEquals(0, header.getRaftTerm());
  }
}
