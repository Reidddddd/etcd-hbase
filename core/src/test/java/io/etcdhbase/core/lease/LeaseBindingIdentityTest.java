package io.etcdhbase.core.lease;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import org.junit.jupiter.api.Test;

class LeaseBindingIdentityTest {
  @Test
  void roundTripsBinaryKeysAndSignedLeaseIds() {
    for (long id : new long[] {Long.MIN_VALUE, -1, 1, Long.MAX_VALUE}) {
      var identity = new LeaseBindingIdentity(id, 3,
          ByteKey.copyOf(new byte[] {0, 1, (byte) 0xff, 0}), 99);
      var decoded = LeaseBindingIdentity.decode(identity.encode());
      assertEquals(identity.leaseId(), decoded.leaseId());
      assertEquals(identity.incarnation(), decoded.incarnation());
      assertEquals(identity.key(), decoded.key());
      assertEquals(identity.sequence(), decoded.sequence());
      assertEquals(identity.routePrefix(), LeaseBindingIdentity.routePrefix(identity.encode()));
      assertEquals(identity.key(), LeaseBindingIdentity.key(identity.encode()));
      assertEquals(identity.sequence(), LeaseBindingIdentity.sequence(identity.encode()));
    }
  }

  @Test
  void preservesKeyOrderBeforeSequenceAndRejectsMalformedIdentity() {
    var shortKey = new LeaseBindingIdentity(4, 1, ByteKey.copyOf(new byte[] {'a'}), 2).encode();
    var longerKey = new LeaseBindingIdentity(4, 1, ByteKey.copyOf(new byte[] {'a', 'a'}), 1).encode();
    var earlierVersion = new LeaseBindingIdentity(4, 1, ByteKey.copyOf(new byte[] {'a'}), 1).encode();

    assertEquals(-1, Integer.signum(shortKey.compareTo(longerKey)));
    assertEquals(-1, Integer.signum(earlierVersion.compareTo(shortKey)));
    assertThrows(IllegalArgumentException.class,
        () -> LeaseBindingIdentity.decode(ByteKey.copyOf(new byte[LeaseBindingIdentity.MINIMUM_BYTES])));
    assertThrows(IllegalArgumentException.class,
        () -> new LeaseBindingIdentity(4, 1, ByteKey.empty(), 1));
  }

  @Test
  void isolatesBindingRowsAcrossIncarnationsOfTheSameLeaseId() {
    ByteKey key = ByteKey.copyOf(new byte[] {'r', 0, (byte) 0xff});
    var first = new LeaseBindingIdentity(-7, 3, key, 11);
    var next = new LeaseBindingIdentity(-7, 4, key, 11);

    assertNotEquals(first.routePrefix(), next.routePrefix());
    assertNotEquals(first.encode(), next.encode());
    assertEquals(3, LeaseBindingIdentity.decode(first.encode()).incarnation());
    assertEquals(4, LeaseBindingIdentity.decode(next.encode()).incarnation());
  }
}
