package io.etcdhbase.core.lease;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.OrderedByteKeyEncoding;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Canonical logical identity for one immutable Lease-to-key binding version. */
public record LeaseBindingIdentity(long leaseId, long incarnation, ByteKey key, long sequence) {
  public static final int ROUTE_BYTES = Long.BYTES * 2;
  public static final int MINIMUM_BYTES = ROUTE_BYTES + 2 + Long.BYTES;

  public LeaseBindingIdentity {
    Objects.requireNonNull(key, "key");
    if (leaseId == 0 || incarnation <= 0 || key.length() == 0 || sequence <= 0) {
      throw new IllegalArgumentException("invalid Lease binding identity");
    }
  }

  public ByteKey encode() {
    byte[] keyBytes = key.bytes();
    byte[] orderedKey = OrderedByteKeyEncoding.encode(keyBytes);
    return ByteKey.copyOf(ByteBuffer.allocate(Math.addExact(ROUTE_BYTES + Long.BYTES, orderedKey.length))
        .putLong(leaseId ^ Long.MIN_VALUE).putLong(incarnation).put(orderedKey).putLong(sequence).array());
  }

  public ByteKey routePrefix() {
    return routePrefix(leaseId, incarnation);
  }

  /** Prefix used for one key's ordered immutable binding history. */
  public static ByteKey keyRoutePrefix(long leaseId, long incarnation, ByteKey key) {
    Objects.requireNonNull(key, "key");
    if (leaseId == 0 || incarnation <= 0 || key.length() == 0) {
      throw new IllegalArgumentException("invalid Lease binding key route");
    }
    byte[] route = routePrefix(leaseId, incarnation).bytes();
    byte[] orderedKey = OrderedByteKeyEncoding.encode(key.bytes());
    return ByteKey.copyOf(ByteBuffer.allocate(Math.addExact(route.length, orderedKey.length))
        .put(route).put(orderedKey).array());
  }

  public static ByteKey routePrefix(long leaseId, long incarnation) {
    if (leaseId == 0 || incarnation <= 0) {
      throw new IllegalArgumentException("invalid Lease binding route");
    }
    return ByteKey.copyOf(ByteBuffer.allocate(ROUTE_BYTES)
        .putLong(leaseId ^ Long.MIN_VALUE).putLong(incarnation).array());
  }

  public static ByteKey routePrefix(ByteKey identity) {
    Decoded decoded = decode(identity);
    return routePrefix(decoded.leaseId(), decoded.incarnation());
  }

  public static ByteKey key(ByteKey identity) {
    return decode(identity).key();
  }

  public static long sequence(ByteKey identity) {
    return decode(identity).sequence();
  }

  public static Decoded decode(ByteKey identity) {
    Objects.requireNonNull(identity, "identity");
    byte[] bytes = identity.bytes();
    if (bytes.length < MINIMUM_BYTES) throw invalid();
    ByteBuffer fields = ByteBuffer.wrap(bytes);
    long leaseId = fields.getLong() ^ Long.MIN_VALUE;
    long incarnation = fields.getLong();
    OrderedByteKeyEncoding.Decoded decoded = OrderedByteKeyEncoding.decode(bytes, ROUTE_BYTES);
    if (decoded.key().length() == 0 || bytes.length - decoded.nextOffset() != Long.BYTES) throw invalid();
    long sequence = ByteBuffer.wrap(bytes, decoded.nextOffset(), Long.BYTES).getLong();
    try {
      new LeaseBindingIdentity(leaseId, incarnation, decoded.key(), sequence);
    } catch (IllegalArgumentException failure) {
      throw invalid();
    }
    return new Decoded(leaseId, incarnation, decoded.key(), sequence);
  }

  public record Decoded(long leaseId, long incarnation, ByteKey key, long sequence) {
    public Decoded {
      Objects.requireNonNull(key, "key");
    }
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("invalid encoded Lease binding identity");
  }
}
