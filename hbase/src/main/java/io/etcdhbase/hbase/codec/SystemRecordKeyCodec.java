package io.etcdhbase.hbase.codec;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.LeaseBindingIdentity;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.spi.salt.SaltStrategy;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import org.apache.hadoop.hbase.HConstants;

/** Encodes candidate rows as bucket || kind || txnId || logical key for bounded ordered scans. */
public final class SystemRecordKeyCodec {
  public static final int FORMAT_VERSION = 2;

  public record DecodedKey(SystemRecordType type, ByteKey identity) {}

  public record ScanBounds(byte[] startRow, boolean startInclusive,
      byte[] stopRow, boolean stopInclusive, boolean reversed) {
    public ScanBounds {
      startRow = Objects.requireNonNull(startRow, "startRow").clone();
      stopRow = Objects.requireNonNull(stopRow, "stopRow").clone();
    }

    @Override public byte[] startRow() { return startRow.clone(); }
    @Override public byte[] stopRow() { return stopRow.clone(); }
  }

  private final SaltStrategy salt;

  public SystemRecordKeyCodec(SaltStrategy salt) {
    this.salt = Objects.requireNonNull(salt, "salt");
  }

  public int formatVersion() {
    return FORMAT_VERSION;
  }

  public byte[] encode(SystemRecordType type, ByteKey identity) {
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(identity, "identity");
    byte[] identifier = identity.bytes();
    byte[] routingIdentifier = routingIdentifier(type, identifier);
    long encodedCandidateKeyLength = type == SystemRecordType.CANDIDATE
        ? OrderedKeyBytes.encodedLength(routingIdentifier) : 0;
    byte[] bindingKey = type == SystemRecordType.LEASE_BINDING
        ? LeaseBindingIdentity.key(identity).bytes() : null;
    long identityBytes = switch (type) {
      case CANDIDATE -> 16L + encodedCandidateKeyLength;
      case LEASE_BINDING -> LeaseBindingIdentity.ROUTE_BYTES
          + OrderedKeyBytes.encodedLength(bindingKey) + Long.BYTES;
      default -> Integer.BYTES + (long) identifier.length;
    };
    long rowLength = (long) salt.profile().saltWidthBytes() + 1 + identityBytes;
    if (type == SystemRecordType.CANDIDATE && rowLength > HConstants.MAX_ROW_LENGTH) {
      throw new PhysicalRowLimitException();
    }
    if (identifier.length == 0 || rowLength > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("invalid or oversized system record identity");
    }
    byte[] prefix = prefix(type, identifier, routingIdentifier);
    byte[] candidateKey = type == SystemRecordType.CANDIDATE
        ? OrderedKeyBytes.encode(routingIdentifier) : null;
    ByteBuffer row = ByteBuffer.allocate((int) rowLength);
    row.put(prefix).put((byte) type.id());
    if (type == SystemRecordType.CANDIDATE) {
      row.put(identifier, 0, 16).put(candidateKey);
    } else if (type == SystemRecordType.LEASE_BINDING) {
      LeaseBindingIdentity.Decoded decoded = LeaseBindingIdentity.decode(identity);
      row.put(routingIdentifier).put(OrderedKeyBytes.encode(bindingKey)).putLong(decoded.sequence());
    } else {
      row.putInt(identifier.length).put(identifier);
    }
    return row.array();
  }

  public DecodedKey decode(byte[] row) {
    Objects.requireNonNull(row, "row");
    int saltWidth = salt.profile().saltWidthBytes();
    if (row.length <= saltWidth + 1 || row.length > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("invalid system record row length");
    }
    ByteBuffer encoded = ByteBuffer.wrap(row);
    byte[] prefix = new byte[saltWidth];
    encoded.get(prefix);
    SystemRecordType type = fromId(Byte.toUnsignedInt(encoded.get()));
    byte[] identity;
    if (type == SystemRecordType.CANDIDATE) {
      if (encoded.remaining() <= 16) {
        throw new IllegalArgumentException("candidate system identity has no logical key");
      }
      byte[] transaction = new byte[16];
      encoded.get(transaction);
      byte[] keyEncoding = new byte[encoded.remaining()];
      encoded.get(keyEncoding);
      OrderedKeyBytes.Decoded decoded = OrderedKeyBytes.decode(keyEncoding, 0);
      if (decoded.nextOffset() != keyEncoding.length || decoded.key().length == 0) {
        throw new IllegalArgumentException("candidate key terminator is misplaced or key is empty");
      }
      byte[] key = decoded.key();
      identity = ByteBuffer.allocate(16 + Integer.BYTES + key.length)
          .put(transaction).putInt(key.length).put(key).array();
    } else if (type == SystemRecordType.LEASE_BINDING) {
      if (encoded.remaining() < LeaseBindingIdentity.MINIMUM_BYTES) {
        throw new IllegalArgumentException("Lease binding system identity is incomplete");
      }
      identity = new byte[encoded.remaining()];
      encoded.get(identity);
      LeaseBindingIdentity.decode(ByteKey.copyOf(identity));
    } else {
      if (encoded.remaining() < Integer.BYTES) {
        throw new IllegalArgumentException("system record identity length is missing");
      }
      int identityLength = encoded.getInt();
      if (identityLength <= 0 || identityLength != encoded.remaining()) {
        throw new IllegalArgumentException("invalid system record identity length");
      }
      identity = new byte[identityLength];
      encoded.get(identity);
    }
    checkedPrefix(prefix);
    if (!Arrays.equals(prefix, prefix(type, identity))) {
      throw new IllegalArgumentException("system record row has an invalid salt prefix");
    }
    return new DecodedKey(type, ByteKey.copyOf(identity));
  }

  /** Returns the configured bucket after validating the exact physical system-row identity. */
  public int bucket(SystemRecordType type, ByteKey identity) {
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(identity, "identity");
    byte[] bytes = identity.bytes();
    byte[] routingIdentifier = routingIdentifier(type, bytes);
    long encodedCandidateKeyLength = type == SystemRecordType.CANDIDATE
        ? OrderedKeyBytes.encodedLength(routingIdentifier) : 0;
    byte[] bindingKey = type == SystemRecordType.LEASE_BINDING
        ? LeaseBindingIdentity.key(identity).bytes() : null;
    long identityBytes = switch (type) {
      case CANDIDATE -> 16L + encodedCandidateKeyLength;
      case LEASE_BINDING -> LeaseBindingIdentity.ROUTE_BYTES
          + OrderedKeyBytes.encodedLength(bindingKey) + Long.BYTES;
      default -> Integer.BYTES + (long) bytes.length;
    };
    long rowLength = (long) salt.profile().saltWidthBytes() + 1 + identityBytes;
    if (type == SystemRecordType.CANDIDATE && rowLength > HConstants.MAX_ROW_LENGTH) {
      throw new PhysicalRowLimitException();
    }
    if (bytes.length == 0 || rowLength > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("invalid or oversized system record identity");
    }
    return bucketNumber(prefix(type, bytes, routingIdentifier));
  }

  /** Bounds a forward scan to one type and one configured bucket. */
  public ScanBounds bucketBounds(SystemRecordType type, int bucket,
      java.util.Optional<ByteKey> afterIdentity) {
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(afterIdentity, "afterIdentity");
    byte[] prefix = typeBucketPrefix(type, bucket);
    byte[] start = prefix;
    boolean startInclusive = true;
    if (afterIdentity.isPresent()) {
      ByteKey identity = afterIdentity.orElseThrow();
      if (bucket(type, identity) != bucket) {
        throw new IllegalArgumentException("resume identity belongs to a different bucket");
      }
      start = encode(type, identity);
      startInclusive = false;
    }
    return new ScanBounds(start, startInclusive, prefixEnd(prefix), false, false);
  }

  /** Reverse-seeks one lease's latest state version at or below the requested sequence. */
  public ScanBounds leaseVersionBounds(SystemRecordType type, ByteKey routePrefix, long sequence) {
    if (type != SystemRecordType.LEASE_STATE || routePrefix.length() != LeaseRecordIdentity.ROUTE_BYTES
        || sequence <= 0) {
      throw new IllegalArgumentException("invalid lease-version seek");
    }
    ByteKey identity = ByteKey.copyOf(ByteBuffer.allocate(LeaseRecordIdentity.IDENTITY_BYTES)
        .put(routePrefix.bytes()).putLong(sequence).array());
    byte[] start = encode(type, identity);
    byte[] prefix = typePrefix(type, bucket(type, identity));
    byte[] exactIdPrefix = Arrays.copyOf(prefix, prefix.length + Integer.BYTES + routePrefix.length());
    ByteBuffer.wrap(exactIdPrefix, prefix.length, Integer.BYTES).putInt(LeaseRecordIdentity.IDENTITY_BYTES);
    System.arraycopy(routePrefix.bytes(), 0, exactIdPrefix, prefix.length + Integer.BYTES,
        routePrefix.length());
    return new ScanBounds(start, true, exactIdPrefix, false, true);
  }

  /** Bounds a forward scan to one Lease incarnation's binding history. */
  public ScanBounds leaseBindingBounds(long leaseId, long incarnation,
      java.util.Optional<ByteKey> afterIdentity) {
    Objects.requireNonNull(afterIdentity, "afterIdentity");
    ByteKey route = LeaseBindingIdentity.routePrefix(leaseId, incarnation);
    afterIdentity.ifPresent(identity -> {
      if (!LeaseBindingIdentity.routePrefix(identity).equals(route)) {
        throw new IllegalArgumentException("Lease binding resume identity belongs to another Lease");
      }
    });
    byte[] routeBytes = route.bytes();
    byte[] saltPrefix = checkedPrefix(salt.salt(saltIdentifier(SystemRecordType.LEASE_BINDING, routeBytes)));
    byte[] typeAndRoute = ByteBuffer.allocate(saltPrefix.length + 1 + routeBytes.length)
        .put(saltPrefix).put((byte) SystemRecordType.LEASE_BINDING.id()).put(routeBytes).array();
    byte[] start = afterIdentity.map(identity -> encode(SystemRecordType.LEASE_BINDING, identity))
        .orElse(typeAndRoute);
    return new ScanBounds(start, afterIdentity.isEmpty(), prefixEnd(typeAndRoute), false, false);
  }

  /** Returns the single configured salt bucket for one Lease incarnation's binding range. */
  public int leaseBindingBucket(long leaseId, long incarnation) {
    ByteKey route = LeaseBindingIdentity.routePrefix(leaseId, incarnation);
    byte[] prefix = checkedPrefix(salt.salt(saltIdentifier(SystemRecordType.LEASE_BINDING, route.bytes())));
    return bucketNumber(prefix);
  }

  /** Bounds an ordered expiration scan to a half-open deadline interval in one salt bucket. */
  public ScanBounds expiryWindowBounds(int bucket, long startDeadlineInclusive,
      long stopDeadlineExclusive, java.util.Optional<ByteKey> afterIdentity) {
    Objects.requireNonNull(afterIdentity, "afterIdentity");
    if (startDeadlineInclusive < 0 || stopDeadlineExclusive <= startDeadlineInclusive) {
      throw new IllegalArgumentException("invalid expiry-window bounds");
    }
    byte[] typePrefix = typeBucketPrefix(SystemRecordType.EXPIRY_CANDIDATE, bucket);
    byte[] start = deadlinePrefix(typePrefix, startDeadlineInclusive);
    boolean startInclusive = true;
    if (afterIdentity.isPresent()) {
      ByteKey resume = afterIdentity.orElseThrow();
      ExpiryCandidateIdentity identity = ExpiryCandidateIdentity.decode(resume);
      if (bucket(SystemRecordType.EXPIRY_CANDIDATE, resume) != bucket
          || identity.deadlineEpochMillis() < startDeadlineInclusive
          || identity.deadlineEpochMillis() >= stopDeadlineExclusive) {
        throw new IllegalArgumentException("expiry resume identity is outside the requested window");
      }
      start = encode(SystemRecordType.EXPIRY_CANDIDATE, resume);
      startInclusive = false;
    }
    return new ScanBounds(start, startInclusive,
        deadlinePrefix(typePrefix, stopDeadlineExclusive), false, false);
  }

  private byte[] typeBucketPrefix(SystemRecordType type, int bucket) {
    byte[] saltPrefix = salt.profile().encodeBucket(bucket);
    return ByteBuffer.allocate(saltPrefix.length + 1).put(saltPrefix).put((byte) type.id()).array();
  }

  private byte[] typePrefix(SystemRecordType type, int bucket) {
    return typeBucketPrefix(type, bucket);
  }

  private static byte[] deadlinePrefix(byte[] typePrefix, long deadline) {
    return ByteBuffer.allocate(typePrefix.length + Integer.BYTES + Long.BYTES)
        .put(typePrefix).putInt(ExpiryCandidateIdentity.ENCODED_BYTES)
        .putLong(deadline ^ Long.MIN_VALUE).array();
  }

  private static byte[] prefixEnd(byte[] prefix) {
    byte[] end = prefix.clone();
    for (int index = end.length - 1; index >= 0; index--) {
      if (Byte.toUnsignedInt(end[index]) != 0xff) {
        end[index]++;
        return Arrays.copyOf(end, index + 1);
      }
    }
    throw new IllegalArgumentException("system record prefix has no finite upper bound");
  }

  private byte[] prefix(SystemRecordType type, byte[] identity) {
    byte[] routingIdentifier = routingIdentifier(type, identity);
    return prefix(type, identity, routingIdentifier);
  }

  private byte[] prefix(SystemRecordType type, byte[] identity, byte[] routingIdentifier) {
    return checkedPrefix(salt.salt(saltIdentifier(type, routingIdentifier)));
  }

  private static byte[] saltIdentifier(SystemRecordType type, byte[] routingIdentifier) {
    return type == SystemRecordType.CANDIDATE ? routingIdentifier : route(type, routingIdentifier);
  }

  private static byte[] route(SystemRecordType type, byte[] identity) {
    return ByteBuffer.allocate(1 + Integer.BYTES + identity.length)
        .put((byte) type.id()).putInt(identity.length).put(identity).array();
  }

  private static SystemRecordType fromId(int id) {
    return SystemRecordType.fromId(id);
  }

  private byte[] checkedPrefix(byte[] prefix) {
    if (prefix == null || prefix.length != salt.profile().saltWidthBytes()) {
      throw new IllegalArgumentException("salt provider returned an invalid system prefix");
    }
    if (bucketNumberUnchecked(prefix) >= salt.profile().bucketCount()) {
      throw new IllegalArgumentException("salt provider returned an unknown system bucket");
    }
    return prefix;
  }

  private int bucketNumber(byte[] prefix) {
    return (int) bucketNumberUnchecked(checkedPrefix(prefix));
  }

  private long bucketNumberUnchecked(byte[] prefix) {
    long bucket = 0;
    for (byte part : prefix) bucket = (bucket << Byte.SIZE) | Byte.toUnsignedInt(part);
    return bucket;
  }

  private static byte[] routingIdentifier(SystemRecordType type, byte[] identity) {
    if (type == SystemRecordType.LEASE_STATE) {
      if (identity.length != LeaseRecordIdentity.IDENTITY_BYTES) {
        throw new IllegalArgumentException("lease state identity must contain 16 bytes");
      }
      return Arrays.copyOf(identity, LeaseRecordIdentity.ROUTE_BYTES);
    }
    if (type == SystemRecordType.LEASE_ENUMERATION) {
      if (identity.length != LeaseRecordIdentity.ROUTE_BYTES) {
        throw new IllegalArgumentException("lease enumeration identity must contain 8 bytes");
      }
      return identity;
    }
    if (type == SystemRecordType.LEASE_BINDING) {
      if (identity.length < LeaseBindingIdentity.MINIMUM_BYTES) {
        throw new IllegalArgumentException("lease binding identity is incomplete");
      }
      return LeaseBindingIdentity.routePrefix(ByteKey.copyOf(identity)).bytes();
    }
    if (type == SystemRecordType.EXPIRY_CANDIDATE) {
      return ExpiryCandidateIdentity.leaseRoute(ByteKey.copyOf(identity)).bytes();
    }
    if (type != SystemRecordType.CANDIDATE) return identity;
    if (identity.length <= 16 + Integer.BYTES) {
      throw new IllegalArgumentException("candidate identity is missing its logical key");
    }
    int keyLength = ByteBuffer.wrap(identity, 16, Integer.BYTES).getInt();
    if (keyLength <= 0 || keyLength != identity.length - 16 - Integer.BYTES) {
      throw new IllegalArgumentException("candidate identity has an invalid logical key length");
    }
    return Arrays.copyOfRange(identity, 16 + Integer.BYTES, identity.length);
  }

}
