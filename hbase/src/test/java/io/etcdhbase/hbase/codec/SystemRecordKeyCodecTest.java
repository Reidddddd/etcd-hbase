package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.LeaseBindingIdentity;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.hadoop.hbase.HConstants;
import org.junit.jupiter.api.Test;

class SystemRecordKeyCodecTest {
  private static final SystemRecordKeyCodec CODEC = new SystemRecordKeyCodec(
      new HashSaltStrategy(new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2)));
  private static final int KEY_OVERHEAD = 2 + 1 + Integer.BYTES;

  @Test
  void binaryIdentityRoundTripsForEveryRecordDomain() {
    ByteKey identity = ByteKey.copyOf(new byte[] {0, 1, (byte) 0xff, 0});
    for (SystemRecordType type : SystemRecordType.values()) {
      ByteKey encodedIdentity = switch (type) {
        case CANDIDATE -> candidateIdentity(identity);
        case LEASE_STATE -> LeaseRecordIdentity.stateVersion(-7, 11);
        case LEASE_ENUMERATION -> LeaseRecordIdentity.enumeration(-7);
        case EXPIRY_CANDIDATE -> new ExpiryCandidateIdentity(1_700_000_000_000L, -7, 1, 0).encode();
        case LEASE_BINDING -> new LeaseBindingIdentity(-7, 2, identity, 11).encode();
        default -> identity;
      };
      var decoded = CODEC.decode(CODEC.encode(type, encodedIdentity));
      assertEquals(type, decoded.type());
      assertEquals(encodedIdentity, decoded.identity());
    }
  }

  @Test
  void leaseBindingRowsAreRoutedAndOrderedByLeaseKeyAndVersion() {
    long leaseId = Long.MIN_VALUE;
    long incarnation = 4;
    var first = new LeaseBindingIdentity(leaseId, incarnation,
        ByteKey.copyOf(new byte[] {'a'}), 11).encode();
    var sameKeyLater = new LeaseBindingIdentity(leaseId, incarnation,
        ByteKey.copyOf(new byte[] {'a'}), 12).encode();
    var nextKey = new LeaseBindingIdentity(leaseId, incarnation,
        ByteKey.copyOf(new byte[] {'a', 0}), 1).encode();
    byte[] firstRow = CODEC.encode(SystemRecordType.LEASE_BINDING, first);
    byte[] laterRow = CODEC.encode(SystemRecordType.LEASE_BINDING, sameKeyLater);
    byte[] nextRow = CODEC.encode(SystemRecordType.LEASE_BINDING, nextKey);

    assertArrayEquals(CODEC.encode(SystemRecordType.LEASE_BINDING, first),
        CODEC.encode(SystemRecordType.LEASE_BINDING, CODEC.decode(firstRow).identity()));
    assertTrue(Arrays.compareUnsigned(firstRow, laterRow) < 0);
    assertTrue(Arrays.compareUnsigned(laterRow, nextRow) < 0);
    assertEquals(CODEC.leaseBindingBucket(leaseId, incarnation),
        CODEC.bucket(SystemRecordType.LEASE_BINDING, nextKey));

    var bounds = CODEC.leaseBindingBounds(leaseId, incarnation, java.util.Optional.empty());
    int routePrefixLength = 2 + 1 + LeaseBindingIdentity.ROUTE_BYTES;
    assertArrayEquals(Arrays.copyOf(firstRow, routePrefixLength), bounds.startRow());
    assertFalse(bounds.stopInclusive());
    assertThrows(IllegalArgumentException.class, () -> CODEC.leaseBindingBounds(leaseId,
        incarnation, java.util.Optional.of(new LeaseBindingIdentity(leaseId, incarnation + 1,
            ByteKey.copyOf(new byte[] {'a'}), 11).encode())));

    var resumed = CODEC.leaseBindingBounds(leaseId, incarnation, java.util.Optional.of(sameKeyLater));
    assertArrayEquals(laterRow, resumed.startRow());
    assertFalse(resumed.startInclusive());
  }

  @Test
  void leaseStateVersionsShareTheLeaseRouteAndCanBeReverseSought() {
    ByteKey earlier = LeaseRecordIdentity.stateVersion(-7, 11);
    ByteKey later = LeaseRecordIdentity.stateVersion(-7, 29);
    byte[] earlierRow = CODEC.encode(SystemRecordType.LEASE_STATE, earlier);
    byte[] laterRow = CODEC.encode(SystemRecordType.LEASE_STATE, later);

    assertArrayEquals(Arrays.copyOf(earlierRow, 3), Arrays.copyOf(laterRow, 3));
    assertEquals(CODEC.bucket(SystemRecordType.LEASE_STATE, earlier),
        CODEC.bucket(SystemRecordType.LEASE_STATE, later));
    assertEquals(-7, LeaseRecordIdentity.leaseId(earlier));
    assertEquals(11, LeaseRecordIdentity.sequence(earlier));

    var bounds = CODEC.leaseVersionBounds(SystemRecordType.LEASE_STATE,
        LeaseRecordIdentity.routePrefix(-7), 19);
    assertTrue(bounds.reversed());
    assertArrayEquals(CODEC.encode(SystemRecordType.LEASE_STATE,
        LeaseRecordIdentity.stateVersion(-7, 19)), bounds.startRow());
    assertTrue(bounds.startInclusive());
    assertFalse(bounds.stopInclusive());
  }

  @Test
  void leaseBindingVersionSeekStopsAtOneExactKeyRoute() {
    long leaseId = -7;
    long incarnation = 3;
    ByteKey key = ByteKey.copyOf(new byte[] {'a', 0});
    ByteKey route = LeaseBindingIdentity.keyRoutePrefix(leaseId, incarnation, key);
    ByteKey requested = new LeaseBindingIdentity(leaseId, incarnation, key, 19).encode();
    byte[] requestedRow = CODEC.encode(SystemRecordType.LEASE_BINDING, requested);
    int keyPrefixLength = 2 + 1 + route.length();
    byte[] exactKeyPrefix = Arrays.copyOf(requestedRow, keyPrefixLength);

    var bounds = CODEC.leaseBindingVersionBounds(route, 19);

    assertTrue(bounds.reversed());
    assertTrue(bounds.startInclusive());
    assertFalse(bounds.stopInclusive());
    assertArrayEquals(requestedRow, bounds.startRow());
    assertArrayEquals(exactKeyPrefix, bounds.stopRow());
    assertTrue(Arrays.compareUnsigned(bounds.stopRow(), bounds.startRow()) < 0,
        "a reverse seek must stop below its inclusive sequence row");
  }

  @Test
  void expiryCandidatesSortByDeadlineAndExposeHalfOpenWindowBounds() {
    long firstLeaseId = -7;
    ByteKey first = new ExpiryCandidateIdentity(100, firstLeaseId, 1, 0).encode();
    int bucket = CODEC.bucket(SystemRecordType.EXPIRY_CANDIDATE, first);
    long secondLeaseId = Long.MIN_VALUE;
    while (CODEC.bucket(SystemRecordType.EXPIRY_CANDIDATE,
        new ExpiryCandidateIdentity(500, secondLeaseId, 1, 0).encode()) != bucket) {
      secondLeaseId++;
    }
    ByteKey lastInWindow = new ExpiryCandidateIdentity(499, secondLeaseId, 1, 0).encode();
    ByteKey firstAtStop = new ExpiryCandidateIdentity(500, firstLeaseId, 1, 0).encode();

    byte[] firstRow = CODEC.encode(SystemRecordType.EXPIRY_CANDIDATE, first);
    byte[] laterRow = CODEC.encode(SystemRecordType.EXPIRY_CANDIDATE, lastInWindow);
    assertTrue(Arrays.compareUnsigned(firstRow, laterRow) < 0);
    assertEquals(bucket, CODEC.bucket(SystemRecordType.EXPIRY_CANDIDATE, lastInWindow));
    assertEquals(first, CODEC.decode(firstRow).identity());

    var bounds = CODEC.expiryWindowBounds(bucket, 100, 500, java.util.Optional.empty());
    int deadlinePrefixLength = 2 + 1 + Integer.BYTES + Long.BYTES;
    assertArrayEquals(Arrays.copyOf(firstRow, deadlinePrefixLength), bounds.startRow());
    assertArrayEquals(Arrays.copyOf(CODEC.encode(SystemRecordType.EXPIRY_CANDIDATE,
        firstAtStop), deadlinePrefixLength), bounds.stopRow());
    assertTrue(bounds.startInclusive());
    assertFalse(bounds.stopInclusive());

    var resumed = CODEC.expiryWindowBounds(bucket, 100, 500,
        java.util.Optional.of(lastInWindow));
    assertArrayEquals(CODEC.encode(SystemRecordType.EXPIRY_CANDIDATE, lastInWindow),
        resumed.startRow());
    assertFalse(resumed.startInclusive());
    assertThrows(IllegalArgumentException.class, () -> CODEC.expiryWindowBounds(bucket,
        100, 500, java.util.Optional.of(firstAtStop)));
  }

  @Test
  void enforcesIdentityAndRowLengthBoundaries() {
    assertThrows(IllegalArgumentException.class,
        () -> CODEC.encode(SystemRecordType.CONTROL, ByteKey.empty()));
    assertThrows(IllegalArgumentException.class, () -> CODEC.encode(
        SystemRecordType.CANDIDATE, ByteKey.copyOf(new byte[16 + Integer.BYTES])));

    byte[] maximumIdentity = new byte[HConstants.MAX_ROW_LENGTH - KEY_OVERHEAD];
    byte[] maximumRow = CODEC.encode(SystemRecordType.CONTROL, ByteKey.copyOf(maximumIdentity));
    assertEquals(HConstants.MAX_ROW_LENGTH, maximumRow.length);
    assertArrayEquals(maximumIdentity,
        CODEC.decode(maximumRow).identity().bytes());
    assertThrows(IllegalArgumentException.class, () -> CODEC.encode(SystemRecordType.CONTROL,
        ByteKey.copyOf(new byte[maximumIdentity.length + 1])));

    int candidateKeyLength = HConstants.MAX_ROW_LENGTH - 2 - 1 - 16 - 2;
    byte[] maximumCandidateKey = new byte[candidateKeyLength];
    Arrays.fill(maximumCandidateKey, (byte) 'x');
    ByteKey maximumCandidateIdentity = candidateIdentity(ByteKey.copyOf(maximumCandidateKey));
    assertEquals(HConstants.MAX_ROW_LENGTH,
        CODEC.encode(SystemRecordType.CANDIDATE, maximumCandidateIdentity).length);
    var decodedMaximum = CODEC.decode(
        CODEC.encode(SystemRecordType.CANDIDATE, maximumCandidateIdentity));
    assertEquals(SystemRecordType.CANDIDATE, decodedMaximum.type());
    assertEquals(maximumCandidateIdentity, decodedMaximum.identity());

    byte[] oversizedCandidateKey = Arrays.copyOf(maximumCandidateKey, maximumCandidateKey.length + 1);
    ByteKey oversizedCandidateIdentity = candidateIdentity(ByteKey.copyOf(oversizedCandidateKey));
    assertThrows(IllegalArgumentException.class,
        () -> CODEC.bucket(SystemRecordType.CANDIDATE, oversizedCandidateIdentity));
    assertThrows(IllegalArgumentException.class,
        () -> CODEC.encode(SystemRecordType.CANDIDATE, oversizedCandidateIdentity));
  }

  @Test
  void candidateRowsReuseTheKvOrderedKeyBytesWithoutChangingEscapeFormat() {
    ByteKey key = ByteKey.copyOf(new byte[] {0, 1, (byte) 0xff, 0});
    byte[] kvRow = new RowKeyCodec(
        new HashSaltStrategy(new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2)),
        RowKeyCodec.Shape.CURRENT).encode(key, 1);
    byte[] candidateRow = CODEC.encode(SystemRecordType.CANDIDATE, candidateIdentity(key));
    assertArrayEquals(Arrays.copyOfRange(kvRow, 2, kvRow.length),
        Arrays.copyOfRange(candidateRow, 2 + 1 + 16, candidateRow.length));
    assertEquals(candidateIdentity(key), CODEC.decode(candidateRow).identity());
  }

  @Test
  void kvRowsContinueToAllowAnEmptyLogicalKey() {
    RowKeyCodec codec = new RowKeyCodec(
        new HashSaltStrategy(new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2)),
        RowKeyCodec.Shape.CURRENT);
    assertEquals(ByteKey.empty(), codec.decode(codec.encode(ByteKey.empty(), 1)).key());
  }

  @Test
  void rejectsUnknownTypesInvalidSaltAndMalformedLengths() {
    byte[] valid = CODEC.encode(SystemRecordType.CONTROL, ByteKey.copyOf(new byte[] {7}));

    byte[] invalidSalt = valid.clone();
    invalidSalt[0] ^= 1;
    assertThrows(IllegalArgumentException.class, () -> CODEC.decode(invalidSalt));

    byte[] unknownType = valid.clone();
    unknownType[2] = (byte) 0xff;
    assertThrows(IllegalArgumentException.class, () -> CODEC.decode(unknownType));

    byte[] invalidLength = valid.clone();
    invalidLength[3] = 0;
    invalidLength[4] = 0;
    invalidLength[5] = 0;
    invalidLength[6] = 2;
    assertThrows(IllegalArgumentException.class, () -> CODEC.decode(invalidLength));
    assertThrows(IllegalArgumentException.class, () -> CODEC.decode(new byte[7]));
  }

  private static ByteKey candidateIdentity(ByteKey logicalKey) {
    byte[] key = logicalKey.bytes();
    return ByteKey.copyOf(ByteBuffer.allocate(16 + Integer.BYTES + key.length)
        .put(new byte[16]).putInt(key.length).put(key).array());
  }
}
