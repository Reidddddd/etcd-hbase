package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class AuthControlKeyCodecTest {
  private static final int SALT_WIDTH_BYTES = 2;
  private static final HashSaltStrategy SALT = new HashSaltStrategy(
      new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 8, SALT_WIDTH_BYTES));
  private static final AuthControlKeyCodec CODEC = new AuthControlKeyCodec(SALT);

  @Test
  void sessionAndAuthorityKeysRoundTripWithTheirFullDigestIdentity() {
    ByteKey token = bytes(32, 0x5a);
    ByteKey subject = bytes(32, 0xa5);

    assertEquals(new AuthControlKeyCodec.DecodedKey(AuthControlKeyCodec.Kind.SESSION, token),
        CODEC.decode(CODEC.session(token)));
    assertEquals(new AuthControlKeyCodec.DecodedKey(
        AuthControlKeyCodec.Kind.SUBJECT_AUTHORITY, subject), CODEC.decode(CODEC.authority(subject)));
  }

  @Test
  void slotKeyUsesBigEndianIndexAndRejectsInvalidIndices() {
    int index = 0x01020304;
    byte[] row = CODEC.slot(index);
    var decoded = CODEC.decode(row);
    assertEquals(AuthControlKeyCodec.Kind.SLOT, decoded.kind());
    assertEquals(index, ByteBuffer.wrap(decoded.identity().bytes()).getInt());
    assertThrows(IllegalArgumentException.class, () -> CODEC.slot(-1));
  }

  @Test
  void deploymentRegistryKeysSeparateTrustDomainsWithoutReusingRemovedNamespaces() {
    byte[] registry = CODEC.deploymentRegistry("trust");
    assertArrayEquals(registry, CODEC.deploymentRegistry("trust"));
    assertEquals(AuthControlKeyCodec.Kind.AUTH_DEPLOYMENT_REGISTRY, CODEC.decode(registry).kind());
    assertEquals(8, CODEC.decode(registry).kind().id());
    assertNotEquals(ByteKey.copyOf(registry), ByteKey.copyOf(CODEC.deploymentRegistry("other")));
    assertThrows(IllegalArgumentException.class, () -> CODEC.deploymentRegistry(""));
    for (int removedKind : new int[] {4, 5, 6, 7}) {
      byte[] unsupported = registry.clone();
      unsupported[SALT_WIDTH_BYTES] = (byte) removedKind;
      byte[] identifier = java.util.Arrays.copyOfRange(
          unsupported, SALT_WIDTH_BYTES, unsupported.length);
      System.arraycopy(SALT.salt(identifier), 0, unsupported, 0, SALT_WIDTH_BYTES);
      assertThrows(IllegalArgumentException.class, () -> CODEC.decode(unsupported));
    }
  }

  @Test
  void rejectsWrongDigestLengthTamperedSaltAndMalformedLength() {
    assertThrows(IllegalArgumentException.class, () -> CODEC.session(bytes(31, 1)));
    byte[] row = CODEC.session(bytes(32, 2));
    row[0] ^= 1;
    assertThrows(IllegalArgumentException.class, () -> CODEC.decode(row));

    byte[] malformed = CODEC.session(bytes(32, 3));
    malformed[SALT_WIDTH_BYTES + 1 + Integer.BYTES - 1] = 0x1f;
    assertThrows(IllegalArgumentException.class, () -> CODEC.decode(malformed));
  }

  private static ByteKey bytes(int size, int value) {
    byte[] bytes = new byte[size];
    java.util.Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }
}
