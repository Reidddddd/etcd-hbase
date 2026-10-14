package io.etcdhbase.spi.salt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class HashSaltStrategyTest {
  private final HashSaltStrategy strategy = new HashSaltStrategy();
  private final HexFormat hex = HexFormat.of();

  @Test
  void matchesThePublicCrc32BucketVectors() {
    assertSalt("", "0000");
    assertSalt("61", "0003");
    assertSalt("2f612f622f632f", "0009");
    assertSalt("00", "000d");
    assertSalt("ff", "0000");
    assertSalt("6100ff", "0008");
    assertSalt("2f726f6f74", "000f");
    assertSalt("2f726f6f742f", "000c");
  }

  @Test
  void usesTheConfiguredProfileAndReturnsCallerOwnedValues() {
    byte[] identifier = hex.parseHex("6100ff");
    byte[] original = identifier.clone();

    byte[] firstSalt = strategy.salt(identifier);
    byte[] secondSalt = strategy.salt(identifier);

    assertEquals(HashSaltStrategy.STRATEGY_ID, strategy.id());
    assertEquals(HashSaltStrategy.PUBLIC_PROFILE, strategy.profile());
    assertArrayEquals(original, identifier);
    assertArrayEquals(hex.parseHex("0008"), firstSalt);
    assertNotSame(firstSalt, secondSalt);
  }

  @Test
  void returnsSortedTwoByteBigEndianSplitKeysWithoutSharedArrays() {
    var splitKeys = strategy.splitKeys();

    assertEquals(15, splitKeys.size());
    for (int index = 0; index < splitKeys.size(); index++) {
      assertArrayEquals(hex.parseHex("000" + Integer.toHexString(index + 1)), splitKeys.get(index));
    }
    splitKeys.getFirst()[1] = (byte) 0xff;
    assertArrayEquals(hex.parseHex("0001"), strategy.splitKeys().getFirst());
  }

  @Test
  void supportsLongIdentifiersAndRejectsInvalidProfiles() {
    byte[] identifier = new byte[1024 * 1024];
    assertEquals(2, strategy.salt(identifier).length);
    assertThrows(
        IllegalArgumentException.class,
        () -> new HashSaltStrategy(new SaltProfile("other", 1, 16, 2)));
    assertThrows(
        IllegalArgumentException.class,
        () -> new HashSaltStrategy(new SaltProfile(HashSaltStrategy.STRATEGY_ID, 2, 16, 2)));
    assertThrows(
        IllegalArgumentException.class,
        () -> new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 257, 1));
  }

  private void assertSalt(String identifierHex, String expectedHex) {
    assertArrayEquals(hex.parseHex(expectedHex), strategy.salt(hex.parseHex(identifierHex)));
  }
}
