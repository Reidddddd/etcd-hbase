package io.etcdhbase.server.auth;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.spi.identity.BackendCredentials;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class CredentialMaterialCodecTest {
  @Test
  void roundTripsUnicodeCredentialMaterialAndKeepsSecretOutOfDiagnostics() {
    char[] secret = "pāss🔐word".toCharArray();
    try (var input = new BackendCredentials("user-α", secret)) {
      byte[] encoded = CredentialMaterialCodec.encode(input);
      try (BackendCredentials decoded = CredentialMaterialCodec.decode(encoded)) {
        assertEquals("user-α", decoded.principal());
        char[] copy = decoded.copySecret();
        try {
          assertArrayEquals("pāss🔐word".toCharArray(), copy);
        } finally {
          Arrays.fill(copy, '\0');
        }
        assertTrue(decoded.toString().contains("redacted"));
      } finally {
        Arrays.fill(encoded, (byte) 0);
      }
    }
    Arrays.fill(secret, '\0');
  }

  @Test
  void rejectsEmptySecretOversizedAndMalformedMaterial() {
    try (var empty = new BackendCredentials("user", new char[0])) {
      assertThrows(IllegalArgumentException.class, () -> CredentialMaterialCodec.encode(empty));
    }
    assertThrows(IllegalArgumentException.class,
        () -> CredentialMaterialCodec.decode(new byte[12]));

    byte[] malformed = ByteBuffer.allocate(4 + 4 + 1 + 4 + 1).putInt(1).putInt(1)
        .put((byte) 'u').putInt(1).put((byte) 0xff).array();
    assertThrows(IllegalArgumentException.class, () -> CredentialMaterialCodec.decode(malformed));
    Arrays.fill(malformed, (byte) 0);

    char[] tooLarge = new char[CredentialMaterialCodec.MAX_SECRET_CHARS + 1];
    Arrays.fill(tooLarge, 'a');
    try (var oversized = new BackendCredentials("user", tooLarge)) {
      assertThrows(IllegalArgumentException.class, () -> CredentialMaterialCodec.encode(oversized));
    }
    Arrays.fill(tooLarge, '\0');
  }
}
