package io.etcdhbase.server.auth;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.auth.CredentialEnvelopeContext;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AesGcmCredentialEnvelopeProtectorTest {
  private static final CredentialEnvelopeContext CONTEXT = new CredentialEnvelopeContext(
      "milvus", key(16, 1), "hbase-provider", new AuthenticationScope("group", "profile"), 7, 200);

  @TempDir Path temporary;

  @Test
  void encryptsAndAuthenticatesCredentialMaterialAgainstSessionContext() throws Exception {
    Path keyFile = Files.createTempFile(temporary, "credential-key", ".bin");
    byte[] key = new byte[FileCredentialKeyProvider.KEY_BYTES];
    Arrays.fill(key, (byte) 0x41);
    Files.write(keyFile, key);
    Arrays.fill(key, (byte) 0);

    try (var keys = new FileCredentialKeyProvider("key-1", Map.of("key-1", keyFile))) {
      var protector = new AesGcmCredentialEnvelopeProtector(keys, new SecureRandom());
      byte[] plaintext = "principal\u0000secret".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      CredentialEnvelope first = protector.encrypt(plaintext, CONTEXT);
      CredentialEnvelope second = protector.encrypt(plaintext, CONTEXT);

      assertNotEquals(java.util.HexFormat.of().formatHex(first.nonce()),
          java.util.HexFormat.of().formatHex(second.nonce()));
      assertNotEquals(java.util.HexFormat.of().formatHex(plaintext),
          java.util.HexFormat.of().formatHex(first.ciphertext()));
      assertArrayEquals(plaintext, protector.decrypt(first, CONTEXT));
      assertThrows(IllegalArgumentException.class,
          () -> protector.decrypt(first, new CredentialEnvelopeContext("milvus", key(16, 2),
              "hbase-provider", new AuthenticationScope("group", "profile"), 7, 200)));

      byte[] tampered = first.ciphertext();
      tampered[0] ^= 1;
      CredentialEnvelope damaged = new CredentialEnvelope(first.keyId(), first.aadVersion(),
          first.nonce(), tampered);
      assertThrows(IllegalArgumentException.class, () -> protector.decrypt(damaged, CONTEXT));
      assertTrue(first.toString().contains("ciphertext=redacted"));
    }
  }

  @Test
  void keyFilesMustBeExactly256BitsAndPrivate() throws Exception {
    Path shortKey = Files.createTempFile(temporary, "short-key", ".bin");
    Files.write(shortKey, new byte[31]);
    try (var keys = new FileCredentialKeyProvider("short", Map.of("short", shortKey))) {
      var protector = new AesGcmCredentialEnvelopeProtector(keys, new SecureRandom());
      assertThrows(IllegalStateException.class,
          () -> protector.encrypt(new byte[] {1}, CONTEXT));
    }
  }

  private static ByteKey key(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }
}
