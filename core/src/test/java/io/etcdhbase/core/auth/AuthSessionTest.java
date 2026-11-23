package io.etcdhbase.core.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class AuthSessionTest {
  private static final AuthenticationScope SCOPE = new AuthenticationScope("group", "profile");

  @Test
  void sessionBindsTokenSubjectDatasetAndSlotGenerations() {
    AuthSession session = new AuthSession(
        key(16, 1), key(32, 2), key(32, 3), "milvus", "hbase-provider", SCOPE,
        key(32, 4), 7, 2, 100, 200, 4, 9, AuthSession.Status.ACTIVE,
        envelope());

    assertTrue(session.isActiveAt(199));
    assertFalse(session.isActiveAt(200));
    assertEquals(AuthSession.Status.ACTIVE, session.status());
    assertThrows(IllegalArgumentException.class, () -> new AuthSession(
        key(16, 1), key(31, 2), key(32, 3), "milvus", "hbase-provider", SCOPE,
        key(32, 4), 7, 2, 100, 200, 4, 9, AuthSession.Status.ACTIVE, envelope()));
  }

  @Test
  void envelopeCopiesCiphertextAndRedactsItFromDiagnostics() {
    byte[] nonce = new byte[CredentialEnvelope.NONCE_BYTES];
    byte[] ciphertext = new byte[CredentialEnvelope.TAG_BYTES + 3];
    Arrays.fill(ciphertext, (byte) 65);
    CredentialEnvelope envelope = new CredentialEnvelope("key-1", 1, nonce, ciphertext);
    ciphertext[0] = 0;
    byte[] returned = envelope.ciphertext();
    returned[0] = 0;

    assertEquals((byte) 65, envelope.ciphertext()[0]);
    assertFalse(envelope.toString().contains("AAAA"));
    assertTrue(envelope.toString().contains("ciphertext=redacted"));
  }

  @Test
  void slotReuseMustAdvanceGenerationAndKeepSessionBinding() {
    AuthSlot reserved = new AuthSlot(2, 5, key(16, 7), "AAAAAAAAAAAAAAAAAAAAAA",
        AuthSlot.State.RESERVED);
    AuthSlot active = new AuthSlot(2, reserved.generation(), reserved.sessionId(),
        reserved.operationId(), AuthSlot.State.ACTIVE);

    assertEquals(reserved.sessionId(), active.sessionId());
    assertThrows(NullPointerException.class, () -> new AuthSlot(2, 5, null, null,
        AuthSlot.State.RESERVED));
    assertThrows(IllegalArgumentException.class, () -> new AuthSlot(2, 0, key(16, 7),
        "AAAAAAAAAAAAAAAAAAAAAA", AuthSlot.State.RETIRING));
  }

  private static CredentialEnvelope envelope() {
    return new CredentialEnvelope("key-1", 1,
        new byte[CredentialEnvelope.NONCE_BYTES], new byte[CredentialEnvelope.TAG_BYTES]);
  }

  private static ByteKey key(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }
}
