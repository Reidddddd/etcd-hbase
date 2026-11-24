package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class SessionAuthorizationContextReferenceTest {
  private static final AuthSession SESSION = session();

  @Test
  void roundTripCarriesOnlyDigestAndGenerationFences() {
    var reference = SessionAuthorizationContextReference.fromSession(SESSION);

    assertEquals(reference, SessionAuthorizationContextReference.parse(reference.encode()));
    assertTrue(reference.matches(SESSION));
    assertFalse(Arrays.equals(reference.encode().bytes(), SESSION.tokenDigest().bytes()));
  }

  @Test
  void rejectsMalformedAndStaleReferences() {
    var reference = SessionAuthorizationContextReference.fromSession(SESSION);
    byte[] changed = reference.encode().bytes();
    changed[0] = 2;
    assertThrows(IllegalArgumentException.class,
        () -> SessionAuthorizationContextReference.parse(ByteKey.copyOf(changed)));

    AuthSession retired = new AuthSession(SESSION.sessionId(), SESSION.tokenDigest(),
        SESSION.subjectDigest(), SESSION.trustDomain(), SESSION.providerId(), SESSION.scope(),
        SESSION.datasetDigest(), SESSION.credentialGeneration(), SESSION.authorizationGeneration(),
        SESSION.createdAtEpochMillis(), SESSION.expiresAtEpochMillis(), SESSION.slotIndex(),
        SESSION.slotGeneration(), AuthSession.Status.REVOKED, SESSION.credentialEnvelope());
    assertFalse(reference.matches(retired));
  }

  private static AuthSession session() {
    var envelope = new CredentialEnvelope("key-1", CredentialEnvelope.CURRENT_AAD_VERSION,
        new byte[CredentialEnvelope.NONCE_BYTES], new byte[CredentialEnvelope.TAG_BYTES]);
    return new AuthSession(key(16, 1), key(32, 2), key(32, 3), "milvus", "hbase-provider",
        new AuthenticationScope("auth-group", "profile"), key(32, 4), 3, 5, 100, 200,
        4, 13, AuthSession.Status.ACTIVE, envelope);
  }

  private static ByteKey key(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }
}
