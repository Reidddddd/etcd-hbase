package io.etcdhbase.core.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import org.junit.jupiter.api.Test;

class AuthTokenTest {
  @Test
  void tokenUsesCanonicalOpaqueEncodingAndStoresOnlyItsDigest() {
    try (AuthToken generated = AuthToken.generate(new SecureRandom())) {
      String responseValue = generated.valueForResponse();
      assertEquals(43, responseValue.length());
      try (AuthToken parsed = AuthToken.parse(responseValue)) {
        assertEquals(generated.digest(), parsed.digest());
      }
      assertTrue(generated.toString().contains("redacted"));
      assertFalse(generated.toString().contains(responseValue));
    }
  }

  @Test
  void rejectsNonCanonicalOrWrongLengthTokensAndDisallowsUseAfterClose() {
    assertThrows(IllegalArgumentException.class, () -> AuthToken.parse("short"));
    String valid;
    try (AuthToken generated = AuthToken.generate(new SecureRandom())) {
      valid = generated.valueForResponse();
    }
    assertThrows(IllegalArgumentException.class, () -> AuthToken.parse(valid + "="));

    AuthToken token = AuthToken.generate(new SecureRandom());
    token.close();
    assertThrows(IllegalStateException.class, token::digest);
    assertThrows(IllegalStateException.class, token::valueForResponse);
  }
}
