package io.etcdhbase.spi.identity;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackendCredentialsTest {
  @Test
  void copiesAndRedactsSecretMaterial() {
    char[] source = {'s', 'e', 'c', 'r', 'e', 't'};
    BackendCredentials credentials = new BackendCredentials("user", source);
    Arrays.fill(source, 'x');

    char[] secretCopy = credentials.copySecret();
    assertArrayEquals(new char[] {'s', 'e', 'c', 'r', 'e', 't'}, secretCopy);
    Arrays.fill(secretCopy, 'x');
    assertArrayEquals(new char[] {'s', 'e', 'c', 'r', 'e', 't'}, credentials.copySecret());
    assertEquals("BackendCredentials[redacted]", credentials.toString());
  }

  @Test
  void closePreventsFurtherSecretAccess() {
    BackendCredentials credentials = new BackendCredentials("user", new char[] {'x'});

    credentials.close();

    assertTrue(credentials.isClosed());
    assertThrows(IllegalStateException.class, credentials::copySecret);
    credentials.close();
  }

  @Test
  void rejectsMissingPrincipalAndSecret() {
    assertThrows(IllegalArgumentException.class, () -> new BackendCredentials(" ", new char[0]));
    assertThrows(NullPointerException.class, () -> new BackendCredentials("user", null));
  }
}
