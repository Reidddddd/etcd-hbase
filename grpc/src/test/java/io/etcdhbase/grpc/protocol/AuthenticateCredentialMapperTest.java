package io.etcdhbase.grpc.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import etcdserverpb.Rpc;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class AuthenticateCredentialMapperTest {
  @Test
  void mapsNameAndPasswordAndKeepsTheListenerBoundScope() {
    String password = "fixture-password-value";
    AuthenticationScope listenerScope = new AuthenticationScope("tenant-a", "hbase-a");
    Rpc.AuthenticateRequest request =
        Rpc.AuthenticateRequest.newBuilder().setName("service-user").setPassword(password).build();

    AuthenticateCredentialMapper.MappedCredentials mapped =
        AuthenticateCredentialMapper.map(request, listenerScope);
    BackendCredentials credentials = mapped.credentials();
    char[] secret = credentials.copySecret();
    try {
      assertEquals("service-user", credentials.principal());
      assertArrayEquals(password.toCharArray(), secret);
      assertEquals(listenerScope, mapped.scope());
      assertFalse(mapped.toString().contains(password));
    } finally {
      Arrays.fill(secret, '\0');
      mapped.close();
    }
    assertTrue(credentials.isClosed());
    assertEquals("BackendCredentials[redacted]", credentials.toString());
  }

  @Test
  void rejectsBlankNamesWithoutEchoingThePassword() {
    Rpc.AuthenticateRequest request =
        Rpc.AuthenticateRequest.newBuilder()
            .setName(" ")
            .setPassword("fixture-password-value")
            .build();

    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AuthenticateCredentialMapper.map(
                    request, new AuthenticationScope("tenant-a", "hbase-a")));

    assertFalse(failure.getMessage().contains("fixture-password-value"));
  }
}
