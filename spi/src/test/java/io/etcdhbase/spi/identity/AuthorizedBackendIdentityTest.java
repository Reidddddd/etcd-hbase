package io.etcdhbase.spi.identity;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuthorizedBackendIdentityTest {
  @Test
  void freezesAuthorizationScopeAndRedactsProviderContext() {
    var groups = new HashSet<>(Set.of("local", "secondary"));
    var identity =
        new AuthorizedBackendIdentity(
            "provider",
            "alice",
            "opaque-context-secret",
            new AuthenticationScope("local", "hbase"),
            groups,
            4);
    groups.clear();

    assertEquals(Set.of("local", "secondary"), identity.allowedTableGroupIds());
    assertThrows(
        UnsupportedOperationException.class,
        () -> identity.allowedTableGroupIds().add("other"));
    assertTrue(identity.toString().contains("subject=redacted"));
    assertTrue(identity.toString().contains("contextRef=redacted"));
    assertFalse(identity.toString().contains("alice"));
    assertFalse(identity.toString().contains("opaque-context-secret"));
  }

  @Test
  void rejectsScopeOutsideAuthorizedTableGroupsAndNegativeCredentialGeneration() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new AuthorizedBackendIdentity(
                "provider", "alice", "context", new AuthenticationScope("local", "hbase"),
                Set.of("other"), 1));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new AuthorizedBackendIdentity(
                "provider", "alice", "context", new AuthenticationScope("local", "hbase"),
                Set.of("local"), -1));
  }

  @Test
  void rejectsUnboundedAndControlCharacterIdentityFields() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new AuthorizedBackendIdentity(
                "provider",
                "a".repeat(257),
                "context",
                new AuthenticationScope("local", "hbase"),
                Set.of("local"),
                1));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new AuthorizedBackendIdentity(
                "provider",
                "alice",
                "context\nref",
                new AuthenticationScope("local", "hbase"),
                Set.of("local"),
                1));
  }
}
