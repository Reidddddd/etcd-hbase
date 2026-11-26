package io.etcdhbase.server.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class AuthorizationRevocationSnapshotTest {
  @Test
  void snapshotsAllAndOnlyRegisteredFamilyScopesWithTheirIncarnations() {
    var a = group("a", "provider", "realm", 1);
    var b = group("b", "provider", "realm", 2);
    var otherDomain = group("c", "provider", "other", 3);
    var otherProvider = group("d", "other", "realm", 4);
    var registry = AuthDeploymentRegistry.initial("trust", List.of(a, b, otherDomain, otherProvider));
    var identity = new AuthorizedBackendIdentity("provider", "alice", "ref",
        new AuthenticationScope("a", "profile"), Set.of("a"), 8);
    var operation = AuthorizationRevocationSnapshot.create(registry, "realm", identity, bytes(9, 16));
    assertEquals(List.of("a", "b"), operation.targets().stream()
        .map(target -> target.identity().tableGroupId()).toList());
    assertEquals(b.datasetDigest(), operation.targets().get(1).identity().datasetDigest());
    assertEquals(SubjectIdentityDigests.scoped("trust", "provider", "realm",
        new AuthenticationScope("b", "profile"), "alice"),
        operation.targets().get(1).identity().authorityDigest());
    assertNotEquals(operation.targets().get(0).identity().authorityDigest(),
        operation.targets().get(1).identity().authorityDigest());
    assertTrue(registry.permitsReplacement(registry.beginRevocation(operation)));
    assertThrows(IllegalArgumentException.class,
        () -> AuthorizationRevocationSnapshot.create(registry, "other", identity, bytes(9, 16)));
  }

  private static AuthDeploymentRegistry.Group group(String id, String provider, String domain, int value) {
    return new AuthDeploymentRegistry.Group(id, provider, domain, "profile", bytes(value, 32),
        bytes(value + 10, 32), true);
  }

  private static ByteKey bytes(int value, int length) {
    byte[] data = new byte[length];
    java.util.Arrays.fill(data, (byte) value);
    return ByteKey.copyOf(data);
  }
}
