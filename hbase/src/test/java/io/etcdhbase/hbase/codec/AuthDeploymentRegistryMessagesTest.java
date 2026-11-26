package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.core.auth.AuthorizationRevocation;
import io.etcdhbase.core.bytes.ByteKey;
import java.util.List;
import org.junit.jupiter.api.Test;

class AuthDeploymentRegistryMessagesTest {
  @Test
  void preservesPartiallyInstalledConfigurationUpdate() {
    var a = new AuthDeploymentRegistry.Group("a", "provider", "realm", "profile",
        bytes(1, 32), bytes(2, 32), true);
    var b = new AuthDeploymentRegistry.Group("b", "provider", "realm", "profile",
        bytes(3, 32), bytes(4, 32), true);
    var registry = AuthDeploymentRegistry.initial("trust", List.of(a, b)).beginConfigurationUpdate(List.of(b));
    registry = registry.advanceConfigurationUpdate(registry.configurationUpdate().assign("a", 8));
    registry = registry.advanceConfigurationUpdate(registry.configurationUpdate().acknowledge("a", 8));
    assertEquals(registry, AuthDeploymentRegistryMessages.decode(AuthDeploymentRegistryMessages.encode(registry)));
  }

  @Test
  void preservesInventoryAndPartialRevocationAndRejectsCorruption() {
    var group = new AuthDeploymentRegistry.Group("group", "provider", "realm", "profile",
        bytes(1, 32), bytes(2, 32), true);
    var registry = AuthDeploymentRegistry.initial("trust", List.of(group));
    var operation = AuthorizationRevocation.start(bytes(3, 16), bytes(4, 32), "provider", "realm", 2, 2, 1,
        List.of(new AuthorizationRevocation.TargetIdentity("group", bytes(1, 32), bytes(5, 32))));
    registry = registry.beginRevocation(operation);
    registry = registry.advanceRevocation(operation.assignBarrier("group", 5));
    byte[] encoded = AuthDeploymentRegistryMessages.encode(registry);
    assertEquals(registry, AuthDeploymentRegistryMessages.decode(encoded));
    encoded[encoded.length / 2] ^= 1;
    assertThrows(IllegalArgumentException.class, () -> AuthDeploymentRegistryMessages.decode(encoded));
  }

  private static ByteKey bytes(int value, int length) {
    byte[] data = new byte[length];
    java.util.Arrays.fill(data, (byte) value);
    return ByteKey.copyOf(data);
  }
}
