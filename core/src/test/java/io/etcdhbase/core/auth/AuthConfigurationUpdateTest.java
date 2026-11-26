package io.etcdhbase.core.auth;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.List;
import org.junit.jupiter.api.Test;

class AuthConfigurationUpdateTest {
  @Test
  void requiresEachOldGroupToAssignAndInstallItsOwnBarrier() {
    var update = AuthConfigurationUpdate.start(2, List.of(group("a"), group("b")),
        List.of(group("c")));
    assertTrue(update.isInitial());
    assertFalse(update.isInstalled());
    assertThrows(IllegalStateException.class, () -> update.acknowledge("a", 1));
    var assigned = update.assign("a", 7);
    assertTrue(update.allowsTransitionTo(assigned));
    assertFalse(assigned.allowsTransitionTo(update));
    assertThrows(IllegalStateException.class, () -> assigned.acknowledge("a", 8));
    var installed = assigned.acknowledge("a", 7);
    assertTrue(assigned.allowsTransitionTo(installed));
    assertFalse(update.allowsTransitionTo(installed));
    assertFalse(installed.isInstalled());
    var secondAssigned = installed.assign("b", 3);
    var complete = secondAssigned.acknowledge("b", 3);
    assertTrue(secondAssigned.allowsTransitionTo(complete));
    assertTrue(complete.isInstalled());
  }

  @Test
  void rejectsDuplicateTargetsAndUnregisteredGroups() {
    assertThrows(IllegalArgumentException.class, () -> AuthConfigurationUpdate.start(2,
        List.of(group("a"), group("a")), List.of(group("b"))));
    var update = AuthConfigurationUpdate.start(2, List.of(group("a")), List.of(group("b")));
    assertThrows(IllegalArgumentException.class, () -> update.assign("missing", 1));
  }

  private static AuthDeploymentRegistry.Group group(String id) {
    return new AuthDeploymentRegistry.Group(id, "provider", "realm", "profile",
        ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]), true);
  }
}
