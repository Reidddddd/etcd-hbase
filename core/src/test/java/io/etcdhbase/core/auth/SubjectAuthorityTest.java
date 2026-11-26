package io.etcdhbase.core.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.util.List;
import org.junit.jupiter.api.Test;

class SubjectAuthorityTest {
  private static final AuthenticationScope SCOPE = new AuthenticationScope("group", "profile");

  @Test
  void onlyCredentialRotationCanStartRevocationAndTargetMustFollowItsFence() {
    SubjectAuthority active = authority(3, 5, SubjectAuthority.State.ACTIVE);
    SubjectAuthority revoking = active.beginRevocation(4, 6);
    assertTrue(active.allowsTransitionTo(revoking));
    assertThrows(IllegalArgumentException.class, () -> active.beginRevocation(3, 6));
    assertThrows(IllegalStateException.class, () -> revoking.finishRevocation(
        SubjectAuthority.State.ACTIVE));

    SubjectAuthority targeted = revoking.assignRevocationBarrierTarget(7);
    assertTrue(revoking.allowsTransitionTo(targeted));
    SubjectAuthority falselyInstalled = new SubjectAuthority(revoking.subjectDigest(), "milvus",
        "provider", SCOPE, 4, 6, SubjectAuthority.State.REVOKING,
        List.of(new SubjectAuthority.RevocationTarget("group", 7, true)));
    assertFalse(revoking.allowsTransitionTo(falselyInstalled));
    SubjectAuthority installed = targeted.markRevocationInstalled("group", 7);
    assertTrue(targeted.allowsTransitionTo(installed));
    SubjectAuthority completed = installed.finishRevocation(SubjectAuthority.State.ACTIVE);
    assertTrue(installed.allowsTransitionTo(completed));
  }

  @Test
  void pendingCredentialGenerationCanAdvanceWithoutOpeningTheSubject() {
    SubjectAuthority revoking = authority(3, 5, SubjectAuthority.State.ACTIVE)
        .beginRevocation(4, 6);
    SubjectAuthority targeted = revoking.assignRevocationBarrierTarget(7);
    SubjectAuthority newerCredentials = targeted.advanceRevocationCredentialGeneration(5, 7);

    assertTrue(targeted.allowsTransitionTo(newerCredentials));
    assertFalse(newerCredentials.allowsTransitionTo(targeted));
    assertFalse(newerCredentials.allowsTransitionTo(
        new SubjectAuthority(newerCredentials.subjectDigest(), "milvus", "provider", SCOPE,
            5, 7, SubjectAuthority.State.ACTIVE)));
    assertThrows(IllegalArgumentException.class,
        () -> new SubjectAuthority(newerCredentials.subjectDigest(), "milvus", "provider", SCOPE,
            3, 6, SubjectAuthority.State.REVOKING,
            List.of(new SubjectAuthority.RevocationTarget("group", 0, true))));
  }

  @Test
  void alreadyCurrentCredentialsCanStillBeFencedForADeploymentOperation() {
    var active = authority(3, 5, SubjectAuthority.State.ACTIVE);
    var refreshing = active.beginAuthorizationRefresh(6);
    assertTrue(active.allowsTransitionTo(refreshing));
    assertEquals(3, refreshing.credentialGeneration());
    assertEquals(6, refreshing.authorizationGeneration());
    assertThrows(IllegalArgumentException.class, () -> active.beginAuthorizationRefresh(5));
  }

  @Test
  void deploymentOperationRemainsBoundThroughCompletionAndCannotRestart() {
    var operationId = ByteKey.copyOf(new byte[16]);
    var active = authority(3, 5, SubjectAuthority.State.ACTIVE);
    var pending = active.beginDeploymentRevocation(operationId, 2, 1, 4);
    assertTrue(active.allowsTransitionTo(pending));
    assertThrows(IllegalArgumentException.class,
        () -> pending.advanceRevocationCredentialGeneration(5, 7));
    var assigned = pending.assignRevocationBarrierTarget(9);
    var installed = assigned.markRevocationInstalled("group", 9);
    var completed = installed.finishRevocation(SubjectAuthority.State.ACTIVE);
    assertEquals(operationId, completed.revocationOperationId());
    assertTrue(installed.allowsTransitionTo(completed));
    assertThrows(IllegalArgumentException.class,
        () -> completed.beginDeploymentRevocation(operationId, 2, 1, 4));
    assertFalse(completed.allowsTransitionTo(completed.beginRevocation(5, 7)));
    byte[] nextBytes = new byte[16];
    nextBytes[0] = 1;
    var later = completed.beginDeploymentRevocation(ByteKey.copyOf(nextBytes), 10, 2, 4)
        .assignRevocationBarrierTarget(10).markRevocationInstalled("group", 10)
        .finishRevocation(SubjectAuthority.State.ACTIVE);
    assertThrows(IllegalArgumentException.class,
        () -> later.beginDeploymentRevocation(operationId, 2, 1, 4));
  }

  @Test
  void terminalTransitionMustPreserveInstalledAuthorityGenerations() {
    SubjectAuthority installed = authority(3, 5, SubjectAuthority.State.ACTIVE)
        .beginRevocation(4, 6)
        .assignRevocationBarrierTarget(7)
        .markRevocationInstalled("group", 7);

    SubjectAuthority lowerGenerations = authority(3, 5, SubjectAuthority.State.REVOKED);
    SubjectAuthority advancedGenerations = authority(5, 7, SubjectAuthority.State.REVOKED);

    assertFalse(installed.allowsTransitionTo(lowerGenerations));
    assertFalse(installed.allowsTransitionTo(advancedGenerations));
    assertTrue(installed.allowsTransitionTo(installed.finishRevocation(
        SubjectAuthority.State.REVOKED)));
  }

  private static SubjectAuthority authority(long credentialGeneration, long authorizationGeneration,
      SubjectAuthority.State state) {
    return new SubjectAuthority(ByteKey.copyOf(new byte[32]), "milvus", "provider", SCOPE,
        credentialGeneration, authorizationGeneration, state);
  }
}
