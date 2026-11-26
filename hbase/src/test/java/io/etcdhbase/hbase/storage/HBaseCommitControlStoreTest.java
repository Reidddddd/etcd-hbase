package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitState;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class HBaseCommitControlStoreTest {
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);

  @Test
  void authorizationBarrierTransitionIsIndependentOfEventRegistryFormat() {
    var formatOne = CommitState.initial(FINGERPRINT, 7).claimOwner("owner-a", 10, 100);
    var barrierOne = formatOne.enableAuthorizationBarrier("owner-a", 1, 11);
    assertTrue(HBaseCommitControlStore.isValidAuthorizationBarrierTransition(formatOne, barrierOne));
    assertFalse(HBaseCommitControlStore.isValidAuthorizationBarrierTransition(
        formatOne, restoreBarrier(formatOne, 0, formatOne.stateVersion() + 1)));

    var formatFour = formatOne.upgradeAuxiliaryFormat("owner-a", 1, 11)
        .upgradeLeaseBindingFormat("owner-a", 2, 12)
        .upgradeLeaseExpiryFormat("owner-a", 3, 13);
    var barrierFour = formatFour.enableAuthorizationBarrier("owner-a", 4, 14);
    assertTrue(HBaseCommitControlStore.isValidAuthorizationBarrierTransition(formatFour, barrierFour));
    assertTrue(barrierFour.eventRegistryNamespace().isEmpty());

    var namespace = ByteKey.copyOf(new byte[16]);
    var formatSix = formatFour.upgradeEventIndexFormat("owner-a", 4, 14)
        .reserveEventRegistry("owner-a", 5, 15, namespace)
        .upgradeEventRegistryFormat("owner-a", 6, 16, namespace);
    var barrierSix = formatSix.enableAuthorizationBarrier("owner-a", 7, 17);

    assertTrue(HBaseCommitControlStore.isValidAuthorizationBarrierTransition(formatSix, barrierSix));
    assertEquals(CommitState.EVENT_REGISTRY_FORMAT_VERSION, barrierSix.formatVersion());

    var advanced = barrierSix.advanceAuthorizationBarrier("owner-a", 8, 0);
    assertTrue(HBaseCommitControlStore.isValidAuthorizationBarrierTransition(barrierSix, advanced));

    var decreased = restoreBarrier(advanced, 0, advanced.stateVersion() + 1);
    assertFalse(HBaseCommitControlStore.isValidAuthorizationBarrierTransition(advanced, decreased));

    var heartbeat = advanced.heartbeat("owner-a", 8, 200);
    var combinedHeartbeatAndBarrier = restoreBarrier(heartbeat, 2, heartbeat.stateVersion());
    assertFalse(HBaseCommitControlStore.isValidAuthorizationBarrierTransition(
        advanced, combinedHeartbeatAndBarrier));

    var exhausted = restoreBarrier(advanced, Long.MAX_VALUE, advanced.stateVersion() + 1);
    assertThrows(ArithmeticException.class, () -> exhausted.advanceAuthorizationBarrier(
        "owner-a", exhausted.ownerEpoch(), Long.MAX_VALUE));
  }

  private static CommitState restoreBarrier(CommitState state, long barrier, long stateVersion) {
    return CommitState.restore(state.formatVersion(), state.profileFingerprint(), state.ownerInstance(),
        state.ownerEpoch(), state.ownerLeaseDeadlineEpochMillis(), stateVersion,
        state.publishedSequence(), state.publishedRevision(), state.finalizedSequence(),
        state.finalizedRevision(), state.publication(), state.compactRevision(),
        state.eventRegistryNamespace(), state.pendingEventRegistryNamespace(),
        state.eventRegistryAttempts(), true, barrier);
  }
}
