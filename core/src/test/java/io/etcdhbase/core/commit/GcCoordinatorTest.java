package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class GcCoordinatorTest {
  private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(50), ZoneOffset.UTC);
  private static final CommitOwnerService.OwnerToken OWNER = new CommitOwnerService.OwnerToken("owner", 1);

  @Test
  void requiresConfirmedLogicalOperationAndBothIndependentPinFloors() {
    var fixture = new CompactionServiceTest.Fixture(8, 7);
    try (var scope = RequestScope.withoutDeadline()) {
      var pins = SnapshotPinManager.initializeFresh(fixture.records,
          new SnapshotPinConfiguration(2, 4, Duration.ofMinutes(1)), scope).join();
      var coordinator = new GcCoordinator(fixture.control, fixture.service, pins, CLOCK);
      assertThrows(CompletionException.class, () -> coordinator.authorizeHistory(OWNER, scope).join());
      ByteKey profile = fixture.control.state.profileFingerprint();
      var kvPin = pins.register(view(profile, 4, 7), scope).join();
      var auxiliaryPin = pins.registerAuxiliary(view(profile, 8, 3), scope).join();
      var operation = fixture.compact(6, scope);
      var permit = coordinator.authorizeHistory(OWNER, scope).join();
      assertEquals(4, permit.safeRevision());
      assertEquals(3, permit.safeSequence());
      assertEquals(profile, permit.profile());
      assertEquals(pins.registryIdentity(), permit.registryIdentity());
      assertEquals(operation.id(), permit.operationId());
      assertEquals(operation.controlVersion() + 1, permit.generation());
      assertThrows(CompletionException.class, () -> pins.register(view(profile, 4, 7), scope).join());
      kvPin.complete(scope).join();
      assertEquals(6, coordinator.authorizeHistory(OWNER, scope).join().safeRevision());
      assertEquals(3, coordinator.authorizeHistory(OWNER, scope).join().safeSequence());
      auxiliaryPin.complete(scope).join();
      assertEquals(7, coordinator.authorizeHistory(OWNER, scope).join().safeSequence());
      permit.requireUsable(profile, scope);
      try (var other = RequestScope.withoutDeadline()) {
        assertThrows(IllegalStateException.class, () -> permit.requireUsable(profile, other));
      }
      assertThrows(IllegalStateException.class,
          () -> permit.requireUsable(ByteKey.copyOf(new byte[32]), scope));
      scope.cancel();
      assertThrows(IllegalStateException.class, () -> permit.requireUsable(profile, scope));
    }
  }

  @Test
  void staleOwnerCannotObtainDeletionPermissionEvenAfterLogicalSelection() {
    var fixture = new CompactionServiceTest.Fixture(8, 7);
    try (var scope = RequestScope.withoutDeadline()) {
      var pins = SnapshotPinManager.initializeFresh(fixture.records,
          new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1)), scope).join();
      fixture.compact(6, scope);
      fixture.control.state = fixture.control.state.claimOwner("replacement", 101, 200);
      var coordinator = new GcCoordinator(fixture.control, fixture.service, pins, CLOCK);
      var exception = assertThrows(CompletionException.class,
          () -> coordinator.authorizeHistory(OWNER, scope).join());
      assertEquals(BackendFailure.Category.UNAVAILABLE,
          assertInstanceOf(BackendFailure.class, CompletionFailures.unwrapCompletion(exception)).category());
      assertEquals(0, pins.readBoundary(scope).join().safeRevision());
    }
  }

  private static ReadView view(ByteKey profile, long revision, long sequence) {
    return ReadView.capture(CommitState.restore(profile, Optional.empty(), 0, 0, 1,
        sequence, revision, sequence, revision, Optional.empty(), 0), revision,
        ByteKey.copyOf(new byte[32]), Optional.empty(), Optional.empty());
  }
}
