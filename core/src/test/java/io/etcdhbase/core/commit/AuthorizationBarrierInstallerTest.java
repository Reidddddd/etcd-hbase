package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class AuthorizationBarrierInstallerTest {
  private static final String OWNER = "authorization-barrier-owner";
  private static final long NOW = 20;
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);
  private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(NOW), ZoneOffset.UTC);

  @Test
  void allocatesTargetAfterCallerHasMadeRevocationDurableAndInstallsIt() {
    var store = new MemoryControlStore(enabledState(100));
    var installer = new AuthorizationBarrierInstaller(store, CLOCK);
    try (var scope = RequestScope.withoutDeadline()) {
      long target = installer.nextTargetGeneration(scope).join();

      assertEquals(1, target);
      assertEquals(0, store.casCalls);
      assertEquals(1, installer.installAtLeast(target, scope).join());
      assertEquals(1, store.current().authorizationBarrierGeneration());
      assertEquals(1, store.casCalls);
    }
  }

  @Test
  void treatsAnotherBarrierInstalledAfterTargetReservationAsSufficient() {
    var store = new MemoryControlStore(enabledState(100));
    var installer = new AuthorizationBarrierInstaller(store, CLOCK);
    try (var scope = RequestScope.withoutDeadline()) {
      long target = installer.nextTargetGeneration(scope).join();
      store.advanceBarrier();

      assertEquals(target, installer.installAtLeast(target, scope).join());
      assertEquals(0, store.casCalls);
    }
  }

  @Test
  void resolvesUnknownCasOnlyAfterStrongReadShowsTheTarget() {
    var store = new MemoryControlStore(enabledState(100));
    store.persistBeforeUnknown = true;
    var installer = new AuthorizationBarrierInstaller(store, CLOCK);
    try (var scope = RequestScope.withoutDeadline()) {
      long target = installer.nextTargetGeneration(scope).join();

      assertEquals(1, installer.installAtLeast(target, scope).join());
      assertEquals(3, store.reads);
      assertEquals(1, store.current().authorizationBarrierGeneration());
    }
  }

  @Test
  void refusesToReserveTargetWithoutEnabledBarrierAndLiveOwner() {
    try (var scope = RequestScope.withoutDeadline()) {
      var noBarrier = new AuthorizationBarrierInstaller(
          new MemoryControlStore(ownedState(100)), CLOCK);
      var barrierFailure = assertThrows(CompletionException.class,
          () -> noBarrier.nextTargetGeneration(scope).join());
      assertEquals(BackendFailure.Category.UNAVAILABLE,
          ((BackendFailure) barrierFailure.getCause()).category());

      var expiredOwner = new AuthorizationBarrierInstaller(
          new MemoryControlStore(enabledState(NOW)), CLOCK);
      var ownerFailure = assertThrows(CompletionException.class,
          () -> expiredOwner.nextTargetGeneration(scope).join());
      assertEquals(BackendFailure.Category.UNAVAILABLE,
          ((BackendFailure) ownerFailure.getCause()).category());
    }
  }

  private static CommitState ownedState(long leaseDeadline) {
    return CommitState.initial(FINGERPRINT, 1).claimOwner(OWNER, 1, leaseDeadline);
  }

  private static CommitState enabledState(long leaseDeadline) {
    CommitState owned = ownedState(leaseDeadline);
    return owned.enableAuthorizationBarrier(OWNER, owned.ownerEpoch(), NOW - 1);
  }

  private static final class MemoryControlStore implements CommitControlStore {
    private final AtomicReference<ControlSnapshot> current;
    private int reads;
    private int casCalls;
    private boolean persistBeforeUnknown;

    private MemoryControlStore(CommitState state) {
      current = new AtomicReference<>(snapshot(state));
    }

    private CommitState current() { return current.get().state(); }

    @Override
    public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      reads++;
      return CompletableFuture.completedFuture(Optional.of(current.get()));
    }

    @Override
    public CompletableFuture<Boolean> initializeIfAbsent(CommitState initialState, RequestScope scope) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      casCalls++;
      if (current.get() != observed) return CompletableFuture.completedFuture(false);
      current.set(snapshot(replacement));
      return persistBeforeUnknown
          ? CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN))
          : CompletableFuture.completedFuture(true);
    }

    private void advanceBarrier() {
      CommitState observed = current();
      CommitState advanced = observed.advanceAuthorizationBarrier(
          OWNER, observed.ownerEpoch(), observed.authorizationBarrierGeneration());
      current.set(snapshot(advanced));
    }

    private static ControlSnapshot snapshot(CommitState state) {
      return new ControlSnapshot(state,
          ByteBuffer.allocate(Long.BYTES).putLong(state.stateVersion()).array());
    }
  }
}
