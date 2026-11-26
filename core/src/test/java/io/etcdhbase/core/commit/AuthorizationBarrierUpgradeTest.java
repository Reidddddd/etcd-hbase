package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class AuthorizationBarrierUpgradeTest {
  private static final String OWNER = "auth-upgrade-owner";
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);

  @Test
  void enablesBarrierWithoutChangingFormatOneAndSixCapabilities() {
    for (CommitState state : new CommitState[] {formatOne(), formatSix()}) {
      var store = new MemoryControlStore(state);
      var owner = new CommitOwnerService.OwnerToken(OWNER, state.ownerEpoch());
      try (var scope = RequestScope.withoutDeadline()) {
        var upgraded = new AuthorizationBarrierUpgrade(store).enable(owner, 20, scope).join();
        CommitState persisted = store.current.get();

        assertEquals(state.formatVersion(), persisted.formatVersion());
        assertTrue(persisted.supportsAuthorizationBarrier());
        assertEquals(0, persisted.authorizationBarrierGeneration());
        assertEquals(owner.epoch() + 1, upgraded.epoch());
        assertEquals(upgraded.epoch(), persisted.ownerEpoch());
      }
    }
  }

  @Test
  void resolvesAnUnknownCasOnlyAfterStrongReadShowsTheBarrierAndOwnerFence() {
    var store = new MemoryControlStore(formatOne());
    store.persistBeforeUnknown = true;
    var owner = new CommitOwnerService.OwnerToken(OWNER, 1);
    try (var scope = RequestScope.withoutDeadline()) {
      var upgraded = new AuthorizationBarrierUpgrade(store).enable(owner, 20, scope).join();

      assertEquals(2, upgraded.epoch());
      assertTrue(store.current.get().supportsAuthorizationBarrier());
      assertEquals(2, store.reads);
    }
  }

  @Test
  void doesNotAcceptUnknownCasWithoutDurableBarrierEvidence() {
    var store = new MemoryControlStore(formatOne());
    store.unknownWithoutPersist = true;
    try (var scope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(CompletionException.class, () ->
          new AuthorizationBarrierUpgrade(store)
              .enable(new CommitOwnerService.OwnerToken(OWNER, 1), 20, scope).join());

      assertTrue(failure.getCause() instanceof BackendFailure);
      assertFalse(store.current.get().supportsAuthorizationBarrier());
    }
  }

  @Test
  void alreadyEnabledBarrierIsIdempotentForTheCurrentOwner() {
    var enabled = formatOne().enableAuthorizationBarrier(OWNER, 1, 11);
    var store = new MemoryControlStore(enabled);
    try (var scope = RequestScope.withoutDeadline()) {
      var token = new AuthorizationBarrierUpgrade(store)
          .enable(new CommitOwnerService.OwnerToken(OWNER, 2), 20, scope).join();

      assertEquals(2, token.epoch());
      assertEquals(0, store.casCalls);
    }
  }

  private static CommitState formatOne() {
    return CommitState.initial(FINGERPRINT, 1).claimOwner(OWNER, 10, 100);
  }

  private static CommitState formatSix() {
    var namespace = ByteKey.copyOf(new byte[16]);
    return formatOne().upgradeAuxiliaryFormat(OWNER, 1, 11)
        .upgradeLeaseBindingFormat(OWNER, 2, 12)
        .upgradeLeaseExpiryFormat(OWNER, 3, 13)
        .upgradeEventIndexFormat(OWNER, 4, 14)
        .reserveEventRegistry(OWNER, 5, 15, namespace)
        .upgradeEventRegistryFormat(OWNER, 6, 16, namespace);
  }

  private static final class MemoryControlStore implements CommitControlStore {
    private final AtomicReference<CommitState> current;
    private int reads;
    private int casCalls;
    private boolean persistBeforeUnknown;
    private boolean unknownWithoutPersist;

    private MemoryControlStore(CommitState initial) {
      current = new AtomicReference<>(initial);
    }

    @Override
    public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      reads++;
      CommitState state = current.get();
      return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state,
          ByteBuffer.allocate(Long.BYTES).putLong(state.stateVersion()).array())));
    }

    @Override
    public CompletableFuture<Boolean> initializeIfAbsent(CommitState initialState, RequestScope scope) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      casCalls++;
      if (persistBeforeUnknown) {
        current.set(replacement);
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      if (unknownWithoutPersist) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      if (current.get() != observed.state()) return CompletableFuture.completedFuture(false);
      current.set(replacement);
      return CompletableFuture.completedFuture(true);
    }
  }
}
