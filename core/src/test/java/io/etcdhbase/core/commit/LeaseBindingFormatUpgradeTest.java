package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class LeaseBindingFormatUpgradeTest {
  @Test
  void upgradeFencesJ40WriterAndResolvesLostControlCasResponse() {
    CommitState auxiliary = CommitState.initial(ByteKey.copyOf(new byte[32]), 1)
        .claimOwner("owner", 1, 100).upgradeAuxiliaryFormat("owner", 1, 2);
    var store = new Store(auxiliary);
    var owner = new CommitOwnerService.OwnerToken("owner", 2);
    try (var scope = RequestScope.withoutDeadline()) {
      var oldSnapshot = store.read(scope).join().orElseThrow();
      store.loseResponse = true;
      var token = new LeaseBindingFormatUpgrade(store).upgrade(owner, 3, scope).join();

      assertEquals(3, token.epoch());
      assertEquals(CommitState.LEASE_BINDING_FORMAT_VERSION, store.current.formatVersion());
      assertThrows(IllegalStateException.class,
          () -> store.current.heartbeat("owner", owner.epoch(), 200));
      assertFalse(store.compareAndSet(oldSnapshot, auxiliary.heartbeat("owner", 2, 200), scope).join());
      assertThrows(CompletionException.class,
          () -> new LeaseBindingFormatUpgrade(store).upgrade(owner, 3, scope).join());
    }
  }

  @Test
  void competingControlRowChangeCannotBeReportedAsSuccessfulUpgrade() {
    CommitState auxiliary = CommitState.initial(ByteKey.copyOf(new byte[32]), 1)
        .claimOwner("owner", 1, 100).upgradeAuxiliaryFormat("owner", 1, 2);
    var store = new Store(auxiliary);
    store.reject = true;
    try (var scope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(CompletionException.class,
          () -> new LeaseBindingFormatUpgrade(store)
              .upgrade(new CommitOwnerService.OwnerToken("owner", 2), 3, scope).join());
      assertEquals(BackendFailure.Category.UNAVAILABLE,
          ((BackendFailure) failure.getCause()).category());
      assertEquals(CommitState.AUXILIARY_FORMAT_VERSION, store.current.formatVersion());
    }
  }

  private static final class Store implements CommitControlStore {
    private CommitState current;
    private int version = 1;
    private boolean loseResponse;
    private boolean reject;

    private Store(CommitState initial) { current = initial; }

    @Override
    public synchronized CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      return CompletableFuture.completedFuture(
          Optional.of(new ControlSnapshot(current, new byte[] {(byte) version})));
    }

    @Override
    public CompletableFuture<Boolean> initializeIfAbsent(CommitState state, RequestScope scope) {
      throw new UnsupportedOperationException();
    }

    @Override
    public synchronized CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      if (reject || observed.comparisonToken()[0] != (byte) version) {
        return CompletableFuture.completedFuture(false);
      }
      current = replacement;
      version++;
      if (loseResponse) {
        loseResponse = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(true);
    }
  }
}
