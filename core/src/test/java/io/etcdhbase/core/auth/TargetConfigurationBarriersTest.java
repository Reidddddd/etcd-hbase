package io.etcdhbase.core.auth;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.AuthorizationBarrierInstaller;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.request.RequestScope;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class TargetConfigurationBarriersTest {
  @Test
  void neverTouchesTheControlOfAnUnknownOrDifferentDataset() {
    var installer = new AuthorizationBarrierInstaller(new CommitControlStore() {
      @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
        throw new AssertionError("wrong dataset reached control");
      }
      @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState state, RequestScope scope) {
        throw new AssertionError("wrong dataset reached control");
      }
      @Override public CompletableFuture<Boolean> compareAndSet(ControlSnapshot old, CommitState next,
          RequestScope scope) { throw new AssertionError("wrong dataset reached control"); }
    });
    var barriers = new TargetConfigurationBarriers(Map.of("a",
        new TargetConfigurationBarriers.Group(bytes(1), installer)));
    var wrongDataset = new AuthConfigurationUpdate.Target("a", bytes(2), 1, false);
    var missing = new AuthConfigurationUpdate.Target("b", bytes(1), 1, false);
    try (var scope = RequestScope.withoutDeadline()) {
      assertThrows(CompletionException.class, () -> barriers.allocate(wrongDataset, scope).toCompletableFuture().join());
      assertThrows(CompletionException.class, () -> barriers.install(wrongDataset, scope).toCompletableFuture().join());
      assertThrows(CompletionException.class, () -> barriers.install(missing, scope).toCompletableFuture().join());
    }
  }

  private static ByteKey bytes(int value) {
    byte[] result = new byte[32];
    java.util.Arrays.fill(result, (byte) value);
    return ByteKey.copyOf(result);
  }
}
