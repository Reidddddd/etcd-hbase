package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class HistoryGcCheckpointStoreTest {
  private static final ByteKey PROFILE = ByteKey.copyOf(new byte[32]);
  private static final ByteKey REGISTRY = ByteKey.copyOf(TransactionId.create().bytes());
  private static final InvocationId OPERATION = InvocationId.create();

  @Test
  void encodingDistinguishesAnEmptyBusinessKeyFromAnAbsentCursor() {
    try (var scope = scope()) {
      var empty = ByteKey.copyOf(new byte[0]);
      var checkpoint = HistoryGcCheckpoint.start("group", 2, 0, 2, permit(100, 200, scope))
          .beginKey(empty);
      assertEquals(checkpoint, HistoryGcCheckpoint.decode(checkpoint.encode()));
      assertTrue(checkpoint.afterKey().isEmpty());
      assertEquals(Optional.of(empty), checkpoint.activeKey());
      var finished = checkpoint.afterPage(0, true, 0).finishBucket();
      assertEquals(Optional.of(empty), HistoryGcCheckpoint.decode(finished.encode()).afterKey());
      assertEquals(100, finished.completedRevision());
      assertEquals(5, finished.completedGeneration());
    }
  }

  @Test
  void rejectsTruncatedOversizedAndTrailingCheckpointBytes() {
    try (var scope = scope()) {
      var checkpoint = HistoryGcCheckpoint.start("g".repeat(255), 2, 0, 2, permit(100, 200, scope))
          .beginKey(ByteKey.copyOf(new byte[32767]));
      assertEquals(checkpoint, HistoryGcCheckpoint.decode(checkpoint.encode()));
      byte[] bytes = checkpoint.encode().bytes();
      for (int length : new int[] {0, 4, bytes.length - 1, bytes.length + 1}) {
        assertThrows(IllegalArgumentException.class,
            () -> HistoryGcCheckpoint.decode(ByteKey.copyOf(Arrays.copyOf(bytes, length))));
      }
      assertThrows(IllegalArgumentException.class, () -> HistoryGcCheckpoint.decode(
          ByteKey.copyOf(new byte[HistoryGcCheckpoint.MAX_ENCODED_BYTES + 1])));
    }
  }

  @Test
  void restartResumesTheExactActiveVersionPageAfterAnUnknownIntentAcknowledgement() {
    var records = new MemoryRecords();
    try (var scope = scope()) {
      var permit = permit(100, 200, scope);
      var first = store(records);
      var acquired = first.claim(2, 0, permit, scope).join().orElseThrow();
      records.loseNextAcknowledgement = true;
      assertThrows(CompletionException.class, () -> first.beginKey(acquired, key(2), scope).join());
      var restarted = store(records);
      var active = restarted.claim(2, 0, permit, scope).join().orElseThrow();
      assertEquals(Optional.of(key(2)), active.checkpoint().activeKey());
      assertTrue(restarted.afterPage(active, 64, false, 64, scope).join());
      var resumed = store(records).claim(2, 0, permit, scope).join().orElseThrow();
      assertEquals(64, resumed.checkpoint().afterRevision());
      assertEquals(0, resumed.checkpoint().completedRevision());
      assertTrue(resumed.checkpoint().afterKey().isEmpty());
    }
  }

  @Test
  void incompleteVersionsCannotAdvanceTheKeyOrPublishCompletion() {
    var records = new MemoryRecords();
    try (var scope = scope()) {
      var store = store(records);
      var permit = permit(100, 200, scope);
      var acquired = store.claim(2, 0, permit, scope).join().orElseThrow();
      assertTrue(store.beginKey(acquired, key(2), scope).join());
      var active = store.claim(2, 0, permit, scope).join().orElseThrow();
      assertThrows(IllegalStateException.class, () -> store.finishBucket(active, scope));
      assertTrue(store.afterPage(active, 64, false, 64, scope).join());
      var partial = store.read(0, scope).join().orElseThrow();
      assertTrue(partial.afterKey().isEmpty());
      assertEquals(Optional.of(key(2)), partial.activeKey());
      assertEquals(0, partial.completedRevision());
      var finalPage = store.claim(2, 0, permit, scope).join().orElseThrow();
      assertTrue(store.afterPage(finalPage, 99, true, 35, scope).join());
      var drained = store.claim(2, 0, permit, scope).join().orElseThrow();
      assertEquals(Optional.of(key(2)), drained.checkpoint().afterKey());
      assertEquals(0, drained.checkpoint().completedRevision(), "EOF has not been confirmed yet");
      assertTrue(store.finishBucket(drained, scope).join());
      assertEquals(100, store.read(0, scope).join().orElseThrow().completedRevision());
    }
  }

  @Test
  void aNewOwnerFencesOldProgressButKeepsTheConfirmedCursor() {
    var records = new MemoryRecords();
    try (var scope = scope()) {
      var store = store(records);
      var permit = permit(100, 200, scope);
      var initial = store.claim(2, 0, permit, scope).join().orElseThrow();
      assertTrue(store.beginKey(initial, key(2), scope).join());
      var old = store.claim(2, 0, permit, scope).join().orElseThrow();
      var takeover = store.claim(3, 0, permit, scope).join().orElseThrow();
      assertEquals(old.checkpoint().activeKey(), takeover.checkpoint().activeKey());
      assertFalse(store.afterPage(old, 64, false, 64, scope).join());
      assertTrue(store.claim(2, 0, permit, scope).join().isEmpty());
      assertTrue(store.afterPage(takeover, 64, false, 64, scope).join());
    }
  }

  @Test
  void strongerFloorsWaitForTheNextSweepAndKeepThePreviousCompletionReceipt() {
    var records = new MemoryRecords();
    try (var scope = scope()) {
      var store = store(records);
      var first = store.claim(2, 0, permit(50, 100, scope), scope).join().orElseThrow();
      assertTrue(store.beginKey(first, key(2), scope).join());
      var wider = store.claim(2, 0, permit(100, 200, scope), scope).join().orElseThrow();
      assertEquals(50, wider.checkpoint().safeRevision());
      assertTrue(store.afterPage(wider, 0, true, 0, scope).join());
      var drained = store.claim(2, 0, permit(100, 200, scope), scope).join().orElseThrow();
      assertTrue(store.finishBucket(drained, scope).join());
      var next = store.claim(2, 0, permit(100, 200, scope), scope).join().orElseThrow().checkpoint();
      assertEquals(100, next.safeRevision());
      assertEquals(50, next.completedRevision(), "a conservative pin floor is not completion at 100");
      assertFalse(next.exhausted());
      assertTrue(next.afterKey().isEmpty());
    }
  }

  @Test
  void foreignBucketKeysCannotBecomeDurableWorkOrCompletion() {
    var records = new MemoryRecords();
    try (var scope = scope()) {
      var store = store(records);
      var acquired = store.claim(2, 0, permit(100, 200, scope), scope).join().orElseThrow();
      assertThrows(IllegalArgumentException.class, () -> store.beginKey(acquired, key(1), scope));
      assertTrue(store.read(0, scope).join().orElseThrow().activeKey().isEmpty());
    }
  }

  @Test
  void permitRestrictionCannotWidenBoundsOrEscapeItsScope() {
    try (var scope = scope(); var another = scope()) {
      var original = permit(100, 200, scope);
      var restricted = original.restrictTo(50, 80);
      assertEquals(original.operationId(), restricted.operationId());
      assertEquals(original.registryIdentity(), restricted.registryIdentity());
      assertEquals(original.generation(), restricted.generation());
      assertEquals(50, restricted.safeRevision());
      assertEquals(80, restricted.safeSequence());
      restricted.requireUsable(PROFILE, scope);
      assertThrows(IllegalArgumentException.class, () -> original.restrictTo(101, 80));
      assertThrows(IllegalArgumentException.class, () -> original.restrictTo(50, 201));
      assertThrows(IllegalStateException.class, () -> restricted.requireUsable(PROFILE, another));
    }
  }

  @Test
  void aDifferentProfileOrPinRegistryCannotClaimHistoryProgress() {
    var records = new MemoryRecords();
    try (var scope = scope()) {
      byte[] otherBytes = new byte[32];
      otherBytes[0] = 1;
      var other = ByteKey.copyOf(otherBytes);
      var store = store(records);
      var foreignProfile = new HistoryGcPermit(other, REGISTRY, OPERATION, 5, 100, 200, scope);
      var foreignRegistry = new HistoryGcPermit(PROFILE, ByteKey.copyOf(TransactionId.create().bytes()),
          OPERATION, 5, 100, 200, scope);
      assertThrows(IllegalStateException.class, () -> store.claim(2, 0, foreignProfile, scope));
      assertThrows(IllegalArgumentException.class, () -> store.claim(2, 0, foreignRegistry, scope));
      assertTrue(records.values.isEmpty());
    }
  }

  private static HistoryGcCheckpointStore store(MemoryRecords records) {
    return new HistoryGcCheckpointStore(records, "group", PROFILE, REGISTRY, 2,
        key -> key.length() == 0 ? 0 : Byte.toUnsignedInt(key.bytes()[0]) & 1);
  }

  private static HistoryGcPermit permit(long revision, long sequence, RequestScope scope) {
    return new HistoryGcPermit(PROFILE, REGISTRY, OPERATION, 5, revision, sequence, scope);
  }

  private static ByteKey key(int value) { return ByteKey.copyOf(new byte[] {(byte) value}); }
  private static RequestScope scope() { return RequestScope.withTimeout(Duration.ofSeconds(5)); }

  /** Models only exact immutable record CAS, including an acknowledgement lost after persistence. */
  private static final class MemoryRecords implements SystemRecordStore {
    private final ConcurrentHashMap<ByteKey, ByteKey> values = new ConcurrentHashMap<>();
    private boolean loseNextAcknowledgement;

    @Override public CompletableFuture<Optional<ByteKey>> read(SystemRecordType type,
        ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(values.get(identity)));
    }

    @Override public int bucket(SystemRecordType type, ByteKey identity) { return 0; }

    @Override public CompletableFuture<CreateResult> createIfAbsent(SystemRecordType type,
        ByteKey identity, ByteKey value, RequestScope scope) {
      return CompletableFuture.completedFuture(values.putIfAbsent(identity, value) == null
          ? CreateResult.CREATED : CreateResult.ALREADY_PRESENT);
    }

    @Override public CompletableFuture<Boolean> compareAndSet(SystemRecordType type,
        ByteKey identity, Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) {
      boolean updated = expected.isPresent()
          ? values.replace(identity, expected.orElseThrow(), replacement)
          : values.putIfAbsent(identity, replacement) == null;
      if (loseNextAcknowledgement) {
        loseNextAcknowledgement = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(updated);
    }
  }
}
