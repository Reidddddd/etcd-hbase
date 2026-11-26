package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * §6.3 requires a checkpoint to resume a shard and a takeover to keep the previous progress while a
 * stale worker loses its update. These are the properties that make background GC restartable.
 */
class GcCheckpointStoreTest {
  private static final String GROUP = "gc-group";

  @Test
  void roundTripsEveryFieldAndRejectsMalformedBytes() {
    var checkpoint = new GcCheckpoint(GROUP, SystemRecordClosure.Domain.MANIFEST, 3, 2, 8, 11, 12,
        new GcCheckpoint.Position(Optional.of(ByteKey.copyOf(new byte[] {4, 5}))), 7);
    var decoded = GcCheckpoint.decode(checkpoint.encode());
    assertEquals(checkpoint, decoded);

    var start = new GcCheckpoint(GROUP, SystemRecordClosure.Domain.DECISION, 1, 0, 1, 0, 0,
        GcCheckpoint.Position.start(), 0);
    assertEquals(start, GcCheckpoint.decode(start.encode()));

    assertThrows(IllegalArgumentException.class,
        () -> GcCheckpoint.decode(ByteKey.copyOf(new byte[] {'G', 'C', 'V', 2})));
    assertThrows(IllegalArgumentException.class,
        () -> GcCheckpoint.decode(ByteKey.copyOf(new byte[0])));
  }

  @Test
  void takeoverKeepsProgressAndNeverWidensTheProtectedBoundary() {
    var previous = new GcCheckpoint(GROUP, SystemRecordClosure.Domain.MANIFEST, 2, 1, 4, 9, 10,
        new GcCheckpoint.Position(Optional.of(ByteKey.copyOf(new byte[] {7}))), 5);
    var proposed = new GcCheckpoint(GROUP, SystemRecordClosure.Domain.MANIFEST, 5, 1, 4, 20, 21,
        GcCheckpoint.Position.start(), 0);
    var takeover = proposed.resumeFrom(previous);
    assertEquals(5, takeover.maintenanceEpoch(), "the newer epoch owns the shard");
    assertEquals(9, takeover.protectedSequence(), "a takeover must not widen the boundary");
    assertEquals(10, takeover.protectedRevision());
    assertEquals(previous.position(), takeover.position(), "a takeover resumes, it does not restart");
    assertEquals(5, takeover.completedPages(), "completed progress is preserved");

    assertThrows(IllegalArgumentException.class,
        () -> previous.resumeFrom(new GcCheckpoint(GROUP, SystemRecordClosure.Domain.MANIFEST, 9, 1,
            4, 0, 0, GcCheckpoint.Position.start(), 0)),
        "a takeover must be strictly newer");
    assertTrue(previous.isSupersededBy(takeover));
    assertFalse(takeover.isSupersededBy(previous));
  }

  @Test
  void firstClaimCreatesAndSameEpochResumesWithoutResettingProgress() throws Exception {
    var records = new MemoryRecords();
    var store = new GcCheckpointStore(records);
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var first = await(store.claim(proposal(2, 7), scope));
      assertInstanceOf(GcCheckpointStore.Acquired.class, first);
      var acquired = (GcCheckpointStore.Acquired) first;
      await(store.advance(advance(acquired.checkpoint(), 3), acquired.exactBytes(), scope));

      var resumed = (GcCheckpointStore.Acquired) await(store.claim(proposal(2, 0), scope));
      assertEquals(3, resumed.checkpoint().completedPages(),
          "a same-epoch claim resumes the durable progress instead of resetting it");
    }
  }

  @Test
  void aNewerEpochTakesOverAndTheOldWorkerLosesItsUpdate() throws Exception {
    var records = new MemoryRecords();
    var store = new GcCheckpointStore(records);
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var held = (GcCheckpointStore.Acquired) await(store.claim(proposal(2, 7), scope));

      var newer = (GcCheckpointStore.Acquired) await(store.claim(proposal(5, 7), scope));
      assertEquals(5, newer.checkpoint().maintenanceEpoch());

      // The fenced worker still holds the bytes it read before the takeover.
      assertFalse(await(store.advance(advance(held.checkpoint(), 9), held.exactBytes(), scope)),
          "a superseded worker must not advance the shard");

      var current = await(store.read(GROUP, SystemRecordClosure.Domain.MANIFEST, 0, scope))
          .orElseThrow();
      assertEquals(5, current.maintenanceEpoch());
      assertEquals(0, current.completedPages(), "the takeover's progress wins, not the stale write");
    }
  }

  @Test
  void anOlderEpochIsReportedSupersededInsteadOfOverwriting() throws Exception {
    var records = new MemoryRecords();
    var store = new GcCheckpointStore(records);
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      await(store.claim(proposal(6, 7), scope));
      var result = await(store.claim(proposal(4, 7), scope));
      assertInstanceOf(GcCheckpointStore.Superseded.class, result);
      assertEquals(6, ((GcCheckpointStore.Superseded) result).observed().maintenanceEpoch());
    }
  }

  private static GcCheckpoint proposal(long epoch, long protectedSequence) {
    return new GcCheckpoint(GROUP, SystemRecordClosure.Domain.MANIFEST, epoch, 0, 1,
        protectedSequence, protectedSequence, GcCheckpoint.Position.start(), 0);
  }

  private static GcCheckpoint advance(GcCheckpoint current, long pages) {
    return new GcCheckpoint(current.tableGroupId(), current.domain(), current.maintenanceEpoch(),
        current.bucket(), current.bucketCount(), current.protectedSequence(),
        current.protectedRevision(),
        new GcCheckpoint.Position(Optional.of(ByteKey.copyOf(new byte[] {(byte) pages}))), pages);
  }

  private static <T> T await(CompletableFuture<T> future) throws Exception {
    return future.get(5, java.util.concurrent.TimeUnit.SECONDS);
  }

  /** Minimal exact-value store: only the compare-and-set semantics under test are modelled. */
  private static final class MemoryRecords implements SystemRecordStore {
    private final ConcurrentHashMap<ByteKey, ByteKey> values = new ConcurrentHashMap<>();
    private final AtomicReference<String> lastFailure = new AtomicReference<>();

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
      if (!updated) lastFailure.set("compare-and-set lost");
      return CompletableFuture.completedFuture(updated);
    }
  }
}
