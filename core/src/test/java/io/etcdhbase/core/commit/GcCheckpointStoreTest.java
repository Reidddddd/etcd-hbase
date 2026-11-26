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
import org.junit.jupiter.api.Test;

/** Exercises durable shard identity, strict encoding, sweeps and exact-value takeover fencing. */
class GcCheckpointStoreTest {
  private static final String GROUP = "gc-group";
  private static final InvocationId OPERATION = InvocationId.create();
  private static final GcCheckpoint.Plan PLAN = new GcCheckpoint.Plan(OPERATION, 5, 20, 16);

  @Test
  void roundTripsEveryDomainAndMaximumGroupAndCursor() {
    for (var domain : SystemRecordClosure.Domain.values()) {
      var checkpoint = new GcCheckpoint("g".repeat(255), domain, 3, 65_535, 65_536, PLAN,
          new GcCheckpoint.Position(Optional.of(ByteKey.copyOf(new byte[GcCheckpoint.MAX_CURSOR_BYTES]))),
          7, 2, true);
      assertEquals(checkpoint, GcCheckpoint.decode(checkpoint.encode()));
      assertTrue(checkpoint.encode().length() <= GcCheckpoint.MAX_ENCODED_BYTES);
    }
    assertThrows(IllegalArgumentException.class, () -> new GcCheckpoint.Position(
        Optional.of(ByteKey.copyOf(new byte[GcCheckpoint.MAX_CURSOR_BYTES + 1]))));
  }

  @Test
  void rejectsNonBooleanPresenceHiddenCursorMalformedUtf8AndTrailingBytes() {
    var checkpoint = proposal(3, 1, PLAN).after(page(false));
    byte[] valid = checkpoint.encode().bytes();
    int presence = valid.length - 1 - Integer.BYTES - 1;
    for (int value : new int[] {0, 2, 255}) {
      byte[] invalid = valid.clone();
      invalid[presence] = (byte) value;
      assertThrows(IllegalArgumentException.class, () -> GcCheckpoint.decode(ByteKey.copyOf(invalid)));
    }
    byte[] utf8 = valid.clone();
    utf8[8] = (byte) 0xff;
    assertThrows(IllegalArgumentException.class, () -> GcCheckpoint.decode(ByteKey.copyOf(utf8)));
    assertThrows(IllegalArgumentException.class, () -> GcCheckpoint.decode(
        ByteKey.copyOf(java.util.Arrays.copyOf(valid, valid.length + 1))));
    for (int length = 0; length < valid.length; length++) {
      byte[] truncated = java.util.Arrays.copyOf(valid, length);
      assertThrows(IllegalArgumentException.class, () -> GcCheckpoint.decode(ByteKey.copyOf(truncated)));
    }
  }

  @Test
  void rejectsTopologyChangesOnResumeAndTakeover() {
    var store = new GcCheckpointStore(new MemoryRecords());
    try (var scope = scope()) {
      store.claim(proposal(3, 4, PLAN), scope).join();
      for (long epoch : new long[] {3, 4}) {
        assertThrows(java.util.concurrent.CompletionException.class,
            () -> store.claim(proposal(epoch, 8, PLAN), scope).join());
      }
    }
  }

  @Test
  void sameEpochResumesAndTakeoverPreservesProgressWhileStaleCasLoses() {
    var store = new GcCheckpointStore(new MemoryRecords());
    try (var scope = scope()) {
      var first = claim(store, proposal(2, 1, PLAN), scope);
      assertTrue(store.advance(first, page(false), scope).join());
      var resumed = claim(store, proposal(2, 1, PLAN), scope);
      assertEquals(Optional.of(key(7)), resumed.checkpoint().position().afterIdentity());
      assertEquals(1, resumed.checkpoint().completedPages());
      var takeover = claim(store, proposal(5, 1, PLAN), scope);
      assertEquals(resumed.checkpoint().position(), takeover.checkpoint().position());
      assertEquals(1, takeover.checkpoint().completedPages());
      assertFalse(store.advance(resumed, page(false), scope).join());
      assertInstanceOf(GcCheckpointStore.Superseded.class, store.claim(proposal(2, 1, PLAN), scope).join());
    }
  }

  @Test
  void exhaustedSweepAndNewPermissionRevisitRetainedPrefixesWithoutAba() {
    var store = new GcCheckpointStore(new MemoryRecords());
    try (var scope = scope()) {
      var first = claim(store, proposal(2, 1, PLAN), scope);
      assertTrue(store.advance(first, page(true), scope).join());
      var nextSweep = claim(store, proposal(2, 1, PLAN), scope);
      assertEquals(GcCheckpoint.Position.start(), nextSweep.checkpoint().position());
      assertEquals(1, nextSweep.checkpoint().sweep());
      assertEquals(1, nextSweep.checkpoint().completedPages());
      assertFalse(store.advance(first, page(false), scope).join());
      assertTrue(store.advance(nextSweep, page(false), scope).join());
      var wider = new GcCheckpoint.Plan(OPERATION, 5, 21, 16);
      var changed = claim(store, proposal(2, 1, wider), scope);
      assertEquals(2, changed.checkpoint().sweep());
      assertEquals(GcCheckpoint.Position.start(), changed.checkpoint().position());
      var newCompact = new GcCheckpoint.Plan(InvocationId.create(), 6, 21, 17);
      assertEquals(3, claim(store, proposal(2, 1, newCompact), scope).checkpoint().sweep());
      assertInstanceOf(GcCheckpointStore.Superseded.class, store.claim(proposal(2, 1, PLAN), scope).join());
    }
  }

  @Test
  void rejectsRowValueShardMismatchInClaimAndRead() {
    var records = new MemoryRecords();
    var store = new GcCheckpointStore(records);
    try (var scope = scope()) {
      var wrong = new GcCheckpoint("other", SystemRecordClosure.Domain.MANIFEST, 3, 0, 1,
          PLAN, GcCheckpoint.Position.start(), 0, 0, false);
      records.createIfAbsent(SystemRecordType.RECOVERY_CHECKPOINT,
          GcCheckpoint.identity(GROUP, wrong.domain(), 0), wrong.encode(), scope).join();
      assertThrows(java.util.concurrent.CompletionException.class,
          () -> store.claim(proposal(3, 1, PLAN), scope).join());
      assertThrows(java.util.concurrent.CompletionException.class,
          () -> store.read(GROUP, wrong.domain(), 0, scope).join());
    }
  }

  private static GcCheckpoint proposal(long epoch, int buckets, GcCheckpoint.Plan plan) {
    return new GcCheckpoint(GROUP, SystemRecordClosure.Domain.MANIFEST, epoch, 0, buckets,
        plan, GcCheckpoint.Position.start(), 0, 0, false);
  }

  private static GcCheckpointStore.Acquired claim(GcCheckpointStore store, GcCheckpoint proposed,
      RequestScope scope) {
    return assertInstanceOf(GcCheckpointStore.Acquired.class, store.claim(proposed, scope).join());
  }

  private static SystemRecordClosure.Page page(boolean exhausted) {
    return new SystemRecordClosure.Page(SystemRecordClosure.Domain.MANIFEST, 1, 0,
        Optional.of(key(7)), exhausted);
  }
  private static ByteKey key(int value) { return ByteKey.copyOf(new byte[] {(byte) value}); }
  private static RequestScope scope() { return RequestScope.withTimeout(Duration.ofSeconds(5)); }

  /** Minimal exact-value store: only the compare-and-set semantics under test are modelled. */
  private static final class MemoryRecords implements SystemRecordStore {
    private final ConcurrentHashMap<ByteKey, ByteKey> values = new ConcurrentHashMap<>();

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
      return CompletableFuture.completedFuture(updated);
    }
  }
}
