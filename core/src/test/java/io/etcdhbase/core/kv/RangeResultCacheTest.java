package io.etcdhbase.core.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class RangeResultCacheTest {
  private static final ByteKey PROFILE = bytes(1, 32);
  private static final ByteKey AUTH = bytes(2, 32);
  private static final RangeCommand.RevisionFilter NO_FILTER = new RangeCommand.RevisionFilter(0, 0, 0, 0);
  private static final RangeCommand.Sort ASC = new RangeCommand.Sort(1, 0);

  @Test
  void evictsLeastRecentlyUsedEntriesWithinByteAndEntryBounds() throws Exception {
    var budget = budget();
    try (var ownerScope = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline();
         var account = budget.admit("public", scope, reservation());
         var cache = new RangeResultCache(4096, 2, 4, budget, "public", ownerScope)) {
      var a = point(1, AUTH, 10);
      var b = point(2, AUTH, 10);
      var c = point(3, AUTH, 10);
      assertTrue(cache.put(a, List.of(record(1)), 1, true));
      assertTrue(cache.put(b, List.of(), 0, true));
      try (var ignored = cache.get(a, account, scope).orElseThrow()) { }
      assertTrue(cache.put(c, List.of(record(3)), 1, true));
      assertTrue(cache.get(b, account, scope).isEmpty());
      try (var hit = cache.get(a, account, scope).orElseThrow()) {
        assertEquals(1, hit.count());
        assertEquals(record(1).key(), hit.records().getFirst().key());
      }
      assertTrue(cache.retainedBytes() <= 4096);
    }
    assertEquals(0, budget.snapshot().allocatedBytes());
  }

  @Test
  void entryCannotCrossAuthorizationOrBarrierAndHitSurvivesEvictionOnlyWhileCharged() throws Exception {
    var budget = budget();
    try (var ownerScope = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline();
         var account = budget.admit("public", scope, reservation());
         var cache = new RangeResultCache(4096, 1, 4, budget, "public", ownerScope)) {
      var key = point(1, AUTH, 10);
      assertTrue(cache.put(key, List.of(record(1)), 1, true));
      assertTrue(cache.get(point(1, bytes(3, 32), 10), account, scope).isEmpty());
      assertTrue(cache.get(point(1, AUTH, 11), account, scope).isEmpty());
      var hit = cache.get(key, account, scope).orElseThrow();
      assertTrue(budget.snapshot().allocatedBytes() > 4096);
      assertTrue(cache.put(point(2, AUTH, 10), List.of(record(2)), 1, true));
      assertTrue(cache.get(key, account, scope).isEmpty());
      assertEquals(record(1).key(), hit.records().getFirst().key());
      hit.close();
      assertEquals(4096, budget.snapshot().allocatedBytes());
      assertThrows(IllegalStateException.class, hit::records);
    }
  }

  @Test
  void invalidationEvictsOnlyItsAuthorizationAndFencesEarlierFills() throws Exception {
    var budget = budget();
    try (var owner = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline();
         var account = budget.admit("public", scope, reservation());
         var cache = new RangeResultCache(4096, 4, 4, budget, "public", owner)) {
      var oldKey = point(1, AUTH, 10);
      var otherKey = point(2, bytes(3, 32), 10);
      assertTrue(cache.put(oldKey, List.of(record(1)), 1, true));
      assertTrue(cache.put(otherKey, List.of(record(2)), 1, true));
      Object earlier = cache.invalidationGeneration();
      cache.invalidateAuthorization(AUTH);
      assertTrue(cache.get(oldKey, account, scope).isEmpty());
      try (var other = cache.get(otherKey, account, scope).orElseThrow()) {
        assertEquals(record(2).key(), other.records().getFirst().key());
      }
      assertFalse(cache.putIfCurrent(oldKey, List.of(record(1)), 1, true, Optional.empty(), earlier));
      assertTrue(cache.get(oldKey, account, scope).isEmpty());
      assertTrue(cache.putIfCurrent(oldKey, List.of(record(1)), 1, true,
          Optional.empty(), cache.invalidationGeneration()));
    }
    assertEquals(0, budget.snapshot().allocatedBytes());
  }

  @Test
  void refusesIncompleteOversizedAndUnmaterializedResults() throws Exception {
    var budget = budget();
    try (var ownerScope = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline();
         var account = budget.admit("public", scope, reservation());
         var cache = new RangeResultCache(4096, 4, 1, budget, "public", ownerScope)) {
      var key = point(1, AUTH, 10);
      assertFalse(cache.put(key, List.of(), 0, false));
      assertFalse(cache.put(key, List.of(record(1), record(2)), 2, true));
      assertFalse(cache.put(key, List.of(StoredRecord.metadataOnly(record(1).key(), record(1).metadata())), 1, true));
      assertFalse(cache.put(key, List.of(record(2)), 1, true));
      assertTrue(cache.get(key, account, scope).isEmpty());
      assertTrue(cache.put(key, List.of(), 0, true));
      try (var hit = cache.get(key, account, scope).orElseThrow()) { assertEquals(0, hit.count()); }
    }
  }

  @Test
  void refusesAHitWhenRequestOutputBudgetIsInsufficient() throws Exception {
    var budget = budget();
    try (var ownerScope = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline();
         var account = budget.admit("public", scope, new BudgetManager.Reservation(64, 64, 64, 1, 64));
         var cache = new RangeResultCache(4096, 4, 4, budget, "public", ownerScope)) {
      var key = point(1, AUTH, 10);
      assertTrue(cache.put(key, List.of(record(1)), 1, true));
      var failure = assertThrows(BackendFailure.class, () -> cache.get(key, account, scope));
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
      assertEquals(4096, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void scopeCancellationClosesAnOutstandingHit() throws Exception {
    var budget = budget();
    var scope = RequestScope.withoutDeadline();
    try (var ownerScope = RequestScope.withoutDeadline();
         var account = budget.admit("public", scope, reservation());
         var cache = new RangeResultCache(4096, 4, 4, budget, "public", ownerScope)) {
      var key = point(1, AUTH, 10);
      assertTrue(cache.put(key, List.of(record(1)), 1, true));
      var hit = cache.get(key, account, scope).orElseThrow();
      scope.cancel();
      assertEquals(4096, budget.snapshot().allocatedBytes());
      assertThrows(IllegalStateException.class, hit::records);
    }
  }

  @Test
  void cancellationKeepsHitChargedUntilPhysicalProducerDrains() throws Exception {
    var budget = budget();
    var scope = RequestScope.withoutDeadline();
    var producer = new CompletableFuture<Void>();
    scope.trackPhysicalProducer(producer);
    try (var owner = RequestScope.withoutDeadline();
         var account = budget.admit("public", scope, reservation());
         var cache = new RangeResultCache(4096, 4, 4, budget, "public", owner)) {
      var key = point(1, AUTH, 10);
      assertTrue(cache.put(key, List.of(record(1)), 1, true));
      var hit = cache.get(key, account, scope).orElseThrow();
      scope.cancel();
      assertTrue(budget.snapshot().allocatedBytes() > 4096);
      producer.complete(null);
      scope.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertEquals(4096, budget.snapshot().allocatedBytes());
      assertThrows(IllegalStateException.class, hit::records);
    } finally {
      producer.complete(null);
      scope.close();
    }
  }

  @Test
  void ownerCancellationClearsCacheButAnExistingHitKeepsItsRequestCharge() throws Exception {
    var budget = budget();
    var ownerScope = RequestScope.withoutDeadline();
    try (var scope = RequestScope.withoutDeadline();
         var account = budget.admit("public", scope, reservation());
         var cache = new RangeResultCache(4096, 4, 4, budget, "public", ownerScope)) {
      var key = point(1, AUTH, 10);
      assertTrue(cache.put(key, List.of(record(1)), 1, true));
      var hit = cache.get(key, account, scope).orElseThrow();
      ownerScope.cancel();
      assertTrue(budget.snapshot().allocatedBytes() > 0);
      assertTrue(budget.snapshot().allocatedBytes() < 4096);
      assertTrue(cache.get(key, account, scope).isEmpty());
      assertThrows(IllegalStateException.class, hit::records);
      hit.close();
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void cannotCreateCacheWithoutOwnerReservation() throws Exception {
    var limits = new BudgetManager.Limits(1, 1, 1, 1, 1, 1024, 1024);
    var budget = new BudgetManager(limits, Map.of("public", limits), limits);
    try (var ownerScope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(BackendFailure.class,
          () -> new RangeResultCache(4096, 4, 4, budget, "public", ownerScope));
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void serviceMemoryDoesNotConsumeTheOnlyRequestSlotAndRejectsAnotherGroup() throws Exception {
    var limits = new BudgetManager.Limits(1, 1, 1, 1, 1, 1024, 1_000_000);
    var budget = new BudgetManager(limits, Map.of("public", limits, "other", limits), limits);
    try (var ownerScope = RequestScope.withoutDeadline();
         var cache = new RangeResultCache(4096, 4, 4, budget, "public", ownerScope);
         var scope = RequestScope.withoutDeadline();
         var account = budget.admit("other", scope, reservation())) {
      assertEquals(1, budget.snapshot().requests());
      assertEquals(4096, budget.snapshot().allocatedBytes());
      assertTrue(cache.put(point(1, AUTH, 10), List.of(record(1)), 1, true));
      assertThrows(IllegalArgumentException.class, () -> cache.get(point(1, AUTH, 10), account, scope));
      assertThrows(BackendFailure.class, () -> budget.admit("public", scope, reservation()));
    }
    assertEquals(0, budget.snapshot().allocatedBytes());
    assertEquals(0, budget.snapshot().reservedBytes());
  }

  @Test
  void cacheKeyIncludesGroupProfileCompactionAndAllQueryOptions() throws Exception {
    var budget = budget();
    try (var ownerScope = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline();
         var account = budget.admit("public", scope, reservation());
         var cache = new RangeResultCache(4096, 4, 4, budget, "public", ownerScope)) {
      var base = point(1, AUTH, 10);
      assertTrue(cache.put(base, List.of(record(1)), 1, true));
      var different = List.of(
          variant(base, 8, PROFILE, 0, 0, ASC, NO_FILTER),
          variant(base, 7, bytes(4, 32), 0, 0, ASC, NO_FILTER),
          new RangeResultCache.Key(7, PROFILE, bytes(5, 32), AUTH, 10, 10, 0,
              KeyRange.Kind.POINT, base.lower(), base.upper(), 0, 0, ASC, NO_FILTER, false, false, false),
          variant(base, 7, PROFILE, 1, 0, ASC, NO_FILTER),
          variant(base, 7, PROFILE, 0, 1, ASC, NO_FILTER),
          variant(base, 7, PROFILE, 0, 0, new RangeCommand.Sort(2, 0), NO_FILTER),
          variant(base, 7, PROFILE, 0, 0, ASC, new RangeCommand.RevisionFilter(2, 0, 0, 0)));
      for (var key : different) assertTrue(cache.get(key, account, scope).isEmpty());
    }
  }

  @Test
  void byteLimitDoesNotEvictUsefulEntriesForAnOversizedReplacement() throws Exception {
    var budget = budget();
    try (var ownerScope = RequestScope.withoutDeadline();
         var scope = RequestScope.withoutDeadline();
         var account = budget.admit("public", scope, reservation());
         var cache = new RangeResultCache(2000, 4, 4, budget, "public", ownerScope)) {
      var key = point(1, AUTH, 10);
      assertTrue(cache.put(key, List.of(record(1)), 1, true));
      var oversized = StoredRecord.live(bytes(2, 1),
          new StorageMetadata(1, 1, 1, 0, StorageMetadata.State.LIVE, 1500), new byte[1500]);
      assertFalse(cache.put(point(2, AUTH, 10), List.of(oversized), 1, true));
      try (var hit = cache.get(key, account, scope).orElseThrow()) { assertEquals(1, hit.count()); }
      assertTrue(cache.retainedBytes() <= 2000);
    }
  }

  @Test
  void rangeEntryMustMatchTheRequestedOrderLimitAndRevisionFilter() throws Exception {
    var budget = budget();
    try (var ownerScope = RequestScope.withoutDeadline();
         var cache = new RangeResultCache(4096, 4, 4, budget, "public", ownerScope)) {
      var key = new RangeResultCache.Key(7, PROFILE, PROFILE, AUTH, 10, 10, 0, KeyRange.Kind.RANGE,
          Optional.of(bytes(1, 1)), Optional.of(bytes(5, 1)), 0, 2,
          new RangeCommand.Sort(2, 0), new RangeCommand.RevisionFilter(2, 0, 0, 0),
          false, false, false);
      var first = liveAtRevision(3, 2);
      var second = liveAtRevision(2, 2);
      assertFalse(cache.put(key, List.of(second, first), 2, true));
      assertFalse(cache.put(key, List.of(first, second, liveAtRevision(1, 2)), 3, true));
      assertFalse(cache.put(key, List.of(record(3)), 1, true));
      assertTrue(cache.put(key, List.of(first, second), 2, true));
    }
  }

  private static RangeResultCache.Key point(int value, ByteKey auth, long sequence) {
    return new RangeResultCache.Key(7, PROFILE, PROFILE, auth, sequence, 10, 0, KeyRange.Kind.POINT,
        Optional.of(bytes(value, 1)), Optional.empty(), 0, 0, ASC, NO_FILTER, false, false, false);
  }

  private static RangeResultCache.Key variant(RangeResultCache.Key base, long group, ByteKey profile,
      long compact, long limit, RangeCommand.Sort sort, RangeCommand.RevisionFilter filter) {
    return new RangeResultCache.Key(group, profile, base.storageProfile(), base.authorization(), base.sequence(),
        base.targetRevision(), compact, base.kind(), base.lower(), base.upper(),
        base.requestedRevision(), limit, sort, filter, false, false, false);
  }

  private static StoredRecord record(int value) {
    return StoredRecord.live(bytes(value, 1),
        new StorageMetadata(1, 1, 1, 0, StorageMetadata.State.LIVE, 1), new byte[] {(byte) value});
  }

  private static StoredRecord liveAtRevision(int value, long revision) {
    return StoredRecord.live(bytes(value, 1),
        new StorageMetadata(1, revision, revision, 0, StorageMetadata.State.LIVE, 1),
        new byte[] {(byte) value});
  }

  private static ByteKey bytes(int value, int length) {
    byte[] bytes = new byte[length];
    java.util.Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }

  private static BudgetManager budget() {
    var limits = new BudgetManager.Limits(4, 32, 1, 1, 1, 1_000_000, 16_000_000);
    return new BudgetManager(limits, Map.of("public", limits), limits);
  }

  private static BudgetManager.Reservation reservation() {
    return new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
  }
}
