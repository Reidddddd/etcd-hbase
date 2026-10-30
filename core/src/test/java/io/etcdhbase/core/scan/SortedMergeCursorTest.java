package io.etcdhbase.core.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class SortedMergeCursorTest {
  @Test
  void globalLimitMoreAndCountMatchTheUnsignedLinearOracleAcrossRandomBuckets() throws Exception {
    var random = new Random(97531);
    List<StoredRecord> all = new ArrayList<>();
    List<List<StoredRecord>> buckets = new ArrayList<>();
    for (int bucket = 0; bucket < 16; bucket++) buckets.add(new ArrayList<>());
    for (int index = 0; index < 250; index++) {
      var record = record(new byte[] {(byte) index, 0, (byte) 0xff}, index % 5 == 0);
      all.add(record); buckets.get(random.nextInt(16)).add(record);
    }
    var expected = all.stream().filter(record -> record.metadata().state() == StorageMetadata.State.LIVE)
        .map(StoredRecord::key).sorted().toList();
    for (int limit : List.of(0, 1, 2, 199, 200, 201)) {
      var source = new FrozenScanSource(buckets);
      var budget = budget();
      try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, memory(16, 1_000_000));
          var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1));
          var cursor = new SortedMergeCursor(scheduler.bind(source, account), template(), 16, account, scope);
          var page = cursor.read(limit).join()) {
        var selected = limit == 0 ? expected : expected.stream().limit(limit).toList();
        assertEquals(selected, page.records().stream().map(StoredRecord::key).toList());
        assertEquals(selected.size(), page.count());
        assertEquals(limit > 0 && expected.size() > limit, page.more());
        assertTrue(source.requests.stream().allMatch(request -> request.maximumRows() == 1));
        assertEquals(0, budget.snapshot().scanners());
      }
      assertEquals(0, budget.snapshot().requests());
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void emptyBucketsAndTombstonesCannotProduceFalseMoreOrAnAccurateCountScan() throws Exception {
    var source = new FrozenScanSource(List.of(List.of(), List.of(record(new byte[] {0}, true)),
        List.of(record(new byte[] {(byte) 0xff}, false)), List.of()));
    var budget = budget();
    try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, memory(4, 4096));
        var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1));
        var cursor = new SortedMergeCursor(scheduler.bind(source, account), template(), 4, account, scope);
        var page = cursor.read(1).join()) {
      assertEquals(1, page.count());
      assertFalse(page.more());
      assertEquals(4, source.requests.size());
    }
  }

  @Test
  void duplicatesAndResponseExhaustionFailWithoutPartialSuccess() throws Exception {
    var one = record(new byte[] {1}, false);
    for (var buckets : List.of(List.of(List.of(one), List.of(one)), List.of(List.of(one), List.<StoredRecord>of()))) {
      var source = new FrozenScanSource(buckets);
      var budget = budget();
      long output = buckets.get(1).isEmpty() ? 1 : 4096;
      try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, memory(2, output));
          var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1));
          var cursor = new SortedMergeCursor(scheduler.bind(source, account), template(), 2, account, scope)) {
        assertThrows(CompletionException.class, () -> cursor.read(0).join());
        assertEquals(0, budget.snapshot().allocatedBytes());
      }
    }
  }

  @Test
  void aSlowOrFailedBucketCannotBeSkippedWhenChoosingTheMinimum() throws Exception {
    var source = new DelayedSource();
    var budget = budget();
    try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, memory(2, 4096));
        var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1));
        var cursor = new SortedMergeCursor(scheduler.bind(source, account), template(), 2, account, scope)) {
      var pending = cursor.read(1);
      source.pending.getFirst().complete(new StoragePage(List.of(record(new byte[] {9}, false)), true, new StoragePage.Cost(1, 256, 1)));
      assertEquals(2, source.pending.size());
      assertFalse(pending.isDone(), "bucket two may still contain a smaller key");
      source.pending.getLast().completeExceptionally(new IllegalStateException("synthetic backend failure"));
      assertThrows(CompletionException.class, pending::join);
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void cancellationReleasesHeadsButWaitsForThePhysicalReadToReleaseIoCapacity() throws Exception {
    var source = new DelayedSource();
    var budget = budget();
    try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, memory(2, 4096));
        var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1));
        var cursor = new SortedMergeCursor(scheduler.bind(source, account), template(), 2, account, scope)) {
      var pending = cursor.read(1);
      cursor.close();
      assertThrows(CompletionException.class, pending::join);
      assertEquals(1, budget.snapshot().scanners());
      source.pending.getFirst().complete(new StoragePage(List.of(), true, new StoragePage.Cost(0, 0, 1)));
      scheduler.close();
      scheduler.termination().toCompletableFuture().join();
      assertEquals(0, budget.snapshot().allocatedBytes());
      assertFalse(scope.isClosed());
    }
  }

  @Test
  void longRunsOfTombstonesUseBoundedContinuationRatherThanRecursiveCallbacks() throws Exception {
    List<StoredRecord> records = new ArrayList<>();
    for (int index = 0; index < 5000; index++) records.add(record(new byte[] {(byte) (index >>> 8), (byte) index}, true));
    var budget = budget();
    try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, memory(1, 4096));
        var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1));
        var cursor = new SortedMergeCursor(scheduler.bind(new FrozenScanSource(List.of(records)), account), template(), 1, account, scope);
        var page = cursor.read(1).join()) {
      assertEquals(0, page.count());
      assertFalse(page.more());
    }
  }

  static BudgetManager budget() {
    var limits = new BudgetManager.Limits(4, 32, 1, 1, 1, 1_000_000, 16_000_000);
    return new BudgetManager(limits, Map.of("public", limits), limits);
  }
  static BudgetManager.Reservation memory(int buckets, long output) {
    return new BudgetManager.Reservation(buckets * 1024L, 1024, 4096, output);
  }
  static ScanRequest template() {
    return new ScanRequest(0, KeyRange.all(), Optional.empty(), Long.MAX_VALUE, ReadProjection.METADATA, 1, 1024, 32);
  }
  static StoredRecord record(byte[] key, boolean deleted) {
    var metadata = new StorageMetadata(1, 1, 1, 0, deleted ? StorageMetadata.State.TOMBSTONE : StorageMetadata.State.LIVE, 0);
    return StoredRecord.metadataOnly(ByteKey.copyOf(key), metadata);
  }

  private static final class DelayedSource implements ScanSource {
    final List<CompletableFuture<StoragePage>> pending = new ArrayList<>();
    @Override public ScanOperation startScan(ScanRequest request) {
      var result = new CompletableFuture<StoragePage>();
      pending.add(result);
      return new ScanOperation() {
        @Override public CompletionStage<StoragePage> completion() { return result.minimalCompletionStage(); }
        @Override public void close() {}
      };
    }
  }
}
