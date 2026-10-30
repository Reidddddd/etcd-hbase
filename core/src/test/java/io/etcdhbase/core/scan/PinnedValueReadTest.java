package io.etcdhbase.core.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class PinnedValueReadTest {
  @Test
  void valuesAreFetchedOnlyForSelectedResultsAndPinnedToTheHeadVersion() throws Exception {
    var store = new FrozenRecordStore(List.of(List.of(ExistsExecutorTest.live(1, 8192)), List.of(ExistsExecutorTest.live(2, 8192))));
    var budget = SortedMergeCursorTest.budget();
    try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, SortedMergeCursorTest.memory(2, 32768));
        var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1));
        var cursor = SortedMergeCursor.values(store, scheduler.bind(store, account), SortedMergeCursorTest.template(), 2, account, scope);
        var page = cursor.read(1).join()) {
      assertEquals(1, page.count());
      assertTrue(page.more());
      assertTrue(page.records().getFirst().hasValue());
      assertEquals(1, store.gets.size());
      assertEquals(7, store.gets.getFirst().upper());
      assertEquals(ReadProjection.FULL, store.gets.getFirst().projection());
      assertTrue(store.scans.requests.stream().allMatch(request -> request.projection() == ReadProjection.METADATA));
      assertEquals(1, page.pointReads());
    }
  }

  @Test
  void aMissingPinnedVersionFailsInsteadOfReadingTheLatestOrReturningPartialResults() throws Exception {
    var store = new FrozenRecordStore(List.of(List.of(ExistsExecutorTest.live(1, 8192))));
    store.mismatch = true;
    var budget = SortedMergeCursorTest.budget();
    try (var scope = RequestScope.withoutDeadline(); var account = budget.admit("public", scope, SortedMergeCursorTest.memory(1, 32768));
        var scheduler = new ScanScheduler(budget, Duration.ofSeconds(1));
        var cursor = SortedMergeCursor.values(store, scheduler.bind(store, account), SortedMergeCursorTest.template(), 1, account, scope)) {
      assertThrows(CompletionException.class, () -> cursor.read(1).join());
      assertEquals(1, store.gets.size());
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }
}
