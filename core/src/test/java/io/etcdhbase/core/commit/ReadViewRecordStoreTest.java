package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.CacheAdmissionTestSupport;
import io.etcdhbase.core.kv.CompareCondition;
import io.etcdhbase.core.kv.CompareReadExecutor;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.kv.RangeCommand;
import io.etcdhbase.core.kv.ResultNode;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.kv.TxnEvaluator;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BucketCursor;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.DirectRecordReader;
import io.etcdhbase.core.scan.ExistsExecutor;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.core.scan.SortedMergeCursor;
import io.etcdhbase.core.storage.BatchWriteResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanDirection;
import io.etcdhbase.core.storage.PhysicalScanProgress;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ReadViewRecordStoreTest {
  @Test
  void auxiliaryReadRejectsLegacyControlBeforeRegisteringAPin() {
    Fixture fixture = currentReadFixture();
    try (RequestScope scope = RequestScope.withoutDeadline()) {
      assertThrows(CompletionException.class,
          () -> fixture.views.openAuxiliary(digest(9), scope).join());
      assertEquals(0, fixture.pinRegistrations.get());
    }
  }

  @Test
  void currentReadOverlaysOnlyTheSelectedPendingTransactionBeforeRangeMerge() throws Exception {
    Fixture fixture = currentReadFixture();
    fixture.physical.pointUsesScanner = true;
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
        BudgetManager.Account account = fixture.account(scope, 1L << 20)) {
      PinnedReadView pinned = fixture.views.open(OptionalLong.empty(), digest(9), scope).join();
      ReadViewRecordStore view = fixture.readStore(pinned, scope);
      assertTrue(view.pointReadUsesScanner());
      assertSelectedPointReads(view, account);
      assertCurrentScanPage(view, pinned, account);
      assertRangeMergeAndExists(fixture, view, scope);
    }
    assertEquals(4, fixture.pinCloses.get());
  }

  private static Fixture currentReadFixture() {
    Fixture fixture = new Fixture(1, List.of(
        candidate("beta", 1, 2, 2, StorageMetadata.State.TOMBSTONE, new byte[0]),
        candidate("doomed", 1, 2, 2, StorageMetadata.State.TOMBSTONE, new byte[0]),
        candidate("gamma", 2, 2, 1, StorageMetadata.State.LIVE, bytes("new-gamma"))));
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "old-alpha"));
    fixture.physical.add(live("beta", 1, 1, 1, 0, "old-beta"));
    fixture.physical.add(live("doomed", 1, 1, 1, 0, "old-doomed"));
    fixture.unselectedCandidate(candidate("leak", 2, 2, 1,
        StorageMetadata.State.LIVE, bytes("private-only")));
    fixture.unselectedCandidate(candidate("gamma", 2, 2, 1,
        StorageMetadata.State.LIVE, bytes("failed-attempt-value")));
    return fixture;
  }

  private static void assertSelectedPointReads(ReadViewRecordStore view, BudgetManager.Account account) {
    StoredRecord selected = view.get(key("gamma"), 2, ReadProjection.FULL, account).join().orElseThrow();
    assertEquals(2, selected.metadata().modificationRevision());
    assertEquals(1, selected.metadata().version());
    assertEquals(2, selected.metadata().createRevision());
    assertEquals("new-gamma", new String(selected.value().orElseThrow()));
    assertTrue(view.get(key("leak"), 2, ReadProjection.FULL, account).join().isEmpty());
  }

  private static void assertCurrentScanPage(ReadViewRecordStore view, PinnedReadView pinned,
      BudgetManager.Account account) {
    StoragePage page = view.startScan(new ScanRequest(0, KeyRange.all(), Optional.empty(),
        2, ReadProjection.FULL, 16, 8192, 64), account).completion().toCompletableFuture().join();
    assertEquals(List.of("alpha", "beta", "doomed", "gamma"),
        page.records().stream().map(record -> text(record.key())).toList());
    assertEquals(StorageMetadata.State.TOMBSTONE, page.records().get(1).metadata().state());
    assertEquals(StorageMetadata.State.TOMBSTONE, page.records().get(2).metadata().state());
    assertEquals("new-gamma", new String(page.records().getLast().value().orElseThrow()));
    assertTrue(page.exhausted());
    assertEquals(1, page.cost().observations().get(StoragePage.Observation.MANIFEST_LEAF_PAGES));
    assertEquals(3, page.cost().observations().get(StoragePage.Observation.MANIFEST_CANDIDATE_RECORDS));
    assertEquals(2, pinned.view().targetRevision());
    assertEquals(1, pinned.view().stableRevision());
  }

  @Test
  void scanCompletionReleasesReadOwnershipBeforeAConsumerFinishesTheView() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "old"));
    fixture.pinRenewal = new CompletableFuture<>();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      ReadViewRecordStore view = fixture.readStore(
          fixture.views.open(OptionalLong.empty(), digest(9), scope).join(), scope);
      ScanOperation scan = view.startScan(new ScanRequest(0, KeyRange.point(key("alpha")),
          Optional.empty(), 2, ReadProjection.METADATA, 1, 4096, 16));
      var abandoned = scan.completion().toCompletableFuture();
      assertTrue(abandoned.cancel(false), "a consumer may abandon its own completion future");
      var finished = scan.completion().thenCompose(page -> {
        assertEquals(1, page.records().size());
        scan.close();
        return view.finishRead();
      }).toCompletableFuture();

      assertFalse(finished.isDone());
      fixture.pinRenewal.complete(null);
      finished.get(5, TimeUnit.SECONDS);
      assertEquals(1, fixture.pinCompletions.get());
      assertEquals(1, fixture.pinCloses.get());
    }
  }

  private static void assertRangeMergeAndExists(Fixture fixture, ReadViewRecordStore view,
      RequestScope scope) throws BackendFailure {
    var limits = new BudgetManager.Limits(4, 32, 1, 1, 1, 1_000_000, 16_000_000);
    var budgets = new BudgetManager(limits, Map.of("public", limits), limits);
    var reservation = new BudgetManager.Reservation(8192, 4096, 8192, 8192, 8192);
    var request = new ScanRequest(0, KeyRange.all(), Optional.empty(), 2,
        ReadProjection.FULL, 1, 1024, 64);
    try (var account = budgets.admit("public", scope, reservation);
        var scheduler = new ScanScheduler(budgets, Duration.ofSeconds(1));
        var cursor = SortedMergeCursor.values(view, scheduler.bind(view, account), request,
            1, account, scope);
        var response = cursor.read(0).join()) {
      assertEquals(List.of("alpha", "gamma"), response.records().stream()
          .map(record -> text(record.key())).toList());
      assertEquals("new-gamma", new String(response.records().getLast().value().orElseThrow()));
      assertEquals(2, response.pointReads(), "selected heads fetch values through the same pinned view");
      PinnedReadView gammaPin = fixture.views.open(OptionalLong.empty(), digest(9), scope).join();
      try (ReadViewRecordStore gammaView = fixture.readStore(gammaPin, scope)) {
        assertEquals(1, ExistsExecutor.point(gammaView, key("gamma"), 2, account).join());
      }
      PinnedReadView doomedPin = fixture.views.open(OptionalLong.empty(), digest(9), scope).join();
      try (ReadViewRecordStore doomedView = fixture.readStore(doomedPin, scope)) {
        assertEquals(0, ExistsExecutor.point(doomedView, key("doomed"), 2, account).join());
      }
      PinnedReadView rangeExistsPin = fixture.views.open(OptionalLong.empty(), digest(9), scope).join();
      try (ReadViewRecordStore rangeExistsView = fixture.readStore(rangeExistsPin, scope);
          var exists = new ExistsExecutor(scheduler.bind(rangeExistsView, account),
          new ScanRequest(0, KeyRange.point(key("gamma")), Optional.empty(), 2,
              ReadProjection.METADATA, 1, 1024, 64), 1, scope, rangeExistsView)) {
        assertEquals(1, exists.execute().join().count());
      }
    }
    assertEquals(4, fixture.pinCloses.get());
  }

  @Test
  void historicalReadUsesItsOwnBarrierAndCanReadDeleteThenRecreateVersions() throws Exception {
    Fixture fixture = new Fixture(2, List.of(
        candidate("beta", 3, 3, 1, StorageMetadata.State.LIVE, bytes("recreated"))));
    fixture.physical.add(live("beta", 1, 1, 1, 0, "created"));
    fixture.physical.add(tombstone("beta", 2, 2));

    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      PinnedReadView current = fixture.views.open(OptionalLong.empty(), digest(9), scope).join();
      ReadViewRecordStore currentStore = fixture.readStore(current, scope);
      try (BudgetManager.Account account = fixture.account(scope, 4096)) {
        StoredRecord recreated = currentStore.get(key("beta"), 3, ReadProjection.FULL, account)
            .join().orElseThrow();
        assertEquals(3, recreated.metadata().createRevision());
        assertEquals(1, recreated.metadata().version());
        assertEquals("recreated", new String(recreated.value().orElseThrow()));

        PinnedReadView historical = fixture.views.open(OptionalLong.of(2), digest(9), scope).join();
        ReadViewRecordStore historicalStore = fixture.readStore(historical, scope);
        StoredRecord deleted = historicalStore.get(key("beta"), 2, ReadProjection.METADATA)
            .join().orElseThrow();
        assertEquals(StorageMetadata.State.TOMBSTONE, deleted.metadata().state());
        assertFalse(deleted.hasValue());
        assertTrue(historical.view().selectedRoot().isEmpty());
        assertEquals(2, historical.view().stableRevision());
      }
    }
    assertEquals(2, fixture.pinCloses.get());
  }

  @Test
  void transactionUsesOneBarrierAndRetainsAllPinsUntilItsFinalValidation() throws Exception {
    Fixture fixture = new Fixture(2, List.of(
        candidate("beta", 3, 3, 1, StorageMetadata.State.LIVE, bytes("recreated"))));
    fixture.physical.add(live("beta", 1, 1, 1, 0, "created"));
    fixture.physical.add(tombstone("beta", 2, 2));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
         BudgetManager.Account account = fixture.account(scope, 1L << 20)) {
      var session = TxnReadSession.open(fixture.views, List.of(1L, 2L, 2L), digest(9),
          fixture.physical, new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join();
      assertEquals(3, session.currentRevision());
      assertEquals(2, session.barrierSequence());
      assertEquals(4, fixture.control.reads.get(), "one barrier and one recheck per distinct pin");
      assertEquals(3, fixture.pinRegistrations.get());
      var first = session.reader(1).get(key("beta"), 1, ReadProjection.FULL).join().orElseThrow();
      assertEquals("created", new String(first.value().orElseThrow()));
      session.reader(1).finishRead().join();
      assertEquals(0, fixture.pinCloses.get(), "a child result must not complete the transaction pin");

      var old = fixture.control.state;
      var next = new PublicationSlot(TransactionId.create(), InvocationId.create(), 1, 2, 3,
          3, 4, true, ByteKey.copyOf(new byte[] {99}), digest(7));
      fixture.control.state = old.finalizePublication(fixture.transactionId).publish(next);
      var deleted = session.reader(2).get(key("beta"), 2, ReadProjection.METADATA).join().orElseThrow();
      assertEquals(StorageMetadata.State.TOMBSTONE, deleted.metadata().state());
      var current = ((io.etcdhbase.core.scan.BudgetedRecordStore) session.reader(0))
          .get(key("beta"), 3, ReadProjection.FULL, account).join().orElseThrow();
      assertEquals("recreated", new String(current.value().orElseThrow()));
      assertEquals(3, session.currentRevision());

      session.finish().join();
      assertEquals(3, fixture.pinCompletions.get());
      assertEquals(3, fixture.pinCloses.get());
      assertFalse(scope.isClosed(), "successful read validation must leave the write scope open");
      assertThrows(IllegalStateException.class, () -> session.reader(0));
      session.close();
    }
  }

  @Test
  void transactionComparesPointAndRangeAgainstOneProtectedBase() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "x"));
    fixture.physical.add(live("beta", 1, 1, 1, 0, "x"));
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 32768, 8192);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var conditions = List.of(
          CompareCondition.numeric(KeyRange.point(key("delta")), CompareCondition.Target.VERSION,
              CompareCondition.Relation.EQUAL, 2),
          CompareCondition.value(KeyRange.between(key("a"), key("z")),
              CompareCondition.Relation.EQUAL, bytes("x")),
          CompareCondition.numeric(KeyRange.point(key("missing")), CompareCondition.Target.VERSION,
              CompareCondition.Relation.EQUAL, 0));
      assertTrue(comparisons.evaluate(conditions).join());
      assertEquals(0, fixture.pinCompletions.get(), "comparison cannot release the transaction pin");
      session.finish().join();
      assertEquals(1, fixture.pinCompletions.get());
    }
  }

  @Test
  void transactionComparePropagatesBackendFailureInsteadOfChoosingElse() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 32768, 8192);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      fixture.physical.blockNextGet(key("broken"));
      var result = comparisons.evaluate(List.of(CompareCondition.numeric(KeyRange.point(key("broken")),
          CompareCondition.Target.VERSION, CompareCondition.Relation.EQUAL, 0)));
      fixture.physical.pendingGet.completeExceptionally(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      var failure = assertInstanceOf(BackendFailure.class, completionFailure(result::join));
      assertEquals(BackendFailure.Category.UNAVAILABLE, failure.category());
      session.close();
    }
  }

  @Test
  void transactionCompareAndShortCircuitsBeforeReadingLaterConditions() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 32768, 8192);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var conditions = List.of(
          CompareCondition.value(KeyRange.point(key("missing")), CompareCondition.Relation.NOT_EQUAL, bytes("x")),
          CompareCondition.numeric(KeyRange.point(key("later")), CompareCondition.Target.VERSION,
              CompareCondition.Relation.EQUAL, 0));
      assertFalse(comparisons.evaluate(conditions).join());
      assertEquals(1, fixture.physical.getCalls.get());
      session.finish().join();
    }
  }

  @Test
  void transactionCompareFailsWhenItsScanWorkBudgetIsExhausted() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "x"));
    fixture.physical.add(live("beta", 1, 1, 1, 0, "x"));
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 32768, 8192);
    var reads = new BudgetManager.ReadLimits(100, 100_000, 100, 1, 100);
    try (var resources = transactionResources(budget, reservation, reads)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var result = comparisons.evaluate(List.of(CompareCondition.numeric(
          KeyRange.between(key("a"), key("z")), CompareCondition.Target.VERSION,
          CompareCondition.Relation.GREATER, 0)));
      var failure = assertInstanceOf(BackendFailure.class, completionFailure(result::join));
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
      session.close();
    }
  }

  @Test
  void transactionCompareRejectsAnAccountFromAnotherRequestScope() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 32768, 8192);
    try (RequestScope owner = RequestScope.withTimeout(Duration.ofSeconds(10));
         RequestScope other = RequestScope.withTimeout(Duration.ofSeconds(10));
         BudgetManager.Account account = budget.admit("public", other, reservation);
         var scheduler = new ScanScheduler(budget, Duration.ofSeconds(5))) {
      var session = TxnReadSession.open(fixture.views, List.of(), digest(9), fixture.physical,
          new ManifestCatalog(fixture.repository), ignored -> 0, 1, owner).join();
      assertThrows(IllegalArgumentException.class, () -> new CompareReadExecutor(
          session, scheduler, account, other, new CompareReadExecutor.Limits(1, 4096, 64, 4096)));
      session.close();
    }
  }

  @Test
  void flatTransactionReadsItsPriorWriteAndUsesOneCommitRevision() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "old"));
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      var put = new PutCommand(key("alpha"), bytes("new"), 0, true, false, false);
      var command = new TxnCommand(List.of(CompareCondition.numeric(KeyRange.point(key("alpha")),
          CompareCondition.Target.VERSION, CompareCondition.Relation.EQUAL, 1)),
          List.of(pointRange(key("alpha")), new TxnCommand.Put(put),
              pointRange(key("alpha"))), List.of());
      InvocationBinding invocation = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      var evaluated = evaluator.evaluate(command, Optional.of(invocation)).join();
      assertTrue(evaluated.belongsTo(invocation, scope));
      assertThrows(CompletionException.class, () -> evaluated.finishReadOnly().join());
      InvocationBinding different = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      assertFalse(evaluated.belongsTo(different, scope));
      var result = assertInstanceOf(ResultNode.Txn.class, evaluated.result().root());
      assertTrue(result.succeeded());
      assertEquals(3, result.header().revision());
      var before = assertInstanceOf(ResultNode.Range.class, result.responses().get(0));
      var after = assertInstanceOf(ResultNode.Range.class, result.responses().get(2));
      assertEquals("old", new String(before.records().getFirst().value().orElseThrow()));
      assertEquals("new", new String(after.records().getFirst().value().orElseThrow()));
      assertEquals(List.of(2L, 3L, 3L), result.responses().stream()
          .map(node -> node.header().revision()).toList());
      assertEquals(1, evaluated.candidateCount());
      assertEquals(3, evaluated.candidates().next().join().orElseThrow().record().metadata().modificationRevision());
      assertEquals(1, evaluated.eventCount());
      assertEquals("old", new String(fixture.physical.versions.get(key("alpha")).getFirst()
          .value().orElseThrow()), "planning cannot modify the backend");
      session.finish().join();
    }
  }

  @Test
  void overlappingDeletesUseOnePrivateViewAndOneRevision() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("a", 1, 1, 1, 0, "one"));
    fixture.physical.add(live("b", 1, 1, 1, 0, "two"));
    fixture.physical.add(live("c", 1, 1, 1, 0, "three"));
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope, 1);
      var comparisons = transactionComparisons(session, scheduler, account, scope, 1);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      var command = new TxnCommand(List.of(), List.of(
          pointRange(key("a")),
          new TxnCommand.Delete(KeyRange.between(key("a"), key("c")), true),
          pointRange(key("a")),
          new TxnCommand.Delete(KeyRange.between(key("a"), key("z")), true),
          new TxnCommand.Delete(KeyRange.between(key("a"), key("z")), true)), List.of());
      var binding = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      var evaluated = evaluator.evaluate(command, Optional.of(binding)).join();
      var result = assertInstanceOf(ResultNode.Txn.class, evaluated.result().root());
      assertEquals(3, result.header().revision());
      assertEquals(List.of(2L, 3L, 3L, 3L, 3L), result.responses().stream()
          .map(node -> node.header().revision()).toList());
      assertEquals(1, assertInstanceOf(ResultNode.Range.class, result.responses().get(0)).records().size());
      var first = assertInstanceOf(ResultNode.Delete.class, result.responses().get(1));
      assertEquals(2, first.deleted());
      assertEquals(List.of(key("a"), key("b")), first.previous().stream().map(StoredRecord::key).toList());
      assertTrue(assertInstanceOf(ResultNode.Range.class, result.responses().get(2)).records().isEmpty());
      var second = assertInstanceOf(ResultNode.Delete.class, result.responses().get(3));
      assertEquals(1, second.deleted());
      assertEquals(key("c"), second.previous().getFirst().key());
      assertEquals(0, assertInstanceOf(ResultNode.Delete.class, result.responses().get(4)).deleted());
      assertEquals(3, evaluated.candidateCount());
      assertEquals(3, evaluated.eventCount());
      assertEquals(3, fixture.physical.versions.size(), "planning must not physically delete public records");
      session.finish().join();
    }
  }

  @Test
  void zeroEndDeleteAndPutOfTheSameKeyUseTheirActualOperationOrder() throws Exception {
    for (boolean deleteFirst : List.of(true, false)) {
      Fixture fixture = new Fixture(1, List.of(
          candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
      fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
      fixture.physical.add(live("z", 1, 1, 1, 0, "old"));
      var budget = transactionBudget();
      var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
      try (var resources = transactionResources(budget, reservation)) {
        var scope = resources.scope();
        var account = resources.account();
        var scheduler = resources.scheduler();
        var session = openTransaction(fixture, scope, 1);
        var comparisons = transactionComparisons(session, scheduler, account, scope, 1);
        var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
        var delete = new TxnCommand.Delete(KeyRange.interval(Optional.of(key("a")), Optional.empty()), true);
        var put = new TxnCommand.Put(new PutCommand(key("z"), bytes("new"), 0, true, false, false));
        var command = new TxnCommand(List.of(), deleteFirst
            ? List.of(delete, put, pointRange(key("z")))
            : List.of(put, delete, pointRange(key("z"))), List.of());
        var binding = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
            TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
        var evaluated = evaluator.evaluate(command, Optional.of(binding)).join();
        var result = assertInstanceOf(ResultNode.Txn.class, evaluated.result().root());
        assertEquals(3, result.header().revision());
        var deleted = assertInstanceOf(ResultNode.Delete.class, result.responses().get(deleteFirst ? 0 : 1));
        assertEquals(1, deleted.deleted());
        assertEquals(deleteFirst ? "old" : "new", new String(deleted.previous().getFirst().value().orElseThrow()));
        var written = assertInstanceOf(ResultNode.Put.class, result.responses().get(deleteFirst ? 1 : 0));
        assertEquals(!deleteFirst, written.previous().isPresent());
        var after = assertInstanceOf(ResultNode.Range.class, result.responses().get(2));
        assertEquals(deleteFirst ? 1 : 0, after.records().size());
        if (deleteFirst) {
          assertEquals(3, after.records().getFirst().metadata().createRevision());
          assertEquals(1, after.records().getFirst().metadata().version());
        }
        assertEquals(1, evaluated.candidateCount());
        assertEquals(2, evaluated.eventCount());
        session.finish().join();
      }
    }
  }

  @Test
  void pointDeleteVisitsOnlyItsSelectedBucket() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("a", 1, 1, 1, 0, "old"));
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(65536, 8192, 8192, 65536, 131072);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope, 16);
      var comparisons = transactionComparisons(session, scheduler, account, scope, 16);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      var command = new TxnCommand(List.of(), List.of(new TxnCommand.Delete(KeyRange.point(key("a")), true)), List.of());
      var binding = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      var evaluated = evaluator.evaluate(command, Optional.of(binding)).join();
      assertEquals(1, assertInstanceOf(ResultNode.Delete.class,
          assertInstanceOf(ResultNode.Txn.class, evaluated.result().root()).responses().getFirst()).deleted());
      assertEquals(List.of(0), fixture.physical.scanBuckets);
      session.finish().join();
    }
  }

  @Test
  void deleteThenIgnoreValueRejectsBeforeAnyPublicMutation() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("z", 1, 1, 1, 0, "old"));
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope, 1);
      var comparisons = transactionComparisons(session, scheduler, account, scope, 1);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      var delete = new TxnCommand.Delete(KeyRange.interval(Optional.of(key("a")), Optional.empty()), false);
      var put = new TxnCommand.Put(new PutCommand(key("z"), new byte[0], 0, false, true, false));
      var command = new TxnCommand(List.of(), List.of(delete, put), List.of());
      var binding = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      var failure = assertThrows(CompletionException.class, () -> evaluator.evaluate(command, Optional.of(binding)).join());
      assertEquals(KvFailure.Reason.KEY_NOT_FOUND, assertInstanceOf(KvFailure.class, failure.getCause()).reason());
      assertEquals("old", new String(fixture.physical.versions.get(key("z")).getFirst().value().orElseThrow()));
      assertEquals(2, fixture.control.state.publishedRevision());
    }
  }

  @Test
  void nestedCompareUsesBaseWhileNestedReadSeesEarlierPut() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      var key = key("new-key");
      var child = new TxnCommand(List.of(CompareCondition.numeric(KeyRange.point(key),
          CompareCondition.Target.VERSION, CompareCondition.Relation.EQUAL, 0)),
          List.of(pointRange(key)), List.of());
      var command = new TxnCommand(List.of(), List.of(
          new TxnCommand.Put(new PutCommand(key, bytes("new"), 0, false, false, false)),
          new TxnCommand.Nested(child)), List.of());
      var binding = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      var evaluated = evaluator.evaluate(command, Optional.of(binding)).join();
      var outer = assertInstanceOf(ResultNode.Txn.class, evaluated.result().root());
      var nested = assertInstanceOf(ResultNode.Txn.class, outer.responses().get(1));
      assertTrue(nested.succeeded(), "nested Compare must use the original base view");
      assertEquals(0, nested.header().revision(), "native nested Txn header is present with zero revision");
      var read = assertInstanceOf(ResultNode.Range.class, nested.responses().getFirst());
      assertEquals("new", new String(read.records().getFirst().value().orElseThrow()));
      assertEquals(3, read.header().revision());
      assertEquals(3, outer.header().revision());
      session.finish().join();
    }
  }

  @Test
  void nestedSelectionDoesNotReadUnselectedBranchesAndPropagatesBackendFailure() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    var brokenCompare = CompareCondition.numeric(KeyRange.point(key("broken")),
        CompareCondition.Target.VERSION, CompareCondition.Relation.EQUAL, 0);
    var child = new TxnCommand(List.of(brokenCompare), List.of(), List.of());
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      fixture.physical.blockNextGet(key("broken"));
      var outer = new TxnCommand(List.of(CompareCondition.numeric(KeyRange.point(key("missing")),
          CompareCondition.Target.VERSION, CompareCondition.Relation.GREATER, 0)),
          List.of(new TxnCommand.Nested(child)), List.of());
      var result = evaluator.evaluate(outer, Optional.empty()).join();
      assertFalse(assertInstanceOf(ResultNode.Txn.class, result.result().root()).succeeded());
      assertFalse(fixture.physical.pendingGet.isDone(), "the unselected nested Compare must not run");
      result.finishReadOnly().join();
    }
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      fixture.physical.blockNextGet(key("broken"));
      var outer = new TxnCommand(List.of(), List.of(
          new TxnCommand.Put(new PutCommand(key("later"), bytes("value"), 0, false, false, false)),
          new TxnCommand.Nested(child)), List.of());
      var binding = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      var pending = evaluator.evaluate(outer, Optional.of(binding));
      assertFalse(pending.isDone());
      fixture.physical.pendingGet.completeExceptionally(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      var failure = assertInstanceOf(BackendFailure.class, completionFailure(pending::join));
      assertEquals(BackendFailure.Category.UNAVAILABLE, failure.category());
      assertFalse(fixture.physical.versions.containsKey(key("later")));
    }
  }

  @Test
  void selectedNestedHistoricalReadUsesSameBarrierWithoutOverlayingPriorPut() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "old"));
    fixture.physical.add(live("alpha", 1, 2, 2, 0, "base"));
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      var key = key("alpha");
      var child = new TxnCommand(List.of(), List.of(pointRange(key),
          pointRange(key, 1), pointRange(key, 2)), List.of());
      var command = new TxnCommand(List.of(), List.of(
          new TxnCommand.Put(new PutCommand(key, bytes("new"), 0, false, false, false)),
          new TxnCommand.Nested(child)), List.of());
      var binding = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      var evaluated = evaluator.evaluate(command, Optional.of(binding)).join();
      var nested = assertInstanceOf(ResultNode.Txn.class,
          assertInstanceOf(ResultNode.Txn.class, evaluated.result().root()).responses().get(1));
      var current = assertInstanceOf(ResultNode.Range.class, nested.responses().get(0));
      var historical = assertInstanceOf(ResultNode.Range.class, nested.responses().get(1));
      var explicitBase = assertInstanceOf(ResultNode.Range.class, nested.responses().get(2));
      assertEquals("new", new String(current.records().getFirst().value().orElseThrow()));
      assertEquals("old", new String(historical.records().getFirst().value().orElseThrow()));
      assertEquals("base", new String(explicitBase.records().getFirst().value().orElseThrow()));
      assertEquals(3, historical.header().revision(), "header follows the transaction, not the historical target");
      assertEquals(2, fixture.pinRegistrations.get());
      assertEquals(0, fixture.pinCompletions.get());
      evaluated.finishRead().join();
      assertEquals(2, fixture.pinCompletions.get());
    }
  }

  @Test
  void transactionRangeUsesWorkingAndHistoricalViewsWithFiltersAndProjection() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "old"));
    fixture.physical.add(live("alpha", 1, 2, 2, 0, "base"));
    fixture.physical.add(live("beta", 1, 1, 1, 0, "other"));
    var budget = transactionBudget();
    var reservation = transactionRangeReservation();
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      var alpha = key("alpha");
      var all = new RangeCommand.RevisionFilter(0, 0, 0, 0);
      var filterNew = new RangeCommand.RevisionFilter(3, 0, 0, 0);
      var filterNone = new RangeCommand.RevisionFilter(0, -1, 0, 0);
      var command = new TxnCommand(List.of(), List.of(
          new TxnCommand.Put(new PutCommand(alpha, bytes("new"), 0, false, false, false)),
          new TxnCommand.Range(range(KeyRange.all(), 0, 1, filterNew, false, false)),
          new TxnCommand.Range(range(KeyRange.all(), 1, 0, all, false, false)),
          new TxnCommand.Range(range(KeyRange.all(), 0, 1, all, true, false)),
          new TxnCommand.Range(range(KeyRange.point(alpha), 0, 0, filterNone, false, true))), List.of());
      var binding = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      var evaluated = evaluator.evaluate(command, Optional.of(binding)).join();
      var responses = assertInstanceOf(ResultNode.Txn.class, evaluated.result().root()).responses();
      assertWorkingAndHistoricalRangeResponses(responses);
      assertProjectedRangeResponses(responses);
      evaluated.finishRead().join();
    }
    assertEquals(0, budget.snapshot().allocatedBytes());
  }

  private static void assertWorkingAndHistoricalRangeResponses(List<ResultNode> responses) {
    var current = assertInstanceOf(ResultNode.Range.class, responses.get(1));
    assertEquals("new", new String(current.records().getFirst().value().orElseThrow()));
    assertEquals(1, current.count());
    assertFalse(current.more());
    var historical = assertInstanceOf(ResultNode.Range.class, responses.get(2));
    assertEquals(List.of("alpha", "beta"), historical.records().stream().map(record -> text(record.key())).toList());
    assertEquals("old", new String(historical.records().getFirst().value().orElseThrow()));
    assertEquals(3, historical.header().revision());
  }

  private static void assertProjectedRangeResponses(List<ResultNode> responses) {
    var keysOnly = assertInstanceOf(ResultNode.Range.class, responses.get(3));
    assertEquals(1, keysOnly.count());
    assertTrue(keysOnly.more());
    assertFalse(keysOnly.records().getFirst().hasValue());
    assertEquals(3, keysOnly.records().getFirst().metadata().modificationRevision());
    var exists = assertInstanceOf(ResultNode.Range.class, responses.get(4));
    assertEquals(1, exists.count());
    assertTrue(exists.records().isEmpty());
    assertFalse(exists.more());
  }

  private static RangeCommand range(KeyRange keyRange, long revision, long limit,
      RangeCommand.RevisionFilter filter, boolean keysOnly, boolean countOnly) {
    return new RangeCommand(keyRange, RangeCommand.Context.TRANSACTION, revision, limit,
        new RangeCommand.Sort(0, 0), filter, false, keysOnly, countOnly);
  }

  private static TxnCommand.Range pointRange(ByteKey key) { return pointRange(key, 0); }

  private static TxnCommand.Range pointRange(ByteKey key, long revision) {
    return new TxnCommand.Range(range(KeyRange.point(key), revision, 0,
        new RangeCommand.RevisionFilter(0, 0, 0, 0), false, false));
  }

  @Test
  void rangeKeysOnlyAndCountOnlyDoNotFetchValues() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "old"));
    fixture.physical.add(live("beta", 1, 1, 1, 0, "other"));
    var budget = transactionBudget();
    var reservation = transactionRangeReservation();
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var evaluator = readRangeEvaluator(fixture, scheduler, account, scope, 16384);
      var all = new RangeCommand.RevisionFilter(0, 0, 0, 0);
      var exclude = new RangeCommand.RevisionFilter(0, -1, 0, 0);
      var command = new TxnCommand(List.of(), List.of(
          new TxnCommand.Range(range(KeyRange.all(), 0, 0, all, true, false)),
          new TxnCommand.Range(range(KeyRange.all(), 0, 1, exclude, true, true))), List.of());
      var read = evaluator.evaluate(command, Optional.empty()).join().finishReadOnly().join();
      var responses = assertInstanceOf(ResultNode.Txn.class, read.result().root()).responses();
      var keys = assertInstanceOf(ResultNode.Range.class, responses.getFirst());
      assertEquals(2, keys.count());
      assertTrue(keys.records().stream().noneMatch(StoredRecord::hasValue));
      var exists = assertInstanceOf(ResultNode.Range.class, responses.getLast());
      assertEquals(1, exists.count());
      assertTrue(exists.records().isEmpty());
      assertEquals(0, fixture.physical.getCalls.get(), "metadata-only requests must not fetch values");
    }
  }

  @Test
  void countOnlyUsesSelectedPendingCandidatesAndTombstones() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("beta", 1, 2, 2, StorageMetadata.State.TOMBSTONE, new byte[0]),
        candidate("gamma", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    fixture.physical.add(live("beta", 1, 1, 1, 0, "old"));
    var budget = transactionBudget();
    var reservation = transactionRangeReservation();
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var evaluator = readRangeEvaluator(fixture, scheduler, account, scope, 16384);
      var ignored = new RangeCommand.RevisionFilter(0, -1, 0, 0);
      var command = new TxnCommand(List.of(), List.of(
          new TxnCommand.Range(range(KeyRange.point(key("gamma")), 0, 0, ignored, false, true)),
          new TxnCommand.Range(range(KeyRange.point(key("beta")), 0, 0, ignored, false, true)),
          new TxnCommand.Range(range(KeyRange.all(), 0, 1, ignored, false, true))), List.of());
      var read = evaluator.evaluate(command, Optional.empty()).join().finishReadOnly().join();
      var responses = assertInstanceOf(ResultNode.Txn.class, read.result().root()).responses();
      assertEquals(List.of(1L, 0L, 1L), responses.stream()
          .map(response -> assertInstanceOf(ResultNode.Range.class, response).count()).toList());
      assertEquals(0, fixture.physical.getCalls.get(), "selected records require no base point fetch");
    }
  }

  @Test
  void rangeCountOnlyStopsAfterAMatchButChecksEveryBucketForAbsence() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "one"));
    fixture.physical.add(live("beta", 1, 1, 1, 0, "two"));
    java.util.function.ToIntFunction<ByteKey> bucket = value -> value.equals(key("alpha")) ? 0 : 1;
    fixture.physical.bucketOf = bucket;
    var limits = new BudgetManager.Limits(4, 32, 2, 2, 2, 1_000_000, 16_000_000);
    var budget = new BudgetManager(limits, Map.of("public", limits), limits);
    var reservation = transactionRangeReservation();
    for (boolean exists : List.of(true, false)) {
      fixture.physical.scanBuckets.clear();
      try (var resources = transactionResources(budget, reservation)) {
        var scope = resources.scope();
        var account = resources.account();
        var scheduler = resources.scheduler();
        var session = TxnReadSession.open(fixture.views, List.of(), digest(9), fixture.physical,
            new ManifestCatalog(fixture.repository), bucket, 2, scope).join();
        var compares = new CompareReadExecutor(session, scheduler, account, scope,
            new CompareReadExecutor.Limits(2, 4096, 64, 4096));
        var evaluator = new TxnEvaluator(session, compares, account, scope,
            new TxnEvaluator.Limits(4096, 16384, 65536), bucket);
        var interval = exists ? KeyRange.between(key("a"), key("z"))
            : KeyRange.between(key("x"), key("z"));
        var command = new TxnCommand(List.of(), List.of(new TxnCommand.Range(range(interval, 0, 1,
            new RangeCommand.RevisionFilter(0, -1, 0, 0), false, true))), List.of());
        var read = evaluator.evaluate(command, Optional.empty()).join().finishReadOnly().join();
        var count = assertInstanceOf(ResultNode.Range.class,
            assertInstanceOf(ResultNode.Txn.class, read.result().root()).responses().getFirst());
        assertEquals(exists ? 1 : 0, count.count());
        assertTrue(count.records().isEmpty());
        assertEquals(exists ? List.of(0) : List.of(0, 1), fixture.physical.scanBuckets);
      }
    }
  }

  @Test
  void pointRangesUseOnePointReadAndRequestOnlyTheNeededProjection() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "value"));
    var budget = transactionBudget();
    var reservation = transactionRangeReservation();
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var evaluator = readRangeEvaluator(fixture, scheduler, account, scope, 16384);
      var all = new RangeCommand.RevisionFilter(0, 0, 0, 0);
      var ignored = new RangeCommand.RevisionFilter(0, -1, 0, 0);
      var point = KeyRange.point(key("alpha"));
      var command = new TxnCommand(List.of(), List.of(
          new TxnCommand.Range(range(point, 0, 0, all, false, false)),
          new TxnCommand.Range(range(point, 0, 0, all, true, false)),
          new TxnCommand.Range(range(point, 0, 0, ignored, false, true))), List.of());
      var read = evaluator.evaluate(command, Optional.empty()).join().finishReadOnly().join();
      var responses = assertInstanceOf(ResultNode.Txn.class, read.result().root()).responses();
      assertEquals("value", new String(assertInstanceOf(ResultNode.Range.class, responses.get(0))
          .records().getFirst().value().orElseThrow()));
      assertFalse(assertInstanceOf(ResultNode.Range.class, responses.get(1)).records().getFirst().hasValue());
      assertEquals(1, assertInstanceOf(ResultNode.Range.class, responses.get(2)).count());
      assertEquals(List.of(ReadProjection.FULL, ReadProjection.METADATA, ReadProjection.METADATA),
          fixture.physical.getProjections);
      assertEquals(0, fixture.physical.scanCalls.get());
    }
  }

  @Test
  void cancelledPointRangeRetainsItsPinUntilThePhysicalReadCompletes() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.blockNextGet(key("alpha"));
    var budget = transactionBudget();
    var reservation = transactionRangeReservation();
    RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
    try (BudgetManager.Account account = budget.admit("public", scope, reservation);
         var scheduler = new ScanScheduler(budget, Duration.ofSeconds(5))) {
      var evaluator = readRangeEvaluator(fixture, scheduler, account, scope, 16384);
      var command = new TxnCommand(List.of(), List.of(new TxnCommand.Range(range(KeyRange.point(key("alpha")),
          0, 0, new RangeCommand.RevisionFilter(0, 0, 0, 0), false, false))), List.of());
      var pending = evaluator.evaluate(command, Optional.empty());
      assertFalse(pending.isDone());
      scope.cancelAsync(task -> Thread.ofVirtual().start(task));
      assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
      assertEquals(0, fixture.pinCloses.get());
      assertTrue(budget.snapshot().allocatedBytes() > 0);
      fixture.physical.pendingGet.complete(Optional.of(live("alpha", 1, 1, 1, 0, "value")));
      assertThrows(CompletionException.class, pending::join);
      scope.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
      scope.physicalProducersCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertEquals(1, fixture.pinCloses.get());
      assertEquals(0, budget.snapshot().allocatedBytes());
    } finally {
      scope.close();
    }
  }

  @Test
  void rangeOutputBudgetFailureDoesNotReturnPartialResults() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(liveBytes("a", 1, 1, 400));
    fixture.physical.add(liveBytes("b", 1, 1, 400));
    var budget = transactionBudget();
    var reservation = transactionRangeReservation();
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var evaluator = readRangeEvaluator(fixture, scheduler, account, scope, 800);
      var command = new TxnCommand(List.of(), List.of(new TxnCommand.Range(range(KeyRange.all(), 0, 0,
          new RangeCommand.RevisionFilter(0, 0, 0, 0), false, false))), List.of());
      var failure = assertInstanceOf(BackendFailure.class,
          completionFailure(() -> evaluator.evaluate(command, Optional.empty()).join()));
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
      scope.close();
      scope.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
      scope.physicalProducersCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void cancelledRangeKeepsItsPinUntilAnInFlightValueReadDrains() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    var value = live("a", 1, 1, 1, 0, "old");
    fixture.physical.add(value);
    fixture.physical.blockNextGet(key("a"));
    var budget = transactionBudget();
    var reservation = transactionRangeReservation();
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var evaluator = readRangeEvaluator(fixture, scheduler, account, scope, 16384);
      var command = new TxnCommand(List.of(), List.of(new TxnCommand.Range(range(KeyRange.all(), 0, 0,
          new RangeCommand.RevisionFilter(0, 0, 0, 0), false, false))), List.of());
      var pending = evaluator.evaluate(command, Optional.empty());
      assertFalse(pending.isDone());
      scope.close();
      assertEquals(0, fixture.pinCloses.get());
      fixture.physical.pendingGet.complete(Optional.of(value));
      assertThrows(CompletionException.class, pending::join);
      scope.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
      scope.physicalProducersCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertEquals(1, fixture.pinCloses.get());
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void rangeRejectsAValueThatDoesNotMatchItsSelectedMetadata() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("a", 1, 1, 1, 0, "old"));
    fixture.physical.blockNextGet(key("a"));
    var budget = transactionBudget();
    var reservation = transactionRangeReservation();
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var evaluator = readRangeEvaluator(fixture, scheduler, account, scope, 16384);
      var command = new TxnCommand(List.of(), List.of(new TxnCommand.Range(range(KeyRange.all(), 0, 0,
          new RangeCommand.RevisionFilter(0, 0, 0, 0), false, false))), List.of());
      var pending = evaluator.evaluate(command, Optional.empty());
      assertFalse(pending.isDone());
      fixture.physical.pendingGet.complete(Optional.of(live("a", 1, 2, 2, 0, "new")));
      assertInstanceOf(IllegalStateException.class, completionFailure(pending::join));
    }
  }

  @Test
  void filteredPointAndUnlimitedRangeCountOnlyReturnedRecords() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "value"));
    var budget = transactionBudget();
    var reservation = transactionRangeReservation();
    var filters = List.of(new RangeCommand.RevisionFilter(2, 0, 0, 0),
        new RangeCommand.RevisionFilter(0, -1, 0, 0), new RangeCommand.RevisionFilter(0, 0, 2, 0),
        new RangeCommand.RevisionFilter(0, 0, 0, -1), new RangeCommand.RevisionFilter(1, 0, 0, 0),
        new RangeCommand.RevisionFilter(0, 1, 0, 0), new RangeCommand.RevisionFilter(0, 0, 1, 0),
        new RangeCommand.RevisionFilter(0, 0, 0, 1));
    for (boolean point : List.of(true, false)) {
      for (long limit : point ? List.of(0L, 1L) : List.of(0L, -1L)) {
        for (int i = 0; i < filters.size(); i++) {
          verifyFilteredRangeCase(fixture, budget, reservation, point, limit, filters.get(i), i < 4 ? 0 : 1);
        }
      }
    }
  }

  private static void verifyFilteredRangeCase(Fixture fixture, BudgetManager budget,
      BudgetManager.Reservation reservation, boolean point, long limit,
      RangeCommand.RevisionFilter filter, int expectedCount) throws Exception {
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var evaluator = readRangeEvaluator(fixture, scheduler, account, scope, 16384);
      var keyRange = point ? KeyRange.point(key("alpha")) : KeyRange.all();
      var command = new TxnCommand(List.of(), List.of(new TxnCommand.Range(range(keyRange, 0, limit,
          filter, false, false))), List.of());
      var read = evaluator.evaluate(command, Optional.empty()).join().finishReadOnly().join();
      var result = assertInstanceOf(ResultNode.Range.class,
          assertInstanceOf(ResultNode.Txn.class, read.result().root()).responses().getFirst());
      assertEquals(expectedCount, result.count());
      assertEquals(result.count(), result.records().size());
    }
  }

  @Test
  void standaloneAndTransactionRangePreserveDifferentRevisionChecks() throws Exception {
    for (int scenario = 0; scenario < 4; scenario++) {
      Fixture fixture = new Fixture(1, List.of(
          candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
      fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
      if (scenario == 3) fixture.control.state = fixture.control.state.advanceCompactRevision(2);
      fixture.physical.add(live("a", 1, 1, 1, 0, "old"));
      var budget = transactionBudget();
      var reservation = transactionRangeReservation();
      try (var resources = transactionResources(budget, reservation)) {
        var scope = resources.scope();
        var account = resources.account();
        var scheduler = resources.scheduler();
        var evaluator = readRangeEvaluator(fixture, scheduler, account, scope, 16384);
        var context = scenario == 0 ? RangeCommand.Context.STANDALONE : RangeCommand.Context.TRANSACTION;
        long revision = scenario < 2 ? -2 : scenario == 2 ? 3 : 1;
        var request = new RangeCommand(KeyRange.point(key("a")), context, revision, 0,
            new RangeCommand.Sort(0, 0), new RangeCommand.RevisionFilter(0, 0, 0, 0),
            false, false, false);
        var command = new TxnCommand(List.of(), List.of(new TxnCommand.Range(request)), List.of());
        if (scenario == 0) {
          var read = evaluator.evaluate(command, Optional.empty()).join().finishReadOnly().join();
          var response = assertInstanceOf(ResultNode.Range.class,
              assertInstanceOf(ResultNode.Txn.class, read.result().root()).responses().getFirst());
          assertEquals("old", new String(response.records().getFirst().value().orElseThrow()));
          assertEquals(2, response.header().revision());
        } else {
          var failure = assertInstanceOf(KvFailure.class,
              completionFailure(() -> evaluator.evaluate(command, Optional.empty()).join()));
          assertEquals(scenario == 2 ? KvFailure.Reason.FUTURE_REVISION : KvFailure.Reason.COMPACTED,
              failure.reason());
        }
      }
    }
  }

  private static BudgetManager.Reservation transactionRangeReservation() {
    return new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
  }

  private static TxnEvaluator readRangeEvaluator(Fixture fixture, ScanScheduler scheduler,
      BudgetManager.Account account, RequestScope scope, long resultBytes) {
    var session = TxnReadSession.open(fixture.views, List.of(), digest(9), fixture.physical,
        new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join();
    var comparisons = new CompareReadExecutor(session, scheduler, account, scope,
        new CompareReadExecutor.Limits(1, 4096, 64, 4096));
    return new TxnEvaluator(session, comparisons, account, scope,
        new TxnEvaluator.Limits(4096, resultBytes, 65536), ignored -> 0);
  }

  @Test
  void cacheAdmissionRequiresACompletedStandaloneViewAndLookupRechecksTheBarrier() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("selected"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "one"));
    fixture.physical.add(live("beta", 1, 1, 1, 0, "two"));
    var limits = new BudgetManager.Limits(8, 32, 2, 2, 2, 1_000_000, 16_000_000);
    var budget = new BudgetManager(limits, Map.of("public", limits), limits);
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    var filter = new RangeCommand.RevisionFilter(0, 0, 0, 0);
    var standalone = new RangeCommand(KeyRange.point(key("alpha")), RangeCommand.Context.STANDALONE,
        0, 0, new RangeCommand.Sort(0, 0), filter, false, false, false);
    var standaloneTxn = new TxnCommand(List.of(), List.of(new TxnCommand.Range(standalone)), List.of());
    try (RequestScope ownerScope = RequestScope.withTimeout(Duration.ofSeconds(10)); var cache = new CacheAdmissionTestSupport(budget, ownerScope)) {
      verifyValidatedAdmission(fixture, budget, reservation, standaloneTxn, cache);
      verifyFreshBarrierAndIdentity(fixture, budget, reservation, standalone, cache);
      verifyPartialAndFailedPinAreNotAdmitted(fixture, budget, reservation, filter, cache);
    }
    assertEquals(0, budget.snapshot().allocatedBytes());
  }

  private static void verifyPartialAndFailedPinAreNotAdmitted(Fixture fixture, BudgetManager budget,
      BudgetManager.Reservation reservation, RangeCommand.RevisionFilter filter,
      CacheAdmissionTestSupport cache) throws Exception {
    var partial = new RangeCommand(KeyRange.all(), RangeCommand.Context.STANDALONE,
        0, 1, new RangeCommand.Sort(0, 0), filter, false, false, false);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10)); var account = budget.admit("public", scope, reservation);
         var scheduler = new ScanScheduler(budget, Duration.ofSeconds(5))) {
      var evaluator = readRangeEvaluator(fixture, scheduler, account, scope, 16384);
      var command = new TxnCommand(List.of(), List.of(new TxnCommand.Range(partial)), List.of());
      var validated = cache.finish(evaluator.evaluate(command, Optional.empty()).join()).join();
      assertFalse(cache.admit(validated, scope, digest(7)), "partial ranges cannot become complete entries");
    }
    var beta = new RangeCommand(KeyRange.point(key("beta")), RangeCommand.Context.STANDALONE,
        0, 0, new RangeCommand.Sort(0, 0), filter, false, false, false);
    fixture.pinCompletion = new CompletableFuture<>();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10)); var account = budget.admit("public", scope, reservation);
         var scheduler = new ScanScheduler(budget, Duration.ofSeconds(5))) {
      var evaluator = readRangeEvaluator(fixture, scheduler, account, scope, 16384);
      var command = new TxnCommand(List.of(), List.of(new TxnCommand.Range(beta)), List.of());
      var pending = cache.finish(evaluator.evaluate(command, Optional.empty()).join());
      assertFalse(pending.isDone());
      fixture.pinCompletion.completeExceptionally(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      assertThrows(CompletionException.class, pending::join);
      scope.cleanupCompletion().toCompletableFuture().join();
    }
    fixture.pinCompletion = CompletableFuture.completedFuture(null);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10)); var account = budget.admit("public", scope, reservation)) {
      var session = TxnReadSession.open(fixture.views, List.of(), digest(9), fixture.physical,
          new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join();
      assertEquals(-1, cache.lookup(session, beta, digest(7), account, scope),
          "a failed pin completion must never create a cache entry");
      session.finish().join();
    }
  }

  private static void verifyFreshBarrierAndIdentity(Fixture fixture, BudgetManager budget,
      BudgetManager.Reservation reservation, RangeCommand standalone,
      CacheAdmissionTestSupport cache) throws Exception {
    int physicalReads = fixture.physical.getCalls.get();
    int barriers = fixture.control.reads.get();
    int pinCompletions = fixture.pinCompletions.get();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10)); var account = budget.admit("public", scope, reservation)) {
      var session = TxnReadSession.open(fixture.views, List.of(), digest(9), fixture.physical,
          new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join();
      assertEquals(1, cache.lookup(session, standalone, digest(7), account, scope));
      assertTrue(fixture.control.reads.get() > barriers, "every cache lookup needs a fresh barrier");
      assertEquals(pinCompletions + 1, fixture.pinCompletions.get());
      assertEquals(physicalReads, fixture.physical.getCalls.get());
    }
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10)); var account = budget.admit("public", scope, reservation)) {
      var session = TxnReadSession.open(fixture.views, List.of(), digest(8), fixture.physical,
          new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join();
      assertEquals(-1, cache.lookup(session, standalone, digest(7), account, scope));
      try (var direct = new DirectRecordReader(session.reader(0), account, 4096)
          .readForPreparation(key("alpha"), session.currentRevision(), ReadProjection.FULL).join()) {
        assertEquals("one", new String(direct.record().orElseThrow().value().orElseThrow()));
      }
      session.finish().join();
    }
  }

  private static void verifyValidatedAdmission(Fixture fixture, BudgetManager budget,
      BudgetManager.Reservation reservation, TxnCommand standaloneTxn,
      CacheAdmissionTestSupport cache) throws Exception {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10)); var account = budget.admit("public", scope, reservation);
         var scheduler = new ScanScheduler(budget, Duration.ofSeconds(5))) {
      var evaluator = readRangeEvaluator(fixture, scheduler, account, scope, 16384);
      var ordinary = evaluator.evaluate(standaloneTxn, Optional.empty()).join().finishReadOnly().join();
      assertFalse(cache.admit(ordinary, scope, digest(7)), "ordinary results have no cache admission proof");
    }
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10)); var account = budget.admit("public", scope, reservation);
         var scheduler = new ScanScheduler(budget, Duration.ofSeconds(5))) {
      var evaluator = readRangeEvaluator(fixture, scheduler, account, scope, 16384);
      var validated = cache.finish(evaluator.evaluate(standaloneTxn, Optional.empty()).join()).join();
      assertTrue(cache.admit(validated, scope, digest(7)));
    }
  }

  @Test
  void validatedCacheEntryCanRepresentSelectedPendingValueAtTheSameBarrier() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("alpha", 1, 2, 2, StorageMetadata.State.LIVE, bytes("pending"))));
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "old"));
    var limits = new BudgetManager.Limits(8, 32, 2, 2, 2, 1_000_000, 16_000_000);
    var budget = new BudgetManager(limits, Map.of("public", limits), limits);
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    var command = new RangeCommand(KeyRange.point(key("alpha")), RangeCommand.Context.STANDALONE,
        0, 0, new RangeCommand.Sort(0, 0), new RangeCommand.RevisionFilter(0, 0, 0, 0),
        false, false, false);
    try (RequestScope owner = RequestScope.withTimeout(Duration.ofSeconds(10));
         var cache = new CacheAdmissionTestSupport(budget, owner)) {
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
           var account = budget.admit("public", scope, reservation);
           var scheduler = new ScanScheduler(budget, Duration.ofSeconds(5))) {
        var evaluator = readRangeEvaluator(fixture, scheduler, account, scope, 16384);
        var request = new TxnCommand(List.of(), List.of(new TxnCommand.Range(command)), List.of());
        var validated = cache.finish(evaluator.evaluate(request, Optional.empty()).join()).join();
        var response = assertInstanceOf(ResultNode.Range.class,
            assertInstanceOf(ResultNode.Txn.class, validated.result().root()).responses().getFirst());
        assertEquals("pending", new String(response.records().getFirst().value().orElseThrow()));
        assertTrue(cache.admit(validated, scope, digest(7)));
      }
      int candidateReads = fixture.repository.candidateReads.get();
      assertTrue(candidateReads > 0);
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
           var account = budget.admit("public", scope, reservation)) {
        var session = TxnReadSession.open(fixture.views, List.of(), digest(9), fixture.physical,
            new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join();
        assertEquals("pending", cache.lookupValue(session, command, digest(7), account, scope).orElseThrow());
        assertEquals(candidateReads, fixture.repository.candidateReads.get());
      }
    }
    assertEquals(0, budget.snapshot().allocatedBytes());
  }

  @Test
  void cacheReuseAcrossBarriersRequiresCoverageAndOtherwiseFallsBackToDirectRead() throws Exception {
    for (ProofScenario scenario : ProofScenario.values()) assertCacheProofScenario(scenario);
  }

  private enum ProofScenario {
    UNRELATED_WRITE(false, false, false, false),
    MATCHING_WRITE(true, false, false, false),
    MISSING_DECISION(true, true, false, false),
    EXHAUSTED_BUDGET(true, false, true, false),
    FAILED_PIN(false, false, false, true);

    final boolean changed;
    final boolean missingDecision;
    final boolean exhaustProofBudget;
    final boolean failPinCompletion;

    ProofScenario(boolean changed, boolean missingDecision, boolean exhaustProofBudget, boolean failPinCompletion) {
      this.changed = changed;
      this.missingDecision = missingDecision;
      this.exhaustProofBudget = exhaustProofBudget;
      this.failPinCompletion = failPinCompletion;
    }
  }

  private static void assertCacheProofScenario(ProofScenario scenario) throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("selected"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "old"));
    var limits = new BudgetManager.Limits(8, 32, 2, 2, 2, 1_000_000, 512L << 20);
    var budget = new BudgetManager(limits, Map.of("public", limits), limits);
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    var command = new RangeCommand(KeyRange.point(key("alpha")), RangeCommand.Context.STANDALONE,
        0, 0, new RangeCommand.Sort(0, 0), new RangeCommand.RevisionFilter(0, 0, 0, 0),
        false, false, false);
    try (RequestScope owner = RequestScope.withTimeout(Duration.ofSeconds(10));
         var cache = new CacheAdmissionTestSupport(budget, owner)) {
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
           var account = budget.admit("public", scope, reservation);
           var scheduler = new ScanScheduler(budget, Duration.ofSeconds(5))) {
        var evaluator = readRangeEvaluator(fixture, scheduler, account, scope, 16384);
        var request = new TxnCommand(List.of(), List.of(new TxnCommand.Range(command)), List.of());
        var validated = cache.finish(evaluator.evaluate(request, Optional.empty()).join()).join();
        assertTrue(cache.admit(validated, scope, digest(7)));
      }
      var transaction = TransactionId.create();
      var changedRecord = live(scenario.changed ? "alpha" : "beta", 3, 3, 1, 0, "new");
      var prepared = new PreparedCandidate(transaction, 1, 1, 2, changedRecord);
      var page = ManifestPage.create(transaction, 0, List.of(ManifestCandidateReference.from(prepared, 0)));
      var root = ManifestRoot.create(transaction, 1, 1, 2, 1,
          changedRecord.metadata().valueLength(), Optional.of(page.reference()));
      var repository = new MemoryManifestRepository(root, page, List.of(prepared));
      var decision = new CommitDecision(transaction, InvocationId.create(), 2, 2, 3,
          root.systemIdentity(), root.digest(), true);
      var decisions = new CommitDecisionRepository() {
        @Override public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope) {
          return CompletableFuture.completedFuture(scenario.missingDecision ? Optional.empty() : Optional.of(decision));
        }
        @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
            CommitDecision ignored, RequestScope scope) { throw new UnsupportedOperationException(); }
      };
      var previous = fixture.control.state;
      fixture.control.state = CommitState.restore(previous.profileFingerprint(), previous.ownerInstance(),
          previous.ownerEpoch(), previous.ownerLeaseDeadlineEpochMillis(), previous.stateVersion() + 1,
          2, 3, 2, 3, Optional.empty(), 0);
      if (scenario.changed) fixture.physical.add(live("alpha", 3, 3, 1, 0, "new"));
      int physicalReads = fixture.physical.getCalls.get();
      verifyProofLookup(new ProofLookupContext(fixture, budget, cache, command, scenario,
          decisions, repository, physicalReads));
    }
    assertEquals(0, budget.snapshot().allocatedBytes());
  }

  private record ProofLookupContext(Fixture fixture, BudgetManager budget, CacheAdmissionTestSupport cache,
      RangeCommand command, ProofScenario scenario, CommitDecisionRepository decisions,
      MemoryManifestRepository repository, int physicalReads) {}

  private static void verifyProofLookup(ProofLookupContext context) throws Exception {
    var fixture = context.fixture();
    var budget = context.budget();
    var cache = context.cache();
    var command = context.command();
    var scenario = context.scenario();
    var decisions = context.decisions();
    var repository = context.repository();
    int physicalReads = context.physicalReads();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    var proofReservation = new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8192);
    var proofLimits = new BudgetManager.ReadLimits(1000, 4L << 30, 1000, 1000, 1000);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
         RequestScope proofScope = RequestScope.withTimeout(Duration.ofSeconds(10));
         var account = budget.admit("public", scope, reservation);
         var proofAccount = budget.admit("public", proofScope, proofReservation, proofLimits)) {
      var session = TxnReadSession.open(fixture.views, List.of(), digest(9), fixture.physical,
          new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join();
      var prover = new UnchangedRangeProof(decisions, new ManifestCatalog(repository), 1, 8, 8192, ignored -> 0);
      var proofBudget = new ManifestCatalog.ProofReadBudget(proofScope, proofAccount, 8,
          scenario.exhaustProofBudget ? 1 : 512L << 20);
      if (scenario.failPinCompletion) {
        fixture.pinCompletion = CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
        assertThrows(CompletionException.class,
            () -> cache.lookupWithProof(session, command, digest(7), prover, proofBudget, account, scope));
        assertEquals(physicalReads, fixture.physical.getCalls.get());
        return;
      }
      var cached = cache.lookupWithProof(session, command, digest(7), prover, proofBudget, account, scope);
      if (!scenario.changed && !scenario.missingDecision && !scenario.exhaustProofBudget) {
        assertEquals("old", cached.orElseThrow());
        assertEquals(physicalReads, fixture.physical.getCalls.get());
        return;
      }
      assertTrue(cached.isEmpty());
      try (var direct = new DirectRecordReader(session.reader(0), account, 4096)
          .readForPreparation(key("alpha"), session.currentRevision(), ReadProjection.FULL).join()) {
        assertEquals("new", new String(direct.record().orElseThrow().value().orElseThrow()));
      }
      session.finish().join();
      assertEquals(physicalReads + 1, fixture.physical.getCalls.get());
    }
  }

  @Test
  void abandonedHistoricalPinFutureCannotCancelInstallationOrLeakItsProtection() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      var session = TxnReadSession.open(fixture.views, List.of(), digest(9), fixture.physical,
          new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join();
      fixture.pinRegistration = new CompletableFuture<>();
      CompletableFuture<Void> abandoned = session.pinRevisions(List.of(1L));
      assertFalse(abandoned.isDone());
      assertTrue(abandoned.cancel(false));
      assertThrows(IllegalStateException.class, () -> session.reader(1));
      fixture.pinRegistration.complete(null);
      session.reader(1);
      assertEquals(2, fixture.pinRegistrations.get());
      session.finish().join();
      assertEquals(2, fixture.pinCompletions.get());
      assertEquals(2, fixture.pinCloses.get());
    }
  }

  @Test
  void failedHistoricalPinClosesThePreviouslyAcquiredBasePin() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      var session = TxnReadSession.open(fixture.views, List.of(), digest(9), fixture.physical,
          new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join();
      fixture.pinRegistration = new CompletableFuture<>();
      var pending = session.pinRevisions(List.of(1L));
      fixture.pinRegistration.completeExceptionally(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      assertEquals(BackendFailure.Category.UNAVAILABLE,
          assertInstanceOf(BackendFailure.class, completionFailure(pending::join)).category());
      scope.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertEquals(1, fixture.pinCloses.get());
      assertThrows(IllegalStateException.class, () -> session.reader(0));
    }
  }

  @Test
  void tooManySelectedHistoricalPinsAreResourceExhaustion() throws Exception {
    Fixture fixture = new Fixture(130, List.of(
        candidate("delta", 1, 131, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      var session = TxnReadSession.open(fixture.views, List.of(), digest(9), fixture.physical,
          new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join();
      List<Long> revisions = java.util.stream.LongStream.rangeClosed(1, 128).boxed().toList();
      var failure = assertInstanceOf(BackendFailure.class,
          completionFailure(() -> session.pinRevisions(revisions).join()));
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
      assertEquals(1, fixture.pinRegistrations.get());
    }
  }

  @Test
  void selectedHistoricalPinsKeepTheOriginalBarrierAfterAnotherCommit() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "old"));
    fixture.physical.add(live("alpha", 1, 2, 2, 0, "base"));
    fixture.physical.add(live("alpha", 1, 3, 3, 0, "later"));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      var session = TxnReadSession.open(fixture.views, List.of(), digest(9), fixture.physical,
          new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join();
      CommitState previous = fixture.control.state;
      fixture.control.state = CommitState.restore(previous.profileFingerprint(), previous.ownerInstance(),
          previous.ownerEpoch(), previous.ownerLeaseDeadlineEpochMillis(), previous.stateVersion() + 1,
          previous.publishedSequence() + 1, 3, previous.finalizedSequence() + 1, 3, Optional.empty(), 0);
      session.pinRevisions(List.of(1L)).join();
      var current = session.reader(0).get(key("alpha"), 2, ReadProjection.FULL).join().orElseThrow();
      var historical = session.reader(1).get(key("alpha"), 1, ReadProjection.FULL).join().orElseThrow();
      assertEquals("base", new String(current.value().orElseThrow()));
      assertEquals("old", new String(historical.value().orElseThrow()));
      assertEquals(2, session.currentRevision());
      assertEquals(previous.publishedSequence(), session.barrierSequence());
      session.finish().join();
    }
  }

  @Test
  void nestedPreflightFollowsNativeDynamicErrorOrderAndSkipsUnselectedTargets() throws Exception {
    var future = pointRange(key("missing"), Long.MAX_VALUE);
    var ignoredMissing = new TxnCommand.Put(new PutCommand(key("missing-ignore"),
        new byte[0], 0, false, true, false));
    var selected = new TxnCommand(List.of(), List.of(future,
        new TxnCommand.Nested(new TxnCommand(List.of(), List.of(ignoredMissing), List.of()))), List.of());
    assertTransactionFailure(selected, KvFailure.Reason.KEY_NOT_FOUND);
    assertTransactionFailure(new TxnCommand(List.of(),
        List.of(pointRange(key("a"), -2)), List.of()), KvFailure.Reason.COMPACTED);
    assertTransactionFailure(new TxnCommand(List.of(), List.of(future), List.of()), KvFailure.Reason.FUTURE_REVISION);

    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var compares = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, compares, account, scope, 16384);
      var unselected = new TxnCommand(List.of(), List.of(pointRange(key("a"), -1)),
          List.of(new TxnCommand.Nested(new TxnCommand(List.of(), List.of(future, ignoredMissing), List.of()))));
      var binding = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      var result = evaluator.evaluate(unselected, Optional.of(binding)).join();
      assertEquals(1, fixture.pinRegistrations.get(), "unselected historical targets must not be pinned");
      assertEquals(1, assertInstanceOf(ResultNode.Txn.class, result.result().root()).responses().size());
      result.finishRead().join();
    }
  }

  @Test
  void uncertainPreparedInvocationCannotRerunItsComparisons() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    var command = new TxnCommand(List.of(CompareCondition.numeric(KeyRange.point(key("compare")),
        CompareCondition.Target.VERSION, CompareCondition.Relation.EQUAL, 0)),
        List.of(new TxnCommand.Put(new PutCommand(key("write"), bytes("value"), 0, false, false, false))), List.of());
    InvocationBinding binding;
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      binding = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      fixture.worksetRecords.failNextPage = true;
      assertThrows(CompletionException.class, () -> evaluator.evaluate(command, Optional.of(binding)).join());
    }
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      fixture.physical.blockNextGet(key("compare"));
      var retry = evaluator.evaluate(command, Optional.of(binding));
      assertTrue(retry.isDone(), "the durable start marker must reject replay before any comparison read");
      assertThrows(CompletionException.class, retry::join);
      assertFalse(fixture.physical.pendingGet.isDone());
    }
  }

  private static void assertTransactionFailure(TxnCommand command, KvFailure.Reason expected) throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var compares = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, compares, account, scope, 16384);
      var binding = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      Optional<InvocationBinding> invocation = command.mayWrite() ? Optional.of(binding) : Optional.empty();
      var failure = assertInstanceOf(KvFailure.class, completionFailure(() -> evaluator.evaluate(command, invocation).join()));
      assertEquals(expected, failure.reason());
      assertEquals(1, fixture.pinRegistrations.get(), "invalid target must fail before acquiring another pin");
    }
  }

  @Test
  void flatTransactionMayChooseAnEmptyElseWithoutIncreasingRevision() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      var command = new TxnCommand(List.of(CompareCondition.numeric(KeyRange.point(key("missing")),
          CompareCondition.Target.VERSION, CompareCondition.Relation.GREATER, 0)),
          List.of(new TxnCommand.Put(new PutCommand(key("a"), new byte[0], 0, false, true, false)),
              new TxnCommand.Unsupported(true)),
          List.of());
      InvocationBinding invocation = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      var evaluated = evaluator.evaluate(command, Optional.of(invocation)).join();
      var result = assertInstanceOf(ResultNode.Txn.class, evaluated.result().root());
      assertFalse(result.succeeded());
      assertEquals(2, result.header().revision());
      assertTrue(result.responses().isEmpty());
      assertEquals(0, evaluated.candidateCount());
      assertEquals(0, evaluated.eventCount());
      session.finish().join();
    }
  }

  @Test
  void flatTransactionCanWriteInElse() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      var command = new TxnCommand(List.of(CompareCondition.numeric(KeyRange.point(key("missing")),
          CompareCondition.Target.VERSION, CompareCondition.Relation.GREATER, 0)), List.of(),
          List.of(new TxnCommand.Put(new PutCommand(key("a"), bytes("else"), 0, false, false, false))));
      InvocationBinding invocation = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      var evaluated = evaluator.evaluate(command, Optional.of(invocation)).join();
      var result = assertInstanceOf(ResultNode.Txn.class, evaluated.result().root());
      assertFalse(result.succeeded());
      assertEquals(3, result.header().revision());
      assertEquals(1, evaluated.candidateCount());
      session.finish().join();
    }
  }

  @Test
  void readOnlyTransactionNeedsNoInvocation() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      var evaluated = evaluator.evaluate(new TxnCommand(List.of(),
          List.of(pointRange(key("missing"))), List.of()), Optional.empty()).join();
      var result = assertInstanceOf(ResultNode.Txn.class, evaluated.result().root());
      assertTrue(result.succeeded());
      assertEquals(2, result.header().revision());
      assertEquals(0, assertInstanceOf(ResultNode.Range.class, result.responses().getFirst()).count());
      assertEquals(0, evaluated.eventCount());
      var validated = evaluated.finishReadOnly().join();
      assertTrue(validated.belongsTo(scope));
      assertEquals(2, validated.revision());
    }
  }

  @Test
  void flatTransactionDiscardsAnOversizeResultAndReleasesItsBudgetOnCancellation() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.physical.add(live("alpha", 1, 1, 1, 0, "old"));
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
    try (BudgetManager.Account account = budget.admit("public", scope, reservation);
         var scheduler = new ScanScheduler(budget, Duration.ofSeconds(5))) {
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 256);
      var result = evaluator.evaluate(new TxnCommand(List.of(),
          List.of(pointRange(key("alpha"))), List.of()), Optional.empty());
      var failure = assertInstanceOf(BackendFailure.class, completionFailure(result::join));
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
      scope.cancelAsync(task -> Thread.ofVirtual().start(task));
      scope.cleanupCompletion().toCompletableFuture().join();
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void flatTransactionRejectsAnInvocationFromAnotherAuthorizationScope() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      var command = new TxnCommand(List.of(), List.of(new TxnCommand.Put(
          new PutCommand(key("a"), bytes("x"), 0, false, false, false))), List.of());
      InvocationBinding foreign = InvocationBinding.begin(InvocationId.create(), digest(7), digest(8),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      assertInstanceOf(IllegalArgumentException.class,
          completionFailure(() -> evaluator.evaluate(command, Optional.of(foreign)).join()));
      assertEquals(0, fixture.physical.getCalls.get());
      session.close();
    }
  }

  @Test
  void potentiallyWritingTransactionRejectsAnUnfinalizedPublication() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      var command = new TxnCommand(List.of(), List.of(new TxnCommand.Put(
          new PutCommand(key("a"), bytes("x"), 0, false, false, false))), List.of());
      InvocationBinding binding = InvocationBinding.begin(InvocationId.create(), digest(7), digest(9),
          TransactionId.create(), 1, session.barrierSequence(), session.currentRevision());
      var failure = assertInstanceOf(BackendFailure.class,
          completionFailure(() -> evaluator.evaluate(command, Optional.of(binding)).join()));
      assertEquals(BackendFailure.Category.UNAVAILABLE, failure.category());
      assertEquals(0, fixture.physical.getCalls.get());
      session.close();
    }
  }

  @Test
  void selectedUnsupportedTransactionOperationFailsExplicitly() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    try (var resources = transactionResources(budget, reservation)) {
      var scope = resources.scope();
      var account = resources.account();
      var scheduler = resources.scheduler();
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      var command = new TxnCommand(List.of(), List.of(new TxnCommand.Unsupported(false)), List.of());
      var failure = assertInstanceOf(KvFailure.class,
          completionFailure(() -> evaluator.evaluate(command, Optional.empty()).join()));
      assertEquals(KvFailure.Reason.UNSUPPORTED_OPERATION, failure.reason());
      session.close();
    }
  }

  @Test
  void flatTransactionCancellationKeepsBudgetUntilItsPhysicalReadStops() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("delta", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    var budget = transactionBudget();
    var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 65536, 131072);
    RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
    try (BudgetManager.Account account = budget.admit("public", scope, reservation);
         var scheduler = new ScanScheduler(budget, Duration.ofSeconds(5))) {
      var session = openTransaction(fixture, scope);
      var comparisons = transactionComparisons(session, scheduler, account, scope);
      var evaluator = transactionEvaluator(fixture, session, comparisons, account, scope, 16384);
      fixture.physical.blockNextGet(key("blocked"));
      var result = evaluator.evaluate(new TxnCommand(List.of(),
          List.of(pointRange(key("blocked"))), List.of()), Optional.empty());
      scope.cancelAsync(task -> Thread.ofVirtual().start(task));
      assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
      assertTrue(budget.snapshot().allocatedBytes() > 0);
      fixture.physical.pendingGet.complete(Optional.empty());
      assertThrows(CompletionException.class, result::join);
      scope.cleanupCompletion().toCompletableFuture().join();
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void transactionRejectsAllResultsIfAnyPinFailsFinalValidation() {
    Fixture fixture = new Fixture(1, List.of(
        candidate("k", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    fixture.pinCompletion = new CompletableFuture<>();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      var session = TxnReadSession.open(fixture.views, List.of(1L), digest(9),
          fixture.physical, new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join();
      var completion = session.finish();
      assertFalse(completion.isDone());
      fixture.pinCompletion.completeExceptionally(new BackendFailure(BackendFailure.Category.CANCELLED));
      assertThrows(CompletionException.class, completion::join);
      assertTrue(scope.isClosed());
      CompletableFuture.allOf(scope.cleanupCompletion().toCompletableFuture(),
          scope.physicalProducersCompletion().toCompletableFuture()).join();
      assertEquals(2, fixture.pinCloses.get());
      assertThrows(IllegalStateException.class, () -> session.reader(0));
    }
  }

  @Test
  void cancellationDuringFinalPinValidationRejectsTheTransaction() {
    Fixture fixture = new Fixture(1, List.of(
        candidate("k", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    fixture.pinCompletion = new CompletableFuture<>();
    RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
    var session = TxnReadSession.open(fixture.views, List.of(1L), digest(9),
        fixture.physical, new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join();
    var completion = session.finish();
    assertFalse(completion.isDone());
    scope.cancelAsync(task -> Thread.ofVirtual().start(task));
    fixture.pinCompletion.complete(null);
    BackendFailure rejected = assertInstanceOf(BackendFailure.class,
        completionFailure(completion::join));
    assertEquals(BackendFailure.Category.CANCELLED, rejected.category());
    scope.cleanupCompletion().toCompletableFuture().join();
    assertEquals(2, fixture.pinCloses.get());
  }

  @Test
  void cancellingTransactionRetainsItsPinUntilThePhysicalReaderStops() {
    Fixture fixture = new Fixture(1, List.of(
        candidate("alpha", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
    var session = TxnReadSession.open(fixture.views, List.of(1L), digest(9), fixture.physical,
        new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join();
    fixture.physical.blockNextGet(key("zeta"));
    var pending = session.reader(1).get(key("zeta"), 1, ReadProjection.FULL);
    session.close();
    assertTrue(scope.isClosed());
    assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());
    assertTrue(fixture.pinCloses.get() < 2, "an active physical reader retains its pin");
    fixture.physical.pendingGet.complete(Optional.empty());
    assertThrows(CompletionException.class, pending::join);
    scope.cleanupCompletion().toCompletableFuture().join();
    assertEquals(2, fixture.pinCloses.get());
  }

  @Test
  void invalidTransactionTargetsFailClosedAndReleaseEarlierPins() {
    Fixture fixture = new Fixture(1, List.of(
        candidate("alpha", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      assertThrows(CompletionException.class, () -> TxnReadSession.open(fixture.views,
          java.util.Collections.nCopies(129, 1L), digest(9), fixture.physical,
          new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join());
      assertEquals(0, fixture.control.reads.get());
    }
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      assertThrows(CompletionException.class, () -> TxnReadSession.open(fixture.views, List.of(99L),
          digest(9), fixture.physical, new ManifestCatalog(fixture.repository), ignored -> 0, 1, scope).join());
      scope.cleanupCompletion().toCompletableFuture().join();
      assertTrue(scope.isClosed());
      assertEquals(1, fixture.pinCloses.get());
    }
  }

  @Test
  void selectedRootIsPinnedAndRecheckedBeforeAnyDataRead() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("k", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      PinnedReadView pinned = fixture.views.open(OptionalLong.empty(), digest(9), scope).join();
      assertEquals(2, fixture.control.reads.get(), "control is read before and after pin registration");
      assertEquals(2, fixture.repository.rootReads.get(), "the selected root is rechecked after pinning");
      assertEquals(1, fixture.pinRegistrations.get());
      assertEquals(17, pinned.view().pinGeneration());
    }
    assertEquals(1, fixture.pinCloses.get());
  }

  @Test
  void failedRenewalPreventsPointAndScanDataReads() {
    Fixture fixture = new Fixture(1, List.of(
        candidate("k", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    fixture.pinRenewal = CompletableFuture.failedFuture(
        new BackendFailure(BackendFailure.Category.UNAVAILABLE));

    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      ReadViewRecordStore view = fixture.readStore(
          fixture.views.open(OptionalLong.empty(), digest(9), scope).join(), scope);
      BackendFailure pointFailure = assertInstanceOf(BackendFailure.class,
          completionFailure(() -> view.get(key("k"), 2, ReadProjection.FULL).join()));
      assertEquals(BackendFailure.Category.UNAVAILABLE, pointFailure.category());
      assertEquals(0, fixture.physical.getCalls.get());
      assertEquals(0, fixture.repository.candidateReads.get());

      ScanOperation scan = view.startScan(new ScanRequest(0, KeyRange.all(), Optional.empty(),
          2, ReadProjection.FULL, 4, 8192, 16));
      BackendFailure scanFailure = assertInstanceOf(BackendFailure.class,
          completionFailure(() -> scan.completion().toCompletableFuture().join()));
      assertEquals(BackendFailure.Category.UNAVAILABLE, scanFailure.category());
      assertEquals(0, fixture.physical.scanCalls.get());
      assertEquals(0, fixture.repository.candidateReads.get());
    }
    assertEquals(2, fixture.pinRenews.get());
    assertEquals(1, fixture.pinCloses.get());
  }

  @Test
  void finalPinValidationFailureDiscardsRangeAndReleasesOutputLease() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("k", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    fixture.pinCompletion = new CompletableFuture<>();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(20))) {
      ReadViewRecordStore view = fixture.readStore(
          fixture.views.open(OptionalLong.empty(), digest(9), scope).join(), scope);
      var limits = new BudgetManager.Limits(4, 32, 1, 1, 1, 1_000_000, 16_000_000);
      var budgets = new BudgetManager(limits, Map.of("public", limits), limits);
      var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 8192, 8192);
      try (var account = budgets.admit("public", scope, reservation);
          var scheduler = new ScanScheduler(budgets, Duration.ofSeconds(15));
          var cursor = SortedMergeCursor.values(view, scheduler.bind(view, account),
              new ScanRequest(0, KeyRange.all(), Optional.empty(), 2,
                  ReadProjection.FULL, 4, 8192, 16), 1, account, scope)) {
        var result = cursor.read(1);
        fixture.pinCompletionStarted.get(10, TimeUnit.SECONDS);
        assertFalse(result.isDone(), "materialized rows wait for durable pin completion");
        assertTrue(budgets.snapshot().allocatedBytes() > 0,
            "the response lease remains owned while final pin validation is pending");

        fixture.pinCompletion.completeExceptionally(
            new BackendFailure(BackendFailure.Category.CANCELLED));
        BackendFailure rejected = assertInstanceOf(BackendFailure.class,
            completionFailure(() -> result.join()));
        assertEquals(BackendFailure.Category.CANCELLED, rejected.category());
        assertEquals(0, budgets.snapshot().allocatedBytes(),
            "failed validation discards the materialized response and releases its lease");
      }
    }
    assertEquals(1, fixture.pinCompletions.get());
    assertEquals(1, fixture.pinCloses.get());
  }

  @Test
  void successfulFinalPinValidationDeliversAndTransfersRangeOwnership() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("k", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    fixture.pinCompletion = new CompletableFuture<>();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(20))) {
      ReadViewRecordStore view = fixture.readStore(
          fixture.views.open(OptionalLong.empty(), digest(9), scope).join(), scope);
      var limits = new BudgetManager.Limits(4, 32, 1, 1, 1, 1_000_000, 16_000_000);
      var budgets = new BudgetManager(limits, Map.of("public", limits), limits);
      var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 8192, 8192);
      try (var account = budgets.admit("public", scope, reservation);
          var scheduler = new ScanScheduler(budgets, Duration.ofSeconds(15));
          var cursor = SortedMergeCursor.values(view, scheduler.bind(view, account),
              new ScanRequest(0, KeyRange.all(), Optional.empty(), 2,
                  ReadProjection.FULL, 4, 8192, 16), 1, account, scope)) {
        var result = cursor.read(1);
        fixture.pinCompletionStarted.get(10, TimeUnit.SECONDS);
        assertFalse(result.isDone(), "materialized rows wait for durable pin completion");
        assertTrue(budgets.snapshot().allocatedBytes() > 0,
            "the pending response owns its output lease until pin completion");
        fixture.pinCompletion.complete(null);
        try (var response = result.get(10, TimeUnit.SECONDS)) {
          assertEquals(List.of("k"), response.records().stream()
              .map(record -> text(record.key())).toList());
          assertEquals(1, fixture.pinCompletions.get());
          assertTrue(budgets.snapshot().allocatedBytes() > 0,
              "the delivered response owns its output lease until the caller closes it");
        }
      }
      assertEquals(0, budgets.snapshot().allocatedBytes());
    }
    assertEquals(1, fixture.pinCloses.get());
  }

  @Test
  void cancellingAReadKeepsItsPinUntilThePhysicalProducerStops() {
    Fixture fixture = new Fixture(1, List.of(
        candidate("alpha", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
    PinnedReadView pinned = fixture.views.open(OptionalLong.empty(), digest(9), scope).join();
    fixture.physical.blockNextGet(key("zeta"));
    ReadViewRecordStore view = fixture.readStore(pinned, scope);
    var pending = view.get(key("zeta"), 2, ReadProjection.FULL);

    scope.close();
    assertEquals(0, fixture.pinCloses.get(), "an unfinished backend read still protects its view");
    fixture.physical.pendingGet.complete(Optional.empty());

    assertTrue(pending.isCompletedExceptionally());
    scope.cleanupCompletion().toCompletableFuture().join();
    assertEquals(1, fixture.pinCloses.get(), "the view pin releases after producer completion");
  }

  @Test
  void byteTruncatedPhysicalPageDoesNotSkipKeysDuringOverlayMerge() throws BackendFailure {
    Fixture fixture = new Fixture(1, List.of(
        candidate("z", 2, 2, 1, StorageMetadata.State.LIVE, new byte[100])));
    fixture.physical.add(liveBytes("a", 1, 1, 100));
    fixture.physical.add(liveBytes("b", 1, 1, 700));

    assertEquals(List.of("a", "b", "z"), scanKeys(fixture, 1024, 3));
    assertEquals(List.of("a", "b", "z"), scanKeys(fixture, 4096, 1));
    assertEquals(2, fixture.pinCloses.get());
  }

  @Test
  void byteTruncatedCandidatePageDoesNotSkipKeysDuringOverlayMerge() throws BackendFailure {
    Fixture fixture = new Fixture(1, List.of(
        candidate("a", 2, 2, 1, StorageMetadata.State.LIVE, new byte[400]),
        candidate("b", 2, 2, 1, StorageMetadata.State.LIVE, new byte[700])));
    fixture.physical.add(liveBytes("z", 1, 1, 100));

    assertEquals(List.of("a", "b", "z"), scanKeys(fixture, 1024, 3));
    assertEquals(List.of("a", "b", "z"), scanKeys(fixture, 4096, 1));
    assertEquals(2, fixture.pinCloses.get());
  }

  @Test
  void cancellingReadResultDoesNotCancelPinAccounting() throws Exception {
    Fixture beforeProducerCompletion = new Fixture(1, List.of(
        candidate("alpha", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    RequestScope firstScope = RequestScope.withTimeout(Duration.ofSeconds(5));
    PinnedReadView firstPinned = beforeProducerCompletion.views
        .open(OptionalLong.empty(), digest(9), firstScope).join();
    beforeProducerCompletion.physical.blockNextGet(key("zeta"));
    var firstResult = beforeProducerCompletion.readStore(firstPinned, firstScope)
        .get(key("zeta"), 2, ReadProjection.FULL);

    assertTrue(firstResult.cancel(false));
    firstScope.close();
    assertEquals(0, beforeProducerCompletion.pinCloses.get());
    beforeProducerCompletion.physical.pendingGet.complete(Optional.empty());
    firstScope.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals(1, beforeProducerCompletion.pinCloses.get(),
        "scope cleanup waits for the physical producer even when the caller cancels its result");

    Fixture afterProducerCompletion = new Fixture(1, List.of(
        candidate("alpha", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    RequestScope secondScope = RequestScope.withTimeout(Duration.ofSeconds(5));
    PinnedReadView secondPinned = afterProducerCompletion.views
        .open(OptionalLong.empty(), digest(9), secondScope).join();
    afterProducerCompletion.physical.blockNextGet(key("zeta"));
    var secondResult = afterProducerCompletion.readStore(secondPinned, secondScope)
        .get(key("zeta"), 2, ReadProjection.FULL);

    assertTrue(secondResult.cancel(false));
    afterProducerCompletion.physical.pendingGet.complete(Optional.empty());
    assertEquals(0, afterProducerCompletion.pinCloses.get());
    secondScope.close();
    secondScope.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals(1, afterProducerCompletion.pinCloses.get(),
        "scope cleanup releases the pin when the producer already completed");
  }

  @Test
  void scopeCancellationWaitsForManifestProducerBeforeReleasingRegisteredPin() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("alpha", 2, 2, 1, StorageMetadata.State.LIVE, new byte[4096])));
    fixture.repository.blockRootRead(2);
    RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));

    var opening = fixture.views.open(OptionalLong.empty(), digest(9), scope);
    assertFalse(opening.isDone(), "the post-registration root recheck owns a pending producer");
    assertEquals(1, fixture.pinRegistrations.get());

    scope.close();

    assertTrue(opening.isCompletedExceptionally(), "scope cancellation completes the request result");
    assertFalse(fixture.repository.pendingRootProducer.isDone());
    assertEquals(0, fixture.pinCloses.get(), "the pin remains while the manifest producer is active");
    assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());

    fixture.repository.pendingRootProducer.complete(null);
    scope.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals(1, fixture.pinCloses.get());
  }

  @Test
  void metadataHeadBudgetDoesNotRejectLargeCandidatePayload() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("a", 2, 2, 1, StorageMetadata.State.LIVE, new byte[4096]),
        candidate("b", 2, 2, 1, StorageMetadata.State.LIVE, bytes("small"))));
    fixture.physical.add(liveBytes("z", 1, 1, 64));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      PinnedReadView pinned = fixture.views.open(OptionalLong.empty(), digest(9), scope).join();
      ReadViewRecordStore view = fixture.readStore(pinned, scope);
      var metadataRequest = new ScanRequest(0, KeyRange.all(), Optional.empty(), 2,
          ReadProjection.METADATA, 1, 1024, 64);
      var limits = new BudgetManager.Limits(4, 32, 2, 2, 2, 1_000_000, 16_000_000);
      var budgets = new BudgetManager(limits, Map.of("public", limits), limits);
      var reservation = new BudgetManager.Reservation(1024, 1024, 8192, 8192, 8192);
      try (var metadataAccount = budgets.admit("public", scope, reservation)) {
        assertMetadataHeadBudget(view, metadataAccount, metadataRequest);
        assertFullMergeAndPointExists(fixture, budgets, scope, reservation);
        assertRangeExistsUsesMetadataHeadBudget(fixture, budgets, scope, reservation);
      }
    }
    assertEquals(5, fixture.pinCloses.get());
  }

  private static void assertMetadataHeadBudget(ReadViewRecordStore view,
      BudgetManager.Account account, ScanRequest request) {
    StoragePage metadata = view.startScan(request, account).completion()
        .toCompletableFuture().join();
    assertEquals(List.of("a"), metadata.records().stream()
        .map(record -> text(record.key())).toList());
    assertFalse(metadata.records().getFirst().hasValue());
    assertTrue(metadata.records().getFirst().retainedBytes() <= request.maximumBytes());
    assertEquals(4353, metadata.cost().observations()
        .get(StoragePage.Observation.MANIFEST_CANDIDATE_BYTES));
  }

  private static void assertFullMergeAndPointExists(Fixture fixture,
      BudgetManager budgets, RequestScope scope, BudgetManager.Reservation reservation)
      throws BackendFailure {
    var mergeRequest = new ScanRequest(0, KeyRange.all(), Optional.empty(), 2,
        ReadProjection.METADATA, 1, 1024, 64);
    ReadViewRecordStore mergeView = fixture.readStore(
        fixture.views.open(OptionalLong.empty(), digest(9), scope).join(), scope);
    try (var account = budgets.admit("public", scope, reservation);
        var scheduler = new ScanScheduler(budgets, Duration.ofSeconds(1));
        var cursor = SortedMergeCursor.values(mergeView,
            scheduler.bind(mergeView, account), mergeRequest, 1, account, scope);
        var response = cursor.read(0).join()) {
      assertEquals(List.of("a", "b", "z"), response.records().stream()
          .map(record -> text(record.key())).toList());
      assertEquals(4096, response.records().getFirst().value().orElseThrow().length,
          "the selected value is fetched after the metadata head");
      assertEquals(5, response.records().get(1).value().orElseThrow().length,
          "a small candidate remains readable");
    }
    ReadViewRecordStore candidateView = fixture.readStore(
        fixture.views.open(OptionalLong.empty(), digest(9), scope).join(), scope);
    try (var account = budgets.admit("public", scope, reservation)) {
      assertEquals(1, ExistsExecutor.point(candidateView, key("a"), 2, account).join());
    }
    ReadViewRecordStore physicalView = fixture.readStore(
        fixture.views.open(OptionalLong.empty(), digest(9), scope).join(), scope);
    try (var account = budgets.admit("public", scope, reservation)) {
      assertEquals(1, ExistsExecutor.point(physicalView, key("z"), 2, account).join(),
          "the physical-only control remains readable");
    }
  }

  private static void assertRangeExistsUsesMetadataHeadBudget(Fixture fixture,
      BudgetManager budgets, RequestScope scope, BudgetManager.Reservation reservation)
      throws BackendFailure {
    ReadViewRecordStore view = fixture.readStore(
        fixture.views.open(OptionalLong.empty(), digest(9), scope).join(), scope);
    try (var account = budgets.admit("public", scope, reservation);
        var scheduler = new ScanScheduler(budgets, Duration.ofSeconds(1));
        var exists = new ExistsExecutor(scheduler.bind(view, account),
            new ScanRequest(0, KeyRange.point(key("a")), Optional.empty(), 2,
                ReadProjection.METADATA, 1, 1024, 64), 1, scope, view)) {
      assertEquals(1, exists.execute().join().count(),
          "range Exists uses the metadata head budget");
    }
  }

  @Test
  void descendingGlobalLimitUsesTheNextFilteredLiveKeyAcrossBuckets() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("x", 1, 2, 2, StorageMetadata.State.LIVE, bytes("x"))));
    fixture.control.state = fixture.control.state.finalizePublication(fixture.transactionId);
    fixture.physical.add(live("a", 1, 2, 2, 0, "a"));
    fixture.physical.add(tombstone("b", 1, 2));
    fixture.physical.add(live("c", 1, 1, 1, 0, "c"));
    fixture.physical.add(live("d", 1, 2, 2, 0, "d"));
    fixture.physical.add(live("e", 1, 2, 2, 0, "e"));
    java.util.function.ToIntFunction<ByteKey> bucket = value -> value.equals(key("b")) || value.equals(key("d")) ? 1 : 0;
    fixture.physical.bucketOf = bucket;
    var limits = new BudgetManager.Limits(4, 32, 2, 1, 1, 1_000_000, 16_000_000);
    var budget = new BudgetManager(limits, Map.of("public", limits), limits);
    var reservation = new BudgetManager.Reservation(32768, 8192, 8192, 65536, 131072);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
         BudgetManager.Account account = budget.admit("public", scope, reservation);
         var scheduler = new ScanScheduler(budget, Duration.ofSeconds(5))) {
      var session = TxnReadSession.open(fixture.views, List.of(), digest(9), fixture.physical,
          new ManifestCatalog(fixture.repository), bucket, 2, scope).join();
      var compares = new CompareReadExecutor(session, scheduler, account, scope,
          new CompareReadExecutor.Limits(2, 4096, 64, 4096));
      var evaluator = new TxnEvaluator(session, compares, account, scope,
          new TxnEvaluator.Limits(4096, 16384, 65536), bucket);
      var range = new RangeCommand(KeyRange.all(), RangeCommand.Context.TRANSACTION, 0, 2,
          new RangeCommand.Sort(2, 0), new RangeCommand.RevisionFilter(2, 0, 0, 0), false, false, false);
      var command = new TxnCommand(List.of(), List.of(new TxnCommand.Range(range)), List.of());
      var read = evaluator.evaluate(command, Optional.empty()).join().finishReadOnly().join();
      var result = assertInstanceOf(ResultNode.Range.class,
          assertInstanceOf(ResultNode.Txn.class, read.result().root()).responses().getFirst());
      assertEquals(List.of("e", "d"), result.records().stream().map(record -> text(record.key())).toList());
      assertEquals(2, result.count());
      assertTrue(result.more());
    }
    assertEquals(0, budget.snapshot().allocatedBytes());
  }

  @Test
  void descendingSelectedViewResumesPhysicalProgressAtItsStableRevision() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("d", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    fixture.physical.add(live("c", 1, 1, 1, 0, "base"));
    fixture.physical.progressOnce.set(true);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      var budgets = budget(scope, 16384);
      var view = fixture.readStore(fixture.views.open(OptionalLong.empty(), digest(9), scope).join(), scope);
      var request = new ScanRequest(0, KeyRange.all(), Optional.empty(), 2,
          ReadProjection.METADATA, 1, 4096, 1, ScanDirection.DESCENDING);
      try (var account = budgets.account();
           var scheduler = new ScanScheduler(budgets.manager(), Duration.ofSeconds(5));
           var cursor = new BucketCursor(scheduler.bind(view, account), request, scope, account);
           var page = cursor.fetch().join()) {
        assertEquals("d", text(page.next().orElseThrow().key()));
        assertTrue(page.cost().physicalRows() >= 2);
        assertEquals(1, fixture.physical.lastProgressRevision);
        assertEquals(1, fixture.repository.candidateReads.get(),
            "physical-only continuation must not reread the selected candidate");
      } finally {
        view.close();
      }
      assertEquals(0, budgets.manager().snapshot().allocatedBytes());
    }
  }

  @Test
  void descendingReadViewMergesSelectedCandidatesAndRejectsAnotherViewCheckpoint() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("b", 1, 2, 2, StorageMetadata.State.TOMBSTONE, new byte[0]),
        candidate("d", 2, 2, 1, StorageMetadata.State.LIVE, bytes("selected"))));
    fixture.physical.add(live("a", 1, 1, 1, 0, "a"));
    fixture.physical.add(live("b", 1, 1, 1, 0, "old"));
    fixture.physical.add(live("c", 1, 1, 1, 0, "c"));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
         var account = fixture.account(scope, 16384)) {
      var firstView = fixture.readStore(fixture.views.open(OptionalLong.empty(), digest(9), scope).join(), scope);
      var otherView = fixture.readStore(fixture.views.open(OptionalLong.empty(), digest(9), scope).join(), scope);
      var request = new ScanRequest(0, KeyRange.all(), Optional.empty(), 2,
          ReadProjection.METADATA, 1, 4096, 16, ScanDirection.DESCENDING);
      try {
        request = assertDescendingRecords(firstView, request, account);
        var foreign = otherView.startScan(request, account);
        assertThrows(CompletionException.class, () -> foreign.completion().toCompletableFuture().join());
        foreign.close();
        try (var otherAccount = fixture.account(scope, 16384)) {
          var wrongAccount = firstView.startScan(request, otherAccount);
          assertThrows(CompletionException.class, () -> wrongAccount.completion().toCompletableFuture().join());
          wrongAccount.close();
        }
      } finally {
        firstView.close();
        otherView.close();
      }
    }
  }

  private static ScanRequest assertDescendingRecords(ReadViewRecordStore view, ScanRequest request,
      BudgetManager.Account account) {
    for (String expected : List.of("d", "c", "b", "a")) {
      var operation = view.startScan(request, account);
      var page = operation.completion().toCompletableFuture().join();
      operation.close();
      assertEquals(expected, text(page.records().getFirst().key()));
      if (expected.equals("b")) {
        assertEquals(StorageMetadata.State.TOMBSTONE, page.records().getFirst().metadata().state());
      }
      request = request.resumeAfter(Optional.of(page.records().getFirst().key()));
    }
    return request;
  }

  @Test
  void descendingManifestTraversalResumesWithoutSkippingCandidates() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("a", 1, 2, 2, StorageMetadata.State.LIVE, bytes("a")),
        candidate("b", 1, 2, 2, StorageMetadata.State.TOMBSTONE, new byte[0]),
        candidate("c", 2, 2, 1, StorageMetadata.State.LIVE, bytes("c"))));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
         var account = fixture.account(scope, 16384)) {
      var catalog = new ManifestCatalog(fixture.repository);
      try (var first = catalog.candidatesInRange(fixture.root, 0, KeyRange.all(), Optional.empty(), 2,
          16384, scope, () -> false, account, ScanDirection.DESCENDING).join()) {
        assertEquals(List.of("c", "b"), first.candidates().stream().map(c -> text(c.record().key())).toList());
        assertFalse(first.exhausted());
      }
      try (var second = catalog.candidatesInRange(fixture.root, 0, KeyRange.all(), Optional.of(key("b")), 2,
          16384, scope, () -> false, account, ScanDirection.DESCENDING).join()) {
        assertEquals(List.of("a"), second.candidates().stream().map(c -> text(c.record().key())).toList());
        assertTrue(second.exhausted());
      }
    }
  }

  @Test
  void requestReadLimitStopsBeforeFetchingCandidateBeyondRemainingBytes() throws Exception {
    Fixture fixture = new Fixture(1, List.of(
        candidate("a", 2, 2, 1, StorageMetadata.State.LIVE, new byte[4 * 1024 * 1024]),
        candidate("b", 2, 2, 1, StorageMetadata.State.LIVE, new byte[4 * 1024 * 1024]),
        candidate("c", 2, 2, 1, StorageMetadata.State.LIVE, new byte[4 * 1024 * 1024])));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      PinnedReadView pinned = fixture.views.open(OptionalLong.empty(), digest(9), scope).join();
      ReadViewRecordStore view = fixture.readStore(pinned, scope);
      var limits = new BudgetManager.Limits(4, 32, 2, 2, 2, 1_000_000, 64L << 20);
      var budgets = new BudgetManager(limits, Map.of("public", limits), limits);
      var reservation = new BudgetManager.Reservation(16_384, 16L << 20, 1L << 20,
          1L << 20, 16L << 20);
      var readLimits = new BudgetManager.ReadLimits(100, 10L << 20, 100, 8, 8);
      try (var account = budgets.admit("public", scope, reservation, readLimits);
          var scheduler = new ScanScheduler(budgets, Duration.ofSeconds(1));
          var cursor = new BucketCursor(scheduler.bind(view, account),
              new ScanRequest(0, KeyRange.all(), Optional.empty(), 2, ReadProjection.FULL,
                  8, 16L << 20, 8), scope)) {
        try (BucketCursor.Page page = cursor.fetch().join()) {
          assertEquals(List.of("a", "b"), pageRecords(page));
          assertFalse(page.exhausted(), "the first candidate slice stops after b");
        }
        assertEquals(2, fixture.repository.candidateReads.get(),
            "the first 8 MiB slice stays below the 10 MiB request limit");
        CompletionException rejected = assertThrows(CompletionException.class,
            () -> cursor.fetch().join());
        BackendFailure failure = assertInstanceOf(BackendFailure.class, rejected.getCause());
        assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
        assertEquals(2, fixture.repository.candidateReads.get(),
            "c must be rejected from authenticated reference lengths before value I/O");
      }
    }
  }

  @Test
  void candidateSliceResumesAfterBoundedSlice() throws Exception {
    Fixture sliceBounded = new Fixture(1, List.of(
        candidate("a", 2, 2, 1, StorageMetadata.State.LIVE, new byte[4 * 1024 * 1024]),
        candidate("b", 2, 2, 1, StorageMetadata.State.LIVE, new byte[4 * 1024 * 1024]),
        candidate("c", 2, 2, 1, StorageMetadata.State.LIVE, new byte[4 * 1024 * 1024])));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      PinnedReadView pinned = sliceBounded.views.open(OptionalLong.empty(), digest(9), scope).join();
      ReadViewRecordStore view = sliceBounded.readStore(pinned, scope);
      var limits = new BudgetManager.Limits(4, 32, 2, 2, 2, 1_000_000, 64L << 20);
      var budgets = new BudgetManager(limits, Map.of("public", limits), limits);
      var reservation = new BudgetManager.Reservation(16_384, 16L << 20, 1L << 20,
          1L << 20, 16L << 20);
      var readLimits = new BudgetManager.ReadLimits(100, 32L << 20, 100, 8, 8);
      try (var account = budgets.admit("public", scope, reservation, readLimits);
          var scheduler = new ScanScheduler(budgets, Duration.ofSeconds(1));
          var cursor = new BucketCursor(scheduler.bind(view, account),
              new ScanRequest(0, KeyRange.all(), Optional.empty(), 2, ReadProjection.FULL,
                  8, 16L << 20, 8), scope)) {
        try (BucketCursor.Page page = cursor.fetch().join()) {
          assertEquals(List.of("a", "b"), pageRecords(page));
          assertFalse(page.exhausted(), "the 9 MiB candidate slice must resume after b");
        }
        assertEquals(2, sliceBounded.repository.candidateReads.get(),
            "c is outside the current 9 MiB slice and must not be read ahead");
        try (BucketCursor.Page page = cursor.fetch().join()) {
          assertEquals(List.of("c"), pageRecords(page));
          assertTrue(page.exhausted());
        }
        assertEquals(3, sliceBounded.repository.candidateReads.get(),
            "resuming after b fetches c exactly once");
      }
    }
  }

  @Test
  void candidateMemoryRejectionOccursBeforeCandidateRead() throws Exception {
    Fixture large = new Fixture(1, List.of(
        candidate("a", 2, 2, 1, StorageMetadata.State.LIVE, new byte[4 * 1024 * 1024])));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      PinnedReadView pinned = large.views.open(OptionalLong.empty(), digest(9), scope).join();
      ReadViewRecordStore view = large.readStore(pinned, scope);
      var limits = new BudgetManager.Limits(4, 32, 2, 2, 2, 1_000_000, 65_536);
      var budgets = new BudgetManager(limits, Map.of("public", limits), limits);
      var reservation = new BudgetManager.Reservation(1024, 1024, 1024, 8192);
      try (var account = budgets.admit("public", scope, reservation)) {
        assertEquals(11_264, reservation.total());
        CompletionException rejected = assertThrows(CompletionException.class,
            () -> new DirectRecordReader(view, account, 8192)
                .read(key("a"), 2, ReadProjection.FULL).join());
        BackendFailure failure = assertInstanceOf(BackendFailure.class, rejected.getCause());
        assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
        assertEquals(0, large.repository.candidateReads.get(),
            "the legacy four-lane reservation has no unaccounted candidate memory");
        assertEquals(0, budgets.snapshot().allocatedBytes(), "a rejected point read releases output and I/O leases");
      }
    }
  }

  @Test
  void candidateValueMemoryIsReservedBeforeReadAndReleasedAfterProjection() throws Exception {
    Fixture small = new Fixture(1, List.of(
        candidate("a", 2, 2, 1, StorageMetadata.State.LIVE, new byte[64])));
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      PinnedReadView pinned = small.views.open(OptionalLong.empty(), digest(9), scope).join();
      ReadViewRecordStore view = small.readStore(pinned, scope);
      var limits = new BudgetManager.Limits(4, 32, 2, 2, 2, 1_000_000, 65_536);
      var budgets = new BudgetManager(limits, Map.of("public", limits), limits);
      var reservation = new BudgetManager.Reservation(1024, 1024, 1024, 8192, 512);
      try (var account = budgets.admit("public", scope, reservation)) {
        try (var result = new DirectRecordReader(view, account, 512)
            .read(key("a"), 2, ReadProjection.FULL).join()) {
          assertEquals(64, result.record().orElseThrow().value().orElseThrow().length);
          assertEquals(1, small.repository.candidateReads.get());
          assertEquals(512, budgets.snapshot().allocatedBytes(),
              "candidate capacity transfers to the retained point result's output lease");
          assertEquals(1857, budgets.snapshot().peakAllocatedBytes(),
              "candidate memory overlaps only the bounded output and physical I/O leases");
        }
        assertEquals(0, budgets.snapshot().allocatedBytes(), "closing the point result releases output capacity");
      }
    }
  }

  @Test
  void cancelledCandidateReadRetainsLeaseUntilProducerStops() throws Exception {
    Fixture cancelled = new Fixture(1, List.of(
        candidate("a", 2, 2, 1, StorageMetadata.State.LIVE, new byte[64])));
    RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
    PinnedReadView pinned = cancelled.views.open(OptionalLong.empty(), digest(9), scope).join();
    PreparedCandidate selected = cancelled.repository.candidates.values().iterator().next();
    cancelled.repository.blockCandidateRead(selected.systemIdentity());
    var limits = new BudgetManager.Limits(4, 32, 2, 2, 2, 1_000_000, 65_536);
    var budgets = new BudgetManager(limits, Map.of("public", limits), limits);
    var reservation = new BudgetManager.Reservation(0, 1024, 1024, 0, 512);
    var account = budgets.admit("public", scope, reservation);
    var pending = new ManifestCatalog(cancelled.repository).candidatesInRange(
        pinned.view().selectedRoot().orElseThrow(), 0, KeyRange.all(), Optional.empty(), 8,
        1024, scope, () -> false, account);

    assertFalse(pending.isDone());
    assertEquals(selected.record().retainedBytes(), budgets.snapshot().allocatedBytes());
    scope.close();
    assertTrue(pending.isCompletedExceptionally());
    assertFalse(cancelled.repository.pendingCandidateProducer.isDone());
    assertEquals(selected.record().retainedBytes(), budgets.snapshot().allocatedBytes(),
        "candidate bytes remain charged while the cancelled request's physical producer is active");
    assertFalse(scope.cleanupCompletion().toCompletableFuture().isDone());

    cancelled.repository.pendingCandidateProducer.complete(null);
    scope.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals(0, budgets.snapshot().allocatedBytes(),
        "candidate lease releases only after the physical producer and scope cleanup finish");
  }

  private record TransactionResources(RequestScope scope, BudgetManager.Account account,
      ScanScheduler scheduler) implements AutoCloseable {
    @Override public void close() {
      try (scope; account; scheduler) {
        // Close in the same scheduler, account, scope order as the individual resources.
      }
    }
  }

  @FunctionalInterface
  private interface TransactionAdmission {
    BudgetManager.Account admit(RequestScope scope) throws BackendFailure;
  }

  private static TransactionResources transactionResources(BudgetManager budget,
      BudgetManager.Reservation reservation) throws BackendFailure {
    return transactionResources(budget, scope -> budget.admit("public", scope, reservation));
  }

  private static TransactionResources transactionResources(BudgetManager budget,
      BudgetManager.Reservation reservation, BudgetManager.ReadLimits reads) throws BackendFailure {
    return transactionResources(budget, scope -> budget.admit("public", scope, reservation, reads));
  }

  private static TransactionResources transactionResources(BudgetManager budget,
      TransactionAdmission admission) throws BackendFailure {
    RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
    try {
      BudgetManager.Account account = admission.admit(scope);
      try {
        var scheduler = new ScanScheduler(budget, Duration.ofSeconds(5));
        try {
          return new TransactionResources(scope, account, scheduler);
        } catch (RuntimeException | Error failure) {
          try { scheduler.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
          throw failure;
        }
      } catch (RuntimeException | Error failure) {
        try { account.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
        throw failure;
      }
    } catch (BackendFailure | RuntimeException | Error failure) {
      try { scope.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
      throw failure;
    }
  }

  private static BudgetManager transactionBudget() {
    var limits = new BudgetManager.Limits(4, 32, 1, 1, 1, 1_000_000, 16_000_000);
    return new BudgetManager(limits, Map.of("public", limits), limits);
  }

  private static TxnReadSession openTransaction(Fixture fixture, RequestScope scope) {
    return openTransaction(fixture, scope, 1);
  }

  private static TxnReadSession openTransaction(Fixture fixture, RequestScope scope, int buckets) {
    return TxnReadSession.open(fixture.views, List.of(), digest(9), fixture.physical,
        new ManifestCatalog(fixture.repository), ignored -> 0, buckets, scope).join();
  }

  private static CompareReadExecutor transactionComparisons(TxnReadSession session,
      ScanScheduler scheduler, BudgetManager.Account account, RequestScope scope) {
    return transactionComparisons(session, scheduler, account, scope, 1);
  }

  private static CompareReadExecutor transactionComparisons(TxnReadSession session,
      ScanScheduler scheduler, BudgetManager.Account account, RequestScope scope, int buckets) {
    return new CompareReadExecutor(session, scheduler, account, scope,
        new CompareReadExecutor.Limits(buckets, 4096, 64, 4096));
  }

  private static TxnEvaluator transactionEvaluator(Fixture fixture, TxnReadSession session,
      CompareReadExecutor comparisons, BudgetManager.Account account, RequestScope scope,
      long maxResultBytes) {
    return new TxnEvaluator(session, comparisons, account, scope,
        new TxnEvaluator.Limits(4096, maxResultBytes, 65536), ignored -> 0,
        fixture.worksetRecords, fixture.repository);
  }

  private static List<String> pageRecords(BucketCursor.Page page) {
    List<String> keys = new ArrayList<>();
    Optional<StoredRecord> record;
    while ((record = page.next()).isPresent()) keys.add(text(record.orElseThrow().key()));
    return List.copyOf(keys);
  }

  private static Throwable completionFailure(Runnable action) {
    CompletionException completion = assertThrows(CompletionException.class, action::run);
    Throwable cause = completion.getCause();
    return cause == null ? completion : cause;
  }

  private static List<String> scanKeys(Fixture fixture, long maximumBytes, int expectedPages)
      throws BackendFailure {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      PinnedReadView pinned = fixture.views.open(OptionalLong.empty(), digest(9), scope).join();
      ReadViewRecordStore view = fixture.readStore(pinned, scope);
      BudgetFixture budget = budget(scope, 1L << 20);
      try (BudgetManager.Account account = budget.account();
          ScanScheduler scheduler = new ScanScheduler(budget.manager(), Duration.ofSeconds(1))) {
        var request = new ScanRequest(0, KeyRange.all(), Optional.empty(), 2,
            ReadProjection.FULL, 8, maximumBytes, 8);
        try (BucketCursor cursor = new BucketCursor(scheduler.bind(view, account), request, scope)) {
          return readAllPages(cursor, expectedPages);
        }
      }
    }
  }

  private static BudgetFixture budget(RequestScope scope, long candidateBytes) throws BackendFailure {
    var limits = new BudgetManager.Limits(8, 64, 8, 8, 8, 1_000_000, 64L << 20);
    var manager = new BudgetManager(limits, Map.of("public", limits), limits);
    var reservation = new BudgetManager.Reservation(16_384, 1L << 20, 1L << 20,
        1L << 20, candidateBytes);
    var account = manager.admit("public", scope, reservation);
    return new BudgetFixture(manager, account);
  }

  private static List<String> readAllPages(BucketCursor cursor, int expectedPages) {
    List<String> keys = new ArrayList<>();
    int pages = 0;
    boolean exhausted = false;
    while (!exhausted && pages < 10) {
      try (BucketCursor.Page page = cursor.fetch().join()) {
        pages++;
        appendPageKeys(page, keys);
        exhausted = page.exhausted();
      }
    }
    assertTrue(exhausted, "read view scan must finish without losing progress");
    assertEquals(expectedPages, pages);
    return List.copyOf(keys);
  }

  private static void appendPageKeys(BucketCursor.Page page, List<String> keys) {
    Optional<StoredRecord> record;
    while ((record = page.next()).isPresent()) {
      keys.add(text(record.orElseThrow().key()));
    }
  }

  private static PreparedCandidate candidate(String key, long createRevision,
      long modificationRevision, long version, StorageMetadata.State state, byte[] value) {
    StorageMetadata metadata = new StorageMetadata(createRevision, modificationRevision, version,
        0, state, value.length);
    StoredRecord record = state == StorageMetadata.State.LIVE
        ? StoredRecord.live(key(key), metadata, value) : StoredRecord.metadataOnly(key(key), metadata);
    return new PreparedCandidate(TransactionId.create(), 1, 0, modificationRevision - 1, record);
  }

  private static StoredRecord live(String key, long createRevision, long modificationRevision,
      long version, long lease, String value) {
    byte[] bytes = bytes(value);
    return StoredRecord.live(key(key), new StorageMetadata(createRevision, modificationRevision,
        version, lease, StorageMetadata.State.LIVE, bytes.length), bytes);
  }

  private static StoredRecord liveBytes(String key, long createRevision,
      long modificationRevision, int valueLength) {
    byte[] value = new byte[valueLength];
    return StoredRecord.live(key(key), new StorageMetadata(createRevision, modificationRevision,
        1, 0, StorageMetadata.State.LIVE, value.length), value);
  }

  private static StoredRecord tombstone(String key, long createRevision, long modificationRevision) {
    return StoredRecord.metadataOnly(key(key), new StorageMetadata(createRevision,
        modificationRevision, 2, 0, StorageMetadata.State.TOMBSTONE, 0));
  }

  private static ByteKey key(String value) { return ByteKey.copyOf(bytes(value)); }
  private static ByteKey digest(int value) {
    byte[] bytes = new byte[32];
    java.util.Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }
  private static byte[] bytes(String value) { return value.getBytes(java.nio.charset.StandardCharsets.UTF_8); }
  private static String text(ByteKey key) { return new String(key.bytes(), java.nio.charset.StandardCharsets.UTF_8); }

  private static final class Fixture {
    private final ByteKey profile = digest(3);
    private final TransactionId transactionId = TransactionId.create();
    private final InvocationId invocationId = InvocationId.create();
    private final List<PreparedCandidate> selectedCandidates;
    private final ManifestRoot root;
    private final MemoryManifestRepository repository;
    private final MemoryControl control;
    private final AtomicInteger pinRegistrations = new AtomicInteger();
    private final AtomicInteger pinCloses = new AtomicInteger();
    private final AtomicInteger pinRenews = new AtomicInteger();
    private final AtomicInteger pinCompletions = new AtomicInteger();
    private final CompletableFuture<Void> pinCompletionStarted = new CompletableFuture<>();
    private volatile CompletableFuture<Void> pinRenewal = CompletableFuture.completedFuture(null);
    private volatile CompletableFuture<Void> pinCompletion = CompletableFuture.completedFuture(null);
    private volatile CompletableFuture<Void> pinRegistration = CompletableFuture.completedFuture(null);
    private final ReadViewService views;
    private final MemoryRecordStore physical = new MemoryRecordStore();
    private final MemorySystemRecords worksetRecords = new MemorySystemRecords();

    private Fixture(long baseRevision, List<PreparedCandidate> candidates) {
      selectedCandidates = candidates.stream().map(candidate -> new PreparedCandidate(transactionId,
          1, baseRevision - 1, baseRevision, candidate.record())).toList();
      var references = selectedCandidates.stream()
          .map(candidate -> ManifestCandidateReference.from(candidate, 0)).toList();
      ManifestPage page = ManifestPage.create(transactionId, 0, references);
      long payloadBytes = selectedCandidates.stream()
          .mapToLong(candidate -> candidate.record().metadata().valueLength()).sum();
      root = ManifestRoot.create(transactionId, 1, baseRevision - 1, baseRevision,
          selectedCandidates.size(), payloadBytes, Optional.of(page.reference()));
      repository = new MemoryManifestRepository(root, page, selectedCandidates);
      long baseSequence = baseRevision - 1;
      long revision = baseRevision + 1;
      PublicationSlot slot = new PublicationSlot(transactionId, invocationId, 1, baseSequence,
          baseRevision, baseSequence + 1, revision, true, root.systemIdentity(), root.digest());
      CommitState state = CommitState.restore(profile, Optional.of("fixture-owner"), 1, 10_000,
          2, slot.sequence(), slot.revision(), baseSequence, baseRevision, Optional.of(slot), 0);
      control = new MemoryControl(state);
      ManifestCatalog catalog = new ManifestCatalog(repository);
      ReadViewPinRegistry pinRegistry = (view, scope) -> pinRegistration.thenApply(ignored -> {
        pinRegistrations.incrementAndGet();
        AtomicBoolean closed = new AtomicBoolean();
        ReadViewPinRegistry.Pin pin = new ReadViewPinRegistry.Pin() {
          @Override public long generation() { return 17; }
          @Override public CompletableFuture<Void> renew(RequestScope requestScope) {
            pinRenews.incrementAndGet();
            return pinRenewal;
          }
          @Override public CompletableFuture<Void> complete(RequestScope requestScope) {
            pinCompletions.incrementAndGet();
            pinCompletionStarted.complete(null);
            return pinCompletion.whenComplete((ignored, failure) -> {
              if (failure == null) close();
            });
          }
          @Override public void close() {
            if (closed.compareAndSet(false, true)) pinCloses.incrementAndGet();
          }
        };
        return pin;
      });
      views = new ReadViewService(control, catalog, pinRegistry, profile);
    }

    private ReadViewRecordStore readStore(PinnedReadView pinned, RequestScope scope) {
      return new ReadViewRecordStore(pinned, physical, new ManifestCatalog(repository), ignored -> 0,
          1, scope);
    }

    private BudgetManager.Account account(RequestScope scope, long candidateBytes) throws BackendFailure {
      return budget(scope, candidateBytes).account();
    }

    private void unselectedCandidate(PreparedCandidate candidate) {
      repository.candidates.put(candidate.systemIdentity(), candidate);
    }
  }

  private record BudgetFixture(BudgetManager manager, BudgetManager.Account account) {}

  private static final class MemorySystemRecords implements SystemRecordStore {
    private final Map<ByteKey, ByteKey> rows = new HashMap<>();
    private boolean failNextPage;
    @Override public CompletableFuture<Optional<ByteKey>> read(
        SystemRecordType type, ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(rows.get(identity)));
    }
    @Override public int bucket(SystemRecordType type, ByteKey identity) { return 0; }
    @Override public CompletableFuture<CreateResult> createIfAbsent(
        SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope) {
      ByteKey old = rows.putIfAbsent(identity, value);
      if (identity.length() == 29 && failNextPage) {
        failNextPage = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(old == null ? CreateResult.CREATED
          : old.equals(value) ? CreateResult.ALREADY_PRESENT : CreateResult.CONFLICT);
    }
    @Override public CompletableFuture<Boolean> compareAndSet(SystemRecordType type, ByteKey identity,
        Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }
  }

  private static final class MemoryControl implements CommitControlStore {
    private volatile CommitState state;
    private final AtomicInteger reads = new AtomicInteger();

    private MemoryControl(CommitState state) { this.state = state; }
    @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      reads.incrementAndGet();
      return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1})));
    }
    @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState initialState, RequestScope scope) {
      return CompletableFuture.completedFuture(false);
    }
    @Override public CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      return CompletableFuture.completedFuture(false);
    }
  }

  private static final class MemoryManifestRepository implements ManifestRepository {
    private final ManifestRoot root;
    private final ManifestPage page;
    private final Map<ByteKey, PreparedCandidate> candidates = new HashMap<>();
    private final Map<ByteKey, PreparedEvent> events = new HashMap<>();
    private final AtomicInteger rootReads = new AtomicInteger();
    private final AtomicInteger candidateReads = new AtomicInteger();
    private int blockedRootReadNumber;
    private CompletableFuture<Optional<ManifestRoot>> pendingRootRead;
    private CompletableFuture<Void> pendingRootProducer;
    private ByteKey blockedCandidateIdentity;
    private CompletableFuture<Optional<PreparedCandidate>> pendingCandidateRead;
    private CompletableFuture<Void> pendingCandidateProducer;

    private MemoryManifestRepository(ManifestRoot root, ManifestPage page,
        List<PreparedCandidate> candidates) {
      this.root = root;
      this.page = page;
      candidates.forEach(candidate -> this.candidates.put(candidate.systemIdentity(), candidate));
    }
    @Override public int candidateBucket(PreparedCandidate candidate) { return 0; }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(
        PreparedCandidate candidate, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(
        ManifestPage value, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(
        ManifestDirectoryPage value, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEvent(
        PreparedEvent event, RequestScope scope) {
      PreparedEvent old = events.putIfAbsent(event.systemIdentity(), event);
      return CompletableFuture.completedFuture(old == null ? SystemRecordStore.CreateResult.CREATED
          : old.digest().equals(event.digest()) ? SystemRecordStore.CreateResult.ALREADY_PRESENT
              : SystemRecordStore.CreateResult.CONFLICT);
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(
        EventPage value, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(
        EventDirectoryPage value, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> seal(
        ManifestRoot value, RequestScope scope) { return created(); }
    @Override public CompletableFuture<Optional<ManifestRoot>> readRoot(ByteKey identity, RequestScope scope) {
      int readNumber = rootReads.incrementAndGet();
      if (readNumber == blockedRootReadNumber) {
        pendingRootRead = new CompletableFuture<>();
        pendingRootProducer = new CompletableFuture<>();
        scope.trackPhysicalProducer(pendingRootProducer);
        scope.registerScopedCleanup(() -> pendingRootRead.completeExceptionally(
            new BackendFailure(BackendFailure.Category.CANCELLED)), pendingRootProducer);
        return pendingRootRead;
      }
      return CompletableFuture.completedFuture(identity.equals(root.systemIdentity())
          ? Optional.of(root) : Optional.empty());
    }
    private void blockRootRead(int readNumber) { blockedRootReadNumber = readNumber; }
    private void blockCandidateRead(ByteKey identity) {
      blockedCandidateIdentity = identity;
      pendingCandidateRead = new CompletableFuture<>();
      pendingCandidateProducer = new CompletableFuture<>();
    }
    @Override public CompletableFuture<Optional<PreparedCandidate>> readCandidate(
        ByteKey identity, RequestScope scope) {
      candidateReads.incrementAndGet();
      if (identity.equals(blockedCandidateIdentity)) {
        scope.trackPhysicalProducer(pendingCandidateProducer);
        scope.registerScopedCleanup(() -> pendingCandidateRead.completeExceptionally(
            new BackendFailure(BackendFailure.Category.CANCELLED)), pendingCandidateProducer);
        return pendingCandidateRead;
      }
      return CompletableFuture.completedFuture(Optional.ofNullable(candidates.get(identity)));
    }
    @Override public CompletableFuture<Optional<ManifestPage>> readPage(ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(identity.equals(page.systemIdentity())
          ? Optional.of(page) : Optional.empty());
    }
    @Override public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<PreparedEvent>> readEvent(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.ofNullable(events.get(identity))); }
    @Override public CompletableFuture<Optional<EventPage>> readEventPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    private static CompletableFuture<SystemRecordStore.CreateResult> created() {
      return CompletableFuture.completedFuture(SystemRecordStore.CreateResult.CREATED);
    }
  }

  private static final class MemoryRecordStore implements RecordStore {
    private final Map<ByteKey, List<StoredRecord>> versions = new HashMap<>();
    private final AtomicInteger getCalls = new AtomicInteger();
    private final List<ReadProjection> getProjections = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final AtomicInteger scanCalls = new AtomicInteger();
    private final AtomicBoolean progressOnce = new AtomicBoolean();
    private volatile long lastProgressRevision;
    private final List<Integer> scanBuckets = new java.util.concurrent.CopyOnWriteArrayList<>();
    private java.util.function.ToIntFunction<ByteKey> bucketOf = ignored -> 0;
    private boolean pointUsesScanner;
    private ByteKey blockedKey;
    private CompletableFuture<Optional<StoredRecord>> pendingGet;
    private void blockNextGet(ByteKey key) {
      blockedKey = key;
      pendingGet = new CompletableFuture<>();
    }
    private void add(StoredRecord record) {
      versions.computeIfAbsent(record.key(), ignored -> new ArrayList<>()).add(record);
      versions.get(record.key()).sort(Comparator.comparingLong(
          (StoredRecord value) -> value.metadata().modificationRevision()).reversed());
    }
    @Override public boolean pointReadUsesScanner() { return pointUsesScanner; }
    @Override public CompletableFuture<Optional<StoredRecord>> get(
        ByteKey key, long upper, ReadProjection projection) {
      getCalls.incrementAndGet();
      getProjections.add(projection);
      if (key.equals(blockedKey)) return pendingGet;
      return CompletableFuture.completedFuture(versions.getOrDefault(key, List.of()).stream()
          .filter(record -> record.metadata().modificationRevision() <= upper)
          .findFirst().map(record -> projection == ReadProjection.METADATA && record.hasValue()
              ? StoredRecord.metadataOnly(record.key(), record.metadata()) : record));
    }
    @Override public ScanOperation startScan(ScanRequest request) {
      scanCalls.incrementAndGet();
      scanBuckets.add(request.bucket());
      if (request.physicalProgress().isPresent()) lastProgressRevision = request.revisionUpperBound();
      if (progressOnce.compareAndSet(true, false)) {
        var progress = PhysicalScanProgress.after(request, key("physical-row"));
        var page = new StoragePage(List.of(), false, new StoragePage.Cost(1, 1, 1), Optional.of(progress));
        return new ScanOperation() {
          @Override public CompletionStage<StoragePage> completion() { return CompletableFuture.completedFuture(page); }
          @Override public void close() { }
        };
      }
      Map<ByteKey, StoredRecord> latest = new HashMap<>();
      for (List<StoredRecord> history : versions.values()) {
        for (StoredRecord record : history) {
          if (bucketOf.applyAsInt(record.key()) == request.bucket() && request.range().contains(record.key())
              && request.after().map(after -> request.direction().compare(record.key(), after) > 0).orElse(true)
              && record.metadata().modificationRevision() <= request.revisionUpperBound()) {
            latest.merge(record.key(), record,
                (left, right) -> left.metadata().modificationRevision()
                    > right.metadata().modificationRevision() ? left : right);
          }
        }
      }
      List<StoredRecord> matching = latest.values().stream()
          .sorted((left, right) -> request.direction().compare(left.key(), right.key())).toList();
      List<StoredRecord> projected = new ArrayList<>();
      long bytes = 0;
      boolean exhausted = true;
      for (StoredRecord record : matching) {
        StoredRecord value = request.projection() == ReadProjection.METADATA && record.hasValue()
            ? StoredRecord.metadataOnly(record.key(), record.metadata()) : record;
        if (projected.size() == request.maximumRows()
            || value.retainedBytes() > request.maximumBytes() - bytes) {
          exhausted = false;
          break;
        }
        projected.add(value);
        bytes += value.retainedBytes();
      }
      StoragePage page = new StoragePage(projected, exhausted,
          new StoragePage.Cost(projected.size(), bytes, 1));
      return new ScanOperation() {
        @Override public CompletionStage<StoragePage> completion() {
          return CompletableFuture.completedFuture(page);
        }
        @Override public void close() {}
      };
    }
    @Override public CompletableFuture<Void> put(StoredRecord record) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }
    @Override public CompletableFuture<Boolean> compareAndSet(
        ByteKey key, Optional<StorageMetadata> expected, StoredRecord replacement) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }
    @Override public CompletableFuture<List<BatchWriteResult>> putBatch(List<StoredRecord> records) {
      return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }
  }
}
