package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.ExpiryCandidateRecord;
import io.etcdhbase.core.lease.LeaseEnumerationPointer;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.ScanDirection;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class ManifestPreparationTest {
  private static final long OWNER_EPOCH = 1;
  private static final long BASE_SEQUENCE = 0;
  private static final long BASE_REVISION = 1;

  @Test
  void rejectsEventRecordsThatCannotBeDecoded() {
    TransactionId transactionId = TransactionId.create();
    ByteKey key = ByteKey.copyOf(new byte[] {'k'});
    byte[] value = {1};
    StoredRecord current = StoredRecord.live(key,
        new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, value.length), value);
    StoredRecord emptyKeyCurrent = StoredRecord.live(ByteKey.empty(),
        new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, value.length), value);
    StoredRecord emptyValueCurrent = StoredRecord.live(key,
        new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, 0), new byte[0]);
    StoredRecord metadataOnlyPrevious = StoredRecord.metadataOnly(key,
        new StorageMetadata(1, 1, 1, 0, StorageMetadata.State.LIVE, value.length));
    byte[] oversizedValue = new byte[PreparedCandidate.MAX_VALUE_BYTES + 1];
    StoredRecord oversizedCurrent = StoredRecord.live(key,
        new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, oversizedValue.length),
        oversizedValue);
    StoredRecord oversizedPrevious = StoredRecord.live(key,
        new StorageMetadata(1, 1, 1, 0, StorageMetadata.State.LIVE, oversizedValue.length),
        oversizedValue);

    assertThrows(IllegalArgumentException.class, () -> new PreparedEvent(transactionId,
        OWNER_EPOCH, BASE_SEQUENCE, BASE_REVISION, 0, PreparedEvent.Kind.PUT, current,
        Optional.of(metadataOnlyPrevious)));
    assertThrows(IllegalArgumentException.class, () -> new PreparedEvent(transactionId,
        OWNER_EPOCH, BASE_SEQUENCE, BASE_REVISION, 0, PreparedEvent.Kind.PUT, oversizedCurrent,
        Optional.empty()));
    assertThrows(IllegalArgumentException.class, () -> new PreparedEvent(transactionId,
        OWNER_EPOCH, BASE_SEQUENCE, BASE_REVISION, 0, PreparedEvent.Kind.PUT, current,
        Optional.of(oversizedPrevious)));
    assertThrows(IllegalArgumentException.class, () -> new PreparedEvent(transactionId,
        OWNER_EPOCH, BASE_SEQUENCE, BASE_REVISION, 0, PreparedEvent.Kind.PUT, emptyKeyCurrent,
        Optional.empty()));
    assertDoesNotThrow(() -> new PreparedEvent(transactionId, OWNER_EPOCH, BASE_SEQUENCE,
        BASE_REVISION, 0, PreparedEvent.Kind.PUT, emptyValueCurrent, Optional.empty()));

    MemoryRepository repository = new MemoryRepository();
    try (RequestScope scope = scope()) {
      assertThrows(IllegalArgumentException.class, () -> prepare(repository, transactionId,
          List.of(candidate(transactionId, new byte[] {'a'}, value)),
          List.of(new PreparedEvent(transactionId, OWNER_EPOCH, BASE_SEQUENCE, BASE_REVISION, 0,
              PreparedEvent.Kind.PUT, emptyKeyCurrent, Optional.empty())),
          new ManifestPreparation.Limits(1, 1), scope));
      assertEquals(0, repository.writeCount);
    }
  }

  @Test
  void preparesLargeLazyCandidatePlanWithoutRecursiveStackGrowth() throws Exception {
    int candidateCount = 5_000;
    TransactionId transactionId = TransactionId.create();
    MemoryRepository repository = new MemoryRepository();
    var candidates = IntStream.range(0, candidateCount)
        .mapToObj(index -> candidate(transactionId, sequenceKey(index), new byte[] {(byte) index}))
        .iterator();

    try (RequestScope scope = scope()) {
      ManifestRoot root = assertDoesNotThrow(() -> join(new ManifestPreparation(repository,
          new ManifestPreparation.Limits(candidateCount, candidateCount, 0, 0))
          .prepare(transactionId, OWNER_EPOCH, BASE_SEQUENCE, BASE_REVISION, candidates,
              java.util.Collections.emptyIterator(), scope)));

      assertEquals(candidateCount, root.candidateCount());
      assertTrue(root.candidateDirectory().orElseThrow().level() >= 2);
      assertTrue(repository.directories.size() >= 2);
      assertEquals(0, root.eventCount());
      var limits = new BudgetManager.Limits(2, 2, 2, 2, 2, 1_000_000, 16L << 20);
      var budget = new BudgetManager(limits, Map.of("public", limits), limits);
      var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 8192, 8192);
      try (var account = budget.admit("public", scope, reservation)) {
        var catalog = new ManifestCatalog(repository);
        try (var first = catalog.candidatesInRange(root, 0, KeyRange.all(), Optional.empty(), 3,
            8192, scope, () -> false, account, ScanDirection.DESCENDING).join()) {
          assertEquals(List.of(ByteKey.copyOf(sequenceKey(4999)), ByteKey.copyOf(sequenceKey(4998)),
              ByteKey.copyOf(sequenceKey(4997))), first.candidates().stream().map(c -> c.record().key()).toList());
          assertFalse(first.exhausted());
        }
        try (var second = catalog.candidatesInRange(root, 0, KeyRange.all(),
            Optional.of(ByteKey.copyOf(sequenceKey(4997))), 2, 8192, scope, () -> false, account,
            ScanDirection.DESCENDING).join()) {
          assertEquals(List.of(ByteKey.copyOf(sequenceKey(4996)), ByteKey.copyOf(sequenceKey(4995))),
              second.candidates().stream().map(c -> c.record().key()).toList());
        }
      }
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  @Test
  void proofTraversalChargesMetadataAndFailsBeforeExceedingItsOwnPageLimit() throws Exception {
    int count = 300;
    var transactionId = TransactionId.create();
    var repository = new MemoryRepository();
    var candidates = IntStream.range(0, count)
        .mapToObj(index -> candidate(transactionId, sequenceKey(index), new byte[] {(byte) index}))
        .iterator();
    var limits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1_000_000, 1024L << 20);
    var budget = new BudgetManager(limits, Map.of("public", limits), limits);
    var proofReservation = new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8192);
    var directReservation = new BudgetManager.Reservation(8192, 8192, 8192, 8192, 8192);
    try (var preparationScope = scope(); var proofScope = scope(); var directScope = scope();
         var proofAccount = budget.admit("public", proofScope, proofReservation, proofReadLimits());
         var directAccount = budget.admit("public", directScope, directReservation)) {
      var root = new ManifestPreparation(repository, new ManifestPreparation.Limits(count, count, 0, 0))
          .prepare(transactionId, OWNER_EPOCH, BASE_SEQUENCE, BASE_REVISION, candidates,
              java.util.Collections.emptyIterator(), preparationScope).join();
      var catalog = new ManifestCatalog(repository);
      var point = KeyRange.point(ByteKey.copyOf(sequenceKey(100)));
      var insufficient = new ManifestCatalog.ProofReadBudget(proofScope, proofAccount, 1, 4096L << 20);
      var failure = assertThrows(CompletionException.class,
          () -> catalog.candidatesForProof(root, 0, point, 8192, insufficient).join());
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, ((BackendFailure) failure.getCause()).category());
      assertEquals(1, insufficient.pages());
      assertTrue(directAccount.isActive(), "proof exhaustion must not exhaust the direct read account");
      assertEquals(0, budget.snapshot().allocatedBytes());

      var proof = new ManifestCatalog.ProofReadBudget(proofScope, proofAccount, 8, 4096L << 20);
      try (var overlap = catalog.candidatesForProof(root, 0, point, 8192, proof).join()) {
        assertEquals(1, overlap.candidates().size());
        assertEquals(ByteKey.copyOf(sequenceKey(100)), overlap.candidates().getFirst().record().key());
      }
      assertTrue(proof.pages() >= 2);
      assertTrue(proof.bytes() >= 2 * ManifestCatalog.PROOF_SYSTEM_RECORD_ENVELOPE);
      assertEquals(0, budget.snapshot().allocatedBytes());

      var absent = KeyRange.point(ByteKey.copyOf(new byte[] {0, 0, 0, 100, 0}));
      var absenceBudget = new ManifestCatalog.ProofReadBudget(proofScope, proofAccount, 8, 4096L << 20);
      try (var noOverlap = catalog.candidatesForProof(root, 0, absent, 8192, absenceBudget).join()) {
        assertTrue(noOverlap.candidates().isEmpty());
        assertTrue(noOverlap.exhausted());
      }
      assertTrue(absenceBudget.pages() > 0);
      assertEquals(0, budget.snapshot().allocatedBytes());

      try (var cancelledScope = scope();
           var cancelledAccount = budget.admit("public", cancelledScope, proofReservation, proofReadLimits())) {
        var producer = new CompletableFuture<Void>();
        cancelledScope.trackPhysicalProducer(producer);
        repository.pendingDirectoryRead = new CompletableFuture<>();
        var cancelledProof = new ManifestCatalog.ProofReadBudget(cancelledScope, cancelledAccount, 8, 4096L << 20);
        var pending = catalog.candidatesForProof(root, 0, point, 8192, cancelledProof);
        assertEquals(ManifestCatalog.PROOF_SYSTEM_RECORD_ENVELOPE, budget.snapshot().allocatedBytes());
        cancelledScope.cancel();
        repository.pendingDirectoryRead.complete(Optional.of(
            repository.directories.get(root.candidateDirectory().orElseThrow().identity())));
        assertThrows(CompletionException.class, pending::join);
        assertEquals(ManifestCatalog.PROOF_SYSTEM_RECORD_ENVELOPE, budget.snapshot().allocatedBytes());
        assertFalse(cancelledScope.cleanupCompletion().toCompletableFuture().isDone());
        producer.complete(null);
        cancelledScope.cleanupCompletion().toCompletableFuture().join();
        assertEquals(0, budget.snapshot().allocatedBytes());
      }
    }
  }

  @Test
  void coverageProofRequiresContiguousDecisionsAndAuthenticCompleteManifests() throws Exception {
    var limits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1_000_000, 1024L << 20);
    var manager = new BudgetManager(limits, Map.of("public", limits), limits);
    var reservation = new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8192);
    try (var preparationScope = scope(); var proofScope = scope(); var directScope = scope();
         var proofAccount = manager.admit("public", proofScope, reservation, proofReadLimits());
         var directAccount = manager.admit("public", directScope,
             new BudgetManager.Reservation(8192, 8192, 8192, 8192, 8192))) {
      var fixture = new CoverageProofFixture(manager, reservation, preparationScope, proofScope,
          proofAccount, directAccount);
      fixture.verifyBasicCoverage();
      fixture.verifyIndependentBudgetsAndCancellation();
      fixture.verifyMissingEvidence();
      fixture.verifyMultiDecisionProof();
      fixture.verifyIdentityBoundaries();
      assertEquals(0, fixture.manager.snapshot().allocatedBytes());
    }
  }

  private static final class CoverageProofFixture {
    final MemoryRepository repository = new MemoryRepository();
    final Map<Long, CommitDecision> storedDecisions = new HashMap<>();
    final BudgetManager manager;
    final BudgetManager.Reservation proofReservation;
    final RequestScope preparationScope;
    final RequestScope proofScope;
    final BudgetManager.Account proofAccount;
    final BudgetManager.Account directAccount;
    final ByteKey profile = ByteKey.copyOf(new byte[32]);
    final ByteKey authorization = ByteKey.copyOf(new byte[32]);
    final ReadView previous = new ReadView(profile, authorization, 0, 0, 1, 1, 0, Optional.empty(), Optional.empty(), 0);
    final ReadView current = new ReadView(profile, authorization, 1, 1, 2, 2, 0, Optional.empty(), Optional.empty(), 0);
    final ReadView later = new ReadView(profile, authorization, 2, 2, 3, 3, 0, Optional.empty(), Optional.empty(), 0);
    final KeyRange pointA = KeyRange.point(ByteKey.copyOf(new byte[] {'a'}));
    final KeyRange pointZ = KeyRange.point(ByteKey.copyOf(new byte[] {'z'}));
    final ManifestRoot root;
    final CommitDecision decision;
    final ReadView pending;
    final UnchangedRangeProof prover;

    CoverageProofFixture(BudgetManager manager, BudgetManager.Reservation proofReservation,
        RequestScope preparationScope, RequestScope proofScope,
        BudgetManager.Account proofAccount, BudgetManager.Account directAccount) {
      this.manager = manager;
      this.proofReservation = proofReservation;
      this.preparationScope = preparationScope;
      this.proofScope = proofScope;
      this.proofAccount = proofAccount;
      this.directAccount = directAccount;
      var transaction = TransactionId.create();
      root = new ManifestPreparation(repository, new ManifestPreparation.Limits(1, 1, 0, 0))
          .prepare(transaction, OWNER_EPOCH, 0, 1,
              List.of(candidate(transaction, new byte[] {'a'}, new byte[] {1})).iterator(),
              java.util.Collections.emptyIterator(), preparationScope).join();
      decision = new CommitDecision(transaction, InvocationId.create(), 1, 1, 2,
          root.systemIdentity(), root.digest(), true);
      storedDecisions.put(1L, decision);
      pending = new ReadView(profile, authorization, 1, 0, 2, 1, 0,
          Optional.of(decision), Optional.of(root), 0);
      CommitDecisionRepository decisions = new CommitDecisionRepository() {
        @Override public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope) {
          return CompletableFuture.completedFuture(Optional.ofNullable(storedDecisions.get(sequence)));
        }
        @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
            CommitDecision value, RequestScope scope) { throw new UnsupportedOperationException(); }
      };
      prover = new UnchangedRangeProof(decisions, new ManifestCatalog(repository), 1, 8, 8192, ignored -> 0);
    }

    void verifyBasicCoverage() {
      assertEquals(UnchangedRangeProof.Verdict.UNCHANGED,
          prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), current, profile, profile, pointZ, proofBudget(proofScope, proofAccount)).join());
      assertEquals(UnchangedRangeProof.Verdict.CHANGED,
          prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), current, profile, profile, pointA, proofBudget(proofScope, proofAccount)).join());
      assertEquals(UnchangedRangeProof.Verdict.CHANGED,
          prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), pending, profile, profile, pointA, proofBudget(proofScope, proofAccount)).join());
      assertEquals(UnchangedRangeProof.Verdict.INCONCLUSIVE,
          prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), current, profile, profile, pointZ,
              new ManifestCatalog.ProofReadBudget(proofScope, proofAccount, 1, 4096L << 20)).join());
      assertTrue(directAccount.isActive());
    }

    void verifyIndependentBudgetsAndCancellation() throws Exception {
      verifyCancelledProofWaitsForProducer(manager, proofReservation, repository, previous, current, profile, pointZ, decision);
      try (var limitedScope = scope();
           var limitedAccount = manager.admit("public", limitedScope, proofReservation,
               new BudgetManager.ReadLimits(100, ManifestCatalog.PROOF_SYSTEM_RECORD_ENVELOPE,
                   100, 100, 100))) {
        assertEquals(UnchangedRangeProof.Verdict.INCONCLUSIVE,
            prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), current, profile, profile, pointZ, proofBudget(limitedScope, limitedAccount)).join());
        assertFalse(limitedAccount.isActive());
        assertTrue(directAccount.isActive(), "proof read exhaustion must not consume the direct account");
      }
    }

    void verifyMissingEvidence() {
      storedDecisions.clear();
      assertEquals(UnchangedRangeProof.Verdict.INCONCLUSIVE,
          prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), current, profile, profile, pointZ, proofBudget(proofScope, proofAccount)).join());
      // The selected pending decision comes from the captured authoritative barrier, not the archive.
      assertEquals(UnchangedRangeProof.Verdict.UNCHANGED,
          prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), pending, profile, profile, pointZ, proofBudget(proofScope, proofAccount)).join());
      storedDecisions.put(1L, decision);
      repository.roots.remove(root.systemIdentity());
      assertEquals(UnchangedRangeProof.Verdict.INCONCLUSIVE,
          prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), current, profile, profile, pointZ, proofBudget(proofScope, proofAccount)).join());
      repository.roots.put(root.systemIdentity(), root);
      assertEquals(UnchangedRangeProof.Verdict.INCONCLUSIVE,
          prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), later, profile, profile, pointZ, proofBudget(proofScope, proofAccount)).join());
    }

    void verifyMultiDecisionProof() {
      var secondTransaction = TransactionId.create();
      var secondRecord = StoredRecord.live(ByteKey.copyOf(new byte[] {'b'}),
          new StorageMetadata(3, 3, 1, 0, StorageMetadata.State.LIVE, 1), new byte[] {2});
      var secondCandidate = new PreparedCandidate(secondTransaction, OWNER_EPOCH, 1, 2, secondRecord);
      var secondRoot = new ManifestPreparation(repository, new ManifestPreparation.Limits(1, 1, 0, 0))
          .prepare(secondTransaction, OWNER_EPOCH, 1, 2, List.of(secondCandidate).iterator(),
              java.util.Collections.emptyIterator(), preparationScope).join();
      var secondDecision = new CommitDecision(secondTransaction, InvocationId.create(), 2, 2, 3,
          secondRoot.systemIdentity(), secondRoot.digest(), true);
      storedDecisions.put(2L, secondDecision);
      var pointB = KeyRange.point(ByteKey.copyOf(new byte[] {'b'}));
      assertEquals(UnchangedRangeProof.Verdict.UNCHANGED,
          prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), later, profile, profile, pointZ, proofBudget(proofScope, proofAccount)).join());
      assertEquals(UnchangedRangeProof.Verdict.CHANGED,
          prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), later, profile, profile, pointB, proofBudget(proofScope, proofAccount)).join());
      storedDecisions.remove(1L);
      assertEquals(UnchangedRangeProof.Verdict.INCONCLUSIVE,
          prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), later, profile, profile, pointZ, proofBudget(proofScope, proofAccount)).join());
      storedDecisions.put(1L, decision);
      storedDecisions.put(2L, new CommitDecision(secondTransaction, secondDecision.invocationId(), 2, 2, 3,
          secondRoot.systemIdentity(), ByteKey.copyOf(new byte[32]), true));
      assertEquals(UnchangedRangeProof.Verdict.INCONCLUSIVE,
          prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), later, profile, profile, pointZ, proofBudget(proofScope, proofAccount)).join());
      storedDecisions.put(2L, new CommitDecision(secondTransaction, secondDecision.invocationId(), 2, 2, 2,
          secondRoot.systemIdentity(), secondRoot.digest(), false));
      var metadataOnly = new ReadView(profile, authorization, 2, 2, 2, 2, 0,
          Optional.empty(), Optional.empty(), 0);
      assertEquals(UnchangedRangeProof.Verdict.INCONCLUSIVE,
          prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), metadataOnly, profile, profile, pointZ, proofBudget(proofScope, proofAccount)).join());
    }

    void verifyIdentityBoundaries() {
      verifyProofIdentityBoundaries(prover, previous, current, profile, authorization, pointZ, proofScope, proofAccount);
    }

  }

  private static void verifyProofIdentityBoundaries(UnchangedRangeProof prover, ReadView previous,
      ReadView current, ByteKey profile, ByteKey authorization, KeyRange pointZ,
      RequestScope proofScope, BudgetManager.Account proofAccount) {
    var compacted = new ReadView(profile, authorization, 1, 1, 2, 2, 1,
        Optional.empty(), Optional.empty(), 0);
    assertEquals(UnchangedRangeProof.Verdict.INCONCLUSIVE,
        prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), compacted, profile, profile, pointZ, proofBudget(proofScope, proofAccount)).join());
    byte[] other = new byte[32];
    other[0] = 1;
    var otherIdentity = new ReadView(profile, ByteKey.copyOf(other), 1, 1, 2, 2, 0,
        Optional.empty(), Optional.empty(), 0);
    assertEquals(UnchangedRangeProof.Verdict.INCONCLUSIVE,
        prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), otherIdentity, profile, profile, pointZ, proofBudget(proofScope, proofAccount)).join());
    assertEquals(UnchangedRangeProof.Verdict.INCONCLUSIVE,
        prover.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), current, profile, ByteKey.copyOf(other), pointZ, proofBudget(proofScope, proofAccount)).join());
    assertEquals(UnchangedRangeProof.Verdict.INCONCLUSIVE,
        prover.prove(7, 8, UnchangedRangeProof.Baseline.capture(previous), current, profile, profile, pointZ, proofBudget(proofScope, proofAccount)).join());
  }

  private static void verifyCancelledProofWaitsForProducer(BudgetManager manager,
      BudgetManager.Reservation proofReservation, MemoryRepository repository, ReadView previous,
      ReadView current, ByteKey profile, KeyRange pointZ, CommitDecision decision) throws Exception {
    try (var cancelledScope = scope();
         var cancelledAccount = manager.admit("public", cancelledScope, proofReservation, proofReadLimits())) {
      var producer = new CompletableFuture<Void>();
      var cancelled = new CompletableFuture<Void>();
      cancelledScope.trackPhysicalProducer(producer);
      cancelledScope.registerCleanup(() -> cancelled.complete(null));
      var delayed = new CompletableFuture<Optional<CommitDecision>>();
      var delayedDecisions = new CommitDecisionRepository() {
        @Override public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope) {
          return delayed;
        }
        @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
            CommitDecision value, RequestScope scope) { throw new UnsupportedOperationException(); }
      };
      var delayedProver = new UnchangedRangeProof(delayedDecisions, new ManifestCatalog(repository), 1, 8, 8192, ignored -> 0);
      var result = delayedProver.prove(7, 7, UnchangedRangeProof.Baseline.capture(previous), current, profile, profile, pointZ,
          proofBudget(cancelledScope, cancelledAccount));
      assertTrue(result.cancel(true));
      cancelled.get(5, java.util.concurrent.TimeUnit.SECONDS);
      delayed.complete(Optional.of(decision));
      assertEquals(ManifestCatalog.PROOF_SYSTEM_RECORD_ENVELOPE, manager.snapshot().allocatedBytes());
      assertFalse(cancelledScope.cleanupCompletion().toCompletableFuture().isDone());
      producer.complete(null);
      cancelledScope.cleanupCompletion().toCompletableFuture().join();
      assertEquals(0, manager.snapshot().allocatedBytes());
    }
  }

  private static ManifestCatalog.ProofReadBudget proofBudget(RequestScope scope, BudgetManager.Account account) {
    return new ManifestCatalog.ProofReadBudget(scope, account, 32, 4096L << 20);
  }

  private static BudgetManager.ReadLimits proofReadLimits() {
    return new BudgetManager.ReadLimits(1_000_000, 8L << 30, 100_000, 65_536, 65_536);
  }

  @Test
  void preparesLargeLazyEventPlanWithoutRecursiveStackGrowth() {
    int eventCount = 8_193;
    TransactionId transactionId = TransactionId.create();
    MemoryRepository repository = new MemoryRepository();
    var events = IntStream.range(0, eventCount)
        .mapToObj(index -> event(transactionId, index, sequenceKey(index), new byte[] {(byte) index}))
        .iterator();

    try (RequestScope scope = scope()) {
      ManifestRoot root = assertDoesNotThrow(() -> join(new ManifestPreparation(repository,
          new ManifestPreparation.Limits(0, 0, eventCount, ManifestRoot.MAX_EVENT_BYTES))
          .prepare(transactionId, OWNER_EPOCH, BASE_SEQUENCE, BASE_REVISION,
              java.util.Collections.emptyIterator(), events, scope)));

      assertEquals(eventCount, root.eventCount());
      assertTrue(root.eventDirectory().orElseThrow().level() >= 2);
      assertTrue(repository.eventDirectories.size() >= 2);
      assertEquals(0, root.candidateCount());
    }
  }

  @Test
  void completesDelayedCandidateAndEventWritesSequentially() {
    TransactionId transactionId = TransactionId.create();
    MemoryRepository repository = new MemoryRepository();
    repository.deferCreateResults = true;
    List<PreparedCandidate> candidates = List.of(
        candidate(transactionId, new byte[] {'a'}, new byte[] {1}),
        candidate(transactionId, new byte[] {'b'}, new byte[] {2}));
    List<PreparedEvent> events = List.of(
        event(transactionId, 0, new byte[] {'a'}, new byte[] {1}),
        event(transactionId, 1, new byte[] {'b'}, new byte[] {2}));

    try (RequestScope scope = scope()) {
      CompletableFuture<ManifestRoot> prepared = new ManifestPreparation(repository,
          new ManifestPreparation.Limits(2, 2, 2, 1_000))
          .prepare(transactionId, OWNER_EPOCH, BASE_SEQUENCE, BASE_REVISION,
              candidates.iterator(), events.iterator(), scope);

      int completedWrites = 0;
      while (!prepared.isDone()) {
        assertEquals(1, repository.pendingCreates.size());
        assertTrue(repository.completeNextCreate());
        completedWrites++;
      }

      ManifestRoot root = join(prepared);
      assertEquals(2, root.candidateCount());
      assertEquals(2, root.eventCount());
      assertEquals(1, repository.maximumPendingCreates);
      assertEquals(repository.writeCount, completedWrites);
      assertEquals(1, repository.sealCalls);
    }
  }

  @Test
  void closedScopeStopsWriterAfterDelayedCreateWithoutSealing() {
    TransactionId transactionId = TransactionId.create();
    MemoryRepository repository = new MemoryRepository();
    repository.deferCreateResults = true;
    List<PreparedCandidate> candidates = List.of(
        candidate(transactionId, new byte[] {'a'}, new byte[] {1}),
        candidate(transactionId, new byte[] {'b'}, new byte[] {2}));

    try (RequestScope scope = scope()) {
      CompletableFuture<ManifestRoot> prepared = new ManifestPreparation(repository,
          new ManifestPreparation.Limits(2, 2))
          .prepare(transactionId, OWNER_EPOCH, BASE_SEQUENCE, BASE_REVISION, candidates.iterator(), scope);
      assertEquals(1, repository.candidateWriteCount);
      scope.close();
      assertTrue(repository.completeNextCreate());

      assertThrows(CompletionException.class, prepared::join);
      assertEquals(1, repository.candidateWriteCount);
      assertEquals(0, repository.sealCalls);
      assertTrue(repository.roots.isEmpty());
    }
  }

  @Test
  void preparesMultipleCandidateAndEventPagesAndStreamsThemInOrder() {
    TransactionId transactionId = TransactionId.create();
    MemoryRepository repository = new MemoryRepository();
    List<PreparedCandidate> candidates = new ArrayList<>();
    List<PreparedEvent> events = new ArrayList<>();
    for (int index = 0; index < 130; index++) {
      byte[] key = numberedKey(index);
      candidates.add(candidate(transactionId, key, new byte[] {(byte) index}));
      events.add(event(transactionId, index, key, new byte[] {(byte) index}));
    }

    try (RequestScope scope = scope()) {
      ManifestRoot root = prepare(repository, transactionId, candidates, events,
          new ManifestPreparation.Limits(130, 130, 130, 1_000_000), scope);

      List<ByteKey> candidateKeys = new ArrayList<>();
      ManifestCatalog.Summary candidateSummary = join(
          new ManifestCatalog(repository).forEachCandidate(root, scope, value -> {
            candidateKeys.add(value.record().key());
            return CompletableFuture.completedFuture(null);
          }));
      List<Long> ordinals = new ArrayList<>();
      ManifestCatalog.Summary eventSummary = join(
          new ManifestCatalog(repository).forEachEvent(root, scope, value -> {
            ordinals.add(value.ordinal());
            return CompletableFuture.completedFuture(null);
          }));

      assertEquals(130, candidateSummary.count());
      assertEquals(130, eventSummary.count());
      assertEquals(2, repository.pages.size());
      assertEquals(3, repository.eventPages.size());
      assertEquals(1, repository.directories.size());
      assertEquals(1, repository.eventDirectories.size());
      assertEquals(candidates.stream().map(value -> value.record().key()).toList(), candidateKeys);
      assertEquals(java.util.stream.LongStream.range(0, 130).boxed().toList(), ordinals);
    }
  }

  @Test
  void catalogTraversesNestedCandidateAndEventDirectories() {
    TransactionId transactionId = TransactionId.create();
    MemoryRepository repository = new MemoryRepository();
    PreparedCandidate first = candidate(transactionId, new byte[] {'a'}, new byte[] {1});
    PreparedCandidate second = candidate(transactionId, new byte[] {'b'}, new byte[] {2});
    repository.put(first);
    repository.put(second);
    ManifestPage firstPage = ManifestPage.create(transactionId, 0,
        List.of(ManifestCandidateReference.from(first, 0)));
    ManifestPage secondPage = ManifestPage.create(transactionId, 1,
        List.of(ManifestCandidateReference.from(second, 0)));
    repository.put(firstPage);
    repository.put(secondPage);
    ManifestDirectoryPage firstDirectory = ManifestDirectoryPage.create(transactionId, 0, 1,
        List.of(firstPage.reference()));
    ManifestDirectoryPage secondDirectory = ManifestDirectoryPage.create(transactionId, 1, 1,
        List.of(secondPage.reference()));
    ManifestDirectoryPage candidateRoot = ManifestDirectoryPage.create(transactionId, 2, 2,
        List.of(firstDirectory.reference(), secondDirectory.reference()));
    repository.put(firstDirectory);
    repository.put(secondDirectory);
    repository.put(candidateRoot);

    PreparedEvent firstEvent = event(transactionId, 0, new byte[] {'a'}, new byte[] {1});
    PreparedEvent secondEvent = event(transactionId, 1, new byte[] {'b'}, new byte[] {2});
    repository.put(firstEvent);
    repository.put(secondEvent);
    EventPage firstEventPage = EventPage.create(transactionId, 0,
        List.of(PreparedEventReference.from(firstEvent)));
    EventPage secondEventPage = EventPage.create(transactionId, 1,
        List.of(PreparedEventReference.from(secondEvent)));
    EventDirectoryPage firstEventDirectory = EventDirectoryPage.create(transactionId, 0, 1,
        List.of(firstEventPage.reference()));
    EventDirectoryPage secondEventDirectory = EventDirectoryPage.create(transactionId, 1, 1,
        List.of(secondEventPage.reference()));
    EventDirectoryPage eventRoot = EventDirectoryPage.create(transactionId, 2, 2,
        List.of(firstEventDirectory.reference(), secondEventDirectory.reference()));
    repository.put(firstEventPage);
    repository.put(secondEventPage);
    repository.put(firstEventDirectory);
    repository.put(secondEventDirectory);
    repository.put(eventRoot);
    ManifestRoot root = ManifestRoot.create(transactionId, OWNER_EPOCH, BASE_SEQUENCE,
        BASE_REVISION, 2, 2, Optional.of(candidateRoot.reference()), 2,
        firstEvent.retainedBytes() + secondEvent.retainedBytes(), Optional.of(eventRoot.reference()));

    try (RequestScope scope = scope()) {
      List<ByteKey> keys = new ArrayList<>();
      List<Long> ordinals = new ArrayList<>();
      ManifestCatalog catalog = new ManifestCatalog(repository);
      assertEquals(2, join(catalog.forEachCandidate(root, scope, value -> {
        keys.add(value.record().key());
        return CompletableFuture.completedFuture(null);
      })).count());
      assertEquals(2, join(catalog.forEachEvent(root, scope, value -> {
        ordinals.add(value.ordinal());
        return CompletableFuture.completedFuture(null);
      })).count());
      assertEquals(List.of(ByteKey.copyOf(new byte[] {'a'}), ByteKey.copyOf(new byte[] {'b'})), keys);
      assertEquals(List.of(0L, 1L), ordinals);
    }
  }

  @Test
  void candidateIdentitiesIsolateTransactionsWithTheSameLogicalKey() {
    MemoryRepository repository = new MemoryRepository();
    TransactionId firstTransaction = TransactionId.create();
    TransactionId secondTransaction = TransactionId.create();
    PreparedCandidate first = candidate(firstTransaction, new byte[] {'k'}, new byte[] {1});
    PreparedCandidate second = candidate(secondTransaction, new byte[] {'k'}, new byte[] {2});

    try (RequestScope scope = scope()) {
      ManifestRoot firstRoot = prepare(repository, firstTransaction, List.of(first), List.of(),
          new ManifestPreparation.Limits(1, 1), scope);
      ManifestRoot secondRoot = prepare(repository, secondTransaction, List.of(second), List.of(),
          new ManifestPreparation.Limits(1, 1), scope);
      assertFalse(first.systemIdentity().equals(second.systemIdentity()));
      assertEquals(first, repository.candidates.get(first.systemIdentity()));
      assertEquals(second, repository.candidates.get(second.systemIdentity()));
      assertFalse(firstRoot.systemIdentity().equals(secondRoot.systemIdentity()));
    }
  }

  @Test
  void rejectsOrderAndBudgetsBeforeWritingTheRejectedRecord() {
    TransactionId transactionId = TransactionId.create();
    try (RequestScope scope = scope()) {
      MemoryRepository unordered = new MemoryRepository();
      var orderFailure = assertThrows(CompletionException.class, () -> prepare(unordered,
          transactionId,
          List.of(candidate(transactionId, new byte[] {'b'}, new byte[] {1}),
              candidate(transactionId, new byte[] {'a'}, new byte[] {2})), List.of(),
          new ManifestPreparation.Limits(2, 2), scope));
      assertTrue(orderFailure.getCause() instanceof IllegalArgumentException);
      assertEquals(1, unordered.candidates.size());
      assertTrue(unordered.roots.isEmpty());

      MemoryRepository candidateBudget = new MemoryRepository();
      assertThrows(CompletionException.class, () -> prepare(candidateBudget, transactionId,
          List.of(candidate(transactionId, new byte[] {'a'}, new byte[] {1})), List.of(),
          new ManifestPreparation.Limits(0, 0), scope));
      assertEquals(0, candidateBudget.writeCount);
      assertTrue(candidateBudget.roots.isEmpty());

      MemoryRepository eventBudget = new MemoryRepository();
      assertThrows(CompletionException.class, () -> prepare(eventBudget, transactionId,
          List.of(), List.of(event(transactionId, 0, new byte[] {'a'}, new byte[] {1})),
          new ManifestPreparation.Limits(0, 0, 0, 0), scope));
      assertEquals(0, eventBudget.writeCount);
      assertTrue(eventBudget.roots.isEmpty());

      MemoryRepository rowLimit = new MemoryRepository();
      rowLimit.maximumLogicalKeyLength = 1;
      assertThrows(CompletionException.class, () -> prepare(rowLimit, transactionId,
          List.of(candidate(transactionId, new byte[] {'a', 'b'}, new byte[] {1})), List.of(),
          new ManifestPreparation.Limits(1, 1), scope));
      assertEquals(0, rowLimit.writeCount);
      assertTrue(rowLimit.roots.isEmpty());

      MemoryRepository unorderedEvents = new MemoryRepository();
      assertThrows(CompletionException.class, () -> prepare(unorderedEvents, transactionId,
          List.of(), List.of(event(transactionId, 1, new byte[] {'a'}, new byte[] {1})),
          new ManifestPreparation.Limits(0, 0, 2, 1_000), scope));
      assertEquals(0, unorderedEvents.writeCount);
      assertTrue(unorderedEvents.roots.isEmpty());
    }
  }

  @Test
  void partialCandidateWriteFailureNeverSealsTheManifest() {
    TransactionId transactionId = TransactionId.create();
    MemoryRepository repository = new MemoryRepository();
    repository.failCandidateWriteNumber = 2;
    try (RequestScope scope = scope()) {
      assertThrows(CompletionException.class, () -> prepare(repository, transactionId,
          List.of(candidate(transactionId, new byte[] {'a'}, new byte[] {1}),
              candidate(transactionId, new byte[] {'b'}, new byte[] {2})), List.of(),
          new ManifestPreparation.Limits(2, 2), scope));
      assertEquals(1, repository.candidates.size());
      assertTrue(repository.roots.isEmpty());
      assertEquals(0, repository.sealCalls);
    }
  }

  @Test
  void businessRootRequiresDurableMatchingResultBeforeSealAndBeforePublication() {
    MemoryRepository repository = new MemoryRepository();
    SingleResultRepository resultRepository = new SingleResultRepository();
    ResultCatalog results = new ResultCatalog(resultRepository);
    InvocationBinding invocation = InvocationBinding.begin(InvocationId.create(),
        ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1);
    ResultIdentity identity = ResultIdentity.from(invocation);
    ResultPage page = new ResultPage(identity, 0, new byte[] {1});
    ResultDirectory directory = ResultDirectory.create(identity, List.of(ResultPageReference.from(page)));
    var preparation = new ManifestPreparation(repository, new ManifestPreparation.Limits(0, 0, 0, 0));

    try (RequestScope scope = scope()) {
      InvocationBinding other = InvocationBinding.begin(InvocationId.create(),
          ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]), invocation.transactionId(), 1, 0, 1);
      assertThrows(CompletionException.class, () -> preparation.prepareWithResult(other,
          List.<PreparedCandidate>of().iterator(), List.<PreparedEvent>of().iterator(),
          directory.reference(), results, () -> CompletableFuture.completedFuture(null), scope).join());
      assertEquals(0, repository.writeCount);
      assertThrows(CompletionException.class, () -> preparation.prepareWithResult(invocation,
          List.<PreparedCandidate>of().iterator(), List.<PreparedEvent>of().iterator(),
          directory.reference(), results, () -> CompletableFuture.completedFuture(null), scope).join());
      assertEquals(0, repository.sealCalls);
      resultRepository.directory = directory;
      resultRepository.page = page;
      var pinValidation = new CompletableFuture<Void>();
      var pending = preparation.prepareWithResult(invocation,
          List.<PreparedCandidate>of().iterator(), List.<PreparedEvent>of().iterator(),
          directory.reference(), results, () -> pinValidation, scope);
      assertFalse(pending.isDone());
      assertEquals(0, repository.sealCalls);
      pinValidation.completeExceptionally(new IllegalStateException("pin was revoked"));
      assertThrows(CompletionException.class, pending::join);
      assertEquals(0, repository.sealCalls, "a failed final pin validation cannot seal a root");
      ManifestRoot root = preparation.prepareWithResult(invocation,
          List.<PreparedCandidate>of().iterator(), List.<PreparedEvent>of().iterator(),
          directory.reference(), results, () -> CompletableFuture.completedFuture(null), scope).join();
      InvocationBinding sealed = invocation.seal(root.systemIdentity());
      MaterializationStore materialization = new MaterializationStore() {
        @Override public CompletableFuture<ApplyResult> applySelected(
            CommitDecision decision, PreparedCandidate candidate, RequestScope ignored) {
          return CompletableFuture.completedFuture(ApplyResult.APPLIED);
        }
        @Override public CompletableFuture<Void> preflight(PreparedCandidate candidate, RequestScope ignored) {
          return CompletableFuture.completedFuture(null);
        }
      };
      ManifestCatalog manifests = new ManifestCatalog(repository);
      assertThrows(CompletionException.class,
          () -> new CommitPlanVerifier(manifests, materialization).verify(sealed, root, scope).join());
      CommitPlanVerifier verifier = new CommitPlanVerifier(manifests, materialization, results);
      assertEquals(root, verifier.verify(sealed, root, scope).join());
      resultRepository.page = null;
      assertThrows(CompletionException.class, () -> verifier.verify(sealed, root, scope).join());
    }
  }

  @Test
  void sealsAuxiliaryUpdatesWithTheDurableRpcResultAndPreflightsEveryTarget() {
    MemoryRepository repository = new MemoryRepository();
    SingleResultRepository resultRepository = new SingleResultRepository();
    ResultCatalog results = new ResultCatalog(resultRepository);
    InvocationBinding invocation = InvocationBinding.begin(InvocationId.create(),
        ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1);
    ResultIdentity resultIdentity = ResultIdentity.from(invocation);
    ResultPage resultPage = new ResultPage(resultIdentity, 0, new byte[] {7, 8});
    ResultDirectory resultDirectory = ResultDirectory.create(resultIdentity,
        List.of(ResultPageReference.from(resultPage)));
    resultRepository.page = resultPage;
    resultRepository.directory = resultDirectory;

    long leaseId = -7;
    List<PreparedAuxiliaryRecord> records = List.of(
        PreparedAuxiliaryRecord.create(invocation.transactionId(), 1, 0, 0,
            SystemRecordType.LEASE_STATE, LeaseRecordIdentity.stateVersion(leaseId, 1),
            ByteKey.copyOf(new byte[] {1})),
        PreparedAuxiliaryRecord.create(invocation.transactionId(), 1, 0, 1,
            SystemRecordType.LEASE_ENUMERATION, LeaseRecordIdentity.enumeration(leaseId),
            new LeaseEnumerationPointer(1).encode()),
        PreparedAuxiliaryRecord.create(invocation.transactionId(), 1, 0, 2,
            SystemRecordType.EXPIRY_CANDIDATE,
            new ExpiryCandidateIdentity(10_000, leaseId, 1, 0).encode(),
            new ExpiryCandidateRecord(1).encode()));

    try (RequestScope scope = scope()) {
      ManifestRoot root = new ManifestPreparation(repository,
          new ManifestPreparation.Limits(0, 0, 0, 0))
          .prepareWithResultAndAuxiliary(invocation, List.<PreparedCandidate>of().iterator(),
              List.<PreparedEvent>of().iterator(), resultDirectory.reference(), results,
              records.iterator(), () -> CompletableFuture.completedFuture(null), scope).join();

      assertFalse(root.containsKeyChanges());
      assertEquals(0, root.eventCount());
      assertTrue(root.resultReference().isPresent());
      assertTrue(root.auxiliaryReference().isPresent());
      List<PreparedAuxiliaryRecord> visited = new ArrayList<>();
      new ManifestCatalog(repository).forEachAuxiliary(root, scope, record -> {
        visited.add(record);
        return CompletableFuture.completedFuture(null);
      }).join();
      assertEquals(records, visited);

      InvocationBinding sealed = invocation.seal(root.systemIdentity());
      MaterializationStore materialization = new MaterializationStore() {
        @Override public CompletableFuture<ApplyResult> applySelected(
            CommitDecision decision, PreparedCandidate candidate, RequestScope ignored) {
          return CompletableFuture.completedFuture(ApplyResult.APPLIED);
        }
        @Override public CompletableFuture<Void> preflight(PreparedCandidate candidate, RequestScope ignored) {
          return CompletableFuture.completedFuture(null);
        }
      };
      AtomicInteger preflightCalls = new AtomicInteger();
      AuxiliaryMaterializationStore auxiliary = new AuxiliaryMaterializationStore() {
        @Override public CompletableFuture<Void> preflight(
            PreparedAuxiliaryRecord record, RequestScope ignored) {
          preflightCalls.incrementAndGet();
          return CompletableFuture.completedFuture(null);
        }
        @Override public CompletableFuture<ApplyResult> applySelected(
            CommitDecision decision, PreparedAuxiliaryRecord record, RequestScope ignored) {
          return CompletableFuture.completedFuture(ApplyResult.APPLIED);
        }
      };
      ManifestCatalog catalog = new ManifestCatalog(repository);
      var statePayload = ByteKey.copyOf(new byte[] {1});
      var leaseState = new LeaseState(new LeaseIdentity(leaseId, 1), 30, 10_000, 0,
          LeaseState.Status.ACTIVE, 1, 0, 0, 0);
      var leasePlanVerifier = new AuxiliaryLeasePlanVerifier(
          payload -> payload.equals(statePayload) ? leaseState
              : throwInvalidLeaseStatePayload(),
          payload -> throwInvalidLeaseBindingPayload());
      assertThrows(CompletionException.class, () -> new CommitPlanVerifier(catalog,
          materialization, results).verify(sealed, root, scope).join());
      assertEquals(root, new CommitPlanVerifier(catalog, materialization, results, auxiliary,
          leasePlanVerifier)
          .verify(sealed, root, scope).join());
      assertEquals(3, preflightCalls.get());
    }
  }

  private static LeaseState throwInvalidLeaseStatePayload() {
    throw new IllegalArgumentException("unexpected test Lease state payload");
  }

  private static io.etcdhbase.core.lease.LeaseBindingVersion throwInvalidLeaseBindingPayload() {
    throw new IllegalArgumentException("unexpected test Lease binding payload");
  }

  @Test
  void traversesMaximumAuxiliaryManifestWithoutRecursiveStackGrowth() {
    int count = (int) AuxiliaryReference.MAX_RECORDS;
    MemoryRepository repository = new MemoryRepository();
    InvocationBinding invocation = InvocationBinding.begin(InvocationId.create(),
        ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1);
    var records = IntStream.range(0, count).mapToObj(index -> PreparedAuxiliaryRecord.create(
        invocation.transactionId(), 1, 0, index, SystemRecordType.LEASE_ENUMERATION,
        LeaseRecordIdentity.enumeration(index + 1L), new LeaseEnumerationPointer(1).encode())).iterator();
    try (RequestScope scope = scope()) {
      var root = new ManifestPreparation(repository, new ManifestPreparation.Limits(0, 0, 0, 0))
          .prepareWithAuxiliary(invocation, records, () -> CompletableFuture.completedFuture(null), scope).join();
      AtomicInteger visited = new AtomicInteger();
      var summary = new ManifestCatalog(repository).forEachAuxiliary(root, scope, record -> {
        assertEquals(visited.getAndIncrement(), record.ordinal());
        return CompletableFuture.completedFuture(null);
      }).join();
      assertEquals(count, visited.get());
      assertEquals(count, summary.count());
    }
  }

  @Test
  void stopsAuxiliaryTraversalWhenAnInFlightReadCompletesAfterCancellation() {
    MemoryRepository repository = new MemoryRepository();
    InvocationBinding invocation = InvocationBinding.begin(InvocationId.create(),
        ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1);
    var records = IntStream.range(0, 2).mapToObj(index -> PreparedAuxiliaryRecord.create(
        invocation.transactionId(), 1, 0, index, SystemRecordType.LEASE_ENUMERATION,
        LeaseRecordIdentity.enumeration(index + 1L), new LeaseEnumerationPointer(1).encode())).toList();
    try (RequestScope scope = scope()) {
      var root = new ManifestPreparation(repository, new ManifestPreparation.Limits(0, 0, 0, 0))
          .prepareWithAuxiliary(invocation, records.iterator(),
              () -> CompletableFuture.completedFuture(null), scope).join();
      repository.pendingAuxiliaryRecordRead = new CompletableFuture<>();
      AtomicInteger visited = new AtomicInteger();
      var traversal = new ManifestCatalog(repository).forEachAuxiliary(root, scope, record -> {
        visited.incrementAndGet();
        return CompletableFuture.completedFuture(null);
      });
      assertEquals(1, repository.auxiliaryRecordReads);
      scope.cancel();
      repository.pendingAuxiliaryRecordRead.complete(Optional.of(records.getFirst()));
      var failure = assertThrows(CompletionException.class, traversal::join);
      assertEquals(BackendFailure.Category.CANCELLED, ((BackendFailure) failure.getCause()).category());
      assertEquals(0, visited.get());
      assertEquals(1, repository.auxiliaryRecordReads);
    }
  }

  @Test
  void chargesAuxiliaryDirectoryPagesAndRecordsBeforeReading() throws Exception {
    MemoryRepository repository = new MemoryRepository();
    InvocationBinding invocation = InvocationBinding.begin(InvocationId.create(),
        ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1);
    var records = IntStream.range(0, 2).mapToObj(index -> PreparedAuxiliaryRecord.create(
        invocation.transactionId(), 1, 0, index, SystemRecordType.LEASE_ENUMERATION,
        LeaseRecordIdentity.enumeration(index + 1L), new LeaseEnumerationPointer(1).encode())).toList();
    var limits = new BudgetManager.Limits(2, 2, 2, 2, 2, 1_000_000, 256L << 20);
    var manager = new BudgetManager(limits, Map.of("public", limits), limits);
    var reservation = new BudgetManager.Reservation(8192, 8192, 128L << 20, 8192, 8192);
    try (RequestScope scope = scope(); var account = manager.admit("public", scope, reservation, proofReadLimits())) {
      var root = new ManifestPreparation(repository, new ManifestPreparation.Limits(0, 0, 0, 0))
          .prepareWithAuxiliary(invocation, records.iterator(),
              () -> CompletableFuture.completedFuture(null), scope).join();
      var budget = new ManifestCatalog.ProofReadBudget(scope, account, 3, 1L << 30);
      var failure = assertThrows(CompletionException.class, () -> new ManifestCatalog(repository)
          .forEachAuxiliary(root, scope, budget, record -> CompletableFuture.completedFuture(null)).join());
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, ((BackendFailure) failure.getCause()).category());
      assertEquals(3, budget.pages());
      assertEquals(1, repository.auxiliaryRecordReads);
      assertEquals(0, manager.snapshot().allocatedBytes());
    }
  }

  @Test
  void rejectsDuplicateAuxiliaryTargetsDuringPreparationAndSealedTraversal() {
    MemoryRepository repository = new MemoryRepository();
    InvocationBinding invocation = InvocationBinding.begin(InvocationId.create(),
        ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1);
    ByteKey identity = LeaseRecordIdentity.stateVersion(-7, 1);
    var first = PreparedAuxiliaryRecord.create(invocation.transactionId(), 1, 0, 0,
        SystemRecordType.LEASE_STATE, identity, ByteKey.copyOf(new byte[] {1}));
    var duplicate = PreparedAuxiliaryRecord.create(invocation.transactionId(), 1, 0, 1,
        SystemRecordType.LEASE_STATE, identity, ByteKey.copyOf(new byte[] {2}));
    try (RequestScope scope = scope()) {
      var preparation = new ManifestPreparation(repository, new ManifestPreparation.Limits(0, 0, 0, 0));
      assertThrows(CompletionException.class, () -> preparation.prepareWithAuxiliary(invocation,
          List.of(first, duplicate).iterator(), () -> CompletableFuture.completedFuture(null), scope).join());
      assertEquals(0, repository.sealCalls);

      var firstReference = new AuxiliaryRecordReference(0, first.systemIdentity(), first.digest(), first.encodedBytes());
      var secondReference = new AuxiliaryRecordReference(1, duplicate.systemIdentity(), duplicate.digest(), duplicate.encodedBytes());
      var page = AuxiliaryPage.create(invocation.transactionId(), 0, List.of(firstReference, secondReference));
      var directory = AuxiliaryDirectory.create(invocation.transactionId(), List.of(page.reference()));
      var root = ManifestRoot.createWithAuxiliary(invocation.transactionId(), 1, 0, 1,
          Optional.of(directory.reference()));
      repository.auxiliaryRecords.put(first.systemIdentity(), first);
      repository.auxiliaryRecords.put(duplicate.systemIdentity(), duplicate);
      repository.auxiliaryPages.put(page.systemIdentity(), page);
      repository.auxiliaryDirectories.put(directory.systemIdentity(), directory);
      assertThrows(CompletionException.class, () -> new ManifestCatalog(repository)
          .forEachAuxiliary(root, scope, record -> CompletableFuture.completedFuture(null)).join());
    }
  }

  @Test
  void asynchronouslyStreamsCandidatesAndEventsBeforeSealing() {
    MemoryRepository repository = new MemoryRepository();
    SingleResultRepository resultRepository = new SingleResultRepository();
    InvocationBinding invocation = InvocationBinding.begin(InvocationId.create(),
        ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1);
    ResultIdentity identity = ResultIdentity.from(invocation);
    ResultPage page = new ResultPage(identity, 0, new byte[] {1});
    ResultDirectory directory = ResultDirectory.create(identity, List.of(ResultPageReference.from(page)));
    resultRepository.page = page;
    resultRepository.directory = directory;
    var candidate = candidate(invocation.transactionId(), new byte[] {'a'}, new byte[] {1});
    var event = event(invocation.transactionId(), 0, new byte[] {'a'}, new byte[] {1});
    var candidateReady = new CompletableFuture<Optional<PreparedCandidate>>();
    var eventReady = new CompletableFuture<Optional<PreparedEvent>>();
    var candidateReads = new AtomicInteger();
    var eventReads = new AtomicInteger();
    AsyncCursor<PreparedCandidate> candidates = () -> candidateReads.getAndIncrement() == 0
        ? candidateReady : CompletableFuture.completedFuture(Optional.empty());
    AsyncCursor<PreparedEvent> events = () -> eventReads.getAndIncrement() == 0
        ? eventReady : CompletableFuture.completedFuture(Optional.empty());
    try (RequestScope scope = scope()) {
      var preparation = new ManifestPreparation(repository, new ManifestPreparation.Limits(1, 1, 1, 4096));
      var pending = preparation.prepareWithResult(invocation, candidates, events,
          directory.reference(), new ResultCatalog(resultRepository),
          () -> CompletableFuture.completedFuture(null), scope);
      assertFalse(pending.isDone());
      assertEquals(0, repository.writeCount);
      assertEquals(0, eventReads.get());
      candidateReady.complete(Optional.of(candidate));
      assertFalse(pending.isDone());
      assertEquals(1, eventReads.get());
      assertEquals(0, repository.sealCalls);
      eventReady.complete(Optional.of(event));
      ManifestRoot root = pending.join();
      assertEquals(1, root.candidateCount());
      assertEquals(1, root.eventCount());
      assertEquals(2, candidateReads.get());
      assertEquals(2, eventReads.get());
      assertEquals(1, repository.sealCalls);
    }
  }

  @Test
  void cancellationBeforeAnAsyncCursorReturnsCannotSealAManifest() {
    MemoryRepository repository = new MemoryRepository();
    InvocationBinding invocation = InvocationBinding.begin(InvocationId.create(),
        ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1);
    ResultIdentity identity = ResultIdentity.from(invocation);
    ResultPage page = new ResultPage(identity, 0, new byte[] {1});
    ResultDirectory directory = ResultDirectory.create(identity, List.of(ResultPageReference.from(page)));
    var ready = new CompletableFuture<Optional<PreparedCandidate>>();
    RequestScope scope = scope();
    var preparation = new ManifestPreparation(repository, new ManifestPreparation.Limits(1, 1, 1, 4096));
    var pending = preparation.prepareWithResult(invocation, () -> ready,
        () -> CompletableFuture.completedFuture(Optional.empty()), directory.reference(),
        new ResultCatalog(new SingleResultRepository()), () -> CompletableFuture.completedFuture(null), scope);
    scope.close();
    ready.complete(Optional.of(candidate(invocation.transactionId(), new byte[] {'a'}, new byte[] {1})));
    assertThrows(CompletionException.class, pending::join);
    assertEquals(0, repository.writeCount);
    assertEquals(0, repository.sealCalls);
  }

  @Test
  void catalogRejectsMissingOrMismatchedPagesAndRestoreRejectsBadDigest() {
    TransactionId transactionId = TransactionId.create();
    MemoryRepository repository = new MemoryRepository();
    PreparedCandidate candidate = candidate(transactionId, new byte[] {'a'}, new byte[] {1});
    try (RequestScope scope = scope()) {
      ManifestRoot root = prepare(repository, transactionId, List.of(candidate), List.of(),
          new ManifestPreparation.Limits(1, 1), scope);
      ManifestPage page = repository.pages.values().iterator().next();
      ManifestCatalog catalog = new ManifestCatalog(repository);

      repository.pages.remove(page.systemIdentity());
      assertThrows(CompletionException.class, () -> catalog.forEachCandidate(root, scope,
          ignored -> CompletableFuture.completedFuture(null)).join());

      PreparedCandidate other = candidate(transactionId, new byte[] {'b'}, new byte[] {2});
      repository.put(ManifestPage.create(transactionId, page.pageIndex(),
          List.of(ManifestCandidateReference.from(other, 0))));
      assertThrows(CompletionException.class, () -> catalog.forEachCandidate(root, scope,
          ignored -> CompletableFuture.completedFuture(null)).join());

      byte[] wrongDigest = page.digest().bytes();
      wrongDigest[0] ^= 1;
      assertThrows(IllegalArgumentException.class, () -> ManifestPage.restore(transactionId,
          page.pageIndex(), page.candidates(), ByteKey.copyOf(wrongDigest)));
    }
  }

  @Test
  void catalogRejectsMissingOrMismatchedEventPages() {
    TransactionId transactionId = TransactionId.create();
    MemoryRepository repository = new MemoryRepository();
    PreparedEvent event = event(transactionId, 0, new byte[] {'a'}, new byte[] {1});
    repository.put(event);
    EventPage page = EventPage.create(transactionId, 0,
        List.of(PreparedEventReference.from(event)));
    repository.put(page);
    ManifestRoot root = ManifestRoot.create(transactionId, OWNER_EPOCH, BASE_SEQUENCE,
        BASE_REVISION, 0, 0, Optional.empty(), 1, event.retainedBytes(), Optional.of(page.reference()));
    ManifestCatalog catalog = new ManifestCatalog(repository);

    try (RequestScope scope = scope()) {
      repository.eventPages.remove(page.systemIdentity());
      assertThrows(CompletionException.class, () -> catalog.forEachEvent(root, scope,
          ignored -> CompletableFuture.completedFuture(null)).join());

      PreparedEvent changed = event(transactionId, 0, new byte[] {'b'}, new byte[] {2});
      repository.put(EventPage.create(transactionId, 0,
          List.of(PreparedEventReference.from(changed))));
      assertThrows(CompletionException.class, () -> catalog.forEachEvent(root, scope,
          ignored -> CompletableFuture.completedFuture(null)).join());
    }
  }

  private static ManifestRoot prepare(MemoryRepository repository, TransactionId transactionId,
      List<PreparedCandidate> candidates, List<PreparedEvent> events,
      ManifestPreparation.Limits limits, RequestScope scope) {
    return join(new ManifestPreparation(repository, limits).prepare(transactionId, OWNER_EPOCH,
        BASE_SEQUENCE, BASE_REVISION, candidates.iterator(), events.iterator(), scope));
  }

  private static PreparedCandidate candidate(TransactionId transactionId, byte[] key, byte[] value) {
    StorageMetadata metadata = new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, value.length);
    return new PreparedCandidate(transactionId, OWNER_EPOCH, BASE_SEQUENCE, BASE_REVISION,
        StoredRecord.live(ByteKey.copyOf(key), metadata, value));
  }

  private static PreparedEvent event(TransactionId transactionId, long ordinal, byte[] key, byte[] value) {
    StorageMetadata metadata = new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, value.length);
    StoredRecord record = StoredRecord.live(ByteKey.copyOf(key), metadata, value);
    return new PreparedEvent(transactionId, OWNER_EPOCH, BASE_SEQUENCE, BASE_REVISION, ordinal,
        PreparedEvent.Kind.PUT, record, Optional.empty());
  }

  private static byte[] numberedKey(int index) {
    return String.format("k%03d", index).getBytes(java.nio.charset.StandardCharsets.US_ASCII);
  }

  private static byte[] sequenceKey(int index) {
    return java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(index).array();
  }

  private static RequestScope scope() {
    return RequestScope.withTimeout(Duration.ofMinutes(1));
  }

  private static <T> T join(CompletableFuture<T> future) {
    return future.join();
  }

  private static final class MemoryRepository implements ManifestRepository {
    private final Map<ByteKey, PreparedCandidate> candidates = new HashMap<>();
    private final Map<ByteKey, ManifestPage> pages = new HashMap<>();
    private final Map<ByteKey, ManifestDirectoryPage> directories = new HashMap<>();
    private final Map<ByteKey, PreparedEvent> events = new HashMap<>();
    private final Map<ByteKey, EventPage> eventPages = new HashMap<>();
    private final Map<ByteKey, EventDirectoryPage> eventDirectories = new HashMap<>();
    private final Map<ByteKey, PreparedAuxiliaryRecord> auxiliaryRecords = new HashMap<>();
    private final Map<ByteKey, AuxiliaryPage> auxiliaryPages = new HashMap<>();
    private final Map<ByteKey, AuxiliaryDirectory> auxiliaryDirectories = new HashMap<>();
    private final Map<ByteKey, ManifestRoot> roots = new HashMap<>();
    private int maximumLogicalKeyLength = Integer.MAX_VALUE;
    private int candidateWriteCount;
    private int failCandidateWriteNumber = -1;
    private int writeCount;
    private int sealCalls;
    private boolean deferCreateResults;
    private final ArrayDeque<PendingCreate> pendingCreates = new ArrayDeque<>();
    private int maximumPendingCreates;
    private CompletableFuture<Optional<ManifestDirectoryPage>> pendingDirectoryRead;
    private CompletableFuture<Optional<PreparedAuxiliaryRecord>> pendingAuxiliaryRecordRead;
    private int auxiliaryRecordReads;

    @Override
    public int candidateBucket(PreparedCandidate candidate) {
      if (candidate.record().key().length() > maximumLogicalKeyLength) {
        throw new IllegalArgumentException("candidate key exceeds test row limit");
      }
      return 0;
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(
        PreparedCandidate candidate, RequestScope scope) {
      candidateWriteCount++;
      if (candidateWriteCount == failCandidateWriteNumber) {
        return CompletableFuture.failedFuture(new IllegalStateException("injected candidate write failure"));
      }
      writeCount++;
      return create(candidates, candidate.systemIdentity(), candidate, PreparedCandidate::contentDigest);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createPage(
        ManifestPage page, RequestScope scope) {
      writeCount++;
      return create(pages, page.systemIdentity(), page, ManifestPage::digest);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(
        ManifestDirectoryPage page, RequestScope scope) {
      writeCount++;
      return create(directories, page.systemIdentity(), page, ManifestDirectoryPage::digest);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createEvent(
        PreparedEvent event, RequestScope scope) {
      writeCount++;
      return create(events, event.systemIdentity(), event, PreparedEvent::digest);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(
        EventPage page, RequestScope scope) {
      writeCount++;
      return create(eventPages, page.systemIdentity(), page, EventPage::digest);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(
        EventDirectoryPage page, RequestScope scope) {
      writeCount++;
      return create(eventDirectories, page.systemIdentity(), page, EventDirectoryPage::digest);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createAuxiliaryRecord(
        PreparedAuxiliaryRecord record, RequestScope scope) {
      writeCount++;
      return create(auxiliaryRecords, record.systemIdentity(), record, PreparedAuxiliaryRecord::digest);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createAuxiliaryPage(
        AuxiliaryPage page, RequestScope scope) {
      writeCount++;
      return create(auxiliaryPages, page.systemIdentity(), page, AuxiliaryPage::digest);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createAuxiliaryDirectory(
        AuxiliaryDirectory directory, RequestScope scope) {
      writeCount++;
      return create(auxiliaryDirectories, directory.systemIdentity(), directory, AuxiliaryDirectory::digest);
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> seal(ManifestRoot root, RequestScope scope) {
      writeCount++;
      sealCalls++;
      return create(roots, root.systemIdentity(), root, ManifestRoot::digest);
    }

    @Override
    public CompletableFuture<Optional<ManifestRoot>> readRoot(ByteKey identity, RequestScope scope) {
      return completed(roots.get(identity));
    }

    @Override
    public CompletableFuture<Optional<PreparedCandidate>> readCandidate(ByteKey identity, RequestScope scope) {
      return completed(candidates.get(identity));
    }

    @Override
    public CompletableFuture<Optional<ManifestPage>> readPage(ByteKey identity, RequestScope scope) {
      return completed(pages.get(identity));
    }

    @Override
    public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(
        ByteKey identity, RequestScope scope) {
      if (pendingDirectoryRead != null) return pendingDirectoryRead;
      return completed(directories.get(identity));
    }

    @Override
    public CompletableFuture<Optional<PreparedEvent>> readEvent(ByteKey identity, RequestScope scope) {
      return completed(events.get(identity));
    }

    @Override
    public CompletableFuture<Optional<EventPage>> readEventPage(ByteKey identity, RequestScope scope) {
      return completed(eventPages.get(identity));
    }

    @Override
    public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(
        ByteKey identity, RequestScope scope) {
      return completed(eventDirectories.get(identity));
    }

    @Override
    public CompletableFuture<Optional<PreparedAuxiliaryRecord>> readAuxiliaryRecord(
        ByteKey identity, RequestScope scope) {
      auxiliaryRecordReads++;
      if (pendingAuxiliaryRecordRead != null) return pendingAuxiliaryRecordRead;
      return completed(auxiliaryRecords.get(identity));
    }

    @Override
    public CompletableFuture<Optional<AuxiliaryPage>> readAuxiliaryPage(
        ByteKey identity, RequestScope scope) {
      return completed(auxiliaryPages.get(identity));
    }

    @Override
    public CompletableFuture<Optional<AuxiliaryDirectory>> readAuxiliaryDirectory(
        ByteKey identity, RequestScope scope) {
      return completed(auxiliaryDirectories.get(identity));
    }

    private void put(PreparedCandidate value) { candidates.put(value.systemIdentity(), value); }
    private void put(ManifestPage value) { pages.put(value.systemIdentity(), value); }
    private void put(ManifestDirectoryPage value) { directories.put(value.systemIdentity(), value); }
    private void put(PreparedEvent value) { events.put(value.systemIdentity(), value); }
    private void put(EventPage value) { eventPages.put(value.systemIdentity(), value); }
    private void put(EventDirectoryPage value) { eventDirectories.put(value.systemIdentity(), value); }

    private <T> CompletableFuture<SystemRecordStore.CreateResult> create(
        Map<ByteKey, T> records, ByteKey identity, T value,
        java.util.function.Function<T, ByteKey> digest) {
      T previous = records.putIfAbsent(identity, value);
      SystemRecordStore.CreateResult result = previous == null
          ? SystemRecordStore.CreateResult.CREATED
          : digest.apply(previous).equals(digest.apply(value))
              ? SystemRecordStore.CreateResult.ALREADY_PRESENT
              : SystemRecordStore.CreateResult.CONFLICT;
      if (!deferCreateResults) return CompletableFuture.completedFuture(result);
      CompletableFuture<SystemRecordStore.CreateResult> future = new CompletableFuture<>();
      pendingCreates.addLast(new PendingCreate(future, result));
      maximumPendingCreates = Math.max(maximumPendingCreates, pendingCreates.size());
      return future;
    }

    private boolean completeNextCreate() {
      PendingCreate pending = pendingCreates.pollFirst();
      return pending != null && pending.future().complete(pending.result());
    }

    private record PendingCreate(CompletableFuture<SystemRecordStore.CreateResult> future,
        SystemRecordStore.CreateResult result) {}

    private static <T> CompletableFuture<Optional<T>> completed(T value) {
      return CompletableFuture.completedFuture(Optional.ofNullable(value));
    }
  }

  private static final class SingleResultRepository implements ResultRepository {
    private ResultPage page;
    private ResultDirectory directory;

    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(
        ResultPage value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectory(
        ResultDirectory value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<ResultPage>> readPage(
        ResultIdentity identity, int index, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(page));
    }
    @Override public CompletableFuture<Optional<ResultDirectory>> readDirectory(
        ResultIdentity identity, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(directory));
    }
  }
}
