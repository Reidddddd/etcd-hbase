package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.EventPage;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ManifestCandidateReference;
import io.etcdhbase.core.commit.ManifestPage;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.PreparedEventReference;
import io.etcdhbase.core.commit.ResultDirectory;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.ResultPage;
import io.etcdhbase.core.commit.ResultPageReference;
import io.etcdhbase.core.commit.SystemRecordClosure;
import io.etcdhbase.core.commit.SystemRecordEntry;
import io.etcdhbase.core.commit.SystemRecordPage;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.SnapshotGcBoundary;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.ManifestMessages;
import io.etcdhbase.hbase.codec.CommitDecisionMessages;
import io.etcdhbase.hbase.codec.ResultMessages;
import io.etcdhbase.hbase.codec.InvocationMessages;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.time.Duration;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class HBaseSystemRecordClosureTest {
  private static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);

  @Test
  void proofBudgetEndsAtTheCompletedPrefixAndTheNextPageRetriesTheUnprovedRecord() {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var records = mock(SystemRecordStore.class);
      var retirement = mock(HBaseSystemRecordStore.class);
      var entries = java.util.stream.IntStream.range(0, 24).mapToObj(index -> candidateEntry()).toList();
      when(records.scanBucket(any(), anyInt(), any(), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(retiredReferencePage()));
      when(records.scanBucket(eq(SystemRecordType.CANDIDATE), eq(0), any(), anyInt(), anyLong(), eq(scope)))
          .thenAnswer(call -> {
            Optional<ByteKey> after = call.getArgument(2);
            var remaining = after.isEmpty() ? entries : entries.subList(21, entries.size());
            return CompletableFuture.completedFuture(new SystemRecordPage(remaining,
                Optional.of(entries.getLast().identity()), true, 1024));
          });
      when(retirement.retire(any(), any(), any(), any(), any(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(new SystemRecordRetirement.Outcome(true, false, 0)));
      var closure = closure(records, retirement, DIGEST, 1);
      var first = closure.collect(SystemRecordClosure.Domain.CANDIDATE, 0, Optional.empty(), 64, permit(scope), scope).join();
      org.mockito.Mockito.verify(records, org.mockito.Mockito.never()).scanBucket(
          eq(SystemRecordType.EVENT_REVISION_INDEX), anyInt(), any(), anyInt(), anyLong(), any());
      assertFalse(first.exhausted());
      assertEquals(21, first.examined());
      assertEquals(21, first.retired());
      assertEquals(Optional.of(entries.get(20).identity()), first.lastIdentity());
      org.mockito.Mockito.verify(retirement, org.mockito.Mockito.never()).retire(
          any(), eq(entries.get(21).identity()), any(), any(), any(), any());
      var next = closure.collect(SystemRecordClosure.Domain.CANDIDATE, 0, first.lastIdentity(), 64, permit(scope), scope).join();
      assertTrue(next.exhausted());
      assertEquals(3, next.retired());
      assertEquals(Optional.of(entries.getLast().identity()), next.lastIdentity());
    }
  }

  @Test
  void anUnprovableFirstRecordStillFailsRatherThanPublishingANonadvancingCursor() {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var records = mock(SystemRecordStore.class);
      var retirement = mock(HBaseSystemRecordStore.class);
      var entry = candidateEntry();
      when(records.scanBucket(any(), anyInt(), any(), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(retiredReferencePage()));
      when(records.scanBucket(eq(SystemRecordType.CANDIDATE), eq(0), any(), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(page(entry)));
      var closure = closure(records, retirement, DIGEST, 65);
      var failure = org.junit.jupiter.api.Assertions.assertThrows(java.util.concurrent.CompletionException.class,
          () -> closure.collect(SystemRecordClosure.Domain.CANDIDATE, 0, Optional.empty(), 64, permit(scope), scope).join());
      assertEquals("closure proof record budget exceeded", failure.getCause().getMessage());
      org.mockito.Mockito.verifyNoInteractions(retirement);
    }
  }

  @Test
  void malformedRecordAfterACompletedPrefixStillAbortsThePage() {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var records = mock(SystemRecordStore.class);
      var retirement = mock(HBaseSystemRecordStore.class);
      var first = candidateEntry();
      var malformed = new SystemRecordEntry(key(8), key(9));
      stubEmptyScans(records, scope);
      when(records.scanBucket(eq(SystemRecordType.CANDIDATE), eq(0), any(), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(new SystemRecordPage(List.of(first, malformed),
              Optional.of(malformed.identity()), true, 1024)));
      when(retirement.retire(any(), any(), any(), any(), any(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(new SystemRecordRetirement.Outcome(true, false, 0)));
      var closure = closure(records, retirement, DIGEST, 1);
      org.junit.jupiter.api.Assertions.assertThrows(java.util.concurrent.CompletionException.class,
          () -> closure.collect(SystemRecordClosure.Domain.CANDIDATE, 0, Optional.empty(), 64, permit(scope), scope).join());
      org.mockito.Mockito.verify(retirement).retire(any(), eq(first.identity()), any(), any(), any(), any());
    }
  }

  private static SystemRecordEntry candidateEntry() {
    var candidate = new PreparedCandidate(TransactionId.create(), 1, 0, 1, storedValue());
    return new SystemRecordEntry(candidate.systemIdentity(), ByteKey.copyOf(ManifestMessages.encode(candidate)));
  }

  private static SystemRecordPage retiredReferencePage() {
    return new SystemRecordPage(List.of(), Optional.of(key(9)), true, 1024, 1024, 1);
  }

  @Test
  void metadataOnlyDecisionStaysOutsideThePreviousWindowAndCollectsAfterItsReferencesDrain() {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var records = mock(SystemRecordStore.class);
      var retirement = mock(HBaseSystemRecordStore.class);
      var binding = InvocationBinding.begin(InvocationId.create(), DIGEST, DIGEST,
          TransactionId.create(), 1, 4, 9);
      var decision = new CommitDecision(binding.transactionId(), binding.invocationId(),
          5, 9, 9, key(1), DIGEST, false);
      var target = new SystemRecordEntry(ByteKey.copyOf(ByteBuffer.allocate(8).putLong(5).array()),
          ByteKey.copyOf(CommitDecisionMessages.encode(decision)));
      stubEmptyScans(records, scope);
      when(records.scanBucket(eq(SystemRecordType.DECISION), eq(0), any(), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(page(target)));
      when(records.scanBucket(eq(SystemRecordType.INVOCATION), eq(0), any(), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(page(new SystemRecordEntry(
              ByteKey.copyOf(binding.invocationId().bytes()), ByteKey.copyOf(InvocationMessages.encode(binding))))));
      var closure = closure(records, retirement, DIGEST, 1);
      var permit = permit(scope);
      var previous = closure.collect(SystemRecordClosure.Domain.DECISION, 0, Optional.empty(), 16, permit, scope).join();
      assertEquals(0, previous.pending());
      assertEquals(0, previous.retired());
      org.mockito.Mockito.verifyNoInteractions(retirement);
      when(permit.safeRevision()).thenReturn(11L);
      assertEquals(1, closure.collect(SystemRecordClosure.Domain.DECISION, 0, Optional.empty(), 16,
          permit, scope).join().pending());
      when(records.scanBucket(eq(SystemRecordType.INVOCATION), eq(0), any(), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(empty()));
      when(retirement.retire(any(), any(), any(), any(), any(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(new SystemRecordRetirement.Outcome(true, false, 0)));
      var drained = closure.collect(SystemRecordClosure.Domain.DECISION, 0, Optional.empty(), 16, permit, scope).join();
      assertEquals(0, drained.pending());
      assertEquals(1, drained.retired());
    }
  }

  @Test
  void terminalPreparedCountersRemainAtTheNonRetireableUpperBound() {
    var transaction = TransactionId.create();
    for (long base : new long[] {Long.MAX_VALUE - 1, Long.MAX_VALUE}) {
      var target = SystemRecordGcTarget.prepared(transaction, base, base);
      assertEquals(Long.MAX_VALUE, target.revision());
      assertEquals(Long.MAX_VALUE, target.sequence());
    }
  }

  @Test
  void newerManifestPagesUseRootOrPermanentInvocationBoundsWithoutBlockingOldCompletion() {
    for (boolean rootPresent : new boolean[] {true, false}) {
      try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
        var records = mock(SystemRecordStore.class);
        var retirement = mock(HBaseSystemRecordStore.class);
        var transaction = TransactionId.create();
        var candidate = new PreparedCandidate(transaction, 1, 20, 20, StoredRecord.live(key(2),
            new StorageMetadata(21, 21, 1, 0, StorageMetadata.State.LIVE, 1), new byte[] {1}));
        var page = ManifestPage.create(transaction, 0, List.of(ManifestCandidateReference.from(candidate, 0)));
        var root = ManifestRoot.create(transaction, 1, 20, 20, 1, 1, Optional.of(page.reference()));
        stubEmptyScans(records, scope);
        when(records.scanBucket(eq(SystemRecordType.MANIFEST), eq(0), any(), anyInt(), anyLong(), eq(scope)))
            .thenReturn(CompletableFuture.completedFuture(page(new SystemRecordEntry(
                page.systemIdentity(), ByteKey.copyOf(ManifestMessages.encode(page))))));
        when(records.read(SystemRecordType.MANIFEST, root.systemIdentity(), scope)).thenReturn(
            CompletableFuture.completedFuture(rootPresent
                ? Optional.of(ByteKey.copyOf(ManifestMessages.encode(root))) : Optional.empty()));
        if (!rootPresent) {
          var binding = InvocationBinding.begin(InvocationId.create(), DIGEST, DIGEST, transaction, 1, 20, 20);
          when(records.scanBucket(eq(SystemRecordType.INVOCATION), eq(0), any(), anyInt(), anyLong(), eq(scope)))
              .thenReturn(CompletableFuture.completedFuture(page(new SystemRecordEntry(
                  ByteKey.copyOf(binding.invocationId().bytes()), ByteKey.copyOf(InvocationMessages.encode(binding))))));
        }
        var result = closure(records, retirement, DIGEST, 1)
            .collect(SystemRecordClosure.Domain.MANIFEST, 0, Optional.empty(), 16, permit(scope), scope).join();
        assertEquals(1, result.examined());
        assertEquals(0, result.pending());
        assertEquals(0, result.retired());
        org.mockito.Mockito.verifyNoInteractions(retirement);
      }
    }
  }

  @Test
  void exactRootProtectionCountsAsPendingEvenAboveNumericFloors() {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var records = mock(SystemRecordStore.class);
      var retirement = mock(HBaseSystemRecordStore.class);
      var root = ManifestRoot.create(TransactionId.create(), 1, 0, 1, 0, 0, Optional.empty());
      stubEmptyScans(records, scope);
      when(records.scanBucket(eq(SystemRecordType.MANIFEST), eq(0), any(), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(page(new SystemRecordEntry(
              root.systemIdentity(), ByteKey.copyOf(ManifestMessages.encode(root))))));
      var authority = mock(SystemRecordGcAuthority.class);
      var protection = new SnapshotGcBoundary.Protection(0, 0, 1, ByteKey.copyOf(new byte[16]), 20, 20,
          Optional.of(root.transactionId()), Optional.of(root.systemIdentity()), Optional.of(root.digest()));
      when(authority.validate(any(), any())).thenReturn(CompletableFuture.completedFuture(List.of(protection)));
      var result = new HBaseSystemRecordClosure(records, retirement, DIGEST, 1, authority)
          .collect(SystemRecordClosure.Domain.MANIFEST, 0, Optional.empty(), 16, permit(scope), scope).join();
      assertEquals(1, result.pending());
      assertTrue(result.exhausted());
      org.mockito.Mockito.verifyNoInteractions(retirement);
    }
  }

  @Test
  void changedRetirementOutcomeRemainsPending() {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var records = mock(SystemRecordStore.class);
      var retirement = mock(HBaseSystemRecordStore.class);
      var candidate = new PreparedCandidate(TransactionId.create(), 1, 0, 1, storedValue());
      stubEmptyScans(records, scope);
      when(records.scanBucket(eq(SystemRecordType.CANDIDATE), eq(0), any(), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(page(new SystemRecordEntry(
              candidate.systemIdentity(), ByteKey.copyOf(ManifestMessages.encode(candidate))))));
      when(retirement.retire(any(), any(), any(), any(), any(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(new SystemRecordRetirement.Outcome(false, false, 1)));
      var result = closure(records, retirement, DIGEST, 1)
          .collect(SystemRecordClosure.Domain.CANDIDATE, 0, Optional.empty(), 16, permit(scope), scope).join();
      assertEquals(1, result.pending());
      assertEquals(0, result.retired());
    }
  }

  @Test
  void retainsCandidateReferencedByManifestPage() {
    var transaction = TransactionId.create();
    var candidate = new PreparedCandidate(transaction, 1, 0, 1, storedValue());
    var target = candidate.systemIdentity();
    var reference = ManifestCandidateReference.from(candidate, 0);
    var page = ManifestPage.create(transaction, 0, List.of(reference));
    assertReferenced(SystemRecordClosure.Domain.CANDIDATE,
        new SystemRecordEntry(target, ByteKey.copyOf(ManifestMessages.encode(candidate))),
        new SystemRecordEntry(page.systemIdentity(), ByteKey.copyOf(ManifestMessages.encode(page))));
  }

  @Test
  void retainsEventReferencedByEventPage() {
    var transaction = TransactionId.create();
    var event = new PreparedEvent(transaction, 1, 0, 1, 0, PreparedEvent.Kind.PUT,
        storedValue(), Optional.empty());
    var target = event.systemIdentity();
    var page = EventPage.create(transaction, 0, List.of(PreparedEventReference.from(event)));
    assertReferenced(SystemRecordClosure.Domain.EVENT,
        new SystemRecordEntry(target, ByteKey.copyOf(ManifestMessages.encode(event))),
        new SystemRecordEntry(page.systemIdentity(), ByteKey.copyOf(ManifestMessages.encode(page))));
  }

  @Test
  void resultPageChecksOnlyItsCanonicalDirectoryAndRejectsDifferentOrMalformedPreparations() {
    for (int state = 0; state < 4; state++) {
      try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
        var records = mock(SystemRecordStore.class);
        var retirement = mock(HBaseSystemRecordStore.class);
        var identity = new ResultIdentity(InvocationId.create(), TransactionId.create(), DIGEST, DIGEST, 1, 0, 1);
        var page = new ResultPage(identity, 0, new byte[] {1});
        var target = new SystemRecordEntry(page.systemIdentity(), ByteKey.copyOf(ResultMessages.encode(page)));
        var different = new ResultIdentity(InvocationId.create(), identity.transactionId(), DIGEST, DIGEST, 1, 0, 1);
        var directoryIdentity = state == 2 ? different : identity;
        var directory = ResultDirectory.create(directoryIdentity,
            List.of(ResultPageReference.from(new ResultPage(directoryIdentity, 0, new byte[] {1}))));
        stubEmptyScans(records, scope);
        when(records.scanBucket(eq(SystemRecordType.RESULT), eq(0), any(), anyInt(), anyLong(), eq(scope)))
            .thenReturn(CompletableFuture.completedFuture(page(target)));
        when(records.read(SystemRecordType.RESULT, directory.systemIdentity(), scope)).thenReturn(
            CompletableFuture.completedFuture(state == 1 ? Optional.empty()
                : Optional.of(state == 3 ? key(9) : ByteKey.copyOf(ResultMessages.encode(directory)))));
        when(retirement.retire(any(), any(), any(), any(), any(), eq(scope)))
            .thenReturn(CompletableFuture.completedFuture(new SystemRecordRetirement.Outcome(true, false, 0)));
        var collected = closure(records, retirement, DIGEST, 1)
            .collect(SystemRecordClosure.Domain.RESULT, 0, Optional.empty(), 64, permit(scope), scope);
        if (state >= 2) {
          org.junit.jupiter.api.Assertions.assertThrows(java.util.concurrent.CompletionException.class, collected::join);
          org.mockito.Mockito.verifyNoInteractions(retirement);
        } else {
          var result = collected.join();
          assertEquals(state == 0 ? 1 : 0, result.pending());
          assertEquals(state == 1 ? 1 : 0, result.retired());
        }
        org.mockito.Mockito.verify(records, org.mockito.Mockito.never()).scanBucket(
            eq(SystemRecordType.RESULT), anyInt(), any(), eq(1024), anyLong(), any());
      }
    }
  }

  @Test
  void retainsResultReferencedByManifestRoot() {
    var transaction = TransactionId.create();
    var identity = new ResultIdentity(InvocationId.create(), transaction, DIGEST, DIGEST, 1, 0, 1);
    var result = ResultDirectory.create(identity,
        List.of(ResultPageReference.from(new ResultPage(identity, 0, new byte[] {1}))));
    var root = ManifestRoot.createWithResult(transaction, 1, 0, 1, 0, 0, Optional.empty(),
        0, 0, Optional.empty(), result.reference());
    assertReferenced(SystemRecordClosure.Domain.RESULT,
        new SystemRecordEntry(result.systemIdentity(), ByteKey.copyOf(ResultMessages.encode(result))),
        new SystemRecordEntry(root.systemIdentity(), ByteKey.copyOf(ManifestMessages.encode(root))));
  }

  @Test
  void followsReferencePagesIncludingAnEmptyTombstonePage() {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var records = mock(SystemRecordStore.class);
      var retirement = mock(HBaseSystemRecordStore.class);
      var candidate = new PreparedCandidate(TransactionId.create(), 1, 0, 1, storedValue());
      var target = new SystemRecordEntry(candidate.systemIdentity(), ByteKey.copyOf(ManifestMessages.encode(candidate)));
      var reference = ManifestPage.create(candidate.transactionId(), 0,
          List.of(ManifestCandidateReference.from(candidate, 0)));
      stubEmptyScans(records, scope);
      when(records.scanBucket(eq(SystemRecordType.CANDIDATE), eq(0), any(), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(page(target)));
      var cursor = key(8);
      when(records.scanBucket(eq(SystemRecordType.MANIFEST), eq(0), eq(Optional.empty()), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(new SystemRecordPage(List.of(),
              Optional.of(cursor), false, 10, 1, 1)));
      when(records.scanBucket(eq(SystemRecordType.MANIFEST), eq(0), eq(Optional.of(cursor)), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(page(new SystemRecordEntry(reference.systemIdentity(),
              ByteKey.copyOf(ManifestMessages.encode(reference))))));
      var page = closure(records, retirement, DIGEST, 1)
          .collect(SystemRecordClosure.Domain.CANDIDATE, 0, Optional.empty(), 16, permit(scope), scope).join();
      assertEquals(0, page.retired());
    }
  }

  @Test
  void resumesFromPhysicalCursorWithoutRestartingEarlierBuckets() {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var records = mock(SystemRecordStore.class);
      var retirement = mock(HBaseSystemRecordStore.class);
      var cursor = key(8);
      var next = key(9);
      when(records.bucket(SystemRecordType.INVOCATION, cursor)).thenReturn(1);
      when(records.scanBucket(SystemRecordType.INVOCATION, 1, Optional.of(cursor), 1, 8L << 20, scope))
          .thenReturn(CompletableFuture.completedFuture(new SystemRecordPage(List.of(), Optional.of(next), false, 10, 1, 1)));
      var page = closure(records, retirement, DIGEST, 2)
          .collect(SystemRecordClosure.Domain.INVOCATION, 1, Optional.of(cursor), 1, permit(scope), scope).join();
      assertEquals(0, page.examined());
      assertEquals(Optional.of(next), page.lastIdentity());
      assertFalse(page.exhausted());
    }
  }

  @Test
  void aFullLastBucketCanStillBeExhausted() {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var records = mock(SystemRecordStore.class);
      var binding = InvocationBinding.begin(InvocationId.create(), DIGEST, DIGEST,
          TransactionId.create(), 1, 0, 1);
      var identity = ByteKey.copyOf(binding.invocationId().bytes());
      var encoded = ByteKey.copyOf(InvocationMessages.encode(binding));
      when(records.read(SystemRecordType.INVOCATION, identity, scope))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(encoded)));
      when(records.scanBucket(SystemRecordType.INVOCATION, 0, Optional.empty(), 1, 8L << 20, scope))
          .thenReturn(CompletableFuture.completedFuture(page(new SystemRecordEntry(identity, encoded))));
      var page = closure(records, mock(HBaseSystemRecordStore.class), DIGEST, 1)
          .collect(SystemRecordClosure.Domain.INVOCATION, 0, Optional.empty(), 1, permit(scope), scope).join();
      assertEquals(1, page.examined());
      assertTrue(page.exhausted());
      assertEquals(1, page.pending(), "an unresolved eligible invocation prevents completion");
    }
  }

  @Test
  void preparingInvocationProtectsRecordsBeforeItsManifestIsWritten() {
    assertCandidateRetirement(InvocationBinding.State.PREPARING, 0);
  }

  @Test
  void closedInvocationDoesNotRetainItsTransactionForever() {
    assertCandidateRetirement(InvocationBinding.State.WINDOW_CLOSED, 1);
  }

  @Test
  void unselectedInvocationDoesNotProtectAbandonedTransactionRecords() {
    assertCandidateRetirement(InvocationBinding.State.NOT_SELECTED, 1);
  }

  @Test
  void lookaheadRowsDoNotMakeTheRemainingPageBudgetNegative() {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var records = mock(SystemRecordStore.class);
      var entry = preparingInvocation(records, scope);
      when(records.scanBucket(SystemRecordType.INVOCATION, 0, Optional.empty(), 1, 8L << 20, scope))
          .thenReturn(CompletableFuture.completedFuture(new SystemRecordPage(List.of(entry),
              Optional.of(entry.identity()), false, 2, 2, 1)));
      var result = closure(records, mock(HBaseSystemRecordStore.class), DIGEST, 1)
          .collect(SystemRecordClosure.Domain.INVOCATION, 0, Optional.empty(), 1, permit(scope), scope).join();
      assertEquals(1, result.examined());
      assertFalse(result.exhausted());
    }
  }

  private static void assertCandidateRetirement(InvocationBinding.State state, int expectedRetired) {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var records = mock(SystemRecordStore.class);
      var retirement = mock(HBaseSystemRecordStore.class);
      var candidate = new PreparedCandidate(TransactionId.create(), 1, 0, 1, storedValue());
      var target = new SystemRecordEntry(candidate.systemIdentity(), ByteKey.copyOf(ManifestMessages.encode(candidate)));
      var invocation = InvocationBinding.restore(InvocationId.create(), DIGEST, DIGEST,
          candidate.transactionId(), 1, 0, 1, state, Optional.empty(), Optional.empty());
      stubEmptyScans(records, scope);
      when(records.scanBucket(eq(SystemRecordType.CANDIDATE), eq(0), any(), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(page(target)));
      when(records.scanBucket(eq(SystemRecordType.INVOCATION), eq(0), any(), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(page(new SystemRecordEntry(
              ByteKey.copyOf(invocation.invocationId().bytes()), ByteKey.copyOf(InvocationMessages.encode(invocation))))));
      when(retirement.retire(any(), any(), any(), any(), any(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(new SystemRecordRetirement.Outcome(true, false, 0)));
      var result = closure(records, retirement, DIGEST, 1)
          .collect(SystemRecordClosure.Domain.CANDIDATE, 0, Optional.empty(), 16, permit(scope), scope).join();
      assertEquals(expectedRetired, result.retired());
      assertEquals(1 - expectedRetired, result.pending());
    }
  }

  @Test
  void byteLimitedPageDoesNotEndCollectionBeforeTheContinuation() {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var records = mock(SystemRecordStore.class);
      var first = preparingInvocation(records, scope);
      var second = preparingInvocation(records, scope);
      when(records.scanBucket(SystemRecordType.INVOCATION, 0, Optional.empty(), 16, 8L << 20, scope))
          .thenReturn(CompletableFuture.completedFuture(new SystemRecordPage(List.of(first),
              Optional.of(first.identity()), false, 2, 2, 1)));
      when(records.scanBucket(SystemRecordType.INVOCATION, 0, Optional.of(first.identity()), 14, 8L << 20, scope))
          .thenReturn(CompletableFuture.completedFuture(page(second)));
      var result = closure(records, mock(HBaseSystemRecordStore.class), DIGEST, 1)
          .collect(SystemRecordClosure.Domain.INVOCATION, 0, Optional.empty(), 16, permit(scope), scope).join();
      assertEquals(2, result.examined());
      assertEquals(Optional.of(second.identity()), result.lastIdentity());
      assertTrue(result.exhausted());
    }
  }

  private static StoredRecord storedValue() {
    return StoredRecord.live(key(2), new StorageMetadata(2, 2, 1, 0, StorageMetadata.State.LIVE, 1), new byte[] {1});
  }

  private static SystemRecordEntry preparingInvocation(SystemRecordStore records, RequestScope scope) {
    var binding = InvocationBinding.begin(InvocationId.create(), DIGEST, DIGEST,
        TransactionId.create(), 1, 0, 1);
    var identity = ByteKey.copyOf(binding.invocationId().bytes());
    var encoded = ByteKey.copyOf(InvocationMessages.encode(binding));
    when(records.read(SystemRecordType.INVOCATION, identity, scope))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(encoded)));
    return new SystemRecordEntry(identity, encoded);
  }

  private static SystemRecordRetirement.Permit permit(RequestScope scope) {
    var permit = mock(SystemRecordRetirement.Permit.class);
    when(permit.safeRevision()).thenReturn(10L);
    when(permit.safeSequence()).thenReturn(10L);
    return permit;
  }

  private static void stubEmptyScans(SystemRecordStore records, RequestScope scope) {
    when(records.scanBucket(any(), anyInt(), any(), anyInt(), anyLong(), eq(scope)))
        .thenReturn(CompletableFuture.completedFuture(empty()));
  }

  private static void assertReferenced(SystemRecordClosure.Domain domain, SystemRecordEntry target,
      SystemRecordEntry reference) {
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var records = mock(SystemRecordStore.class);
      var retirement = mock(HBaseSystemRecordStore.class);
      var targetPage = page(target);
      stubEmptyScans(records, scope);
      when(records.scanBucket(eq(domain.recordType()), eq(0), any(), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(targetPage));
      when(records.scanBucket(eq(SystemRecordType.MANIFEST), eq(0), any(), anyInt(), anyLong(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(page(reference)));
      when(retirement.retire(any(), any(), any(), any(), any(), eq(scope)))
          .thenReturn(CompletableFuture.completedFuture(new SystemRecordRetirement.Outcome(true, false, 0)));
      var permit = permit(scope);
      var result = closure(records, retirement, DIGEST, 1)
          .collect(domain, 0, Optional.empty(), 16, permit, scope).join();
      assertEquals(1, result.examined());
      assertEquals(0, result.retired(), "a durable manifest reference must prevent retirement");
      assertEquals(1, result.pending());
    }
  }

  private static HBaseSystemRecordClosure closure(SystemRecordStore records, HBaseSystemRecordStore retirement,
      ByteKey profile, int buckets) {
    var authority = mock(SystemRecordGcAuthority.class);
    when(authority.validate(any(), any())).thenReturn(CompletableFuture.completedFuture(List.of()));
    return new HBaseSystemRecordClosure(records, retirement, profile, buckets, authority);
  }

  private static SystemRecordPage page(SystemRecordEntry entry) {
    return new SystemRecordPage(List.of(entry), Optional.of(entry.identity()), true, entry.value().length());
  }

  private static SystemRecordPage empty() {
    return new SystemRecordPage(List.of(), Optional.empty(), true, 0);
  }

  private static ByteKey key(int value) { return ByteKey.copyOf(new byte[] {(byte) value}); }
}
