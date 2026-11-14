package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.ExpiryCandidateRecord;
import io.etcdhbase.core.lease.LeaseBindingIdentity;
import io.etcdhbase.core.lease.LeaseEnumerationPointer;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CommitFinalizationServiceTest {
  @Test
  void archivesAndFinalizesSelectedMetadataOnlyCommitIdempotently() throws Exception {
    Fixture fixture = new Fixture(SystemRecordStore.CreateResult.ALREADY_PRESENT);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pending = new CommitOutcomeResolver.Resolution(
          CommitOutcomeResolver.State.SELECTED_PENDING, Optional.of(fixture.decision), Optional.empty());

      var result = fixture.service.finish(fixture.invocation, pending, scope).get();

      assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, result.state());
      assertEquals(1, fixture.control.state.finalizedSequence());
      assertTrue(fixture.control.state.publication().isEmpty());
      assertEquals(1, fixture.decisions.createCalls.get());
      assertEquals(2, fixture.decisions.readCalls.get());
      assertEquals(0, fixture.materializationCalls.get());
    }
  }

  @Test
  void materializesNonEmptySealedPlanBeforeAdvancingFinalizedSequence() throws Exception {
    Fixture fixture = new Fixture(SystemRecordStore.CreateResult.ALREADY_PRESENT, true, false);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pending = new CommitOutcomeResolver.Resolution(
          CommitOutcomeResolver.State.SELECTED_PENDING, Optional.of(fixture.decision), Optional.empty());

      var result = fixture.service.finish(fixture.invocation, pending, scope).get();

      assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, result.state());
      assertEquals(1, fixture.materializationCalls.get());
      assertEquals(1, fixture.control.state.finalizedSequence());
      assertTrue(fixture.control.state.publication().isEmpty());
    }
  }

  @Test
  void failedMaterializationDoesNotAdvanceFinalizedSequence() {
    Fixture fixture = new Fixture(SystemRecordStore.CreateResult.ALREADY_PRESENT, true, true);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pending = new CommitOutcomeResolver.Resolution(
          CommitOutcomeResolver.State.SELECTED_PENDING, Optional.of(fixture.decision), Optional.empty());

      assertThrows(ExecutionException.class,
          () -> fixture.service.finish(fixture.invocation, pending, scope).get());

      assertEquals(1, fixture.materializationCalls.get());
      assertEquals(0, fixture.control.state.finalizedSequence());
      assertTrue(fixture.control.state.publication().isPresent());
    }
  }

  @Test
  void conflictingDecisionArchiveCannotAdvanceFinalizedSequence() {
    Fixture fixture = new Fixture(SystemRecordStore.CreateResult.CONFLICT);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pending = new CommitOutcomeResolver.Resolution(
          CommitOutcomeResolver.State.SELECTED_PENDING, Optional.of(fixture.decision), Optional.empty());

      assertThrows(ExecutionException.class,
          () -> fixture.service.finish(fixture.invocation, pending, scope).get());

      assertEquals(0, fixture.control.state.finalizedSequence());
      assertTrue(fixture.control.state.publication().isPresent());
      assertEquals(0, fixture.decisions.readCalls.get());
    }
  }

  @Test
  void businessResultMustBeSupportedAndDurableBeforeFinalization() throws Exception {
    for (boolean equipped : List.of(false, true)) {
      Fixture fixture = new Fixture(SystemRecordStore.CreateResult.ALREADY_PRESENT,
          true, false, true, false, equipped);
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
        var pending = new CommitOutcomeResolver.Resolution(
            CommitOutcomeResolver.State.SELECTED_PENDING, Optional.of(fixture.decision), Optional.empty());
        assertThrows(ExecutionException.class, () -> fixture.service.finish(fixture.invocation, pending, scope).get());
        assertEquals(0, fixture.materializationCalls.get());
        assertEquals(0, fixture.control.state.finalizedSequence());
      }
    }
    Fixture complete = new Fixture(SystemRecordStore.CreateResult.ALREADY_PRESENT,
        true, false, true, true, true);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pending = new CommitOutcomeResolver.Resolution(
          CommitOutcomeResolver.State.SELECTED_PENDING, Optional.of(complete.decision), Optional.empty());
      assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED,
          complete.service.finish(complete.invocation, pending, scope).get().state());
      assertEquals(1, complete.materializationCalls.get());
    }
  }

  @Test
  void upgradedFinalizerStillRecoversLegacyRoots() throws Exception {
    Fixture fixture = new Fixture(SystemRecordStore.CreateResult.ALREADY_PRESENT,
        false, false, false, false, true);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pending = new CommitOutcomeResolver.Resolution(
          CommitOutcomeResolver.State.SELECTED_PENDING, Optional.of(fixture.decision), Optional.empty());
      assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED,
          fixture.service.finish(fixture.invocation, pending, scope).get().state());
    }
  }

  @Test
  void selectedAuxiliaryRecordsMustFinishBeforeFinalizedWatermarkAdvances() throws Exception {
    AuxiliaryFixture fixture = new AuxiliaryFixture();
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pending = new CommitOutcomeResolver.Resolution(
          CommitOutcomeResolver.State.SELECTED_PENDING, Optional.of(fixture.decision), Optional.empty());

      assertThrows(ExecutionException.class,
          () -> fixture.service.finish(fixture.invocation, pending, scope).get());
      assertEquals(0, fixture.control.state.finalizedSequence());
      assertTrue(fixture.control.state.publication().isPresent());
      assertEquals(1, fixture.materialized.size());

      var recovered = fixture.service.finish(fixture.invocation, pending, scope).get();
      assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, recovered.state());
      assertEquals(4, fixture.materialized.size());
      assertEquals(6, fixture.materializationCalls.get());
      assertEquals(1, fixture.control.state.finalizedSequence());
      assertTrue(fixture.control.state.publication().isEmpty());
    }
  }

  @Test
  void selectedLeaseBindingRecordIsReplayableAfterRestartAtItsMaterializationBoundary() throws Exception {
    AuxiliaryFixture fixture = new AuxiliaryFixture(false, 3);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pending = new CommitOutcomeResolver.Resolution(
          CommitOutcomeResolver.State.SELECTED_PENDING, Optional.of(fixture.decision), Optional.empty());

      assertThrows(ExecutionException.class,
          () -> fixture.service.finish(fixture.invocation, pending, scope).get());
      assertEquals(3, fixture.materialized.size(), "state, pointer, and expiry should already be durable");
      assertFalse(fixture.materialized.contains(fixture.leaseBindingSystemIdentity));
      assertEquals(0, fixture.control.state.finalizedSequence());
      assertTrue(fixture.control.state.publication().isPresent());

      CommitFinalizationService recoveredService = fixture.reopenService();
      var recovered = recoveredService.finish(fixture.invocation, pending, scope).get();

      assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, recovered.state());
      assertTrue(fixture.materialized.contains(fixture.leaseBindingSystemIdentity));
      assertEquals(4, fixture.materialized.size());
      assertEquals(8, fixture.materializationCalls.get());
      assertEquals(1, fixture.control.state.finalizedSequence());
      assertTrue(fixture.control.state.publication().isEmpty());
    }
  }

  @Test
  void supersededAuxiliaryPointerCannotAdvancePendingCommit() {
    AuxiliaryFixture fixture = new AuxiliaryFixture(true);
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var pending = new CommitOutcomeResolver.Resolution(
          CommitOutcomeResolver.State.SELECTED_PENDING, Optional.of(fixture.decision), Optional.empty());

      assertThrows(ExecutionException.class,
          () -> fixture.service.finish(fixture.invocation, pending, scope).get());
      assertEquals(0, fixture.control.state.finalizedSequence());
      assertTrue(fixture.control.state.publication().isPresent());
      assertEquals(2, fixture.materializationCalls.get());
    }
  }

  private static final class Fixture {
    private final TransactionId transactionId = TransactionId.create();
    private final InvocationId invocationId = InvocationId.create();
    private final ManifestRoot root;
    private final InvocationBinding invocation;
    private final CommitDecision decision;
    private final PreparedCandidate candidate;
    private final MemoryControl control = new MemoryControl();
    private final MemoryDecisions decisions;
    private final AtomicInteger materializationCalls = new AtomicInteger();
    private final CommitFinalizationService service;

    private Fixture(SystemRecordStore.CreateResult archiveResult) {
      this(archiveResult, false, false);
    }

    private Fixture(SystemRecordStore.CreateResult archiveResult,
        boolean withCandidate, boolean failMaterialization) {
      this(archiveResult, withCandidate, failMaterialization, false, false, false);
    }

    private Fixture(SystemRecordStore.CreateResult archiveResult, boolean withCandidate,
        boolean failMaterialization, boolean withResult, boolean resultAvailable, boolean equipped) {
      InvocationBinding preparing = InvocationBinding.begin(invocationId,
          repeatedDigest(), repeatedDigest(), transactionId, 1, 0, 1);
      ResultIdentity resultIdentity = ResultIdentity.from(preparing);
      ResultPage resultPage = new ResultPage(resultIdentity, 0, new byte[] {1});
      ResultDirectory resultDirectory = ResultDirectory.create(resultIdentity,
          List.of(ResultPageReference.from(resultPage)));
      if (withCandidate) {
        byte[] value = {7};
        candidate = new PreparedCandidate(transactionId, 1, 0, 1,
            StoredRecord.live(ByteKey.copyOf(new byte[] {'k'}),
                new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, value.length), value));
        ManifestPage page = ManifestPage.create(transactionId, 0,
            List.of(ManifestCandidateReference.from(candidate, 0)));
        root = withResult
            ? ManifestRoot.createWithResult(transactionId, 1, 0, 1, 1, value.length,
                Optional.of(page.reference()), 0, 0, Optional.empty(), resultDirectory.reference())
            : ManifestRoot.create(transactionId, 1, 0, 1, 1, value.length, Optional.of(page.reference()));
        invocation = preparing.seal(root.systemIdentity());
        decision = new CommitDecision(transactionId, invocationId,
            1, 1, 2, root.systemIdentity(), root.digest(), true);
      } else {
        candidate = null;
        root = withResult
            ? ManifestRoot.createWithResult(transactionId, 1, 0, 1, 0, 0, Optional.empty(),
                0, 0, Optional.empty(), resultDirectory.reference())
            : ManifestRoot.create(transactionId, 1, 0, 1, 0, 0, Optional.empty());
        invocation = preparing.seal(root.systemIdentity());
        decision = new CommitDecision(transactionId, invocationId,
            1, 1, 1, root.systemIdentity(), root.digest(), false);
      }
      CommitState state = CommitState.initial(repeatedDigest(), 1)
          .claimOwner("fixture-owner", 1, 100);
      PublicationSlot slot = new PublicationSlot(transactionId, invocationId, 1,
          0, 1, 1, withCandidate ? 2 : 1, withCandidate, root.systemIdentity(), root.digest());
      control.state = state.publish(slot);
      decisions = new MemoryDecisions(decision, archiveResult);
      ManifestRepository repository = new StubManifestRepository(root, candidate,
          withCandidate ? ManifestPage.create(transactionId, 0,
              List.of(ManifestCandidateReference.from(candidate, 0))) : null);
      ManifestCatalog catalog = new ManifestCatalog(repository);
      MaterializationStore materialization = new MaterializationStore() {
        @Override
        public CompletableFuture<ApplyResult> applySelected(
            CommitDecision selected, PreparedCandidate candidate, RequestScope scope) {
          materializationCalls.incrementAndGet();
          return failMaterialization
              ? CompletableFuture.failedFuture(new IllegalStateException("materialization failed"))
              : CompletableFuture.completedFuture(ApplyResult.APPLIED);
        }

        @Override
        public CompletableFuture<Void> preflight(PreparedCandidate candidate, RequestScope scope) {
          return CompletableFuture.completedFuture(null);
        }
      };
      ResultRepository resultRepository = new ResultRepository() {
        @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(
            ResultPage page, RequestScope scope) { throw new UnsupportedOperationException(); }
        @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectory(
            ResultDirectory directory, RequestScope scope) { throw new UnsupportedOperationException(); }
        @Override public CompletableFuture<Optional<ResultPage>> readPage(
            ResultIdentity identity, int index, RequestScope scope) {
          return CompletableFuture.completedFuture(resultAvailable ? Optional.of(resultPage) : Optional.empty());
        }
        @Override public CompletableFuture<Optional<ResultDirectory>> readDirectory(
            ResultIdentity identity, RequestScope scope) {
          return CompletableFuture.completedFuture(resultAvailable ? Optional.of(resultDirectory) : Optional.empty());
        }
      };
      service = equipped
          ? new CommitFinalizationService(control, new CommitOutcomeResolver(control, decisions),
              decisions, catalog, materialization, new ResultCatalog(resultRepository))
          : new CommitFinalizationService(control, new CommitOutcomeResolver(control, decisions),
              decisions, catalog, materialization);
    }
  }

  private static final class AuxiliaryFixture {
    private final TransactionId transactionId = TransactionId.create();
    private final InvocationId invocationId = InvocationId.create();
    private final InvocationBinding invocation;
    private final CommitDecision decision;
    private final MemoryControl control = new MemoryControl();
    private final MemoryDecisions decisions;
    private final AtomicInteger materializationCalls = new AtomicInteger();
    private final java.util.Set<ByteKey> materialized = new java.util.HashSet<>();
    private final ManifestCatalog catalog;
    private final MaterializationStore materialization;
    private final ResultCatalog results;
    private final AuxiliaryMaterializationStore auxiliaryMaterialization;
    private final ByteKey leaseBindingSystemIdentity;
    private final CommitFinalizationService service;

    private AuxiliaryFixture() {
      this(false, 1);
    }

    private AuxiliaryFixture(boolean superseded) {
      this(superseded, 1);
    }

    private AuxiliaryFixture(boolean superseded, long injectedFailureOrdinal) {
      InvocationBinding preparing = InvocationBinding.begin(invocationId,
          repeatedDigest(), repeatedDigest(), transactionId, 1, 0, 1);
      ResultIdentity resultIdentity = ResultIdentity.from(preparing);
      ResultPage resultPage = new ResultPage(resultIdentity, 0, new byte[] {3, 4});
      ResultDirectory resultDirectory = ResultDirectory.create(resultIdentity,
          List.of(ResultPageReference.from(resultPage)));
      List<PreparedAuxiliaryRecord> auxiliaryRecords = auxiliaryRecords(transactionId);
      leaseBindingSystemIdentity = auxiliaryRecords.getLast().systemIdentity();
      List<AuxiliaryRecordReference> recordReferences = auxiliaryRecords.stream()
          .map(record -> new AuxiliaryRecordReference(record.ordinal(), record.systemIdentity(),
              record.digest(), record.encodedBytes())).toList();
      AuxiliaryPage auxiliaryPage = AuxiliaryPage.create(transactionId, 0, recordReferences);
      AuxiliaryDirectory auxiliaryDirectory = AuxiliaryDirectory.create(transactionId,
          List.of(auxiliaryPage.reference()));
      ManifestRoot root = ManifestRoot.createWithResultAndAuxiliary(transactionId, 1, 0, 1,
          0, 0, Optional.empty(), 0, 0, Optional.empty(), resultDirectory.reference(),
          auxiliaryDirectory.reference());
      invocation = preparing.seal(root.systemIdentity());
      decision = new CommitDecision(transactionId, invocationId,
          1, 1, 1, root.systemIdentity(), root.digest(), false);
      CommitState state = CommitState.initial(repeatedDigest(), 1)
          .claimOwner("auxiliary-owner", 1, 100);
      control.state = state.publish(new PublicationSlot(transactionId, invocationId, 1,
          0, 1, 1, 1, false, root.systemIdentity(), root.digest()));
      decisions = new MemoryDecisions(decision, SystemRecordStore.CreateResult.ALREADY_PRESENT);
      ManifestRepository repository = new StubManifestRepository(root, null, null, auxiliaryRecords,
          auxiliaryPage, auxiliaryDirectory);
      catalog = new ManifestCatalog(repository);
      materialization = new MaterializationStore() {
        @Override public CompletableFuture<ApplyResult> applySelected(
            CommitDecision selected, PreparedCandidate candidate, RequestScope scope) {
          throw new UnsupportedOperationException();
        }
        @Override public CompletableFuture<Void> preflight(
            PreparedCandidate candidate, RequestScope scope) {
          throw new UnsupportedOperationException();
        }
      };
      ResultRepository resultRepository = new ResultRepository() {
        @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(
            ResultPage page, RequestScope scope) { throw new UnsupportedOperationException(); }
        @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectory(
            ResultDirectory directory, RequestScope scope) { throw new UnsupportedOperationException(); }
        @Override public CompletableFuture<Optional<ResultPage>> readPage(
            ResultIdentity identity, int index, RequestScope scope) {
          return CompletableFuture.completedFuture(Optional.of(resultPage));
        }
        @Override public CompletableFuture<Optional<ResultDirectory>> readDirectory(
            ResultIdentity identity, RequestScope scope) {
          return CompletableFuture.completedFuture(Optional.of(resultDirectory));
        }
      };
      results = new ResultCatalog(resultRepository);
      AtomicInteger injectedFailures = new AtomicInteger();
      auxiliaryMaterialization = new AuxiliaryMaterializationStore() {
        @Override public CompletableFuture<Void> preflight(
            PreparedAuxiliaryRecord record, RequestScope scope) {
          return CompletableFuture.completedFuture(null);
        }
        @Override public CompletableFuture<ApplyResult> applySelected(
            CommitDecision selected, PreparedAuxiliaryRecord record, RequestScope scope) {
          materializationCalls.incrementAndGet();
          if (record.ordinal() == 1 && superseded) {
            return CompletableFuture.completedFuture(ApplyResult.SUPERSEDED);
          }
          if (record.ordinal() == injectedFailureOrdinal && injectedFailures.getAndIncrement() == 0) {
            return CompletableFuture.failedFuture(new IllegalStateException("injected auxiliary write failure"));
          }
          return CompletableFuture.completedFuture(materialized.add(record.systemIdentity())
              ? ApplyResult.APPLIED : ApplyResult.ALREADY_APPLIED);
        }
      };
      service = reopenService();
    }

    private CommitFinalizationService reopenService() {
      return new CommitFinalizationService(control, new CommitOutcomeResolver(control, decisions),
          decisions, catalog, materialization, results, auxiliaryMaterialization);
    }
  }

  private static final class MemoryControl implements CommitControlStore {
    private CommitState state;
    private int version;

    @Override
    public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(snapshot()));
    }

    @Override
    public CompletableFuture<Boolean> initializeIfAbsent(CommitState initialState, RequestScope scope) {
      return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      if (!java.util.Arrays.equals(observed.comparisonToken(), token())) {
        return CompletableFuture.completedFuture(false);
      }
      state = replacement;
      version++;
      return CompletableFuture.completedFuture(true);
    }

    private ControlSnapshot snapshot() {
      return new ControlSnapshot(state, token());
    }

    private byte[] token() {
      return java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(version).array();
    }
  }

  private static final class MemoryDecisions implements CommitDecisionRepository {
    private final CommitDecision decision;
    private final SystemRecordStore.CreateResult createResult;
    private final AtomicInteger createCalls = new AtomicInteger();
    private final AtomicInteger readCalls = new AtomicInteger();

    private MemoryDecisions(CommitDecision decision, SystemRecordStore.CreateResult createResult) {
      this.decision = decision;
      this.createResult = createResult;
    }

    @Override
    public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope) {
      readCalls.incrementAndGet();
      return CompletableFuture.completedFuture(sequence == decision.sequence()
          ? Optional.of(decision) : Optional.empty());
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        CommitDecision value, RequestScope scope) {
      createCalls.incrementAndGet();
      return CompletableFuture.completedFuture(createResult);
    }
  }

  private static final class StubManifestRepository implements ManifestRepository {
    private final ManifestRoot root;
    private final PreparedCandidate candidate;
    private final ManifestPage page;
    private final List<PreparedAuxiliaryRecord> auxiliaryRecords;
    private final AuxiliaryPage auxiliaryPage;
    private final AuxiliaryDirectory auxiliaryDirectory;

    private StubManifestRepository(ManifestRoot root, PreparedCandidate candidate, ManifestPage page) {
      this(root, candidate, page, List.of(), null, null);
    }

    private StubManifestRepository(ManifestRoot root, PreparedCandidate candidate, ManifestPage page,
        List<PreparedAuxiliaryRecord> auxiliaryRecords, AuxiliaryPage auxiliaryPage,
        AuxiliaryDirectory auxiliaryDirectory) {
      this.root = root;
      this.candidate = candidate;
      this.page = page;
      this.auxiliaryRecords = List.copyOf(auxiliaryRecords);
      this.auxiliaryPage = auxiliaryPage;
      this.auxiliaryDirectory = auxiliaryDirectory;
    }

    @Override public int candidateBucket(PreparedCandidate candidate) { return 0; }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(
        PreparedCandidate candidate, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(
        ManifestPage page, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(
        ManifestDirectoryPage page, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEvent(
        PreparedEvent event, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(
        EventPage page, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(
        EventDirectoryPage page, RequestScope scope) { return created(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> seal(
        ManifestRoot value, RequestScope scope) { return created(); }
    @Override public CompletableFuture<Optional<ManifestRoot>> readRoot(
        ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(identity.equals(root.systemIdentity())
          ? Optional.of(root) : Optional.empty());
    }
    @Override public CompletableFuture<Optional<PreparedCandidate>> readCandidate(
        ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(candidate != null && candidate.systemIdentity().equals(identity)
          ? Optional.of(candidate) : Optional.empty());
    }
    @Override public CompletableFuture<Optional<ManifestPage>> readPage(
        ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(page != null && page.systemIdentity().equals(identity)
          ? Optional.of(page) : Optional.empty());
    }
    @Override public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<PreparedEvent>> readEvent(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<EventPage>> readEventPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(
        ByteKey identity, RequestScope scope) { return CompletableFuture.completedFuture(Optional.empty()); }
    @Override public CompletableFuture<Optional<PreparedAuxiliaryRecord>> readAuxiliaryRecord(
        ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(auxiliaryRecords.stream()
          .filter(record -> record.systemIdentity().equals(identity)).findFirst());
    }
    @Override public CompletableFuture<Optional<AuxiliaryPage>> readAuxiliaryPage(
        ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(auxiliaryPage != null
          && auxiliaryPage.systemIdentity().equals(identity) ? Optional.of(auxiliaryPage) : Optional.empty());
    }
    @Override public CompletableFuture<Optional<AuxiliaryDirectory>> readAuxiliaryDirectory(
        ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(auxiliaryDirectory != null
          && auxiliaryDirectory.systemIdentity().equals(identity)
              ? Optional.of(auxiliaryDirectory) : Optional.empty());
    }

    private static CompletableFuture<SystemRecordStore.CreateResult> created() {
      return CompletableFuture.completedFuture(SystemRecordStore.CreateResult.CREATED);
    }
  }

  private static List<PreparedAuxiliaryRecord> auxiliaryRecords(TransactionId transactionId) {
    long leaseId = 31;
    ByteKey bindingKey = ByteKey.copyOf(new byte[] {'k'});
    LeaseBindingIdentity bindingIdentity = new LeaseBindingIdentity(leaseId, 1, bindingKey, 1);
    return List.of(
        PreparedAuxiliaryRecord.create(transactionId, 1, 0, 0, SystemRecordType.LEASE_STATE,
            LeaseRecordIdentity.stateVersion(leaseId, 1), ByteKey.copyOf(new byte[] {1})),
        PreparedAuxiliaryRecord.create(transactionId, 1, 0, 1, SystemRecordType.LEASE_ENUMERATION,
            LeaseRecordIdentity.enumeration(leaseId), new LeaseEnumerationPointer(1).encode()),
        PreparedAuxiliaryRecord.create(transactionId, 1, 0, 2, SystemRecordType.EXPIRY_CANDIDATE,
            new ExpiryCandidateIdentity(20_000, leaseId, 1, 0).encode(),
            new ExpiryCandidateRecord(1).encode()),
        PreparedAuxiliaryRecord.create(transactionId, 1, 0, 3, SystemRecordType.LEASE_BINDING,
            bindingIdentity.encode(), ByteKey.copyOf(new byte[] {4})));
  }

  private static ByteKey repeatedDigest() {
    return ByteKey.copyOf(new byte[32]);
  }
}
