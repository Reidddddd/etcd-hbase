package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseBindingIdentity;
import io.etcdhbase.core.lease.LeaseBindingVersion;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseEnumerationPointer;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class LeaseReadViewTest {
  @Test
  void enumeratesStableAndSelectedLeasesWithoutIncludingFutureOnlyPointers() throws Exception {
    try (Fixture fixture = new Fixture(true); var budget = new EnumerationBudget(fixture.scope)) {
      fixture.records.pointer(9, 2);
      fixture.records.pointer(11, 5);
      var metadata = new ManifestCatalog.ProofReadBudget(fixture.scope, budget.account,
          16, ManifestCatalog.PROOF_SYSTEM_RECORD_ENVELOPE);
      var reader = fixture.enumerator(10, 10);
      assertEquals(List.of(fixture.selected, fixture.other),
          reader.read(fixture.pinned, fixture.scope, budget.account, metadata).join());
      assertTrue(metadata.bytes() < ManifestCatalog.PROOF_SYSTEM_RECORD_ENVELOPE);
      fixture.reader(10, 4096).finish(fixture.pinned, fixture.scope).join();
    }
  }

  @Test
  void enumerationFailsForMissingStateOrIncompleteResult() throws Exception {
    try (Fixture fixture = new Fixture(true); var budget = new EnumerationBudget(fixture.scope)) {
      fixture.records.pointer(9, 3);
      assertThrows(CompletionException.class,
          () -> fixture.enumerator(10, 10).read(fixture.pinned, fixture.scope,
              budget.account, budget.metadata).join());
    }
    try (Fixture fixture = new Fixture(true); var budget = new EnumerationBudget(fixture.scope)) {
      fixture.records.pointer(9, 2);
      var failure = assertThrows(CompletionException.class,
          () -> fixture.enumerator(10, 1).read(fixture.pinned, fixture.scope,
              budget.account, budget.metadata).join());
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, ((BackendFailure) failure.getCause()).category());
    }
  }

  @Test
  void enumerationReservesMemoryForVisibleStatesRatherThanTheConfiguredCeiling() throws Exception {
    try (Fixture fixture = new Fixture(true); var budget = new EnumerationBudget(fixture.scope)) {
      fixture.records.pointer(9, 2);
      fixture.records.pointer(11, 5);
      var reader = fixture.enumerator(100_000, 100_000, 1_024, 64L << 20, 1, 4096);

      assertEquals(List.of(fixture.selected, fixture.other),
          reader.read(fixture.pinned, fixture.scope, budget.account, budget.metadata).join());
    }
  }

  @Test
  void emptyEnumerationReservesScanPagesButChargesOnlyReturnedBytes() throws Exception {
    try (Fixture fixture = new Fixture(false, false); var budget = new EnumerationBudget(fixture.scope, 1)) {
      var metadata = new ManifestCatalog.ProofReadBudget(fixture.scope, budget.account, 3, 1);

      assertTrue(fixture.enumerator(10, 10).read(
          fixture.pinned, fixture.scope, budget.account, metadata).join().isEmpty());
      assertEquals(2, metadata.pages());
      assertEquals(0, metadata.bytes());
    }
  }

  @Test
  void ttlWithNoBindingsFitsOneMetadataBudgetAfterReadingLeaseState() throws Exception {
    try (Fixture fixture = new Fixture(1); var budget = new EnumerationBudget(fixture.scope)) {
      var metadata = new ManifestCatalog.ProofReadBudget(
          fixture.scope, budget.account, 8, ManifestCatalog.PROOF_SYSTEM_RECORD_ENVELOPE);
      LeaseState lease = fixture.reader(10, 4096).find(fixture.pinned, -7,
          fixture.scope, metadata).join().orElseThrow();

      assertTrue(fixture.bindingReader().read(
          fixture.pinned, lease, fixture.scope, budget.account, metadata).join().isEmpty());
      assertEquals(2, metadata.pages());
      assertEquals(17, metadata.bytes());
    }
  }

  @Test
  void enumerationCancellationRetainsPinAndBudgetUntilPhysicalReadDrains() throws Exception {
    try (Fixture fixture = new Fixture(true); var budget = new EnumerationBudget(fixture.scope)) {
      fixture.records.pointer(9, 2);
      fixture.records.blocked = new CompletableFuture<>();
      var producer = new CompletableFuture<Void>();
      fixture.scope.trackPhysicalProducer(producer);
      var result = fixture.enumerator(10, 10).read(fixture.pinned, fixture.scope,
          budget.account, budget.metadata);
      fixture.pinned.close();
      fixture.records.blocked.complete(Optional.of(new SystemRecordEntry(
          LeaseRecordIdentity.stateVersion(9, 2), ByteKey.copyOf(new byte[] {2}))));
      assertThrows(CompletionException.class, result::join);
      assertEquals(0, fixture.pin.closes);
      assertTrue(budget.manager.snapshot().allocatedBytes() > 0);
      producer.complete(null);
      fixture.scope.cleanupCompletion().toCompletableFuture().join();
      assertEquals(1, fixture.pin.closes);
      assertEquals(0, budget.manager.snapshot().allocatedBytes());
    }
  }

  @Test
  void pointReadChargesStableAndSelectedMetadataBeforeReturning() throws Exception {
    try (Fixture fixture = new Fixture(); var budget = new EnumerationBudget(fixture.scope)) {
      var limited = new ManifestCatalog.ProofReadBudget(fixture.scope, budget.account, 3, 8L << 30);
      var failure = assertThrows(CompletionException.class, () -> fixture.reader(10, 4096)
          .find(fixture.pinned, -7, fixture.scope, limited).join());
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, ((BackendFailure) failure.getCause()).category());
      assertEquals(3, limited.pages());
      assertEquals(fixture.selected, fixture.reader(10, 4096)
          .find(fixture.pinned, -7, fixture.scope, budget.metadata).join().orElseThrow());
    }
  }

  @Test
  void overlaysSelectedMetadataOnlyCommitAndKeepsOtherLeaseAtStableSequence() {
    try (Fixture fixture = new Fixture()) {
      assertFalse(fixture.pinned.view().includesSelectedKeyChanges());
      assertEquals(fixture.selected, fixture.reader(10, 4096).find(fixture.pinned, -7, fixture.scope)
          .join().orElseThrow());
      assertEquals(fixture.other, fixture.reader(10, 4096).find(fixture.pinned, 9, fixture.scope)
          .join().orElseThrow());
      assertTrue(fixture.reader(10, 4096).find(fixture.pinned, 11, fixture.scope).join().isEmpty());
      assertEquals(3, fixture.records.lastSequence);
      fixture.reader(10, 4096).finish(fixture.pinned, fixture.scope).join();
      assertEquals(1, fixture.pin.closes);
      assertFalse(fixture.pinned.isOpen());
      assertFalse(fixture.scope.isClosed(), "finishing one pinned view must not close its request scope");
    }
  }

  @Test
  void missingPageBudgetAndCorruptedStableStateFailClosed() {
    try (Fixture fixture = new Fixture()) {
      CompletionException budget = assertThrows(CompletionException.class,
          () -> fixture.reader(10, 1).find(fixture.pinned, -7, fixture.scope).join());
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
          ((BackendFailure) budget.getCause()).category());
      fixture.manifests.page = null;
      assertThrows(CompletionException.class,
          () -> fixture.reader(10, 4096).find(fixture.pinned, -7, fixture.scope).join());
      fixture.records.rows.put(LeaseRecordIdentity.stateVersion(9, 3), ByteKey.copyOf(new byte[] {2}));
      assertThrows(CompletionException.class,
          () -> fixture.reader(10, 4096).find(fixture.pinned, 9, fixture.scope).join());
    }
  }

  @Test
  void holdsPinUntilCancelledReadSettles() {
    try (Fixture fixture = new Fixture()) {
      fixture.records.blocked = new CompletableFuture<>();
      var pending = fixture.reader(10, 4096).find(fixture.pinned, -7, fixture.scope);
      fixture.pinned.close();
      assertEquals(0, fixture.pin.closes);
      fixture.records.blocked.complete(Optional.empty());
      assertThrows(CompletionException.class, pending::join);
      assertEquals(1, fixture.pin.closes);
    }
  }

  @Test
  void refusesToUseOrFinishPinFromAnotherRequest() {
    try (Fixture fixture = new Fixture(); RequestScope other = RequestScope.withTimeout(Duration.ofSeconds(1))) {
      assertThrows(CompletionException.class,
          () -> fixture.reader(10, 4096).find(fixture.pinned, -7, other).join());
      assertThrows(CompletionException.class,
          () -> fixture.reader(10, 4096).finish(fixture.pinned, other).join());
      assertTrue(fixture.pinned.isOpen());
    }
  }

  @Test
  void refusesPinThatDoesNotProtectAuxiliaryHistory() {
    try (Fixture fixture = new Fixture()) {
      var unprotected = new PinnedReadView(fixture.pinned.view(), new Pin(), fixture.scope);
      assertThrows(CompletionException.class,
          () -> fixture.reader(10, 4096).find(unprotected, -7, fixture.scope).join());
      assertThrows(CompletionException.class,
          () -> fixture.reader(10, 4096).finish(unprotected, fixture.scope).join());
    }
  }

  @Test
  void bindingReadMergesStableHistoryWithSelectedChangesAtOneBarrier() throws Exception {
    try (BindingFixture fixture = new BindingFixture();
        var budget = new EnumerationBudget(fixture.scope)) {
      LeaseBindingReader reader = fixture.reader(10, 1L << 20);
      assertEquals(List.of(fixture.keyB, fixture.keyC), reader.read(fixture.pinned, fixture.lease,
          fixture.scope, budget.account, budget.metadata).join());
    }
  }

  @Test
  void bindingReadFailsAsAWholeWhenTheOutputBudgetIsExceeded() throws Exception {
    try (BindingFixture fixture = new BindingFixture();
        var budget = new EnumerationBudget(fixture.scope)) {
      var failure = assertThrows(CompletionException.class, () -> fixture.reader(1, 1L << 20)
          .read(fixture.pinned, fixture.lease, fixture.scope, budget.account, budget.metadata).join());
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
          ((BackendFailure) failure.getCause()).category());
    }
  }

  @Test
  void bindingReadReservesMemoryForTheLeaseRatherThanMaximumLimits() throws Exception {
    try (BindingFixture fixture = new BindingFixture();
        var budget = new EnumerationBudget(fixture.scope)) {
      LeaseBindingReader reader = fixture.reader(100_000, 4L << 20, 1_024, 64L << 20);

      assertEquals(List.of(fixture.keyB, fixture.keyC), reader.read(fixture.pinned, fixture.lease,
          fixture.scope, budget.account, budget.metadata).join());
    }
  }

  private static final class BindingFixture implements AutoCloseable {
    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
    private final LeaseIdentity identity = new LeaseIdentity(-7, 1);
    private final ByteKey keyA = ByteKey.copyOf(new byte[] {'a'});
    private final ByteKey keyB = ByteKey.copyOf(new byte[] {'b'});
    private final ByteKey keyC = ByteKey.copyOf(new byte[] {'c'});
    private final ByteKey keyD = ByteKey.copyOf(new byte[] {'d'});
    private final LeaseState lease = new LeaseState(identity, 10, 10_000, 0,
        LeaseState.Status.ACTIVE, 4, 2, 2, 0);
    private final Map<ByteKey, LeaseBindingVersion> decoded = new HashMap<>();
    private final Records records = new Records();
    private final Manifests manifests;
    private final PinnedReadView pinned;

    private BindingFixture() {
      addStable(new LeaseBindingVersion(identity, keyA, true, 2, 2, 2));
      addStable(new LeaseBindingVersion(identity, keyB, true, 3, 2, 3));
      addStable(new LeaseBindingVersion(identity, keyD, true, 5, 2, 5));
      var removedA = new LeaseBindingVersion(identity, keyA, false, 4, 2, 4);
      var addedC = new LeaseBindingVersion(identity, keyC, true, 4, 1, 4);
      ByteKey removedPayload = payload(removedA);
      ByteKey addedPayload = payload(addedC);
      TransactionId transaction = TransactionId.create();
      var selected = List.of(
          PreparedAuxiliaryRecord.create(transaction, 1, 3, 0, SystemRecordType.LEASE_BINDING,
              removedA.recordIdentity().encode(), removedPayload),
          PreparedAuxiliaryRecord.create(transaction, 1, 3, 1, SystemRecordType.LEASE_BINDING,
              addedC.recordIdentity().encode(), addedPayload));
      var references = selected.stream().map(value -> new AuxiliaryRecordReference(value.ordinal(),
          value.systemIdentity(), value.digest(), value.encodedBytes())).toList();
      var page = AuxiliaryPage.create(transaction, 0, references);
      var directory = AuxiliaryDirectory.create(transaction, List.of(page.reference()));
      var root = ManifestRoot.createWithAuxiliary(transaction, 1, 3, 1,
          Optional.of(directory.reference()));
      var decision = new CommitDecision(transaction, InvocationId.create(), 4, 1, 1,
          root.systemIdentity(), root.digest(), false);
      var view = new ReadView(ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]),
          4, 3, 1, 1, 0, Optional.of(decision), Optional.of(root), 1);
      manifests = new Manifests(selected, page, directory);
      pinned = new PinnedReadView(view, new Pin(), scope, true);
    }

    private void addStable(LeaseBindingVersion binding) {
      ByteKey payload = payload(binding);
      records.bindingRows.put(binding.recordIdentity().encode(), payload);
    }

    private ByteKey payload(LeaseBindingVersion binding) {
      ByteKey payload = ByteKey.copyOf(new byte[] {(byte) decoded.size(), 42});
      decoded.put(payload, binding);
      return payload;
    }

    private LeaseBindingReader reader(int maximumBindings, long maximumOutputBytes) {
      return reader(maximumBindings, maximumOutputBytes, 10, 1L << 20);
    }

    private LeaseBindingReader reader(int maximumBindings, long maximumOutputBytes,
        int maximumOverlayRecords, long maximumOverlayBytes) {
      return new LeaseBindingReader(records, new ManifestCatalog(manifests), decoded::get,
          1, 4096, 100, 1L << 20, maximumBindings, maximumOutputBytes,
          maximumOverlayRecords, maximumOverlayBytes);
    }

    @Override public void close() { pinned.close(); }
  }

  private static final class Fixture implements AutoCloseable {
    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
    private final LeaseState selected = LeaseState.grant(new LeaseIdentity(-7, 1), 5, 5_000, 4);
    private final LeaseState other = LeaseState.grant(new LeaseIdentity(9, 1), 5, 6_000, 2);
    private final Map<ByteKey, LeaseState> decoded = new HashMap<>();
    private final Records records = new Records();
    private final Manifests manifests;
    private final Pin pin = new Pin();
    private final PinnedReadView pinned;

    private Fixture() { this(false, true); }

    private Fixture(boolean includeSelectedPointer) { this(includeSelectedPointer, true); }

    private Fixture(boolean includeSelectedPointer, boolean includeSelectedRoot) {
      ByteKey selectedPayload = ByteKey.copyOf(new byte[] {1});
      ByteKey otherPayload = ByteKey.copyOf(new byte[] {2});
      decoded.put(selectedPayload, selected);
      decoded.put(otherPayload, other);
      records.rows.put(LeaseRecordIdentity.stateVersion(9, 2), otherPayload);
      if (!includeSelectedRoot) {
        manifests = new Manifests(List.of(), null, null);
        var view = new ReadView(ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]),
            0, 0, 1, 1, 0, Optional.empty(), Optional.empty(), 1);
        pinned = new PinnedReadView(view, pin, scope, true);
        return;
      }
      TransactionId transaction = TransactionId.create();
      var record = PreparedAuxiliaryRecord.create(transaction, 1, 3, 0,
          SystemRecordType.LEASE_STATE, LeaseRecordIdentity.stateVersion(-7, 4), selectedPayload);
      var selectedRecords = new java.util.ArrayList<PreparedAuxiliaryRecord>();
      selectedRecords.add(record);
      if (includeSelectedPointer) selectedRecords.add(PreparedAuxiliaryRecord.create(transaction, 1, 3, 1,
          SystemRecordType.LEASE_ENUMERATION, LeaseRecordIdentity.enumeration(-7),
          new LeaseEnumerationPointer(4).encode()));
      var references = selectedRecords.stream().map(value -> new AuxiliaryRecordReference(value.ordinal(),
          value.systemIdentity(), value.digest(), value.encodedBytes())).toList();
      var page = AuxiliaryPage.create(transaction, 0, references);
      var directory = AuxiliaryDirectory.create(transaction, List.of(page.reference()));
      var root = ManifestRoot.createWithAuxiliary(transaction, 1, 3, 1,
          Optional.of(directory.reference()));
      var decision = new CommitDecision(transaction, InvocationId.create(), 4, 1, 1,
          root.systemIdentity(), root.digest(), false);
      var view = new ReadView(ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]),
          4, 3, 1, 1, 0, Optional.of(decision), Optional.of(root), 1);
      manifests = new Manifests(selectedRecords, page, directory);
      pinned = new PinnedReadView(view, pin, scope, true);
    }

    private Fixture(long stableSequence) {
      LeaseState stable = LeaseState.grant(new LeaseIdentity(-7, 1), 5, 5_000, stableSequence);
      ByteKey payload = ByteKey.copyOf(new byte[] {3});
      decoded.put(payload, stable);
      records.rows.put(LeaseRecordIdentity.stateVersion(-7, stableSequence), payload);
      manifests = new Manifests(List.of(), null, null);
      var view = new ReadView(ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]),
          stableSequence, stableSequence, 1, 1, 0, Optional.empty(), Optional.empty(), 1);
      pinned = new PinnedReadView(view, pin, scope, true);
    }

    private LeaseReadView reader(long count, long bytes) {
      return new LeaseReadView(records, new ManifestCatalog(manifests), decoded::get, count, bytes);
    }

    private LeaseBindingReader bindingReader() {
      return new LeaseBindingReader(records, new ManifestCatalog(manifests), payload -> {
        throw new IllegalStateException("empty binding fixture must not decode a record");
      }, 1, 4096, 100, 1L << 20, 10, 4096, 10, 4096);
    }

    private LeaseEnumerationReader enumerator(int maximumExamined, int maximumResults) {
      return enumerator(maximumExamined, maximumResults, 10, 4096, 1, 4096);
    }

    private LeaseEnumerationReader enumerator(int maximumExamined, int maximumResults,
        int maximumOverlayRecords, long maximumOverlayBytes, int pageEntries, long pageBytes) {
      return new LeaseEnumerationReader(records, new ManifestCatalog(manifests), decoded::get,
          2, pageEntries, pageBytes, maximumExamined, maximumResults,
          maximumOverlayRecords, maximumOverlayBytes);
    }

    @Override public void close() { pinned.close(); }
  }

  private static final class EnumerationBudget implements AutoCloseable {
    private final BudgetManager manager;
    private final BudgetManager.Account account;
    private final ManifestCatalog.ProofReadBudget metadata;

    private EnumerationBudget(RequestScope scope) throws BackendFailure {
      this(scope, 1000);
    }

    private EnumerationBudget(RequestScope scope, long pointOperations) throws BackendFailure {
      var limits = new BudgetManager.Limits(2, 2, 2, 2, 2, 1_000_000, 256L << 20);
      manager = new BudgetManager(limits, Map.of("public", limits), limits);
      var reservation = new BudgetManager.Reservation(
          8192, 32768, 128L << 20, 2L << 20, 4L << 20);
      account = manager.admit("public", scope, reservation,
          new BudgetManager.ReadLimits(1000, 8L << 30, 1000, 1000, pointOperations));
      metadata = new ManifestCatalog.ProofReadBudget(scope, account, 100, 8L << 30);
    }

    @Override public void close() { account.close(); }
  }

  private static final class Pin implements ReadViewPinRegistry.Pin {
    private int closes;
    @Override public long generation() { return 1; }
    @Override public void close() { closes++; }
  }

  private static final class Records implements SystemRecordStore {
    private final Map<ByteKey, ByteKey> rows = new HashMap<>();
    private final Map<ByteKey, ByteKey> pointers = new HashMap<>();
    private final Map<ByteKey, ByteKey> bindingRows = new HashMap<>();
    private long lastSequence;
    private CompletableFuture<Optional<SystemRecordEntry>> blocked;

    private void pointer(long id, long sequence) {
      pointers.put(LeaseRecordIdentity.enumeration(id), new LeaseEnumerationPointer(sequence).encode());
    }

    @Override public CompletableFuture<SystemRecordPage> scanBucket(SystemRecordType type, int bucket,
        Optional<ByteKey> after, int maximumEntries, long maximumBytes, RequestScope scope) {
      var matching = pointers.entrySet().stream().filter(row -> bucket(type, row.getKey()) == bucket)
          .filter(row -> after.isEmpty() || row.getKey().compareTo(after.orElseThrow()) > 0)
          .sorted(Map.Entry.comparingByKey()).toList();
      var page = matching.stream().limit(maximumEntries)
          .map(row -> new SystemRecordEntry(row.getKey(), row.getValue())).toList();
      return CompletableFuture.completedFuture(new SystemRecordPage(page,
          page.isEmpty() ? Optional.empty() : Optional.of(page.getLast().identity()),
          matching.size() <= maximumEntries,
          page.stream().mapToLong(row -> row.identity().length() + row.value().length()).sum()));
    }

    @Override public CompletableFuture<Optional<SystemRecordEntry>> latestAtOrBefore(
        SystemRecordType type, ByteKey route, long sequence, RequestScope scope) {
      lastSequence = sequence;
      if (blocked != null) return blocked;
      return CompletableFuture.completedFuture(rows.entrySet().stream()
          .filter(row -> LeaseRecordIdentity.routePrefix(LeaseRecordIdentity.leaseId(row.getKey())).equals(route))
          .filter(row -> LeaseRecordIdentity.sequence(row.getKey()) <= sequence)
          .max(Map.Entry.comparingByKey())
          .map(row -> new SystemRecordEntry(row.getKey(), row.getValue())));
    }
    @Override public CompletableFuture<SystemRecordPage> scanLeaseBindings(long leaseId,
        long incarnation, Optional<ByteKey> after, int maximumEntries, long maximumBytes,
        RequestScope scope) {
      var matching = bindingRows.entrySet().stream().filter(row -> {
        LeaseBindingIdentity.Decoded decoded = LeaseBindingIdentity.decode(row.getKey());
        return decoded.leaseId() == leaseId && decoded.incarnation() == incarnation;
      }).filter(row -> after.isEmpty() || row.getKey().compareTo(after.orElseThrow()) > 0)
          .sorted(Map.Entry.comparingByKey()).toList();
      var page = matching.stream().limit(maximumEntries)
          .map(row -> new SystemRecordEntry(row.getKey(), row.getValue())).toList();
      long encodedBytes = page.stream()
          .mapToLong(row -> row.identity().length() + row.value().length()).sum();
      return CompletableFuture.completedFuture(new SystemRecordPage(page,
          page.isEmpty() ? Optional.empty() : Optional.of(page.getLast().identity()),
          matching.size() <= maximumEntries, encodedBytes));
    }
    @Override public CompletableFuture<Optional<ByteKey>> read(
        SystemRecordType type, ByteKey identity, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public int bucket(SystemRecordType type, ByteKey identity) {
      if (type == SystemRecordType.LEASE_BINDING) {
        return (int) (LeaseBindingIdentity.decode(identity).leaseId() & 1);
      }
      return (int) (LeaseRecordIdentity.leaseId(identity) & 1);
    }
    @Override public CompletableFuture<CreateResult> createIfAbsent(
        SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Boolean> compareAndSet(SystemRecordType type, ByteKey identity,
        Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) { throw new UnsupportedOperationException(); }
  }

  private static final class Manifests implements ManifestRepository {
    private final Map<ByteKey, PreparedAuxiliaryRecord> records;
    private AuxiliaryPage page;
    private final AuxiliaryDirectory directory;
    private Manifests(List<PreparedAuxiliaryRecord> records, AuxiliaryPage page, AuxiliaryDirectory directory) {
      this.records = new HashMap<>();
      records.forEach(record -> this.records.put(record.systemIdentity(), record));
      this.page = page; this.directory = directory;
    }
    @Override public CompletableFuture<Optional<PreparedAuxiliaryRecord>> readAuxiliaryRecord(
        ByteKey id, RequestScope scope) { return CompletableFuture.completedFuture(Optional.ofNullable(records.get(id))); }
    @Override public CompletableFuture<Optional<AuxiliaryPage>> readAuxiliaryPage(
        ByteKey id, RequestScope scope) { return CompletableFuture.completedFuture(Optional.ofNullable(page)); }
    @Override public CompletableFuture<Optional<AuxiliaryDirectory>> readAuxiliaryDirectory(
        ByteKey id, RequestScope scope) { return CompletableFuture.completedFuture(Optional.of(directory)); }
    @Override public int candidateBucket(PreparedCandidate candidate) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(PreparedCandidate value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(ManifestPage value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(ManifestDirectoryPage value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEvent(PreparedEvent value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(EventPage value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(EventDirectoryPage value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> seal(ManifestRoot value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<ManifestRoot>> readRoot(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<PreparedCandidate>> readCandidate(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<ManifestPage>> readPage(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<PreparedEvent>> readEvent(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<EventPage>> readEventPage(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
  }
}
