package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class InvocationResultResolverTest {
  @Test
  void resolvesLostTransitionAcknowledgementAndReplaysTheSamePersistedBytes() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture.invocations.loseAcknowledgement = true;
      try (var first = fixture.resolve()) {
        assertArrayEquals(new byte[] {1, 2, 3}, bytes(first));
      }
      assertEquals(InvocationBinding.State.SELECTED, fixture.invocations.binding.state());
      assertEquals(fixture.reference.token(), fixture.invocations.binding.resultReference().orElseThrow());
      try (var replay = fixture.resolve()) {
        assertArrayEquals(new byte[] {1, 2, 3}, bytes(replay));
      }
      assertEquals(1, fixture.invocations.casCalls);
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void refusesMissingResultWrongAuthorizationAndClosedQueryWindow() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture.results.missingPage = true;
      assertThrows(CompletionException.class, fixture::resolve);
      assertEquals(InvocationBinding.State.SEALED, fixture.invocations.binding.state());
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
      byte[] wrongAuth = new byte[32];
      wrongAuth[0] = 1;
      assertThrows(CompletionException.class, () -> fixture.resolver.resolve(fixture.id, fixture.digest,
          ByteKey.copyOf(wrongAuth), fixture.scope, fixture.account).join());
      fixture.results.missingPage = false;
      fixture.invocations.closeAfterCas = true;
      assertThrows(CompletionException.class, fixture::resolve);
      assertEquals(InvocationBinding.State.WINDOW_CLOSED, fixture.invocations.binding.state());
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
      assertThrows(CompletionException.class, fixture::resolve);
    }
  }

  @Test
  void replaysTheSameNoChangeResultWithoutReadingACommitDecision() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture.completeWithoutChanges();
      try (var first = fixture.resolveNoChanges(); var replay = fixture.resolveNoChanges()) {
        assertEquals(1, first.revision());
        assertArrayEquals(new byte[] {1, 2, 3}, bytes(first.payload()));
        assertArrayEquals(new byte[] {1, 2, 3}, bytes(replay.payload()));
      }
      assertEquals(0, fixture.controlReads);
      assertEquals(0, fixture.invocations.casCalls);
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
      byte[] wrongAuth = new byte[32];
      wrongAuth[0] = 1;
      assertThrows(CompletionException.class, () -> fixture.resolver.resolveNoChanges(fixture.id,
          fixture.digest, ByteKey.copyOf(wrongAuth), fixture.scope, fixture.account).join());
    }
  }

  @Test
  void resolvesEitherDurableInvocationOutcomeForProtocolProjection() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture.completeWithoutChanges();
      try (var resolved = fixture.resolver.resolveInvocation(fixture.id, fixture.digest,
          fixture.digest, fixture.scope, fixture.account).join()) {
        assertEquals(1, resolved.revision());
        assertFalse(resolved.containsKeyChanges());
        assertArrayEquals(new byte[] {1, 2, 3}, bytes(resolved.payload()));
      }
      assertEquals(0, fixture.controlReads);
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void noChangeReplayRejectsMissingPagesAndAWindowClosedDuringLoad() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture.completeWithoutChanges();
      fixture.results.missingPage = true;
      assertThrows(CompletionException.class, fixture::resolveNoChanges);
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
      fixture.results.missingPage = false;
      fixture.results.afterRead = () -> fixture.invocations.binding =
          fixture.invocations.binding.closeQueryWindow();
      assertThrows(CompletionException.class, fixture::resolveNoChanges);
      assertEquals(InvocationBinding.State.WINDOW_CLOSED, fixture.invocations.binding.state());
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
      assertThrows(CompletionException.class, fixture::resolveNoChanges);
      assertEquals(0, fixture.controlReads);
    }
  }

  @Test
  void transactionResolutionDistinguishesNoChangeFromMetadataOnlyPublication() throws Exception {
    try (Fixture fixture = new Fixture()) {
      assertThrows(CompletionException.class, () -> fixture.resolver.resolveTransaction(
          fixture.id, fixture.digest, fixture.digest, fixture.scope, fixture.account).join());
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
      fixture.completeWithoutChanges();
      try (var result = fixture.resolver.resolveTransaction(fixture.id, fixture.digest,
          fixture.digest, fixture.scope, fixture.account).join()) {
        assertEquals(1, result.revision());
        assertFalse(result.changed());
        assertArrayEquals(new byte[] {1, 2, 3}, bytes(result.payload()));
      }
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void selectedAndNoChangeQueriesHoldDurableFloorsUntilMaterialization() throws Exception {
    for (boolean noChanges : new boolean[] {false, true}) {
      try (var fixture = new Fixture()) {
        if (noChanges) fixture.completeWithoutChanges();
        fixture.results.afterRead = () -> {
          var boundary = fixture.pins.advanceRegistrationFloors(9, 8, fixture.scope).join();
          assertEquals(2, boundary.safeRevision());
          assertEquals(1, boundary.safeSequence());
          var reopened = SnapshotPinManager.open(fixture.pinRecords, fixture.pinConfiguration,
              fixture.clock, fixture.scope).join();
          assertEquals(boundary, reopened.readBoundary(fixture.scope).join());
        };
        try (var result = fixture.resolver.resolveInvocation(fixture.id, fixture.digest,
            fixture.digest, fixture.scope, fixture.account).join()) {
          assertArrayEquals(new byte[] {1, 2, 3}, bytes(result.payload()));
          var boundary = fixture.pins.readBoundary(fixture.scope).join();
          assertEquals(9, boundary.safeRevision());
          assertEquals(8, boundary.safeSequence());
          assertTrue(boundary.protections().isEmpty());
        }
      }
    }
  }

  @Test
  void resultQueryProtectionAcceptsTerminalRevisionAndSequence() throws Exception {
    try (var fixture = new Fixture()) {
      var binding = InvocationBinding.begin(fixture.id, fixture.digest, fixture.digest,
          TransactionId.create(), 1, Long.MAX_VALUE, Long.MAX_VALUE)
          .completeWithoutChanges(fixture.reference.token());
      var pin = fixture.pins.registerResultQuery(binding, fixture.scope).join();
      var boundary = fixture.pins.advanceRegistrationFloors(Long.MAX_VALUE, Long.MAX_VALUE, fixture.scope).join();
      assertEquals(Long.MAX_VALUE, boundary.safeRevision());
      assertEquals(Long.MAX_VALUE, boundary.safeSequence());
      assertEquals(1, boundary.protections().size());
      pin.complete(fixture.scope).join();
      assertTrue(fixture.pins.readBoundary(fixture.scope).join().protections().isEmpty());
    }
  }

  @Test
  void gcWinningEitherRegistrationFloorPreventsAnyResultRead() throws Exception {
    for (boolean revision : new boolean[] {false, true}) {
      try (var fixture = new Fixture()) {
        fixture.pins.advanceRegistrationFloors(revision ? 3 : 0, revision ? 0 : 2, fixture.scope).join();
        fixture.results.afterRead = () -> { throw new AssertionError("unprotected result read"); };
        var failure = assertThrows(CompletionException.class, fixture::resolve);
        assertInstanceOf(SnapshotPinManager.RegistrationFloorExceeded.class, failure.getCause());
        assertEquals(0, fixture.controlReads, "rejection precedes collectable decision and manifest reads");
        assertTrue(fixture.pins.readBoundary(fixture.scope).join().protections().isEmpty());
      }
    }
  }

  @Test
  void expiredOrRevokedQueryCannotDeliverMaterializedBytes() throws Exception {
    for (boolean revoke : new boolean[] {false, true}) {
      try (var fixture = new Fixture()) {
        fixture.completeWithoutChanges();
        fixture.results.afterRead = () -> {
          fixture.now.addAndGet(61_000);
          if (revoke) fixture.pins.revokeExpiredPins(fixture.scope).join();
        };
        assertThrows(CompletionException.class, fixture::resolveNoChanges);
        assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
      }
    }
  }

  @Test
  void cancelledQueryKeepsDurableProtectionUntilTheActualProducerStops() throws Exception {
    try (var fixture = new Fixture(); var inspection = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      fixture.completeWithoutChanges();
      var producer = new CompletableFuture<Void>();
      fixture.scope.trackPhysicalProducer(producer);
      fixture.results.afterRead = () -> fixture.scope.cancel();
      assertThrows(CompletionException.class, fixture::resolveNoChanges);
      assertEquals(1, fixture.pins.readBoundary(inspection).join().protections().size());
      assertFalse(fixture.scope.cleanupCompletion().toCompletableFuture().isDone());
      producer.complete(null);
      fixture.scope.cleanupOutcomeCompletion().toCompletableFuture().join();
      assertTrue(fixture.pins.readBoundary(inspection).join().protections().isEmpty());
      assertEquals(0, fixture.budgets.snapshot().allocatedBytes());
    }
  }

  @Test
  void failedProducerDrainKeepsThePinAndSurfacesCleanupFailure() throws Exception {
    try (var fixture = new Fixture(); var inspection = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      fixture.completeWithoutChanges();
      var producer = new CompletableFuture<Void>();
      fixture.scope.trackPhysicalProducer(producer);
      fixture.results.afterRead = () -> fixture.scope.cancel();
      assertThrows(CompletionException.class, fixture::resolveNoChanges);
      producer.completeExceptionally(new IllegalStateException("producer drain not confirmed"));
      assertThrows(CompletionException.class, () -> fixture.scope.cleanupOutcomeCompletion().toCompletableFuture().join());
      assertEquals(1, fixture.pins.readBoundary(inspection).join().protections().size());
    }
  }

  private static byte[] bytes(InvocationResultResolver.ResolvedResult value) {
    return bytes(value.payload());
  }

  private static byte[] bytes(ResultCatalog.RetainedResult value) {
    var buffer = value.buffer();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  private static final class Fixture implements AutoCloseable {
    private final ByteKey digest = ByteKey.copyOf(new byte[32]);
    private final InvocationId id = InvocationId.create();
    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5));
    private final BudgetManager budgets;
    private final BudgetManager.Account account;
    private final InvocationMemory invocations;
    private final ResultMemory results;
    private final ResultReference reference;
    private final InvocationResultResolver resolver;
    private final PinRecords pinRecords = new PinRecords();
    private final SnapshotPinConfiguration pinConfiguration =
        new SnapshotPinConfiguration(1, 16, Duration.ofMinutes(1));
    private final java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong(1000);
    private final Clock clock = new Clock() {
      @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
      @Override public Clock withZone(java.time.ZoneId zone) { return this; }
      @Override public Instant instant() { return Instant.ofEpochMilli(now.get()); }
    };
    private final SnapshotPinManager pins;
    private int controlReads;

    private Fixture() throws Exception {
      var limits = new BudgetManager.Limits(2, 0, 2, 1, 1, 0, 2_000_000);
      budgets = new BudgetManager(limits, Map.of("group", limits), limits);
      account = budgets.admit("group", scope, new BudgetManager.Reservation(0, 1, 1_000_000, 100));
      InvocationBinding preparing = InvocationBinding.begin(id, digest, digest, TransactionId.create(), 1, 0, 1);
      ResultIdentity identity = ResultIdentity.from(preparing);
      ResultPage page = new ResultPage(identity, 0, new byte[] {1, 2, 3});
      ResultDirectory directory = ResultDirectory.create(identity, List.of(ResultPageReference.from(page)));
      reference = directory.reference();
      ManifestRoot root = ManifestRoot.createWithResult(identity.transactionId(), 1, 0, 1,
          0, 0, Optional.empty(), 0, 0, Optional.empty(), reference);
      invocations = new InvocationMemory(preparing.seal(root.systemIdentity()));
      results = new ResultMemory(directory, page);
      PublicationSlot slot = new PublicationSlot(identity.transactionId(), id, 1, 0, 1,
          1, 1, false, root.systemIdentity(), root.digest());
      CommitState finalized = CommitState.initial(digest, 1).claimOwner("owner", 0, 1000)
          .publish(slot).finalizePublication(identity.transactionId());
      CommitControlStore control = new CommitControlStore() {
        @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope ignored) {
          controlReads++;
          return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(finalized, new byte[] {1})));
        }
        @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState state, RequestScope ignored) {
          throw new UnsupportedOperationException();
        }
        @Override public CompletableFuture<Boolean> compareAndSet(
            ControlSnapshot observed, CommitState state, RequestScope ignored) { throw new UnsupportedOperationException(); }
      };
      CommitDecision decision = CommitDecision.from(slot);
      CommitDecisionRepository decisions = new CommitDecisionRepository() {
        @Override public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope ignored) {
          return CompletableFuture.completedFuture(sequence == 1 ? Optional.of(decision) : Optional.empty());
        }
        @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
            CommitDecision value, RequestScope ignored) { throw new UnsupportedOperationException(); }
      };
      pins = SnapshotPinManager.initializeFresh(pinRecords, pinConfiguration, clock, scope).join();
      resolver = new InvocationResultResolver(invocations, new CommitOutcomeResolver(control, decisions),
          new ManifestCatalog(new RootMemory(root)), new ResultCatalog(results), pins);
    }

    private InvocationResultResolver.ResolvedResult resolve() {
      return resolver.resolve(id, digest, digest, scope, account).join();
    }

    private void completeWithoutChanges() {
      var prior = invocations.binding;
      var preparing = InvocationBinding.begin(prior.invocationId(), prior.requestDigest(),
          prior.authorizationScopeDigest(), prior.transactionId(), prior.preparedOwnerEpoch(),
          prior.baseSequence(), prior.baseRevision());
      invocations.binding = preparing.completeWithoutChanges(reference.token());
    }

    private InvocationResultResolver.ResolvedNoChanges resolveNoChanges() {
      return resolver.resolveNoChanges(id, digest, digest, scope, account).join();
    }

    @Override public void close() { scope.close(); }
  }

  private static final class PinRecords implements SystemRecordStore {
    private record Key(SystemRecordType type, ByteKey identity) {}
    private final Map<Key, ByteKey> values = new HashMap<>();
    @Override public int bucket(SystemRecordType type, ByteKey identity) { return 0; }
    @Override public CompletableFuture<Optional<ByteKey>> read(
        SystemRecordType type, ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(values.get(new Key(type, identity))));
    }
    @Override public CompletableFuture<CreateResult> createIfAbsent(
        SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope) {
      var previous = values.putIfAbsent(new Key(type, identity), value);
      return CompletableFuture.completedFuture(previous == null ? CreateResult.CREATED
          : previous.equals(value) ? CreateResult.ALREADY_PRESENT : CreateResult.CONFLICT);
    }
    @Override public CompletableFuture<Boolean> compareAndSet(SystemRecordType type, ByteKey identity,
        Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) {
      var key = new Key(type, identity);
      if (!Optional.ofNullable(values.get(key)).equals(expected)) return CompletableFuture.completedFuture(false);
      values.put(key, replacement);
      return CompletableFuture.completedFuture(true);
    }
  }

  private static final class InvocationMemory implements InvocationRepository {
    private InvocationBinding binding;
    private boolean loseAcknowledgement;
    private boolean closeAfterCas;
    private int casCalls;
    private InvocationMemory(InvocationBinding binding) { this.binding = binding; }
    @Override public CompletableFuture<Optional<InvocationBinding>> read(InvocationId id, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(binding));
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        InvocationBinding binding, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Boolean> compareAndSet(
        InvocationBinding expected, InvocationBinding replacement, RequestScope scope) {
      casCalls++;
      if (binding != expected) return CompletableFuture.completedFuture(false);
      binding = closeAfterCas ? replacement.closeQueryWindow() : replacement;
      if (loseAcknowledgement) {
        loseAcknowledgement = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(true);
    }
  }

  private static final class ResultMemory implements ResultRepository {
    private final ResultDirectory directory;
    private final ResultPage page;
    private boolean missingPage;
    private Runnable afterRead;
    private ResultMemory(ResultDirectory directory, ResultPage page) { this.directory = directory; this.page = page; }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(ResultPage value, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectory(
        ResultDirectory value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<ResultPage>> readPage(
        ResultIdentity identity, int index, RequestScope scope) {
      if (afterRead != null) afterRead.run();
      return CompletableFuture.completedFuture(missingPage ? Optional.empty() : Optional.of(page));
    }
    @Override public CompletableFuture<Optional<ResultDirectory>> readDirectory(ResultIdentity identity, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(directory));
    }
  }

  private static final class RootMemory implements ManifestRepository {
    private final ManifestRoot root;
    private RootMemory(ManifestRoot root) { this.root = root; }
    @Override public CompletableFuture<Optional<ManifestRoot>> readRoot(ByteKey id, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(root));
    }
    @Override public int candidateBucket(PreparedCandidate value) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(PreparedCandidate value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(ManifestPage value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(ManifestDirectoryPage value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEvent(PreparedEvent value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(EventPage value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(EventDirectoryPage value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> seal(ManifestRoot value, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<PreparedCandidate>> readCandidate(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<ManifestPage>> readPage(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<PreparedEvent>> readEvent(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<EventPage>> readEventPage(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(ByteKey id, RequestScope scope) { throw new UnsupportedOperationException(); }
  }
}
