package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.PublicationSlot;
import io.etcdhbase.core.commit.ResultCatalog;
import io.etcdhbase.core.commit.ResultDirectory;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.ResultPage;
import io.etcdhbase.core.commit.ResultPageReference;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.commit.ResultReference;
import io.etcdhbase.core.commit.ResultRepository;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.commit.SystemRecordClosure;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.codec.InvocationMessages;
import io.etcdhbase.hbase.codec.ResultMessages;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/**
 * §6.2 requires an invocation's result window to close before its result is retired, and the
 * retained binding to keep the request digests so the same request can never run again.
 */
class InvocationWindowCloserIT {
  private static final byte[] FAMILY = {'s'};
  private static final int SALT_BUCKETS = 4;
  private static final HashSaltStrategy SALT = new HashSaltStrategy(
      new io.etcdhbase.spi.salt.SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, SALT_BUCKETS, 2));
  private static final String RECORD_CATEGORIES = String.join(",",
      "CONTROL", "INVOCATION", "CANDIDATE", "MANIFEST", "DECISION", "SNAPSHOT_PIN",
      "RESULT", "TABLE_GROUP_IDENTITY", "WORKSET", "LEASE_STATE", "LEASE_ENUMERATION",
      "EXPIRY_CANDIDATE", "AUXILIARY", "LEASE_RESULT", "LEASE_BINDING",
      "LEASE_EXPIRY_CHECKPOINT");

  @Test
  void runtimeClosureWaitsForADurableResultQueryThenKeepsTheNonReplayBinding() throws Exception {
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3));
        var queryScope = RequestScope.withTimeout(Duration.ofMinutes(1))) {
      var table = createSystemTable(environment);
      var store = HBaseSystemRecordStore.open(environment.factory(), environment.identity(), table, scope).join();
      var authority = SystemRecordGcTestFixture.prepare(store, scope);
      try (var query = new QueryFixture(store, authority.pins(), queryScope)) {
        var preparing = query.binding;
        query.reading.get(20, java.util.concurrent.TimeUnit.SECONDS);
        assertFalse(query.result.isDone());
        var reopenedPins = SnapshotPinManager.open(store, SystemRecordGcTestFixture.CONFIGURATION, scope).join();
        var registered = reopenedPins.readBoundary(scope).join().protections();
        assertEquals(1, registered.size());
        var queryPin = registered.getFirst().pinId();
        var invocation = ByteKey.copyOf(preparing.invocationId().bytes());
        int bucket = store.bucket(SystemRecordType.INVOCATION, invocation);
        var closure = new HBaseSystemRecordClosure(store);
        closure.collect(SystemRecordClosure.Domain.INVOCATION, bucket, Optional.empty(), 1,
            authority.authorize(scope), scope).join();
        assertEquals(InvocationBinding.State.NO_CHANGES,
            new HBaseInvocationRepository(store).read(preparing.invocationId(), scope).join().orElseThrow().state());
        query.gate.complete(null);
        try (var result = query.result.get(20, java.util.concurrent.TimeUnit.SECONDS)) {
          assertEquals(3, result.payload().buffer().remaining());
        }
        assertTrue(reopenedPins.readBoundary(scope).join().protections().stream()
            .noneMatch(protection -> protection.pinId().equals(queryPin)),
            "the result pin completes independently of the GC scope's auxiliary read pins");
        var permit = authority.authorize(scope);
        var progress = closure.collect(SystemRecordClosure.Domain.INVOCATION, bucket, Optional.empty(), 1,
            permit, scope).join();
        assertEquals(1, progress.examined());
        assertEquals(0, progress.retired(), "the invocation remains the non-replay proof");
        var reopened = HBaseSystemRecordStore.open(environment.factory(), environment.identity(), table, scope).join();
        var joined = new InvocationService(new HBaseInvocationRepository(reopened)).bindOrJoin(preparing, scope).join();
        assertEquals(InvocationBinding.State.WINDOW_CLOSED, joined.state());
        assertTrue(joined.sameAttempt(preparing));
        assertTrue(reopened.readTombstone(SystemRecordType.INVOCATION, invocation, scope).join().isEmpty());
        org.junit.jupiter.api.Assertions.assertThrows(java.util.concurrent.CompletionException.class,
            () -> query.resolver.resolveNoChanges(preparing.invocationId(), preparing.requestDigest(),
                preparing.authorizationScopeDigest(), query.scope, query.account).join());
      }
    }
  }

  @Test
  void closesAFinalizedSealedInvocationWithoutRequiringAClientReplay() throws Exception {
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var store = open(environment, scope);
      var authority = SystemRecordGcTestFixture.prepare(store, scope);
      var preparing = proposedInvocation();
      var identity = ResultIdentity.from(preparing);
      var reference = ResultDirectory.create(identity, List.of(ResultPageReference.from(
          new ResultPage(identity, 0, new byte[] {1})))).reference();
      var root = ManifestRoot.createWithResult(preparing.transactionId(), preparing.preparedOwnerEpoch(),
          preparing.baseSequence(), preparing.baseRevision(), 0, 0, Optional.empty(), 0, 0, Optional.empty(), reference);
      var sealed = preparing.seal(root.systemIdentity());
      var invocation = ByteKey.copyOf(sealed.invocationId().bytes());
      store.createIfAbsent(SystemRecordType.INVOCATION, invocation,
          ByteKey.copyOf(InvocationMessages.encode(sealed)), scope).join();
      store.createIfAbsent(SystemRecordType.MANIFEST, root.systemIdentity(),
          ByteKey.copyOf(io.etcdhbase.hbase.codec.ManifestMessages.encode(root)), scope).join();
      var decision = CommitDecision.from(new PublicationSlot(sealed.transactionId(), sealed.invocationId(),
          sealed.preparedOwnerEpoch(), sealed.baseSequence(), sealed.baseRevision(), sealed.baseSequence() + 1,
          sealed.baseRevision() + 1, true, root.systemIdentity(), root.digest()));
      var permit = authority.authorize(scope);
      var closer = new InvocationWindowCloser(store);
      var unresolved = assertInstanceOf(InvocationWindowCloser.Retained.class,
          closer.close(invocation, permit, scope).join());
      assertEquals(InvocationWindowCloser.Skip.UNRESOLVED, unresolved.skip());
      assertEquals(InvocationBinding.State.SEALED,
          new HBaseInvocationRepository(store).read(sealed.invocationId(), scope).join().orElseThrow().state());
      new HBaseCommitDecisionRepository(store).createIfAbsent(decision, scope).join();
      assertInstanceOf(InvocationWindowCloser.Closed.class, closer.close(invocation, permit, scope).join());
      assertEquals(InvocationBinding.State.WINDOW_CLOSED,
          new HBaseInvocationRepository(store).read(sealed.invocationId(), scope).join().orElseThrow().state());
    }
  }

  private static final class QueryFixture implements AutoCloseable {
    private final RequestScope scope;
    private final CompletableFuture<Void> gate = new CompletableFuture<>();
    private final CompletableFuture<Void> reading = new CompletableFuture<>();
    private final InvocationBinding binding;
    private final InvocationResultResolver resolver;
    private final BudgetManager.Account account;
    private final CompletableFuture<InvocationResultResolver.ResolvedNoChanges> result;

    private QueryFixture(HBaseSystemRecordStore store, SnapshotPinManager pins, RequestScope scope) throws Exception {
      this.scope = scope;
      var invocations = new InvocationService(new HBaseInvocationRepository(store));
      binding = invocations.bindOrJoin(proposedInvocation(), scope).join();
      var identity = ResultIdentity.from(binding);
      var page = new ResultPage(identity, 0, new byte[] {1, 2, 3});
      var repository = new HBaseResultRepository(store);
      var reference = new ResultPreparation(repository).prepare(identity, List.of(page), scope).join();
      invocations.completeWithoutChanges(binding, reference, new ResultCatalog(repository),
          () -> CompletableFuture.completedFuture(null), scope).join();
      resolver = new InvocationResultResolver(new HBaseInvocationRepository(store),
          new CommitOutcomeResolver(store.controlStore(), new HBaseCommitDecisionRepository(store)),
          new ManifestCatalog(new HBaseManifestRepository(store)), new ResultCatalog(delayedPages(repository, gate, reading)), pins);
      var limits = new BudgetManager.Limits(2, 0, 2, 1, 1, 0, 2_000_000);
      var budgets = new BudgetManager(limits, Map.of("group", limits), limits);
      account = budgets.admit("group", scope, new BudgetManager.Reservation(0, 1, 1_000_000, 100));
      result = resolver.resolveNoChanges(binding.invocationId(), binding.requestDigest(),
          binding.authorizationScopeDigest(), scope, account);
    }

    @Override public void close() throws Exception {
      scope.close();
      gate.complete(null);
      scope.cleanupOutcomeCompletion().toCompletableFuture().get(20, java.util.concurrent.TimeUnit.SECONDS);
    }
  }

  private static ResultRepository delayedPages(ResultRepository delegate, CompletableFuture<Void> gate,
      CompletableFuture<Void> reading) {
    return new ResultRepository() {
      @Override public CompletableFuture<Optional<ResultDirectory>> readDirectory(ResultIdentity id, RequestScope scope) {
        return delegate.readDirectory(id, scope).thenApply(directory -> {
          reading.complete(null);
          return directory;
        });
      }
      @Override public CompletableFuture<Optional<ResultPage>> readPage(ResultIdentity id, int index, RequestScope scope) {
        return gate.thenCompose(ignored -> delegate.readPage(id, index, scope));
      }
      @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectory(ResultDirectory value, RequestScope scope) {
        return delegate.createDirectory(value, scope);
      }
      @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(ResultPage value, RequestScope scope) {
        return delegate.createPage(value, scope);
      }
    };
  }

  @Test
  void closesTheWindowFirstAndKeepsTheDigestsNeededToRejectAReplay() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var store = open(environment, scope);
      var invocations = new InvocationService(new HBaseInvocationRepository(store));
      var binding = selectedInvocation(invocations, scope);
      var permit = SystemRecordGcTestFixture.authorize(store, scope);
      var closer = new InvocationWindowCloser(store);

      var identity = ByteKey.copyOf(binding.invocationId().bytes());
      var resultIdentity = ResultIdentity.from(binding);
      var directory = ResultDirectory.create(resultIdentity,
          java.util.List.of(new ResultPageReference(resultIdentity, 0, 8, 1,
              ByteKey.copyOf(new byte[32]))));
      var resultRow = ResultDirectory.systemIdentity(resultIdentity);
      assertEquals(SystemRecordStore.CreateResult.CREATED,
          store.createIfAbsent(SystemRecordType.RESULT, resultRow,
              ByteKey.copyOf(ResultMessages.encode(directory)), scope).join());
      var outcome = closer.close(identity, permit, scope).join();
      assertInstanceOf(InvocationWindowCloser.Closed.class, outcome);

      var stored = InvocationMessages.decode(store.read(SystemRecordType.INVOCATION, identity,
          scope).join().orElseThrow().bytes());
      assertEquals(InvocationBinding.State.WINDOW_CLOSED, stored.state());
      assertTrue(stored.resultReference().isEmpty(), "a closed window keeps no result reference");
      assertEquals(binding.requestDigest(), stored.requestDigest(),
          "the retained request digest is what refuses a duplicate write");
      assertEquals(binding.authorizationScopeDigest(), stored.authorizationScopeDigest());
      assertEquals(1, collectResults(store, resultRow, permit, scope).retired(),
          "the result sweep retires the unreferenced directory after closure");
      assertTrue(store.read(SystemRecordType.RESULT, resultRow, scope).join().isEmpty(),
          "a retired result must no longer be queryable");
      assertTrue(store.readTombstone(SystemRecordType.RESULT, resultRow, scope).join().isPresent(),
          "the result keeps a tombstone so it cannot be recreated");

      var again = assertInstanceOf(InvocationWindowCloser.Retained.class,
          closer.close(identity, permit, scope).join());
      assertEquals(InvocationWindowCloser.Skip.ALREADY_CLOSED, again.skip());
    }
  }

  @Test
  void retainsResolvedWindowsAtTheTerminalCounters() throws Exception {
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var store = open(environment, scope);
      var permit = SystemRecordGcTestFixture.authorize(store, scope);
      var proposed = proposedInvocation();
      var binding = InvocationBinding.begin(proposed.invocationId(), proposed.requestDigest(),
          proposed.authorizationScopeDigest(), proposed.transactionId(), 1, Long.MAX_VALUE, Long.MAX_VALUE)
          .seal(ByteKey.copyOf(new byte[] {7})).selected(ByteKey.copyOf(new byte[] {8}));
      var identity = ByteKey.copyOf(binding.invocationId().bytes());
      var encoded = ByteKey.copyOf(InvocationMessages.encode(binding));
      store.createIfAbsent(SystemRecordType.INVOCATION, identity, encoded, scope).join();
      var outcome = assertInstanceOf(InvocationWindowCloser.Retained.class,
          new InvocationWindowCloser(store).close(identity, permit, scope).join());
      assertEquals(InvocationWindowCloser.Skip.STILL_PROTECTED, outcome.skip());
      assertEquals(Optional.of(encoded), store.read(SystemRecordType.INVOCATION, identity, scope).join());
    }
  }

  @Test
  void anUnresolvedInvocationIsRetainedRatherThanClosed() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var store = open(environment, scope);
      var invocations = new InvocationService(new HBaseInvocationRepository(store));
      var preparing = proposedInvocation();
      invocations.bindOrJoin(preparing, scope).join();
      var permit = SystemRecordGcTestFixture.authorize(store, scope);
      var closer = new InvocationWindowCloser(store);

      var identity = ByteKey.copyOf(preparing.invocationId().bytes());
      var outcome = assertInstanceOf(InvocationWindowCloser.Retained.class,
          closer.close(identity, permit, scope).join());
      assertEquals(InvocationWindowCloser.Skip.UNRESOLVED, outcome.skip());
      var stored = InvocationMessages.decode(store.read(SystemRecordType.INVOCATION, identity,
          scope).join().orElseThrow().bytes());
      assertEquals(InvocationBinding.State.PREPARING, stored.state(),
          "an unresolved invocation keeps its window open");
      assertTrue(stored.requestDigest().equals(preparing.requestDigest()),
          "an unresolved invocation keeps its original binding untouched");
    }
  }

  @Test
  void closingAWindowRetainsAResultStillReferencedByAManifest() throws Exception {
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var store = open(environment, scope);
      var invocations = new InvocationService(new HBaseInvocationRepository(store));
      var binding = selectedInvocation(invocations, scope);
      var permit = SystemRecordGcTestFixture.authorize(store, scope);
      var resultIdentity = ResultIdentity.from(binding);
      var directory = ResultDirectory.create(resultIdentity, java.util.List.of(
          new ResultPageReference(resultIdentity, 0, 8, 1, ByteKey.copyOf(new byte[32]))));
      var resultRow = directory.systemIdentity();
      var resultValue = ByteKey.copyOf(ResultMessages.encode(directory));
      assertEquals(SystemRecordStore.CreateResult.CREATED,
          store.createIfAbsent(SystemRecordType.RESULT, resultRow, resultValue, scope).join());
      var root = io.etcdhbase.core.commit.ManifestRoot.createWithResult(binding.transactionId(),
          binding.preparedOwnerEpoch(), binding.baseSequence(), binding.baseRevision(), 0, 0,
          java.util.Optional.empty(), 0, 0, java.util.Optional.empty(), directory.reference());
      store.createIfAbsent(SystemRecordType.MANIFEST, root.systemIdentity(),
          ByteKey.copyOf(io.etcdhbase.hbase.codec.ManifestMessages.encode(root)), scope).join();
      var identity = ByteKey.copyOf(binding.invocationId().bytes());
      var outcome = new InvocationWindowCloser(store).close(identity, permit, scope).join();
      assertEquals(0, collectResults(store, resultRow, permit, scope).retired());
      assertAll(() -> assertInstanceOf(InvocationWindowCloser.Closed.class, outcome),
          () -> assertEquals(java.util.Optional.of(resultValue), store.read(SystemRecordType.RESULT, resultRow, scope).join()),
          () -> assertTrue(store.readTombstone(SystemRecordType.RESULT, resultRow, scope).join().isEmpty()));
      assertEquals(InvocationBinding.State.WINDOW_CLOSED, InvocationMessages.decode(
          store.read(SystemRecordType.INVOCATION, identity, scope).join().orElseThrow().bytes()).state());
    }
  }

  private static SystemRecordClosure.Page collectResults(HBaseSystemRecordStore store, ByteKey identity,
      SystemRecordRetirement.Permit permit, RequestScope scope) {
    return new HBaseSystemRecordClosure(store).collect(SystemRecordClosure.Domain.RESULT,
        store.bucket(SystemRecordType.RESULT, identity), Optional.empty(), 16, permit, scope).join();
  }

  private static InvocationBinding selectedInvocation(InvocationService invocations,
      RequestScope scope) {
    var prepared = proposedInvocation();
    var bound = invocations.bindOrJoin(prepared, scope).join();
    var root = io.etcdhbase.core.commit.ManifestRoot.create(bound.transactionId(), bound.preparedOwnerEpoch(),
        bound.baseSequence(), bound.baseRevision(), 0, 0, java.util.Optional.empty());
    var sealed = invocations.transition(bound, bound.seal(root.systemIdentity()), scope).join();
    var resultIdentity = ResultIdentity.from(sealed);
    var directory = ResultDirectory.create(resultIdentity,
        java.util.List.of(new ResultPageReference(resultIdentity, 0, 8, 1,
            ByteKey.copyOf(new byte[32]))));
    return invocations.transition(sealed, sealed.selected(directory.reference().token()), scope)
        .join();
  }

  private static InvocationBinding proposedInvocation() {
    return InvocationBinding.begin(InvocationId.create(), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 1, 2);
  }

  private static HBaseSystemRecordStore open(M1TestEnvironment environment, RequestScope scope)
      throws Exception {
    return HBaseSystemRecordStore.open(environment.factory(), environment.identity(),
        createSystemTable(environment), scope).join();
  }

  private static SystemTableBinding createSystemTable(M1TestEnvironment environment)
      throws Exception {
    String suffix = UUID.randomUUID().toString().replace("-", "");
    String group = "j52-window-" + suffix;
    String saltProfileId = "j52-window-salt-" + suffix;
    TableName table = TableName.valueOf("j52_window_" + suffix);
    SystemTableBinding binding = new SystemTableBinding(table, FAMILY, group, saltProfileId, SALT);
    var descriptor = TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", group)
        .setValue("etcdhbase.m0.salt-profile-id", saltProfileId)
        .setValue("etcdhbase.m0.salt-strategy-id", SALT.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version",
            Integer.toString(SALT.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count", Integer.toString(SALT.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes", Integer.toString(SALT.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories", RECORD_CATEGORIES)
        .setValue("etcdhbase.m4.auxiliary-format", "3")
        .build();
    environment.create(table, SystemRecordGcTestFixture.descriptor(binding, descriptor), SALT.splitKeys().toArray(byte[][]::new));
    return binding;
  }
}
