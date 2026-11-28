package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.TxnReadSession;
import io.etcdhbase.core.commit.UnchangedRangeProof;
import io.etcdhbase.core.kv.CompareReadExecutor;
import io.etcdhbase.core.kv.M3CacheTestProbe;
import io.etcdhbase.core.kv.RangeCommand;
import io.etcdhbase.core.kv.RangeReadCache;
import io.etcdhbase.core.kv.ResultNode;
import io.etcdhbase.core.kv.TxnCommand;
import io.etcdhbase.core.kv.TxnEvaluator;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.DirectRecordReader;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.hbase.storage.M3TestEnvironment;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.server.commit.TableGroupStores;
import io.etcdhbase.server.commit.TxnExecutionLimits;
import io.etcdhbase.server.commit.TxnReadExecutor;
import io.etcdhbase.server.observability.RequestMetrics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Coverage proof against committed HBase decisions, not a cache-enablement acceptance test. */
@Timeout(600)
class M3CacheProofIT {
  private static final Duration TIMEOUT = Duration.ofMinutes(2);
  private static final SnapshotPinConfiguration PINS =
      new SnapshotPinConfiguration(2, 16, Duration.ofMinutes(5));

  @Test
  void provesUnrelatedAndRejectsRelatedWritesOnBothLayouts() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (var fixture = M3TestEnvironment.start(layout); var engine = new M3PutCommitIT.Engine(fixture)) {
        engine.put(new byte[] {1}, false);
        TableGroupStores stores;
        try (var opening = RequestScope.withTimeout(TIMEOUT)) {
          stores = TableGroupStores.open(fixture.factory(), fixture.identity(), fixture.system(),
              fixture.primary(), fixture.current(), PINS, opening).get(30, TimeUnit.SECONDS);
        }
        var before = capture(fixture, stores);
        engine.put(new byte[] {2}, false);
        ByteKey unrelated = unrelatedKeyInSameBucket(fixture);
        assertEquals(UnchangedRangeProof.Verdict.UNCHANGED, prove(fixture, stores, before, unrelated, 512L << 20));
        assertEquals(UnchangedRangeProof.Verdict.CHANGED, prove(fixture, stores, before, M3PutCommitIT.KEY, 512L << 20));
        assertEquals(UnchangedRangeProof.Verdict.INCONCLUSIVE, prove(fixture, stores, before, unrelated, 1));
      }
    }
  }

  @Test
  void cacheUsesProtectedRealHBaseViewsWithoutEnablingRuntimeCaching() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (var fixture = M3TestEnvironment.start(layout); var engine = new M3PutCommitIT.Engine(fixture)) {
        engine.put("before".getBytes(StandardCharsets.UTF_8), false);
        TableGroupStores stores;
        try (var opening = RequestScope.withTimeout(TIMEOUT)) {
          stores = TableGroupStores.open(fixture.factory(), fixture.identity(), fixture.system(),
              fixture.primary(), fixture.current(), PINS, opening).get(30, TimeUnit.SECONDS);
        }
        String group = fixture.system().tableGroupId();
        var limits = new BudgetManager.Limits(8, 8, 8, 8, 8, 1_000_000, 512L << 20);
        var budget = new BudgetManager(limits, Map.of(group, limits), limits);
        ByteKey unrelated = unrelatedKeyInSameBucket(fixture);
        var selected = command(M3PutCommitIT.KEY);
        var missing = command(unrelated);
        try (var owner = RequestScope.withTimeout(TIMEOUT);
             var cache = new M3CacheTestProbe(budget, group, 7, fixture.system().profileFingerprint(), owner)) {
          admit(fixture, stores, budget, cache, selected);
          admit(fixture, stores, budget, cache, missing);
          assertEquals(Optional.of("before"), sameSequence(fixture, stores, budget, cache, selected)
              .orElseThrow().value());
          engine.put("after".getBytes(StandardCharsets.UTF_8), false);
          assertTrue(sameSequence(fixture, stores, budget, cache, selected).isEmpty());
          var unchanged = withProof(fixture, stores, budget, cache, missing);
          assertTrue(unchanged.isPresent());
          assertEquals(0, unchanged.orElseThrow().count());
          assertFalse(unchanged.orElseThrow().value().isPresent());
          assertTrue(withProof(fixture, stores, budget, cache, selected).isEmpty());
        }
        assertEquals(0, budget.snapshot().allocatedBytes());
      }
    }
  }

  @Test
  void standaloneRangeExecutorUsesCacheOnlyAtTheCurrentProtectedBarrier() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      verifyExecutorLayout(layout);
    }
  }

  private void verifyExecutorLayout(StorageTableBinding.Layout layout) throws Exception {
    try (var fixture = M3TestEnvironment.start(layout); var engine = new M3PutCommitIT.Engine(fixture)) {
      engine.put("before".getBytes(StandardCharsets.UTF_8), false);
      TableGroupStores stores;
      try (var opening = RequestScope.withTimeout(TIMEOUT)) {
        stores = TableGroupStores.open(fixture.factory(), fixture.identity(), fixture.system(),
            fixture.primary(), fixture.current(), PINS, opening).get(30, TimeUnit.SECONDS);
      }
      String group = fixture.system().tableGroupId();
      var limits = new BudgetManager.Limits(8, 8, 8, 8, 8, 1_000_000, 512L << 20);
      var budget = new BudgetManager(limits, Map.of(group, limits), limits);
      var prover = new UnchangedRangeProof(stores.decisions(), stores.manifests(),
          fixture.salt().profile().bucketCount(), 8, 8L << 20, fixture.primary().codec()::bucket);
      var proofOptions = new RangeReadCache.ProofOptions(prover,
          new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8192),
          new BudgetManager.ReadLimits(1000, 4L << 30, 1000, 1000, 1000),
          8, 512L << 20, 8, TIMEOUT.toNanos());
      new ExecutorCacheFixture(fixture, engine, stores, budget, group, prover, proofOptions).run();
      assertEquals(0, budget.snapshot().allocatedBytes());
    }
  }

  /** Owns the scopes, caches, scheduler and worker for both barrier scenarios. */
  private static final class ExecutorCacheFixture {
    private final M3TestEnvironment fixture;
    private final M3PutCommitIT.Engine engine;
    private final TableGroupStores stores;
    private final BudgetManager budget;
    private final String group;
    private final UnchangedRangeProof prover;
    private final RangeReadCache.ProofOptions proofOptions;

    ExecutorCacheFixture(M3TestEnvironment fixture, M3PutCommitIT.Engine engine,
        TableGroupStores stores, BudgetManager budget, String group, UnchangedRangeProof prover,
        RangeReadCache.ProofOptions proofOptions) {
      this.fixture = fixture;
      this.engine = engine;
      this.stores = stores;
      this.budget = budget;
      this.group = group;
      this.prover = prover;
      this.proofOptions = proofOptions;
    }

    void run() throws Exception {
      try (var owner = RequestScope.withTimeout(TIMEOUT);
           var cache = new RangeReadCache(stores.clusterId(), fixture.system().profileFingerprint(),
               1L << 20, 8, 8, budget, group, owner, Optional.of(proofOptions));
           var scans = new ScanScheduler(budget, Duration.ofSeconds(10));
           var worker = Executors.newVirtualThreadPerTaskExecutor()) {
        var reads = new TxnReadExecutor(group, stores.views(), stores.manifests(), fixture.factory(),
            fixture.primary(), worker, scans, new TxnExecutionLimits(4096, 1L << 20, 1L << 20, 4096, 64),
            new RequestMetrics(), Optional.of(cache));
        var commands = verifyCacheAtBarriers(fixture, engine, budget, reads, cache);
        verifyLimitedProof(owner, scans, worker, cache, reads, commands);
      }
    }

    private void verifyLimitedProof(RequestScope owner, ScanScheduler scans,
        java.util.concurrent.ExecutorService worker, RangeReadCache cache, TxnReadExecutor reads,
        ReadCommands commands) throws Exception {
      var selected = commands.selected();
      var unrelated = commands.unrelated();
      var limitedProof = new RangeReadCache.ProofOptions(prover, proofOptions.reservation(),
          proofOptions.reads(), 8, 1, 8, TIMEOUT.toNanos());
      try (var limitedCache = new RangeReadCache(stores.clusterId(), fixture.system().profileFingerprint(),
          1L << 20, 8, 8, budget, group, owner, Optional.of(limitedProof))) {
        var limitedReads = new TxnReadExecutor(group, stores.views(), stores.manifests(), fixture.factory(),
            fixture.primary(), worker, scans, new TxnExecutionLimits(4096, 1L << 20, 1L << 20, 4096, 64),
            new RequestMetrics(), Optional.of(limitedCache));
        assertEquals(new ReadResult(Optional.empty(), 3), readThroughExecutor(fixture, budget, limitedReads, unrelated));
        engine.put("last".getBytes(StandardCharsets.UTF_8), false);
        // The first cache and this second cache have independent ownership and observe the
        // same HBase barrier; an unrelated entry may be proved, but the changed key must miss.
        assertEquals(new ReadResult(Optional.empty(), 4), readThroughExecutor(fixture, budget, reads, unrelated));
        assertEquals(4, M3CacheTestProbe.hits(cache));
        assertEquals(new ReadResult(Optional.of("last"), 4), readThroughExecutor(fixture, budget, reads, selected));
        assertEquals(4, M3CacheTestProbe.hits(cache));
        assertEquals(new ReadResult(Optional.empty(), 4), readThroughExecutor(fixture, budget, limitedReads, unrelated));
        assertEquals(0, M3CacheTestProbe.hits(limitedCache));
        assertEquals(new ReadResult(Optional.empty(), 4), readThroughExecutor(fixture, budget, limitedReads, unrelated));
        assertEquals(1, M3CacheTestProbe.hits(limitedCache));
      }
    }
  }

  private record ReadCommands(RangeCommand selected, RangeCommand unrelated) {}

  private static ReadCommands verifyCacheAtBarriers(M3TestEnvironment fixture, M3PutCommitIT.Engine engine,
      BudgetManager budget, TxnReadExecutor reads, RangeReadCache cache) throws Exception {
    var selected = command(M3PutCommitIT.KEY);
    var unrelated = command(unrelatedKeyInSameBucket(fixture));
    assertEquals(new ReadResult(Optional.of("before"), 2), readThroughExecutor(fixture, budget, reads, selected));
    assertEquals(0, M3CacheTestProbe.hits(cache));
    assertEquals(new ReadResult(Optional.of("before"), 2), readThroughExecutor(fixture, budget, reads, selected));
    assertEquals(1, M3CacheTestProbe.hits(cache));
    assertEquals(new ReadResult(Optional.empty(), 2), readThroughExecutor(fixture, budget, reads, unrelated));
    engine.put("after".getBytes(StandardCharsets.UTF_8), false);
    assertEquals(new ReadResult(Optional.empty(), 3), readThroughExecutor(fixture, budget, reads, unrelated));
    assertEquals(2, M3CacheTestProbe.hits(cache));
    assertEquals(new ReadResult(Optional.of("after"), 3), readThroughExecutor(fixture, budget, reads, selected));
    assertEquals(2, M3CacheTestProbe.hits(cache));
    assertEquals(new ReadResult(Optional.of("after"), 3), readThroughExecutor(fixture, budget, reads, selected));
    assertEquals(3, M3CacheTestProbe.hits(cache));
    var historical = new RangeCommand(selected.range(), selected.context(), 2, selected.limit(),
        selected.sort(), selected.filter(), false, false, false);
    assertEquals(new ReadResult(Optional.of("before"), 3),
        readThroughExecutor(fixture, budget, reads, historical));
    assertEquals(3, M3CacheTestProbe.hits(cache));
    return new ReadCommands(selected, unrelated);
  }

  private record ReadResult(Optional<String> value, long revision) {}

  private static ReadResult readThroughExecutor(M3TestEnvironment fixture, BudgetManager budget,
      TxnReadExecutor reads, RangeCommand command) throws Exception {
    var scope = RequestScope.withTimeout(TIMEOUT);
    try (var drain = new M3CacheScopeDrain(scope); scope;
         var account = budget.admit(fixture.system().tableGroupId(), scope, readReservation())) {
      var request = new TxnCommand(List.of(), List.of(new TxnCommand.Range(command)), List.of());
      var result = reads.execute(request, M3PutCommitIT.AUTHORIZATION, fixture.identity(), scope, account)
          .get(30, TimeUnit.SECONDS);
      var root = (ResultNode.Txn) result.result().root();
      var range = (ResultNode.Range) root.responses().getFirst();
      var value = range.records().isEmpty() ? Optional.<String>empty()
          : Optional.of(new String(range.records().getFirst().value().orElseThrow(), StandardCharsets.UTF_8));
      return new ReadResult(value, range.header().revision());
    }
  }

  private static RangeCommand command(ByteKey key) {
    return new RangeCommand(KeyRange.point(key), RangeCommand.Context.STANDALONE, 0, 0,
        new RangeCommand.Sort(0, 0), new RangeCommand.RevisionFilter(0, 0, 0, 0), false, false, false);
  }

  private static BudgetManager.Reservation readReservation() {
    return new BudgetManager.Reservation(64L << 10, 64L << 10, 1L << 20, 4L << 20, 1L << 20);
  }

  private static void admit(M3TestEnvironment fixture, TableGroupStores stores, BudgetManager budget,
      M3CacheTestProbe cache, RangeCommand command) throws Exception {
    String group = fixture.system().tableGroupId();
    var scope = RequestScope.withTimeout(TIMEOUT);
    try (var drain = new M3CacheScopeDrain(scope); scope;
         var account = budget.admit(group, scope, readReservation());
         var scans = new ScanScheduler(budget, Duration.ofSeconds(10))) {
      var session = session(fixture, stores, scope);
      int buckets = fixture.salt().profile().bucketCount();
      var compares = new CompareReadExecutor(session, scans, account, scope,
          new CompareReadExecutor.Limits(buckets, 4096, 64, 1L << 20));
      var evaluator = new TxnEvaluator(session, compares, account, scope,
          new TxnEvaluator.Limits(1L << 20, 1L << 20, 1L << 20), fixture.primary().codec()::bucket);
      var request = new TxnCommand(List.of(), List.of(new TxnCommand.Range(command)), List.of());
      assertTrue(cache.admit(evaluator.evaluate(request, Optional.empty()).get(30, TimeUnit.SECONDS), scope));
    }
  }

  private static Optional<M3CacheTestProbe.Hit> sameSequence(M3TestEnvironment fixture, TableGroupStores stores,
      BudgetManager budget, M3CacheTestProbe cache, RangeCommand command) throws Exception {
    var scope = RequestScope.withTimeout(TIMEOUT);
    try (var drain = new M3CacheScopeDrain(scope); scope;
         var account = budget.admit(fixture.system().tableGroupId(), scope, readReservation())) {
      var session = session(fixture, stores, scope);
      var hit = cache.sameSequence(session, command, account, scope);
      if (hit.isEmpty()) session.finish().get(30, TimeUnit.SECONDS);
      return hit;
    }
  }

  private static Optional<M3CacheTestProbe.Hit> withProof(M3TestEnvironment fixture, TableGroupStores stores,
      BudgetManager budget, M3CacheTestProbe cache, RangeCommand command) throws Exception {
    String group = fixture.system().tableGroupId();
    var proofReservation = new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8192);
    var reads = new BudgetManager.ReadLimits(1000, 4L << 30, 1000, 1000, 1000);
    var scope = RequestScope.withTimeout(TIMEOUT);
    var proofScope = RequestScope.withTimeout(TIMEOUT);
    try (var drain = new M3CacheScopeDrain(scope, proofScope); scope; proofScope;
         var account = budget.admit(group, scope, readReservation());
         var proofAccount = budget.admit(group, proofScope, proofReservation, reads)) {
      var session = session(fixture, stores, scope);
      var prover = new UnchangedRangeProof(stores.decisions(), stores.manifests(),
          fixture.salt().profile().bucketCount(), 8, 8L << 20, fixture.primary().codec()::bucket);
      var proofBudget = new ManifestCatalog.ProofReadBudget(proofScope, proofAccount, 8, 512L << 20);
      var hit = cache.withProof(session, command, prover, proofBudget, account, scope);
      if (hit.isEmpty()) {
        try (var direct = new DirectRecordReader(session.reader(0), account, 1L << 20)
            .readForPreparation(command.range().lowerInclusive().orElseThrow(), session.currentRevision(),
                ReadProjection.FULL).get(30, TimeUnit.SECONDS)) {
          assertEquals("after", new String(direct.record().orElseThrow().value().orElseThrow(), StandardCharsets.UTF_8));
        }
        session.finish().get(30, TimeUnit.SECONDS);
      }
      return hit;
    }
  }

  private static UnchangedRangeProof.Baseline capture(M3TestEnvironment fixture, TableGroupStores stores)
      throws Exception {
    try (var scope = RequestScope.withTimeout(TIMEOUT)) {
      var session = session(fixture, stores, scope);
      var baseline = UnchangedRangeProof.Baseline.capture(session.protectedView(session.currentRevision()));
      session.finish().get(30, TimeUnit.SECONDS);
      return baseline;
    }
  }

  private static UnchangedRangeProof.Verdict prove(M3TestEnvironment fixture, TableGroupStores stores,
      UnchangedRangeProof.Baseline baseline, ByteKey key, long maximumProofBytes) throws Exception {
    String group = fixture.system().tableGroupId();
    var limits = new BudgetManager.Limits(8, 8, 4, 4, 4, 1_000_000, 512L << 20);
    var budget = new BudgetManager(limits, Map.of(group, limits), limits);
    var reservation = new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8192);
    var reads = new BudgetManager.ReadLimits(1000, 4L << 30, 1000, 1000, 1000);
    UnchangedRangeProof.Verdict result;
    var scope = RequestScope.withTimeout(TIMEOUT);
    var proofScope = RequestScope.withTimeout(TIMEOUT);
    try (var drain = new M3CacheScopeDrain(scope, proofScope); scope; proofScope;
         var account = budget.admit(group, proofScope, reservation, reads)) {
      var session = session(fixture, stores, scope);
      var view = session.protectedView(session.currentRevision());
      var prover = new UnchangedRangeProof(stores.decisions(), stores.manifests(),
          fixture.salt().profile().bucketCount(), 8, 8L << 20, fixture.primary().codec()::bucket);
      var proofBudget = new ManifestCatalog.ProofReadBudget(proofScope, account, 8, maximumProofBytes);
      // This fixture uses one explicitly identified table group and its own storage profile.
      var profile = fixture.system().profileFingerprint();
      result = prover.prove(7, 7, baseline, view, profile, profile, KeyRange.point(key), proofBudget)
          .get(30, TimeUnit.SECONDS);
      session.finish().get(30, TimeUnit.SECONDS);
    }
    assertEquals(0, budget.snapshot().allocatedBytes());
    return result;
  }

  private static TxnReadSession session(M3TestEnvironment fixture, TableGroupStores stores, RequestScope scope)
      throws Exception {
    return TxnReadSession.open(stores.views(), List.of(), M3PutCommitIT.AUTHORIZATION,
        fixture.openPrimary(scope), stores.manifests(), fixture.primary().codec()::bucket,
        fixture.salt().profile().bucketCount(), scope).get(30, TimeUnit.SECONDS);
  }

  private static ByteKey unrelatedKeyInSameBucket(M3TestEnvironment fixture) {
    int bucket = fixture.primary().codec().bucket(M3PutCommitIT.KEY);
    for (int i = 0; i < 10_000; i++) {
      var key = ByteKey.copyOf(("unrelated-" + i).getBytes(StandardCharsets.UTF_8));
      if (fixture.primary().codec().bucket(key) == bucket) return key;
    }
    throw new IllegalStateException("could not construct a same-bucket test key");
  }
}
