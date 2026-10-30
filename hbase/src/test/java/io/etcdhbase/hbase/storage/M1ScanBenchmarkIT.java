package io.etcdhbase.hbase.storage;

import static io.etcdhbase.hbase.storage.M1TestEnvironment.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.core.scan.SortedMergeCursor;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.hbase.codec.RowKeyCodec;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.junit.jupiter.api.Test;

/** Reproducible functional/cost baseline, not a target throughput or capacity acceptance. */
class M1ScanBenchmarkIT {
  private static final int REGIONS = 16;
  private static final int SAMPLES = 3;
  private static final BudgetManager.Limits LIMITS = new BudgetManager.Limits(2, 32, 1, 1, 1, 65536, 128L << 20);

  @Test
  void frozenDatasetMatchesTheOracleAcrossLayoutsBucketsAndFlushStates() throws Exception {
    FrozenScanDataset dataset = FrozenScanDataset.create();
    try (var environment = M1TestEnvironment.start(3)) {
      assertEquals(3, await(environment.connection.getAdmin().getRegionServers()).size());
      for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
        for (int buckets : List.of(1, 4, 16)) runProfile(environment, dataset, layout, buckets);
      }
    }
  }

  private static void runProfile(M1TestEnvironment environment, FrozenScanDataset dataset,
      StorageTableBinding.Layout layout, int bucketCount) throws Exception {
    var salt = new HashSaltStrategy(new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, bucketCount, 2));
    var binding = binding(layout, salt);
    environment.create(binding, splits(binding.codec(), dataset.keys()));
    var latest = layout == StorageTableBinding.Layout.HISTORY ? binding(StorageTableBinding.Layout.CURRENT, salt) : null;
    if (latest != null) environment.create(latest, splits(latest.codec(), dataset.keys()));
    assertEquals(REGIONS, await(environment.connection.getAdmin().getRegions(binding.table())).size());
    List<Long> writeNanos = new ArrayList<>();
    try (var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var store = environment.store(binding, scope);
      var current = latest == null ? null : environment.store(latest, scope);
      for (var mutation : dataset.mutations()) {
        long start = System.nanoTime();
        await(store.put(mutation));
        if (current != null) await(current.put(mutation));
        writeNanos.add(System.nanoTime() - start);
      }
    }
    int[] uniqueBuckets = new int[bucketCount];
    int[] writeBuckets = new int[bucketCount];
    for (var key : dataset.keys()) uniqueBuckets[binding.codec().bucket(key)]++;
    for (var mutation : dataset.mutations()) writeBuckets[binding.codec().bucket(mutation.key())]++;
    var hotRegions = new java.util.HashSet<String>();
    for (var mutation : dataset.mutations()) {
      if (mutation.key().equals(dataset.hotKey())) hotRegions.add(await(environment.connection.getRegionLocator(binding.table())
          .getRegionLocation(binding.codec().encode(mutation.key(), mutation.metadata().modificationRevision()))).getRegion().getEncodedName());
    }
    assertEquals(1, hotRegions.size(), "salt cannot distribute revisions of one fixed logical key");
    System.out.printf(java.util.Locale.ROOT,
        "m1-profile {\"layout\":\"%s\",\"buckets\":%d,\"seed\":%d,\"digest\":\"%s\",\"logicalKeys\":%d,\"mutations\":%d,\"applicationPutCalls\":%d,\"regionsPerTable\":%d,\"tables\":%d,\"servers\":3,\"uniqueKeysPerBucket\":%s,\"writesPerBucket\":%s,\"hotObservedRegions\":%d,\"writeP50Micros\":%d,\"writeP95Micros\":%d,\"clientHBase\":\"%s\",\"java\":\"%s\"}%n",
        layout, bucketCount, FrozenScanDataset.SEED, dataset.digest(), dataset.keys().size(), dataset.mutations().size(),
        dataset.mutations().size() * (latest == null ? 1 : 2), REGIONS, latest == null ? 1 : 2,
        Arrays.toString(uniqueBuckets), Arrays.toString(writeBuckets), hotRegions.size(), percentile(writeNanos, 50) / 1000,
        percentile(writeNanos, 95) / 1000, VersionInfo.getVersion(), Runtime.version());
    for (boolean flushed : List.of(false, true)) {
      if (flushed) {
        await(environment.connection.getAdmin().flush(binding.table()));
        await(environment.connection.getAdmin().clearBlockCache(binding.table()));
        if (latest != null) await(environment.connection.getAdmin().flush(latest.table()));
      }
      for (Scenario scenario : scenarios()) runScenario(environment, dataset, binding, bucketCount, scenario, flushed);
    }
  }

  private static void runScenario(M1TestEnvironment environment, FrozenScanDataset dataset,
      StorageTableBinding binding, int bucketCount, Scenario scenario, boolean flushed) throws Exception {
    List<Long> times = new ArrayList<>();
    StoragePage.Cost total = new StoragePage.Cost(0, 0, 0);
    long pointReads = 0, peakBytes = 0, peakScanners = 0;
    var expected = dataset.liveView(scenario.range(), scenario.upper());
    int expectedCount = scenario.limit() == 0 ? expected.size() : Math.min(scenario.limit(), expected.size());
    for (int sample = 0; sample < SAMPLES; sample++) {
      var budget = new BudgetManager(LIMITS, Map.of("public", LIMITS), LIMITS);
      try (var scope = RequestScope.withTimeout(Duration.ofSeconds(60));
          var account = budget.admit("public", scope, new BudgetManager.Reservation(bucketCount * 16384L, 16384, 64L << 20, 2L << 20));
          var scheduler = new ScanScheduler(budget, Duration.ofSeconds(10))) {
        var store = environment.store(binding, scope);
        var template = new ScanRequest(0, scenario.range(), Optional.empty(), scenario.upper(), ReadProjection.METADATA, 1, 16384, 4096);
        var source = scheduler.bind(store, account);
        try (var cursor = scenario.full() ? SortedMergeCursor.values(store, source, template, bucketCount, account, scope)
            : new SortedMergeCursor(source, template, bucketCount, account, scope)) {
          long start = System.nanoTime();
          try (var page = await(cursor.read(scenario.limit()))) {
            times.add(System.nanoTime() - start);
            assertEquals(expectedCount, page.count());
            assertEquals(scenario.limit() > 0 && expected.size() > scenario.limit(), page.more());
            for (int index = 0; index < expectedCount; index++) {
              var actual = page.records().get(index);
              var reference = expected.get(index);
              assertEquals(reference.key(), actual.key());
              assertEquals(reference.metadata(), actual.metadata());
              if (scenario.full()) assertArrayEquals(reference.value().orElseThrow(), actual.value().orElseThrow());
              else assertFalse(actual.hasValue());
            }
            total = total.plus(page.cost());
            pointReads += page.pointReads();
          }
        }
        peakBytes = Math.max(peakBytes, budget.snapshot().peakAllocatedBytes());
        peakScanners = Math.max(peakScanners, budget.snapshot().peakScanners());
        assertTrue(peakScanners <= 1);
        assertEquals(0, budget.snapshot().allocatedBytes());
      }
      assertEquals(0, budget.snapshot().requests());
    }
    System.out.printf(java.util.Locale.ROOT,
        "m1-benchmark {\"layout\":\"%s\",\"buckets\":%d,\"phase\":\"%s\",\"case\":\"%s\",\"samples\":%d,\"returnedPerSample\":%d,\"returnedPhysicalRows\":%d,\"returnedCellBytes\":%d,\"observedScanRpcCalls\":%d,\"applicationPointReads\":%d,\"peakLeasedBytes\":%d,\"peakLocalScannerPermits\":%d,\"p50Micros\":%d,\"p95Micros\":%d,\"p99Micros\":%d,\"observedServerCounters\":%s,\"oracleMatches\":true}%n",
        binding.layout(), bucketCount, flushed ? "flushed_cache_reset_then_warming" : "memstore_warming", scenario.id(), SAMPLES,
        expectedCount, total.physicalRows(), total.returnedCellBytes(), total.scanRpcCalls(), pointReads, peakBytes, peakScanners,
        percentile(times, 50) / 1000, percentile(times, 95) / 1000, percentile(times, 99) / 1000, observationJson(total));
  }

  private static StorageTableBinding binding(StorageTableBinding.Layout layout, HashSaltStrategy salt) {
    return new StorageTableBinding(TableName.valueOf("m1bench_" + UUID.randomUUID().toString().replace("-", "")), new byte[] {'d'}, layout, salt);
  }

  private static byte[][] splits(RowKeyCodec codec, List<ByteKey> keys) {
    var encoded = keys.stream().map(key -> codec.encode(key, 1)).sorted(Arrays::compareUnsigned).toList();
    byte[][] split = new byte[REGIONS - 1][];
    for (int index = 1; index < REGIONS; index++) split[index - 1] = encoded.get(encoded.size() * index / REGIONS);
    return split;
  }

  private static List<Scenario> scenarios() {
    return List.of(new Scenario("empty", prefix("absent/"), 1, Long.MAX_VALUE, false),
        new Scenario("small", prefix("small/"), 0, Long.MAX_VALUE, false),
        new Scenario("recovery", prefix("recovery/a/b/c/d/"), 8, Long.MAX_VALUE, false),
        new Scenario("tombstones", prefix("deleted/"), 0, Long.MAX_VALUE, false),
        new Scenario("old_view", prefix("deleted/"), 0, 78, false),
        new Scenario("large_value", prefix("large/"), 1, Long.MAX_VALUE, true),
        new Scenario("hot_fixed", prefix("hot/"), 1, Long.MAX_VALUE, false),
        new Scenario("binary", KeyRange.prefix(ByteKey.copyOf(new byte[] {0})), 0, Long.MAX_VALUE, false),
        new Scenario("long_key", prefix("l/"), 1, Long.MAX_VALUE, false));
  }

  private static KeyRange prefix(String prefix) { return KeyRange.prefix(ByteKey.copyOf(prefix.getBytes(StandardCharsets.UTF_8))); }
  private static long percentile(List<Long> values, int percentage) {
    var sorted = values.stream().sorted().toList();
    return sorted.get(Math.max(0, (sorted.size() * percentage + 99) / 100 - 1));
  }
  private static String observationJson(StoragePage.Cost cost) {
    return cost.observations().entrySet().stream().sorted(Map.Entry.comparingByKey())
        .map(entry -> "\"" + entry.getKey().name() + "\":" + entry.getValue()).collect(java.util.stream.Collectors.joining(",", "{", "}"));
  }
  private record Scenario(String id, KeyRange range, int limit, long upper, boolean full) {}
}
