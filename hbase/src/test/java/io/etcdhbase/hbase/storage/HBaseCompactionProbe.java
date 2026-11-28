package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.CompactionState;

/** Observes actual active StoreFiles and major-compaction timestamps in the disposable backend. */
final class HBaseCompactionProbe {
  record StoreFiles(Set<String> paths, long bytes) {}
  record Completed(StoreFiles before, StoreFiles after) {}
  private final AsyncAdmin admin;
  private final M1TestEnvironment environment;

  HBaseCompactionProbe(M1TestEnvironment environment) {
    this.environment = environment;
    admin = environment.connection().getAdmin();
  }

  StoreFiles activeFiles(StorageTableBinding binding) throws Exception {
    var files = environment.activeStoreFiles(binding);
    return new StoreFiles(files.keySet(), files.values().stream().mapToLong(Long::longValue).sum());
  }

  Completed majorCompact(StorageTableBinding binding) throws Exception {
    admin.flush(binding.table()).get(30, TimeUnit.SECONDS);
    var before = activeFiles(binding);
    assertFalse(before.paths().isEmpty(), "the experiment needs actual input StoreFiles");
    assertTrue(before.bytes() > 0);
    var regions = admin.getRegions(binding.table()).get(30, TimeUnit.SECONDS);
    var timestamps = new HashMap<byte[], Long>();
    for (var region : regions) {
      if (before.paths().stream().anyMatch(path -> path.contains("/" + region.getEncodedName() + "/"))) {
        timestamps.put(region.getRegionName(), admin.getLastMajorCompactionTimestampForRegion(region.getRegionName())
            .get(30, TimeUnit.SECONDS).orElse(0L));
      }
    }
    assertFalse(timestamps.isEmpty());
    admin.majorCompact(binding.table()).get(30, TimeUnit.SECONDS);
    long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
    while (System.nanoTime() < deadline) {
      var after = activeFiles(binding);
      if (!after.paths().isEmpty() && java.util.Collections.disjoint(after.paths(), before.paths())
          && timestampsAdvanced(timestamps)
          && admin.getCompactionState(binding.table()).get(30, TimeUnit.SECONDS) == CompactionState.NONE) {
        assertTrue(after.bytes() > 0);
        return new Completed(before, after);
      }
      Thread.sleep(100);
    }
    throw new AssertionError("major compaction lacked new timestamps and a changed active StoreFile set");
  }

  private boolean timestampsAdvanced(Map<byte[], Long> previous) throws Exception {
    for (var region : previous.entrySet()) {
      long current = admin.getLastMajorCompactionTimestampForRegion(region.getKey())
          .get(30, TimeUnit.SECONDS).orElse(0L);
      if (current <= region.getValue()) return false;
    }
    return true;
  }

}
