package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.HistoryGcPermit;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.client.Scan;

/** Enumerates bounded KV key pages in physical salt/key order without fetching business values. */
public final class HBaseHistoryKeyEnumerator {
  /** Last key resumes after its entire physical row prefix, including all HISTORY revisions. */
  public record Page(List<ByteKey> keys, Optional<ByteKey> lastKey, boolean exhausted) {
    public Page {
      keys = List.copyOf(keys);
      Objects.requireNonNull(lastKey, "lastKey");
      if (!exhausted && lastKey.isEmpty()) {
        throw new IllegalArgumentException("an unfinished key page must publish a resume point");
      }
    }
  }

  private final HBaseMaterializationStore rows;
  private final StorageTableBinding primary;
  private final ByteKey profile;

  private HBaseHistoryKeyEnumerator(HBaseMaterializationStore rows, StorageTableBinding primary,
      ByteKey profile) {
    this.rows = rows;
    this.primary = primary;
    this.profile = profile;
  }

  public static CompletableFuture<HBaseHistoryKeyEnumerator> open(HBaseConnectionFactory factory,
      AuthorizedBackendIdentity identity, SystemTableBinding system, StorageTableBinding primary,
      Optional<StorageTableBinding> current, RequestScope scope) {
    return HBaseMaterializationStore.openForTableGroup(factory, identity, system, primary, current,
        scope).thenApply(rows -> new HBaseHistoryKeyEnumerator(rows, primary, system.profileFingerprint()));
  }

  /**
   * Seeks one guard row per distinct key and one lookahead row. Skipping each exact key prefix
   * makes work independent of its version count; at most maximumKeys + 1 RPCs are dispatched.
   */
  public CompletableFuture<Page> nextKeys(HistoryGcPermit permit, Optional<ByteKey> afterKey,
      int maximumKeys, RequestScope scope) {
    Objects.requireNonNull(permit, "permit");
    Objects.requireNonNull(afterKey, "afterKey");
    Objects.requireNonNull(scope, "scope");
    if (maximumKeys < 1 || maximumKeys > 1024) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("key page budget is outside its limit"));
    }
    return seekKeys(permit, afterKey, new ArrayList<>(), maximumKeys, scope);
  }

  private CompletableFuture<Page> seekKeys(HistoryGcPermit permit, Optional<ByteKey> afterKey,
      ArrayList<ByteKey> collected, int maximumKeys, RequestScope scope) {
    permit.requireUsable(profile, scope);
    byte[] guard = primary.layout() == StorageTableBinding.Layout.HISTORY
        ? HBaseMaterializationStore.HISTORY_GUARD : HBaseMaterializationStore.CURRENT_FENCE;
    var scan = new Scan().setLimit(1).setCaching(1).setMaxResultSize(1L << 20)
        .addColumn(primary.family(), guard);
    if (afterKey.isPresent()) {
      // Physical order is salt first. Comparing decoded keys across buckets would omit keys in
      // later buckets; the exclusive end of this exact key prefix excludes every old revision.
      var key = afterKey.orElseThrow();
      var next = primary.codec().physicalRange(primary.codec().bucket(key), KeyRange.point(key))
          .upperExclusive();
      if (next.isEmpty()) return CompletableFuture.completedFuture(new Page(collected, afterKey, true));
      scan.withStartRow(next.orElseThrow().bytes(), true);
    }
    return rows.execute(scope, primary, table -> table.scanAll(scan)).thenCompose(results -> {
      if (results.isEmpty()) return CompletableFuture.completedFuture(new Page(collected, afterKey, true));
      if (collected.size() == maximumKeys) {
        return CompletableFuture.completedFuture(new Page(collected, afterKey, false));
      }
      ByteKey key = primary.codec().decode(results.getFirst().getRow()).key();
      collected.add(key);
      return seekKeys(permit, Optional.of(key), collected, maximumKeys, scope);
    });
  }
}
