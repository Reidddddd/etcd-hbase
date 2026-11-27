package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.HistoryGcPermit;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.client.Scan;

/** Seeks one KV key within one durable salt bucket without fetching business values. */
public final class HBaseHistoryKeyEnumerator {
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

  /** One key seek confined to a durable maintenance bucket. */
  public CompletableFuture<Optional<ByteKey>> nextKey(HistoryGcPermit permit, int bucket,
      Optional<ByteKey> afterKey, RequestScope scope) {
    Objects.requireNonNull(afterKey, "afterKey");
    if (afterKey.isPresent() && primary.codec().bucket(afterKey.orElseThrow()) != bucket) {
      throw new IllegalArgumentException("history enumeration cursor belongs to another bucket");
    }
    return readKeyAfter(permit, afterKey, primary.codec().physicalRange(bucket, KeyRange.all()), scope);
  }

  private CompletableFuture<Optional<ByteKey>> readKeyAfter(HistoryGcPermit permit,
      Optional<ByteKey> afterKey, KeyRange physicalRange, RequestScope scope) {
    permit.requireUsable(profile, scope);
    byte[] guard = primary.layout() == StorageTableBinding.Layout.HISTORY
        ? HBaseMaterializationStore.HISTORY_GUARD : HBaseMaterializationStore.CURRENT_FENCE;
    var scan = new Scan().setLimit(1).setCaching(1).setMaxResultSize(1L << 20)
        .addColumn(primary.family(), guard);
    var start = physicalRange.lowerInclusive();
    var stop = physicalRange.upperExclusive();
    if (afterKey.isPresent()) {
      // Physical order is salt first. Comparing decoded keys across buckets would omit keys in
      // later buckets; the exclusive end of this exact key prefix excludes every old revision.
      var key = afterKey.orElseThrow();
      start = primary.codec().physicalRange(primary.codec().bucket(key), KeyRange.point(key))
          .upperExclusive();
      if (start.isEmpty() || stop.isPresent() && start.orElseThrow().compareTo(stop.orElseThrow()) >= 0) {
        return CompletableFuture.completedFuture(Optional.empty());
      }
    }
    start.ifPresent(value -> scan.withStartRow(value.bytes(), true));
    stop.ifPresent(value -> scan.withStopRow(value.bytes(), false));
    return rows.execute(scope, primary, table -> table.scanAll(scan)).thenApply(results ->
        results.isEmpty() ? Optional.empty() : Optional.of(primary.codec().decode(results.getFirst().getRow()).key()));
  }
}
