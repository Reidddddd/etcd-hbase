package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.HistoryGcPermit;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Enumerates business keys of one table group so a {@code physical} Compact can actually finish the
 * history deletion it promises, instead of reporting a logical boundary as physical completion.
 *
 * <p>§6.3 of the M5 design requires a durable cursor and a bounded page: the resume point is the
 * last fully processed key, and the cursor must not advance before that key's deletion is confirmed.
 * This type only enumerates; deciding reachability and performing the delete stay with the closure
 * authority and the history collector.
 */
public final class HBaseHistoryKeyEnumerator {
  /** One bounded page of keys plus an exclusive resume point. */
  public record Page(List<ByteKey> keys, Optional<ByteKey> lastKey, boolean exhausted) {
    public Page {
      keys = List.copyOf(keys);
      Objects.requireNonNull(lastKey, "lastKey");
      if (!exhausted && lastKey.isEmpty()) {
        throw new IllegalArgumentException("an unfinished key page must publish a resume point");
      }
    }
  }

  /** Upper bound of versions one key may contribute before the page stops growing. */
  private static final int MAX_VERSIONS_PER_KEY_WINDOW = 16;

  private final HBaseMaterializationStore rows;
  private final StorageTableBinding primary;

  private HBaseHistoryKeyEnumerator(HBaseMaterializationStore rows, StorageTableBinding primary) {
    this.rows = Objects.requireNonNull(rows, "rows");
    this.primary = Objects.requireNonNull(primary, "primary");
  }

  public static CompletableFuture<HBaseHistoryKeyEnumerator> open(HBaseConnectionFactory factory,
      io.etcdhbase.spi.identity.AuthorizedBackendIdentity identity, SystemTableBinding system,
      StorageTableBinding primary, Optional<StorageTableBinding> current, RequestScope scope) {
    return HBaseMaterializationStore.openForTableGroup(factory, identity, system, primary, current,
        scope).thenApply(rows -> new HBaseHistoryKeyEnumerator(rows, primary));
  }

  /**
   * Reads at most {@code maximumKeys} distinct business keys after {@code afterKey}. The scan is
   * deliberately version-bounded: enumeration must never depend on how many historical versions a
   * key happens to hold, or the page becomes unbounded under churn.
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
    permit.requireUsable(permit.profile(), scope);
    // A row limit cannot express "N distinct keys": each key may hold many versions, so a fixed
    // row limit would return fewer keys as churn grows. Read a bounded row window and stop as soon
    // as enough distinct keys were collected, which keeps the page bounded in both directions.
    int rowWindow = Math.multiplyExact(maximumKeys, MAX_VERSIONS_PER_KEY_WINDOW);
    Scan scan = new Scan().setLimit(rowWindow).setCaching(rowWindow);
    // The resume point must exclude every version of the previous key, and a key's highest revision
    // is not known in advance. Starting strictly after the encoded key prefix is what guarantees it,
    // so the row is compared by decoded key rather than by an encoded revision guess.
    if (afterKey.isPresent()) {
      scan.withStartRow(primary.codec().encode(afterKey.orElseThrow(), 1), false);
    }
    var collected = new ArrayList<ByteKey>();
    return rows.execute(scope, primary, table -> table.scanAll(scan).thenApply(results -> {
      for (var result : results) {
        if (collected.size() >= maximumKeys) break;
        ByteKey key = primary.codec().decode(result.getRow()).key();
        if (afterKey.isPresent() && key.compareTo(afterKey.orElseThrow()) <= 0) continue;
        if (collected.isEmpty() || !collected.getLast().equals(key)) {
          collected.add(key);
        }
      }
      return true;
    })).thenApply(ignored -> new Page(collected,
        collected.isEmpty() ? afterKey : Optional.of(collected.getLast()),
        collected.size() < maximumKeys));
  }
}
