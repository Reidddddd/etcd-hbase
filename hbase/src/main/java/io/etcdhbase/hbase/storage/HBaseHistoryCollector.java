package io.etcdhbase.hbase.storage;

import static io.etcdhbase.hbase.storage.HBaseMaterializationStore.CURRENT_FENCE;
import static io.etcdhbase.hbase.storage.HBaseMaterializationStore.HISTORY_GUARD;
import static io.etcdhbase.hbase.storage.HBaseMaterializationStore.METADATA;
import static io.etcdhbase.hbase.storage.HBaseMaterializationStore.VALUE;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.HistoryGcPermit;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.codec.RevisionTimestamp;
import io.etcdhbase.hbase.codec.RowFenceMessages;
import io.etcdhbase.hbase.codec.StorageMessages;
import org.apache.hadoop.hbase.HConstants;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;

/** Exact-version KV reclamation with retained baselines, tombstones and row-local fencing. */
public final class HBaseHistoryCollector {
  private static final int MAX_ATTEMPTS = 16;
  private static final int MAX_HISTORY_PAGE = 64;
  private final HBaseMaterializationStore rows;
  private final StorageTableBinding primary;
  private final ByteKey profile;
  private final ByteKey registryIdentity;

  /** Last revision is an exclusive resume point only after all mutations in this page drain. */
  public record Page(long lastRevision, boolean exhausted, int deletedVersions) { }

  private HBaseHistoryCollector(HBaseMaterializationStore rows, StorageTableBinding primary,
      ByteKey profile, ByteKey registryIdentity) {
    this.rows = rows;
    this.primary = primary;
    this.profile = profile;
    this.registryIdentity = registryIdentity;
  }

  /** Uses the same descriptor and ownership checks as normal table-group materialization. */
  public static CompletableFuture<HBaseHistoryCollector> open(HBaseConnectionFactory factory,
      AuthorizedBackendIdentity identity, SystemTableBinding system, StorageTableBinding primary,
      Optional<StorageTableBinding> current, SnapshotPinConfiguration pinConfiguration, RequestScope scope) {
    return HBaseMaterializationStore.openForTableGroup(factory, identity, system, primary, current, scope)
        .thenCompose(rows -> HBaseSystemRecordStore.open(factory, identity, system, scope)
            .thenCompose(records -> SnapshotPinManager.open(records, pinConfiguration, scope))
            .thenApply(pins -> new HBaseHistoryCollector(rows, primary, system.profileFingerprint(),
                pins.registryIdentity())));
  }

  /** Bounds retained wire/results and value verification buffers for this validated descriptor. */
  public long maximumPageReadBytes() {
    long versions = primary.layout() == StorageTableBinding.Layout.NATIVE
        ? rows.nativeVersionLimit() : MAX_HISTORY_PAGE + 1L;
    long cellBytes = HConstants.MAX_ROW_LENGTH + (long) primary.family().length
        + Math.max(StorageMessages.MAX_METADATA_BYTES, RowFenceMessages.MAX_BYTES) + 128;
    // Native reads all retained metadata/fences before selecting a bounded delete page. Each cell
    // repeats the row key; the provisioned native version limit can exceed the 64-delete budget.
    return 2 * (2 * versions) * cellBytes + 4L * StorageMessages.MAX_VALUE_BYTES + (1L << 20);
  }

  /**
   * Collects one explicitly enumerated key. Enumeration and its durable checkpoint belong to the
   * caller. This method cannot delete directories, results, decisions or event records.
   */
  public CompletableFuture<Page> collectKey(HistoryGcPermit permit, ByteKey key,
      long afterRevision, int maximumVersions, RequestScope scope) {
    Objects.requireNonNull(key, "key");
    permit.requireUsable(profile, scope);
    if (!registryIdentity.equals(permit.registryIdentity())) {
      throw new IllegalStateException("history GC pin registry differs");
    }
    if (afterRevision < 0 || primary.layout() == StorageTableBinding.Layout.NATIVE && afterRevision != 0
        || maximumVersions < 1 || maximumVersions > MAX_HISTORY_PAGE) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid history collection page"));
    }
    if (permit.safeRevision() == 0) return CompletableFuture.completedFuture(new Page(afterRevision, true, 0));
    return primary.layout() == StorageTableBinding.Layout.NATIVE
        ? collectNative(permit, key, maximumVersions, scope, 0)
        : collectHistory(permit, key, afterRevision, maximumVersions, scope);
  }

  private CompletableFuture<Page> collectHistory(HistoryGcPermit permit, ByteKey key,
      long afterRevision, int maximumVersions, RequestScope scope) {
    byte[] first = primary.codec().encode(key, 1);
    var baselineScan = metadataScan().setReversed(true).setLimit(1)
        .withStartRow(primary.codec().encode(key, permit.safeRevision()), true)
        .withStopRow(first, true);
    return rows.execute(scope, primary, table -> table.scanAll(baselineScan)).thenCompose(found -> {
      if (found.isEmpty()) return CompletableFuture.completedFuture(new Page(afterRevision, true, 0));
      Result baseline = found.getFirst();
      var fence = historyGuard(baseline);
      if (fence.state() != RowFenceMessages.State.PRESENT) return stale();
      long revision = primary.codec().decode(baseline.getRow()).revision().orElseThrow();
      if (!primary.codec().decode(baseline.getRow()).key().equals(key)
          || revision > permit.safeRevision() || fence.revision() != revision) {
        throw new IllegalArgumentException("history baseline escaped its key or revision");
      }
      return rows.readRow(primary, baseline.getRow(), 1, scope).thenCompose(full -> {
        rows.observedFence(primary, baseline.getRow(), full, RowFenceMessages.RowKind.HISTORY).orElseThrow();
        if (afterRevision >= revision - 1) {
          return CompletableFuture.completedFuture(new Page(afterRevision, true, 0));
        }
        var scan = metadataScan().setLimit(maximumVersions + 1)
            .withStartRow(primary.codec().encode(key, Math.max(1, afterRevision)), afterRevision == 0)
            .withStopRow(primary.codec().encode(key, revision), false);
        return rows.execute(scope, primary, table -> table.scanAll(scan)).thenCompose(versions ->
            retireHistoryPage(permit, key, afterRevision, maximumVersions, versions, scope));
      });
    });
  }

  private Scan metadataScan() {
    return new Scan().setCaching(1).setMaxResultSize(1L << 20)
        .addColumn(primary.family(), HISTORY_GUARD).addColumn(primary.family(), METADATA);
  }

  private CompletableFuture<Page> retireHistoryPage(HistoryGcPermit permit, ByteKey key,
      long afterRevision, int maximumVersions, List<Result> versions, RequestScope scope) {
    int count = Math.min(maximumVersions, versions.size());
    CompletableFuture<Integer> retired = CompletableFuture.completedFuture(0);
    long last = afterRevision;
    for (int index = 0; index < count; index++) {
      Result row = versions.get(index);
      var decoded = primary.codec().decode(row.getRow());
      long revision = decoded.revision().orElseThrow();
      if (!decoded.key().equals(key) || revision <= last || revision >= permit.safeRevision()) {
        return CompletableFuture.failedFuture(new IllegalArgumentException("history page escaped its bounds"));
      }
      last = revision;
      retired = retired.thenCompose(total -> retireHistoryRow(permit, row.getRow(), scope, 0)
          .thenApply(deleted -> total + (deleted ? 1 : 0)));
    }
    long checkpoint = last;
    return retired.thenApply(deleted -> new Page(checkpoint, versions.size() <= maximumVersions, deleted));
  }

  private CompletableFuture<Boolean> retireHistoryRow(HistoryGcPermit permit, byte[] row,
      RequestScope scope, int attempt) {
    if (attempt >= MAX_ATTEMPTS) return stale();
    permit.requireUsable(profile, scope);
    return rows.readRow(primary, row, 1, scope).thenCompose(observed -> {
      if (observed.isEmpty()) throw new IllegalStateException("history guard disappeared");
      RowFenceMessages.Fence fence = historyGuard(observed);
      if (fence.state() == RowFenceMessages.State.RETIRED) {
        requireRetiredDataAbsent(observed);
        return CompletableFuture.completedFuture(false);
      }
      rows.observedFence(primary, row, observed, RowFenceMessages.RowKind.HISTORY).orElseThrow();
      if (fence.revision() >= permit.safeRevision() || fence.appliedSequence() > permit.safeSequence()
          || fence.maintenanceGeneration() > permit.generation()) return stale();
      long timestamp = RevisionTimestamp.timestamp(fence.revision());
      var retired = maintenanceFence(fence, RowFenceMessages.State.RETIRED, permit.generation());
      var put = new Put(row).setDurability(Durability.SYNC_WAL)
          .addColumn(primary.family(), HISTORY_GUARD, timestamp, RowFenceMessages.encode(retired));
      var delete = new Delete(row).setDurability(Durability.SYNC_WAL)
          .addColumn(primary.family(), METADATA, timestamp).addColumn(primary.family(), VALUE, timestamp);
      return mutate(row, HISTORY_GUARD, RowFenceMessages.encode(fence), List.of(put, delete), scope)
          .thenCompose(ignored -> rows.readRow(primary, row, 1, scope))
          .thenCompose(confirmed -> {
            if (historyGuard(confirmed).state() != RowFenceMessages.State.RETIRED) {
              return retireHistoryRow(permit, row, scope, attempt + 1);
            }
            requireRetiredDataAbsent(confirmed);
            return CompletableFuture.completedFuture(true);
          });
    });
  }

  private CompletableFuture<Page> collectNative(HistoryGcPermit permit, ByteKey key,
      int maximumVersions, RequestScope scope, int attempt) {
    if (attempt >= MAX_ATTEMPTS) return stale();
    permit.requireUsable(profile, scope);
    byte[] row = primary.codec().encode(key, 1);
    return rows.readNativeState(row, rows.nativeVersionLimit(), scope).thenCompose(observed -> {
      if (observed.fence().isEmpty()) return CompletableFuture.completedFuture(new Page(0, true, 0));
      var fence = observed.fence().orElseThrow().fence();
      if (fence.maintenanceGeneration() > permit.generation()) return stale();
      List<Cell> fences = cells(observed.versions(), CURRENT_FENCE);
      long baseline = fences.stream().mapToLong(Cell::getTimestamp)
          .filter(timestamp -> RevisionTimestamp.revision(timestamp) <= permit.safeRevision()).max().orElse(-1);
      List<Long> eligible = fences.stream().filter(cell -> cell.getTimestamp() < baseline)
          .filter(cell -> RowFenceMessages.decode(HBaseMaterializationStore.copyFence(cell)).appliedSequence()
              <= permit.safeSequence()).map(Cell::getTimestamp).toList();
      List<Long> victims = eligible.stream().limit(maximumVersions).toList();
      if (victims.isEmpty()) return CompletableFuture.completedFuture(new Page(0, true, 0));
      var delete = new Delete(row).setDurability(Durability.SYNC_WAL);
      for (long timestamp : victims) {
        delete.addColumn(primary.family(), METADATA, timestamp).addColumn(primary.family(), VALUE, timestamp)
            .addColumn(primary.family(), CURRENT_FENCE, timestamp);
      }
      var maintained = maintenanceFence(fence, RowFenceMessages.State.PRESENT, permit.generation());
      var put = new Put(row).setDurability(Durability.SYNC_WAL)
          .addColumn(primary.family(), CURRENT_FENCE, RevisionTimestamp.timestamp(fence.revision()),
              RowFenceMessages.encode(maintained));
      return mutate(row, CURRENT_FENCE, observed.fence().orElseThrow().encoded(), List.of(put, delete), scope)
          .thenCompose(ignored -> rows.readNativeState(row, rows.nativeVersionLimit(), scope))
          .thenCompose(confirmed -> {
            boolean remains = cells(confirmed.versions(), METADATA).stream()
                .anyMatch(cell -> victims.contains(cell.getTimestamp()));
            return remains ? collectNative(permit, key, maximumVersions, scope, attempt + 1)
                : confirmNativeDeletion(row, victims, scope).thenApply(ignored ->
                    new Page(0, eligible.size() <= maximumVersions, victims.size()));
          });
    });
  }

  private CompletableFuture<Void> confirmNativeDeletion(byte[] row, List<Long> timestamps,
      RequestScope scope) {
    CompletableFuture<Void> confirmed = CompletableFuture.completedFuture(null);
    for (long timestamp : timestamps) {
      confirmed = confirmed.thenCompose(ignored -> rows.readNativeVersion(row,
          RevisionTimestamp.revision(timestamp), scope).thenAccept(result -> {
            if (!result.isEmpty()) throw new IllegalStateException("native version deletion is incomplete");
          }));
    }
    return confirmed;
  }

  /** Unknown acknowledgement is resolved by the caller's strong read, never by resending a plan. */
  private CompletableFuture<Void> mutate(byte[] row, byte[] qualifier, byte[] expected,
      List<org.apache.hadoop.hbase.client.Mutation> changes, RequestScope scope) {
    try {
      var mutation = CheckAndMutate.newBuilder(row).ifEquals(primary.family(), qualifier, expected)
          .build(RowMutations.of(changes));
      return rows.execute(scope, primary, table -> table.checkAndMutate(mutation))
          .handle((result, failure) -> {
            if (failure != null) {
              Throwable cause = CompletionFailures.unwrapCompletion(failure);
              if (!(cause instanceof BackendFailure backend) || backend.category() != BackendFailure.Category.UNKNOWN
                  && backend.category() != BackendFailure.Category.UNAVAILABLE
                  && backend.category() != BackendFailure.Category.TIMEOUT) throw new CompletionException(cause);
            }
            return null;
          });
    } catch (IOException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private RowFenceMessages.Fence historyGuard(Result row) {
    HBaseMaterializationStore.validateQualifiers(primary, row, RowFenceMessages.RowKind.HISTORY);
    List<Cell> guards = cells(row, HISTORY_GUARD);
    if (guards.size() != 1) throw new IllegalArgumentException("history guard is missing or ambiguous");
    var fence = RowFenceMessages.decode(HBaseMaterializationStore.copyFence(guards.getFirst()));
    if (fence.rowKind() != RowFenceMessages.RowKind.HISTORY
        || fence.revision() != RevisionTimestamp.revision(guards.getFirst().getTimestamp())
        || fence.revision() != primary.codec().decode(row.getRow()).revision().orElseThrow()) {
      throw new IllegalArgumentException("history guard identity differs");
    }
    return fence;
  }

  private static RowFenceMessages.Fence maintenanceFence(RowFenceMessages.Fence previous,
      RowFenceMessages.State state, long generation) {
    return new RowFenceMessages.Fence(previous.rowKind(), state, previous.appliedSequence(),
        previous.transactionId(), previous.revision(), previous.contentDigest(), generation,
        previous.ownerEpoch(), previous.baseSequence());
  }

  private static void requireRetiredDataAbsent(Result row) {
    if (!cells(row, METADATA).isEmpty() || !cells(row, VALUE).isEmpty()) {
      throw new IllegalStateException("retired history guard still contains data");
    }
  }

  private static List<Cell> cells(Result row, byte[] qualifier) {
    return Arrays.stream(row.rawCells()).filter(cell -> CellUtil.matchingQualifier(cell, qualifier)).toList();
  }

  private static <T> CompletableFuture<T> stale() {
    return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
  }
}
