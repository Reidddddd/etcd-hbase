package io.etcdhbase.hbase.storage;

import static io.etcdhbase.hbase.storage.HBaseSystemRecordStore.RETIRED_VALUE_GUARD;
import static io.etcdhbase.hbase.storage.HBaseSystemRecordStore.RETIREMENT_TIMESTAMP;
import static io.etcdhbase.hbase.storage.HBaseSystemRecordStore.TOMBSTONE;
import static io.etcdhbase.hbase.storage.HBaseSystemRecordStore.VALUE;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.RetirementTombstone;
import io.etcdhbase.core.commit.SystemRecordRetirement.Outcome;
import io.etcdhbase.core.commit.SystemRecordRetirement.Permit;
import io.etcdhbase.core.commit.SystemRecordRetirement.Reason;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.SystemRecordRetirementMessages;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

/** Serializes fence installation and irreversible retirement on the target row itself. */
final class HBaseSystemRecordGcMutations {
  private static final int MAX_FENCE_ATTEMPTS = 8;
  private final HBaseSystemRecordStore records;
  private final SystemTableBinding binding;

  HBaseSystemRecordGcMutations(HBaseSystemRecordStore records, SystemTableBinding binding) {
    this.records = records;
    this.binding = binding;
  }

  CompletableFuture<Outcome> retire(SystemRecordType type, ByteKey identity, ByteKey expected,
      Permit permit, Reason reason, RequestScope scope) {
    permit.requireUsable(binding.profileFingerprint(), scope);
    return new SystemRecordGcAuthority(records).validate(permit, scope)
        .thenCompose(ignored -> prepareFence(type, identity, permit, scope))
        .thenCompose(ignored -> retireValue(type, identity, expected, permit, reason, scope));
  }

  /** Closes a window under the same row fence used for retirement; stale passes cannot mutate it. */
  CompletableFuture<Boolean> closeInvocationWindow(ByteKey identity, ByteKey expected,
      ByteKey replacement, Permit permit, RequestScope scope) {
    var type = SystemRecordType.INVOCATION;
    var authority = new SystemRecordGcAuthority(records);
    return authority.validate(permit, scope)
        .thenCompose(ignored -> prepareFence(type, identity, permit, scope))
        .thenCompose(ignored -> authority.validate(permit, scope))
        .thenCompose(ignored -> {
          byte[] row = binding.row(type, identity);
          var fence = new SingleColumnValueFilter(SystemTableBinding.GC_FENCE_FAMILY,
              SystemRecordGcFence.COLUMN, CompareOperator.EQUAL,
              new BinaryComparator(SystemRecordGcFence.from(permit).encode()));
          fence.setFilterIfMissing(true);
          var condition = new FilterList(FilterList.Operator.MUST_PASS_ALL,
              records.liveValueCondition(expected.bytes()), fence);
          var put = new Put(row).setDurability(Durability.SYNC_WAL)
              .addColumn(binding.family(), VALUE, replacement.bytes());
          var mutation = CheckAndMutate.newBuilder(row).ifMatches(condition).build(put);
          return records.execute(scope, records.logicalMutationPayloadBytes(row,
              (long) expected.length() + replacement.length() + SystemRecordGcFence.BYTES),
              table -> table.checkAndMutate(mutation)).thenApply(result -> result.isSuccess());
        });
  }

  /** The caller has validated this page's authority; a late older pass cannot lower a row fence. */
  CompletableFuture<Void> prepareFence(SystemRecordType type, ByteKey identity, Permit permit, RequestScope scope) {
    permit.requireUsable(binding.profileFingerprint(), scope);
    return prepareFence(type, identity, SystemRecordGcFence.from(permit), scope, 0);
  }

  private CompletableFuture<Void> prepareFence(SystemRecordType type, ByteKey identity,
      SystemRecordGcFence desired, RequestScope scope, int attempt) {
    if (attempt == MAX_FENCE_ATTEMPTS) return unavailable();
    byte[] row = binding.row(type, identity);
    return records.execute(scope, records.logicalReadPayloadBytes(row), table -> table.get(
        new Get(row).setConsistency(Consistency.STRONG).addFamily(binding.family())
            .addFamily(SystemTableBinding.GC_FENCE_FAMILY))).thenCompose(found -> {
          if (found.isEmpty()) return unavailable();
          var current = SystemRecordGcFence.read(found, SystemTableBinding.GC_FENCE_FAMILY);
          if (HBaseSystemRecordStore.decode(selectPayloadFamily(found), binding.family()).isEmpty()) {
            return records.readTombstone(type, identity, scope).thenAccept(marker -> {
              var tombstone = marker.orElseThrow();
              if (current.filter(fence -> fence.compactGeneration() != tombstone.maintenanceGeneration()).isPresent()) {
                throw new IllegalArgumentException("retirement marker differs from its row fence");
              }
            });
          }
          if (current.filter(fence -> fence.compareTo(desired) > 0).isPresent()) return unavailable();
          if (current.equals(Optional.of(desired))) return CompletableFuture.completedFuture(null);
          var mutation = CheckAndMutate.newBuilder(row).ifMatches(fenceInstallationCondition(current))
              .build(new Put(row).setDurability(Durability.SYNC_WAL)
                  .addColumn(SystemTableBinding.GC_FENCE_FAMILY, SystemRecordGcFence.COLUMN, SystemRecordGcFence.TIMESTAMP, desired.encode()));
          return records.execute(scope, records.logicalMutationPayloadBytes(row, SystemRecordGcFence.BYTES),
              table -> table.checkAndMutate(mutation)).thenCompose(updated -> updated.isSuccess()
                  ? CompletableFuture.completedFuture(null)
                  : prepareFence(type, identity, desired, scope, attempt + 1));
        });
  }

  private Result selectPayloadFamily(Result result) {
    var cells = new ArrayList<Cell>();
    for (var cell : result.rawCells()) {
      if (CellUtil.matchingFamily(cell, binding.family())) cells.add(cell);
    }
    return Result.create(cells);
  }

  private Filter fenceInstallationCondition(Optional<SystemRecordGcFence> expected) {
    var fence = new SingleColumnValueFilter(SystemTableBinding.GC_FENCE_FAMILY, SystemRecordGcFence.COLUMN,
        CompareOperator.EQUAL, expected.<org.apache.hadoop.hbase.filter.ByteArrayComparable>map(
            value -> new BinaryComparator(value.encode())).orElseGet(NullComparator::new));
    fence.setFilterIfMissing(expected.isPresent());
    var present = new SingleColumnValueFilter(binding.family(), VALUE, CompareOperator.NOT_EQUAL, new NullComparator());
    present.setFilterIfMissing(true);
    var live = new SingleColumnValueFilter(binding.family(), TOMBSTONE, CompareOperator.EQUAL, new NullComparator());
    live.setFilterIfMissing(false);
    return new FilterList(FilterList.Operator.MUST_PASS_ALL, fence, present, live);
  }

  private CompletableFuture<Outcome> retireValue(SystemRecordType type, ByteKey identity, ByteKey expected,
      Permit permit, Reason reason, RequestScope scope) {
    byte[] row = binding.row(type, identity);
    byte[] observed = expected.bytes();
    var tombstone = new RetirementTombstone(type, identity, digestOf(observed), permit.generation(),
        permit.operationId(), reason, permit.safeRevision(), permit.safeSequence());
    byte[] encoded = SystemRecordRetirementMessages.encode(tombstone);
    var retirement = retirement(row, encoded);
    var fence = new SingleColumnValueFilter(SystemTableBinding.GC_FENCE_FAMILY, SystemRecordGcFence.COLUMN,
        CompareOperator.EQUAL, new BinaryComparator(SystemRecordGcFence.from(permit).encode()));
    fence.setFilterIfMissing(true);
    var condition = new FilterList(FilterList.Operator.MUST_PASS_ALL, records.liveValueCondition(observed), fence);
    var mutation = CheckAndMutate.newBuilder(row).ifMatches(condition).build(retirement);
    long bytes = records.logicalMutationPayloadBytes(row,
        (long) observed.length + encoded.length + RETIRED_VALUE_GUARD.length + SystemRecordGcFence.BYTES);
    return records.execute(scope, bytes, table -> table.checkAndMutate(mutation)).thenCompose(updated ->
        updated.isSuccess() ? CompletableFuture.completedFuture(new Outcome(true, false, 0))
            : confirmUnapplied(type, identity, tombstone, scope));
  }

  private RowMutations retirement(byte[] row, byte[] encoded) {
    var mutation = new RowMutations(row);
    try {
      mutation.add(new Delete(row).setDurability(Durability.SYNC_WAL)
          .addColumns(binding.family(), VALUE, RETIREMENT_TIMESTAMP - 1));
      mutation.add(new Put(row).setDurability(Durability.SYNC_WAL)
          .addColumn(binding.family(), VALUE, RETIREMENT_TIMESTAMP, RETIRED_VALUE_GUARD)
          .addColumn(binding.family(), TOMBSTONE, RETIREMENT_TIMESTAMP, encoded));
      return mutation;
    } catch (java.io.IOException failure) {
      throw new CompletionException(failure);
    }
  }

  private CompletableFuture<Outcome> confirmUnapplied(SystemRecordType type, ByteKey identity,
      RetirementTombstone proposed, RequestScope scope) {
    return records.readTombstone(type, identity, scope).thenCompose(existing -> {
      if (existing.isPresent()) {
        requireSameRetirement(existing.orElseThrow(), proposed);
        return CompletableFuture.completedFuture(new Outcome(false, true, 0));
      }
      return records.read(type, identity, scope).thenApply(current -> new Outcome(false, false,
          current.orElseThrow(() -> new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE))).length()));
    });
  }

  private static void requireSameRetirement(RetirementTombstone stored,
      RetirementTombstone proposed) {
    if (stored.type() != proposed.type() || !stored.identity().equals(proposed.identity())
        || !stored.retiredValueDigest().equals(proposed.retiredValueDigest())
        || stored.maintenanceGeneration() != proposed.maintenanceGeneration()
        || !stored.operationId().equals(proposed.operationId())) {
      throw new IllegalArgumentException("retirement tombstone conflicts with the stored value");
    }
  }

  private static ByteKey digestOf(byte[] value) {
    try {
      return ByteKey.copyOf(java.security.MessageDigest.getInstance("SHA-256").digest(value));
    } catch (java.security.NoSuchAlgorithmException unavailable) {
      throw new IllegalStateException("SHA-256 is unavailable", unavailable);
    }
  }

  private static <T> CompletableFuture<T> unavailable() {
    return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
  }
}
