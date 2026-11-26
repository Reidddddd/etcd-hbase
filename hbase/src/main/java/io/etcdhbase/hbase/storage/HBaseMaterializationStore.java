package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.MaterializationStore;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.codec.RevisionTimestamp;
import io.etcdhbase.hbase.codec.RowFenceMessages;
import io.etcdhbase.hbase.codec.StorageMessages;
import io.etcdhbase.hbase.connection.HBaseRequestContext;
import io.etcdhbase.hbase.error.HBaseFailures;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.TableDescriptor;

/** Fenced HISTORY/CURRENT and NATIVE materialization; layout choice remains external evidence. */
public final class HBaseMaterializationStore implements MaterializationStore {
  static final byte[] METADATA = {'m'};
  static final byte[] VALUE = {'v'};
  static final byte[] CURRENT_FENCE = {'f'};
  static final byte[] HISTORY_GUARD = {'g'};
  private static final int MAX_CAS_ATTEMPTS = 32;
  private static final int MAX_NATIVE_VERSIONS = 1024;

  private final AsyncConnection connection;
  private final StorageTableBinding primary;
  private final Optional<StorageTableBinding> current;
  private final int nativeVersionLimit;

  private HBaseMaterializationStore(AsyncConnection connection, StorageTableBinding primary,
      Optional<StorageTableBinding> current, int nativeVersionLimit) {
    this.connection = connection;
    this.primary = primary;
    this.current = current;
    this.nativeVersionLimit = nativeVersionLimit;
  }

  /** NATIVE requires only primary; HISTORY requires a matching CURRENT table binding. */
  public static CompletableFuture<HBaseMaterializationStore> open(
      HBaseConnectionFactory factory,
      AuthorizedBackendIdentity identity,
      StorageTableBinding primary,
      Optional<StorageTableBinding> current,
      RequestScope scope) {
    return open(factory, identity, Optional.empty(), primary, current, scope);
  }

  /** Opens only data tables provisioned as part of the supplied system-table group. */
  public static CompletableFuture<HBaseMaterializationStore> openForTableGroup(
      HBaseConnectionFactory factory,
      AuthorizedBackendIdentity identity,
      SystemTableBinding system,
      StorageTableBinding primary,
      Optional<StorageTableBinding> current,
      RequestScope scope) {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(system, "system");
    if (!identity.scope().tableGroupId().equals(system.tableGroupId())
        || !identity.allowedTableGroupIds().contains(system.tableGroupId())) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
    }
    return open(factory, identity, Optional.of(system), primary, current, scope);
  }

  private static CompletableFuture<HBaseMaterializationStore> open(
      HBaseConnectionFactory factory,
      AuthorizedBackendIdentity identity,
      Optional<SystemTableBinding> system,
      StorageTableBinding primary,
      Optional<StorageTableBinding> current,
      RequestScope scope) {
    Objects.requireNonNull(primary, "primary");
    Objects.requireNonNull(current, "current");
    boolean history = primary.layout() == StorageTableBinding.Layout.HISTORY;
    boolean nativeLayout = primary.layout() == StorageTableBinding.Layout.NATIVE;
    if (!history && !nativeLayout
        || history != current.isPresent()
        || current.filter(value -> value.layout() != StorageTableBinding.Layout.CURRENT).isPresent()) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("materialization requires HISTORY+CURRENT or NATIVE bindings"));
    }
    if (history && !primary.codec().saltProfile().equals(current.orElseThrow().codec().saltProfile())) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("HISTORY and CURRENT salt profiles differ"));
    }
    var context = new HBaseRequestContext(scope);
    return context.borrowConnection(factory, identity).thenCompose(connection -> {
      CompletableFuture<TableDescriptor> primaryDescriptor = context.execute(
          () -> connection.getAdmin().getDescriptor(primary.table()));
      CompletableFuture<Optional<TableDescriptor>> currentDescriptor = current.isPresent()
          ? context.execute(() -> connection.getAdmin().getDescriptor(current.orElseThrow().table()))
              .thenApply(Optional::of)
          : CompletableFuture.completedFuture(Optional.empty());
      CompletableFuture<Optional<TableDescriptor>> systemDescriptor = system.isPresent()
          ? context.execute(() -> connection.getAdmin().getDescriptor(system.orElseThrow().table()))
              .thenApply(Optional::of)
          : CompletableFuture.completedFuture(Optional.empty());
      return primaryDescriptor.thenCombine(currentDescriptor, Descriptors::new)
          .thenCombine(systemDescriptor, (tables, systemTable) -> {
        primary.validate(tables.primary());
        tables.current().ifPresent(descriptor -> current.orElseThrow().validate(descriptor));
        systemTable.ifPresent(descriptor -> validateTableGroupDescriptors(
            system.orElseThrow(), descriptor, primary, tables.primary(), tables.current()));
        int versions = nativeLayout
            ? tables.primary().getColumnFamily(primary.family()).getMaxVersions() : 1;
        if (nativeLayout && (versions < 2 || versions > MAX_NATIVE_VERSIONS)) {
          throw new IllegalArgumentException("native version limit is outside the bounded M2 range");
        }
        return new HBaseMaterializationStore(connection, primary, current, versions);
      });
    }).exceptionallyCompose(failure -> CompletableFuture.failedFuture(HBaseFailures.classify(failure)));
  }

  int nativeVersionLimit() { return nativeVersionLimit; }

  private record Descriptors(TableDescriptor primary, Optional<TableDescriptor> current) {}

  /** Rejects mixed or unowned tables before a group can be made ready for writes. */
  static void validateTableGroupDescriptors(SystemTableBinding system, TableDescriptor systemTable,
      StorageTableBinding primary, TableDescriptor primaryTable, Optional<TableDescriptor> currentTable) {
    system.validate(systemTable);
    String expectedLayout = switch (primary.layout()) {
      case HISTORY -> "SEPARATE_HISTORY_TABLE";
      case NATIVE -> "NATIVE_CELL_VERSIONS";
      case CURRENT -> throw new IllegalArgumentException("current table cannot be the primary history");
    };
    String cluster = systemTable.getValue("etcdhbase.m0.backend-cluster-id");
    String plan = systemTable.getValue("etcdhbase.m0.plan-fingerprint");
    String schema = systemTable.getValue("etcdhbase.m0.schema-version");
    String salt = systemTable.getValue("etcdhbase.m0.salt-profile-id");
    if (cluster == null || cluster.isBlank() || plan == null || !plan.matches("[0-9a-f]{64}")
        || !"1".equals(schema) || salt == null || salt.isBlank()
        || !expectedLayout.equals(systemTable.getValue("etcdhbase.m0.history-layout"))
        || (primary.layout() == StorageTableBinding.Layout.HISTORY) != currentTable.isPresent()) {
      throw new IllegalArgumentException("system table lacks matching group provisioning metadata");
    }
    validateDataOwner(system, primaryTable, cluster, plan, schema, salt, expectedLayout);
    currentTable.ifPresent(table -> validateDataOwner(
        system, table, cluster, plan, schema, salt, expectedLayout));
    if (system.table().equals(primaryTable.getTableName())
        || currentTable.filter(table -> table.getTableName().equals(system.table())
            || table.getTableName().equals(primaryTable.getTableName())).isPresent()) {
      throw new IllegalArgumentException("table group physical tables must be distinct");
    }
  }

  private static void validateDataOwner(SystemTableBinding system, TableDescriptor table,
      String cluster, String plan, String schema, String salt, String expectedLayout) {
    if (!system.table().getNamespaceAsString().equals(table.getTableName().getNamespaceAsString())
        || !system.tableGroupId().equals(table.getValue("etcdhbase.m0.table-group-id"))
        || !cluster.equals(table.getValue("etcdhbase.m0.backend-cluster-id"))
        || !plan.equals(table.getValue("etcdhbase.m0.plan-fingerprint"))
        || !schema.equals(table.getValue("etcdhbase.m0.schema-version"))
        || !salt.equals(table.getValue("etcdhbase.m0.salt-profile-id"))
        || !expectedLayout.equals(table.getValue("etcdhbase.m0.history-layout"))) {
      throw new IllegalArgumentException("data table does not belong to the configured table group");
    }
  }

  @Override
  public CompletableFuture<Void> preflight(PreparedCandidate candidate, RequestScope scope) {
    Objects.requireNonNull(candidate, "candidate");
    long revision = candidate.record().metadata().modificationRevision();
    byte[] primaryRow;
    try {
      primaryRow = primary.codec().encode(candidate.record().key(), revision);
      current.ifPresent(binding -> binding.codec().encode(candidate.record().key(), revision));
    } catch (RuntimeException invalidKey) {
      return CompletableFuture.failedFuture(invalidKey);
    }
    if (primary.layout() != StorageTableBinding.Layout.NATIVE) {
      return CompletableFuture.completedFuture(null);
    }
    return readNativeState(primaryRow, nativeVersionLimit + 1, scope).thenApply(observed -> {
      int versions = cells(observed.versions(), METADATA).size();
      if (versions >= nativeVersionLimit) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      return null;
    });
  }

  @Override
  public CompletableFuture<MaterializationStore.ApplyResult> applySelected(
      CommitDecision decision, PreparedCandidate candidate, RequestScope scope) {
    Objects.requireNonNull(decision, "decision");
    Objects.requireNonNull(candidate, "candidate");
    validateSelected(decision, candidate);
    if (primary.layout() == StorageTableBinding.Layout.NATIVE) {
      return applyNative(decision, candidate, scope, 0);
    }
    return applyHistory(decision, candidate, scope, 0).thenCompose(historyResult ->
        applyCurrent(decision, candidate, scope, 0).thenApply(currentResult ->
            combine(historyResult, currentResult)));
  }

  private CompletableFuture<MaterializationStore.ApplyResult> applyHistory(
      CommitDecision decision, PreparedCandidate candidate, RequestScope scope, int attempt) {
    if (attempt >= MAX_CAS_ATTEMPTS) return resourceLimit();
    StorageTableBinding history = primary;
    byte[] row = history.codec().encode(candidate.record().key(), decision.revision());
    return readRow(history, row, 1, scope).thenCompose(result -> {
      Optional<ObservedFence> observed = observedFence(history, row, result,
          RowFenceMessages.RowKind.HISTORY);
      if (observed.isPresent()) {
        RowFenceMessages.Fence fence = observed.orElseThrow().fence();
        if (!sameFence(fence, decision, candidate)) {
          throw new CompletionException(new IllegalStateException("history row is fenced by another version"));
        }
        // observedFence validates the stored bytes against the fence; sameFence binds that
        // validated result to the selected candidate without copying and hashing cells again.
        return CompletableFuture.completedFuture(MaterializationStore.ApplyResult.ALREADY_APPLIED);
      }
      return readValidatedFence(current.orElseThrow(),
          current.orElseThrow().codec().encode(candidate.record().key(), decision.revision()),
          RowFenceMessages.RowKind.CURRENT, scope).thenCompose(currentFence -> {
            if (currentFence.filter(value -> value.fence().appliedSequence() >= decision.sequence()).isPresent()) {
              throw new CompletionException(new IllegalStateException(
                  "selected history row is missing behind an equal or newer current fence"));
            }
            return mutateRow(history, row, Optional.empty(), HISTORY_GUARD,
                RowFenceMessages.RowKind.HISTORY, decision, candidate, 0, scope)
                .thenCompose(updated -> updated
                    ? CompletableFuture.completedFuture(MaterializationStore.ApplyResult.APPLIED)
                    : applyHistory(decision, candidate, scope, attempt + 1));
          });
    });
  }

  private CompletableFuture<MaterializationStore.ApplyResult> applyCurrent(
      CommitDecision decision, PreparedCandidate candidate, RequestScope scope, int attempt) {
    if (attempt >= MAX_CAS_ATTEMPTS) return resourceLimit();
    StorageTableBinding binding = current.orElseThrow();
    byte[] row = binding.codec().encode(candidate.record().key(), decision.revision());
    return readRow(binding, row, 1, scope).thenCompose(result -> {
      Optional<ObservedFence> observed = observedFence(binding, row, result,
          RowFenceMessages.RowKind.CURRENT);
      if (observed.isPresent()) {
        RowFenceMessages.Fence fence = observed.orElseThrow().fence();
        if (fence.appliedSequence() > decision.sequence()) {
          return CompletableFuture.completedFuture(MaterializationStore.ApplyResult.CURRENT_SUPERSEDED);
        }
        if (fence.appliedSequence() == decision.sequence()) {
          if (!sameFence(fence, decision, candidate)) {
            throw new CompletionException(new IllegalStateException("current row sequence conflicts"));
          }
          return CompletableFuture.completedFuture(MaterializationStore.ApplyResult.ALREADY_APPLIED);
        }
      }
      Optional<byte[]> expected = observed.map(value -> value.encoded().clone());
      return mutateRow(binding, row, expected, CURRENT_FENCE,
          RowFenceMessages.RowKind.CURRENT, decision, candidate,
          observed.map(value -> value.fence().maintenanceGeneration()).orElse(0L), scope)
          .thenCompose(updated -> updated
              ? CompletableFuture.completedFuture(MaterializationStore.ApplyResult.APPLIED)
              : applyCurrent(decision, candidate, scope, attempt + 1));
    });
  }

  private CompletableFuture<MaterializationStore.ApplyResult> applyNative(
      CommitDecision decision, PreparedCandidate candidate, RequestScope scope, int attempt) {
    if (attempt >= MAX_CAS_ATTEMPTS) return resourceLimit();
    byte[] row = primary.codec().encode(candidate.record().key(), decision.revision());
    return readNativeState(row, nativeVersionLimit, scope).thenCompose(observed -> {
      if (observed.fence().isPresent()) {
        RowFenceMessages.Fence fence = observed.fence().orElseThrow().fence();
        if (fence.appliedSequence() > decision.sequence()) {
          return CompletableFuture.completedFuture(MaterializationStore.ApplyResult.CURRENT_SUPERSEDED);
        }
        if (fence.appliedSequence() == decision.sequence()) {
          if (!sameFence(fence, decision, candidate)) {
            throw new CompletionException(new IllegalStateException("native row sequence conflicts"));
          }
          return CompletableFuture.completedFuture(MaterializationStore.ApplyResult.ALREADY_APPLIED);
        }
      }
      int versions = cells(observed.versions(), METADATA).size();
      if (versions >= nativeVersionLimit) return resourceLimit();
      Optional<byte[]> expected = observed.fence().map(value -> value.encoded().clone());
      return mutateRow(primary, row, expected, CURRENT_FENCE,
          RowFenceMessages.RowKind.NATIVE, decision, candidate,
          observed.fence().map(value -> value.fence().maintenanceGeneration()).orElse(0L), scope)
          .thenCompose(updated -> updated
              ? CompletableFuture.completedFuture(MaterializationStore.ApplyResult.APPLIED)
              : applyNative(decision, candidate, scope, attempt + 1));
    });
  }

  CompletableFuture<NativeRowState> readNativeState(
      byte[] row, int versions, RequestScope scope) {
    CompletableFuture<Result> readVersions;
    try {
      Get get = new Get(row).setConsistency(Consistency.STRONG).readVersions(versions)
          .addColumn(primary.family(), METADATA)
          .addColumn(primary.family(), CURRENT_FENCE);
      readVersions = execute(scope, primary, table -> table.get(get));
    } catch (IOException failure) {
      return CompletableFuture.failedFuture(HBaseFailures.classify(failure));
    }
    return readVersions.thenCompose(result -> {
      Optional<ObservedFence> observed = observedNativeFence(result);
      if (observed.isEmpty()) {
        return CompletableFuture.completedFuture(new NativeRowState(result, Optional.empty()));
      }
      ObservedFence fence = observed.orElseThrow();
      return readNativeVersion(row, fence.fence().revision(), scope).thenApply(latest -> {
        verifyFenceData(primary, row, latest, fence.fence());
        return new NativeRowState(result, observed);
      });
    });
  }

  CompletableFuture<Result> readNativeVersion(byte[] row, long revision, RequestScope scope) {
    long timestamp = RevisionTimestamp.timestamp(revision);
    try {
      Get get = new Get(row).setConsistency(Consistency.STRONG)
          .setTimeRange(timestamp, Math.addExact(timestamp, 1))
          .addFamily(primary.family());
      return execute(scope, primary, table -> table.get(get));
    } catch (IOException | ArithmeticException failure) {
      return CompletableFuture.failedFuture(HBaseFailures.classify(failure));
    }
  }

  Optional<ObservedFence> observedNativeFence(Result versions) {
    if (versions.isEmpty()) return Optional.empty();
    validateQualifiers(primary, versions, RowFenceMessages.RowKind.NATIVE);
    List<Cell> metadataVersions = cells(versions, METADATA);
    List<Cell> fenceVersions = cells(versions, CURRENT_FENCE);
    if (metadataVersions.size() != fenceVersions.size()) {
      throw new IllegalArgumentException("native metadata and fence versions are not aligned");
    }
    Map<Long, Cell> metadataByTimestamp = metadataVersions.stream()
        .collect(java.util.stream.Collectors.toMap(Cell::getTimestamp, cell -> cell));
    long previousSequence = 0;
    for (Cell fenceCell : fenceVersions.stream()
        .sorted(java.util.Comparator.comparingLong(Cell::getTimestamp)).toList()) {
      RowFenceMessages.Fence versionFence = RowFenceMessages.decode(copyFence(fenceCell));
      Cell metadata = metadataByTimestamp.get(fenceCell.getTimestamp());
      if (metadata == null || versionFence.rowKind() != RowFenceMessages.RowKind.NATIVE
          || versionFence.state() != RowFenceMessages.State.PRESENT
          || versionFence.revision() != RevisionTimestamp.revision(fenceCell.getTimestamp())
          || versionFence.appliedSequence() <= previousSequence
          || StorageMessages.decodeMetadata(copyMetadata(metadata)).modificationRevision()
              != versionFence.revision()) {
        throw new IllegalArgumentException("native version fence does not match its metadata version");
      }
      previousSequence = versionFence.appliedSequence();
    }
    Cell guard = latestCell(versions, CURRENT_FENCE);
    if (guard == null) {
      if (!metadataVersions.isEmpty() || !cells(versions, VALUE).isEmpty()) {
        throw new IllegalArgumentException("native row data is missing its durable fence");
      }
      return Optional.empty();
    }
    byte[] encoded = copyFence(guard);
    RowFenceMessages.Fence fence = RowFenceMessages.decode(encoded);
    if (fence.rowKind() != RowFenceMessages.RowKind.NATIVE) {
      throw new IllegalArgumentException("native row fence kind differs from its layout");
    }
    requirePresent(fence);
    long timestamp = RevisionTimestamp.timestamp(fence.revision());
    Cell timestampFence = cellAt(versions, CURRENT_FENCE, timestamp);
    if (timestampFence == null || !Arrays.equals(copyFence(timestampFence), encoded)) {
      throw new IllegalArgumentException("latest native fence differs from its retained version");
    }
    return Optional.of(new ObservedFence(fence, encoded));
  }

  private CompletableFuture<Optional<ObservedFence>> readValidatedFence(
      StorageTableBinding binding, byte[] row, RowFenceMessages.RowKind kind, RequestScope scope) {
    return readRow(binding, row, 1, scope).thenApply(result -> observedFence(binding, row, result, kind));
  }

  Optional<ObservedFence> observedFence(
      StorageTableBinding binding, byte[] row, Result result, RowFenceMessages.RowKind expectedKind) {
    if (result.isEmpty()) return Optional.empty();
    validateQualifiers(binding, result, expectedKind);
    byte[] guardQualifier = guardQualifier(expectedKind);
    Cell guard = latestCell(result, guardQualifier);
    if (guard == null) {
      if (!cells(result, METADATA).isEmpty() || !cells(result, VALUE).isEmpty()) {
        throw new IllegalArgumentException("M2 row data is missing its durable fence");
      }
      return Optional.empty();
    }
    byte[] encoded = copyFence(guard);
    RowFenceMessages.Fence fence = RowFenceMessages.decode(encoded);
    if (fence.rowKind() != expectedKind) {
      throw new IllegalArgumentException("row fence kind does not match its table layout");
    }
    requirePresent(fence);
    verifyFenceData(binding, row, result, fence);
    return Optional.of(new ObservedFence(fence, encoded));
  }

  private void verifyFenceData(StorageTableBinding binding, byte[] row, Result result,
      RowFenceMessages.Fence fence) {
    long timestamp = RevisionTimestamp.timestamp(fence.revision());
    Cell guard = cellAt(result, guardQualifier(fence.rowKind()), timestamp);
    if (guard == null || !Arrays.equals(copyFence(guard), RowFenceMessages.encode(fence))) {
      throw new IllegalArgumentException("row fence is missing from its revision timestamp");
    }
    Cell metadata = cellAt(result, METADATA, timestamp);
    if (metadata == null) throw new IllegalArgumentException("row fence points to missing metadata");
    byte[] encodedMetadata = copyMetadata(metadata);
    StorageMetadata storageMetadata = StorageMessages.decodeMetadata(encodedMetadata);
    if (storageMetadata.modificationRevision() != fence.revision()) {
      throw new IllegalArgumentException("row fence revision differs from stored metadata");
    }
    var decodedRow = binding.codec().decode(row);
    if (decodedRow.revision().isPresent()
        && decodedRow.revision().getAsLong() != fence.revision()) {
      throw new IllegalArgumentException("history row revision differs from its row fence");
    }
    ByteKey key = decodedRow.key();
    Optional<byte[]> value = Optional.empty();
    if (storageMetadata.state() == StorageMetadata.State.LIVE) {
      Cell valueCell = cellAt(result, VALUE, timestamp);
      if (valueCell == null) throw new IllegalArgumentException("fenced live row is missing its value");
      value = Optional.of(copyValuePayload(valueCell));
    } else if (cellAt(result, VALUE, timestamp) != null) {
      throw new IllegalArgumentException("fenced tombstone contains a value at its revision");
    }
    StoredRecord record = StorageMessages.decode(key, encodedMetadata, value);
    PreparedCandidate reconstructed = new PreparedCandidate(fence.transactionId(), fence.ownerEpoch(),
        fence.baseSequence(), fence.revision() - 1, record);
    if (!reconstructed.contentDigest().equals(fence.contentDigest())) {
      throw new IllegalArgumentException("row fence content digest differs from stored data");
    }
  }

  private CompletableFuture<Boolean> mutateRow(StorageTableBinding binding, byte[] row,
      Optional<byte[]> expectedFence, byte[] guardQualifier, RowFenceMessages.RowKind rowKind,
      CommitDecision decision, PreparedCandidate candidate, long generation, RequestScope scope) {
    long timestamp = RevisionTimestamp.timestamp(decision.revision());
    byte[] family = binding.family();
    byte[] metadata = StorageMessages.metadata(candidate.record().metadata());
    var fence = new RowFenceMessages.Fence(rowKind, RowFenceMessages.State.PRESENT,
        decision.sequence(), candidate.transactionId(), decision.revision(), candidate.contentDigest(),
        generation, candidate.ownerEpoch(), candidate.baseSequence());
    Put put = new Put(row).setDurability(Durability.SYNC_WAL)
        .addColumn(family, METADATA, timestamp, metadata)
        .addColumn(family, guardQualifier, timestamp, RowFenceMessages.encode(fence));
    if (candidate.record().hasValue()) {
      put.addColumn(family, VALUE, timestamp, StorageMessages.value(candidate.record()));
    }
    try {
      RowMutations mutations = RowMutations.of(List.of(put));
      var condition = CheckAndMutate.newBuilder(row);
      if (expectedFence.isPresent()) {
        condition.ifEquals(family, guardQualifier, expectedFence.orElseThrow());
      } else {
        condition.ifNotExists(family, guardQualifier);
      }
      return execute(scope, binding, table -> table.checkAndMutate(condition.build(mutations)))
          .thenApply(result -> result.isSuccess());
    } catch (IOException failure) {
      return CompletableFuture.failedFuture(HBaseFailures.classify(failure));
    }
  }

  CompletableFuture<Result> readRow(
      StorageTableBinding binding, byte[] row, int versions, RequestScope scope) {
    try {
      Get get = new Get(row).setConsistency(Consistency.STRONG).readVersions(versions)
          .addFamily(binding.family());
      return execute(scope, binding, table -> table.get(get));
    } catch (IOException failure) {
      return CompletableFuture.failedFuture(HBaseFailures.classify(failure));
    }
  }

  <T> CompletableFuture<T> execute(RequestScope scope, StorageTableBinding binding,
      Function<AsyncTable<?>, CompletableFuture<T>> operation) {
    return HBaseRequestCompletion.startRequest(scope, () -> {
      long timeout = Math.max(1, Math.min(TimeUnit.SECONDS.toNanos(30), scope.remainingDeadlineNanos()));
      AsyncTable<?> table = connection.getTableBuilder(binding.table())
          .setOperationTimeout(timeout, TimeUnit.NANOSECONDS)
          .setRpcTimeout(timeout, TimeUnit.NANOSECONDS).build();
      return operation.apply(table);
    });
  }

  private static void validateSelected(CommitDecision decision, PreparedCandidate candidate) {
    if (!decision.containsKeyChanges() || !decision.transactionId().equals(candidate.transactionId())
        || decision.baseRevision() != candidate.baseRevision()
        || decision.sequence() != Math.addExact(candidate.baseSequence(), 1)
        || decision.revision() != candidate.record().metadata().modificationRevision()) {
      throw new IllegalArgumentException("candidate does not belong to the selected decision");
    }
  }

  private static boolean sameFence(
      RowFenceMessages.Fence fence, CommitDecision decision, PreparedCandidate candidate) {
    // Callers validate persisted row bytes before comparing the fence to the selected candidate.
    return fence.appliedSequence() == decision.sequence()
        && fence.revision() == decision.revision()
        && fence.transactionId().equals(candidate.transactionId())
        && fence.ownerEpoch() == candidate.ownerEpoch()
        && fence.baseSequence() == candidate.baseSequence()
        && fence.contentDigest().equals(candidate.contentDigest());
  }

  private static void requirePresent(RowFenceMessages.Fence fence) {
    if (fence.state() != RowFenceMessages.State.PRESENT) {
      throw new IllegalStateException("retired row guard permanently rejects late materialization");
    }
  }

  static byte[] copyMetadata(Cell cell) {
    StorageMessages.requireMetadataLength(cell.getValueLength());
    return CellUtil.cloneValue(cell);
  }

  static byte[] copyValuePayload(Cell cell) {
    StorageMessages.requireValuePayloadLength(cell.getValueLength());
    return CellUtil.cloneValue(cell);
  }

  static byte[] copyFence(Cell cell) {
    RowFenceMessages.requireEncodedLength(cell.getValueLength());
    return CellUtil.cloneValue(cell);
  }

  private static byte[] guardQualifier(RowFenceMessages.RowKind kind) {
    return kind == RowFenceMessages.RowKind.HISTORY ? HISTORY_GUARD : CURRENT_FENCE;
  }

  private static List<Cell> cells(Result row, byte[] qualifier) {
    if (row == null || row.rawCells() == null) return List.of();
    return Arrays.stream(row.rawCells())
        .filter(cell -> matchesQualifier(cell, qualifier)).toList();
  }

  private static Cell latestCell(Result row, byte[] qualifier) {
    return cells(row, qualifier).stream().max(java.util.Comparator.comparingLong(Cell::getTimestamp))
        .orElse(null);
  }

  private static Cell cellAt(Result row, byte[] qualifier, long timestamp) {
    return cells(row, qualifier).stream().filter(cell -> cell.getTimestamp() == timestamp)
        .findFirst().orElse(null);
  }

  private static boolean matchesQualifier(Cell cell, byte[] qualifier) {
    return cell.getQualifierLength() == qualifier.length
        && CellUtil.matchingQualifier(cell, qualifier);
  }

  static void validateQualifiers(
      StorageTableBinding binding, Result row, RowFenceMessages.RowKind kind) {
    byte[] family = binding.family();
    byte[] guard = guardQualifier(kind);
    for (Cell cell : row.rawCells()) {
      if (!CellUtil.matchingFamily(cell, family)) {
        throw new IllegalArgumentException("M2 row contains an unexpected column family");
      }
      if (!matchesQualifier(cell, METADATA) && !matchesQualifier(cell, VALUE)
          && !matchesQualifier(cell, guard)) {
        throw new IllegalArgumentException("M2 row contains an unknown qualifier");
      }
    }
  }

  private static MaterializationStore.ApplyResult combine(
      MaterializationStore.ApplyResult history, MaterializationStore.ApplyResult current) {
    if (history == MaterializationStore.ApplyResult.CURRENT_SUPERSEDED
        || current == MaterializationStore.ApplyResult.CURRENT_SUPERSEDED) {
      return MaterializationStore.ApplyResult.CURRENT_SUPERSEDED;
    }
    if (history == MaterializationStore.ApplyResult.APPLIED
        || current == MaterializationStore.ApplyResult.APPLIED) {
      return MaterializationStore.ApplyResult.APPLIED;
    }
    return MaterializationStore.ApplyResult.ALREADY_APPLIED;
  }

  private static <T> CompletableFuture<T> resourceLimit() {
    return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
  }

  record ObservedFence(RowFenceMessages.Fence fence, byte[] encoded) {}
  record NativeRowState(Result versions, Optional<ObservedFence> fence) {}
}
