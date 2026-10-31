package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.BatchWriteResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.codec.RevisionTimestamp;
import io.etcdhbase.hbase.codec.StorageMessages;
import io.etcdhbase.hbase.connection.HBaseRequestContext;
import io.etcdhbase.hbase.error.HBaseFailures;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

/** Request-bound physical adapter. The issuing factory retains the shared connection. */
public final class HBaseRecordStore implements RecordStore {
  static final byte[] METADATA = {'m'};
  static final byte[] VALUE = {'v'};
  private final AsyncConnection connection;
  private final StorageTableBinding binding;
  private final RequestScope scope;
  private final Executor worker;

  private HBaseRecordStore(AsyncConnection connection, StorageTableBinding binding,
      RequestScope scope, Executor worker) {
    this.connection = connection;
    this.binding = binding;
    this.scope = scope;
    this.worker = worker;
  }

  public static CompletableFuture<HBaseRecordStore> open(HBaseConnectionFactory factory,
      AuthorizedBackendIdentity identity, StorageTableBinding binding, RequestScope scope, Executor worker) {
    var request = new HBaseRequestContext(scope);
    return request.borrowConnection(factory, identity).thenCompose(connection ->
        request.execute(() -> connection.getAdmin().getDescriptor(binding.table())).thenApply(descriptor -> {
          binding.validate(descriptor);
          return new HBaseRecordStore(connection, binding, scope, worker);
        })).exceptionallyCompose(failure -> CompletableFuture.failedFuture(HBaseFailures.classify(failure)));
  }

  @Override
  public CompletableFuture<Optional<StoredRecord>> get(ByteKey key, long upper, ReadProjection projection) {
    RevisionTimestamp.timestamp(upper);
    if (binding.layout() == StorageTableBinding.Layout.HISTORY) return getHistory(key, upper, projection);
    return execute(() -> {
      try {
        var get = new Get(binding.codec().encode(key, 1)).setTimeRange(0, upper)
            .addColumn(binding.family(), METADATA);
        if (projection == ReadProjection.FULL) get.addColumn(binding.family(), VALUE);
        return table().get(get).thenCompose(row -> decodeAsync(row, projection));
      } catch (java.io.IOException failure) {
        return CompletableFuture.failedFuture(failure);
      }
    });
  }

  private CompletableFuture<Optional<StoredRecord>> getHistory(ByteKey key, long upper, ReadProjection projection) {
    return execute(() -> {
      byte[] startRow = binding.codec().encode(key, upper);
      KeyRange physical = binding.codec().physicalRange(binding.codec().bucket(key), KeyRange.point(key));
      var scan = new Scan().withStartRow(startRow, true)
          .withStopRow(physical.lowerInclusive().orElseThrow().bytes(), false).setReversed(true)
          .setLimit(1).setCaching(1).setMaxResultSize(StorageMessages.MAX_VALUE_BYTES + 8192L)
          .addColumn(binding.family(), METADATA);
      if (projection == ReadProjection.FULL) scan.addColumn(binding.family(), VALUE);
      return table().scanAll(scan).thenCompose(rows ->
          decodeAsync(rows.isEmpty() ? Result.EMPTY_RESULT : rows.getFirst(), projection));
    });
  }

  private CompletableFuture<Optional<StoredRecord>> decodeAsync(Result row, ReadProjection projection) {
    try {
      checkActive();
      if (row.isEmpty()) return CompletableFuture.completedFuture(Optional.empty());
      return CompletableFuture.supplyAsync(() -> {
        checkActive();
        return Optional.of(decode(row, projection));
      }, worker);
    } catch (RejectedExecutionException rejected) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT, rejected));
    }
  }

  @Override
  public CompletableFuture<Void> put(StoredRecord record) {
    return new WriteAttempt().write(record);
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(ByteKey key, Optional<StorageMetadata> expected, StoredRecord replacement) {
    if (!key.equals(replacement.key())) throw new IllegalArgumentException("CAS key mismatch");
    if (binding.layout() == StorageTableBinding.Layout.HISTORY && expected.isPresent()) {
      throw new IllegalArgumentException("immutable history supports absent-row CAS only");
    }
    if (expected.isPresent() && replacement.metadata().modificationRevision() <= expected.get().modificationRevision()) {
      throw new IllegalArgumentException("replacement revision must advance");
    }
    return execute(() -> {
      Put put = mutation(replacement);
      var builder = CheckAndMutate.newBuilder(put.getRow());
      if (expected.isEmpty()) builder.ifNotExists(binding.family(), METADATA);
      else builder.ifEquals(binding.family(), METADATA, StorageMessages.metadata(expected.get()));
      return table().checkAndMutate(builder.build(put)).thenApply(result -> result.isSuccess());
    });
  }

  @Override
  public CompletableFuture<List<BatchWriteResult>> putBatch(List<StoredRecord> records) {
    if (records.size() > 128) return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    var snapshot = List.copyOf(records);
    long bytes = snapshot.stream().mapToLong(StoredRecord::retainedBytes).sum();
    if (bytes > 16L * 1024 * 1024) return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    List<CompletableFuture<BatchWriteResult>> pending = new ArrayList<>(snapshot.size());
    for (int index = 0; index < snapshot.size(); index++) {
      int inputIndex = index;
      var attempt = new WriteAttempt();
      pending.add(attempt.write(snapshot.get(index))
          .handle((ignored, failure) -> attempt.outcome(inputIndex, failure)));
    }
    return CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new))
        .thenApply(ignored -> pending.stream().map(CompletableFuture::join).toList());
  }

  /** Owns one client invocation and the certainty of its batch outcome. */
  private final class WriteAttempt {
    private volatile boolean backendInvoked;

    private CompletableFuture<Void> write(StoredRecord record) {
      return execute(() -> {
        Put put = mutation(record);
        AsyncTable<?> requestTable = table();
        if (binding.layout() != StorageTableBinding.Layout.HISTORY) {
          backendInvoked = true;
          return requestTable.put(put);
        }
        var condition = CheckAndMutate.newBuilder(put.getRow())
            .ifNotExists(binding.family(), METADATA).build(put);
        backendInvoked = true;
        return requestTable.checkAndMutate(condition).thenApply(result -> {
          if (!result.isSuccess()) throw new ImmutableHistoryConflict();
          return null;
        });
      });
    }

    private BatchWriteResult outcome(int index, Throwable failure) {
      if (failure == null) return new BatchWriteResult(index, BatchWriteResult.State.SUCCEEDED, Optional.empty());
      BackendFailure classified = HBaseFailures.classify(failure);
      if (!backendInvoked || classified.getCause() instanceof ImmutableHistoryConflict) {
        return new BatchWriteResult(index, BatchWriteResult.State.FAILED, Optional.of(classified));
      }
      var state = switch (classified.category()) {
        case TIMEOUT, CANCELLED, UNAVAILABLE, UNKNOWN -> BatchWriteResult.State.UNKNOWN;
        default -> BatchWriteResult.State.FAILED;
      };
      return new BatchWriteResult(index, state, Optional.of(classified));
    }
  }

  private static final class ImmutableHistoryConflict extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    ImmutableHistoryConflict() {
      super("immutable history row already exists");
    }
  }

  @Override
  public CompletableFuture<StoragePage> scan(ScanRequest scan) {
    return new HBasePageReader(this, binding, scope, worker).read(scan);
  }

  AsyncTable<?> table() {
    long remaining = Math.max(1, Math.min(TimeUnit.SECONDS.toNanos(30), scope.remainingDeadlineNanos()));
    return connection.getTableBuilder(binding.table()).setOperationTimeout(remaining, TimeUnit.NANOSECONDS)
        .setScanTimeout(remaining, TimeUnit.NANOSECONDS).setRpcTimeout(remaining, TimeUnit.NANOSECONDS).build();
  }

  StoredRecord decode(Result row, ReadProjection projection) {
    var decoded = binding.codec().decode(row.getRow());
    byte[] family = binding.family();
    var metadataCell = row.getColumnLatestCell(family, METADATA);
    if (metadataCell == null) throw new IllegalArgumentException("stored row has no metadata");
    StorageMessages.requireMetadataLength(metadataCell.getValueLength());
    byte[] metadataBytes = CellUtil.cloneValue(metadataCell);
    StorageMetadata metadata = StorageMessages.decodeMetadata(metadataBytes);
    long physicalRevision = RevisionTimestamp.revision(metadataCell.getTimestamp());
    if (physicalRevision != metadata.modificationRevision()
        || decoded.revision().isPresent() && decoded.revision().getAsLong() != physicalRevision) {
      throw new IllegalArgumentException("row/metadata revision mismatch");
    }
    if (projection == ReadProjection.METADATA || metadata.state() == StorageMetadata.State.TOMBSTONE) {
      return StoredRecord.metadataOnly(decoded.key(), metadata);
    }
    var valueCell = row.getColumnLatestCell(family, VALUE);
    if (valueCell == null || valueCell.getTimestamp() != metadataCell.getTimestamp()) {
      throw new IllegalArgumentException("missing or mismatched stored value version");
    }
    StorageMessages.requireValuePayloadLength(valueCell.getValueLength());
    return StorageMessages.decode(decoded.key(), metadataBytes, Optional.of(CellUtil.cloneValue(valueCell)));
  }

  private Put mutation(StoredRecord record) {
    long revision = record.metadata().modificationRevision();
    var put = new Put(binding.codec().encode(record.key(), revision)).setDurability(Durability.SYNC_WAL)
        .addColumn(binding.family(), METADATA, RevisionTimestamp.timestamp(revision), StorageMessages.metadata(record.metadata()));
    if (record.metadata().state() == StorageMetadata.State.LIVE) {
      put.addColumn(binding.family(), VALUE, RevisionTimestamp.timestamp(revision), StorageMessages.value(record));
    }
    return put;
  }

  private <T> CompletableFuture<T> execute(Supplier<CompletableFuture<T>> operation) {
    CompletableFuture<T> backend;
    try {
      checkActive();
      backend = operation.get();
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(HBaseFailures.classify(failure));
    }
    // Completion follows backend work and queued decoding, not cancellation of a dependent
    // future. Writes accepted by HBase may still have an unknown outcome.
    return backend.handle((value, failure) -> {
      if (failure != null) throw new CompletionException(HBaseFailures.classify(failure));
      checkActive();
      return value;
    }).copy();
  }

  private void checkActive() {
    if (scope.isDeadlineExceeded()) throw new CompletionException(HBaseFailures.classify(new TimeoutException()));
    if (scope.isClosed()) throw new CompletionException(HBaseFailures.classify(new CancellationException()));
  }
}
