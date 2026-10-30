package io.etcdhbase.core.scan;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.storage.BatchWriteResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

final class FrozenRecordStore implements RecordStore {
  final FrozenScanSource scans;
  final List<Get> gets = new ArrayList<>();
  private final List<StoredRecord> records;
  boolean mismatch;

  FrozenRecordStore(List<List<StoredRecord>> buckets) {
    scans = new FrozenScanSource(buckets);
    records = buckets.stream().flatMap(List::stream).toList();
  }
  @Override public ScanOperation startScan(ScanRequest request) { return scans.startScan(request); }
  @Override public CompletableFuture<Optional<StoredRecord>> get(ByteKey key, long upper, ReadProjection projection) {
    gets.add(new Get(key, upper, projection));
    var selected = records.stream().filter(record -> record.key().equals(key) && record.metadata().modificationRevision() <= upper).findFirst();
    if (mismatch) selected = Optional.empty();
    return CompletableFuture.completedFuture(projection == ReadProjection.METADATA
        ? selected.map(record -> StoredRecord.metadataOnly(record.key(), record.metadata())) : selected);
  }
  @Override public CompletableFuture<Void> put(StoredRecord record) { throw new UnsupportedOperationException(); }
  @Override public CompletableFuture<Boolean> compareAndSet(ByteKey key, Optional<StorageMetadata> expected, StoredRecord record) { throw new UnsupportedOperationException(); }
  @Override public CompletableFuture<List<BatchWriteResult>> putBatch(List<StoredRecord> records) { throw new UnsupportedOperationException(); }
  record Get(ByteKey key, long upper, ReadProjection projection) {}
}
