package io.etcdhbase.core.scan;

import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Linear frozen oracle, independent of the merge implementation and HBase row encoding. */
final class FrozenScanSource implements ScanSource {
  private final List<List<StoredRecord>> buckets;
  final List<ScanRequest> requests = new ArrayList<>();

  FrozenScanSource(List<List<StoredRecord>> buckets) { this.buckets = buckets.stream().map(List::copyOf).toList(); }

  @Override
  public ScanOperation startScan(ScanRequest request) {
    requests.add(request);
    var matching = buckets.get(request.bucket()).stream().filter(record -> request.range().contains(record.key()))
        .filter(record -> request.after().map(after -> record.key().compareTo(after) > 0).orElse(true))
        .sorted(java.util.Comparator.comparing(StoredRecord::key)).toList();
    var selected = matching.stream().limit(request.maximumRows()).map(record -> request.projection() == ReadProjection.METADATA
        ? StoredRecord.metadataOnly(record.key(), record.metadata()) : record).toList();
    var page = new StoragePage(selected, matching.size() <= selected.size(),
        new StoragePage.Cost(selected.size(), selected.stream().mapToLong(StoredRecord::retainedBytes).sum(), 1));
    return new ScanOperation() {
      @Override public CompletionStage<StoragePage> completion() { return CompletableFuture.completedStage(page); }
      @Override public void close() {}
    };
  }
}
