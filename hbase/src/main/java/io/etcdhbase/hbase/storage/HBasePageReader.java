package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.codec.StorageMessages;
import io.etcdhbase.hbase.error.HBaseFailures;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/** Finite physical pages. Callback threads never block on ResultScanner.next(). */
final class HBasePageReader {
  static final long MAX_RECORD_BYTES = StorageMessages.MAX_VALUE_BYTES + 2L * Short.MAX_VALUE + 8192;
  private final HBaseRecordStore store;
  private final StorageTableBinding binding;
  private final RequestScope scope;
  private final Executor worker;

  HBasePageReader(HBaseRecordStore store, StorageTableBinding binding, RequestScope scope, Executor worker) {
    this.store = store;
    this.binding = binding;
    this.scope = scope;
    this.worker = worker;
  }

  CompletableFuture<StoragePage> read(ScanRequest request) {
    return start(request).completion().toCompletableFuture();
  }

  ScanOperation start(ScanRequest request) {
    var task = new PageTask(request);
    task.registration = scope.registerScopedCleanup(task);
    try {
      worker.execute(task);
    } catch (RejectedExecutionException rejected) {
      task.finish(null, new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT, rejected));
    }
    // Caller cancellation cannot complete the authoritative cleanup future early.
    return task;
  }

  private final class PageTask implements Runnable, ScanOperation {
    private final ScanRequest request;
    private final CompletableFuture<StoragePage> outcome = new CompletableFuture<>();
    private RequestScope.ScopedCleanup registration;
    private ResultScanner scanner;
    private boolean started;
    private boolean cancelled;
    private boolean completing;
    private boolean closeStarted;
    private final CompletableFuture<Void> cleanupComplete = new CompletableFuture<>();

    PageTask(ScanRequest request) { this.request = request; }

    @Override
    public CompletionStage<StoragePage> completion() { return outcome.minimalCompletionStage(); }

    @Override
    public void run() {
      synchronized (this) {
        if (cancelled) return;
        started = true;
      }
      StoragePage page = null;
      Throwable failure = null;
      try {
        checkActive();
        KeyRange physical = physicalRange();
        if (physical.isEmpty()) page = new StoragePage(List.of(), true, new StoragePage.Cost(0, 0, 0));
        else {
          ResultScanner opened = store.table().getScanner(scan(physical));
          synchronized (this) {
            if (cancelled) opened.close();
            else scanner = opened;
          }
          checkActive();
          page = collect(opened);
        }
      } catch (Exception problem) {
        failure = problem;
      } catch (Error fatal) {
        failure = fatal;
        throw fatal;
      } finally {
        finish(page, failure);
      }
    }

    private KeyRange physicalRange() {
      KeyRange physical = binding.codec().physicalRange(request.bucket(), request.range());
      if (physical.isEmpty() || request.after().isEmpty()) return physical;
      KeyRange afterGroup = binding.codec().physicalRange(request.bucket(), KeyRange.point(request.after().get()));
      ByteKey resume = afterGroup.upperExclusive().orElseThrow();
      ByteKey lower = physical.lowerInclusive().orElseThrow();
      return KeyRange.interval(Optional.of(resume.compareTo(lower) > 0 ? resume : lower), physical.upperExclusive());
    }

    private Scan scan(KeyRange physical) throws java.io.IOException {
      var scan = new Scan().setCaching(1).setMaxResultSize(request.maximumBytes())
          .setAllowPartialResults(false).setScanMetricsEnabled(true)
          .setLimit(request.maximumPhysicalRows() + 1).addColumn(binding.family(), HBaseRecordStore.METADATA);
      physical.lowerInclusive().ifPresent(lower -> scan.withStartRow(lower.bytes(), true));
      physical.upperExclusive().ifPresent(upper -> scan.withStopRow(upper.bytes(), false));
      if (binding.layout() != StorageTableBinding.Layout.HISTORY) scan.setTimeRange(0, request.revisionUpperBound());
      if (request.projection() == ReadProjection.FULL) scan.addColumn(binding.family(), HBaseRecordStore.VALUE);
      return scan;
    }

    private StoragePage collect(ResultScanner opened) throws Exception {
      List<StoredRecord> records = new ArrayList<>();
      StoredRecord candidate = null;
      ByteKey group = null;
      long retained = 0;
      long physicalRows = 0;
      long cellBytes = 0;
      Result row;
      while ((row = opened.next()) != null) {
        checkActive();
        physicalRows++;
        for (var cell : row.rawCells()) {
          cellBytes += KeyValueUtil.length(cell.getRowLength(), cell.getFamilyLength(),
              cell.getQualifierLength(), cell.getValueLength(), cell.getTagsLength(), true);
        }
        if (physicalRows > request.maximumPhysicalRows()
            || cellBytes > 4 * request.maximumBytes() + MAX_RECORD_BYTES) throw resourceLimit();
        var decoded = binding.codec().decode(row.getRow());
        if (group != null && !group.equals(decoded.key()) && candidate != null) {
          if (!append(records, candidate, retained)) return page(records, false, opened, physicalRows, cellBytes);
          retained += candidate.retainedBytes();
          if (records.size() == request.maximumRows()) return page(records, false, opened, physicalRows, cellBytes);
          candidate = null;
        }
        group = decoded.key();
        if (request.after().isPresent() && group.compareTo(request.after().get()) <= 0) continue;
        if (decoded.revision().isPresent() && decoded.revision().getAsLong() > request.revisionUpperBound()) continue;
        candidate = store.decode(row, request.projection());
      }
      checkActive();
      if (candidate != null && !append(records, candidate, retained)) {
        return page(records, false, opened, physicalRows, cellBytes);
      }
      return page(records, true, opened, physicalRows, cellBytes);
    }

    private boolean append(List<StoredRecord> records, StoredRecord record, long retained) throws BackendFailure {
      if (record.retainedBytes() > request.maximumBytes()) throw resourceLimit();
      if (retained + record.retainedBytes() > request.maximumBytes()) return false;
      records.add(record);
      return true;
    }

    private StoragePage page(List<StoredRecord> records, boolean exhausted, ResultScanner opened, long rows, long bytes) {
      long rpcCalls = opened.getScanMetrics().countOfRPCcalls.get();
      return new StoragePage(records, exhausted, new StoragePage.Cost(rows, bytes, rpcCalls));
    }

    private void checkActive() throws TimeoutException {
      if (scope.isDeadlineExceeded()) throw new TimeoutException();
      synchronized (this) {
        if (cancelled || scope.isClosed()) throw new CancellationException();
      }
    }

    private void finish(StoragePage page, Throwable original) {
      boolean cancelledBeforeCompletion;
      synchronized (this) {
        cancelledBeforeCompletion = cancelled;
        completing = true;
      }
      Throwable failure = original;
      Error fatalCleanup = null;
      try {
        registration.close();
      } catch (Exception cleanupFailure) {
        if (failure == null) failure = cleanupFailure;
        else if (failure != cleanupFailure) failure.addSuppressed(cleanupFailure);
      } catch (Error fatal) {
        fatalCleanup = fatal;
        if (failure == null) failure = fatal;
        else if (failure != fatal) failure.addSuppressed(fatal);
      }
      Throwable operationFailure = failure;
      // A scope may already be closing the scanner on another thread. Completion follows the
      // actual close, so a scheduler cannot release its physical permit while close still runs.
      cleanupComplete.whenComplete((ignored, cleanupFailure) -> {
        Throwable terminal = operationFailure;
        if (terminal == null) terminal = cleanupFailure;
        else if (cleanupFailure != null && terminal != cleanupFailure) terminal.addSuppressed(cleanupFailure);
        if (terminal == null && (cancelledBeforeCompletion || scope.isClosed())) terminal = new CancellationException();
        if (terminal == null) outcome.complete(page);
        else outcome.completeExceptionally(HBaseFailures.classify(terminal));
      });
      if (fatalCleanup != null) throw fatalCleanup;
    }

    @Override
    public void close() {
      ResultScanner owned;
      boolean beforeStart;
      synchronized (this) {
        cancelled = true;
        if (closeStarted) return;
        closeStarted = true;
        owned = scanner;
        scanner = null;
        beforeStart = !started && !completing;
      }
      try {
        if (owned != null) owned.close();
        cleanupComplete.complete(null);
      } catch (RuntimeException | Error failure) {
        cleanupComplete.completeExceptionally(failure);
        throw failure;
      }
      if (beforeStart) {
        try {
          if (registration != null) registration.close();
        } catch (Exception cleanupFailure) {
          outcome.completeExceptionally(HBaseFailures.classify(cleanupFailure));
          throw new IllegalStateException("scan registration cleanup failed", cleanupFailure);
        }
        outcome.completeExceptionally(HBaseFailures.classify(new CancellationException()));
      }
    }
  }

  private static BackendFailure resourceLimit() {
    return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
  }
}
