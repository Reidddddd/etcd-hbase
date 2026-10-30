package io.etcdhbase.core.scan;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Frozen-data merge with one head per bucket, global Limit and one extra visible result for More. */
public final class SortedMergeCursor implements AutoCloseable {
  private final List<BucketCursor> buckets;
  private final BudgetManager.Account account;
  private final RequestScope scope;
  private final RequestScope.ScopedCleanup registration;
  private final PriorityQueue<Head> heap = new PriorityQueue<>(Comparator.comparing(head -> head.record.key()));
  private final Head[] heads;
  private final boolean[] exhausted;
  private final List<StoredRecord> output = new ArrayList<>();
  private final List<BudgetManager.Lease> outputLeases = new ArrayList<>();
  private final CompletableFuture<RangePage> result = new CompletableFuture<>();
  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean stopRequested = new AtomicBoolean();
  private final AtomicInteger draining = new AtomicInteger();
  private final AtomicReference<Completion> completion = new AtomicReference<>();
  // The drain loop alone mutates merge state; callbacks publish one bounded completion slot.
  private int initializing;
  private int refill = -1;
  private boolean waiting;
  private volatile boolean terminal;
  private int limit;
  private ByteKey previous;
  private StoragePage.Cost cost = new StoragePage.Cost(0, 0, 0);

  public SortedMergeCursor(ScanSource source, ScanRequest template, int bucketCount,
      BudgetManager.Account account, RequestScope scope) {
    if (bucketCount < 1 || bucketCount > 4096
        || account.reservation().heads() / bucketCount < template.maximumBytes()) {
      throw new IllegalArgumentException("all bucket heads require reserved memory before scanning");
    }
    this.account = account;
    this.scope = scope;
    heads = new Head[bucketCount];
    exhausted = new boolean[bucketCount];
    List<BucketCursor> opened = new ArrayList<>();
    for (int bucket = 0; bucket < bucketCount; bucket++) {
      opened.add(new BucketCursor(source, new ScanRequest(bucket, template.range(), template.after(),
          template.revisionUpperBound(), template.projection(), 1, template.maximumBytes(), template.maximumPhysicalRows()), scope));
    }
    buckets = List.copyOf(opened);
    registration = scope.registerScopedCleanup(this);
  }

  public CompletableFuture<RangePage> read(int limit) {
    if (limit < 0) return CompletableFuture.failedFuture(new IllegalArgumentException("limit must not be negative"));
    if (!started.compareAndSet(false, true)) return CompletableFuture.failedFuture(new IllegalStateException("merge already started"));
    this.limit = limit;
    drain();
    return result.copy();
  }

  private void drain() {
    if (draining.getAndIncrement() != 0) return;
    int missed = 1;
    do {
      try {
        while (step()) { /* finite CPU steps until the next asynchronous page */ }
      } catch (Exception failure) {
        finish(false, failure);
      } catch (Error fatal) {
        finish(false, fatal);
        throw fatal;
      }
      missed = draining.addAndGet(-missed);
    } while (missed != 0);
  }

  private boolean step() throws BackendFailure {
    Completion arrived = completion.getAndSet(null);
    if (terminal) {
      if (arrived != null && arrived.page != null) arrived.page.close();
      return false;
    }
    if (stopRequested.get() || scope.isClosed() || scope.isDeadlineExceeded()) {
      if (arrived != null && arrived.page != null) arrived.page.close();
      finish(false, new BackendFailure(scope.isDeadlineExceeded() ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
      return false;
    }
    if (arrived != null) accept(arrived);
    if (waiting) return false;
    if (refill >= 0) {
      int bucket = refill;
      refill = -1;
      fetch(bucket);
      return true;
    }
    if (initializing < buckets.size()) {
      fetch(initializing++);
      return true;
    }
    if (heap.isEmpty()) { finish(false, null); return false; }
    Head head = heap.remove();
    if (previous != null && previous.compareTo(head.record.key()) >= 0) throw new IllegalStateException("duplicate or unordered bucket result");
    if (limit > 0 && output.size() == limit) { finish(true, null); return false; }
    var lease = account.tryRetain(BudgetManager.Memory.OUTPUT, head.record.retainedBytes()).orElseThrow(SortedMergeCursor::resourceLimit);
    outputLeases.add(lease);
    output.add(head.record);
    previous = head.record.key();
    heads[head.bucket] = null;
    head.lease.close();
    if (!exhausted[head.bucket]) refill = head.bucket;
    return true;
  }

  private void fetch(int bucket) {
    waiting = true;
    buckets.get(bucket).fetch().whenComplete((page, failure) -> {
      completion.set(new Completion(bucket, page, failure));
      drain();
    });
  }

  private void accept(Completion arrived) throws BackendFailure {
    waiting = false;
    if (arrived.failure != null) throw unwrap(arrived.failure);
    try (var page = arrived.page) {
      cost = new StoragePage.Cost(Math.addExact(cost.physicalRows(), page.cost().physicalRows()),
          Math.addExact(cost.returnedCellBytes(), page.cost().returnedCellBytes()),
          Math.addExact(cost.scanRpcCalls(), page.cost().scanRpcCalls()));
      Optional<StoredRecord> candidate = page.next();
      exhausted[arrived.bucket] = page.exhausted();
      if (candidate.isEmpty()) return;
      StoredRecord record = candidate.get();
      if (record.metadata().state() == StorageMetadata.State.TOMBSTONE) {
        if (!exhausted[arrived.bucket]) refill = arrived.bucket;
        return;
      }
      var lease = account.tryRetain(BudgetManager.Memory.HEAD, record.retainedBytes()).orElseThrow(SortedMergeCursor::resourceLimit);
      var head = new Head(arrived.bucket, record, lease);
      heads[arrived.bucket] = head;
      heap.add(head);
    }
  }

  private void finish(boolean more, Throwable failure) {
    if (terminal) return;
    terminal = true;
    for (var bucket : buckets) {
      try { bucket.close(); }
      catch (RuntimeException | Error cleanup) {
        if (failure == null) failure = cleanup;
        else if (failure != cleanup) failure.addSuppressed(cleanup);
      }
    }
    for (Head head : heads) if (head != null) head.lease.close();
    java.util.Arrays.fill(heads, null);
    heap.clear();
    RangePage response = null;
    if (failure == null) {
      try { response = new RangePage(output, outputLeases, more, cost, scope); }
      catch (RuntimeException | Error allocationFailure) { failure = allocationFailure; }
    }
    if (failure != null) for (var lease : outputLeases) lease.close();
    output.clear();
    outputLeases.clear();
    try {
      if (registration != null) registration.close();
    } catch (Exception cleanup) {
      if (failure == null) failure = cleanup;
      else failure.addSuppressed(cleanup);
    }
    if (failure == null && scope.isClosed()) failure = new BackendFailure(BackendFailure.Category.CANCELLED);
    if (failure != null) {
      if (response != null) response.close();
      result.completeExceptionally(failure);
    } else result.complete(response);
  }

  @Override public void close() { stopRequested.set(true); drain(); }

  private static BackendFailure unwrap(Throwable failure) {
    while (failure instanceof java.util.concurrent.CompletionException && failure.getCause() != null) failure = failure.getCause();
    return failure instanceof BackendFailure backend ? backend : new BackendFailure(BackendFailure.Category.UNKNOWN, failure);
  }
  private static BackendFailure resourceLimit() { return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT); }
  private record Head(int bucket, StoredRecord record, BudgetManager.Lease lease) {}
  private record Completion(int bucket, BucketCursor.Page page, Throwable failure) {}
}
