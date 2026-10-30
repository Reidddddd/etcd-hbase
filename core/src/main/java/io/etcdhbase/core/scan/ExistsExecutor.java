package io.etcdhbase.core.scan;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Unordered metadata existence: one positive suffices, a negative requires every bucket. */
public final class ExistsExecutor implements AutoCloseable {
  public record Result(int count, StoragePage.Cost cost) {
    public Result { if (count != 0 && count != 1) throw new IllegalArgumentException("existence is zero or one"); }
  }

  private final ScanSource source;
  private final ScanRequest template;
  private final int bucketCount;
  private final RequestScope scope;
  private final RequestScope.ScopedCleanup registration;
  private final CompletableFuture<Result> result = new CompletableFuture<>();
  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean stopped = new AtomicBoolean();
  private final AtomicInteger draining = new AtomicInteger();
  private final AtomicReference<Completion> completion = new AtomicReference<>();
  private BucketCursor cursor;
  private int bucket;
  private boolean waiting;
  private boolean terminal;
  private StoragePage.Cost cost = new StoragePage.Cost(0, 0, 0);

  public ExistsExecutor(ScanSource source, ScanRequest template, int bucketCount, RequestScope scope) {
    if (bucketCount < 1 || bucketCount > 4096) throw new IllegalArgumentException("invalid bucket count");
    this.source = source;
    this.template = template;
    this.bucketCount = bucketCount;
    this.scope = scope;
    registration = scope.registerScopedCleanup(this);
  }

  public static CompletableFuture<Integer> point(RecordStore store, ByteKey key, long upper,
      BudgetManager.Account account) {
    return PointRead.get(store, key, upper, ReadProjection.METADATA, account)
        .thenApply(record -> record.filter(value -> value.metadata().state() == StorageMetadata.State.LIVE).isPresent() ? 1 : 0);
  }

  public CompletableFuture<Result> execute() {
    if (!started.compareAndSet(false, true)) return CompletableFuture.failedFuture(new IllegalStateException("existence query already started"));
    drain();
    return result.copy();
  }

  private void drain() {
    if (draining.getAndIncrement() != 0) return;
    int missed = 1;
    do {
      try { while (step()) { /* continue until one physical page is pending */ } }
      catch (Exception failure) { finish(0, failure); }
      catch (Error fatal) { finish(0, fatal); throw fatal; }
      missed = draining.addAndGet(-missed);
    } while (missed != 0);
  }

  private boolean step() throws BackendFailure {
    Completion arrived = completion.getAndSet(null);
    if (terminal || stopped.get() || scope.isClosed() || scope.isDeadlineExceeded()) {
      if (arrived != null && arrived.page != null) arrived.page.close();
      if (!terminal) finish(0, new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
      return false;
    }
    if (arrived != null) {
      waiting = false;
      if (arrived.failure != null) throw unwrap(arrived.failure);
      boolean found;
      boolean exhausted;
      try (var page = arrived.page) {
        cost = cost.plus(page.cost());
        found = page.next().filter(record -> record.metadata().state() == StorageMetadata.State.LIVE).isPresent();
        exhausted = page.exhausted();
      }
      if (found) { finish(1, null); return false; }
      if (exhausted) { cursor.close(); cursor = null; bucket++; }
    }
    if (waiting) return false;
    if (bucket == bucketCount) { finish(0, null); return false; }
    if (cursor == null) cursor = new BucketCursor(source, new ScanRequest(bucket, template.range(), template.after(),
        template.revisionUpperBound(), ReadProjection.METADATA, 1, template.maximumBytes(), template.maximumPhysicalRows()), scope);
    waiting = true;
    cursor.fetch().whenComplete((page, failure) -> { completion.set(new Completion(page, failure)); drain(); });
    return true;
  }

  private void finish(int count, Throwable failure) {
    if (terminal) return;
    terminal = true;
    Error fatal = failure instanceof Error error ? error : null;
    var resources = new AutoCloseable[] {cursor, registration};
    cursor = null;
    for (var resource : resources) {
      try { if (resource != null) resource.close(); }
      catch (Exception | Error cleanup) {
        if (failure == null) failure = cleanup;
        else if (failure != cleanup) failure.addSuppressed(cleanup);
        if (fatal == null && cleanup instanceof Error error) fatal = error;
      }
    }
    if (failure == null) result.complete(new Result(count, cost));
    else result.completeExceptionally(failure);
    if (fatal != null) throw fatal;
  }

  @Override public void close() { stopped.set(true); drain(); }
  private static BackendFailure unwrap(Throwable failure) {
    while (failure instanceof java.util.concurrent.CompletionException && failure.getCause() != null) failure = failure.getCause();
    return failure instanceof BackendFailure backend ? backend : new BackendFailure(BackendFailure.Category.UNKNOWN, failure);
  }
  private record Completion(BucketCursor.Page page, Throwable failure) {}
}
