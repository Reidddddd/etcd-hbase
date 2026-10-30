package io.etcdhbase.core.scan;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

/** One logical bucket cursor; physical page scanners belong to the request-bound store. */
public final class BucketCursor implements AutoCloseable {
  public enum State { IDLE, FETCHING, PAGE_READY, PAUSED, EXHAUSTED, FAILED, CLOSED }

  private final RecordStore store;
  private final ScanRequest template;
  private final RequestScope scope;
  private final RequestScope.ScopedCleanup registration;
  private Optional<ByteKey> checkpoint;
  private State state = State.IDLE;
  private boolean pauseRequested;
  private ScanOperation activeRead;
  private Throwable failure;
  private CompletableFuture<Page> pending;
  private Page outstanding;

  public BucketCursor(RecordStore store, ScanRequest template, RequestScope scope) {
    this.store = Objects.requireNonNull(store, "store");
    this.template = Objects.requireNonNull(template, "template");
    this.scope = Objects.requireNonNull(scope, "scope");
    this.checkpoint = template.after();
    this.registration = scope.registerScopedCleanup(this);
  }

  public synchronized State state() { return state; }

  public CompletableFuture<Page> fetch() {
    CompletableFuture<Page> result;
    ScanRequest request;
    synchronized (this) {
      if (state == State.FAILED) return CompletableFuture.failedFuture(failure);
      if (state == State.CLOSED || scope.isClosed()) return CompletableFuture.failedFuture(cancelled());
      if (scope.isDeadlineExceeded()) return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.TIMEOUT));
      if (state != State.IDLE) return CompletableFuture.failedFuture(new IllegalStateException("cursor cannot fetch in this state"));
      state = State.FETCHING;
      result = new CompletableFuture<>();
      pending = result;
      request = new ScanRequest(template.bucket(), template.range(), checkpoint,
          template.revisionUpperBound(), template.projection(), template.maximumRows(),
          template.maximumBytes(), template.maximumPhysicalRows());
    }
    try {
      ScanOperation read = Objects.requireNonNull(store.startScan(request), "page operation");
      boolean stop;
      synchronized (this) {
        activeRead = read;
        stop = state == State.CLOSED || scope.isClosed();
      }
      read.completion().whenComplete((page, problem) -> {
        synchronized (this) {
          if (activeRead == read) activeRead = null;
        }
        receive(result, request, page, problem);
      });
      if (stop) read.close();
    } catch (RuntimeException problem) {
      fail(result, problem);
    } catch (Error fatal) {
      fail(result, fatal);
      throw fatal;
    }
    return result.copy();
  }

  public synchronized void pause() {
    pauseRequested = true;
    if (state == State.IDLE) state = State.PAUSED;
  }

  public synchronized void resume() {
    pauseRequested = false;
    if (state == State.PAUSED) state = State.IDLE;
  }

  private void receive(CompletableFuture<Page> result, ScanRequest request, StoragePage source, Throwable problem) {
    if (problem != null) {
      fail(result, problem);
      return;
    }
    try {
      validate(source, request);
    } catch (RuntimeException invalid) {
      fail(result, invalid);
      return;
    }
    Page page = null;
    synchronized (this) {
      if (state != State.CLOSED && !scope.isClosed()) {
        page = new Page(source);
        outstanding = page;
        state = State.PAGE_READY;
      }
    }
    if (page == null) {
      result.completeExceptionally(cancelled());
      return;
    }
    if (!result.complete(page)) page.close();
    synchronized (this) {
      if (pending == result) pending = null;
    }
  }

  private static void validate(StoragePage page, ScanRequest request) {
    Objects.requireNonNull(page, "page");
    if (page.records().isEmpty() && !page.exhausted()) throw new IllegalStateException("page made no logical progress");
    if (page.records().size() > request.maximumRows()) throw new IllegalStateException("page exceeded its row bound");
    long retained = 0;
    Optional<ByteKey> previous = request.after();
    for (StoredRecord record : page.records()) {
      if (!request.range().contains(record.key()) || previous.map(key -> key.compareTo(record.key()) >= 0).orElse(false)) {
        throw new IllegalStateException("page violates its ordered logical range");
      }
      retained = Math.addExact(retained, record.retainedBytes());
      previous = Optional.of(record.key());
    }
    if (retained > request.maximumBytes()) throw new IllegalStateException("page exceeded its byte bound");
  }

  private void fail(CompletableFuture<Page> result, Throwable problem) {
    synchronized (this) {
      if (state == State.CLOSED) return;
      failure = problem;
      state = State.FAILED;
      if (pending == result) pending = null;
    }
    result.completeExceptionally(problem);
    releaseRegistration();
  }

  private void release(Page page, Optional<ByteKey> consumed, boolean exhausted) {
    synchronized (this) {
      if (outstanding != page) return;
      outstanding = null;
      if (state == State.CLOSED || state == State.FAILED) return;
      if (consumed.isPresent()) checkpoint = consumed;
      state = exhausted ? State.EXHAUSTED : pauseRequested ? State.PAUSED : State.IDLE;
      if (pending != null && pending.isDone()) pending = null;
    }
    if (exhausted) releaseRegistration();
  }

  @Override
  public void close() {
    Page page;
    CompletableFuture<Page> operation;
    ScanOperation read;
    synchronized (this) {
      if (state == State.CLOSED) return;
      if (state != State.EXHAUSTED && state != State.FAILED) state = State.CLOSED;
      page = outstanding;
      outstanding = null;
      operation = pending;
      pending = null;
      read = activeRead;
      activeRead = null;
    }
    try {
      if (read != null) read.close();
    } finally {
      try {
        if (page != null) page.close();
      } finally {
        if (operation != null) operation.completeExceptionally(cancelled());
        releaseRegistration();
      }
    }
  }

  private void releaseRegistration() {
    if (registration == null) return; // A scope already closed during constructor ownership transfer.
    try {
      registration.close();
    } catch (Exception cleanupFailure) {
      throw new IllegalStateException("cursor cleanup failed", cleanupFailure);
    }
  }

  private static BackendFailure cancelled() {
    return new BackendFailure(BackendFailure.Category.CANCELLED);
  }

  /** Closing discards unread records and advances only through records actually handed out. */
  public final class Page implements AutoCloseable {
    private List<StoredRecord> records;
    private final boolean sourceExhausted;
    private final StoragePage.Cost cost;
    private int position;
    private boolean closed;

    private Page(StoragePage source) {
      records = source.records();
      sourceExhausted = source.exhausted();
      cost = source.cost();
    }

    public synchronized Optional<StoredRecord> next() {
      requireOpen();
      if (scope.isClosed()) throw new CancellationException();
      return position == records.size() ? Optional.empty() : Optional.of(records.get(position++));
    }

    public synchronized boolean exhausted() {
      requireOpen();
      return sourceExhausted && position == records.size();
    }

    public StoragePage.Cost cost() { return cost; }

    @Override
    public void close() {
      Optional<ByteKey> consumed;
      boolean exhausted;
      synchronized (this) {
        if (closed) return;
        closed = true;
        consumed = position == 0 ? Optional.empty() : Optional.of(records.get(position - 1).key());
        exhausted = sourceExhausted && position == records.size();
        records = List.of();
      }
      release(this, consumed, exhausted);
    }

    private void requireOpen() {
      if (closed) throw new IllegalStateException("cursor page is closed");
    }
  }
}
