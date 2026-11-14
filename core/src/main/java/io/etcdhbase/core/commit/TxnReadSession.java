package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.BudgetedRecordStore;
import io.etcdhbase.core.storage.BatchWriteResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.ToIntFunction;

/** Owns all pinned read views derived from a single transaction control barrier. */
public final class TxnReadSession implements AutoCloseable {
  private enum State { ACTIVE, FINISHING, FINISHED, CLOSED }
  private static final Executor CLEANUP = task -> Thread.ofVirtual().name("txn-read-cleanup").start(task);

  private final ReadViewService service;
  private final CommitState barrier;
  private final RecordStore physical;
  private final ManifestCatalog manifests;
  private final ToIntFunction<ByteKey> bucketSelector;
  private final int bucketCount;
  private final RequestScope scope;
  private final ByteKey authorizationScopeDigest;
  private final long currentRevision;
  private final long barrierSequence;
  private final PinnedReadView currentPinnedView;
  private final boolean readyForMutation;
  private final Map<Long, ReadViewRecordStore> stores = new LinkedHashMap<>();
  private final Map<Long, RecordStore> readers = new LinkedHashMap<>();
  private State state = State.ACTIVE;
  private boolean expanding;
  private CompletableFuture<Void> finished;

  private TxnReadSession(ReadViewService service, ReadViewService.TransactionViews captured, RecordStore physical,
      ManifestCatalog manifests, ToIntFunction<ByteKey> bucketSelector, int bucketCount,
      ByteKey authorizationScopeDigest, RequestScope scope) {
    this.service = service;
    this.barrier = captured.barrier();
    this.physical = physical;
    this.manifests = manifests;
    this.bucketSelector = bucketSelector;
    this.bucketCount = bucketCount;
    this.scope = scope;
    this.authorizationScopeDigest = authorizationScopeDigest;
    this.currentRevision = captured.currentRevision();
    this.barrierSequence = captured.barrierSequence();
    PinnedReadView current = Objects.requireNonNull(captured.views().get(currentRevision),
        "current transaction view");
    this.currentPinnedView = current;
    ReadView currentReadView = current.view();
    this.readyForMutation = currentReadView.selectedDecision().isEmpty()
        && currentReadView.finalizedSequence() == currentReadView.barrierSequence();
    install(captured.views());
  }

  private void install(Map<Long, PinnedReadView> views) {
    for (var entry : views.entrySet()) {
      var store = new ReadViewRecordStore(entry.getValue(), physical, manifests,
          bucketSelector, bucketCount, scope);
      stores.put(entry.getKey(), store);
      readers.put(entry.getKey(), new SessionReader(store));
    }
  }

  /** Revision 0 denotes the captured current revision; all requested targets share one barrier. */
  public static CompletableFuture<TxnReadSession> open(ReadViewService service, List<Long> revisions,
      ByteKey authorizationScopeDigest, RecordStore physical, ManifestCatalog manifests,
      ToIntFunction<ByteKey> bucketSelector, int bucketCount, RequestScope scope) {
    return open(service, revisions, authorizationScopeDigest, physical, manifests,
        bucketSelector, bucketCount, scope, false);
  }

  /** Opens a KV transaction session whose current view also protects Lease auxiliary history. */
  public static CompletableFuture<TxnReadSession> openAuxiliary(ReadViewService service,
      List<Long> revisions, ByteKey authorizationScopeDigest, RecordStore physical,
      ManifestCatalog manifests, ToIntFunction<ByteKey> bucketSelector, int bucketCount,
      RequestScope scope) {
    return open(service, revisions, authorizationScopeDigest, physical, manifests,
        bucketSelector, bucketCount, scope, true);
  }

  private static CompletableFuture<TxnReadSession> open(ReadViewService service, List<Long> revisions,
      ByteKey authorizationScopeDigest, RecordStore physical, ManifestCatalog manifests,
      ToIntFunction<ByteKey> bucketSelector, int bucketCount, RequestScope scope,
      boolean auxiliary) {
    Objects.requireNonNull(service, "service");
    Objects.requireNonNull(revisions, "revisions");
    Objects.requireNonNull(authorizationScopeDigest, "authorizationScopeDigest");
    Objects.requireNonNull(physical, "physical");
    Objects.requireNonNull(manifests, "manifests");
    Objects.requireNonNull(bucketSelector, "bucketSelector");
    Objects.requireNonNull(scope, "scope");
    if (bucketCount <= 0 || bucketCount > 4096) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid transaction bucket count"));
    }
    var capturedView = auxiliary
        ? service.openAuxiliaryForTransaction(revisions, authorizationScopeDigest, scope)
        : service.openForTransaction(revisions, authorizationScopeDigest, scope);
    return capturedView.thenApply(captured -> {
      try {
        var session = new TxnReadSession(service, captured, physical, manifests, bucketSelector, bucketCount,
            authorizationScopeDigest, scope);
        if (scope.isClosed() || scope.isDeadlineExceeded()) {
          throw new java.util.concurrent.CompletionException(inactive(scope));
        }
        return session;
      } catch (RuntimeException | Error failure) {
        try { scope.cancelAsync(CLEANUP); }
        catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
        throw failure;
      }
    });
  }

  public long currentRevision() { return currentRevision; }
  public long barrierSequence() { return barrierSequence; }
  public long compactRevision() { return barrier.compactRevision(); }
  public boolean belongsTo(RequestScope expectedScope) { return scope == expectedScope; }
  public boolean authorizes(ByteKey expectedDigest) { return authorizationScopeDigest.equals(expectedDigest); }
  public boolean readyForMutation() { return readyForMutation; }

  /** Returns the current Lease-capable view only when this session was opened with auxiliary protection. */
  public synchronized PinnedReadView currentAuxiliaryView() {
    if (state != State.ACTIVE || expanding || scope.isClosed() || scope.isDeadlineExceeded()
        || !currentPinnedView.protectsAuxiliary() || !currentPinnedView.isOpen()) {
      throw new IllegalStateException("transaction session has no active auxiliary-protected view");
    }
    return currentPinnedView;
  }

  /** Pins only selected historical targets, all derived from the original control barrier. */
  public CompletableFuture<Void> pinRevisions(List<Long> revisions) {
    Objects.requireNonNull(revisions, "revisions");
    final List<Long> missing;
    synchronized (this) {
      if (state != State.ACTIVE || expanding || scope.isClosed() || scope.isDeadlineExceeded()) {
        return CompletableFuture.failedFuture(inactive(scope));
      }
      var selected = new LinkedHashSet<Long>();
      for (Long revision : revisions) {
        if (revision == null || revision <= 0 || revision > currentRevision
            || revision < barrier.compactRevision()) {
          return CompletableFuture.failedFuture(new IllegalArgumentException("invalid historical pin target"));
        }
        if (!stores.containsKey(revision)) selected.add(revision);
      }
      if (selected.size() + stores.size() > ReadViewService.MAX_TRANSACTION_TARGETS) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      if (selected.isEmpty()) return CompletableFuture.completedFuture(null);
      missing = List.copyOf(selected);
      expanding = true;
    }
    CompletableFuture<Void> operation = service.pinTargets(barrier, missing, authorizationScopeDigest, scope).thenAccept(opened -> {
      synchronized (TxnReadSession.this) {
        if (state != State.ACTIVE || scope.isClosed() || scope.isDeadlineExceeded()) {
          throw new java.util.concurrent.CompletionException(inactive(scope));
        }
        install(opened);
      }
    }).whenComplete((ignored, failure) -> {
      synchronized (TxnReadSession.this) { expanding = false; }
      if (failure != null) {
        try { scope.cancelAsync(CLEANUP); }
        catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
      }
    });
    // A caller abandoning its dependent future must not cancel pin installation or cleanup.
    return operation.copy();
  }

  /** Child readers may complete their own operations but cannot release a transaction pin. */
  public synchronized RecordStore reader(long revision) {
    if (state != State.ACTIVE || expanding || scope.isClosed() || scope.isDeadlineExceeded()) {
      throw new IllegalStateException("transaction read session is not active");
    }
    RecordStore reader = readers.get(revision == 0 ? currentRevision : revision);
    if (reader == null) throw new IllegalArgumentException("revision was not captured by the transaction");
    return reader;
  }

  /** Captures metadata for an already pinned target; sending still requires {@link #finish()}. */
  public synchronized ReadView protectedView(long revision) {
    if (state != State.ACTIVE || expanding || scope.isClosed() || scope.isDeadlineExceeded()) {
      throw new IllegalStateException("transaction read session is not active");
    }
    ReadViewRecordStore store = stores.get(revision == 0 ? currentRevision : revision);
    if (store == null) throw new IllegalArgumentException("revision was not captured by the transaction");
    return store.protectedView();
  }

  /** The caller retains all child outputs until this final protection check has succeeded. */
  public CompletableFuture<Void> finish() {
    CompletableFuture<Void> completion;
    synchronized (this) {
      if (finished != null) return finished.copy();
      if (state != State.ACTIVE || expanding || scope.isClosed() || scope.isDeadlineExceeded()) {
        return CompletableFuture.failedFuture(inactive(scope));
      }
      state = State.FINISHING;
      finished = new CompletableFuture<>();
      completion = finished;
    }
    var checks = stores.values().stream().map(TxnReadSession::finishStore).toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(checks).whenComplete((ignored, failure) -> {
      Throwable outcome = failure == null ? null : CompletionFailures.unwrapCompletion(failure);
      synchronized (TxnReadSession.this) {
        if (outcome == null) {
          // Claim successful completion while request cancellation is excluded. Otherwise a pin
          // can finish after the scope closes and an abandoned transaction can report success.
          if (state != State.FINISHING || !scope.startIfOpen(() -> state = State.FINISHED)) {
            outcome = inactive(scope);
            state = State.CLOSED;
          }
        } else {
          state = State.CLOSED;
        }
      }
      if (outcome != null) {
        try { scope.cancelAsync(CLEANUP); }
        catch (RuntimeException | Error cleanup) { if (cleanup != outcome) outcome.addSuppressed(cleanup); }
        completion.completeExceptionally(outcome);
      } else {
        completion.complete(null);
      }
    });
    return completion.copy();
  }

  private static CompletableFuture<Void> finishStore(ReadViewRecordStore store) {
    try { return Objects.requireNonNull(store.finishRead(), "read completion"); }
    catch (RuntimeException | Error failure) { return CompletableFuture.failedFuture(failure); }
  }

  /** Aborts an unfinished transaction; the scope retains pins until physical producers drain. */
  @Override public void close() {
    synchronized (this) {
      if (state == State.FINISHED || state == State.CLOSED) return;
      state = State.CLOSED;
    }
    scope.cancelAsync(CLEANUP);
  }

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }

  private final class SessionReader implements RecordStore, BudgetedRecordStore {
    private final ReadViewRecordStore delegate;
    private SessionReader(ReadViewRecordStore delegate) { this.delegate = delegate; }
    @Override public CompletableFuture<Optional<StoredRecord>> get(
        ByteKey key, long upper, ReadProjection projection) { return delegate.get(key, upper, projection); }
    @Override public CompletableFuture<Optional<StoredRecord>> get(
        ByteKey key, long upper, ReadProjection projection, BudgetManager.Account account) {
      return delegate.get(key, upper, projection, account);
    }
    @Override public ScanOperation startScan(ScanRequest request) { return delegate.startScan(request); }
    @Override public ScanOperation startScan(ScanRequest request, BudgetManager.Account account) {
      return delegate.startScan(request, account);
    }
    @Override public boolean pointReadUsesScanner() { return delegate.pointReadUsesScanner(); }
    @Override public CompletableFuture<Void> finishRead() {
      synchronized (TxnReadSession.this) {
        return state == State.ACTIVE && !scope.isClosed() && !scope.isDeadlineExceeded()
            ? CompletableFuture.completedFuture(null) : CompletableFuture.failedFuture(inactive(scope));
      }
    }
    @Override public CompletableFuture<Void> put(StoredRecord record) { return delegate.put(record); }
    @Override public CompletableFuture<Boolean> compareAndSet(
        ByteKey key, Optional<StorageMetadata> expected, StoredRecord replacement) {
      return delegate.compareAndSet(key, expected, replacement);
    }
    @Override public CompletableFuture<List<BatchWriteResult>> putBatch(List<StoredRecord> records) {
      return delegate.putBatch(records);
    }
  }
}
