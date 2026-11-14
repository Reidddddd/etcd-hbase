package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.AsyncCursor;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.ManifestRepository;
import io.etcdhbase.core.commit.PagedWorksetIndex;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.concurrent.ContinuationExecutor;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseBindingChange;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.ScanDirection;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

/** Durable private overlay whose completed operations alone are visible to this invocation. */
public final class TxnWorkingSet {
  public static final long MIN_RETAINED_BYTES = 16_384;
  private static final long FIXED_OPERATION_BYTES = 4096;
  private static final long COPY_ALLOWANCE = 16;
  // Leave the rest of the retained bound for a complete index path and the current event copies.
  private static final int PAGE_DEPTH_ALLOWANCE = 128;
  private static final ContinuationExecutor CONTINUE = new ContinuationExecutor();

  private final InvocationBinding invocation;
  private final long maximumRetainedBytes;
  private final long eventReadBytes;
  private final RequestScope scope;
  private final BudgetManager.Account account;
  private final ManifestRepository manifests;
  private final ToIntFunction<ByteKey> bucket;
  private final PagedWorksetIndex index;
  private final PutPlanner planner = new PutPlanner();
  private final DeletePlanner deletePlanner = new DeletePlanner();
  private final LeaseBindingChangeSet leaseBindingChanges;
  private boolean begun;
  private boolean updating;
  private boolean frozen;
  private volatile boolean failed;

  /** The caller reserves the working bound in its request ledger until all consumers drain. */
  public TxnWorkingSet(InvocationBinding invocation, long maximumRetainedBytes,
      SystemRecordStore records, ManifestRepository manifests, RequestScope scope,
      BudgetManager.Account account, ToIntFunction<ByteKey> bucket) {
    this(invocation, maximumRetainedBytes, records, manifests, scope, account, bucket, false);
  }

  /** Enables bounded Lease-delta tracking only when the table-group runtime has M4 enabled. */
  TxnWorkingSet(InvocationBinding invocation, long maximumRetainedBytes,
      SystemRecordStore records, ManifestRepository manifests, RequestScope scope,
      BudgetManager.Account account, ToIntFunction<ByteKey> bucket, boolean trackLeaseBindings) {
    this.invocation = Objects.requireNonNull(invocation, "invocation");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.scope = Objects.requireNonNull(scope, "scope");
    this.account = Objects.requireNonNull(account, "account");
    this.bucket = Objects.requireNonNull(bucket, "bucket");
    if (invocation.state() != InvocationBinding.State.PREPARING || maximumRetainedBytes < MIN_RETAINED_BYTES
        || maximumRetainedBytes > ResultTree.MAX_RETAINED_BYTES || !account.belongsTo(scope)
        || maximumRetainedBytes > account.reservation().candidates()) {
      throw new IllegalArgumentException("invalid transaction working-set bound or invocation");
    }
    this.maximumRetainedBytes = maximumRetainedBytes;
    this.eventReadBytes = Math.min(PreparedEvent.MAX_EVENT_BYTES,
        Math.addExact(FIXED_OPERATION_BYTES, maximumRetainedBytes / 8));
    int pageBytes = (int) Math.min(1024 * 1024, maximumRetainedBytes / PAGE_DEPTH_ALLOWANCE);
    this.index = new PagedWorksetIndex(ResultIdentity.from(invocation),
        Objects.requireNonNull(records, "records"), scope, account, pageBytes);
    this.leaseBindingChanges = trackLeaseBindings ? new LeaseBindingChangeSet(maximumRetainedBytes) : null;
  }

  /** Must succeed before the evaluator can access or change this private workset. */
  public CompletableFuture<Void> begin() {
    return index.begin().thenRun(() -> {
      synchronized (TxnWorkingSet.this) {
        requireReady();
        begun = true;
      }
    }).copy();
  }

  /** Returns a completed private value when present, otherwise the caller's fixed base record. */
  public CompletableFuture<Optional<StoredRecord>> current(ByteKey key, Optional<StoredRecord> base) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(base, "base");
    if (base.filter(record -> !record.key().equals(key)).isPresent()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("working-view base key differs"));
    }
    if (failed) return CompletableFuture.failedFuture(unavailable());
    int selected = checkedBucket(key);
    return fenceOnFailure(index.find(selected, key).thenCompose(found -> found.isEmpty()
        ? CompletableFuture.completedFuture(base)
        : event(found.orElseThrow()).thenApply(value -> Optional.of(value.current())))).copy();
  }

  /** Persists one Put and exposes it only after its operation checkpoint is durable. */
  public CompletableFuture<PutResult> put(PutCommand command, Optional<StoredRecord> base) {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(base, "base");
    synchronized (this) {
      if (!begun || failed || frozen || updating) return CompletableFuture.failedFuture(unavailable());
      updating = true;
    }
    CompletableFuture<PutResult> operation;
    try {
      if (accountingBytes(command, base) > maximumRetainedBytes / 2) {
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
      int selected = checkedBucket(command.key());
      operation = index.find(selected, command.key()).thenCompose(existing -> {
        if (existing.isEmpty()) return preparePut(command, base, selected, false);
        return event(existing.orElseThrow()).thenCompose(previous -> previous.kind() == PreparedEvent.Kind.DELETE
            ? preparePut(command, Optional.of(previous.current()), selected, true)
            : CompletableFuture.failedFuture(new KvFailure(KvFailure.Reason.DUPLICATE_KEY)));
      });
    } catch (BackendFailure | RuntimeException failure) {
      synchronized (this) { updating = false; failed = true; }
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      synchronized (this) { updating = false; failed = true; }
      throw fatal;
    }
    return operation.whenComplete((ignored, failure) -> {
      synchronized (TxnWorkingSet.this) {
        if (failure != null) failed = true;
        updating = false;
      }
    }).copy();
  }

  private CompletableFuture<PutResult> preparePut(PutCommand command, Optional<StoredRecord> previous,
      int selected, boolean workingView) {
    PutPlanner.Plan plan = leaseBindingChanges == null
        ? workingView ? planner.planWorking(command, invocation, previous)
            : planner.plan(command, invocation, previous)
        : workingView ? planner.planWorkingWithLease(command, invocation, previous)
            : planner.planWithLease(command, invocation, previous);
    if (manifests.candidateBucket(plan.candidate()) != selected) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("data and manifest buckets differ"));
    }
    PreparedEvent original = plan.event();
    PreparedEvent event = new PreparedEvent(invocation.transactionId(), invocation.preparedOwnerEpoch(),
        invocation.baseSequence(), invocation.baseRevision(), index.eventCount(), original.kind(),
        original.current(), original.previous());
    var reference = new PagedWorksetIndex.EventRef(selected, command.key(), event.ordinal(), event.digest());
    if (leaseBindingChanges != null) {
      try {
        leaseBindingChanges.record(previous, plan.candidate().record());
      } catch (BackendFailure failure) {
        return CompletableFuture.failedFuture(failure);
      }
    }
    return manifests.createEvent(event, scope).thenCompose(created -> {
      requireCreated(created);
      return index.commitOperation(AsyncCursor.fromIterator(List.of(reference).iterator()));
    }).thenApply(ignored -> {
      return plan.result();
    });
  }

  /** Streams a single ordered delete into one private operation checkpoint. */
  public CompletableFuture<Long> delete(KeyRange range, AsyncCursor<StoredRecord> view,
      Consumer<StoredRecord> onDelete) {
    Objects.requireNonNull(range, "range");
    Objects.requireNonNull(view, "view");
    Objects.requireNonNull(onDelete, "onDelete");
    synchronized (this) {
      if (!begun || failed || frozen || updating) return CompletableFuture.failedFuture(unavailable());
      updating = true;
    }
    var input = new DeleteEvents(range, view, onDelete, index.eventCount());
    CompletableFuture<Long> operation;
    try {
      operation = index.commitOperation(input).thenApply(ignored -> input.count);
    } catch (RuntimeException | Error failure) {
      synchronized (this) { updating = false; failed = true; }
      throw failure;
    }
    return operation.whenComplete((ignored, failure) -> {
      synchronized (TxnWorkingSet.this) {
        if (failure != null) failed = true;
        updating = false;
      }
    }).copy();
  }

  private final class DeleteEvents implements AsyncCursor<PagedWorksetIndex.EventRef> {
    private final KeyRange range;
    private final AsyncCursor<StoredRecord> view;
    private final Consumer<StoredRecord> onDelete;
    private final long firstOrdinal;
    private ByteKey previousKey;
    private long count;

    private DeleteEvents(KeyRange range, AsyncCursor<StoredRecord> view,
        Consumer<StoredRecord> onDelete, long firstOrdinal) {
      this.range = range;
      this.view = view;
      this.onDelete = onDelete;
      this.firstOrdinal = firstOrdinal;
    }

    @Override public CompletableFuture<Optional<PagedWorksetIndex.EventRef>> next() {
      var result = new CompletableFuture<Optional<PagedWorksetIndex.EventRef>>();
      advance(result);
      return result;
    }

    private void advance(CompletableFuture<Optional<PagedWorksetIndex.EventRef>> result) {
      try {
        Objects.requireNonNull(view.next(), "delete view future").whenCompleteAsync((found, failure) -> {
          if (failure != null) {
            result.completeExceptionally(failure);
            return;
          }
          accept(found, result);
        }, CONTINUE);
      } catch (RuntimeException failure) {
        result.completeExceptionally(failure);
      } catch (Error fatal) {
        result.completeExceptionally(fatal);
        throw fatal;
      }
    }

    private void accept(Optional<StoredRecord> found,
        CompletableFuture<Optional<PagedWorksetIndex.EventRef>> result) {
      try {
        if (Objects.requireNonNull(found, "delete view result").isEmpty()) {
          result.complete(Optional.empty());
          return;
        }
        StoredRecord observed = found.orElseThrow();
        validateObserved(observed);
        var planned = leaseBindingChanges == null
            ? deletePlanner.planWorking(invocation, Optional.of(observed), Math.addExact(firstOrdinal, count))
            : deletePlanner.planWorkingWithLease(invocation, Optional.of(observed), Math.addExact(firstOrdinal, count));
        if (planned.isEmpty()) {
          advance(result);
          return;
        }
        persist(observed, planned.orElseThrow(), result);
      } catch (RuntimeException failure) {
        result.completeExceptionally(failure);
      } catch (Error fatal) {
        result.completeExceptionally(fatal);
        throw fatal;
      }
    }

    private void validateObserved(StoredRecord observed) {
      if (!range.contains(observed.key()) || previousKey != null && previousKey.compareTo(observed.key()) >= 0) {
        throw new IllegalStateException("delete view is unordered or outside its requested range");
      }
      previousKey = observed.key();
      if (observed.metadata().state() == StorageMetadata.State.LIVE
          && Math.addExact(FIXED_OPERATION_BYTES, Math.multiplyExact(COPY_ALLOWANCE, observed.retainedBytes()))
              > maximumRetainedBytes / 2) {
        throw new java.util.concurrent.CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
    }

    private void persist(StoredRecord observed, DeletePlanner.Plan plan,
        CompletableFuture<Optional<PagedWorksetIndex.EventRef>> result) {
      int selected = checkedBucket(observed.key());
      if (manifests.candidateBucket(plan.candidate()) != selected) {
        throw new IllegalArgumentException("data and manifest buckets differ");
      }
      onDelete.accept(observed);
      PreparedEvent event = plan.event();
      Objects.requireNonNull(manifests.createEvent(event, scope), "delete event future")
          .whenComplete((created, failure) -> completePersisted(observed, selected, event, created, failure, result));
    }

    private void completePersisted(StoredRecord observed, int selected, PreparedEvent event,
        SystemRecordStore.CreateResult created, Throwable failure,
        CompletableFuture<Optional<PagedWorksetIndex.EventRef>> result) {
      if (failure != null) {
        result.completeExceptionally(failure);
        return;
      }
      try {
        requireCreated(created);
        if (leaseBindingChanges != null) {
          leaseBindingChanges.record(Optional.of(observed), event.current());
        }
        count = Math.addExact(count, 1);
        result.complete(Optional.of(new PagedWorksetIndex.EventRef(selected, observed.key(),
            event.ordinal(), event.digest())));
      } catch (BackendFailure failureDuringTracking) {
        result.completeExceptionally(failureDuringTracking);
      } catch (RuntimeException invalid) {
        result.completeExceptionally(invalid);
      } catch (Error fatal) {
        result.completeExceptionally(fatal);
        throw fatal;
      }
    }
  }

  public boolean hasChanges() { return index.eventCount() > 0; }
  public long candidateCount() { return index.entryCount(); }
  public long eventCount() { return index.eventCount(); }

  /** Returns immutable net binding changes after all private operations have completed. */
  public synchronized List<LeaseBindingChange> leaseBindingChanges() {
    requireReady();
    if (!frozen || updating) throw new IllegalStateException("workset is not frozen");
    return leaseBindingChanges == null ? List.of() : leaseBindingChanges.snapshot();
  }

  /** Freezes the completed checkpoint before candidates and ordered events are streamed. */
  public synchronized void freeze() {
    requireReady();
    if (!begun || updating) throw new IllegalStateException("workset is not ready to freeze");
    frozen = true;
  }

  /**
   * Merges one fixed base bucket with completed private operations in key order. The caller owns
   * the base cursor and its read-view protection until this cursor and the request have drained.
   * Tombstones are retained so the consumer can apply its own visibility and counting rules.
   */
  public AsyncCursor<StoredRecord> merge(int selectedBucket, KeyRange range, AsyncCursor<StoredRecord> base) {
    return merge(selectedBucket, range, base, ScanDirection.ASCENDING);
  }

  public AsyncCursor<StoredRecord> merge(int selectedBucket, KeyRange range, AsyncCursor<StoredRecord> base,
      ScanDirection direction) {
    requireReady();
    Objects.requireNonNull(range, "range");
    Objects.requireNonNull(base, "base");
    Objects.requireNonNull(direction, "direction");
    return new MergeCursor(selectedBucket, range, base, index.scan(selectedBucket, range, direction), direction);
  }

  /** Final candidates are streamed in bucket/key order, with each referenced event verified. */
  public AsyncCursor<PreparedCandidate> candidates() {
    requireFrozen();
    return new CandidateCursor(index.scanAll(), index.entryCount());
  }

  /** Replays the original event order and checks it against the completed checkpoint chain. */
  public AsyncCursor<PreparedEvent> events() {
    requireFrozen();
    return new EventCursor(index.eventCount(), index.eventChain());
  }

  private final class CandidateCursor implements AsyncCursor<PreparedCandidate> {
    private final AsyncCursor<PagedWorksetIndex.EventRef> source;
    private final long expected;
    private final AtomicBoolean reading = new AtomicBoolean();
    private long count;

    private CandidateCursor(AsyncCursor<PagedWorksetIndex.EventRef> source, long expected) {
      this.source = source;
      this.expected = expected;
    }

    @Override public CompletableFuture<Optional<PreparedCandidate>> next() {
      if (!reading.compareAndSet(false, true)) {
        return CompletableFuture.failedFuture(new IllegalStateException("concurrent workset candidate reads"));
      }
      try {
        return settleRead(source.next().thenCompose(this::accept), reading);
      } catch (RuntimeException failure) {
        reading.set(false);
        failed = true;
        return CompletableFuture.failedFuture(failure);
      } catch (Error fatal) {
        reading.set(false);
        failed = true;
        throw fatal;
      }
    }

    private CompletableFuture<Optional<PreparedCandidate>> accept(Optional<PagedWorksetIndex.EventRef> found) {
      if (found.isEmpty()) {
        if (count != expected) return CompletableFuture.failedFuture(new IllegalStateException("workset candidate count differs"));
        return CompletableFuture.completedFuture(Optional.empty());
      }
      return event(found.orElseThrow()).thenApply(value -> {
        count = Math.addExact(count, 1);
        return Optional.of(new PreparedCandidate(invocation.transactionId(), invocation.preparedOwnerEpoch(),
            invocation.baseSequence(), invocation.baseRevision(), value.current()));
      });
    }
  }

  private final class EventCursor implements AsyncCursor<PreparedEvent> {
    private final long expected;
    private final ByteKey expectedChain;
    private final AtomicBoolean reading = new AtomicBoolean();
    private long ordinal;
    private ByteKey chain = ByteKey.copyOf(new byte[32]);

    private EventCursor(long expected, ByteKey expectedChain) {
      this.expected = expected;
      this.expectedChain = expectedChain;
    }

    @Override public CompletableFuture<Optional<PreparedEvent>> next() {
      if (!reading.compareAndSet(false, true)) {
        return CompletableFuture.failedFuture(new IllegalStateException("concurrent workset event reads"));
      }
      if (ordinal == expected) {
        reading.set(false);
        return fenceOnFailure(chain.equals(expectedChain) ? CompletableFuture.completedFuture(Optional.empty())
            : CompletableFuture.failedFuture(new IllegalStateException("workset event chain differs")));
      }
      try {
        return settleRead(readEvent(ordinal).thenApply(this::accept), reading);
      } catch (Error fatal) {
        reading.set(false);
        failed = true;
        throw fatal;
      }
    }

    private Optional<PreparedEvent> accept(PreparedEvent event) {
      var reference = new PagedWorksetIndex.EventRef(checkedBucket(event.current().key()),
          event.current().key(), event.ordinal(), event.digest());
      chain = PagedWorksetIndex.advanceEventChain(chain, reference);
      ordinal++;
      return Optional.of(event);
    }
  }

  private CompletableFuture<PreparedEvent> event(PagedWorksetIndex.EventRef reference) {
    return readEvent(reference.ordinal()).thenApply(event -> {
      if (!event.current().key().equals(reference.key()) || !event.digest().equals(reference.digest())
          || checkedBucket(event.current().key()) != reference.bucket()) {
        throw new IllegalStateException("workset event differs from its completed index");
      }
      return event;
    });
  }

  private CompletableFuture<PreparedEvent> readEvent(long ordinal) {
    try {
      requireReady();
      account.chargeInternalPointRead(eventReadBytes);
      return Objects.requireNonNull(
          manifests.readEvent(PreparedEvent.systemIdentity(invocation.transactionId(), ordinal), scope),
          "workset event future").thenApply(found -> {
            PreparedEvent event = found.orElseThrow(() -> new IllegalStateException("workset event is missing"));
            if (!event.transactionId().equals(invocation.transactionId())
                || event.ownerEpoch() != invocation.preparedOwnerEpoch()
                || event.baseSequence() != invocation.baseSequence() || event.baseRevision() != invocation.baseRevision()
                || event.ordinal() != ordinal || event.retainedBytes() > eventReadBytes) {
              throw new IllegalStateException("workset event belongs to a different attempt or exceeds its bound");
            }
            return event;
          });
    } catch (BackendFailure | RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private void requireReady() {
    if (scope.isClosed() || scope.isDeadlineExceeded()) {
      throw new java.util.concurrent.CompletionException(unavailable());
    }
    if (!account.isActive()) {
      throw new java.util.concurrent.CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    }
    if (failed) {
      throw new IllegalStateException("workset is not active");
    }
  }

  private synchronized void requireFrozen() {
    requireReady();
    if (!frozen || updating) throw new IllegalStateException("workset checkpoint is not frozen");
  }

  private final class MergeCursor implements AsyncCursor<StoredRecord> {
    private final int selectedBucket;
    private final KeyRange range;
    private final AsyncCursor<StoredRecord> base;
    private final AsyncCursor<PagedWorksetIndex.EventRef> overlay;
    private final ScanDirection direction;
    private final AtomicBoolean reading = new AtomicBoolean();
    private StoredRecord baseHead;
    private StoredRecord overlayHead;
    private ByteKey previousBase;
    private boolean baseEnded;
    private boolean overlayEnded;

    private MergeCursor(int selectedBucket, KeyRange range, AsyncCursor<StoredRecord> base,
        AsyncCursor<PagedWorksetIndex.EventRef> overlay, ScanDirection direction) {
      this.selectedBucket = selectedBucket;
      this.range = range;
      this.base = base;
      this.overlay = overlay;
      this.direction = direction;
    }

    @Override public CompletableFuture<Optional<StoredRecord>> next() {
      if (!reading.compareAndSet(false, true)) {
        return CompletableFuture.failedFuture(new IllegalStateException("concurrent workset merge reads"));
      }
      try {
        requireReady();
        return settleRead(fillBase().thenCompose(ignored -> fillOverlay()).thenApply(ignored -> selectHead()), reading);
      } catch (RuntimeException failure) {
        reading.set(false);
        failed = true;
        return CompletableFuture.failedFuture(failure);
      } catch (Error fatal) {
        reading.set(false);
        failed = true;
        throw fatal;
      }
    }

    private Optional<StoredRecord> selectHead() {
      requireReady();
      if (baseHead == null && overlayHead == null) return Optional.empty();
      if (baseHead == null) return consumeOverlay(false);
      if (overlayHead == null) return consumeBase();
      int order = direction.compare(baseHead.key(), overlayHead.key());
      return order < 0 ? consumeBase() : consumeOverlay(order == 0);
    }

    private Optional<StoredRecord> consumeBase() {
      StoredRecord selected = baseHead;
      baseHead = null;
      return Optional.of(selected);
    }

    private Optional<StoredRecord> consumeOverlay(boolean consumesBase) {
      StoredRecord selected = overlayHead;
      overlayHead = null;
      if (consumesBase) baseHead = null;
      return Optional.of(selected);
    }

    private CompletableFuture<Void> fillBase() {
      if (baseHead != null || baseEnded) return CompletableFuture.completedFuture(null);
      return Objects.requireNonNull(base.next(), "base cursor future").thenAccept(found -> {
        requireReady();
        Optional<StoredRecord> value = Objects.requireNonNull(found, "base cursor result");
        if (value.isEmpty()) {
          baseEnded = true;
          return;
        }
        StoredRecord record = value.orElseThrow();
        if (!range.contains(record.key()) || checkedBucket(record.key()) != selectedBucket
            || previousBase != null && direction.compare(previousBase, record.key()) >= 0
            || record.metadata().modificationRevision() > invocation.baseRevision()) {
          throw new IllegalStateException("base cursor is outside the fixed, ordered bucket view");
        }
        previousBase = record.key();
        baseHead = record;
      });
    }

    private CompletableFuture<Void> fillOverlay() {
      if (overlayHead != null || overlayEnded) return CompletableFuture.completedFuture(null);
      return overlay.next().thenCompose(found -> {
        if (found.isEmpty()) {
          overlayEnded = true;
          return CompletableFuture.completedFuture(null);
        }
        return event(found.orElseThrow()).thenAccept(value -> overlayHead = value.current());
      });
    }
  }

  private <T> CompletableFuture<T> settleRead(CompletableFuture<T> operation, AtomicBoolean reading) {
    return fenceOnFailure(operation).whenComplete((ignored, failure) -> reading.set(false)).copy();
  }

  private <T> CompletableFuture<T> fenceOnFailure(CompletableFuture<T> operation) {
    return operation.whenComplete((ignored, failure) -> { if (failure != null) failed = true; });
  }

  private int checkedBucket(ByteKey key) {
    int selected = bucket.applyAsInt(key);
    if (selected < 0 || selected >= 4096) throw new IllegalArgumentException("invalid workset bucket");
    return selected;
  }

  private BackendFailure unavailable() {
    return new BackendFailure(scope.isDeadlineExceeded() ? BackendFailure.Category.TIMEOUT
        : scope.isClosed() ? BackendFailure.Category.CANCELLED : BackendFailure.Category.UNKNOWN);
  }

  private static void requireCreated(SystemRecordStore.CreateResult result) {
    if (result != SystemRecordStore.CreateResult.CREATED && result != SystemRecordStore.CreateResult.ALREADY_PRESENT) {
      throw new IllegalStateException("immutable workset event conflicts with this attempt");
    }
  }

  private static long accountingBytes(PutCommand command, Optional<StoredRecord> base) {
    long data = Math.addExact(command.key().length(), command.valueLength());
    if (base.isPresent()) data = Math.addExact(data, base.orElseThrow().retainedBytes());
    return Math.addExact(FIXED_OPERATION_BYTES, Math.multiplyExact(COPY_ALLOWANCE, data));
  }
}
