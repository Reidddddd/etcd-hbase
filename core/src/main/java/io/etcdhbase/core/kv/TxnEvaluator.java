package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.AsyncCursor;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.ManifestRepository;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.ReadView;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.TxnReadSession;
import io.etcdhbase.core.concurrent.ContinuationExecutor;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseBindingChange;
import io.etcdhbase.core.lease.LeaseFailure;
import io.etcdhbase.core.lease.LeaseStateLookup;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.DirectRecordReader;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToIntFunction;

/** Selects transaction branches on one base, then plans their operations in order. */
public final class TxnEvaluator {
  private static final ContinuationExecutor CONTINUATION_EXECUTOR = new ContinuationExecutor();

  public record Limits(long pointBytes, long resultBytes, long workingBytes) {
    public Limits {
      if (pointBytes < 1 || resultBytes < 128 || resultBytes > ResultTree.MAX_RETAINED_BYTES
          || workingBytes < 1 || workingBytes > ResultTree.MAX_RETAINED_BYTES) {
        throw new IllegalArgumentException("invalid transaction memory limits");
      }
    }
  }

  record CacheContext(RangeCommand command, ReadView view) {}

  /** Private result remains bound to its original invocation, scope and protected read session. */
  public static final class Evaluation {
    private final ResultTree result;
    private final TxnWorkingSet working;
    private final Optional<InvocationBinding> invocation;
    private final TxnReadSession session;
    private final Optional<RangeCommand> standaloneRange;

    private Evaluation(ResultTree result, TxnWorkingSet working,
        Optional<InvocationBinding> invocation, TxnReadSession session, Optional<RangeCommand> standaloneRange) {
      this.result = Objects.requireNonNull(result, "result");
      this.working = working;
      this.invocation = Objects.requireNonNull(invocation, "invocation");
      this.session = Objects.requireNonNull(session, "session");
      this.standaloneRange = Objects.requireNonNull(standaloneRange, "standaloneRange");
    }

    public ResultTree result() { return result; }
    public long candidateCount() { return working == null ? 0 : working.candidateCount(); }
    public long eventCount() { return working == null ? 0 : working.eventCount(); }
    public List<LeaseBindingChange> leaseBindingChanges() {
      return working == null ? List.of() : working.leaseBindingChanges();
    }
    public AsyncCursor<PreparedCandidate> candidates() {
      return working == null ? AsyncCursor.fromIterator(List.<PreparedCandidate>of().iterator()) : working.candidates();
    }
    public AsyncCursor<PreparedEvent> events() {
      return working == null ? AsyncCursor.fromIterator(List.<PreparedEvent>of().iterator()) : working.events();
    }
    public boolean belongsTo(InvocationBinding expected, RequestScope scope) {
      return session.belongsTo(scope) && invocation.filter(binding -> binding.sameAttempt(expected)).isPresent();
    }
    public CompletableFuture<Void> finishRead() { return session.finish(); }

    /** Produces an externally projectable read only after all transaction pins validate. */
    public CompletableFuture<ValidatedRead> finishReadOnly() {
      return finishReadOnly(false);
    }

    /** Used only by the disabled cache path; the result is cacheable after pin validation. */
    CompletableFuture<ValidatedRead> finishReadOnlyForCache() {
      return finishReadOnly(true);
    }

    private CompletableFuture<ValidatedRead> finishReadOnly(boolean captureCacheContext) {
      if (invocation.isPresent() || candidateCount() != 0 || eventCount() != 0) {
        return CompletableFuture.failedFuture(new IllegalStateException("result is not a read-only transaction"));
      }
      Optional<CacheContext> context;
      try {
        context = captureCacheContext ? standaloneRange.map(command -> new CacheContext(command,
            session.protectedView(command.revision() > 0 ? command.revision() : session.currentRevision())))
            : Optional.empty();
      } catch (RuntimeException failure) {
        return CompletableFuture.failedFuture(failure);
      }
      return session.finish().thenApply(ignored -> new ValidatedRead(result, session, context));
    }
  }

  public static final class ValidatedRead {
    private final ResultTree result;
    private final TxnReadSession session;
    private final Optional<CacheContext> cacheContext;
    private ValidatedRead(ResultTree result, TxnReadSession session, Optional<CacheContext> cacheContext) {
      this.result = result;
      this.session = session;
      this.cacheContext = cacheContext;
    }
    public ResultTree result() { return result; }
    public long revision() { return result.root().header().revision(); }
    public boolean belongsTo(RequestScope scope) { return session.belongsTo(scope); }
    Optional<CacheContext> cacheContext() { return cacheContext; }

    static CompletableFuture<ValidatedRead> finishCached(ResultTree result, TxnReadSession session) {
      return session.finish().thenApply(ignored -> new ValidatedRead(result, session, Optional.empty()));
    }
  }

  private final TxnReadSession session;
  private final CompareReadExecutor compares;
  private final BudgetManager.Account account;
  private final RequestScope scope;
  private final Limits limits;
  private final ToIntFunction<ByteKey> bucket;
  private final SystemRecordStore worksetRecords;
  private final ManifestRepository worksetManifests;
  private final LeaseStateLookup leaseStates;
  private final AtomicBoolean started = new AtomicBoolean();

  public TxnEvaluator(TxnReadSession session, CompareReadExecutor compares,
      BudgetManager.Account account, RequestScope scope, Limits limits,
      ToIntFunction<ByteKey> bucket) {
    this(session, compares, account, scope, limits, bucket, null, null);
  }

  public TxnEvaluator(TxnReadSession session, CompareReadExecutor compares,
      BudgetManager.Account account, RequestScope scope, Limits limits,
      ToIntFunction<ByteKey> bucket, SystemRecordStore worksetRecords, ManifestRepository worksetManifests) {
    this(session, compares, account, scope, limits, bucket, worksetRecords, worksetManifests, null);
  }

  /** Supplies the same-barrier Lease lookup only for an M4-enabled mutation runtime. */
  public TxnEvaluator(TxnReadSession session, CompareReadExecutor compares,
      BudgetManager.Account account, RequestScope scope, Limits limits,
      ToIntFunction<ByteKey> bucket, SystemRecordStore worksetRecords,
      ManifestRepository worksetManifests, LeaseStateLookup leaseStates) {
    this.session = Objects.requireNonNull(session, "session");
    this.compares = Objects.requireNonNull(compares, "compares");
    this.account = Objects.requireNonNull(account, "account");
    this.scope = Objects.requireNonNull(scope, "scope");
    this.limits = Objects.requireNonNull(limits, "limits");
    this.bucket = Objects.requireNonNull(bucket, "bucket");
    if ((worksetRecords == null) != (worksetManifests == null)) {
      throw new IllegalArgumentException("workset stores must be provided together");
    }
    this.worksetRecords = worksetRecords;
    this.worksetManifests = worksetManifests;
    this.leaseStates = leaseStates;
    if (leaseStates != null && worksetRecords == null) {
      throw new IllegalArgumentException("Lease-aware transactions require durable workset stores");
    }
    if (!session.belongsTo(scope) || !account.belongsTo(scope)
        || !compares.belongsTo(session, account, scope)
        || limits.resultBytes() > account.reservation().output()
        || limits.pointBytes() > account.reservation().output() - limits.resultBytes()) {
      throw new IllegalArgumentException("transaction inputs do not share a sufficient request budget");
    }
  }

  boolean belongsTo(TxnReadSession expectedSession, BudgetManager.Account expectedAccount,
      RequestScope expectedScope) {
    return session == expectedSession && account == expectedAccount && scope == expectedScope;
  }

  long maximumResultBytes() { return limits.resultBytes(); }

  /**
   * A potentially writing request requires its original invocation. The caller closes the scope
   * after publication or rejection, and must register subsequent physical producers before use.
   * Neither this result nor any partial response may be exposed before session.finish succeeds.
   */
  public CompletableFuture<Evaluation> evaluate(TxnCommand command,
      Optional<InvocationBinding> invocation) {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(invocation, "invocation");
    if (!started.compareAndSet(false, true)) {
      return CompletableFuture.failedFuture(new IllegalStateException("transaction evaluator already used"));
    }
    if (command.mayWrite() && invocation.isEmpty()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("writing transaction requires an invocation"));
    }
    if (command.mayWrite() && worksetRecords == null) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("writing transaction requires durable workset stores"));
    }
    if (command.mayWrite() && limits.workingBytes() < TxnWorkingSet.MIN_RETAINED_BYTES) {
      return CompletableFuture.failedFuture(resourceLimit());
    }
    if (invocation.filter(binding -> binding.state() != InvocationBinding.State.PREPARING
        || binding.baseRevision() != session.currentRevision()
        || binding.baseSequence() != session.barrierSequence()
        || !session.authorizes(binding.authorizationScopeDigest())).isPresent()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invocation and transaction barrier differ"));
    }
    if (command.mayWrite() && !session.readyForMutation()) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) {
      return CompletableFuture.failedFuture(inactive());
    }
    CompletableFuture<Evaluation> operation;
    try {
      retainUntilDrain(requireMemory(BudgetManager.Memory.OUTPUT, limits.resultBytes()));
      TxnWorkingSet working = null;
      if (command.mayWrite()) {
        retainUntilDrain(requireMemory(BudgetManager.Memory.CANDIDATE, limits.workingBytes()));
        working = new TxnWorkingSet(invocation.orElseThrow(), limits.workingBytes(),
            worksetRecords, worksetManifests, scope, account, bucket, leaseStates != null);
      }
      var state = new State(working, invocation, standaloneRange(command));
      CompletableFuture<Void> started = state.working == null
          ? CompletableFuture.completedFuture(null) : state.working.begin();
      operation = started.thenComposeAsync(ignored -> select(command).thenComposeAsync(plan ->
          preflight(plan).thenComposeAsync(ready -> execute(plan, 0, new ArrayList<>(), state)
              .thenApply(responses -> finish(state, plan, responses)), CONTINUATION_EXECUTOR),
          CONTINUATION_EXECUTOR), CONTINUATION_EXECUTOR);
    } catch (BackendFailure | RuntimeException failure) {
      drained.complete(null);
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      drained.complete(null);
      throw fatal;
    }
    operation.whenComplete((ignored, failure) -> drained.complete(null));
    return operation.copy();
  }

  /** Selects every nested branch before any operation changes the private working view. */
  private CompletableFuture<BranchPlan> select(TxnCommand command) {
    return compares.evaluate(command.compares()).thenComposeAsync(matched -> selectOperations(
        matched ? command.success() : command.failure(), 0, matched, new ArrayList<>()), CONTINUATION_EXECUTOR);
  }

  private CompletableFuture<BranchPlan> selectOperations(List<TxnCommand.Operation> operations,
      int index, boolean succeeded, List<SelectedOperation> selected) {
    if (index == operations.size()) {
      return CompletableFuture.completedFuture(new BranchPlan(succeeded, List.copyOf(selected)));
    }
    TxnCommand.Operation operation = operations.get(index);
    if (operation instanceof TxnCommand.Nested nested) {
      return select(nested.command()).thenComposeAsync(child -> {
        selected.add(new SelectedNested(child));
        return selectOperations(operations, index + 1, succeeded, selected);
      }, CONTINUATION_EXECUTOR);
    }
    selected.add(new SelectedLeaf(operation));
    return CompletableFuture.completedFuture(null).thenComposeAsync(ignored ->
        selectOperations(operations, index + 1, succeeded, selected), CONTINUATION_EXECUTOR);
  }

  /** Native Txn validates selected Put conditions before validating any selected Range revision. */
  private CompletableFuture<Void> preflight(BranchPlan plan) {
    var writes = new ArrayList<PutCommand>();
    var revisions = new ArrayList<RevisionTarget>();
    collectSelected(plan, writes, revisions);
    return checkPut(writes, 0).thenComposeAsync(ignored -> {
      long firstRevision = session.compactRevision() == 0 ? -1 : session.compactRevision();
      var historical = new LinkedHashSet<Long>();
      for (RevisionTarget target : revisions) {
        long revision = target.revision();
        if (target.context() == RangeCommand.Context.STANDALONE && revision <= 0) continue;
        if (revision != 0 && revision > session.currentRevision()) {
          return CompletableFuture.failedFuture(new KvFailure(KvFailure.Reason.FUTURE_REVISION));
        }
        if (revision != 0 && revision < firstRevision) {
          return CompletableFuture.failedFuture(new KvFailure(KvFailure.Reason.COMPACTED));
        }
        if (revision > 0) historical.add(revision);
      }
      return session.pinRevisions(List.copyOf(historical));
    }, CONTINUATION_EXECUTOR);
  }

  private static void collectSelected(BranchPlan plan, List<PutCommand> writes, List<RevisionTarget> revisions) {
    for (SelectedOperation selected : plan.operations()) {
      if (selected instanceof SelectedNested nested) collectSelected(nested.plan(), writes, revisions);
      else if (((SelectedLeaf) selected).operation() instanceof TxnCommand.Put put) writes.add(put.command());
      else if (((SelectedLeaf) selected).operation() instanceof TxnCommand.Range range) {
        revisions.add(new RevisionTarget(range.command().revision(), range.command().context()));
      }
    }
  }

  private CompletableFuture<Void> checkPut(List<PutCommand> writes, int index) {
    if (index == writes.size()) return CompletableFuture.completedFuture(null);
    PutCommand command = writes.get(index);
    boolean needsPrevious = command.ignoreValue() || command.leaseIntent() == PutCommand.LeaseIntent.PRESERVE;
    if (needsPrevious) {
      long metadataBytes = Math.min(limits.pointBytes(), Math.addExact(512L, command.key().length()));
      return new DirectRecordReader(session.reader(0), account, metadataBytes)
          .readForPreparation(command.key(), session.currentRevision(), ReadProjection.METADATA)
          .thenComposeAsync(point -> {
            Optional<StoredRecord> previous;
            try (point) {
              previous = point.record().filter(record -> record.metadata().state() == StorageMetadata.State.LIVE);
              if (previous.isEmpty()) throw new KvFailure(KvFailure.Reason.KEY_NOT_FOUND);
            }
            return validatePutLease(command, previous).thenComposeAsync(ignored ->
                checkPut(writes, index + 1), CONTINUATION_EXECUTOR);
          }, CONTINUATION_EXECUTOR);
    }
    return validatePutLease(command, Optional.empty()).thenComposeAsync(ignored ->
        checkPut(writes, index + 1), CONTINUATION_EXECUTOR);
  }

  private CompletableFuture<Void> validatePutLease(PutCommand command,
      Optional<StoredRecord> previous) {
    if (leaseStates == null) {
      if (command.leaseIntent() == PutCommand.LeaseIntent.EXPLICIT
          || previous.filter(record -> record.metadata().leaseId() != 0).isPresent()) {
        return CompletableFuture.failedFuture(new KvFailure(KvFailure.Reason.LEASE_UNAVAILABLE));
      }
      return CompletableFuture.completedFuture(null);
    }
    if (command.leaseIntent() == PutCommand.LeaseIntent.EXPLICIT) {
      return leaseStates.requireAttachable(command.leaseId()).thenApply(ignored -> null);
    }
    if (command.leaseIntent() == PutCommand.LeaseIntent.PRESERVE) {
      long leaseId = previous.orElseThrow(() -> new KvFailure(KvFailure.Reason.KEY_NOT_FOUND))
          .metadata().leaseId();
      return leaseId == 0 ? CompletableFuture.completedFuture(null)
          : leaseStates.requireAttachable(leaseId).thenApply(ignored -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  private CompletableFuture<List<ResultNode>> execute(BranchPlan plan,
      int index, List<ResultNode> responses, State state) {
    try {
      requireActive();
      session.reader(0);
      if (index == plan.operations().size()) return CompletableFuture.completedFuture(List.copyOf(responses));
      SelectedOperation selected = plan.operations().get(index);
      if (selected instanceof SelectedNested nested) return executeNested(plan, index, responses, state, nested);
      TxnCommand.Operation operation = ((SelectedLeaf) selected).operation();
      if (operation instanceof TxnCommand.Unsupported) {
        return CompletableFuture.failedFuture(new KvFailure(KvFailure.Reason.UNSUPPORTED_OPERATION));
      }
      if (operation instanceof TxnCommand.Range range) {
        return appendRange(state, responses, index, range.command()).thenComposeAsync(ignored ->
            execute(plan, index + 1, responses, state), CONTINUATION_EXECUTOR);
      }
      if (operation instanceof TxnCommand.Delete delete) {
        return appendDelete(state, responses, index, delete).thenComposeAsync(ignored ->
            execute(plan, index + 1, responses, state), CONTINUATION_EXECUTOR);
      }
      return executePut(plan, index, responses, state, (TxnCommand.Put) operation);
    } catch (BackendFailure | RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletableFuture<List<ResultNode>> executeNested(BranchPlan plan, int index,
      List<ResultNode> responses, State state, SelectedNested nested) {
    return execute(nested.plan(), 0, new ArrayList<>(), state).thenComposeAsync(children -> {
      try {
        state.reserve(128);
        responses.add(new ResultNode.Txn(index, new ResultNode.Header(true, 0),
            nested.plan().succeeded(), children));
        return execute(plan, index + 1, responses, state);
      } catch (BackendFailure failure) {
        return CompletableFuture.failedFuture(failure);
      }
    }, CONTINUATION_EXECUTOR);
  }

  private CompletableFuture<List<ResultNode>> executePut(BranchPlan plan, int index,
      List<ResultNode> responses, State state, TxnCommand.Put put) {
    return point(put.command().key(), session.currentRevision()).thenComposeAsync(point -> {
      CompletableFuture<Void> write;
      try { write = appendPut(state, responses, index, put.command(), point.record()); }
      catch (RuntimeException | Error failure) {
        try { point.close(); }
        catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
        throw failure;
      }
      return write.whenComplete((ignored, failure) -> point.close()).thenComposeAsync(ignored ->
          execute(plan, index + 1, responses, state), CONTINUATION_EXECUTOR);
    }, CONTINUATION_EXECUTOR);
  }

  private CompletableFuture<io.etcdhbase.core.scan.PointResult> point(ByteKey key, long revision) {
    return new DirectRecordReader(session.reader(revision), account, limits.pointBytes())
        .readForPreparation(key, revision, ReadProjection.FULL);
  }

  private CompletableFuture<Void> appendRange(State state, List<ResultNode> responses, int ordinal,
      RangeCommand command) throws BackendFailure {
    state.reserve(128);
    var records = new ArrayList<StoredRecord>();
    long target = command.revision() > 0 ? command.revision() : session.currentRevision();
    Optional<TxnWorkingSet> overlay = command.context() == RangeCommand.Context.TRANSACTION
        && command.revision() <= 0 ? Optional.ofNullable(state.working) : Optional.empty();
    var reader = new RangeReadExecutor(session, compares, account, scope, bucket, limits.pointBytes());
    return reader.read(command, target, overlay, record -> {
      if (records.size() >= ResultNode.MAX_RECORDS_PER_NODE) {
        throw new java.util.concurrent.CompletionException(resourceLimit());
      }
      try { state.reserve(resultRecordBytes(record)); }
      catch (BackendFailure failure) { throw new java.util.concurrent.CompletionException(failure); }
      records.add(record);
    }).thenAccept(selection -> {
      long count = command.countOnly() ? selection.observedLive() : selection.included();
      responses.add(new ResultNode.Range(ordinal, new ResultNode.Header(true, state.currentRevision()),
          records, count, selection.more()));
    });
  }

  private CompletableFuture<Void> appendPut(State state, List<ResultNode> responses, int ordinal, PutCommand command,
      Optional<StoredRecord> base) {
    if (state.working == null) throw new IllegalStateException("write was not routed through an invocation");
    return state.working.put(command, base).thenAccept(result -> {
      long bytes = Math.addExact(128, command.key().length());
      if (result.previous().isPresent()) bytes = Math.addExact(bytes, resultRecordBytes(result.previous().orElseThrow()));
      try { state.reserve(bytes); }
      catch (BackendFailure failure) { throw new java.util.concurrent.CompletionException(failure); }
      responses.add(new ResultNode.Put(ordinal, new ResultNode.Header(true, result.revision()),
          result.key(), result.previous()));
    });
  }

  private CompletableFuture<Void> appendDelete(State state, List<ResultNode> responses, int ordinal,
      TxnCommand.Delete command) throws BackendFailure {
    if (state.working == null) throw new IllegalStateException("delete was not routed through an invocation");
    state.reserve(128);
    var previous = new ArrayList<StoredRecord>();
    var view = new DeleteViewCursor(session, state.working, command.range(), bucket, compares,
        account, scope, limits.pointBytes());
    CompletableFuture<Void> operation;
    try {
      operation = state.working.delete(command.range(), view, record -> {
        if (command.previousRequested()) {
          if (previous.size() >= ResultNode.MAX_RECORDS_PER_NODE) {
            throw new java.util.concurrent.CompletionException(resourceLimit());
          }
          try { state.reserve(resultRecordBytes(record)); }
          catch (BackendFailure failure) { throw new java.util.concurrent.CompletionException(failure); }
          previous.add(record);
        }
      }).thenAccept(deleted -> responses.add(new ResultNode.Delete(ordinal,
          new ResultNode.Header(true, state.currentRevision()), deleted, previous)));
    } catch (RuntimeException | Error failure) {
      try { view.close(); }
      catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
      throw failure;
    }
    return operation.whenComplete((ignored, failure) -> view.close());
  }

  private Evaluation finish(State state, BranchPlan plan, List<ResultNode> responses) {
    try {
      requireActive();
      var result = new ResultTree(new ResultNode.Txn(0,
          new ResultNode.Header(true, state.currentRevision()), plan.succeeded(), responses));
      if (result.retainedBytes() > limits.resultBytes()) throw resourceLimit();
      if (state.working != null) state.working.freeze();
      return new Evaluation(result, state.working, state.invocation, session, state.standaloneRange);
    } catch (BackendFailure failure) {
      throw new java.util.concurrent.CompletionException(failure);
    }
  }

  private BudgetManager.Lease requireMemory(BudgetManager.Memory lane, long bytes) throws BackendFailure {
    return account.tryRetain(lane, bytes).orElseThrow(TxnEvaluator::resourceLimit);
  }

  private void retainUntilDrain(BudgetManager.Lease lease) {
    var released = new CompletableFuture<Void>();
    try {
      scope.registerScopedCleanup(() -> scope.physicalProducersCompletion().whenComplete((ignored, failure) -> {
        try {
          lease.close();
          released.complete(null);
        } catch (RuntimeException | Error cleanup) {
          released.completeExceptionally(cleanup);
        }
      }), released);
    } catch (RuntimeException | Error failure) {
      try { lease.close(); }
      catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
      throw failure;
    }
  }

  private void requireActive() throws BackendFailure {
    if (scope.isDeadlineExceeded()) throw new BackendFailure(BackendFailure.Category.TIMEOUT);
    if (scope.isClosed()) throw new BackendFailure(BackendFailure.Category.CANCELLED);
    if (!account.isActive()) throw resourceLimit();
  }

  private BackendFailure inactive() {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }

  private static BackendFailure resourceLimit() {
    return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
  }

  private static long resultRecordBytes(StoredRecord record) {
    return Math.addExact(128L + record.key().length(), record.hasValue() ? record.metadata().valueLength() : 0);
  }

  static Optional<RangeCommand> standaloneRange(TxnCommand command) {
    if (!command.compares().isEmpty() || !command.failure().isEmpty() || command.success().size() != 1
        || !(command.success().getFirst() instanceof TxnCommand.Range range)
        || range.command().context() != RangeCommand.Context.STANDALONE) return Optional.empty();
    return Optional.of(range.command());
  }

  private final class State {
    private final TxnWorkingSet working;
    private final Optional<InvocationBinding> invocation;
    private final Optional<RangeCommand> standaloneRange;
    private long resultBytes = 128;

    private State(TxnWorkingSet working, Optional<InvocationBinding> invocation, Optional<RangeCommand> standaloneRange) {
      this.working = working;
      this.invocation = invocation;
      this.standaloneRange = standaloneRange;
    }
    private long currentRevision() {
      return working != null && working.hasChanges()
          ? Math.addExact(session.currentRevision(), 1) : session.currentRevision();
    }
    private void reserve(long bytes) throws BackendFailure {
      if (bytes > limits.resultBytes() - resultBytes) throw resourceLimit();
      resultBytes += bytes;
    }
  }

  private sealed interface SelectedOperation permits SelectedLeaf, SelectedNested {}
  private record SelectedLeaf(TxnCommand.Operation operation) implements SelectedOperation {}
  private record SelectedNested(BranchPlan plan) implements SelectedOperation {}
  private record BranchPlan(boolean succeeded, List<SelectedOperation> operations) {}
  private record RevisionTarget(long revision, RangeCommand.Context context) {}
}
