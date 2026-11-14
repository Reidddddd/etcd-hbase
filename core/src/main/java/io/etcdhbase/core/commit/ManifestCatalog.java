package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.concurrent.ContinuationExecutor;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.ScanDirection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/** Verifies and streams a sealed candidate/event manifest without collecting transaction data. */
public final class ManifestCatalog {
  private static final ContinuationExecutor AUXILIARY_CONTINUATIONS = new ContinuationExecutor();
  // Conservative accounting for ordinary adapter-sized values and transient copies. The HBase
  // client can receive an oversized cell before the adapter rejects it; this is not a hard wire
  // bound and must be addressed before production proof reuse is enabled.
  static final long PROOF_SYSTEM_RECORD_ENVELOPE = 64L << 20;

  /** Minimum I/O lane needed to admit one conservative system-record proof read. */
  public static long maximumProofReadPageBytes() {
    return PROOF_SYSTEM_RECORD_ENVELOPE;
  }

  /** Proof-only page allowance; callers use a separate account so a miss can fall back safely. */
  public static final class ProofReadBudget {
    private final RequestScope scope;
    private final BudgetManager.Account account;
    private final int maximumPages;
    private final long maximumBytes;
    private int pages;
    private long bytes;

    public ProofReadBudget(RequestScope scope, BudgetManager.Account account, int maximumPages, long maximumBytes) {
      this.scope = Objects.requireNonNull(scope, "scope");
      this.account = Objects.requireNonNull(account, "account");
      if (!account.belongsTo(scope) || maximumPages < 1 || maximumBytes < 1) {
        throw new IllegalArgumentException("invalid proof read budget");
      }
      this.maximumPages = maximumPages;
      this.maximumBytes = maximumBytes;
    }

    private synchronized BudgetManager.Lease retainPage(long maximumPageBytes, boolean pointRead)
        throws BackendFailure {
      if (pages >= maximumPages || maximumPageBytes > maximumBytes - bytes) {
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
      var lease = account.tryRetain(BudgetManager.Memory.IO, maximumPageBytes)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      try {
        if (pointRead) account.chargeInternalPointRead(maximumPageBytes);
      } catch (BackendFailure | RuntimeException | Error failure) {
        try { lease.close(); }
        catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
        throw failure;
      }
      pages++;
      bytes += maximumPageBytes;
      return lease;
    }

    private synchronized BudgetManager.Lease retainMeasuredPage(long maximumPageBytes)
        throws BackendFailure {
      if (maximumPageBytes < 1 || pages >= maximumPages) {
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
      var lease = account.tryRetain(BudgetManager.Memory.IO, maximumPageBytes)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      pages++;
      return lease;
    }

    private synchronized void recordMeasuredBytes(long actualBytes, long maximumPageBytes,
        boolean pointRead) throws BackendFailure {
      if (actualBytes < 0 || actualBytes > maximumPageBytes || actualBytes > maximumBytes - bytes) {
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
      if (pointRead) account.chargeInternalPointRead(actualBytes);
      bytes = Math.addExact(bytes, actualBytes);
    }

    public synchronized int pages() { return pages; }
    public synchronized long bytes() { return bytes; }
    public boolean belongsTo(RequestScope candidate) { return scope == candidate; }
    RequestScope scope() { return scope; }

    <T> CompletableFuture<T> read(long maximumBytes, Supplier<CompletableFuture<T>> work) {
      BudgetManager.Lease lease;
      try { lease = retainPage(maximumBytes, true); }
      catch (BackendFailure | RuntimeException failure) { return CompletableFuture.failedFuture(failure); }
      return new ProofPageRead<T>(lease).start(work);
    }

    /**
     * Reserves a maximum-sized I/O page but charges the actual encoded bytes returned. This keeps
     * worst-case in-flight memory separate from cumulative request work, so a sequence of small
     * metadata records does not consume the budget as though every record were maximum-sized.
     */
    <T> CompletableFuture<T> readMeasured(long maximumPageBytes,
        ToLongFunction<? super T> encodedBytes, Supplier<CompletableFuture<T>> work) {
      Objects.requireNonNull(encodedBytes, "encodedBytes");
      BudgetManager.Lease lease;
      try { lease = retainMeasuredPage(maximumPageBytes); }
      catch (BackendFailure | RuntimeException failure) { return CompletableFuture.failedFuture(failure); }
      return new ProofPageRead<T>(lease, maximumPageBytes, encodedBytes, true).start(work);
    }

    /** Reserves a bounded scan page without charging it as a point Get. */
    <T> CompletableFuture<T> scan(long maximumBytes, Supplier<CompletableFuture<T>> work) {
      BudgetManager.Lease lease;
      try { lease = retainPage(maximumBytes, false); }
      catch (BackendFailure | RuntimeException failure) { return CompletableFuture.failedFuture(failure); }
      return new ProofPageRead<T>(lease).start(work);
    }

    /** Reserves a bounded scan page and charges only the returned encoded bytes. */
    <T> CompletableFuture<T> scanMeasured(long maximumPageBytes,
        ToLongFunction<? super T> encodedBytes, Supplier<CompletableFuture<T>> work) {
      Objects.requireNonNull(encodedBytes, "encodedBytes");
      BudgetManager.Lease lease;
      try { lease = retainMeasuredPage(maximumPageBytes); }
      catch (BackendFailure | RuntimeException failure) { return CompletableFuture.failedFuture(failure); }
      return new ProofPageRead<T>(lease, maximumPageBytes, encodedBytes, false).start(work);
    }

    /** Owns one admitted page until its producer and cleanup have both settled. */
    private final class ProofPageRead<T> {
      private final BudgetManager.Lease lease;
      private final long maximumPageBytes;
      private final ToLongFunction<? super T> encodedBytes;
      private final boolean pointRead;
      private final CompletableFuture<Void> released = new CompletableFuture<>();
      private RequestScope.ScopedCleanup registration;

      private ProofPageRead(BudgetManager.Lease lease) {
        this(lease, 0, null, false);
      }

      private ProofPageRead(BudgetManager.Lease lease, long maximumPageBytes,
          ToLongFunction<? super T> encodedBytes, boolean pointRead) {
        this.lease = lease;
        this.maximumPageBytes = maximumPageBytes;
        this.encodedBytes = encodedBytes;
        this.pointRead = pointRead;
      }

      private CompletableFuture<T> start(Supplier<CompletableFuture<T>> work) {
        try {
          registration = scope.registerScopedCleanup(this::releaseAfterProducers, released);
        } catch (RuntimeException | Error failure) {
          try { lease.close(); }
          catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
          return CompletableFuture.failedFuture(failure);
        }
        if (scope.isClosed() || scope.isDeadlineExceeded()) {
          var failure = new BackendFailure(scope.isDeadlineExceeded()
              ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
          try { registration.close(); }
          catch (Exception | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
          return CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<T> operation;
        try { operation = Objects.requireNonNull(work.get(), "proof metadata read"); }
        catch (RuntimeException | Error failure) {
          try { registration.close(); }
          catch (Exception | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
          return CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<T> measured = operation;
        if (encodedBytes != null) {
          measured = operation.thenApply(value -> {
            try {
              recordMeasuredBytes(encodedBytes.applyAsLong(value), maximumPageBytes, pointRead);
            } catch (BackendFailure budgetFailure) {
              throw new java.util.concurrent.CompletionException(budgetFailure);
            }
            return value;
          });
        }
        return measured.whenComplete((ignored, failure) -> {
          try { registration.close(); }
          catch (Exception cleanup) { throw new java.util.concurrent.CompletionException(cleanup); }
        });
      }

      private void releaseAfterProducers() {
        Runnable release = this::releasePage;
        if (scope.isClosed()) scope.physicalProducersCompletion().whenComplete((ignored, failure) -> release.run());
        else release.run();
      }

      private void releasePage() {
        try {
          lease.close();
          released.complete(null);
        } catch (RuntimeException | Error failure) {
          released.completeExceptionally(failure);
          throw failure;
        }
      }
    }
  }
  /** Maximum retained candidate payload fetched for one range slice, independent of result bytes. */
  static final long MAX_RANGE_CANDIDATE_BYTES = 9L * 1024 * 1024;

  public record Summary(long count, long payloadBytes) {
    public Summary {
      if (count < 0 || payloadBytes < 0) throw new IllegalArgumentException("negative manifest summary");
    }
  }

  /** Bounded, authenticated candidate slice for one physical salt bucket and logical key range. */
  public static final class CandidateSlice implements AutoCloseable {
    private volatile List<PreparedCandidate> candidates;
    private final boolean exhausted;
    private final int directoryPages;
    private final int leafPages;
    private final long payloadBytes;
    private final List<BudgetManager.Lease> leases;
    private final AtomicBoolean closed = new AtomicBoolean();

    private CandidateSlice(List<PreparedCandidate> candidates, boolean exhausted,
        int directoryPages, int leafPages, long payloadBytes, List<BudgetManager.Lease> leases) {
      this.candidates = List.copyOf(candidates);
      if (directoryPages < 0 || leafPages < 0 || payloadBytes < 0) {
        throw new IllegalArgumentException("negative candidate slice accounting");
      }
      this.exhausted = exhausted;
      this.directoryPages = directoryPages;
      this.leafPages = leafPages;
      this.payloadBytes = payloadBytes;
      this.leases = List.copyOf(leases);
    }

    static CandidateSlice empty() {
      return new CandidateSlice(List.of(), true, 0, 0, 0, List.of());
    }

    public List<PreparedCandidate> candidates() { return candidates; }
    public boolean exhausted() { return exhausted; }
    public int directoryPages() { return directoryPages; }
    public int leafPages() { return leafPages; }
    public long payloadBytes() { return payloadBytes; }

    /**
     * Releases candidate-buffer ownership after the consumer has projected or transferred it.
     * Every lease is attempted even when cleanup throws; the first failure remains primary and
     * later cleanup failures are suppressed before the slice owner observes the failure.
     */
    @Override public void close() {
      if (!closed.compareAndSet(false, true)) return;
      candidates = List.of();
      Throwable failure = null;
      for (BudgetManager.Lease lease : leases) {
        try {
          lease.close();
        } catch (RuntimeException | Error cleanup) {
          // Capture fatal cleanup failures only long enough to settle every sibling lease; the
          // first failure is rethrown after the loop and later failures remain suppressed.
          if (failure == null) failure = cleanup;
          else if (failure != cleanup) failure.addSuppressed(cleanup);
        }
      }
      if (failure instanceof Error fatal) throw fatal;
      if (failure instanceof RuntimeException runtime) throw runtime;
    }
  }

  private static final int MAX_CANDIDATE_PAGES = Math.toIntExact(ManifestRoot.MAX_CANDIDATES);
  private static final int MAX_EVENT_PAGES = Math.toIntExact(ManifestRoot.MAX_EVENTS);
  private static final int MAX_CANDIDATE_DIRECTORY_PAGES =
      MAX_CANDIDATE_PAGES * ManifestDirectoryPage.MAX_DEPTH;
  private static final int MAX_EVENT_DIRECTORY_PAGES = MAX_EVENT_PAGES * EventDirectoryPage.MAX_DEPTH;

  private final ManifestRepository repository;

  public ManifestCatalog(ManifestRepository repository) {
    this.repository = Objects.requireNonNull(repository, "repository");
  }

  public CompletableFuture<Optional<ManifestRoot>> readRoot(ByteKey identity, RequestScope scope) {
    return repository.readRoot(identity, scope).thenApply(root -> root.map(value -> {
      if (!value.systemIdentity().equals(identity)) {
        throw new IllegalStateException("manifest root identity differs from its durable key");
      }
      return value;
    }));
  }

  public CompletableFuture<Summary> forEachCandidate(ManifestRoot root, RequestScope scope,
      Function<PreparedCandidate, CompletableFuture<Void>> consumer) {
    Objects.requireNonNull(root, "root");
    Objects.requireNonNull(consumer, "consumer");
    var cursor = new CandidateCursor(root, scope, consumer);
    if (root.candidateDirectory().isEmpty()) {
      return root.candidateCount() == 0 && root.totalValueBytes() == 0
          ? CompletableFuture.completedFuture(new Summary(0, 0))
          : CompletableFuture.failedFuture(new IllegalStateException("manifest candidate directory is missing"));
    }
    return cursor.visit(root.candidateDirectory().orElseThrow())
        .thenApply(ignored -> cursor.summary());
  }

  public CompletableFuture<Summary> forEachEvent(ManifestRoot root, RequestScope scope,
      Function<PreparedEvent, CompletableFuture<Void>> consumer) {
    Objects.requireNonNull(root, "root");
    Objects.requireNonNull(consumer, "consumer");
    var cursor = new EventCursor(root, scope, consumer);
    if (root.eventDirectory().isEmpty()) {
      return root.eventCount() == 0 && root.totalEventBytes() == 0
          ? CompletableFuture.completedFuture(new Summary(0, 0))
          : CompletableFuture.failedFuture(new IllegalStateException("manifest event directory is missing"));
    }
    return cursor.visit(root.eventDirectory().orElseThrow()).thenApply(ignored -> cursor.summary());
  }

  /** Streams and authenticates every immutable auxiliary update covered by a sealed root. */
  public CompletableFuture<Summary> forEachAuxiliary(ManifestRoot root, RequestScope scope,
      Function<PreparedAuxiliaryRecord, CompletableFuture<Void>> consumer) {
    return forEachAuxiliary(root, scope, null, consumer);
  }

  /** Charges each bounded system-record read against the caller's metadata budget. */
  public CompletableFuture<Summary> forEachAuxiliary(ManifestRoot root, RequestScope scope,
      ProofReadBudget budget, Function<PreparedAuxiliaryRecord, CompletableFuture<Void>> consumer) {
    Objects.requireNonNull(root, "root");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(consumer, "consumer");
    if (budget != null && !budget.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("auxiliary budget belongs to another scope"));
    }
    if (root.auxiliaryReference().isEmpty()) {
      return CompletableFuture.completedFuture(new Summary(0, 0));
    }
    AuxiliaryReference reference = root.auxiliaryReference().orElseThrow();
    return readAuxiliary(budget,
        () -> repository.readAuxiliaryDirectoryWithSize(reference.identity(), scope))
        .thenCompose(stored -> {
      requireAuxiliaryActive(scope);
      AuxiliaryDirectory directory = stored.orElseThrow(
          () -> new IllegalStateException("sealed auxiliary directory is missing"));
      if (!directory.transactionId().equals(root.transactionId())
          || !directory.systemIdentity().equals(reference.identity())
          || !directory.reference().equals(reference)) {
        return CompletableFuture.failedFuture(
            new IllegalStateException("auxiliary directory differs from its sealed root"));
      }
      var cursor = new AuxiliaryCursor(root, directory, scope, budget, consumer);
      return cursor.visitPage(0).thenApply(ignored -> cursor.summary(reference));
    });
  }

  private final class AuxiliaryCursor {
    private final ManifestRoot root;
    private final AuxiliaryDirectory directory;
    private final RequestScope scope;
    private final ProofReadBudget budget;
    private final Function<PreparedAuxiliaryRecord, CompletableFuture<Void>> consumer;
    private final java.util.Set<AuxiliaryTarget> targets = new java.util.HashSet<>();
    private long count;
    private long bytes;
    private long nextOrdinal;

    private AuxiliaryCursor(ManifestRoot root, AuxiliaryDirectory directory, RequestScope scope,
        ProofReadBudget budget, Function<PreparedAuxiliaryRecord, CompletableFuture<Void>> consumer) {
      this.root = root;
      this.directory = directory;
      this.scope = scope;
      this.budget = budget;
      this.consumer = consumer;
    }

    private CompletableFuture<Void> visitPage(int pageIndex) {
      requireAuxiliaryActive(scope);
      if (pageIndex == directory.pages().size()) return CompletableFuture.completedFuture(null);
      AuxiliaryPageReference reference = directory.pages().get(pageIndex);
      if (reference.pageIndex() != pageIndex || reference.firstOrdinal() != nextOrdinal) {
        return invalidAuxiliary("auxiliary page range is not contiguous");
      }
      return readAuxiliary(budget,
          () -> repository.readAuxiliaryPageWithSize(reference.identity(), scope))
          .thenCompose(stored -> {
        requireAuxiliaryActive(scope);
        AuxiliaryPage page = stored.orElseThrow(
            () -> new IllegalStateException("sealed auxiliary page is missing"));
        if (!page.transactionId().equals(root.transactionId())
            || !page.reference().equals(reference)) {
          return invalidAuxiliary("auxiliary page differs from its directory reference");
        }
        return visitRecord(page, 0).thenComposeAsync(ignored -> {
          if (nextOrdinal != reference.lastOrdinal() + 1) {
            return invalidAuxiliary("auxiliary page did not cover its declared ordinal range");
          }
          return visitPage(pageIndex + 1);
        }, AUXILIARY_CONTINUATIONS);
      });
    }

    private CompletableFuture<Void> visitRecord(AuxiliaryPage page, int recordIndex) {
      requireAuxiliaryActive(scope);
      if (recordIndex == page.records().size()) return CompletableFuture.completedFuture(null);
      AuxiliaryRecordReference reference = page.records().get(recordIndex);
      if (reference.ordinal() != nextOrdinal) {
        return invalidAuxiliary("auxiliary records are not contiguous");
      }
      return readAuxiliary(budget,
          () -> repository.readAuxiliaryRecordWithSize(reference.identity(), scope))
          .thenCompose(stored -> {
        requireAuxiliaryActive(scope);
        PreparedAuxiliaryRecord record = stored.orElseThrow(
            () -> new IllegalStateException("sealed auxiliary record is missing"));
        if (!reference.matches(record) || !record.transactionId().equals(root.transactionId())
            || record.ownerEpoch() != root.ownerEpoch()
            || record.baseSequence() != root.baseSequence() || record.ordinal() != nextOrdinal) {
          return invalidAuxiliary("auxiliary record differs from its authenticated reference");
        }
        if (!targets.add(new AuxiliaryTarget(record.targetType(), record.targetIdentity()))) {
          return invalidAuxiliary("duplicate auxiliary materialization target");
        }
        count = Math.addExact(count, 1);
        bytes = Math.addExact(bytes, record.encodedBytes());
        nextOrdinal = Math.addExact(nextOrdinal, 1);
        return Objects.requireNonNull(consumer.apply(record), "auxiliary consumer future")
            .thenComposeAsync(ignored -> visitRecord(page, recordIndex + 1), AUXILIARY_CONTINUATIONS);
      });
    }

    private Summary summary(AuxiliaryReference reference) {
      if (count != reference.itemCount() || bytes != reference.totalBytes()
          || directory.pages().size() != reference.pageCount()) {
        throw new IllegalStateException("auxiliary root aggregates differ from its records");
      }
      return new Summary(count, bytes);
    }
  }

  private record AuxiliaryTarget(SystemRecordType type, ByteKey identity) {}

  private static <T> CompletableFuture<Optional<T>> readAuxiliary(ProofReadBudget budget,
      Supplier<CompletableFuture<ManifestRepository.ReadValue<T>>> work) {
    CompletableFuture<ManifestRepository.ReadValue<T>> read = budget == null
        ? work.get()
        : budget.readMeasured(PROOF_SYSTEM_RECORD_ENVELOPE,
            ManifestRepository.ReadValue::encodedBytes, work);
    return read.thenApply(ManifestRepository.ReadValue::value);
  }

  private static void requireAuxiliaryActive(RequestScope scope) {
    if (scope.isDeadlineExceeded() || scope.isClosed()) {
      throw new java.util.concurrent.CompletionException(new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
    }
  }

  private static <T> CompletableFuture<T> invalidAuxiliary(String message) {
    return CompletableFuture.failedFuture(new IllegalStateException(message));
  }

  /**
   * Reads only candidate leaves that can intersect a bucket/range. The caller resumes after the
   * last returned key; a full slice reports non-exhaustion conservatively until the next read.
   * Candidate memory is reserved before each immutable value is fetched. The request stop signal
   * prevents more reads after its page operation is closed.
   */
  public CompletableFuture<CandidateSlice> candidatesInRange(ManifestRoot root, int bucket,
      KeyRange range, Optional<ByteKey> afterExclusive, int maximumCandidates,
      long maximumCandidateBytes, RequestScope scope, BooleanSupplier cancelled,
      BudgetManager.Account account) {
    return candidatesInRange(root, bucket, range, afterExclusive, maximumCandidates, maximumCandidateBytes,
        scope, cancelled, account, ScanDirection.ASCENDING);
  }

  public CompletableFuture<CandidateSlice> candidatesInRange(ManifestRoot root, int bucket,
      KeyRange range, Optional<ByteKey> afterExclusive, int maximumCandidates,
      long maximumCandidateBytes, RequestScope scope, BooleanSupplier cancelled,
      BudgetManager.Account account, ScanDirection direction) {
    return candidatesInRange(root, bucket, range, afterExclusive, maximumCandidates,
        maximumCandidateBytes, scope, cancelled, account, direction, null);
  }

  /** Reads a bounded overlap candidate with separate, precharged metadata-page ownership. */
  public CompletableFuture<CandidateSlice> candidatesForProof(ManifestRoot root, int bucket,
      KeyRange range, long maximumCandidateBytes, ProofReadBudget proof) {
    Objects.requireNonNull(proof, "proof");
    return candidatesInRange(root, bucket, range, Optional.empty(), 1, maximumCandidateBytes,
        proof.scope, () -> false, proof.account, ScanDirection.ASCENDING, proof);
  }

  private CompletableFuture<CandidateSlice> candidatesInRange(ManifestRoot root, int bucket,
      KeyRange range, Optional<ByteKey> afterExclusive, int maximumCandidates,
      long maximumCandidateBytes, RequestScope scope, BooleanSupplier cancelled,
      BudgetManager.Account account, ScanDirection direction, ProofReadBudget proof) {
    Objects.requireNonNull(root, "root");
    Objects.requireNonNull(range, "range");
    Objects.requireNonNull(afterExclusive, "afterExclusive");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(cancelled, "cancelled");
    Objects.requireNonNull(direction, "direction");
    if (account != null && !account.belongsTo(scope)) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("candidate budget belongs to another request scope"));
    }
    if (bucket < 0 || maximumCandidates <= 0 || maximumCandidates > 1025
        || maximumCandidateBytes <= 0) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid candidate slice bounds"));
    }
    if (range.isEmpty() || root.candidateDirectory().isEmpty()) {
      return CompletableFuture.completedFuture(CandidateSlice.empty());
    }
    var callerCancelled = new AtomicBoolean();
    var cursor = new CandidateRangeCursor(root, bucket, range, afterExclusive,
        maximumCandidates, maximumCandidateBytes, scope,
        () -> cancelled.getAsBoolean() || callerCancelled.get(), account, direction, proof);
    var result = new CompletableFuture<CandidateSlice>();
    CompletableFuture<Void> traversal;
    try {
      traversal = cursor.visit(root.candidateDirectory().orElseThrow());
    } catch (RuntimeException | Error failure) {
      // Preserve the future-based API while draining cursor ownership; callers observe the same
      // fatal Error through the exceptional result after deferred producer cleanup is registered.
      result.completeExceptionally(cursor.releaseAfterFailure(failure));
      return result;
    }
    traversal.whenComplete((ignored, failure) -> {
      if (failure != null) {
        result.completeExceptionally(
            cursor.releaseAfterFailure(CompletionFailures.unwrapCompletion(failure)));
      } else {
        CandidateSlice slice = cursor.slice();
        if (!result.complete(slice)) slice.close();
      }
    });
    result.whenComplete((slice, failure) -> {
      if (result.isCancelled()) callerCancelled.set(true);
    });
    return result;
  }

  private boolean candidateMatchesReference(ManifestRoot root,
      ManifestCandidateReference reference, PreparedCandidate candidate) {
    return candidate.transactionId().equals(root.transactionId())
        && candidate.ownerEpoch() == root.ownerEpoch()
        && candidate.baseSequence() == root.baseSequence()
        && candidate.baseRevision() == root.baseRevision()
        && candidate.systemIdentity().equals(reference.candidateIdentity())
        && repository.candidateBucket(candidate) == reference.bucket()
        && candidate.record().key().equals(reference.logicalKey())
        && candidate.contentDigest().equals(reference.candidateDigest())
        && candidate.valueDigest().equals(reference.valueDigest())
        && candidate.record().metadata().valueLength() == reference.valueLength()
        && (candidate.record().metadata().state() == StorageMetadata.State.TOMBSTONE)
            == reference.tombstone();
  }

  /**
   * Owns candidate leases until {@link #slice()} transfers them. Read, validation, and cleanup
   * failures remain exceptional outcomes of {@code candidatesInRange}; if a closed scope still
   * has physical producers, lease release waits for their completion and any later close failure
   * is suppressed on the same terminal throwable. Scope cleanup separately tracks producer drain.
   */
  private final class CandidateRangeCursor {
    private final ManifestRoot root;
    private final int bucket;
    private final KeyRange range;
    private final Optional<ByteKey> after;
    private final int maximumCandidates;
    private final long maximumCandidateBytes;
    private final RequestScope scope;
    private final BooleanSupplier cancelled;
    private final BudgetManager.Account account;
    private final ScanDirection direction;
    private final ProofReadBudget proof;
    private final List<PreparedCandidate> candidates = new java.util.ArrayList<>();
    private final List<BudgetManager.Lease> leases = new java.util.ArrayList<>();
    private int directoryPages;
    private int leafPages;
    private long payloadBytes;
    private ByteKey lastKey;
    private boolean exhausted = true;

    private CandidateRangeCursor(ManifestRoot root, int bucket, KeyRange range,
        Optional<ByteKey> after, int maximumCandidates, long maximumCandidateBytes,
        RequestScope scope, BooleanSupplier cancelled, BudgetManager.Account account,
        ScanDirection direction, ProofReadBudget proof) {
      this.root = root;
      this.bucket = bucket;
      this.range = range;
      this.after = after;
      this.maximumCandidates = maximumCandidates;
      this.maximumCandidateBytes = maximumCandidateBytes;
      this.scope = scope;
      this.cancelled = cancelled;
      this.account = account;
      this.direction = direction;
      this.proof = proof;
    }

    private CompletableFuture<Void> visit(ManifestChildReference reference) {
      if (stopped()) return CompletableFuture.completedFuture(null);
      if (cancelled.getAsBoolean()) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.CANCELLED));
      }
      if (scope.isClosed() || scope.isDeadlineExceeded()) {
        return CompletableFuture.failedFuture(new BackendFailure(scope.isDeadlineExceeded()
            ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
      }
      if (!mayIntersect(reference)) return CompletableFuture.completedFuture(null);
      if (reference.level() == 0) return visitLeaf(reference);
      if (++directoryPages > MAX_CANDIDATE_DIRECTORY_PAGES) {
        return invalid("manifest range traversal exceeds its directory budget");
      }
      return withProofPage(PROOF_SYSTEM_RECORD_ENVELOPE, () -> repository.readDirectoryPage(reference.identity(), scope).thenCompose(stored -> {
        ManifestDirectoryPage page = stored.orElseThrow(() -> missing("manifest directory page"));
        if (!page.transactionId().equals(root.transactionId()) || !page.reference().equals(reference)) {
          return invalid("manifest directory page differs from its authenticated range reference");
        }
        return visitChildren(page.children(), firstIndex(page.children().size()));
      }));
    }

    private CompletableFuture<Void> visitChildren(List<ManifestChildReference> children, int index) {
      if (index < 0 || index >= children.size() || stopped()) return CompletableFuture.completedFuture(null);
      return visit(children.get(index)).thenCompose(ignored -> visitChildren(children, nextIndex(index)));
    }

    private CompletableFuture<Void> visitLeaf(ManifestChildReference reference) {
      if (++leafPages > MAX_CANDIDATE_PAGES) {
        return invalid("manifest range traversal exceeds its leaf budget");
      }
      return withProofPage(PROOF_SYSTEM_RECORD_ENVELOPE, () -> repository.readPage(reference.identity(), scope).thenCompose(stored -> {
        ManifestPage page = stored.orElseThrow(() -> missing("manifest leaf page"));
        if (!page.transactionId().equals(root.transactionId()) || !page.reference().equals(reference)
            || page.bucket() != bucket) {
          return invalid("manifest leaf page differs from its authenticated bucket range");
        }
        return visitCandidates(page.candidates(), firstIndex(page.candidates().size()));
      }));
    }

    private CompletableFuture<Void> withProofPage(long maximumBytes, Supplier<CompletableFuture<Void>> work) {
      return proof == null ? work.get() : proof.read(maximumBytes, work);
    }

    private CompletableFuture<Void> visitCandidates(
        List<ManifestCandidateReference> references, int index) {
      if (index < 0 || index >= references.size() || stopped()) return CompletableFuture.completedFuture(null);
      if (cancelled.getAsBoolean() || scope.isClosed() || scope.isDeadlineExceeded()) {
        return CompletableFuture.failedFuture(stopFailure());
      }
      ManifestCandidateReference reference = references.get(index);
      return visitCandidateReference(references, index, reference);
    }

    private CompletableFuture<Void> visitCandidateReference(
        List<ManifestCandidateReference> references, int index,
        ManifestCandidateReference reference) {
      if (reference.bucket() != bucket) {
        return invalid("manifest leaf contains a candidate from another bucket");
      }
      ByteKey key = reference.logicalKey();
      if (!range.contains(key) || after.filter(previous -> direction.compare(key, previous) <= 0).isPresent()) {
        return visitCandidates(references, nextIndex(index));
      }
      if (lastKey != null && direction.compare(lastKey, key) >= 0) {
        return invalid("manifest range candidates are duplicated or unordered");
      }
      return proof == null ? readCandidate(references, index, reference, key)
          : proof.read(PROOF_SYSTEM_RECORD_ENVELOPE, () -> readCandidate(references, index, reference, key));
    }

    private CompletableFuture<Void> readCandidate(List<ManifestCandidateReference> references,
        int index, ManifestCandidateReference reference, ByteKey key) {
      long retained = reference.retainedBytes();
      if (retained > maximumCandidateBytes - payloadBytes) {
        return stopAtCandidateLimit();
      }
      return reserveCandidateBeforeRead(references, index, reference, key, retained);
    }

    private CompletableFuture<Void> reserveCandidateBeforeRead(
        List<ManifestCandidateReference> references, int index,
        ManifestCandidateReference reference, ByteKey key, long retained) {
      if (account == null) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      BudgetManager.Lease lease;
      try {
        lease = account.tryRetain(BudgetManager.Memory.CANDIDATE, retained).orElse(null);
      } catch (BackendFailure | RuntimeException failure) {
        return CompletableFuture.failedFuture(failure);
      }
      if (lease == null) {
        return stopAtCandidateLimit();
      }
      try {
        account.chargeCandidateRead(retained);
      } catch (BackendFailure | RuntimeException | Error failure) {
        // Once admitted, release this lease before exposing any accounting failure through the
        // traversal future, including a fatal Error.
        return failedCandidate(lease, failure);
      }
      return fetchCandidate(references, index, reference, key, retained, lease);
    }

    private CompletableFuture<Void> fetchCandidate(List<ManifestCandidateReference> references,
        int index, ManifestCandidateReference reference, ByteKey key, long retained,
        BudgetManager.Lease lease) {
      CompletableFuture<Optional<PreparedCandidate>> read;
      try {
        read = Objects.requireNonNull(repository.readCandidate(reference.candidateIdentity(), scope),
            "candidate read future");
      } catch (RuntimeException | Error failure) {
        // A synchronous producer failure has no future to own cleanup; return it through the
        // traversal result after releasing this read's lease.
        return failedCandidate(lease, failure);
      }
      return read.handle((stored, failure) -> {
        if (failure != null) {
          return failedCandidate(lease, CompletionFailures.unwrapCompletion(failure));
        }
        return acceptCandidate(references, index, reference, key, retained, lease, stored);
      }).thenCompose(stage -> stage);
    }

    private CompletableFuture<Void> acceptCandidate(List<ManifestCandidateReference> references,
        int index, ManifestCandidateReference reference, ByteKey key, long retained,
        BudgetManager.Lease lease, Optional<PreparedCandidate> stored) {
      try {
        if (cancelled.getAsBoolean() || scope.isClosed() || scope.isDeadlineExceeded()) {
          return failedCandidate(lease, stopFailure());
        }
        PreparedCandidate candidate = stored.orElseThrow(() -> missing("private candidate"));
        if (!candidateMatchesReference(root, reference, candidate)) {
          return failedCandidate(lease, new IllegalStateException(
              "private candidate differs from its authenticated range reference"));
        }
        candidates.add(candidate);
        leases.add(lease);
        payloadBytes = Math.addExact(payloadBytes, retained);
        lastKey = key;
        if (candidates.size() == maximumCandidates) {
          exhausted = false;
          return CompletableFuture.completedFuture(null);
        }
        return visitCandidates(references, nextIndex(index));
      } catch (RuntimeException | Error failure) {
        // Keep fatal failures observable through the traversal future, but settle this read's
        // ownership first; failedCandidate defers release if a physical producer is still active.
        return failedCandidate(lease, failure);
      }
    }

    private CompletableFuture<Void> stopAtCandidateLimit() {
      if (candidates.isEmpty()) {
        return CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      exhausted = false;
      return CompletableFuture.completedFuture(null);
    }

    private BackendFailure stopFailure() {
      return new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
    }

    private CompletableFuture<Void> failedCandidate(BudgetManager.Lease lease, Throwable failure) {
      releaseCandidateLease(lease, failure);
      return CompletableFuture.failedFuture(failure);
    }

    private void releaseCandidateLease(BudgetManager.Lease lease, Throwable failure) {
      if (scope.isClosed()) {
        // Cancellation can complete the request result before HBase has stopped its physical
        // producer. Keep the candidate-buffer charge until every producer owned by this scope
        // has actually stopped.
        scope.physicalProducersCompletion().whenComplete((ignored, producerFailure) ->
            closeCandidateLease(lease, failure));
        return;
      }
      closeCandidateLease(lease, failure);
    }

    private void closeCandidateLease(BudgetManager.Lease lease, Throwable failure) {
      try {
        lease.close();
      } catch (RuntimeException | Error cleanup) {
        // Keep the original candidate failure as the future's cause; deferred cleanup failures
        // remain attached to that same throwable after physical-producer drain.
        if (cleanup != failure) failure.addSuppressed(cleanup);
      }
    }

    private boolean mayIntersect(ManifestChildReference reference) {
      if (bucket < reference.firstBucket() || bucket > reference.lastBucket()) return false;
      if (reference.firstBucket() != reference.lastBucket()) return true;
      if (after.filter(previous -> direction == ScanDirection.ASCENDING
          ? reference.lastKey().compareTo(previous) <= 0 : reference.firstKey().compareTo(previous) >= 0).isPresent()) {
        return false;
      }
      if (range.kind() == KeyRange.Kind.POINT) {
        ByteKey key = range.lowerInclusive().orElseThrow();
        return reference.firstKey().compareTo(key) <= 0 && reference.lastKey().compareTo(key) >= 0;
      }
      return range.upperExclusive().map(upper -> reference.firstKey().compareTo(upper) < 0).orElse(true)
          && range.lowerInclusive().map(lower -> reference.lastKey().compareTo(lower) >= 0).orElse(true);
    }

    private boolean stopped() {
      return !exhausted;
    }

    private int firstIndex(int size) {
      return direction == ScanDirection.ASCENDING ? 0 : size - 1;
    }

    private int nextIndex(int index) {
      return direction == ScanDirection.ASCENDING ? index + 1 : index - 1;
    }

    private CandidateSlice slice() {
      List<PreparedCandidate> transferredCandidates = List.copyOf(candidates);
      List<BudgetManager.Lease> transferredLeases = List.copyOf(leases);
      candidates.clear();
      leases.clear();
      return new CandidateSlice(transferredCandidates, exhausted, directoryPages, leafPages,
          payloadBytes, transferredLeases);
    }

    private Throwable releaseAfterFailure(Throwable failure) {
      candidates.clear();
      for (BudgetManager.Lease lease : List.copyOf(leases)) {
        releaseCandidateLease(lease, failure);
      }
      leases.clear();
      return failure;
    }
  }

  private final class CandidateCursor {
    private final ManifestRoot root;
    private final RequestScope scope;
    private final Function<PreparedCandidate, CompletableFuture<Void>> consumer;
    private long count;
    private long valueBytes;
    private ByteKey lastKey;
    private ByteKey firstKey;
    private int lastBucket = -1;
    private int firstBucket = -1;
    private int leafPages;
    private int directoryPages;

    private CandidateCursor(ManifestRoot root, RequestScope scope,
        Function<PreparedCandidate, CompletableFuture<Void>> consumer) {
      this.root = root;
      this.scope = Objects.requireNonNull(scope, "scope");
      this.consumer = consumer;
    }

    private CompletableFuture<Void> visit(ManifestChildReference reference) {
      if (reference.level() == 0) return visitLeaf(reference);
      if (++directoryPages > MAX_CANDIDATE_DIRECTORY_PAGES) {
        return invalid("manifest directory exceeds its traversal budget");
      }
      return repository.readDirectoryPage(reference.identity(), scope).thenCompose(page -> {
        ManifestDirectoryPage directory = page.orElseThrow(() -> missing("manifest directory page"));
        if (!directory.transactionId().equals(root.transactionId())
            || !directory.reference().equals(reference)) {
          return invalid("manifest directory page does not match its sealed reference");
        }
        return visitChildren(directory.children(), 0);
      });
    }

    private CompletableFuture<Void> visitLeaf(ManifestChildReference reference) {
      if (++leafPages > MAX_CANDIDATE_PAGES) {
        return invalid("manifest leaf pages exceed their traversal budget");
      }
      return repository.readPage(reference.identity(), scope).thenCompose(page -> {
        ManifestPage leaf = page.orElseThrow(() -> missing("manifest leaf page"));
        if (!leaf.transactionId().equals(root.transactionId()) || !leaf.reference().equals(reference)) {
          return invalid("manifest leaf page does not match its sealed reference");
        }
        return visitCandidates(leaf.candidates(), 0);
      });
    }

    private CompletableFuture<Void> visitChildren(List<ManifestChildReference> children, int index) {
      if (index == children.size()) return CompletableFuture.completedFuture(null);
      return visit(children.get(index)).thenCompose(ignored -> visitChildren(children, index + 1));
    }

    private CompletableFuture<Void> visitCandidates(List<ManifestCandidateReference> refs, int index) {
      if (index == refs.size()) return CompletableFuture.completedFuture(null);
      ManifestCandidateReference reference = refs.get(index);
      if (lastBucket > reference.bucket() || lastBucket == reference.bucket() && lastKey != null
          && lastKey.compareTo(reference.logicalKey()) >= 0) {
        return invalid("manifest candidate key order overlaps across pages");
      }
      return repository.readCandidate(reference.candidateIdentity(), scope).thenCompose(value -> {
        PreparedCandidate candidate = value.orElseThrow(() -> missing("private candidate"));
        if (!candidateMatchesReference(root, reference, candidate)) {
          return invalid("private candidate differs from its sealed manifest leaf");
        }
        try {
          count = Math.addExact(count, 1);
          valueBytes = Math.addExact(valueBytes, reference.valueLength());
          if (count > root.candidateCount() || valueBytes > root.totalValueBytes()) {
            return invalid("candidate traversal exceeds sealed root aggregates");
          }
          lastKey = reference.logicalKey();
          lastBucket = reference.bucket();
          if (firstKey == null) {
            firstBucket = reference.bucket();
            firstKey = reference.logicalKey();
          }
        } catch (ArithmeticException overflow) {
          return invalid("candidate traversal aggregate overflowed");
        }
        return consumer.apply(candidate).thenCompose(ignored -> visitCandidates(refs, index + 1));
      });
    }

    private Summary summary() {
      if (count != root.candidateCount() || valueBytes != root.totalValueBytes()
          || leafPages != root.candidateDirectory().orElseThrow().leafPageCount()
          || directoryPages > MAX_CANDIDATE_DIRECTORY_PAGES
          || firstBucket != root.candidateDirectory().orElseThrow().firstBucket()
          || lastBucket != root.candidateDirectory().orElseThrow().lastBucket()
          || !firstKey.equals(root.candidateDirectory().orElseThrow().firstKey())
          || !lastKey.equals(root.candidateDirectory().orElseThrow().lastKey())) {
        throw new IllegalStateException("manifest traversal does not cover sealed candidate aggregates");
      }
      return new Summary(count, valueBytes);
    }
  }

  private final class EventCursor {
    private final ManifestRoot root;
    private final RequestScope scope;
    private final Function<PreparedEvent, CompletableFuture<Void>> consumer;
    private long count;
    private long bytes;
    private long nextOrdinal;
    private int leafPages;
    private int directoryPages;

    private EventCursor(ManifestRoot root, RequestScope scope,
        Function<PreparedEvent, CompletableFuture<Void>> consumer) {
      this.root = root;
      this.scope = Objects.requireNonNull(scope, "scope");
      this.consumer = consumer;
    }

    private CompletableFuture<Void> visit(EventReference reference) {
      if (reference.level() == 0) return visitLeaf(reference);
      if (++directoryPages > MAX_EVENT_DIRECTORY_PAGES) {
        return invalid("event directory exceeds its traversal budget");
      }
      return repository.readEventDirectoryPage(reference.identity(), scope).thenCompose(page -> {
        EventDirectoryPage directory = page.orElseThrow(() -> missing("event directory page"));
        if (!directory.transactionId().equals(root.transactionId())
            || !directory.reference().equals(reference)) {
          return invalid("event directory page does not match its sealed reference");
        }
        return visitChildren(directory.children(), 0);
      });
    }

    private CompletableFuture<Void> visitLeaf(EventReference reference) {
      if (++leafPages > MAX_EVENT_PAGES) {
        return invalid("event leaf pages exceed their traversal budget");
      }
      return repository.readEventPage(reference.identity(), scope).thenCompose(page -> {
        EventPage leaf = page.orElseThrow(() -> missing("event leaf page"));
        if (!leaf.transactionId().equals(root.transactionId()) || !leaf.reference().equals(reference)) {
          return invalid("event leaf page does not match its sealed reference");
        }
        return visitEvents(leaf.events(), 0);
      });
    }

    private CompletableFuture<Void> visitChildren(List<EventReference> children, int index) {
      if (index == children.size()) return CompletableFuture.completedFuture(null);
      return visit(children.get(index)).thenCompose(ignored -> visitChildren(children, index + 1));
    }

    private CompletableFuture<Void> visitEvents(List<PreparedEventReference> refs, int index) {
      if (index == refs.size()) return CompletableFuture.completedFuture(null);
      PreparedEventReference reference = refs.get(index);
      if (reference.ordinal() != nextOrdinal) return invalid("event manifest ordinals contain a gap");
      return repository.readEvent(reference.identity(), scope).thenCompose(value -> {
        PreparedEvent event = value.orElseThrow(() -> missing("prepared event"));
        if (!event.transactionId().equals(root.transactionId()) || event.ownerEpoch() != root.ownerEpoch()
            || event.baseSequence() != root.baseSequence() || event.baseRevision() != root.baseRevision()
            || !event.systemIdentity().equals(reference.identity())
            || event.ordinal() != reference.ordinal() || !event.digest().equals(reference.digest())
            || event.retainedBytes() != reference.encodedBytes()) {
          return invalid("prepared event differs from its sealed manifest reference");
        }
        count = Math.addExact(count, 1);
        bytes = Math.addExact(bytes, reference.encodedBytes());
        nextOrdinal++;
        if (count > root.eventCount() || bytes > root.totalEventBytes()) {
          return invalid("event traversal exceeds sealed root aggregates");
        }
        return consumer.apply(event).thenCompose(ignored -> visitEvents(refs, index + 1));
      });
    }

    private Summary summary() {
      if (count != root.eventCount() || bytes != root.totalEventBytes()
          || leafPages != root.eventDirectory().orElseThrow().leafPageCount()
          || directoryPages > MAX_EVENT_DIRECTORY_PAGES) {
        throw new IllegalStateException("manifest traversal does not cover sealed event aggregates");
      }
      return new Summary(count, bytes);
    }
  }

  private static IllegalStateException missing(String item) {
    return new IllegalStateException(item + " referenced by a sealed manifest is missing");
  }

  private static <T> CompletableFuture<T> invalid(String message) {
    return CompletableFuture.failedFuture(new IllegalStateException(message));
  }
}
