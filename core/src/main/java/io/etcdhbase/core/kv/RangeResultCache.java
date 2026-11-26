package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.ReadView;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.TxnReadSession;
import io.etcdhbase.core.commit.UnchangedRangeProof;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/** Bounded materialized results. Runtime activation and identity lifecycle are separate gates. */
final class RangeResultCache implements AutoCloseable {
  private static final long ENTRY_OVERHEAD = 512;
  private static final long INDEX_BASE_BYTES = 512;
  private static final long INDEX_BYTES_PER_PEAK_ENTRY = 64;

  /** A key includes the authoritative barrier and all query options; no response header is cached. */
  record Key(long tableGroupId, ByteKey profile, ByteKey storageProfile, ByteKey authorization, long sequence,
      long targetRevision, long compactRevision, KeyRange.Kind kind, Optional<ByteKey> lower,
      Optional<ByteKey> upper, long requestedRevision, long limit, RangeCommand.Sort sort,
      RangeCommand.RevisionFilter filter, boolean serializable, boolean keysOnly, boolean countOnly) {
    Key {
      Objects.requireNonNull(profile, "profile");
      Objects.requireNonNull(storageProfile, "storageProfile");
      Objects.requireNonNull(authorization, "authorization");
      Objects.requireNonNull(kind, "kind");
      Objects.requireNonNull(lower, "lower");
      Objects.requireNonNull(upper, "upper");
      Objects.requireNonNull(sort, "sort");
      Objects.requireNonNull(filter, "filter");
      if (tableGroupId == 0 || profile.length() != 32 || storageProfile.length() != 32 || authorization.length() != 32
          || sequence < 0 || targetRevision <= 0 || compactRevision < 0 || compactRevision > targetRevision
          || requestedRevision < 0 || requestedRevision > 0 && requestedRevision != targetRevision
          || kind == KeyRange.Kind.EMPTY && (lower.isPresent() || upper.isPresent())
          || kind == KeyRange.Kind.POINT && (lower.isEmpty() || upper.isPresent())
          || kind == KeyRange.Kind.RANGE && lower.isPresent() && upper.isPresent()
              && lower.orElseThrow().compareTo(upper.orElseThrow()) >= 0) {
        throw new IllegalArgumentException("invalid cache view");
      }
    }

    static Key from(long tableGroupId, ByteKey storageProfile, ReadView view, RangeCommand command) {
      Objects.requireNonNull(view, "view");
      Objects.requireNonNull(command, "command");
      if (command.context() != RangeCommand.Context.STANDALONE) {
        throw new IllegalArgumentException("transaction results are not cacheable");
      }
      var range = command.range();
      return new Key(tableGroupId, view.profileFingerprint(), storageProfile, view.authorizationScopeDigest(),
          view.barrierSequence(), view.targetRevision(), view.compactRevision(), range.kind(),
          range.lowerInclusive(), range.upperExclusive(), command.revision(), command.limit(),
          command.sort(), command.filter(), command.serializable(), command.keysOnly(), command.countOnly());
    }

    boolean contains(ByteKey key) {
      return switch (kind) {
        case EMPTY -> false;
        case POINT -> lower.filter(key::equals).isPresent();
        case RANGE -> lower.map(bound -> key.compareTo(bound) >= 0).orElse(true)
            && upper.map(bound -> key.compareTo(bound) < 0).orElse(true);
      };
    }

    long retainedBytes() {
      return ENTRY_OVERHEAD + profile.length() + storageProfile.length() + authorization.length()
          + lower.map(ByteKey::length).orElse(0) + upper.map(ByteKey::length).orElse(0);
    }

    boolean sameCurrentQuery(Key other) {
      return tableGroupId == other.tableGroupId && profile.equals(other.profile)
          && storageProfile.equals(other.storageProfile) && authorization.equals(other.authorization)
          && compactRevision == other.compactRevision && kind == other.kind
          && lower.equals(other.lower) && upper.equals(other.upper)
          && requestedRevision == 0 && other.requestedRevision == 0 && limit == other.limit
          && sort.equals(other.sort) && filter.equals(other.filter) && serializable == other.serializable
          && keysOnly == other.keysOnly && countOnly == other.countOnly;
    }
  }

  private record Entry(List<StoredRecord> records, long count, long bytes,
      Optional<UnchangedRangeProof.Baseline> baseline) {}
  private record Candidate(Key key, UnchangedRangeProof.Baseline baseline, Hit hit) {}

  private final long maximumBytes;
  private final int maximumEntries;
  private final int maximumRecords;
  private final BudgetManager.ServiceMemory ownerLease;
  private final RequestScope.ScopedCleanup registration;
  private LinkedHashMap<Key, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);
  private long retainedBytes;
  private int peakEntries;
  private boolean closed;
  private Object invalidationGeneration = new Object();

  RangeResultCache(long maximumBytes, int maximumEntries, int maximumRecords,
      BudgetManager budget, String groupId, RequestScope ownerScope) throws BackendFailure {
    Objects.requireNonNull(budget, "budget");
    Objects.requireNonNull(groupId, "groupId");
    Objects.requireNonNull(ownerScope, "ownerScope");
    if (maximumBytes <= INDEX_BASE_BYTES || maximumEntries <= 0 || maximumRecords <= 0) {
      throw new IllegalArgumentException("cache bounds must be positive");
    }
    this.maximumBytes = maximumBytes;
    this.maximumEntries = maximumEntries;
    this.maximumRecords = maximumRecords;
    ownerLease = budget.reserveServiceMemory(groupId, ownerScope, maximumBytes);
    try {
      registration = ownerScope.registerScopedCleanup(this);
    } catch (RuntimeException | Error failure) {
      try { ownerLease.close(); }
      catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
      throw failure;
    }
    if (ownerScope.isClosed() || ownerScope.isDeadlineExceeded()) {
      close();
      throw new BackendFailure(ownerScope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
    }
  }

  /** Only an exhausted, materialized result is eligible, including a negative result. */
  synchronized boolean put(Key key, List<StoredRecord> result, long count, boolean exhausted) {
    return put(key, result, count, exhausted, Optional.empty());
  }

  synchronized Object invalidationGeneration() {
    return invalidationGeneration;
  }

  synchronized boolean isCurrent(Object generation) {
    return !closed && ownerLease.isActive() && generation == invalidationGeneration;
  }

  /** Clears one authorization and fences fills started before this invalidation. */
  synchronized void invalidateAuthorization(ByteKey authorization) {
    Objects.requireNonNull(authorization, "authorization");
    if (authorization.length() != 32) throw new IllegalArgumentException("invalid authorization digest");
    if (closed) return;
    invalidationGeneration = new Object();
    var iterator = entries.entrySet().iterator();
    while (iterator.hasNext()) {
      var entry = iterator.next();
      if (entry.getKey().authorization().equals(authorization)) {
        retainedBytes -= entry.getValue().bytes();
        iterator.remove();
      }
    }
  }

  synchronized boolean putIfCurrent(Key key, List<StoredRecord> result, long count,
      boolean exhausted, Optional<UnchangedRangeProof.Baseline> baseline, Object generation) {
    if (generation != invalidationGeneration) return false;
    return put(key, result, count, exhausted, baseline);
  }

  private synchronized boolean put(Key key, List<StoredRecord> result, long count, boolean exhausted,
      Optional<UnchangedRangeProof.Baseline> baseline) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(result, "result");
    if (closed || !ownerLease.isActive()) return false;
    OptionalLong size = admittedBytes(key, result, count, exhausted);
    if (size.isEmpty()) return false;
    return retain(key, result, count, baseline, size.orElseThrow());
  }

  private OptionalLong admittedBytes(Key key, List<StoredRecord> result, long count, boolean exhausted) {
    if (!eligibleResult(key, result, count, exhausted)) return OptionalLong.empty();
    long bytes = key.retainedBytes();
    ByteKey previous = null;
    for (StoredRecord record : result) {
      Objects.requireNonNull(record, "record");
      if (!eligibleRecord(key, record, previous)) return OptionalLong.empty();
      previous = record.key();
      try { bytes = Math.addExact(bytes, record.retainedBytes()); }
      catch (ArithmeticException overflow) { return OptionalLong.empty(); }
      if (bytes > maximumBytes) return OptionalLong.empty();
    }
    return OptionalLong.of(bytes);
  }

  private boolean eligibleResult(Key key, List<StoredRecord> result, long count, boolean exhausted) {
    if (!exhausted || result.size() > maximumRecords || count < 0) return false;
    if (key.sort().target() != 0 || key.sort().order() < 0 || key.sort().order() > 2) return false;
    if (key.countOnly()) {
      return (key.kind() == KeyRange.Kind.POINT || key.limit() == 1)
          && result.isEmpty() && count <= 1;
    }
    return (key.limit() <= 0 || result.size() <= key.limit()) && count == result.size();
  }

  private boolean eligibleRecord(Key key, StoredRecord record, ByteKey previous) {
    if (!key.contains(record.key()) || record.metadata().state() != StorageMetadata.State.LIVE
        || record.metadata().modificationRevision() > key.targetRevision()
        || !key.filter().matches(record.metadata())) return false;
    if (key.keysOnly() == record.hasValue()) return false;
    return previous == null || (key.sort().order() == 2
        ? previous.compareTo(record.key()) > 0 : previous.compareTo(record.key()) < 0);
  }

  private boolean retain(Key key, List<StoredRecord> result, long count,
      Optional<UnchangedRangeProof.Baseline> baseline, long bytes) {
    // LinkedHashMap does not shrink its backing array after eviction. Continue charging its
    // peak capacity until the map itself is discarded, even when few entries remain.
    if (bytes > maximumBytes - indexBytes(peakEntries)) return false;
    Entry next = new Entry(List.copyOf(result), count, bytes, baseline);
    Entry old = entries.remove(key);
    if (old != null) retainedBytes -= old.bytes();
    while (!entries.isEmpty() && (entries.size() >= maximumEntries
        || bytes > maximumBytes - retainedBytes - indexBytes(Math.max(peakEntries, entries.size() + 1)))) {
      var iterator = entries.entrySet().iterator();
      Map.Entry<Key, Entry> victim = iterator.next();
      retainedBytes -= victim.getValue().bytes();
      iterator.remove();
    }
    int nextPeak = Math.max(peakEntries, entries.size() + 1);
    if (bytes > maximumBytes - retainedBytes - indexBytes(nextPeak)) return false;
    entries.put(key, next);
    retainedBytes += bytes;
    peakEntries = nextPeak;
    return true;
  }

  /** Accepts only a standalone result whose original protected read has finished successfully. */
  boolean putValidated(long tableGroupId, ByteKey storageProfile,
      TxnEvaluator.ValidatedRead validated, RequestScope scope, Object generation) {
    Objects.requireNonNull(validated, "validated");
    Objects.requireNonNull(scope, "scope");
    if (!validated.belongsTo(scope) || scope.isClosed() || scope.isDeadlineExceeded()) return false;
    var context = validated.cacheContext();
    if (context.isEmpty() || !(validated.result().root() instanceof ResultNode.Txn root)
        || !root.succeeded() || root.responses().size() != 1
        || !(root.responses().getFirst() instanceof ResultNode.Range range)) return false;
    var captured = context.orElseThrow();
    Key key = Key.from(tableGroupId, storageProfile, captured.view(), captured.command());
    return putIfCurrent(key, range.records(), range.count(), !range.more(),
        Optional.of(UnchangedRangeProof.Baseline.capture(captured.view())), generation);
  }

  /**
   * Tests one older entry against a separately budgeted continuous-commit proof. The caller must
   * still finish the current protected session and validate live authorization before sending.
   */
  CompletableFuture<Optional<Hit>> getWithProof(long tableGroupId, ByteKey storageProfile,
      TxnReadSession session, RangeCommand command, UnchangedRangeProof prover,
      ManifestCatalog.ProofReadBudget proofBudget, int maximumEntriesExamined,
      BudgetManager.Account account, RequestScope scope) {
    Objects.requireNonNull(session, "session");
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(prover, "prover");
    Objects.requireNonNull(proofBudget, "proofBudget");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(scope, "scope");
    if (!session.belongsTo(scope) || proofBudget.belongsTo(scope)
        || maximumEntriesExamined < 1 || maximumEntriesExamined > 256) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid cache proof ownership or bound"));
    }
    long target = command.revision() > 0 ? command.revision() : session.currentRevision();
    final ReadView current;
    final Key key;
    final Optional<Candidate> candidate;
    try {
      current = session.protectedView(target);
      key = Key.from(tableGroupId, storageProfile, current, command);
      candidate = olderCandidate(key, maximumEntriesExamined, account, scope);
    } catch (BackendFailure failure) {
      return failure.category() == BackendFailure.Category.RESOURCE_LIMIT
          ? CompletableFuture.completedFuture(Optional.empty()) : CompletableFuture.failedFuture(failure);
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    if (candidate.isEmpty()) return CompletableFuture.completedFuture(Optional.empty());
    Candidate saved = candidate.orElseThrow();
    CompletableFuture<UnchangedRangeProof.Verdict> proof;
    try {
      proof = prover.prove(saved.key().tableGroupId(), key.tableGroupId(), saved.baseline(), current,
          saved.key().storageProfile(), key.storageProfile(), command.range(), proofBudget);
    } catch (RuntimeException | Error failure) {
      saved.hit().close();
      return CompletableFuture.failedFuture(failure);
    }
    return new ProofDelivery(saved, scope, account, session, target, command, key).start(proof);
  }

  /** Owns the retained hit until it is delivered, cancelled, or rejected by the current view. */
  private final class ProofDelivery {
    private final Candidate saved;
    private final RequestScope scope;
    private final BudgetManager.Account account;
    private final TxnReadSession session;
    private final long target;
    private final RangeCommand command;
    private final Key key;
    private final CompletableFuture<Optional<Hit>> outcome = new CompletableFuture<>();
    private final CompletableFuture<Optional<Hit>> exposed = outcome.copy();

    private ProofDelivery(Candidate saved, RequestScope scope, BudgetManager.Account account,
        TxnReadSession session, long target, RangeCommand command, Key key) {
      this.saved = saved;
      this.scope = scope;
      this.account = account;
      this.session = session;
      this.target = target;
      this.command = command;
      this.key = key;
    }

    private CompletableFuture<Optional<Hit>> start(CompletableFuture<UnchangedRangeProof.Verdict> proof) {
      outcome.thenAccept(this::closeUndeliveredHit);
      exposed.whenComplete((ignored, failure) -> {
        if (exposed.isCancelled()) {
          proof.cancel(true);
          saved.hit().close();
        }
      });
      proof.whenComplete(this::completeProof);
      return exposed;
    }

    private void closeUndeliveredHit(Optional<Hit> found) {
      exposed.whenComplete((delivered, failure) -> {
        if (found.isPresent() && (delivered == null || delivered.orElse(null) != found.orElseThrow())) {
          found.orElseThrow().close();
        }
      });
    }

    private void completeProof(UnchangedRangeProof.Verdict verdict, Throwable failure) {
      boolean valid = false;
      try {
        if (scope.isClosed() || scope.isDeadlineExceeded()) {
          throw new java.util.concurrent.CompletionException(new BackendFailure(scope.isDeadlineExceeded()
              ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
        }
        if (failure != null) {
          Throwable cause = io.etcdhbase.core.error.CompletionFailures.unwrapCompletion(failure);
          if (cause instanceof Error fatal) throw fatal;
        }
        if (failure == null && verdict == UnchangedRangeProof.Verdict.UNCHANGED
            && isOpen() && account.isActive() && !scope.isClosed() && !scope.isDeadlineExceeded()) {
          ReadView stillPinned = session.protectedView(target);
          valid = Key.from(key.tableGroupId(), key.storageProfile(), stillPinned, command).equals(key);
        }
        if (valid) outcome.complete(Optional.of(saved.hit()));
        else {
          saved.hit().close();
          outcome.complete(Optional.empty());
        }
      } catch (RuntimeException | Error problem) {
        try { saved.hit().close(); }
        catch (RuntimeException | Error cleanup) { if (cleanup != problem) problem.addSuppressed(cleanup); }
        outcome.completeExceptionally(problem);
        if (problem instanceof Error fatal) throw fatal;
      }
    }
  }

  private synchronized Optional<Candidate> olderCandidate(Key current, int maximumEntriesExamined,
      BudgetManager.Account account, RequestScope scope) throws BackendFailure {
    if (closed || !ownerLease.isActive() || current.requestedRevision() != 0) return Optional.empty();
    Key selected = null;
    int examined = 0;
    for (var entry : entries.reversed().entrySet()) {
      if (++examined > maximumEntriesExamined) break;
      Key previous = entry.getKey();
      if (previous.sequence() < current.sequence() && previous.targetRevision() <= current.targetRevision()
          && previous.sameCurrentQuery(current) && entry.getValue().baseline().isPresent()) {
        selected = previous;
        break;
      }
    }
    if (selected == null) return Optional.empty();
    Entry entry = entries.get(selected);
    Optional<Hit> hit = get(selected, account, scope);
    if (hit.isEmpty()) return Optional.empty();
    return Optional.of(new Candidate(selected, entry.baseline().orElseThrow(), hit.orElseThrow()));
  }

  /** A hit remains charged to its request even if the cache evicts the entry. */
  synchronized Optional<Hit> get(Key key, BudgetManager.Account account, RequestScope scope) throws BackendFailure {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(scope, "scope");
    if (closed || !ownerLease.isActive()) return Optional.empty();
    if (!account.belongsTo(scope) || !ownerLease.permits(account)) {
      throw new IllegalArgumentException("cache account belongs to another request or table group");
    }
    Entry entry = entries.get(key);
    if (entry == null) return Optional.empty();
    var lease = account.tryRetain(BudgetManager.Memory.OUTPUT, entry.bytes())
        .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    try {
      Hit hit = new Hit(this, entry.records(), entry.count(), lease, scope);
      if (scope.isClosed()) {
        hit.close();
        throw new BackendFailure(BackendFailure.Category.CANCELLED);
      }
      if (scope.isDeadlineExceeded()) {
        hit.close();
        throw new BackendFailure(BackendFailure.Category.TIMEOUT);
      }
      if (!ownerLease.isActive()) {
        hit.close();
        return Optional.empty();
      }
      return Optional.of(hit);
    } catch (BackendFailure | RuntimeException | Error failure) {
      lease.close();
      throw failure;
    }
  }

  synchronized long retainedBytes() { return closed ? 0 : retainedBytes + indexBytes(peakEntries); }

  private synchronized boolean isOpen() { return !closed && ownerLease.isActive(); }

  private static long indexBytes(int peakEntries) {
    return INDEX_BASE_BYTES + INDEX_BYTES_PER_PEAK_ENTRY * peakEntries;
  }

  @Override public void close() {
    synchronized (this) {
      if (closed) return;
      closed = true;
      entries = null;
      retainedBytes = 0;
      peakEntries = 0;
    }
    try { ownerLease.close(); }
    finally {
      try { if (registration != null) registration.close(); }
      catch (Exception failure) { throw new IllegalStateException("cache owner cleanup failed", failure); }
    }
  }

  static final class Hit implements AutoCloseable {
    private final RangeResultCache owner;
    private List<StoredRecord> records;
    private final long count;
    private final BudgetManager.Lease lease;
    private final CompletableFuture<Void> released = new CompletableFuture<>();
    private final RequestScope.ScopedCleanup registration;
    private boolean closed;

    Hit(RangeResultCache owner, List<StoredRecord> records, long count, BudgetManager.Lease lease, RequestScope scope) {
      this.owner = owner;
      this.records = records;
      this.count = count;
      this.lease = lease;
      this.registration = scope.registerScopedCleanup(() -> {
        synchronized (this) {
          if (closed) return;
        }
        scope.physicalProducersCompletion().whenComplete((ignored, failure) -> {
            try {
              close();
            } catch (RuntimeException | Error cleanup) {
              released.completeExceptionally(cleanup);
            }
          });
      }, released);
    }

    synchronized List<StoredRecord> records() {
      if (closed || !owner.isOpen()) throw new IllegalStateException("cache hit is closed");
      return records;
    }

    synchronized long count() {
      if (closed || !owner.isOpen()) throw new IllegalStateException("cache hit is closed");
      return count;
    }

    @Override public void close() {
      synchronized (this) {
        if (closed) return;
        closed = true;
        records = List.of();
      }
      try {
        lease.close();
        released.complete(null);
      } catch (RuntimeException | Error cleanup) {
        released.completeExceptionally(cleanup);
        throw cleanup;
      }
      finally {
        try { if (registration != null) registration.close(); }
        catch (Exception failure) { throw new IllegalStateException("cache hit cleanup failed", failure); }
      }
    }
  }
}
