package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.TxnReadSession;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.UnchangedRangeProof;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;

/** Internal read-path adapter. The runtime must not construct it until cache activation is safe. */
public final class RangeReadCache implements AutoCloseable {
  /** Separate bounded proof admission prevents an inconclusive proof consuming direct-read budget. */
  public record ProofOptions(UnchangedRangeProof prover, BudgetManager.Reservation reservation,
      BudgetManager.ReadLimits reads, int maximumPages, long maximumBytes,
      int maximumEntriesExamined, long maximumNanos) {
    public ProofOptions {
      Objects.requireNonNull(prover, "prover");
      Objects.requireNonNull(reservation, "reservation");
      Objects.requireNonNull(reads, "reads");
      if (maximumPages < 1 || maximumBytes < 1 || maximumEntriesExamined < 1
          || maximumEntriesExamined > 256 || maximumNanos < 1) {
        throw new IllegalArgumentException("invalid cache proof bounds");
      }
    }
  }

  private final long tableGroupId;
  private final ByteKey storageProfile;
  private final RangeResultCache cache;
  private final LongAdder hits = new LongAdder();
  private final BudgetManager budget;
  private final String group;
  private final Optional<ProofOptions> proofOptions;

  /** Captured before physical reading begins; a later invalidation fences its cache fill. */
  public static final class ReadGeneration {
    private final RangeReadCache owner;
    private final Object value;

    private ReadGeneration(RangeReadCache owner, Object value) {
      this.owner = owner;
      this.value = value;
    }
  }

  public RangeReadCache(long tableGroupId, ByteKey storageProfile, long maximumBytes,
      int maximumEntries, int maximumRecords, BudgetManager budget, String group,
      RequestScope owner) throws BackendFailure {
    this(tableGroupId, storageProfile, maximumBytes, maximumEntries, maximumRecords,
        budget, group, owner, Optional.empty());
  }

  public RangeReadCache(long tableGroupId, ByteKey storageProfile, long maximumBytes,
      int maximumEntries, int maximumRecords, BudgetManager budget, String group,
      RequestScope owner, Optional<ProofOptions> proofOptions) throws BackendFailure {
    if (tableGroupId == 0 || Objects.requireNonNull(storageProfile, "storageProfile").length() != 32) {
      throw new IllegalArgumentException("invalid cache storage identity");
    }
    this.tableGroupId = tableGroupId;
    this.storageProfile = storageProfile;
    this.budget = Objects.requireNonNull(budget, "budget");
    this.group = Objects.requireNonNull(group, "group");
    this.proofOptions = Objects.requireNonNull(proofOptions, "proofOptions");
    this.cache = new RangeResultCache(maximumBytes, maximumEntries, maximumRecords, budget, group, owner);
  }

  /** Evaluates one standalone current Range after its authoritative read view has been opened. */
  public CompletableFuture<TxnEvaluator.ValidatedRead> read(TxnCommand command, TxnReadSession session,
      TxnEvaluator evaluator, BudgetManager.Account account, RequestScope scope) {
    return read(command, session, evaluator, account, scope, beginRead());
  }

  public ReadGeneration beginRead() {
    return new ReadGeneration(this, cache.invalidationGeneration());
  }

  /** Evicts the specified authorization; all earlier fills are conservatively fenced. */
  public void invalidateAuthorization(ByteKey authorization) {
    cache.invalidateAuthorization(authorization);
  }

  public CompletableFuture<TxnEvaluator.ValidatedRead> read(TxnCommand command, TxnReadSession session,
      TxnEvaluator evaluator, BudgetManager.Account account, RequestScope scope, ReadGeneration generation) {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(session, "session");
    Objects.requireNonNull(evaluator, "evaluator");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(scope, "scope");
    if (Objects.requireNonNull(generation, "generation").owner != this) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("read generation belongs to another cache"));
    }
    if (!session.belongsTo(scope) || !account.belongsTo(scope)
        || !evaluator.belongsTo(session, account, scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid cached read ownership or limit"));
    }
    long maximumResultBytes = evaluator.maximumResultBytes();
    Optional<RangeCommand> selected = TxnEvaluator.standaloneRange(command);
    if (selected.isEmpty() || selected.orElseThrow().revision() != 0) {
      return direct(command, evaluator, scope, false, generation.value);
    }
    if (!cache.isCurrent(generation.value)) {
      return direct(command, evaluator, scope, false, generation.value);
    }
    RangeCommand range = selected.orElseThrow();
    try {
      var key = RangeResultCache.Key.from(tableGroupId, storageProfile,
          session.protectedView(session.currentRevision()), range);
      Optional<RangeResultCache.Hit> found = cache.get(key, account, scope);
      if (found.isPresent()) return finishHit(found.orElseThrow(), command, session, evaluator, scope, maximumResultBytes, generation.value);
      if (proofOptions.isEmpty()) return direct(command, evaluator, scope, true, generation.value);
      return prove(range, session, account, scope).thenCompose(proved -> proved.isPresent()
          ? finishHit(proved.orElseThrow(), command, session, evaluator, scope, maximumResultBytes, generation.value)
          : direct(command, evaluator, scope, true, generation.value));
    } catch (BackendFailure failure) {
      return failure.category() == BackendFailure.Category.RESOURCE_LIMIT
          ? direct(command, evaluator, scope, true, generation.value) : CompletableFuture.failedFuture(failure);
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletableFuture<TxnEvaluator.ValidatedRead> finishHit(RangeResultCache.Hit hit,
      TxnCommand command, TxnReadSession session, TxnEvaluator evaluator, RequestScope scope,
      long maximumResultBytes, Object generation) {
    try {
      if (!cache.isCurrent(generation)) {
        hit.close();
        return direct(command, evaluator, scope, false, generation);
      }
      var header = new ResultNode.Header(true, session.currentRevision());
      var tree = new ResultTree(new ResultNode.Txn(0, header, true,
          List.of(new ResultNode.Range(0, header, hit.records(), hit.count(), false))));
      if (tree.retainedBytes() > maximumResultBytes) {
        hit.close();
        return direct(command, evaluator, scope, true, generation);
      }
      return TxnEvaluator.ValidatedRead.finishCached(tree, session).thenCompose(result -> {
        if (!cache.isCurrent(generation)) {
          return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
        }
        hits.increment();
        return CompletableFuture.completedFuture(result);
      }).whenComplete((result, failure) -> {
        if (failure != null) hit.close();
      });
    } catch (RuntimeException | Error failure) {
      hit.close();
      throw failure;
    }
  }

  private CompletableFuture<Optional<RangeResultCache.Hit>> prove(RangeCommand range,
      TxnReadSession session, BudgetManager.Account account, RequestScope scope) {
    return new ProofOperation(proofOptions.orElseThrow(), scope).start(range, session, account);
  }

  /** Owns the independent proof scope and its registration until cleanup and producer drain. */
  private final class ProofOperation {
    private final ProofOptions options;
    private final RequestScope parentScope;
    private final RequestScope proofScope;
    private final RequestScope.ScopedCleanup registration;

    private ProofOperation(ProofOptions options, RequestScope parentScope) {
      this.options = options;
      this.parentScope = parentScope;
      proofScope = parentScope.childWithTimeoutNanos(options.maximumNanos());
      registration = parentScope.registerScopedCleanup(
          () -> proofScope.cancelAsync(Runnable::run), proofScope.cleanupCompletion());
    }

    private CompletableFuture<Optional<RangeResultCache.Hit>> start(RangeCommand range,
        TxnReadSession session, BudgetManager.Account account) {
      CompletableFuture<Optional<RangeResultCache.Hit>> operation;
      try {
        var proofAccount = budget.admit(group, proofScope, options.reservation(), options.reads());
        var allowance = new ManifestCatalog.ProofReadBudget(
            proofScope, proofAccount, options.maximumPages(), options.maximumBytes());
        operation = cache.getWithProof(tableGroupId, storageProfile, session, range, options.prover(),
            allowance, options.maximumEntriesExamined(), account, parentScope);
      } catch (BackendFailure failure) {
        operation = failure.category() == BackendFailure.Category.RESOURCE_LIMIT
            ? CompletableFuture.completedFuture(Optional.empty()) : CompletableFuture.failedFuture(failure);
      } catch (RuntimeException failure) {
        operation = CompletableFuture.failedFuture(failure);
      }
      return operation.handle((found, failure) -> {
        proofScope.close();
        return new ProofOutcome(found, failure);
      }).thenCompose(this::settleProof)
          .whenComplete((ignored, failure) -> closeRegistration());
    }

    private CompletableFuture<Optional<RangeResultCache.Hit>> settleProof(ProofOutcome outcome) {
      return CompletableFuture.allOf(
          proofScope.cleanupCompletion().toCompletableFuture(),
          proofScope.physicalProducersCompletion().toCompletableFuture())
          .handle((ignored, cleanupFailure) -> proofResult(outcome, cleanupFailure));
    }

    private Optional<RangeResultCache.Hit> proofResult(ProofOutcome outcome, Throwable cleanupFailure) {
      Throwable failure = outcome.failure();
      if (failure == null) failure = cleanupFailure;
      else if (cleanupFailure != null && cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
      if (failure == null) return outcome.found();
      if (outcome.found() != null && outcome.found().isPresent()) {
        try { outcome.found().orElseThrow().close(); }
        catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
      }
      throw new java.util.concurrent.CompletionException(failure);
    }

    private void closeRegistration() {
      try { registration.close(); }
      catch (Exception cleanup) { throw new java.util.concurrent.CompletionException(cleanup); }
    }
  }

  private record ProofOutcome(Optional<RangeResultCache.Hit> found, Throwable failure) {}

  /** Number of completed cache hits; intended for bounded runtime diagnostics. */
  public long hitCount() { return hits.sum(); }

  private CompletableFuture<TxnEvaluator.ValidatedRead> direct(
      TxnCommand command, TxnEvaluator evaluator, RequestScope scope, boolean cacheable, Object generation) {
    return evaluator.evaluate(command, Optional.empty()).thenCompose(evaluation -> cacheable
            ? evaluation.finishReadOnlyForCache() : evaluation.finishReadOnly())
        .thenApply(result -> {
          cache.putValidated(tableGroupId, storageProfile, result, scope, generation);
          return result;
        });
  }

  @Override public void close() { cache.close(); }
}
