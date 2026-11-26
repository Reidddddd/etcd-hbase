package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.TxnReadSession;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.UnchangedRangeProof;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.concurrent.CompletableFuture;
import java.util.Optional;

/** Test-only bridge for exercising cache admission with the real transaction read session. */
public final class CacheAdmissionTestSupport implements AutoCloseable {
  private final RangeResultCache cache;

  public CacheAdmissionTestSupport(BudgetManager budget, RequestScope ownerScope) throws BackendFailure {
    cache = new RangeResultCache(16_384, 8, 8, budget, "public", ownerScope);
  }

  public CompletableFuture<TxnEvaluator.ValidatedRead> finish(TxnEvaluator.Evaluation evaluation) {
    return evaluation.finishReadOnlyForCache();
  }

  public boolean admit(TxnEvaluator.ValidatedRead result, RequestScope scope, ByteKey storageProfile) {
    return cache.putValidated(7, storageProfile, result, scope, cache.invalidationGeneration());
  }

  public Object generation() { return cache.invalidationGeneration(); }

  public void invalidate(ByteKey authorization) { cache.invalidateAuthorization(authorization); }

  public boolean admit(TxnEvaluator.ValidatedRead result, RequestScope scope,
      ByteKey storageProfile, Object generation) {
    return cache.putValidated(7, storageProfile, result, scope, generation);
  }

  /** Test bridge obtains a fresh view and validates its pin before exposing a hit count. */
  public long lookup(TxnReadSession session, RangeCommand command, ByteKey storageProfile,
      BudgetManager.Account account, RequestScope scope) throws BackendFailure {
    long target = command.revision() > 0 ? command.revision() : session.currentRevision();
    var view = session.protectedView(target);
    var key = RangeResultCache.Key.from(7, storageProfile, view, command);
    var found = cache.get(key, account, scope);
    if (found.isEmpty()) return -1;
    try (var hit = found.orElseThrow()) {
      long count = hit.count();
      session.finish().join();
      return count;
    }
  }

  public Optional<String> lookupValue(TxnReadSession session, RangeCommand command, ByteKey storageProfile,
      BudgetManager.Account account, RequestScope scope) throws BackendFailure {
    long target = command.revision() > 0 ? command.revision() : session.currentRevision();
    var key = RangeResultCache.Key.from(7, storageProfile, session.protectedView(target), command);
    var found = cache.get(key, account, scope);
    if (found.isEmpty()) return Optional.empty();
    try (var hit = found.orElseThrow()) {
      var value = hit.records().getFirst().value().orElseThrow();
      session.finish().join();
      return Optional.of(new String(value, java.nio.charset.StandardCharsets.UTF_8));
    }
  }

  public Optional<String> lookupWithProof(TxnReadSession session, RangeCommand command, ByteKey storageProfile,
      UnchangedRangeProof prover, ManifestCatalog.ProofReadBudget proofBudget,
      BudgetManager.Account account, RequestScope scope) {
    var found = cache.getWithProof(7, storageProfile, session, command, prover, proofBudget, 8, account, scope).join();
    if (found.isEmpty()) return Optional.empty();
    try (var hit = found.orElseThrow()) {
      var value = hit.records().getFirst().value().orElseThrow();
      session.finish().join();
      return Optional.of(new String(value, java.nio.charset.StandardCharsets.UTF_8));
    }
  }

  @Override public void close() { cache.close(); }
}
