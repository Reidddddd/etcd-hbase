package io.etcdhbase.core.kv;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.TxnReadSession;
import io.etcdhbase.core.commit.UnchangedRangeProof;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Test-only bridge to exercise the internal cache against a real backend read session. */
public final class M3CacheTestProbe implements AutoCloseable {
  public static long hits(RangeReadCache cache) { return cache.hitCount(); }
  public record Hit(long count, Optional<String> value) {}

  private final RangeResultCache cache;
  private final long group;
  private final ByteKey storageProfile;

  public M3CacheTestProbe(BudgetManager budget, String groupName, long group,
      ByteKey storageProfile, RequestScope owner) throws BackendFailure {
    this.group = group;
    this.storageProfile = storageProfile;
    this.cache = new RangeResultCache(1L << 20, 8, 8, budget, groupName, owner);
  }

  public boolean admit(TxnEvaluator.Evaluation evaluation, RequestScope scope) throws Exception {
    var validated = evaluation.finishReadOnlyForCache().get(30, TimeUnit.SECONDS);
    return cache.putValidated(group, storageProfile, validated, scope, cache.invalidationGeneration());
  }

  public Optional<Hit> sameSequence(TxnReadSession session, RangeCommand command,
      BudgetManager.Account account, RequestScope scope) throws Exception {
    var key = RangeResultCache.Key.from(group, storageProfile,
        session.protectedView(session.currentRevision()), command);
    return take(cache.get(key, account, scope), session);
  }

  public Optional<Hit> withProof(TxnReadSession session, RangeCommand command, UnchangedRangeProof prover,
      ManifestCatalog.ProofReadBudget proofBudget, BudgetManager.Account account, RequestScope scope)
      throws Exception {
    var result = cache.getWithProof(group, storageProfile, session, command, prover, proofBudget,
        8, account, scope).get(30, TimeUnit.SECONDS);
    return take(result, session);
  }

  private static Optional<Hit> take(Optional<RangeResultCache.Hit> found, TxnReadSession session) throws Exception {
    if (found.isEmpty()) return Optional.empty();
    try (var hit = found.orElseThrow()) {
      Optional<String> value = hit.records().isEmpty() ? Optional.empty()
          : Optional.of(new String(hit.records().getFirst().value().orElseThrow(), java.nio.charset.StandardCharsets.UTF_8));
      long count = hit.count();
      session.finish().get(30, TimeUnit.SECONDS);
      return Optional.of(new Hit(count, value));
    }
  }

  @Override public void close() { cache.close(); }
}
