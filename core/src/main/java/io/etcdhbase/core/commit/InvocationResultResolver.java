package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Resolves a finalized invocation's original result without rereading current business data. */
public final class InvocationResultResolver {
  /** One durable invocation result, selected or completed without changes. */
  public sealed interface ResolvedInvocation extends AutoCloseable
      permits ResolvedResult, ResolvedNoChanges {
    long revision();
    boolean containsKeyChanges();
    ResultCatalog.RetainedResult payload();
    @Override void close();
  }

  /** The selected decision accompanies the bytes so protocol projection can verify its revision. */
  public static final class ResolvedResult implements ResolvedInvocation {
    private final CommitDecision decision;
    private final ResultCatalog.RetainedResult payload;

    private ResolvedResult(CommitDecision decision, ResultCatalog.RetainedResult payload) {
      this.decision = Objects.requireNonNull(decision, "decision");
      this.payload = Objects.requireNonNull(payload, "payload");
    }
    public CommitDecision decision() { return decision; }
    @Override public long revision() { return decision.revision(); }
    @Override public boolean containsKeyChanges() { return decision.containsKeyChanges(); }
    @Override public ResultCatalog.RetainedResult payload() { return payload; }
    @Override public void close() { payload.close(); }
  }

  /** A no-change result has a read revision but no commit decision or publication sequence. */
  public static final class ResolvedNoChanges implements ResolvedInvocation {
    private final long revision;
    private final ResultCatalog.RetainedResult payload;

    private ResolvedNoChanges(long revision, ResultCatalog.RetainedResult payload) {
      this.revision = revision;
      this.payload = Objects.requireNonNull(payload, "payload");
    }
    @Override public long revision() { return revision; }
    @Override public boolean containsKeyChanges() { return false; }
    @Override public ResultCatalog.RetainedResult payload() { return payload; }
    @Override public void close() { payload.close(); }
  }

  /** A replayable Txn result, with its original revision and whether it published KV changes. */
  public static final class ResolvedTransaction implements AutoCloseable {
    private final long revision;
    private final boolean changed;
    private final ResultCatalog.RetainedResult payload;

    /** Value form of an already-resolved decision; only the resolver attributes it to an invocation. */
    public ResolvedTransaction(long revision, boolean changed, ResultCatalog.RetainedResult payload) {
      this.revision = revision;
      this.changed = changed;
      this.payload = Objects.requireNonNull(payload, "payload");
    }
    public long revision() { return revision; }
    public boolean changed() { return changed; }
    public ResultCatalog.RetainedResult payload() { return payload; }
    @Override public void close() { payload.close(); }
  }

  private final InvocationRepository repository;
  private final InvocationService invocations;
  private final CommitOutcomeResolver outcomes;
  private final ManifestCatalog manifests;
  private final ResultCatalog results;

  public InvocationResultResolver(InvocationRepository repository, CommitOutcomeResolver outcomes,
      ManifestCatalog manifests, ResultCatalog results) {
    this.repository = Objects.requireNonNull(repository, "repository");
    this.invocations = new InvocationService(repository);
    this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.results = Objects.requireNonNull(results, "results");
  }

  public CompletableFuture<ResolvedResult> resolve(
      InvocationId id, ByteKey requestDigest, ByteKey authorizationDigest,
      RequestScope scope, BudgetManager.Account account) {
    return readSelectedBinding(id, requestDigest, authorizationDigest, scope, account)
        .thenCompose(binding -> resolveSelectedBinding(binding, scope, account));
  }

  /** Resolves either kind of durable invocation outcome without consulting current KV data. */
  public CompletableFuture<ResolvedInvocation> resolveInvocation(
      InvocationId id, ByteKey requestDigest, ByteKey authorizationDigest,
      RequestScope scope, BudgetManager.Account account) {
    return readMatching(id, requestDigest, authorizationDigest, scope, account)
        .thenCompose(binding -> {
          if (binding.state() == InvocationBinding.State.NO_CHANGES) {
            return loadNoChanges(binding, scope, account)
                .thenApply(result -> (ResolvedInvocation) result);
          }
          if (binding.state() != InvocationBinding.State.SEALED
              && binding.state() != InvocationBinding.State.SELECTED) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("invocation result is not queryable"));
          }
          return resolveSelectedBinding(binding, scope, account)
              .thenApply(result -> (ResolvedInvocation) result);
        });
  }

  private CompletableFuture<InvocationBinding> readSelectedBinding(InvocationId id, ByteKey requestDigest,
      ByteKey authorizationDigest, RequestScope scope, BudgetManager.Account account) {
    return readMatching(id, requestDigest, authorizationDigest, scope, account).thenCompose(binding -> {
      if (binding.state() != InvocationBinding.State.SEALED
          && binding.state() != InvocationBinding.State.SELECTED) {
        return CompletableFuture.failedFuture(new IllegalStateException("invocation result is not queryable"));
      }
      return CompletableFuture.completedFuture(binding);
    });
  }

  private CompletableFuture<SelectedInvocation> resolveSelectedInvocation(
      InvocationBinding binding, RequestScope scope) {
    return outcomes.resolve(binding, scope).thenCompose(outcome -> {
      if (outcome.state() != CommitOutcomeResolver.State.SELECTED_FINALIZED) {
        return CompletableFuture.failedFuture(new IllegalStateException("invocation is not finalized"));
      }
      return CompletableFuture.completedFuture(new SelectedInvocation(
          binding, outcome.decision().orElseThrow()));
    });
  }

  private CompletableFuture<ResolvedResult> resolveSelectedBinding(
      InvocationBinding binding, RequestScope scope, BudgetManager.Account account) {
    return resolveSelectedInvocation(binding, scope)
        .thenCompose(selected -> readVerifiedResult(selected, scope))
        .thenCompose(verified -> loadAndMark(verified.binding(), verified.decision(),
            verified.reference(), scope, account));
  }

  private CompletableFuture<VerifiedResult> readVerifiedResult(
      SelectedInvocation selected, RequestScope scope) {
    InvocationBinding binding = selected.binding();
    CommitDecision decision = selected.decision();
    return manifests.readRoot(decision.manifestRoot(), scope).thenCompose(root -> {
      ManifestRoot manifest = root.orElseThrow(() -> new IllegalStateException("result manifest is missing"));
      if (!manifest.systemIdentity().equals(decision.manifestRoot())
          || !manifest.digest().equals(decision.manifestDigest())
          || !manifest.transactionId().equals(binding.transactionId())
          || manifest.ownerEpoch() != binding.preparedOwnerEpoch()
          || manifest.baseSequence() != binding.baseSequence()
          || manifest.baseRevision() != binding.baseRevision()) {
        return CompletableFuture.failedFuture(new IllegalStateException("result manifest differs from the decision"));
      }
      ResultReference reference = manifest.resultReference().orElseThrow(() ->
          new IllegalStateException("finalized business manifest has no result"));
      if (!reference.identity().matches(binding)) {
        return CompletableFuture.failedFuture(new IllegalStateException("result belongs to another invocation"));
      }
      return CompletableFuture.completedFuture(new VerifiedResult(binding, decision, reference));
    });
  }

  /** Replays only the original durable no-change result, without consulting current KV or control. */
  public CompletableFuture<ResolvedNoChanges> resolveNoChanges(
      InvocationId id, ByteKey requestDigest, ByteKey authorizationDigest,
      RequestScope scope, BudgetManager.Account account) {
    return readMatching(id, requestDigest, authorizationDigest, scope, account)
        .thenCompose(binding -> loadNoChanges(binding, scope, account));
  }

  private CompletableFuture<ResolvedNoChanges> loadNoChanges(
      InvocationBinding binding, RequestScope scope, BudgetManager.Account account) {
    if (binding.state() != InvocationBinding.State.NO_CHANGES) {
      return CompletableFuture.failedFuture(new IllegalStateException("no-change result is not queryable"));
    }
    ResultReference reference = ResultReference.fromToken(ResultIdentity.from(binding),
        binding.resultReference().orElseThrow());
    return results.load(reference, scope, account)
        .thenCompose(payload -> verifyNoChangesWindow(binding, reference, payload, scope));
  }

  private CompletableFuture<ResolvedNoChanges> verifyNoChangesWindow(InvocationBinding binding,
      ResultReference reference, ResultCatalog.RetainedResult payload, RequestScope scope) {
    try {
      return repository.read(binding.invocationId(), scope)
          .thenApply(current -> deliverNoChanges(current, binding, reference, payload, scope))
          .whenComplete((ignored, failure) -> { if (failure != null) payload.close(); });
    } catch (RuntimeException | Error failure) {
      try { payload.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
      throw failure;
    }
  }

  private ResolvedNoChanges deliverNoChanges(java.util.Optional<InvocationBinding> current,
      InvocationBinding binding, ResultReference reference, ResultCatalog.RetainedResult payload,
      RequestScope scope) {
    InvocationBinding latest = current.orElseThrow(() -> new IllegalStateException("invocation disappeared"));
    if (!latest.sameAttempt(binding) || latest.state() != InvocationBinding.State.NO_CHANGES
        || latest.resultReference().filter(reference.token()::equals).isEmpty()) {
      throw new IllegalStateException("no-change result window changed during query");
    }
    if (scope.isClosed() || scope.isDeadlineExceeded()) {
      throw new java.util.concurrent.CompletionException(new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
    }
    return new ResolvedNoChanges(binding.baseRevision(), payload);
  }

  /** Resolves the original Txn outcome without recomputing a branch on newer data. */
  public CompletableFuture<ResolvedTransaction> resolveTransaction(
      InvocationId id, ByteKey requestDigest, ByteKey authorizationDigest,
      RequestScope scope, BudgetManager.Account account) {
    return readMatching(id, requestDigest, authorizationDigest, scope, account).thenCompose(binding -> {
      if (binding.state() == InvocationBinding.State.NO_CHANGES) {
        return resolveNoChanges(id, requestDigest, authorizationDigest, scope, account)
            .thenApply(result -> new ResolvedTransaction(result.revision(), false, result.payload()));
      }
      if (binding.state() != InvocationBinding.State.SEALED
          && binding.state() != InvocationBinding.State.SELECTED) {
        return CompletableFuture.failedFuture(new IllegalStateException("transaction result is not queryable"));
      }
      return resolve(id, requestDigest, authorizationDigest, scope, account).thenApply(result -> {
        if (!result.decision().containsKeyChanges()) {
          result.close();
          throw new IllegalStateException("published transaction has no KV changes");
        }
        return new ResolvedTransaction(result.decision().revision(), true, result.payload());
      });
    });
  }

  private CompletableFuture<InvocationBinding> readMatching(
      InvocationId id, ByteKey requestDigest, ByteKey authorizationDigest,
      RequestScope scope, BudgetManager.Account account) {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(requestDigest, "requestDigest");
    Objects.requireNonNull(authorizationDigest, "authorizationDigest");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    if (requestDigest.length() != 32 || authorizationDigest.length() != 32 || !account.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid result query identity or budget"));
    }
    return repository.read(id, scope).thenCompose(found -> {
      InvocationBinding binding = found.orElseThrow(() -> new IllegalStateException("invocation is missing"));
      if (!binding.invocationId().equals(id) || !binding.requestDigest().equals(requestDigest)
          || !binding.authorizationScopeDigest().equals(authorizationDigest)) {
        return CompletableFuture.failedFuture(new IllegalArgumentException("invocation request or authorization differs"));
      }
      return CompletableFuture.completedFuture(binding);
    });
  }

  private CompletableFuture<ResolvedResult> loadAndMark(InvocationBinding binding,
      CommitDecision decision, ResultReference reference, RequestScope scope, BudgetManager.Account account) {
    if (binding.state() == InvocationBinding.State.SELECTED
        && binding.resultReference().filter(reference.token()::equals).isEmpty()) {
      return CompletableFuture.failedFuture(new IllegalStateException("stored invocation result token differs"));
    }
    return results.load(reference, scope, account).thenCompose(result -> {
      try {
        CompletableFuture<InvocationBinding> marked = binding.state() == InvocationBinding.State.SEALED
            ? invocations.transition(binding, binding.selected(reference.token()), scope)
            : CompletableFuture.completedFuture(binding);
        return marked.thenCompose(ignored -> repository.read(binding.invocationId(), scope))
            .thenApply(current -> {
              InvocationBinding latest = current.orElseThrow(() -> new IllegalStateException("invocation disappeared"));
              if (!latest.sameAttempt(binding) || latest.state() != InvocationBinding.State.SELECTED
                  || latest.resultReference().filter(reference.token()::equals).isEmpty()) {
                throw new IllegalStateException("invocation result window changed during query");
              }
              return new ResolvedResult(decision, result);
            }).whenComplete((ignored, failure) -> {
              if (failure != null) result.close();
            });
      } catch (RuntimeException | Error failure) {
        try { result.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
        throw failure;
      }
    });
  }

  private record SelectedInvocation(InvocationBinding binding, CommitDecision decision) {}

  private record VerifiedResult(
      InvocationBinding binding, CommitDecision decision, ResultReference reference) {}
}
