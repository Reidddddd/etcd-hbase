package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Selects logical compaction through the existing owner-fenced control row. */
public final class CompactionService {
  private static final int MAX_ATTEMPTS = 32;
  private final CommitControlStore control;
  private final SystemRecordStore records;
  private final Clock clock;

  public CompactionService(CommitControlStore control, SystemRecordStore records, Clock clock) {
    this.control = Objects.requireNonNull(control, "control");
    this.records = Objects.requireNonNull(records, "records");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  /** The caller serializes new attempts on the existing mutation owner lane. */
  public CompletableFuture<CompactionOperation> compact(long revision, InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, CommitAuthorizationProof authorization, RequestScope scope) {
    return read(binding.invocationId(), scope).thenCompose(existing -> {
      if (existing.isPresent()) return resume(existing.orElseThrow(), revision, binding, owner, authorization, scope);
      return control.read(scope).thenCompose(found -> {
        ControlSnapshot observed = found.orElseThrow(() -> new IllegalStateException("missing control"));
        requireOwner(observed.state(), owner);
        if (!observed.state().supportsAuthorizationBarrier()) return failed(BackendFailure.Category.UNAVAILABLE);
        var proposed = new CompactionOperation(binding.invocationId(), binding.requestDigest(),
            binding.authorizationScopeDigest(), observed.state().profileFingerprint(), revision,
            observed.state().publishedRevision(), owner.epoch(), observed.state().publishedSequence(),
            observed.state().stateVersion(), ManifestDigest.sha256(observed.comparisonToken()),
            CompactionOperation.Stage.PREPARED);
        return records.createIfAbsent(SystemRecordType.COMPACTION, identity(proposed.id()), proposed.encode(), scope)
            .handle((created, failure) -> {
              requireKnownOrUnknownWrite(failure);
              return created;
            }).thenCompose(ignored -> read(proposed.id(), scope))
            .thenCompose(stored -> resume(stored.orElseThrow(() -> unknown(null)), revision,
                binding, owner, authorization, scope));
      });
    });
  }

  /** Reconciles the previous selection before another Compact may replace its control identity. */
  public CompletableFuture<Optional<CompactionOperation>> reconcile(CommitState state, RequestScope scope) {
    if (state.compactOperationId().isEmpty()) return CompletableFuture.completedFuture(Optional.empty());
    return read(state.compactOperationId().orElseThrow(), scope).thenCompose(found -> {
      CompactionOperation operation = found.orElseThrow(() -> new IllegalStateException("selected Compact is missing"));
      if (!operation.profile().equals(state.profileFingerprint()) || operation.revision() != state.compactRevision()
          || operation.controlVersion() >= state.stateVersion()
          || operation.stage() != CompactionOperation.Stage.PREPARED
              && operation.stage() != CompactionOperation.Stage.LOGICAL) {
        return CompletableFuture.failedFuture(new IllegalStateException("Compact selection identity differs"));
      }
      return operation.stage() == CompactionOperation.Stage.PREPARED
          ? replace(operation, operation.transition(CompactionOperation.Stage.LOGICAL), scope).thenApply(Optional::of)
          : CompletableFuture.completedFuture(Optional.of(operation));
    });
  }

  public CompletableFuture<Optional<CompactionOperation>> read(InvocationId id, RequestScope scope) {
    return records.read(SystemRecordType.COMPACTION, identity(id), scope).thenApply(found -> found.map(value -> {
      CompactionOperation operation = CompactionOperation.decode(value);
      if (!operation.id().equals(id)) throw new IllegalStateException("Compact record identity differs");
      return operation;
    }));
  }

  private CompletableFuture<CompactionOperation> resume(CompactionOperation operation, long revision,
      InvocationBinding binding, CommitOwnerService.OwnerToken owner,
      CommitAuthorizationProof authorization, RequestScope scope) {
    if (!operation.requestDigest().equals(binding.requestDigest())
        || !operation.authorizationDigest().equals(binding.authorizationScopeDigest())
        || operation.revision() != revision) return failed(BackendFailure.Category.PERMISSION);
    return select(operation, owner, authorization, scope, 0);
  }

  private CompletableFuture<CompactionOperation> select(CompactionOperation operation,
      CommitOwnerService.OwnerToken owner, CommitAuthorizationProof authorization, RequestScope scope, int attempt) {
    if (scope.isClosed() || scope.isDeadlineExceeded()) return failed(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
    if (operation.stage() != CompactionOperation.Stage.PREPARED) return CompletableFuture.completedFuture(operation);
    if (attempt >= MAX_ATTEMPTS) return failed(BackendFailure.Category.UNKNOWN);
    return control.read(scope).thenCompose(found -> {
      ControlSnapshot observed = found.orElseThrow(() -> new IllegalStateException("missing control"));
      CommitState state = observed.state();
      return reconcile(state, scope).thenCompose(previous -> {
        if (state.compactOperationId().filter(operation.id()::equals).isPresent()) {
          return CompletableFuture.completedFuture(previous.orElseThrow());
        }
        requireOwner(state, owner);
        if (!state.profileFingerprint().equals(operation.profile())) {
          return CompletableFuture.failedFuture(new IllegalStateException("Compact profile differs"));
        }
        if (!state.supportsAuthorizationBarrier() || state.publication().isPresent()
            || operation.ownerEpoch() != owner.epoch()) {
          return failed(BackendFailure.Category.UNAVAILABLE);
        }
        return prepareAndSelect(operation, observed, owner, authorization, scope, attempt);
      });
    });
  }

  private CompletableFuture<CompactionOperation> prepareAndSelect(CompactionOperation operation,
      ControlSnapshot observed, CommitOwnerService.OwnerToken owner,
      CommitAuthorizationProof authorization, RequestScope scope, int attempt) {
    CompactionOperation prepared = operation.forAttempt(observed);
    if (!prepared.equals(operation)) {
      return replace(operation, prepared, scope)
          .thenCompose(stored -> select(stored, owner, authorization, scope, attempt + 1));
    }
    CommitState state = observed.state();
    boolean initialZero = operation.revision() == 0 && state.compactRevision() == 0
        && state.compactOperationId().isEmpty();
    if (!initialZero && operation.revision() <= state.compactRevision()) {
      return replace(operation, operation.transition(CompactionOperation.Stage.COMPACTED), scope);
    }
    if (operation.revision() > state.publishedRevision()) {
      return replace(operation, operation.transition(CompactionOperation.Stage.FUTURE), scope);
    }
    return authorization.reauthorize(state.authorizationBarrierGeneration(), scope).thenCompose(proof -> {
      requireOwner(state, owner);
      if (scope.isClosed() || scope.isDeadlineExceeded()) return failed(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
      CommitState replacement = state.selectCompaction(operation.revision(), operation.id());
      return control.compareAndSet(observed, replacement, scope).handle((updated, failure) -> {
        requireKnownOrUnknownWrite(failure);
        return updated;
      }).thenCompose(ignored -> select(operation, owner, authorization, scope, attempt + 1));
    });
  }

  private CompletableFuture<CompactionOperation> replace(CompactionOperation current,
      CompactionOperation replacement, RequestScope scope) {
    return records.compareAndSet(SystemRecordType.COMPACTION, identity(current.id()),
        Optional.of(current.encode()), replacement.encode(), scope).handle((changed, failure) -> {
          requireKnownOrUnknownWrite(failure);
          return changed;
        }).thenCompose(ignored -> read(current.id(), scope)).thenApply(found -> {
          CompactionOperation stored = found.orElseThrow(() -> new IllegalStateException("Compact disappeared"));
          if (!stored.equals(replacement)) throw unknown(null);
          return stored;
        });
  }

  private void requireOwner(CommitState state, CommitOwnerService.OwnerToken owner) {
    if (state.ownerEpoch() != owner.epoch() || state.ownerInstance().filter(owner.instance()::equals).isEmpty()
        || state.ownerLeaseExpired(clock.millis())) {
      throw new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
  }

  private static void requireKnownOrUnknownWrite(Throwable failure) {
    if (failure == null) return;
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    if (cause instanceof BackendFailure backend && (backend.category() == BackendFailure.Category.TIMEOUT
        || backend.category() == BackendFailure.Category.UNAVAILABLE || backend.category() == BackendFailure.Category.UNKNOWN)) return;
    if (cause instanceof Error fatal) throw fatal;
    throw new CompletionException(cause);
  }

  private static ByteKey identity(InvocationId id) { return ByteKey.copyOf(id.bytes()); }

  private static CompletionException unknown(Throwable cause) {
    return new CompletionException(new BackendFailure(BackendFailure.Category.UNKNOWN, cause));
  }

  private static <T> CompletableFuture<T> failed(BackendFailure.Category category) {
    return CompletableFuture.failedFuture(new BackendFailure(category));
  }
}
