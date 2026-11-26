package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Installs a durable table-group authorization barrier for a persisted subject transition. */
public final class AuthorizationBarrierInstaller {
  private static final int MAX_CAS_ATTEMPTS = 16;

  private final CommitControlStore control;
  private final Clock clock;

  public AuthorizationBarrierInstaller(CommitControlStore control) {
    this(control, Clock.systemUTC());
  }

  public AuthorizationBarrierInstaller(CommitControlStore control, Clock clock) {
    this.control = Objects.requireNonNull(control, "control");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  /**
   * Reserves the next required generation after the caller has durably made its subject REVOKING.
   * A later barrier advance from another subject may satisfy this target as well.
   */
  public CompletableFuture<Long> nextTargetGeneration(RequestScope scope) {
    Objects.requireNonNull(scope, "scope");
    return control.read(scope).thenCompose(found -> {
      if (found.isEmpty()) return CompletableFuture.failedFuture(unavailable());
      CommitState state = found.orElseThrow().state();
      if (!hasWritableOwner(state)) return CompletableFuture.failedFuture(unavailable());
      try {
        return CompletableFuture.completedFuture(
            Math.addExact(state.authorizationBarrierGeneration(), 1));
      } catch (ArithmeticException exhausted) {
        return CompletableFuture.failedFuture(unavailable());
      }
    });
  }

  /** Installs at least the persisted target and returns the strongly observed generation. */
  public CompletableFuture<Long> installAtLeast(long targetGeneration, RequestScope scope) {
    Objects.requireNonNull(scope, "scope");
    if (targetGeneration < 1) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("authorization barrier target must be positive"));
    }
    return installAtLeast(targetGeneration, scope, 0);
  }

  private CompletableFuture<Long> installAtLeast(
      long targetGeneration, RequestScope scope, int attempt) {
    if (attempt >= MAX_CAS_ATTEMPTS) {
      return CompletableFuture.failedFuture(
          new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    }
    return control.read(scope).thenCompose(found -> {
      if (found.isEmpty()) return CompletableFuture.failedFuture(unavailable());
      CommitState observed = found.orElseThrow().state();
      if (!observed.supportsAuthorizationBarrier()) {
        return CompletableFuture.failedFuture(unavailable());
      }
      long currentGeneration = observed.authorizationBarrierGeneration();
      if (currentGeneration >= targetGeneration) {
        return CompletableFuture.completedFuture(currentGeneration);
      }
      if (!hasWritableOwner(observed)) return CompletableFuture.failedFuture(unavailable());
      long nextGeneration;
      try {
        nextGeneration = Math.addExact(currentGeneration, 1);
      } catch (ArithmeticException exhausted) {
        return CompletableFuture.failedFuture(unavailable());
      }
      if (nextGeneration != targetGeneration) {
        return CompletableFuture.failedFuture(unavailable());
      }
      CommitState replacement;
      try {
        replacement = observed.advanceAuthorizationBarrier(
            observed.ownerInstance().orElseThrow(), observed.ownerEpoch(), currentGeneration);
      } catch (IllegalArgumentException | IllegalStateException | ArithmeticException invalid) {
        return CompletableFuture.failedFuture(unavailable());
      }
      ControlSnapshot snapshot = found.orElseThrow();
      return control.compareAndSet(snapshot, replacement, scope)
          .handle((applied, failure) -> new CasResult(applied, failure))
          .thenCompose(result -> resolveCas(targetGeneration, scope, attempt, result));
    });
  }

  private CompletableFuture<Long> resolveCas(
      long targetGeneration, RequestScope scope, int attempt, CasResult result) {
    if (result.failure() == null && Boolean.TRUE.equals(result.applied())) {
      return CompletableFuture.completedFuture(targetGeneration);
    }
    if (result.failure() != null && !isUnknown(result.failure())) {
      return CompletableFuture.failedFuture(CompletionFailures.unwrapCompletion(result.failure()));
    }
    return control.read(scope).handle((found, readFailure) -> {
      if (readFailure != null) {
        Throwable problem = result.failure() == null
            ? CompletionFailures.unwrapCompletion(readFailure)
            : CompletionFailures.unwrapCompletion(result.failure());
        if (result.failure() != null) {
          Throwable readProblem = CompletionFailures.unwrapCompletion(readFailure);
          if (problem != readProblem) problem.addSuppressed(readProblem);
        }
        throw new CompletionException(problem);
      }
      if (found.isEmpty()) throw new CompletionException(unavailable());
      CommitState current = found.orElseThrow().state();
      if (!current.supportsAuthorizationBarrier()) throw new CompletionException(unavailable());
      return current.authorizationBarrierGeneration() >= targetGeneration
          ? current.authorizationBarrierGeneration() : null;
    }).thenCompose(installed -> installed != null
        ? CompletableFuture.completedFuture(installed)
        : installAtLeast(targetGeneration, scope, attempt + 1));
  }

  private boolean hasWritableOwner(CommitState state) {
    if (!state.supportsAuthorizationBarrier()
        || state.ownerInstance().isEmpty()
        || state.ownerEpoch() <= 0
        || state.ownerLeaseExpired(clock.millis())) {
      return false;
    }
    return true;
  }

  private static boolean isUnknown(Throwable failure) {
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    return cause instanceof BackendFailure backend && switch (backend.category()) {
      case TIMEOUT, CANCELLED, UNAVAILABLE, UNKNOWN -> true;
      default -> false;
    };
  }

  private static BackendFailure unavailable() {
    return new BackendFailure(BackendFailure.Category.UNAVAILABLE);
  }

  private record CasResult(Boolean applied, Throwable failure) {}
}
