package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Enables the durable authorization fence without changing unrelated control capabilities. */
public final class AuthorizationBarrierUpgrade {
  private final CommitControlStore control;

  public AuthorizationBarrierUpgrade(CommitControlStore control) {
    this.control = Objects.requireNonNull(control, "control");
  }

  /** The caller must quiesce local owner work before enabling the barrier. */
  public CompletableFuture<CommitOwnerService.OwnerToken> enable(
      CommitOwnerService.OwnerToken owner, long nowMillis, RequestScope scope) {
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(scope, "scope");
    return control.read(scope).thenCompose(found -> {
      ControlSnapshot observed = found.orElseThrow(() -> new IllegalStateException("missing control row"));
      CommitState state = observed.state();
      if (state.supportsAuthorizationBarrier()) {
        return state.ownerEpoch() == owner.epoch()
                && state.ownerInstance().filter(owner.instance()::equals).isPresent()
                && !state.ownerLeaseExpired(nowMillis)
            ? CompletableFuture.completedFuture(owner)
            : unavailable();
      }
      CommitState replacement = state.enableAuthorizationBarrier(owner.instance(), owner.epoch(), nowMillis);
      var upgraded = new CommitOwnerService.OwnerToken(owner.instance(), replacement.ownerEpoch());
      return control.compareAndSet(observed, replacement, scope)
          .handle((applied, failure) -> new CasResult(applied, failure))
          .thenCompose(result -> resolveResult(owner, upgraded, result, scope));
    }).copy();
  }

  private CompletableFuture<CommitOwnerService.OwnerToken> resolveResult(
      CommitOwnerService.OwnerToken owner, CommitOwnerService.OwnerToken upgraded,
      CasResult result, RequestScope scope) {
    if (result.failure() == null && Boolean.TRUE.equals(result.applied())) {
      return CompletableFuture.completedFuture(upgraded);
    }
    Throwable problem = result.failure() == null
        ? new BackendFailure(BackendFailure.Category.UNAVAILABLE)
        : CompletionFailures.unwrapCompletion(result.failure());
    if (result.failure() != null
        && (!(problem instanceof BackendFailure backend) || !isUnknown(backend.category()))) {
      return CompletableFuture.failedFuture(problem);
    }
    return control.read(scope).handle((after, readFailure) -> {
      if (readFailure != null) {
        Throwable readProblem = CompletionFailures.unwrapCompletion(readFailure);
        if (readProblem != problem) problem.addSuppressed(readProblem);
        throw new CompletionException(problem);
      }
      if (after.isPresent()) {
        CommitState current = after.orElseThrow().state();
        if (current.supportsAuthorizationBarrier()
            && current.ownerEpoch() >= upgraded.epoch()
            && current.ownerInstance().filter(owner.instance()::equals).isPresent()) {
          return new CommitOwnerService.OwnerToken(owner.instance(), current.ownerEpoch());
        }
      }
      throw new CompletionException(problem);
    });
  }

  private static boolean isUnknown(BackendFailure.Category category) {
    return switch (category) {
      case TIMEOUT, CANCELLED, UNAVAILABLE, UNKNOWN -> true;
      default -> false;
    };
  }

  private static CompletableFuture<CommitOwnerService.OwnerToken> unavailable() {
    return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
  }

  private record CasResult(Boolean applied, Throwable failure) {}
}
