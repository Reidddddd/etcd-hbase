package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Fences J40 owners before J41 begins publishing binding-aware Lease state. */
public final class LeaseBindingFormatUpgrade {
  private final CommitControlStore control;

  public LeaseBindingFormatUpgrade(CommitControlStore control) {
    this.control = Objects.requireNonNull(control, "control");
  }

  /** The caller must quiesce local work before advancing the control row's owner epoch. */
  public CompletableFuture<CommitOwnerService.OwnerToken> upgrade(
      CommitOwnerService.OwnerToken owner, long nowMillis, RequestScope scope) {
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(scope, "scope");
    return control.read(scope).thenCompose(found -> {
      ControlSnapshot observed = found.orElseThrow(() -> new IllegalStateException("missing control row"));
      CommitState replacement = observed.state().upgradeLeaseBindingFormat(
          owner.instance(), owner.epoch(), nowMillis);
      var upgraded = new CommitOwnerService.OwnerToken(owner.instance(), replacement.ownerEpoch());
      return control.compareAndSet(observed, replacement, scope)
          .handle((applied, failure) -> new CasResult(applied, failure))
          .thenCompose(result -> resolveResult(owner, upgraded, result, scope));
    }).copy();
  }

  private CompletableFuture<CommitOwnerService.OwnerToken> resolveResult(
      CommitOwnerService.OwnerToken owner, CommitOwnerService.OwnerToken upgraded,
      CasResult result, RequestScope scope) {
    if (result.failure() == null) {
      return Boolean.TRUE.equals(result.applied())
          ? CompletableFuture.completedFuture(upgraded)
          : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    Throwable problem = CompletionFailures.unwrapCompletion(result.failure());
    if (!(problem instanceof BackendFailure backend) || !isUnknown(backend.category())) {
      return CompletableFuture.failedFuture(problem);
    }
    return control.read(scope).handle((after, readFailure) -> {
      if (readFailure != null) {
        Throwable readProblem = CompletionFailures.unwrapCompletion(readFailure);
        if (readProblem != problem) problem.addSuppressed(readProblem);
        throw new CompletionException(problem);
      }
      if (after.isPresent()) {
        CommitState state = after.orElseThrow().state();
        if (state.supportsLeaseBindings()
            && state.ownerEpoch() >= upgraded.epoch()
            && state.ownerInstance().filter(owner.instance()::equals).isPresent()) {
          return new CommitOwnerService.OwnerToken(owner.instance(), state.ownerEpoch());
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

  private record CasResult(Boolean applied, Throwable failure) {}
}
