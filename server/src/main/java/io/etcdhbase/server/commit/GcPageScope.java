package io.etcdhbase.server.commit;

import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Drains a page's reads, mutations and protection cleanup before its durable receipt is written. */
final class GcPageScope {
  private final RequestScope scope;
  private final CompletableFuture<Void> drained;
  private final RequestScope.ScopedCleanup registration;

  GcPageScope(RequestScope parent) {
    scope = parent.childWithTimeoutNanos(parent.remainingDeadlineNanos());
    drained = CompletableFuture.allOf(scope.cleanupOutcomeCompletion().toCompletableFuture(),
        scope.physicalProducersCompletion().toCompletableFuture());
    registration = parent.registerScopedCleanup(scope, drained);
  }

  RequestScope scope() { return scope; }

  <T> CompletableFuture<T> finish(CompletableFuture<T> work) {
    return work.handle((value, failure) -> {
      Throwable primary = unwrap(failure);
      // Preserve synchronous cleanup failures while still waiting for cleanup and all producers.
      try { registration.close(); }
      catch (Exception | Error cleanup) { primary = append(primary, cleanup); }
      Throwable outcome = primary;
      return drained.handle((ignored, failureToDrain) -> {
        Throwable problem = append(outcome, unwrap(failureToDrain));
        if (problem != null) throw new CompletionException(problem);
        return value;
      });
    }).thenCompose(completed -> completed);
  }

  private static Throwable unwrap(Throwable failure) {
    return failure == null ? null : CompletionFailures.unwrapCompletionPreservingSuppressed(failure);
  }

  private static Throwable append(Throwable primary, Throwable additional) {
    if (primary == null) return additional;
    if (additional != null && primary != additional) primary.addSuppressed(additional);
    return primary;
  }
}
