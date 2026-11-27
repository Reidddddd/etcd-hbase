package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.concurrent.CompletableFuture;

/** Owns a query pin until materialization is validated or a stopped request's producers drain. */
final class ResultQueryProtection {
  private final ReadViewPinRegistry.Pin pin;
  private final RequestScope scope;
  private final RequestScope.ScopedCleanup cleanup;
  private final CompletableFuture<Void> released = new CompletableFuture<>();
  private volatile boolean materialized;

  ResultQueryProtection(ReadViewPinRegistry.Pin pin, RequestScope scope) {
    this.pin = pin;
    this.scope = scope;
    cleanup = scope.registerScopedCleanup(this::release, released);
  }

  /** No storage-backed view escapes: after this CAS the complete bytes are owned in memory. */
  CompletableFuture<Void> complete() {
    return pin.complete(scope).thenRun(() -> {
      materialized = true;
      try {
        cleanup.close();
      } catch (Exception failure) {
        throw new IllegalStateException("result query protection did not close", failure);
      }
    });
  }

  private void release() {
    if (materialized) {
      released.complete(null);
      return;
    }
    scope.physicalProducersCompletion().whenComplete((ignored, failure) -> {
      if (failure != null) released.completeExceptionally(failure);
      else releasePin();
    });
  }

  private void releasePin() {
    try {
      pin.close();
    } catch (RuntimeException | Error failure) {
      // Scope cleanup must observe release failure even if a fatal error leaves this callback.
      released.completeExceptionally(failure);
      if (failure instanceof Error fatal) throw fatal;
      return;
    }
    pin.releaseCompletion().whenComplete((ignored, failure) -> {
      if (failure == null) released.complete(null);
      else released.completeExceptionally(failure);
    });
  }
}
