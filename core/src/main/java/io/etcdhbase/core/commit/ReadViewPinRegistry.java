package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Required backend boundary for registering a view before its first data read. */
@FunctionalInterface
public interface ReadViewPinRegistry {
  /** Registration must race safely with GC; an in-memory/no-op pin is not a production implementation. */
  CompletableFuture<Pin> register(ReadView view, RequestScope scope);

  /** Protects the auxiliary history at the captured sequence as well as any selected root. */
  default CompletableFuture<Pin> registerAuxiliary(ReadView view, RequestScope scope) {
    return CompletableFuture.failedFuture(new UnsupportedOperationException(
        "auxiliary snapshot protection is not available"));
  }

  interface Pin extends AutoCloseable {
    /** Monotonic generation for this registration slot. */
    long generation();

    /** Renews and fences this exact pin generation before another read operation starts. */
    default CompletableFuture<Void> renew(RequestScope scope) {
      return CompletableFuture.completedFuture(null);
    }

    /** Atomically validates and completes the pin after the full unary result is materialized. */
    default CompletableFuture<Void> complete(RequestScope scope) {
      close();
      return CompletableFuture.completedFuture(null);
    }

    /** Completion of the best-effort durable release started by {@link #close()}. */
    default CompletionStage<Void> releaseCompletion() {
      return CompletableFuture.completedFuture(null);
    }

    /** Called after every scanner and point-read producer owned by the request has stopped. */
    @Override
    void close();
  }
}
