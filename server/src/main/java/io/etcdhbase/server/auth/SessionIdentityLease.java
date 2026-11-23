package io.etcdhbase.server.auth;

import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** One request's independently releasable borrow of a provider-owned identity context. */
public final class SessionIdentityLease {
  private final AuthorizedBackendIdentity identity;
  private final Supplier<? extends CompletionStage<Void>> releaser;
  private final AtomicReference<CompletableFuture<Void>> release = new AtomicReference<>();

  public SessionIdentityLease(AuthorizedBackendIdentity identity,
      Supplier<? extends CompletionStage<Void>> releaser) {
    this.identity = Objects.requireNonNull(identity, "identity");
    this.releaser = Objects.requireNonNull(releaser, "releaser");
  }

  /** Binds this lease to the provider that owns the identity and its connection resources. */
  public static SessionIdentityLease fromProvider(
      HBaseBackendIdentityProvider provider, AuthorizedBackendIdentity identity) {
    Objects.requireNonNull(provider, "provider");
    Objects.requireNonNull(identity, "identity");
    return new SessionIdentityLease(identity, () -> provider.release(identity));
  }

  public AuthorizedBackendIdentity identity() { return identity; }

  /**
   * Releases this borrow once. The provider remains open and owns any shared-resource reference
   * counting; callers should invoke this only after their physical request work has drained.
   */
  public CompletionStage<Void> release() {
    CompletableFuture<Void> completion = release.get();
    if (completion == null) {
      CompletableFuture<Void> created = new CompletableFuture<>();
      if (release.compareAndSet(null, created)) {
        completion = created;
        try {
          Objects.requireNonNull(releaser.get(), "identity release stage")
              .whenComplete((ignored, failure) -> {
                if (failure == null) created.complete(null);
                else created.completeExceptionally(failure);
              });
        } catch (RuntimeException failure) {
          created.completeExceptionally(failure);
        } catch (Error fatal) {
          created.completeExceptionally(fatal);
          throw fatal;
        }
      } else {
        completion = release.get();
      }
    }
    return completion.minimalCompletionStage();
  }
}
