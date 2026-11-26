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
  private final Supplier<? extends CompletionStage<Void>> retirer;
  private final AtomicReference<CompletableFuture<Void>> release = new AtomicReference<>();
  private final AtomicReference<CompletableFuture<Void>> retirement = new AtomicReference<>();

  public SessionIdentityLease(AuthorizedBackendIdentity identity,
      Supplier<? extends CompletionStage<Void>> releaser) {
    this(identity, releaser, () -> CompletableFuture.failedFuture(
        new UnsupportedOperationException("identity retirement is unavailable")));
  }

  public SessionIdentityLease(AuthorizedBackendIdentity identity,
      Supplier<? extends CompletionStage<Void>> releaser,
      Supplier<? extends CompletionStage<Void>> retirer) {
    this.identity = Objects.requireNonNull(identity, "identity");
    this.releaser = Objects.requireNonNull(releaser, "releaser");
    this.retirer = Objects.requireNonNull(retirer, "retirer");
  }

  /** Binds this lease to the provider that owns the identity and its connection resources. */
  public static SessionIdentityLease fromProvider(
      HBaseBackendIdentityProvider provider, AuthorizedBackendIdentity identity) {
    Objects.requireNonNull(provider, "provider");
    Objects.requireNonNull(identity, "identity");
    return new SessionIdentityLease(identity, () -> provider.release(identity),
        () -> provider.retire(identity));
  }

  public AuthorizedBackendIdentity identity() { return identity; }

  /** Stops new borrows immediately; completion waits for the provider's physical retirement. */
  public CompletionStage<Void> retire() {
    CompletableFuture<Void> completion = retirement.get();
    if (completion == null) {
      CompletableFuture<Void> created = new CompletableFuture<>();
      if (retirement.compareAndSet(null, created)) {
        completion = created;
        try {
          Objects.requireNonNull(retirer.get(), "identity retirement stage")
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
        completion = retirement.get();
      }
    }
    return completion.minimalCompletionStage();
  }

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
