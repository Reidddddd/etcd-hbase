package io.etcdhbase.spi.identity;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Authenticates backend credentials for one configured table group.
 *
 * <p>This provider does not create or validate etcd tokens or sessions. A successful result must
 * grant access to the requested table group and report an attested credential generation, or
 * explicitly mark it unknown. Unknown generations cannot authorize durable sessions or reuse.
 * Implementations classify failures with {@link BackendIdentityException.Reason}, do not log
 * credentials, and do not expose credentials in exception messages.
 *
 * <p>{@link #authenticate} must return promptly and must not wait for backend I/O on an RPC or
 * event-loop thread. Implementations that need blocking I/O must use a bounded provider-managed
 * executor. Provider instances may be shared across requests, so implementations must be safe for
 * concurrent calls.
 *
 * <p>If authentication continues asynchronously, the provider must copy the secret before returning
 * from {@link #authenticate} and clear the copy when it is no longer needed. The caller owns and
 * closes the supplied credentials after the returned stage completes.
 *
 * <p>The provider owns contexts referenced by returned identities and all provider-created
 * resources. Callers must acquire a {@link BackendIdentityBorrow} before using an identity and
 * release it only after the physical backend work has drained. {@link #retire} rejects new borrows,
 * waits for existing borrows and late provider work, and completes only after identity-owned
 * resources have been released. A failed or unsupported retirement must remain a failure so the
 * caller can retain its capacity reservation. {@link #close()} must be idempotent, stop new
 * authentication work, release provider-owned resources, and cancel or finish provider-owned work.
 */
public interface BackendIdentityProvider extends AutoCloseable {
  String DEVELOPMENT_NO_AUTH_PROVIDER_ID = "dev-no-auth";

  /** Returns the stable ID used by runtime configuration to select this provider. */
  String providerId();

  /**
   * Authenticates credentials for the given configured scope.
   *
   * @return a non-null stage that completes with an identity or an exceptional completion carrying
   *     a {@link BackendIdentityException}
   */
  CompletionStage<AuthorizedBackendIdentity> authenticate(
      BackendCredentials credentials, AuthenticationScope scope);

  /**
   * Acquires a provider-local borrow before starting work with an authenticated identity.
   *
   * <p>This method must return promptly and perform no backend I/O. Implementations reject a
   * borrow once retirement or provider shutdown has begun. The default deliberately fails instead
   * of manufacturing a lease for providers that have not implemented borrow accounting.
   *
   * @throws UnsupportedOperationException when this provider has not implemented identity borrows
   * @throws BackendIdentityException when the identity is foreign, retired, or capacity is closed
   */
  default BackendIdentityBorrow borrow(AuthorizedBackendIdentity identity)
      throws BackendIdentityException {
    throw new UnsupportedOperationException("per-identity borrowing is not supported");
  }

  /**
   * Stops new borrows for one identity, drains prior borrows and late provider work, and releases
   * its provider-owned resources.
   *
   * <p>The returned stage must not complete successfully until cleanup has actually succeeded.
   * Implementations must retain their bounded capacity reservation after cleanup failure. The
   * default fails because a provider that has not implemented per-identity cleanup cannot claim
   * that resources were released.
   */
  default CompletionStage<Void> retire(AuthorizedBackendIdentity identity) {
    return CompletableFuture.failedFuture(
        new UnsupportedOperationException("per-identity retirement is not supported"));
  }

  /** Stops provider work and releases all provider-owned resources. */
  @Override
  void close();
}
