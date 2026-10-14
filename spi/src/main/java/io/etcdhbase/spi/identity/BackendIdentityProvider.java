package io.etcdhbase.spi.identity;

import java.util.concurrent.CompletionStage;

/**
 * Authenticates backend credentials for one configured table group.
 *
 * <p>This provider does not create or validate etcd tokens or sessions. A successful result must
 * grant access to the requested table group and report the provider's credential generation.
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
 * resources. The identity may be used only during its owning authorization session. This SPI has
 * no per-identity release callback, so providers must bound or expire retained context state.
 * {@link #close()} must be idempotent, stop new authentication work, release provider-owned
 * resources, and cancel or finish provider-owned work.
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

  /** Stops provider work and releases all provider-owned resources. */
  @Override
  void close();
}
