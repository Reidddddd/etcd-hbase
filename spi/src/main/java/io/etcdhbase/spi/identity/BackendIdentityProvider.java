package io.etcdhbase.spi.identity;

/**
 * Authenticates backend credentials for one configured table group.
 *
 * <p>This provider does not create or validate etcd tokens or sessions. It must verify access to the
 * requested table group before returning an identity and must distinguish rejected credentials,
 * insufficient permission, and backend unavailability. Implementations must not log credentials.
 *
 * <p>This M0 contract is synchronous and does not choose an executor. A caller on an RPC or other
 * latency-sensitive thread must dispatch authentication to a bounded executor if an implementation
 * can block on backend I/O. Provider instances may be shared across requests, so implementations
 * must be safe for concurrent calls.
 *
 * <p>The provider owns any backend context represented by the returned identity; callers may retain
 * the identity only for the corresponding request scope and must discard it when that scope ends.
 * This SPI does not yet define a per-identity release callback or provider shutdown hook. A
 * stateful production provider must not be added until its context cleanup and lifecycle contract
 * is defined.
 */
public interface BackendIdentityProvider {
  String providerId();

  BackendIdentity authenticate(BackendCredentials credentials, BackendIdentityRequest request)
      throws BackendIdentityException;
}
