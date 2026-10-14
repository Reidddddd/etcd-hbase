package io.etcdhbase.spi.identity;

/**
 * Provider-owned, immutable backend identity handle.
 *
 * <p>Implementations may hold provider-specific connection context, but must not expose passwords,
 * tokens, or other secret material through these accessors or {@code toString()}. Callers may keep
 * the handle only while its request scope is active and must discard it when that scope ends. The
 * provider owns any referenced context; this M0 API does not yet provide a per-handle release hook.
 */
public interface BackendIdentity {
  String providerId();

  String principal();

  String tableGroupId();
}
