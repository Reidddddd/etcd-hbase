package io.etcdhbase.server.auth;

import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.util.concurrent.CompletionStage;

/** Creates and restores real backend identities with per-session resource ownership. */
public interface SessionIdentityManager extends AutoCloseable {
  CompletionStage<AuthorizedBackendIdentity> authenticate(
      BackendCredentials credentials, AuthenticationScope scope);

  /** Verifies that the just-created session can be independently recreated from its credentials. */
  CompletionStage<Void> verifyRestorable(
      BackendCredentials credentials, AuthorizedBackendIdentity authenticatedIdentity);

  /** Activates a bounded request borrow from a valid persisted session. */
  CompletionStage<SessionIdentityLease> activate(
      AuthSession session, BackendCredentials credentials);

  /** Releases resources for an identity whose token was never delivered. */
  CompletionStage<Void> discardUnpublished(AuthorizedBackendIdentity identity);

  @Override
  void close();
}
