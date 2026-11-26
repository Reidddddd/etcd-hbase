package io.etcdhbase.spi.identity;

import java.util.concurrent.CompletionStage;

/**
 * One caller's bounded right to use a provider-owned identity.
 *
 * <p>The caller owns this borrow and must release it only after all physical backend work using
 * the identity has drained. {@link #release()} is idempotent. Completion means this borrow no
 * longer prevents retirement; it does not mean that the identity or its backend resources have
 * been retired.
 */
public interface BackendIdentityBorrow {
  /** The exact provider-issued identity protected by this borrow. */
  AuthorizedBackendIdentity identity();

  /** Releases this borrow after its backend work has physically completed. */
  CompletionStage<Void> release();
}
