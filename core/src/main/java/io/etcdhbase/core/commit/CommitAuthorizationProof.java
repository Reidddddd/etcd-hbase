package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Captured authorization for one new publication attempt.
 *
 * <p>The identity digest stays fixed across retries. A barrier generation is attached only after
 * the revalidator has checked that captured identity against the current durable authorization
 * state. A caller must not copy an old proof onto a newer barrier generation.
 */
public final class CommitAuthorizationProof {
  @FunctionalInterface
  public interface Revalidator {
    /** Confirms the same captured authorization is current for the observed barrier generation. */
    CompletableFuture<Void> revalidate(
        ByteKey proofIdentity, long authorizationBarrierGeneration, RequestScope scope);
  }

  private final ByteKey proofIdentity;
  private final long authorizationBarrierGeneration;
  private final Revalidator revalidator;

  private CommitAuthorizationProof(
      ByteKey proofIdentity, long authorizationBarrierGeneration, Revalidator revalidator) {
    this.proofIdentity = Objects.requireNonNull(proofIdentity, "proofIdentity");
    this.revalidator = Objects.requireNonNull(revalidator, "revalidator");
    if (proofIdentity.length() != 32 || authorizationBarrierGeneration < 0) {
      throw new IllegalArgumentException("invalid commit authorization proof");
    }
    this.authorizationBarrierGeneration = authorizationBarrierGeneration;
  }

  /** Captures proof material already checked by the request or service-identity resolver. */
  public static CommitAuthorizationProof capture(
      ByteKey proofIdentity, long authorizationBarrierGeneration, Revalidator revalidator) {
    return new CommitAuthorizationProof(proofIdentity, authorizationBarrierGeneration, revalidator);
  }

  public ByteKey proofIdentity() {
    return proofIdentity;
  }

  public long authorizationBarrierGeneration() {
    return authorizationBarrierGeneration;
  }

  /**
   * Revalidates the captured identity before a selection CAS and binds the returned proof to the
   * exact barrier generation observed from the control row.
   */
  public CompletableFuture<CommitAuthorizationProof> reauthorize(
      long observedBarrierGeneration, RequestScope scope) {
    Objects.requireNonNull(scope, "scope");
    if (observedBarrierGeneration < 0) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("authorization barrier generation must be non-negative"));
    }
    CompletableFuture<Void> checked;
    try {
      checked = Objects.requireNonNull(
          revalidator.revalidate(proofIdentity, observedBarrierGeneration, scope),
          "authorization proof revalidator returned null");
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    return checked.thenApply(ignored ->
        new CommitAuthorizationProof(proofIdentity, observedBarrierGeneration, revalidator));
  }
}
