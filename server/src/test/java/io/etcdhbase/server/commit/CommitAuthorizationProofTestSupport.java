package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.concurrent.CompletableFuture;

/** Small proof fixtures for tests that mock the durable authorization resolver. */
public final class CommitAuthorizationProofTestSupport {
  private static final ByteKey TEST_PROOF_IDENTITY = ByteKey.copyOf(new byte[32]);

  private CommitAuthorizationProofTestSupport() {}

  public static CommitAuthorizationProof alwaysValid(long barrierGeneration) {
    return CommitAuthorizationProof.capture(TEST_PROOF_IDENTITY, barrierGeneration,
        (identity, generation, scope) -> CompletableFuture.completedFuture(null));
  }

  public static MutationForwardReceiver.VerifiedAuthorization verified(
      AuthorizedBackendIdentity identity, ByteKey authorizationDigest, long barrierGeneration) {
    return new MutationForwardReceiver.VerifiedAuthorization(identity, authorizationDigest,
        serviceProof(identity, authorizationDigest, barrierGeneration));
  }

  public static CommitAuthorizationProof serviceProof(
      AuthorizedBackendIdentity identity, ByteKey authorizationDigest, long barrierGeneration) {
    return ServiceAuthorizationContextReference.publicationProof(
        identity, authorizationDigest, barrierGeneration);
  }
}
