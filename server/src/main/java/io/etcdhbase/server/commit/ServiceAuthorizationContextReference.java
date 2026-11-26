package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Builds a stable, non-secret label for the configured table-group service identity.
 *
 * <p>This is not a credential and does not authorize a request by itself. The receiver must still
 * authenticate the mTLS peer and validate the table group and authorization-scope digest. M3
 * currently admits only its single DEV_NO_AUTH service identity per table group; a runtime that
 * admits per-user or credentialed contexts must replace this mapping with provider-backed context
 * resolution before enabling that profile.
 */
public final class ServiceAuthorizationContextReference {
  private static final byte[] DOMAIN =
      "etcd-hbase/m3/service-authorization-context/v1".getBytes(StandardCharsets.US_ASCII);

  private ServiceAuthorizationContextReference() {}

  public static ByteKey create(AuthorizedBackendIdentity identity, ByteKey authorizationScopeDigest) {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(authorizationScopeDigest, "authorizationScopeDigest");
    if (authorizationScopeDigest.length() != 32
        || !identity.allowedTableGroupIds().contains(identity.scope().tableGroupId())) {
      throw new IllegalArgumentException("invalid service authorization context");
    }
    byte[] provider = identity.providerId().getBytes(StandardCharsets.UTF_8);
    byte[] tableGroup = identity.scope().tableGroupId().getBytes(StandardCharsets.UTF_8);
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(DOMAIN);
      updateField(digest, provider);
      updateField(digest, tableGroup);
      updateField(digest, authorizationScopeDigest.bytes());
      return ByteKey.copyOf(digest.digest());
    } catch (NoSuchAlgorithmException impossible) {
      throw new AssertionError("the JDK must provide SHA-256", impossible);
    }
  }

  public static boolean matches(AuthorizedBackendIdentity identity, ByteKey authorizationScopeDigest,
      ByteKey candidate) {
    Objects.requireNonNull(candidate, "candidate");
    return candidate.equals(create(identity, authorizationScopeDigest));
  }

  /** Captures a configured service identity for one observed table-group barrier generation. */
  public static CommitAuthorizationProof publicationProof(
      AuthorizedBackendIdentity identity, ByteKey authorizationScopeDigest,
      long authorizationBarrierGeneration) {
    ByteKey expected = create(identity, authorizationScopeDigest);
    return CommitAuthorizationProof.capture(expected, authorizationBarrierGeneration,
        (proofIdentity, barrierGeneration, scope) -> proofIdentity.equals(expected)
            ? CompletableFuture.completedFuture(null)
            : CompletableFuture.failedFuture(
                new BackendFailure(BackendFailure.Category.PERMISSION)));
  }

  private static void updateField(MessageDigest digest, byte[] value) {
    digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value.length).array());
    digest.update(value);
  }
}
