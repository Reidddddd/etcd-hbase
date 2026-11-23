package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/** Stable authorization fence derived from the provider-issued backend identity. */
final class AuthorizationScopeDigest {
  private AuthorizationScopeDigest() {}

  static ByteKey fromIdentity(AuthorizedBackendIdentity identity) {
    Objects.requireNonNull(identity, "identity");
    if (identity.allowedTableGroupIds().size() > 64) {
      throw new IllegalArgumentException("identity has too many authorized table groups");
    }
    try {
      var bytes = new ByteArrayOutputStream();
      try (var output = new DataOutputStream(bytes)) {
        output.writeInt(1);
        output.writeUTF(identity.providerId());
        output.writeUTF(identity.subject());
        output.writeUTF(identity.scope().tableGroupId());
        output.writeUTF(identity.scope().connectionProfileId());
        output.writeLong(identity.credentialGeneration());
        output.writeInt(identity.allowedTableGroupIds().size());
        for (String group : identity.allowedTableGroupIds().stream().sorted().toList()) {
          output.writeUTF(group);
        }
      }
      return ByteKey.copyOf(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    } catch (IOException | NoSuchAlgorithmException failure) {
      throw new IllegalStateException("cannot derive the authorization scope digest", failure);
    }
  }
}
