package io.etcdhbase.server.auth;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/** Derives non-secret identity keys from an already authenticated provider result. */
final class SubjectIdentityDigests {
  private SubjectIdentityDigests() {}

  static ByteKey family(String trustDomain, String backendIdentityDomain,
      AuthorizedBackendIdentity identity) {
    requireId(trustDomain);
    requireId(backendIdentityDomain);
    Objects.requireNonNull(identity, "identity");
    return digest("etcd-hbase-auth-family-v1", trustDomain, identity.providerId(),
        backendIdentityDomain, identity.subject());
  }

  static ByteKey scoped(String trustDomain, String providerId, String backendIdentityDomain,
      AuthenticationScope scope, String verifiedSubject) {
    requireId(trustDomain);
    requireId(backendIdentityDomain);
    Objects.requireNonNull(scope, "scope");
    // Credential generations are comparable only within this explicitly configured family.
    return digest("etcd-hbase-auth-subject-v2", trustDomain, providerId, backendIdentityDomain,
        scope.tableGroupId(), scope.connectionProfileId(), verifiedSubject);
  }

  private static ByteKey digest(String prefix, String... values) {
    try {
      var bytes = new ByteArrayOutputStream();
      var output = new DataOutputStream(bytes);
      output.write(prefix.getBytes(StandardCharsets.UTF_8));
      for (String value : values) {
        byte[] encoded = Objects.requireNonNull(value, "identity field").getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
      }
      output.flush();
      return ByteKey.copyOf(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    } catch (IOException | NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("subject digest is unavailable", impossible);
    }
  }

  private static void requireId(String id) {
    if (id == null || !id.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException("invalid subject family domain");
    }
  }
}
