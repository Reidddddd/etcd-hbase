package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.auth.AuthSession;
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

  /** Binds read views and their cache proofs to the durable session authorization generation. */
  static ByteKey fromSessionRead(AuthorizedBackendIdentity identity, AuthSession session) {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(session, "session");
    if (session.status() != AuthSession.Status.ACTIVE
        || !identity.providerId().equals(session.providerId())
        || !identity.scope().equals(session.scope())
        || identity.credentialGeneration() != session.credentialGeneration()) {
      throw new IllegalArgumentException("read identity does not match the active session");
    }
    try {
      var bytes = new ByteArrayOutputStream();
      try (var output = new DataOutputStream(bytes)) {
        output.writeUTF("etcd-hbase/session-read-authorization/v1");
        output.write(fromIdentity(identity).bytes());
        output.writeUTF(session.trustDomain());
        output.write(session.datasetDigest().bytes());
        output.write(session.subjectDigest().bytes());
        output.writeLong(session.credentialGeneration());
        output.writeLong(session.authorizationGeneration());
      }
      return ByteKey.copyOf(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    } catch (IOException | NoSuchAlgorithmException failure) {
      throw new IllegalStateException("cannot derive the read authorization scope digest", failure);
    }
  }
}
