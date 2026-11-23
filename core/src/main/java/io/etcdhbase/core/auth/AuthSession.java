package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.util.Objects;

/** Persisted authorization session; bearer tokens and backend handles are never part of it. */
public record AuthSession(
    ByteKey sessionId,
    ByteKey tokenDigest,
    ByteKey subjectDigest,
    String trustDomain,
    String providerId,
    AuthenticationScope scope,
    ByteKey datasetDigest,
    long credentialGeneration,
    long authorizationGeneration,
    long createdAtEpochMillis,
    long expiresAtEpochMillis,
    int slotIndex,
    long slotGeneration,
    Status status,
    CredentialEnvelope credentialEnvelope) {
  public AuthSession {
    requireBytes(sessionId, 16, "sessionId");
    requireBytes(tokenDigest, 32, "tokenDigest");
    requireBytes(subjectDigest, 32, "subjectDigest");
    trustDomain = requireId(trustDomain, "trustDomain");
    providerId = requireId(providerId, "providerId");
    Objects.requireNonNull(scope, "scope");
    requireBytes(datasetDigest, 32, "datasetDigest");
    Objects.requireNonNull(status, "status");
    Objects.requireNonNull(credentialEnvelope, "credentialEnvelope");
    if (credentialGeneration < 0 || authorizationGeneration < 1 || createdAtEpochMillis <= 0
        || expiresAtEpochMillis <= createdAtEpochMillis || slotIndex < 0 || slotGeneration < 1) {
      throw new IllegalArgumentException("auth session fields are outside their valid range");
    }
  }

  public boolean isActiveAt(long nowEpochMillis) {
    return status == Status.ACTIVE && nowEpochMillis < expiresAtEpochMillis;
  }

  private static void requireBytes(ByteKey value, int length, String name) {
    Objects.requireNonNull(value, name);
    if (value.length() != length) {
      throw new IllegalArgumentException(name + " has an invalid length");
    }
  }

  private static String requireId(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!value.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException(name + " must be a bounded configured identifier");
    }
    return value;
  }

  public enum Status {
    PREPARING,
    ACTIVE,
    RETIRING,
    REVOKED,
    EXPIRED
  }
}
