package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.util.Objects;

/** Current per-subject authorization fence shared by all persisted sessions. */
public record SubjectAuthority(
    ByteKey subjectDigest,
    String trustDomain,
    String providerId,
    AuthenticationScope scope,
    long credentialGeneration,
    long authorizationGeneration,
    State state) {
  public SubjectAuthority {
    Objects.requireNonNull(subjectDigest, "subjectDigest");
    if (subjectDigest.length() != 32) {
      throw new IllegalArgumentException("subjectDigest must contain 32 bytes");
    }
    trustDomain = requireId(trustDomain, "trustDomain");
    providerId = requireId(providerId, "providerId");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(state, "state");
    if (credentialGeneration < 0 || authorizationGeneration < 1) {
      throw new IllegalArgumentException("subject authority generation is outside its valid range");
    }
  }

  public enum State {
    ACTIVE,
    REVOKED
  }

  private static String requireId(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!value.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException(name + " must be a bounded configured identifier");
    }
    return value;
  }
}
