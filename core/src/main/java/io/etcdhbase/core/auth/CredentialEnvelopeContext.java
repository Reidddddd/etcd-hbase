package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Immutable fields authenticated as AES-GCM additional data for one persisted session. */
public record CredentialEnvelopeContext(String trustDomain, ByteKey sessionId, String providerId,
    AuthenticationScope scope, long credentialGeneration, long expiresAtEpochMillis) {
  public CredentialEnvelopeContext {
    trustDomain = requireId(trustDomain, "trustDomain");
    Objects.requireNonNull(sessionId, "sessionId");
    if (sessionId.length() != 16) throw new IllegalArgumentException("sessionId must contain 16 bytes");
    providerId = requireId(providerId, "providerId");
    Objects.requireNonNull(scope, "scope");
    if (credentialGeneration < 0 || expiresAtEpochMillis <= 0) {
      throw new IllegalArgumentException("credential envelope context is outside its valid range");
    }
  }

  /** Stable, length-prefixed AAD encoding; changing it requires a new AAD version. */
  public byte[] additionalAuthenticatedData(int version) {
    if (version != CredentialEnvelope.CURRENT_AAD_VERSION) {
      throw new IllegalArgumentException("unsupported credential-envelope AAD version");
    }
    try {
      var bytes = new ByteArrayOutputStream();
      var output = new DataOutputStream(bytes);
      output.writeInt(version);
      write(output, trustDomain);
      output.write(sessionId.bytes());
      write(output, providerId);
      write(output, scope.tableGroupId());
      write(output, scope.connectionProfileId());
      output.writeLong(credentialGeneration);
      output.writeLong(expiresAtEpochMillis);
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("in-memory AAD encoding failed", impossible);
    }
  }

  private static void write(DataOutputStream output, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    output.writeInt(bytes.length);
    output.write(bytes);
  }

  private static String requireId(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!value.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException(name + " must be a bounded configured identifier");
    }
    return value;
  }
}
