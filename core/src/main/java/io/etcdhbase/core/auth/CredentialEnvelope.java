package io.etcdhbase.core.auth;

import java.util.Arrays;
import java.util.Objects;

/** Versioned AES-GCM envelope metadata stored beside one auth session. */
public final class CredentialEnvelope {
  public static final int CURRENT_AAD_VERSION = 1;
  public static final int NONCE_BYTES = 12;
  public static final int TAG_BYTES = 16;
  public static final int MAX_CIPHERTEXT_BYTES = 64 * 1024;

  private final String keyId;
  private final int aadVersion;
  private final byte[] nonce;
  private final byte[] ciphertext;

  public CredentialEnvelope(String keyId, int aadVersion, byte[] nonce, byte[] ciphertext) {
    this.keyId = requireKeyId(keyId);
    if (aadVersion != CURRENT_AAD_VERSION) {
      throw new IllegalArgumentException("unsupported credential-envelope AAD version");
    }
    Objects.requireNonNull(nonce, "nonce");
    Objects.requireNonNull(ciphertext, "ciphertext");
    if (nonce.length != NONCE_BYTES || ciphertext.length < TAG_BYTES
        || ciphertext.length > MAX_CIPHERTEXT_BYTES) {
      throw new IllegalArgumentException("credential envelope is outside its size limit");
    }
    this.aadVersion = aadVersion;
    this.nonce = nonce.clone();
    this.ciphertext = ciphertext.clone();
  }

  public String keyId() {
    return keyId;
  }

  public int aadVersion() {
    return aadVersion;
  }

  public byte[] nonce() {
    return nonce.clone();
  }

  public byte[] ciphertext() {
    return ciphertext.clone();
  }

  @Override
  public String toString() {
    return "CredentialEnvelope[keyId=" + keyId + ", aadVersion=" + aadVersion
        + ", nonce=redacted, ciphertext=redacted]";
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof CredentialEnvelope envelope
        && keyId.equals(envelope.keyId)
        && aadVersion == envelope.aadVersion
        && Arrays.equals(nonce, envelope.nonce)
        && Arrays.equals(ciphertext, envelope.ciphertext);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(keyId, aadVersion);
    result = 31 * result + Arrays.hashCode(nonce);
    return 31 * result + Arrays.hashCode(ciphertext);
  }

  private static String requireKeyId(String value) {
    Objects.requireNonNull(value, "keyId");
    if (!value.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException("keyId must be a bounded configured identifier");
    }
    return value;
  }
}
