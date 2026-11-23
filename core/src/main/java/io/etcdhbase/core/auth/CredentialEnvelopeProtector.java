package io.etcdhbase.core.auth;

/** Encrypts portable credential material without persisting a decryption key. */
public interface CredentialEnvelopeProtector {
  CredentialEnvelope encrypt(byte[] plaintext, CredentialEnvelopeContext context);

  byte[] decrypt(CredentialEnvelope envelope, CredentialEnvelopeContext context);
}
