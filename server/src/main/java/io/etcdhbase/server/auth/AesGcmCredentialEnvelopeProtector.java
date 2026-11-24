package io.etcdhbase.server.auth;

import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.auth.CredentialEnvelopeContext;
import io.etcdhbase.core.auth.CredentialEnvelopeProtector;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/** AES-256-GCM envelope protector backed by external key references. */
public final class AesGcmCredentialEnvelopeProtector implements CredentialEnvelopeProtector {
  private static final String TRANSFORMATION = "AES/GCM/NoPadding";
  private static final int TAG_BITS = CredentialEnvelope.TAG_BYTES * Byte.SIZE;
  private static final int AES_KEY_BYTES = 32;

  private final CredentialKeyProvider keys;
  private final SecureRandom random;

  public AesGcmCredentialEnvelopeProtector(CredentialKeyProvider keys, SecureRandom random) {
    this.keys = Objects.requireNonNull(keys, "keys");
    this.random = Objects.requireNonNull(random, "random");
  }

  @Override
  public CredentialEnvelope encrypt(byte[] plaintext, CredentialEnvelopeContext context) {
    Objects.requireNonNull(plaintext, "plaintext");
    Objects.requireNonNull(context, "context");
    if (plaintext.length == 0 || plaintext.length > CredentialEnvelope.MAX_CIPHERTEXT_BYTES
        - CredentialEnvelope.TAG_BYTES) {
      throw new IllegalArgumentException("credential material exceeds its size limit");
    }
    String keyId = keys.activeKeyId();
    byte[] key = keys.keyBytes(keyId);
    byte[] nonce = new byte[CredentialEnvelope.NONCE_BYTES];
    byte[] ciphertext = null;
    try {
      validateKey(key);
      random.nextBytes(nonce);
      Cipher cipher = Cipher.getInstance(TRANSFORMATION);
      cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"),
          new GCMParameterSpec(TAG_BITS, nonce));
      cipher.updateAAD(context.additionalAuthenticatedData(CredentialEnvelope.CURRENT_AAD_VERSION));
      ciphertext = cipher.doFinal(plaintext);
      return new CredentialEnvelope(keyId, CredentialEnvelope.CURRENT_AAD_VERSION,
          nonce, ciphertext);
    } catch (GeneralSecurityException failure) {
      throw new IllegalStateException("credential envelope encryption failed");
    } finally {
      Arrays.fill(key, (byte) 0);
      Arrays.fill(nonce, (byte) 0);
      if (ciphertext != null) Arrays.fill(ciphertext, (byte) 0);
    }
  }

  @Override
  public byte[] decrypt(CredentialEnvelope envelope, CredentialEnvelopeContext context) {
    Objects.requireNonNull(envelope, "envelope");
    Objects.requireNonNull(context, "context");
    byte[] key = keys.keyBytes(envelope.keyId());
    byte[] nonce = envelope.nonce();
    byte[] ciphertext = envelope.ciphertext();
    try {
      validateKey(key);
      Cipher cipher = Cipher.getInstance(TRANSFORMATION);
      cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"),
          new GCMParameterSpec(TAG_BITS, nonce));
      cipher.updateAAD(context.additionalAuthenticatedData(envelope.aadVersion()));
      byte[] plaintext = cipher.doFinal(ciphertext);
      if (plaintext.length == 0 || plaintext.length > CredentialEnvelope.MAX_CIPHERTEXT_BYTES) {
        Arrays.fill(plaintext, (byte) 0);
        throw new IllegalArgumentException("credential material exceeds its size limit");
      }
      return plaintext;
    } catch (GeneralSecurityException failure) {
      throw new IllegalArgumentException("credential envelope cannot be authenticated");
    } finally {
      Arrays.fill(key, (byte) 0);
      Arrays.fill(nonce, (byte) 0);
      Arrays.fill(ciphertext, (byte) 0);
    }
  }

  private static void validateKey(byte[] key) {
    if (key.length != AES_KEY_BYTES) {
      Arrays.fill(key, (byte) 0);
      throw new IllegalArgumentException("credential encryption key must contain 256 bits");
    }
  }
}
