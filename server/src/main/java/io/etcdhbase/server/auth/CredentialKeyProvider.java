package io.etcdhbase.server.auth;

/** Supplies key bytes by a non-secret configured reference; callers must clear returned arrays. */
public interface CredentialKeyProvider extends AutoCloseable {
  String activeKeyId();

  byte[] keyBytes(String keyId);

  @Override
  void close();
}
