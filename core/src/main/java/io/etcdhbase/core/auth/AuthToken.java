package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

/** Opaque 256-bit bearer token; diagnostics intentionally never expose its value. */
public final class AuthToken implements AutoCloseable {
  public static final int TOKEN_BYTES = 32;
  public static final int DIGEST_BYTES = 32;
  private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
  private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

  private final byte[] value;
  private boolean closed;

  private AuthToken(byte[] value) {
    if (value.length != TOKEN_BYTES) throw new IllegalArgumentException("invalid token length");
    this.value = value.clone();
  }

  public static AuthToken generate(SecureRandom random) {
    Objects.requireNonNull(random, "random");
    byte[] value = new byte[TOKEN_BYTES];
    random.nextBytes(value);
    try {
      return new AuthToken(value);
    } finally {
      Arrays.fill(value, (byte) 0);
    }
  }

  /** Parses only the canonical unpadded Base64URL encoding of exactly 256 random bits. */
  public static AuthToken parse(String encoded) {
    Objects.requireNonNull(encoded, "encoded");
    if (encoded.length() != 43) throw invalidToken();
    byte[] decoded;
    try {
      decoded = DECODER.decode(encoded);
    } catch (IllegalArgumentException malformed) {
      throw invalidToken();
    }
    try {
      if (decoded.length != TOKEN_BYTES || !ENCODER.encodeToString(decoded).equals(encoded)) {
        throw invalidToken();
      }
      return new AuthToken(decoded);
    } finally {
      Arrays.fill(decoded, (byte) 0);
    }
  }

  public String valueForResponse() {
    ensureOpen();
    return ENCODER.encodeToString(value);
  }

  public ByteKey digest() {
    ensureOpen();
    try {
      return ByteKey.copyOf(MessageDigest.getInstance("SHA-256").digest(value));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      Arrays.fill(value, (byte) 0);
      closed = true;
    }
  }

  @Override
  public String toString() {
    return "AuthToken[redacted]";
  }

  private synchronized void ensureOpen() {
    if (closed) throw new IllegalStateException("auth token is closed");
  }

  private static IllegalArgumentException invalidToken() {
    return new IllegalArgumentException("authentication token has an invalid format");
  }
}
