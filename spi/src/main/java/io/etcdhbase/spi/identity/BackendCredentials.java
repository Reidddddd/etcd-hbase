package io.etcdhbase.spi.identity;

import java.util.Arrays;
import java.util.Objects;

/** Credentials submitted to a backend identity provider. The secret is never exposed as a String. */
public final class BackendCredentials implements AutoCloseable {
  private final String principal;
  private char[] secret;
  private boolean closed;

  public BackendCredentials(String principal, char[] secret) {
    this.principal = requireText(principal, "principal");
    this.secret = Objects.requireNonNull(secret, "secret").clone();
  }

  public String principal() {
    return principal;
  }

  /** Returns a copy that the caller must clear after use. */
  public synchronized char[] copySecret() {
    ensureOpen();
    return secret.clone();
  }

  public synchronized boolean isClosed() {
    return closed;
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      Arrays.fill(secret, '\0');
      closed = true;
    }
  }

  @Override
  public String toString() {
    return "BackendCredentials[redacted]";
  }

  private synchronized void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("credentials have been closed");
    }
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
