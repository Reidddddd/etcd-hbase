package io.etcdhbase.spi.identity;

import java.util.Objects;

/** Safe, machine-readable failure categories returned by a backend identity provider. */
public final class BackendIdentityException extends Exception {
  private static final long serialVersionUID = 1L;

  private final Reason reason;

  public BackendIdentityException(Reason reason) {
    super(Objects.requireNonNull(reason, "reason").safeMessage());
    this.reason = reason;
  }

  public Reason reason() {
    return reason;
  }

  public enum Reason {
    INVALID_CREDENTIALS("backend credentials were rejected"),
    INSUFFICIENT_PERMISSION("backend identity lacks table-group access"),
    BACKEND_UNAVAILABLE("backend identity could not be verified");

    private final String safeMessage;

    Reason(String safeMessage) {
      this.safeMessage = safeMessage;
    }

    private String safeMessage() {
      return safeMessage;
    }
  }
}
