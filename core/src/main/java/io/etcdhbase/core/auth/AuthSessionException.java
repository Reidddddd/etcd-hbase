package io.etcdhbase.core.auth;

import java.util.Objects;

/** Bounded machine-readable session failures; messages never include names, tokens or secrets. */
public final class AuthSessionException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  private final Reason reason;

  public AuthSessionException(Reason reason) {
    super(Objects.requireNonNull(reason, "reason").message());
    this.reason = reason;
  }

  public Reason reason() { return reason; }

  public enum Reason {
    INVALID_CREDENTIALS("authentication credentials were rejected"),
    PERMISSION_DENIED("backend identity lacks table-group access"),
    INVALID_TOKEN("authentication token is invalid"),
    RESOURCE_LIMIT("authentication session capacity was reached"),
    DEPENDENCY_UNAVAILABLE("authentication dependency is unavailable");

    private final String message;
    Reason(String message) { this.message = message; }
    private String message() { return message; }
  }
}
