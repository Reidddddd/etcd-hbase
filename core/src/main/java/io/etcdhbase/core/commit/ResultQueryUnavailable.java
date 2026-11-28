package io.etcdhbase.core.commit;

import java.util.Objects;

/** An original result cannot be queried safely; never grants permission to execute it again. */
public final class ResultQueryUnavailable extends IllegalStateException {
  private static final long serialVersionUID = 1L;

  public enum Reason { WINDOW_CLOSED, PROTECTION_UNAVAILABLE }

  private final Reason reason;

  public ResultQueryUnavailable() {
    this(Reason.WINDOW_CLOSED);
  }

  public ResultQueryUnavailable(Reason reason) {
    super(reason == Reason.WINDOW_CLOSED ? "invocation result is no longer queryable"
        : "invocation result protection is unavailable");
    this.reason = Objects.requireNonNull(reason, "reason");
  }

  public Reason reason() { return reason; }
}
