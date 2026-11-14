package io.etcdhbase.core.lease;

import java.util.Objects;

/** Bounded Lease domain failures whose public gRPC projection is owned by the protocol module. */
public final class LeaseFailure extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public enum Reason {
    NOT_FOUND,
    ALREADY_EXISTS,
    TTL_TOO_LARGE,
    CLOCK_UNHEALTHY,
    RESOURCE_LIMIT,
    ID_SPACE_EXHAUSTED
  }

  private final Reason reason;

  public LeaseFailure(Reason reason) {
    super(message(Objects.requireNonNull(reason, "reason")));
    this.reason = reason;
  }

  public LeaseFailure(Reason reason, Throwable cause) {
    super(message(Objects.requireNonNull(reason, "reason")), cause);
    this.reason = reason;
  }

  public Reason reason() { return reason; }

  private static String message(Reason reason) {
    return switch (reason) {
      case NOT_FOUND -> "lease does not exist";
      case ALREADY_EXISTS -> "lease ID already exists";
      case TTL_TOO_LARGE -> "lease TTL exceeds the configured limit";
      case CLOCK_UNHEALTHY -> "lease time source is unhealthy";
      case RESOURCE_LIMIT -> "lease operation exceeds its resource budget";
      case ID_SPACE_EXHAUSTED -> "lease ID allocation did not find a free ID";
    };
  }
}
