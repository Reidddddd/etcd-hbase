package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeSource;
import java.time.Duration;
import java.util.Objects;

/** Configured minimum from the first durable GC eligibility observation, independent of floors. */
public record ResultWindowRetention(Duration minimum, LeaseTimeSource time) {
  public static final Duration DEFAULT_MINIMUM = Duration.ofMinutes(30);

  public ResultWindowRetention {
    Objects.requireNonNull(minimum, "minimum");
    Objects.requireNonNull(time, "time");
    if (minimum.isNegative() || minimum.isZero() || minimum.compareTo(Duration.ofDays(30)) > 0
        || !minimum.equals(Duration.ofMillis(minimum.toMillis()))) {
      throw new IllegalArgumentException("invalid invocation result retention duration");
    }
  }

  /** Without a configured trusted clock no window can acquire or satisfy an expiry witness. */
  static ResultWindowRetention unconfigured() {
    return new ResultWindowRetention(DEFAULT_MINIMUM, () -> new LeaseTimeObservation(0, 0, false));
  }
}
