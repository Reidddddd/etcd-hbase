package io.etcdhbase.server.commit;

import io.etcdhbase.core.lease.LeaseTimeObservation;
import io.etcdhbase.core.lease.LeaseTimeSource;
import java.time.Clock;
import java.util.Objects;

/** Applies an operator-supplied maximum wall-clock uncertainty to a local clock sample. */
final class BoundedSystemLeaseTimeSource implements LeaseTimeSource {
  private final Clock clock;
  private final long uncertaintyMillis;

  BoundedSystemLeaseTimeSource(Clock clock, long uncertaintyMillis) {
    this.clock = Objects.requireNonNull(clock, "clock");
    if (uncertaintyMillis <= 0 || uncertaintyMillis > 60_000) {
      throw new IllegalArgumentException("lease clock uncertainty must be between 1 and 60000 ms");
    }
    this.uncertaintyMillis = uncertaintyMillis;
  }

  @Override
  public LeaseTimeObservation observe() {
    try {
      long now = clock.millis();
      if (now < 0) return unhealthy();
      long lower = Math.max(0, Math.subtractExact(now, uncertaintyMillis));
      long upper = Math.addExact(now, uncertaintyMillis);
      return new LeaseTimeObservation(lower, upper, true);
    } catch (RuntimeException unhealthyClock) {
      return unhealthy();
    }
  }

  private static LeaseTimeObservation unhealthy() {
    return new LeaseTimeObservation(0, 0, false);
  }
}
