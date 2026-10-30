package io.etcdhbase.core.request;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** Drives the actual scope deadline without sleeps or dependence on the scheduler's timer. */
public final class ManualRequestClock implements LongSupplier {
  private final AtomicLong elapsedNanos = new AtomicLong();

  public RequestScope scope(Duration timeout) {
    return new RequestScope(timeout.toNanos(), this);
  }

  public void advance(Duration elapsed) {
    elapsedNanos.addAndGet(elapsed.toNanos());
  }

  @Override
  public long getAsLong() {
    return elapsedNanos.get();
  }
}
