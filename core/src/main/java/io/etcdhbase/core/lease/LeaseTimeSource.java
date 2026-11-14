package io.etcdhbase.core.lease;

/** Supplies a trusted cross-process time interval; tests can inject a deterministic source. */
@FunctionalInterface
public interface LeaseTimeSource {
  LeaseTimeObservation observe();
}
