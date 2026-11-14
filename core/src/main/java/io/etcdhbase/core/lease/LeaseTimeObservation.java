package io.etcdhbase.core.lease;

/** Conservative bounds around real time, plus the health decision for using those bounds. */
public record LeaseTimeObservation(
    long nowLowerEpochMillis, long nowUpperEpochMillis, boolean healthy) {
  public LeaseTimeObservation {
    if (nowLowerEpochMillis < 0 || nowUpperEpochMillis < nowLowerEpochMillis) {
      throw new IllegalArgumentException("invalid lease time bounds");
    }
  }
}
