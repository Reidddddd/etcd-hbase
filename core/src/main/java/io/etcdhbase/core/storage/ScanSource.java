package io.etcdhbase.core.storage;

/** Starts one nonblocking finite page; the returned handle owns its result until closed. */
@FunctionalInterface
public interface ScanSource {
  ScanOperation startScan(ScanRequest request);
}
