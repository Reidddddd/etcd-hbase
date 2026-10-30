package io.etcdhbase.core.storage;

import java.util.concurrent.CompletionStage;

/** One finite physical read. Completion follows local scanner and callback cleanup. */
public interface ScanOperation extends AutoCloseable {
  CompletionStage<StoragePage> completion();

  /** Cancels an unfinished read or releases its delivered page; I/O capacity follows completion. */
  @Override
  void close();
}
