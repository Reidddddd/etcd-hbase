package io.etcdhbase.core.storage;

import java.util.concurrent.CompletionStage;

/** One finite physical read. Completion follows local scanner and callback cleanup. */
public interface ScanOperation extends AutoCloseable {
  CompletionStage<StoragePage> completion();

  /** Requests cancellation; callers must still await completion before releasing I/O capacity. */
  @Override
  void close();
}
