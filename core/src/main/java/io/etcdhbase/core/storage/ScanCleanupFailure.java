package io.etcdhbase.core.storage;

/** Local scanner cleanup failed; its capacity must be retired rather than reused. */
public final class ScanCleanupFailure extends Exception {
  private static final long serialVersionUID = 1L;

  public ScanCleanupFailure(Throwable cause) { super("physical scan cleanup failed", cause); }
}
