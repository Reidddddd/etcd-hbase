package io.etcdhbase.server.config;

/** Safe startup error that never retains parser input or a low-level exception message. */
public final class RuntimeConfigurationException extends IllegalArgumentException {
  private static final long serialVersionUID = 1L;

  public RuntimeConfigurationException(String message) {
    super(message);
  }
}
