package io.etcdhbase.server.auth;

/** A durable write outcome could not be proven; its slot must remain reserved for recovery. */
final class UnresolvedAuthSessionWrite extends RuntimeException {
  private static final long serialVersionUID = 1L;

  UnresolvedAuthSessionWrite() {
    super("authentication session write outcome is unresolved");
  }
}
