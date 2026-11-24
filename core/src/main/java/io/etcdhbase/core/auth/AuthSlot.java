package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/** One globally counted session slot with a generation that fences delayed creators. */
public record AuthSlot(int index, long generation, ByteKey sessionId, String operationId, State state) {
  public AuthSlot {
    Objects.requireNonNull(state, "state");
    if (index < 0 || generation < 0) {
      throw new IllegalArgumentException("auth slot index and generation must not be negative");
    }
    if (state == State.EMPTY) {
      if (sessionId != null || operationId != null) {
        throw new IllegalArgumentException("empty auth slot cannot retain a session");
      }
    } else {
      Objects.requireNonNull(sessionId, "sessionId");
      Objects.requireNonNull(operationId, "operationId");
      if (sessionId.length() != 16 || !operationId.matches("[A-Za-z0-9_-]{22}")) {
        throw new IllegalArgumentException("auth slot reservation is malformed");
      }
      if (generation < 1) {
        throw new IllegalArgumentException("reserved auth slot generation must be positive");
      }
    }
  }

  public enum State {
    EMPTY,
    RESERVED,
    ACTIVE,
    RETIRING
  }
}
