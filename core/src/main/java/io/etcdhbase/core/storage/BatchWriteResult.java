package io.etcdhbase.core.storage;

import io.etcdhbase.core.error.BackendFailure;
import java.util.Objects;
import java.util.Optional;

public record BatchWriteResult(int index, State state, Optional<BackendFailure> failure) {
  public enum State {
    /** The physical write was confirmed, not a global transaction commit. */
    SUCCEEDED,
    /** No backend call started, or the write was definitively rejected. */
    FAILED,
    /** A client invocation started, but its write outcome could not be confirmed. */
    UNKNOWN
  }

  public BatchWriteResult {
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(failure, "failure");
    if (index < 0 || (state == State.SUCCEEDED) != failure.isEmpty()) {
      throw new IllegalArgumentException("invalid batch outcome");
    }
  }
}
