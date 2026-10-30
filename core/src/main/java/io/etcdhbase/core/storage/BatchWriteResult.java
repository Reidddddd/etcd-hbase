package io.etcdhbase.core.storage;

import io.etcdhbase.core.error.BackendFailure;
import java.util.Objects;
import java.util.Optional;

public record BatchWriteResult(int index, State state, Optional<BackendFailure> failure) {
  public enum State { SUCCEEDED, FAILED, UNKNOWN }

  public BatchWriteResult {
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(failure, "failure");
    if (index < 0 || (state == State.SUCCEEDED) != failure.isEmpty()) {
      throw new IllegalArgumentException("invalid batch outcome");
    }
  }
}
