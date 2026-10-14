package io.etcdhbase.server.observability;

import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.RequestCompletionRecorder;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.etcdhbase.grpc.RequestCompletionRecorder.MethodClass;
import static io.etcdhbase.grpc.RequestCompletionRecorder.Outcome;

/** Bounded request counters and safe completion logging with no caller-controlled labels. */
public final class RequestMetrics implements RequestCompletionRecorder {
  private static final Logger LOGGER = LoggerFactory.getLogger(RequestMetrics.class);

  private final Map<Series, Counter> counters;

  public RequestMetrics() {
    Map<Series, Counter> initialized = new HashMap<>();
    for (MethodClass methodClass : MethodClass.values()) {
      for (Outcome outcome : Outcome.values()) {
        initialized.put(new Series(methodClass, outcome), new Counter());
      }
    }
    counters = Map.copyOf(initialized);
  }

  @Override
  public void recordCompletion(
      MethodClass methodClass,
      Outcome outcome,
      String traceId,
      RequestScope scope,
      long elapsedNanos) {
    Objects.requireNonNull(methodClass, "methodClass");
    Objects.requireNonNull(outcome, "outcome");
    Objects.requireNonNull(traceId, "traceId");
    if (traceId.isBlank()) {
      throw new IllegalArgumentException("traceId must not be blank");
    }
    Objects.requireNonNull(scope, "scope");
    if (elapsedNanos < 0) {
      throw new IllegalArgumentException("elapsedNanos must not be negative");
    }

    Series series = new Series(methodClass, outcome);
    Counter counter = counters.get(series);
    counter.completed.increment();
    counter.totalElapsedNanos.add(elapsedNanos);

    long remainingNanos = scope.hasDeadline() ? scope.remainingDeadlineNanos() : -1;
    if (outcome != Outcome.SUCCESS) {
      LOGGER.warn(
          "request_complete method_class={} outcome={} elapsed_nanos={} deadline_remaining_nanos={} trace_id={}",
          methodClass,
          outcome,
          elapsedNanos,
          remainingNanos,
          traceId);
    } else {
      LOGGER.debug(
          "request_complete method_class={} outcome={} elapsed_nanos={} deadline_remaining_nanos={} trace_id={}",
          methodClass,
          outcome,
          elapsedNanos,
          remainingNanos,
          traceId);
    }
  }

  public Map<Series, Snapshot> snapshot() {
    Map<Series, Snapshot> snapshot = new HashMap<>();
    counters.forEach(
        (series, counter) ->
            snapshot.put(
                series,
                new Snapshot(counter.completed.sum(), counter.totalElapsedNanos.sum())));
    return Map.copyOf(snapshot);
  }

  public record Series(MethodClass methodClass, Outcome outcome) {
    public Series {
      Objects.requireNonNull(methodClass, "methodClass");
      Objects.requireNonNull(outcome, "outcome");
    }
  }

  public record Snapshot(long completed, long totalElapsedNanos) {}

  private static final class Counter {
    private final LongAdder completed = new LongAdder();
    private final LongAdder totalElapsedNanos = new LongAdder();
  }
}
