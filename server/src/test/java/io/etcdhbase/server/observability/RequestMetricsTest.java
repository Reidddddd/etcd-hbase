package io.etcdhbase.server.observability;

import io.etcdhbase.core.request.RequestScope;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static io.etcdhbase.grpc.RequestCompletionRecorder.MethodClass.KV_UNARY;
import static io.etcdhbase.grpc.RequestCompletionRecorder.Outcome.BACKEND_FAILURE;
import static io.etcdhbase.grpc.RequestCompletionRecorder.Outcome.SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RequestMetricsTest {
  @Test
  void countsOnlyBoundedMethodAndOutcomeDimensions() {
    RequestMetrics metrics = new RequestMetrics();
    RequestScope scope = RequestScope.withoutDeadline();

    metrics.recordCompletion(KV_UNARY, SUCCESS, "trace-a", scope, 11);
    metrics.recordCompletion(KV_UNARY, SUCCESS, "trace-b", scope, 17);
    metrics.recordCompletion(KV_UNARY, BACKEND_FAILURE, "trace-c", scope, 19);
    Map<RequestMetrics.Series, RequestMetrics.Snapshot> snapshot = metrics.snapshot();

    assertEquals(6 * 5, snapshot.size());
    assertEquals(2, snapshot.get(new RequestMetrics.Series(KV_UNARY, SUCCESS)).completed());
    assertEquals(
        28,
        snapshot.get(new RequestMetrics.Series(KV_UNARY, SUCCESS)).totalElapsedNanos());
    assertEquals(1, snapshot.get(new RequestMetrics.Series(KV_UNARY, BACKEND_FAILURE)).completed());
  }

  @Test
  void rejectsNegativeDurations() {
    RequestMetrics metrics = new RequestMetrics();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            metrics.recordCompletion(
                KV_UNARY, SUCCESS, "trace", RequestScope.withoutDeadline(), -1));
  }
}
