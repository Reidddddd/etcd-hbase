package io.etcdhbase.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.grpc.Status;
import org.junit.jupiter.api.Test;

class RequestLifecycleOutcomeTest {
  @Test
  void classifiesExpectedEtcdDomainRejectionsSeparatelyFromBackendFailures() {
    assertEquals(RequestCompletionRecorder.Outcome.REQUEST_REJECTED,
        RequestLifecycle.classifyOutcome(Status.FAILED_PRECONDITION));
    assertEquals(RequestCompletionRecorder.Outcome.REQUEST_REJECTED,
        RequestLifecycle.classifyOutcome(Status.NOT_FOUND));
    assertEquals(RequestCompletionRecorder.Outcome.REQUEST_REJECTED,
        RequestLifecycle.classifyOutcome(Status.OUT_OF_RANGE));
    assertEquals(RequestCompletionRecorder.Outcome.REQUEST_REJECTED,
        RequestLifecycle.classifyOutcome(Status.ALREADY_EXISTS));
    assertEquals(RequestCompletionRecorder.Outcome.BACKEND_FAILURE,
        RequestLifecycle.classifyOutcome(Status.INTERNAL));
  }
}
