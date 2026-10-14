package io.etcdhbase.grpc;

import io.etcdhbase.core.request.RequestScope;

/** Records bounded request completion data without depending on server or backend types. */
@FunctionalInterface
public interface RequestCompletionRecorder {
  RequestCompletionRecorder NOOP =
      (methodClass, outcome, traceId, requestScope, elapsedNanos) -> {};

  void recordCompletion(
      MethodClass methodClass,
      Outcome outcome,
      String traceId,
      RequestScope requestScope,
      long elapsedNanos);

  enum MethodClass {
    AUTHENTICATION,
    KV_UNARY,
    WATCH_STREAM,
    LEASE,
    ADMINISTRATION,
    OTHER
  }

  enum Outcome {
    SUCCESS,
    CANCELLED,
    DEADLINE_EXCEEDED,
    BACKEND_FAILURE,
    UNIMPLEMENTED
  }
}
