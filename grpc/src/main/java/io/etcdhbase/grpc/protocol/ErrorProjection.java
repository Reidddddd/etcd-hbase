package io.etcdhbase.grpc.protocol;

import io.etcdhbase.core.error.BackendFailure;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

/** Projects backend failures to bounded gRPC errors without exposing raw backend messages. */
public final class ErrorProjection {
  private static final String AUTHENTICATION_MESSAGE =
      "etcdserver: authentication failed, invalid user ID or password";
  private static final String PERMISSION_MESSAGE = "etcdserver: permission denied";

  private ErrorProjection() {}

  public static StatusRuntimeException project(Throwable failure) {
    Throwable cause = unwrap(Objects.requireNonNull(failure, "failure"));
    if (cause instanceof BackendFailure backendFailure) {
      return projectBackendFailure(backendFailure);
    }
    if (cause instanceof CancellationException || cause instanceof InterruptedException) {
      return status(
          Status.CANCELLED, safeMessage(BackendFailure.Category.CANCELLED));
    }
    if (cause instanceof TimeoutException) {
      return status(Status.DEADLINE_EXCEEDED, safeMessage(BackendFailure.Category.TIMEOUT));
    }
    if (cause instanceof RejectedExecutionException) {
      return status(
          Status.RESOURCE_EXHAUSTED, safeMessage(BackendFailure.Category.RESOURCE_LIMIT));
    }
    return status(Status.INTERNAL, safeMessage(BackendFailure.Category.UNKNOWN));
  }

  public static StatusRuntimeException notImplemented(String method) {
    if (method == null || method.isBlank()) {
      throw new IllegalArgumentException("method must not be blank");
    }
    return Status.UNIMPLEMENTED
        .withDescription("etcd-hbase M0: " + method + " is not implemented")
        .asRuntimeException();
  }

  private static StatusRuntimeException projectBackendFailure(BackendFailure failure) {
    return switch (failure.category()) {
      case AUTHENTICATION -> status(Status.INVALID_ARGUMENT, AUTHENTICATION_MESSAGE);
      case PERMISSION -> status(Status.PERMISSION_DENIED, PERMISSION_MESSAGE);
      case UNAVAILABLE -> status(Status.UNAVAILABLE, failure.safeMessage());
      case TIMEOUT -> status(Status.DEADLINE_EXCEEDED, failure.safeMessage());
      case CANCELLED -> status(Status.CANCELLED, failure.safeMessage());
      case RESOURCE_LIMIT -> status(Status.RESOURCE_EXHAUSTED, failure.safeMessage());
      case UNKNOWN -> status(Status.INTERNAL, failure.safeMessage());
    };
  }

  private static StatusRuntimeException status(Status status, String description) {
    return status.withDescription(description).asRuntimeException();
  }

  private static String safeMessage(BackendFailure.Category category) {
    return new BackendFailure(category).safeMessage();
  }

  private static Throwable unwrap(Throwable failure) {
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    Throwable current = failure;
    while (seen.add(current)) {
      if (!(current instanceof CompletionException || current instanceof ExecutionException)
          || current.getCause() == null) {
        return current;
      }
      current = current.getCause();
    }
    return current;
  }
}
