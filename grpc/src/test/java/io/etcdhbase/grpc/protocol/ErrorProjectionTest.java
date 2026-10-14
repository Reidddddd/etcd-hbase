package io.etcdhbase.grpc.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.error.BackendFailure;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class ErrorProjectionTest {
  @Test
  void keepsAuthenticationFailureDistinctFromPermissionDenied() {
    StatusRuntimeException authentication =
        ErrorProjection.project(new BackendFailure(BackendFailure.Category.AUTHENTICATION));
    StatusRuntimeException permission =
        ErrorProjection.project(new BackendFailure(BackendFailure.Category.PERMISSION));

    assertStatus(
        authentication,
        Status.Code.INVALID_ARGUMENT,
        "etcdserver: authentication failed, invalid user ID or password");
    assertStatus(permission, Status.Code.PERMISSION_DENIED, "etcdserver: permission denied");
  }

  @Test
  void projectsBackendAvailabilityTimeoutCancellationAndResourceLimitSeparately() {
    assertStatus(
        ErrorProjection.project(new BackendFailure(BackendFailure.Category.UNAVAILABLE)),
        Status.Code.UNAVAILABLE,
        "backend is unavailable");
    assertStatus(
        ErrorProjection.project(new BackendFailure(BackendFailure.Category.TIMEOUT)),
        Status.Code.DEADLINE_EXCEEDED,
        "backend operation timed out");
    assertStatus(
        ErrorProjection.project(new BackendFailure(BackendFailure.Category.CANCELLED)),
        Status.Code.CANCELLED,
        "backend operation was cancelled");
    assertStatus(
        ErrorProjection.project(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT)),
        Status.Code.RESOURCE_EXHAUSTED,
        "backend resource limit was reached");
  }

  @Test
  void classifiesStandardTimeoutCancellationAndAdmissionExceptions() throws Exception {
    assertEquals(
        Status.Code.DEADLINE_EXCEEDED,
        ErrorProjection.project(new TimeoutException()).getStatus().getCode());
    assertEquals(
        Status.Code.CANCELLED,
        ErrorProjection.project(new CancellationException()).getStatus().getCode());
    assertEquals(
        Status.Code.RESOURCE_EXHAUSTED,
        ErrorProjection.project(new RejectedExecutionException()).getStatus().getCode());
  }

  @Test
  void unwrapsCompletionAndExecutionWrappersWithoutLosingBackendCategory() {
    BackendFailure timeout = new BackendFailure(BackendFailure.Category.TIMEOUT);
    Throwable wrapped = new CompletionException(new ExecutionException(timeout));

    assertEquals(
        Status.Code.DEADLINE_EXCEEDED,
        ErrorProjection.project(wrapped).getStatus().getCode());
  }

  @Test
  void hidesRawUnknownBackendMessagesAndDoesNotAttachTheCause() {
    StatusRuntimeException projected =
        ErrorProjection.project(new IllegalStateException("secret password must not escape"));

    assertStatus(projected, Status.Code.INTERNAL, "backend operation failed");
    assertNull(projected.getCause());
    assertFalse(projected.getStatus().getDescription().contains("secret"));
  }

  @Test
  void returnsUnimplementedForUnimplementedMethodsAndRejectsAnEmptyMethodName() {
    StatusRuntimeException projected = ErrorProjection.notImplemented("KV.Range");

    assertStatus(
        projected,
        Status.Code.UNIMPLEMENTED,
        "etcd-hbase M0: KV.Range is not implemented");
    assertThrows(IllegalArgumentException.class, () -> ErrorProjection.notImplemented(" "));
  }

  private static void assertStatus(
      StatusRuntimeException failure, Status.Code expectedCode, String expectedDescription) {
    assertEquals(expectedCode, failure.getStatus().getCode());
    assertEquals(expectedDescription, failure.getStatus().getDescription());
  }
}
