package io.etcdhbase.grpc.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.lease.LeaseFailure;
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
  void projectsLeaseFailuresWithoutExposingTheirCause() {
    assertStatus(ErrorProjection.project(new LeaseFailure(LeaseFailure.Reason.NOT_FOUND)),
        Status.Code.NOT_FOUND, "etcdserver: requested lease not found");
    assertStatus(ErrorProjection.project(new LeaseFailure(LeaseFailure.Reason.ALREADY_EXISTS)),
        Status.Code.FAILED_PRECONDITION, "etcdserver: lease already exists");
    assertStatus(ErrorProjection.project(new LeaseFailure(LeaseFailure.Reason.TTL_TOO_LARGE)),
        Status.Code.OUT_OF_RANGE, "etcdserver: too large lease TTL");
    assertStatus(ErrorProjection.project(new LeaseFailure(LeaseFailure.Reason.CLOCK_UNHEALTHY)),
        Status.Code.UNAVAILABLE, "lease time source is unavailable");
    for (LeaseFailure.Reason reason : new LeaseFailure.Reason[] {
        LeaseFailure.Reason.RESOURCE_LIMIT, LeaseFailure.Reason.ID_SPACE_EXHAUSTED}) {
      var projected = ErrorProjection.project(new CompletionException(
          new LeaseFailure(reason, new IllegalStateException("sensitive backend detail"))));
      assertStatus(projected, Status.Code.RESOURCE_EXHAUSTED, "backend resource limit was reached");
      assertNull(projected.getCause());
    }
  }

  @Test
  void projectsKvSemanticsUsingNativeErrorIdentity() {
    assertStatus(ErrorProjection.project(new KvFailure(KvFailure.Reason.EMPTY_KEY)),
        Status.Code.INVALID_ARGUMENT, "etcdserver: key is not provided");
    assertStatus(ErrorProjection.project(new KvFailure(KvFailure.Reason.KEY_NOT_FOUND)),
        Status.Code.INVALID_ARGUMENT, "etcdserver: key not found");
    assertStatus(ErrorProjection.project(new KvFailure(KvFailure.Reason.VALUE_PROVIDED)),
        Status.Code.INVALID_ARGUMENT, "etcdserver: value is provided");
    assertStatus(ErrorProjection.project(new KvFailure(KvFailure.Reason.LEASE_PROVIDED)),
        Status.Code.INVALID_ARGUMENT, "etcdserver: lease is provided");
    assertStatus(ErrorProjection.project(new KvFailure(KvFailure.Reason.TOO_MANY_OPS)),
        Status.Code.INVALID_ARGUMENT, "etcdserver: too many operations in txn request");
    assertStatus(ErrorProjection.project(new KvFailure(KvFailure.Reason.DUPLICATE_KEY)),
        Status.Code.INVALID_ARGUMENT, "etcdserver: duplicate key given in txn request");
    assertStatus(ErrorProjection.project(new KvFailure(KvFailure.Reason.COMPACTED)),
        Status.Code.OUT_OF_RANGE, "etcdserver: mvcc: required revision has been compacted");
    assertStatus(ErrorProjection.project(new KvFailure(KvFailure.Reason.FUTURE_REVISION)),
        Status.Code.OUT_OF_RANGE, "etcdserver: mvcc: required revision is a future revision");
    assertStatus(ErrorProjection.project(new KvFailure(KvFailure.Reason.REQUEST_TOO_LARGE)),
        Status.Code.INVALID_ARGUMENT, "etcdserver: request is too large");
    assertStatus(ErrorProjection.project(new KvFailure(KvFailure.Reason.LEASE_UNAVAILABLE)),
        Status.Code.UNIMPLEMENTED, "etcd-hbase: lease binding is not available");
    assertStatus(ErrorProjection.project(new KvFailure(KvFailure.Reason.UNSUPPORTED_OPERATION)),
        Status.Code.UNIMPLEMENTED, "etcd-hbase: transaction operation is not available");
    assertStatus(ErrorProjection.project(new KvFailure(KvFailure.Reason.REQUEST_BUDGET_EXHAUSTED)),
        Status.Code.RESOURCE_EXHAUSTED, "backend resource limit was reached");
  }

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
        Status.Code.CANCELLED,
        ErrorProjection.project(new InterruptedException()).getStatus().getCode());
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
