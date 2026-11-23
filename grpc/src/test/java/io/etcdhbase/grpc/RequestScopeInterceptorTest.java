package io.etcdhbase.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import etcdserverpb.Rpc;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RequestScopeInterceptorTest {
  @Test
  void bidiStreamCanOutliveDefaultRequestTimeoutUntilCancelled() throws Exception {
    var scheduler = new ScheduledThreadPoolExecutor(1);
    var interceptor = new RequestScopeInterceptor(scheduler, 1,
        Duration.ofMillis(40), 1024, RequestCompletionRecorder.NOOP);
    try {
      var call = newCall(MethodDescriptor.MethodType.BIDI_STREAMING);
      var terminalStatus = new AtomicReference<Status>();
      var terminated = new CountDownLatch(1);
      observeClose(call, terminalStatus, terminated);
      ServerCall.Listener<byte[]> listener = interceptor.interceptCall(call, new Metadata(),
          emptyHandler());

      assertFalse(terminated.await(150, TimeUnit.MILLISECONDS),
          "the default unary deadline must not close a long-lived bidi stream");
      listener.onCancel();
      interceptor.close();
      assertEquals(0, interceptor.activeRequests());
    } finally {
      interceptor.close();
      scheduler.shutdownNow();
      assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void unaryCallsStillUseTheConfiguredRequestTimeout() throws Exception {
    var scheduler = new ScheduledThreadPoolExecutor(1);
    var interceptor = new RequestScopeInterceptor(scheduler, 1,
        Duration.ofMillis(40), 1024, RequestCompletionRecorder.NOOP);
    try {
      var call = newCall(MethodDescriptor.MethodType.UNARY);
      var terminalStatus = new AtomicReference<Status>();
      var terminated = new CountDownLatch(1);
      observeClose(call, terminalStatus, terminated);
      interceptor.interceptCall(call, new Metadata(), emptyHandler());

      assertTrue(terminated.await(2, TimeUnit.SECONDS), "unary call did not honor its deadline");
      assertEquals(Status.Code.DEADLINE_EXCEEDED, terminalStatus.get().getCode());
    } finally {
      interceptor.close();
      scheduler.shutdownNow();
      assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void bidiStreamStillHonorsAnExplicitClientDeadline() throws Exception {
    var scheduler = new ScheduledThreadPoolExecutor(1);
    var interceptor = new RequestScopeInterceptor(scheduler, 1,
        Duration.ofSeconds(1), 1024, RequestCompletionRecorder.NOOP);
    var clientContext = Context.current().withDeadlineAfter(80, TimeUnit.MILLISECONDS, scheduler);
    try {
      var call = newCall(MethodDescriptor.MethodType.BIDI_STREAMING);
      var terminalStatus = new AtomicReference<Status>();
      var terminated = new CountDownLatch(1);
      observeClose(call, terminalStatus, terminated);
      var listener = new AtomicReference<ServerCall.Listener<byte[]>>();
      clientContext.run(() -> listener.set(interceptor.interceptCall(
          call, new Metadata(), emptyHandler())));

      assertTrue(terminated.await(2, TimeUnit.SECONDS), "client deadline did not close the bidi stream");
      assertEquals(Status.Code.DEADLINE_EXCEEDED, terminalStatus.get().getCode());
    } finally {
      clientContext.close();
      interceptor.close();
      scheduler.shutdownNow();
      assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void bidiStreamAppliesRequestByteLimitToEachMessage() throws Exception {
    var scheduler = new ScheduledThreadPoolExecutor(1);
    var interceptor = new RequestScopeInterceptor(scheduler, 1,
        Duration.ofSeconds(1), 2, RequestCompletionRecorder.NOOP);
    var call = newKeepAliveCall();
    var closed = new AtomicReference<Status>();
    var terminated = new CountDownLatch(1);
    observeClose(call, closed, terminated);
    var received = new AtomicInteger();
    ServerCall.Listener<Rpc.LeaseKeepAliveRequest> listener = interceptor.interceptCall(
        call, new Metadata(), (ignoredCall, ignoredHeaders) -> new ServerCall.Listener<>() {
          @Override
          public void onMessage(Rpc.LeaseKeepAliveRequest request) {
            received.incrementAndGet();
          }
        });

    try {
      Rpc.LeaseKeepAliveRequest request = Rpc.LeaseKeepAliveRequest.newBuilder().setID(1).build();
      assertTrue(request.getSerializedSize() <= 2);
      for (int message = 0; message < 4; message++) listener.onMessage(request);

      assertEquals(4, received.get(), "a long-lived stream must not accumulate message bytes");
      assertNull(closed.get(), "individually bounded requests should remain open");
      listener.onCancel();
      interceptor.close();
      assertEquals(0, interceptor.activeRequests());
    } finally {
      interceptor.close();
      scheduler.shutdownNow();
      assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void carriesEtcdTokenMetadataOnlyInsideTheRequestContext() throws Exception {
    var scheduler = new ScheduledThreadPoolExecutor(1);
    var interceptor = new RequestScopeInterceptor(scheduler, 1,
        Duration.ofSeconds(1), 1024, RequestCompletionRecorder.NOOP);
    try {
      var headers = new Metadata();
      headers.put(EtcdAuthTokenMetadata.key(), "opaque-test-token");
      var observed = new AtomicReference<java.util.Optional<String>>();
      ServerCall.Listener<byte[]> listener = interceptor.interceptCall(newCall(
          MethodDescriptor.MethodType.UNARY), headers, (call, ignoredHeaders) -> {
            observed.set(RequestScopeInterceptor.currentAuthToken());
            return new ServerCall.Listener<>() {};
          });

      assertEquals(java.util.Optional.of("opaque-test-token"), observed.get());
      listener.onCancel();
    } finally {
      interceptor.close();
      scheduler.shutdownNow();
      assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void rejectsDuplicateEtcdTokenMetadataWithoutEnteringTheService() throws Exception {
    var scheduler = new ScheduledThreadPoolExecutor(1);
    var interceptor = new RequestScopeInterceptor(scheduler, 1,
        Duration.ofSeconds(1), 1024, RequestCompletionRecorder.NOOP);
    try {
      var headers = new Metadata();
      headers.put(EtcdAuthTokenMetadata.key(), "first-token");
      headers.put(EtcdAuthTokenMetadata.key(), "second-token");
      var closed = new AtomicReference<Status>();
      var terminated = new CountDownLatch(1);
      var call = newCall(MethodDescriptor.MethodType.UNARY);
      observeClose(call, closed, terminated);
      var entered = new AtomicInteger();
      interceptor.interceptCall(call, headers,
          (unusedCall, ignoredHeaders) -> {
            entered.incrementAndGet();
            return new ServerCall.Listener<>() {};
          });

      assertTrue(terminated.await(1, TimeUnit.SECONDS));
      assertEquals(Status.Code.UNAUTHENTICATED, closed.get().getCode());
      assertEquals("etcdserver: invalid auth token", closed.get().getDescription());
      assertEquals(0, entered.get());
    } finally {
      interceptor.close();
      scheduler.shutdownNow();
      assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static ServerCall<byte[], byte[]> newCall(MethodDescriptor.MethodType type) {
    var call = mock(ServerCall.class);
    when(call.getMethodDescriptor()).thenReturn(methodDescriptor(type));
    return call;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static ServerCall<Rpc.LeaseKeepAliveRequest, Rpc.LeaseKeepAliveResponse> newKeepAliveCall() {
    var call = mock(ServerCall.class);
    when(call.getMethodDescriptor()).thenReturn(keepAliveMethodDescriptor());
    return call;
  }

  private static MethodDescriptor<Rpc.LeaseKeepAliveRequest, Rpc.LeaseKeepAliveResponse>
      keepAliveMethodDescriptor() {
    return MethodDescriptor.<Rpc.LeaseKeepAliveRequest, Rpc.LeaseKeepAliveResponse>newBuilder()
        .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
        .setFullMethodName("etcdserverpb.Lease/LeaseKeepAlive")
        .setRequestMarshaller(ProtoUtils.marshaller(Rpc.LeaseKeepAliveRequest.getDefaultInstance()))
        .setResponseMarshaller(ProtoUtils.marshaller(Rpc.LeaseKeepAliveResponse.getDefaultInstance()))
        .build();
  }

  private static MethodDescriptor<byte[], byte[]> methodDescriptor(MethodDescriptor.MethodType type) {
    MethodDescriptor.Marshaller<byte[]> bytes = new MethodDescriptor.Marshaller<>() {
      @Override public java.io.InputStream stream(byte[] value) {
        return new ByteArrayInputStream(value);
      }

      @Override public byte[] parse(java.io.InputStream stream) {
        try {
          return stream.readAllBytes();
        } catch (IOException failure) {
          throw new IllegalArgumentException("test byte stream could not be read", failure);
        }
      }
    };
    return MethodDescriptor.<byte[], byte[]>newBuilder()
        .setType(type)
        .setFullMethodName("test.Service/Call")
        .setRequestMarshaller(bytes)
        .setResponseMarshaller(bytes)
        .build();
  }

  private static ServerCallHandler<byte[], byte[]> emptyHandler() {
    return (call, headers) -> new ServerCall.Listener<>() {};
  }

  private static <RequestT, ResponseT> void observeClose(ServerCall<RequestT, ResponseT> call,
      AtomicReference<Status> terminalStatus, CountDownLatch terminated) {
    doAnswer(invocation -> {
      terminalStatus.set(invocation.getArgument(0));
      terminated.countDown();
      return null;
    }).when(call).close(any(), any());
  }
}
