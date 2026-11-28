package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.core.commit.ResultQueryUnavailable;
import io.etcdhbase.grpc.protocol.ErrorProjection;
import io.etcdhbase.grpc.service.KvService;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Wire-level error identity, paired with resolver tests for actual closed-window admission. */
class InvocationResultErrorRpcTest {
  @Test
  void distinguishesClosedWindowsProtectionAdmissionAndCorruptionOverRpc() throws Exception {
    var failure = new AtomicReference<Throwable>(new ResultQueryUnavailable());
    var server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
        .addService(new KvService((request, observer) ->
            observer.onError(ErrorProjection.project(failure.get())))).build().start();
    var channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
    try {
      var client = KVGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS);
      var closed = assertThrows(StatusRuntimeException.class, () -> client.put(Rpc.PutRequest.getDefaultInstance()));
      assertEquals(Status.Code.FAILED_PRECONDITION, closed.getStatus().getCode());
      assertEquals("invocation result is no longer queryable", closed.getStatus().getDescription());
      failure.set(new ResultQueryUnavailable(ResultQueryUnavailable.Reason.PROTECTION_UNAVAILABLE));
      var protection = assertThrows(StatusRuntimeException.class, () -> client.put(Rpc.PutRequest.getDefaultInstance()));
      assertEquals(Status.Code.UNAVAILABLE, protection.getStatus().getCode());
      failure.set(new IllegalStateException("private corrupt result detail"));
      var corrupt = assertThrows(StatusRuntimeException.class, () -> client.put(Rpc.PutRequest.getDefaultInstance()));
      assertEquals(Status.Code.INTERNAL, corrupt.getStatus().getCode());
    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
