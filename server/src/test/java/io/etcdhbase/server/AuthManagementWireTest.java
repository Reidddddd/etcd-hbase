package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import etcdserverpb.AuthGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.core.auth.AuthConfigurationRecord;
import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.grpc.service.AuthService;
import io.etcdhbase.grpc.service.KvService;
import io.etcdhbase.grpc.service.LeaseService;
import io.etcdhbase.grpc.service.WatchService;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.auth.AuthStatusRpcHandler;
import io.etcdhbase.server.observability.RequestMetrics;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import java.io.ByteArrayInputStream;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class AuthManagementWireTest {
  private static final Set<String> UNSUPPORTED_METHODS = Set.of(
      "AuthEnable", "AuthDisable", "UserAdd", "UserGet", "UserList", "UserDelete",
      "UserChangePassword", "UserGrantRole", "UserRevokeRole", "RoleAdd", "RoleGet",
      "RoleList", "RoleDelete", "RoleGrantPermission", "RoleRevokePermission");

  @Test
  void authStatusReturnsThePersistedBridgeRevisionAndUnavailableWhenMissing() throws Exception {
    var listener = new RuntimeConfiguration.Listener("j51-status", "127.0.0.1", 0, "group");
    var budgets = new RuntimeConfiguration.ResourceBudgets(65_536, 2, 3_000, 1_048_576,
        2_097_152, 4, 0);
    var current = new AtomicReference<>(new AuthConfigurationRecord(
        "milvus", "group", ByteKey.copyOf(new byte[32]), true, 7));
    var handler = new AuthStatusRpcHandler(scope -> current.get() == null
        ? CompletableFuture.failedFuture(new AuthSessionException(
            AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE))
        : CompletableFuture.completedFuture(current.get()),
        scope -> CompletableFuture.completedFuture(Rpc.ResponseHeader.newBuilder()
            .setClusterId(10).setMemberId(11).setRevision(12).build()));
    var auth = new AuthService((request, observer) -> {}, handler);
    try (Bootstrap bootstrap = Bootstrap.startConfigured(listener, budgets, new KvService(),
        new LeaseService(), new WatchService(), auth, new RequestMetrics())) {
      ManagedChannel channel = ManagedChannelBuilder
          .forAddress("127.0.0.1", bootstrap.boundListener().port()).usePlaintext().build();
      try {
        var client = AuthGrpc.newBlockingStub(channel).withDeadlineAfter(2, TimeUnit.SECONDS);
        var response = client.authStatus(Rpc.AuthStatusRequest.getDefaultInstance());
        assertTrue(response.getEnabled());
        assertEquals(7, response.getAuthRevision());
        assertEquals(12, response.getHeader().getRevision());
        current.set(null);
        var failure = assertThrows(StatusRuntimeException.class,
            () -> client.authStatus(Rpc.AuthStatusRequest.getDefaultInstance()));
        assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
      } finally {
        channel.shutdownNow();
        assertTrue(channel.awaitTermination(2, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  void configuredAuthServiceRejectsEveryUnsupportedManagementMethodOverWire() throws Exception {
    var listener = new RuntimeConfiguration.Listener("j51-auth", "127.0.0.1", 0, "group");
    var budgets = new RuntimeConfiguration.ResourceBudgets(65_536, 2, 3_000, 1_048_576,
        2_097_152, 4, 0);
    var authenticateCalled = new AtomicBoolean();
    var auth = new AuthService((request, observer) -> authenticateCalled.set(true));
    try (Bootstrap bootstrap = Bootstrap.startConfigured(listener, budgets, new KvService(),
        new LeaseService(), new WatchService(), auth, new RequestMetrics())) {
      ManagedChannel channel = ManagedChannelBuilder
          .forAddress("127.0.0.1", bootstrap.boundListener().port()).usePlaintext().build();
      try {
        var seen = new java.util.HashSet<String>();
        for (MethodDescriptor<?, ?> descriptor : AuthGrpc.getServiceDescriptor().getMethods()) {
          String name = MethodDescriptor.extractBareMethodName(descriptor.getFullMethodName());
          if (!UNSUPPORTED_METHODS.contains(name)) continue;
          assertTrue(seen.add(name), name);
          assertRejected(channel, descriptor, false);
          assertRejected(channel, descriptor, true);
        }
        assertEquals(UNSUPPORTED_METHODS, seen);
        assertFalse(authenticateCalled.get());
      } finally {
        channel.shutdownNow();
        assertTrue(channel.awaitTermination(2, TimeUnit.SECONDS));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void assertRejected(ManagedChannel channel, MethodDescriptor<?, ?> descriptor,
      boolean populated) {
    var method = (MethodDescriptor<Message, Message>) descriptor;
    Message empty = method.getRequestMarshaller().parse(new ByteArrayInputStream(new byte[0]));
    Message request = populated ? populate(empty.toBuilder()).build() : empty;
    StatusRuntimeException failure = assertThrows(StatusRuntimeException.class,
        () -> ClientCalls.blockingUnaryCall(channel, method,
            CallOptions.DEFAULT.withDeadlineAfter(2, TimeUnit.SECONDS), request),
        method.getFullMethodName());
    assertEquals(Status.Code.UNIMPLEMENTED, failure.getStatus().getCode(), method.getFullMethodName());
    assertEquals("etcd-hbase M0: Auth."
        + MethodDescriptor.extractBareMethodName(method.getFullMethodName())
        + " is not implemented", failure.getStatus().getDescription());
  }

  private static Message.Builder populate(Message.Builder builder) {
    for (FieldDescriptor field : builder.getDescriptorForType().getFields()) {
      if (field.isRepeated()) continue;
      Object value = switch (field.getJavaType()) {
        case STRING -> "test-value";
        case BYTE_STRING -> ByteString.copyFromUtf8("test-key");
        case BOOLEAN -> true;
        case INT -> 7;
        case LONG -> 7L;
        case FLOAT -> 7F;
        case DOUBLE -> 7D;
        case ENUM -> field.getEnumType().findValueByNumberCreatingIfUnknown(999);
        case MESSAGE -> populate(builder.newBuilderForField(field)).build();
      };
      builder.setField(field, value);
    }
    var unknown = UnknownFieldSet.Field.newBuilder().addVarint(17).build();
    builder.mergeUnknownFields(UnknownFieldSet.newBuilder().addField(200, unknown).build());
    return builder;
  }
}
