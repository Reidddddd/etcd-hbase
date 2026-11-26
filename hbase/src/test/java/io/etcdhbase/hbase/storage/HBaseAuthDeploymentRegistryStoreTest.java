package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.auth.AuthorizationRevocation;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.AuthDeploymentRegistryMessages;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class HBaseAuthDeploymentRegistryStoreTest {
  private final AuthControlBinding binding = new AuthControlBinding(
      TableName.valueOf("auth_control"), "trust", new HashSaltStrategy());
  private final AsyncConnection connection = mock(AsyncConnection.class);
  private final AsyncTableBuilder<?> builder = mock(AsyncTableBuilder.class, RETURNS_SELF);
  private final AsyncTable<?> table = mock(AsyncTable.class);
  private final HBaseAuthSessionStore store;

  HBaseAuthDeploymentRegistryStoreTest() {
    doReturn(builder).when(connection).getTableBuilder(binding.table());
    doReturn(table).when(builder).build();
    store = new HBaseAuthSessionStore(connection, binding);
  }

  @Test
  void usesStrongReadsAndExactDurableCasAndRejectsInvalidTransitions() {
    var group = new AuthDeploymentRegistry.Group("group", "provider", "realm", "profile",
        bytes(1, 32), bytes(2, 32), true);
    var initial = AuthDeploymentRegistry.initial("trust", List.of(group));
    var operation = AuthorizationRevocation.start(bytes(3, 16), bytes(4, 32), "provider", "realm", 2, 2, 1,
        List.of(new AuthorizationRevocation.TargetIdentity("group", bytes(1, 32), bytes(5, 32))));
    var reserved = initial.beginRevocation(operation);
    byte[] row = binding.keyCodec().deploymentRegistry("trust");
    var result = Result.create(new org.apache.hadoop.hbase.Cell[] {
        new KeyValue(row, binding.family(), new byte[] {'r'}, AuthDeploymentRegistryMessages.encode(initial))});
    doReturn(CompletableFuture.completedFuture(result)).when(table).get(any(Get.class));
    doReturn(CompletableFuture.completedFuture(new CheckAndMutateResult(true, null)))
        .when(table).checkAndMutate(any(CheckAndMutate.class));
    try (var scope = RequestScope.withoutDeadline()) {
      assertEquals(initial, store.readRegistry(scope).toCompletableFuture().join().orElseThrow());
      var get = ArgumentCaptor.forClass(Get.class);
      verify(table).get(get.capture());
      assertEquals(Consistency.STRONG, get.getValue().getConsistency());
      assertTrue(store.createRegistryIfAbsent(initial, scope).toCompletableFuture().join());
      assertTrue(store.compareAndSetRegistry(initial, reserved, scope).toCompletableFuture().join());
      var calls = ArgumentCaptor.forClass(CheckAndMutate.class);
      verify(table, times(2)).checkAndMutate(calls.capture());
      assertArrayEquals(AuthDeploymentRegistryMessages.encode(initial), calls.getAllValues().get(1).getValue());
      for (var call : calls.getAllValues()) {
        assertEquals(Durability.SYNC_WAL, assertInstanceOf(Put.class, call.getAction()).getDurability());
      }
      assertThrows(CompletionException.class,
          () -> store.compareAndSetRegistry(reserved, initial, scope).toCompletableFuture().join());
    }
  }

  @Test
  void missingRegistryStaysMissingAndCorruptOrMisboundRecordsAreUnavailable() {
    var group = new AuthDeploymentRegistry.Group("group", "provider", "realm", "profile",
        bytes(1, 32), bytes(2, 32), true);
    var initial = AuthDeploymentRegistry.initial("trust", List.of(group));
    var otherDomain = AuthDeploymentRegistry.initial("other", List.of(group));
    byte[] row = binding.keyCodec().deploymentRegistry("trust");
    byte[] wrongRow = binding.keyCodec().deploymentRegistry("other");
    byte[] value = AuthDeploymentRegistryMessages.encode(initial);
    doReturn(CompletableFuture.completedFuture(Result.EMPTY_RESULT)).when(table).get(any(Get.class));
    try (var scope = RequestScope.withoutDeadline()) {
      assertTrue(store.readRegistry(scope).toCompletableFuture().join().isEmpty());
      for (Result invalid : List.of(
          result(row, AuthDeploymentRegistryMessages.encode(otherDomain)),
          result(row, new byte[] {(byte) 0xff}), result(wrongRow, value),
          Result.create(new org.apache.hadoop.hbase.Cell[] {
              new KeyValue(row, binding.family(), new byte[] {'x'}, value)}))) {
        doReturn(CompletableFuture.completedFuture(invalid)).when(table).get(any(Get.class));
        var failure = assertThrows(CompletionException.class,
            () -> store.readRegistry(scope).toCompletableFuture().join());
        assertEquals(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE,
            assertInstanceOf(AuthSessionException.class, failure.getCause()).reason());
      }
    }
  }

  @Test
  void backendTimeoutIsNotMisreportedAsCorruptConfiguration() {
    doReturn(CompletableFuture.failedFuture(new TimeoutException("backend timeout")))
        .when(table).get(any(Get.class));
    try (var scope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(CompletionException.class,
          () -> store.readRegistry(scope).toCompletableFuture().join());
      assertEquals(BackendFailure.Category.TIMEOUT,
          assertInstanceOf(BackendFailure.class, failure.getCause()).category());
    }
  }

  private Result result(byte[] row, byte[] value) {
    return Result.create(new org.apache.hadoop.hbase.Cell[] {
        new KeyValue(row, binding.family(), new byte[] {'r'}, value)});
  }

  private static ByteKey bytes(int value, int length) {
    byte[] data = new byte[length];
    java.util.Arrays.fill(data, (byte) value);
    return ByteKey.copyOf(data);
  }
}
