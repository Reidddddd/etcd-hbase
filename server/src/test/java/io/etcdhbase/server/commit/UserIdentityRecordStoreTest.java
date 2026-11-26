package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.storage.BatchWriteResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.storage.HBaseRecordStore;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.junit.jupiter.api.Test;

class UserIdentityRecordStoreTest {
  private static final ByteKey KEY = ByteKey.copyOf(new byte[] {1});

  @Test
  void physicalAuthorizationFailureRetiresIdentityButUnavailabilityDoesNot() {
    var delegate = new TestStore();
    var retirements = new AtomicInteger();
    var store = new UserIdentityRecordStore(delegate, retirements::incrementAndGet);
    var denied = new BackendFailure(BackendFailure.Category.PERMISSION);
    delegate.point = CompletableFuture.failedFuture(denied);
    var failure = assertThrows(CompletionException.class,
        () -> store.get(KEY, 1, ReadProjection.FULL).join());
    assertSame(denied, failure.getCause());
    assertEquals(1, retirements.get());

    delegate.point = CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    assertThrows(CompletionException.class, () -> store.get(KEY, 1, ReadProjection.FULL).join());
    assertEquals(1, retirements.get());
  }

  @Test
  void cancellationDoesNotHideLatePhysicalAuthorizationFailure() {
    var delegate = new TestStore();
    var retirements = new AtomicInteger();
    var store = new UserIdentityRecordStore(delegate, retirements::incrementAndGet);
    delegate.point = new CompletableFuture<>();
    var request = store.get(KEY, 1, ReadProjection.FULL);
    request.cancel(false);
    assertFalse(delegate.point.isCancelled());
    delegate.point.completeExceptionally(new BackendFailure(BackendFailure.Category.AUTHENTICATION));
    assertEquals(1, retirements.get());
  }

  @Test
  void realHbasePointReadRetiresAfterScopeCancellationAndBackendDenial() {
    var factory = mock(HBaseConnectionFactory.class);
    var identity = mock(AuthorizedBackendIdentity.class);
    var connection = mock(AsyncConnection.class);
    AsyncTableBuilder<?> builder = mock(AsyncTableBuilder.class, RETURNS_SELF);
    AsyncTable<?> table = mock(AsyncTable.class);
    var binding = new StorageTableBinding(TableName.valueOf("synthetic"), new byte[] {'d'},
        StorageTableBinding.Layout.CURRENT, new HashSaltStrategy());
    doReturn(CompletableFuture.completedFuture(connection)).when(factory).connectionFor(identity);
    doReturn(builder).when(connection).getTableBuilder(binding.table());
    doReturn(table).when(builder).build();
    var backend = new CompletableFuture<Result>();
    doReturn(backend).when(table).get(any(Get.class));
    var retirements = new AtomicInteger();
    try (var scope = RequestScope.withoutDeadline()) {
      var physical = HBaseRecordStore.open(factory, identity, binding, scope, Runnable::run).join();
      var result = new UserIdentityRecordStore(physical, retirements::incrementAndGet)
          .get(KEY, 1, ReadProjection.FULL);
      scope.cancel();
      assertFalse(result.isDone());
      assertFalse(backend.isCancelled());
      backend.completeExceptionally(new AccessDeniedException("denied"));
      var failure = assertThrows(CompletionException.class, result::join);
      assertEquals(BackendFailure.Category.PERMISSION, ((BackendFailure) failure.getCause()).category());
      assertEquals(1, retirements.get());
      assertTrue(backend.isDone());
    }
  }

  @Test
  void historyPointReadRetiresAfterScopeCancellationAndBackendDenial() {
    var factory = mock(HBaseConnectionFactory.class);
    var identity = mock(AuthorizedBackendIdentity.class);
    var connection = mock(AsyncConnection.class);
    AsyncTableBuilder<?> builder = mock(AsyncTableBuilder.class, RETURNS_SELF);
    AsyncTable<?> table = mock(AsyncTable.class);
    var binding = new StorageTableBinding(TableName.valueOf("synthetic"), new byte[] {'d'},
        StorageTableBinding.Layout.HISTORY, new HashSaltStrategy());
    doReturn(CompletableFuture.completedFuture(connection)).when(factory).connectionFor(identity);
    doReturn(builder).when(connection).getTableBuilder(binding.table());
    doReturn(table).when(builder).build();
    var backend = new CompletableFuture<List<Result>>();
    doReturn(backend).when(table).scanAll(any(Scan.class));
    var retirements = new AtomicInteger();
    try (var scope = RequestScope.withoutDeadline()) {
      var physical = HBaseRecordStore.open(factory, identity, binding, scope, Runnable::run).join();
      var result = new UserIdentityRecordStore(physical, retirements::incrementAndGet)
          .get(KEY, 1, ReadProjection.FULL);
      scope.cancel();
      assertFalse(result.isDone());
      backend.completeExceptionally(new AccessDeniedException("denied"));
      var failure = assertThrows(CompletionException.class, result::join);
      assertEquals(BackendFailure.Category.PERMISSION, ((BackendFailure) failure.getCause()).category());
      assertEquals(1, retirements.get());
    }
  }

  @Test
  void lateConnectionDenialAfterCancellationStillRetiresTheUser() {
    var factory = mock(HBaseConnectionFactory.class);
    var identity = mock(AuthorizedBackendIdentity.class);
    var connection = new CompletableFuture<AsyncConnection>();
    doReturn(connection).when(factory).connectionFor(identity);
    var binding = new StorageTableBinding(TableName.valueOf("synthetic"), new byte[] {'d'},
        StorageTableBinding.Layout.HISTORY, new HashSaltStrategy());
    var retirements = new AtomicInteger();
    try (var scope = RequestScope.withoutDeadline()) {
      var opened = UserIdentityRecordStore.open(factory, identity, binding, scope, Runnable::run,
          retirements::incrementAndGet);
      scope.cancel();
      assertFalse(connection.isCancelled());
      assertThrows(CompletionException.class, opened::join);
      assertFalse(scope.physicalProducersCompletion().toCompletableFuture().isDone());
      connection.completeExceptionally(new AccessDeniedException("denied"));
      assertEquals(1, retirements.get());
      assertTrue(scope.physicalProducersCompletion().toCompletableFuture().isDone());
    }
  }

  @Test
  void cancelledRealScanClosesScannerAndObservesLateDenial() throws Exception {
    var factory = mock(HBaseConnectionFactory.class);
    var identity = mock(AuthorizedBackendIdentity.class);
    var connection = mock(AsyncConnection.class);
    AsyncTableBuilder<?> builder = mock(AsyncTableBuilder.class, RETURNS_SELF);
    AsyncTable<?> table = mock(AsyncTable.class);
    var scanner = mock(ResultScanner.class);
    var binding = new StorageTableBinding(TableName.valueOf("synthetic"), new byte[] {'d'},
        StorageTableBinding.Layout.HISTORY, new HashSaltStrategy());
    doReturn(CompletableFuture.completedFuture(connection)).when(factory).connectionFor(identity);
    doReturn(builder).when(connection).getTableBuilder(binding.table());
    doReturn(table).when(builder).build();
    doReturn(scanner).when(table).getScanner(any(Scan.class));
    var entered = new CountDownLatch(1);
    var deliverFailure = new CountDownLatch(1);
    doAnswer(ignored -> {
      entered.countDown();
      if (!deliverFailure.await(3, TimeUnit.SECONDS)) throw new IllegalStateException("test timed out");
      throw new AccessDeniedException("denied");
    }).when(scanner).next();
    var retirements = new AtomicInteger();
    var worker = Executors.newSingleThreadExecutor();
    try (var scope = RequestScope.withoutDeadline()) {
      var store = UserIdentityRecordStore.open(factory, identity, binding, scope, worker,
          retirements::incrementAndGet).join();
      var request = new ScanRequest(binding.codec().bucket(KEY), KeyRange.point(KEY), Optional.empty(),
          1, ReadProjection.FULL, 1, 4096, 10);
      var scan = store.startScan(request);
      assertTrue(entered.await(3, TimeUnit.SECONDS));
      scope.cancel();
      verify(scanner).close();
      assertFalse(scope.physicalProducersCompletion().toCompletableFuture().isDone());
      deliverFailure.countDown();
      var failure = assertThrows(CompletionException.class,
          () -> scan.completion().toCompletableFuture().join());
      assertEquals(BackendFailure.Category.PERMISSION, ((BackendFailure) failure.getCause()).category());
      assertEquals(1, retirements.get());
      scope.cleanupCompletion().toCompletableFuture().get(3, TimeUnit.SECONDS);
    } finally {
      deliverFailure.countDown();
      worker.shutdownNow();
      assertTrue(worker.awaitTermination(3, TimeUnit.SECONDS));
    }
  }

  @Test
  void connectionUnavailableDoesNotRetireUserAndImmediateDenialRetiresOnce() {
    var factory = mock(HBaseConnectionFactory.class);
    var identity = mock(AuthorizedBackendIdentity.class);
    var binding = new StorageTableBinding(TableName.valueOf("synthetic"), new byte[] {'d'},
        StorageTableBinding.Layout.HISTORY, new HashSaltStrategy());
    var retirements = new AtomicInteger();
    doReturn(CompletableFuture.failedFuture(new java.io.IOException("unavailable")),
        CompletableFuture.failedFuture(new AccessDeniedException("denied")))
        .when(factory).connectionFor(identity);
    try (var scope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(CompletionException.class,
          () -> UserIdentityRecordStore.open(factory, identity, binding, scope, Runnable::run,
              retirements::incrementAndGet).join());
      assertEquals(BackendFailure.Category.UNAVAILABLE, ((BackendFailure) failure.getCause()).category());
      assertEquals(0, retirements.get());
    }
    try (var scope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(CompletionException.class,
          () -> UserIdentityRecordStore.open(factory, identity, binding, scope, Runnable::run,
              retirements::incrementAndGet).join());
      assertEquals(BackendFailure.Category.PERMISSION, ((BackendFailure) failure.getCause()).category());
      assertEquals(1, retirements.get());
    }
  }

  @Test
  void scannerFailureRetiresAndCloseRemainsOwnedByCaller() {
    var delegate = new TestStore();
    var retirements = new AtomicInteger();
    var store = new UserIdentityRecordStore(delegate, retirements::incrementAndGet);
    var scan = store.startScan(null);
    var denied = new BackendFailure(BackendFailure.Category.PERMISSION);
    delegate.scanCompletion.completeExceptionally(denied);
    var failure = assertThrows(CompletionException.class,
        () -> scan.completion().toCompletableFuture().join());
    assertSame(denied, failure.getCause());
    assertEquals(1, retirements.get());
    assertEquals(0, delegate.closes.get());
    scan.close();
    assertEquals(1, delegate.closes.get());
  }

  @Test
  void scannerCloseAuthorizationFailureRetiresTheUser() {
    var delegate = new TestStore();
    var denied = new IllegalStateException(new AccessDeniedException("denied"));
    delegate.closeFailure = denied;
    var retirements = new AtomicInteger();
    var scan = new UserIdentityRecordStore(delegate, retirements::incrementAndGet).startScan(null);
    assertSame(denied, assertThrows(IllegalStateException.class, scan::close));
    assertEquals(1, retirements.get());
    assertEquals(1, delegate.closes.get());
  }

  @Test
  void connectionOpenFailureUsesSameAttributionBoundary() {
    var retirements = new AtomicInteger();
    var denied = new BackendFailure(BackendFailure.Category.AUTHENTICATION);
    var future = UserIdentityRecordStore.observe(
        CompletableFuture.failedFuture(denied), retirements::incrementAndGet);
    assertSame(denied, assertThrows(CompletionException.class, future::join).getCause());
    assertEquals(1, retirements.get());
  }

  @Test
  void scannerIsClosedIfItsCompletionStageCannotBeObtained() {
    var delegate = new TestStore();
    delegate.failCompletion = true;
    var retirements = new AtomicInteger();
    var store = new UserIdentityRecordStore(delegate, retirements::incrementAndGet);
    assertThrows(IllegalStateException.class, () -> store.startScan(null));
    assertEquals(1, delegate.closes.get());
    assertEquals(0, retirements.get());
  }

  @Test
  void batchPerItemAuthorizationFailuresAreObservedWithoutChangingTheResult() {
    var delegate = new TestStore();
    var retirements = new AtomicInteger();
    var store = new UserIdentityRecordStore(delegate, retirements::incrementAndGet);
    var results = List.of(new BatchWriteResult(0, BatchWriteResult.State.FAILED,
        Optional.of(new BackendFailure(BackendFailure.Category.PERMISSION))));
    delegate.batch = CompletableFuture.completedFuture(results);
    assertSame(results, store.putBatch(List.of()).join());
    assertEquals(1, retirements.get());
  }

  @Test
  void retirementFailureDoesNotReplaceTheOriginalBackendFailure() {
    var delegate = new TestStore();
    var denied = new BackendFailure(BackendFailure.Category.PERMISSION);
    var retirementFailure = new IllegalStateException("retirement failed");
    delegate.point = CompletableFuture.failedFuture(denied);
    var store = new UserIdentityRecordStore(delegate, () -> { throw retirementFailure; });
    var failure = assertThrows(CompletionException.class,
        () -> store.get(KEY, 1, ReadProjection.FULL).join());
    assertSame(denied, failure.getCause());
    assertSame(retirementFailure, denied.getSuppressed()[0]);
  }

  private static final class TestStore implements RecordStore {
    private CompletableFuture<Optional<StoredRecord>> point;
    private CompletableFuture<List<BatchWriteResult>> batch;
    private final CompletableFuture<StoragePage> scanCompletion = new CompletableFuture<>();
    private final AtomicInteger closes = new AtomicInteger();
    private boolean failCompletion;
    private RuntimeException closeFailure;

    @Override public CompletableFuture<Optional<StoredRecord>> get(
        ByteKey key, long revision, ReadProjection projection) { return point; }
    @Override public ScanOperation startScan(ScanRequest request) {
      return new ScanOperation() {
        @Override public CompletionStage<StoragePage> completion() {
          if (failCompletion) throw new IllegalStateException("missing completion");
          return scanCompletion;
        }
        @Override public void close() {
          closes.incrementAndGet();
          if (closeFailure != null) throw closeFailure;
        }
      };
    }
    @Override public CompletableFuture<Void> put(StoredRecord record) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<Boolean> compareAndSet(
        ByteKey key, Optional<StorageMetadata> expected, StoredRecord replacement) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<List<BatchWriteResult>> putBatch(List<StoredRecord> records) {
      return batch;
    }
  }
}
