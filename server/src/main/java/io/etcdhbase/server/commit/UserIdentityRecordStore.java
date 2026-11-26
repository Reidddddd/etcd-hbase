package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.BatchWriteResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.error.HBaseFailures;
import io.etcdhbase.hbase.storage.HBaseRecordStore;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.client.AsyncConnection;

/** Tracks failures from operations performed with one user's physical backend identity. */
final class UserIdentityRecordStore implements RecordStore {
  private final RecordStore delegate;
  private final Runnable retire;

  UserIdentityRecordStore(RecordStore delegate, Runnable retire) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.retire = Objects.requireNonNull(retire, "retire");
  }

  /** Observes the shared connection producer even if its request-owned copy is cancelled. */
  static CompletableFuture<UserIdentityRecordStore> open(HBaseConnectionFactory factory,
      AuthorizedBackendIdentity identity, StorageTableBinding binding, RequestScope scope,
      Executor worker, Runnable retire) {
    Objects.requireNonNull(factory, "factory");
    Objects.requireNonNull(retire, "retire");
    HBaseConnectionFactory observedFactory = new HBaseConnectionFactory() {
      @Override public CompletionStage<AsyncConnection> connectionFor(AuthorizedBackendIdentity requested) {
        var drained = new CompletableFuture<Void>();
        if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) {
          return CompletableFuture.failedFuture(new CancellationException());
        }
        try {
          CompletionStage<AsyncConnection> shared = Objects.requireNonNull(
              factory.connectionFor(requested), "connection future");
          shared.whenComplete((connection, failure) -> {
            try {
              if (failure != null) retireIfRejected(HBaseFailures.classify(failure), retire);
            } finally {
              drained.complete(null);
            }
          });
          return shared;
        } catch (RuntimeException | Error failure) {
          try {
            if (failure instanceof RuntimeException) {
              retireIfRejected(HBaseFailures.classify(failure), retire);
            }
          } finally {
            drained.complete(null);
          }
          throw failure;
        }
      }

      @Override public void close() {
        throw new UnsupportedOperationException("request does not own the connection factory");
      }
    };
    return HBaseRecordStore.open(observedFactory, identity, binding, scope, worker)
        .thenApply(physical -> new UserIdentityRecordStore(physical, retire));
  }

  static <T> CompletableFuture<T> observe(CompletionStage<T> operation, Runnable retire) {
    Objects.requireNonNull(operation, "operation");
    Objects.requireNonNull(retire, "retire");
    var result = new CompletableFuture<T>();
    operation.whenComplete((value, failure) -> {
      if (failure == null) {
        result.complete(value);
        return;
      }
      try {
        retireIfRejected(failure, retire);
      } catch (Error fatal) {
        result.completeExceptionally(fatal);
        throw fatal;
      }
      result.completeExceptionally(failure);
    });
    return result;
  }

  @Override public CompletableFuture<Optional<StoredRecord>> get(
      ByteKey key, long revision, ReadProjection projection) {
    return invoke(() -> delegate.get(key, revision, projection));
  }

  @Override public ScanOperation startScan(ScanRequest request) {
    final ScanOperation scan;
    try {
      scan = Objects.requireNonNull(delegate.startScan(request), "scan");
    } catch (RuntimeException failure) {
      retireIfRejected(failure, retire);
      throw failure;
    }
    try {
      CompletionStage<StoragePage> completion = observe(scan.completion(), retire).minimalCompletionStage();
      return new ScanOperation() {
        @Override public CompletionStage<StoragePage> completion() { return completion; }
        @Override public void close() {
          try {
            scan.close();
          } catch (RuntimeException failure) {
            retireIfRejected(failure, retire);
            throw failure;
          }
        }
      };
    } catch (RuntimeException | Error failure) {
      try {
        scan.close();
      } catch (RuntimeException | Error closeFailure) {
        if (closeFailure != failure) failure.addSuppressed(closeFailure);
      }
      if (failure instanceof RuntimeException) retireIfRejected(failure, retire);
      throw failure;
    }
  }
  @Override public boolean pointReadUsesScanner() { return delegate.pointReadUsesScanner(); }
  @Override public CompletableFuture<Void> finishRead() { return invoke(delegate::finishRead); }
  @Override public CompletableFuture<Void> put(StoredRecord record) { return invoke(() -> delegate.put(record)); }
  @Override public CompletableFuture<Boolean> compareAndSet(
      ByteKey key, Optional<StorageMetadata> expected, StoredRecord replacement) {
    return invoke(() -> delegate.compareAndSet(key, expected, replacement));
  }
  @Override public CompletableFuture<List<BatchWriteResult>> putBatch(List<StoredRecord> records) {
    return invoke(() -> delegate.putBatch(records).thenApply(results -> {
      results.stream().flatMap(result -> result.failure().stream())
          .filter(UserIdentityRecordStore::isAuthorizationFailure).findFirst()
          .ifPresent(failure -> retireIfRejected(failure, retire));
      return results;
    }));
  }

  private <T> CompletableFuture<T> invoke(Supplier<? extends CompletionStage<T>> operation) {
    try {
      return observe(operation.get(), retire);
    } catch (RuntimeException failure) {
      retireIfRejected(failure, retire);
      throw failure;
    }
  }

  private static void retireIfRejected(Throwable failure, Runnable retire) {
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    if (!isAuthorizationFailure(cause)) return;
    try {
      retire.run();
    } catch (RuntimeException retirementFailure) {
      if (retirementFailure != cause) cause.addSuppressed(retirementFailure);
    }
  }

  private static boolean isAuthorizationFailure(Throwable failure) {
    BackendFailure backend = failure instanceof BackendFailure known
        ? known : HBaseFailures.classify(failure);
    return backend.category() == BackendFailure.Category.AUTHENTICATION
            || backend.category() == BackendFailure.Category.PERMISSION
            || backend.category() == BackendFailure.Category.INVALID_AUTH_TOKEN;
  }
}
