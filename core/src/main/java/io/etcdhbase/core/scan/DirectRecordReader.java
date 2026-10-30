package io.etcdhbase.core.scan;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** No cache or coalescing: each accepted read uses its bound, authorized backend port. */
public final class DirectRecordReader implements RecordReader {
  private final RecordStore store;
  private final BudgetManager.Account account;
  private final long maximumResultBytes;

  public DirectRecordReader(RecordStore store, BudgetManager.Account account, long maximumResultBytes) {
    this.store = Objects.requireNonNull(store, "store");
    this.account = Objects.requireNonNull(account, "account");
    if (maximumResultBytes < 1 || maximumResultBytes > account.reservation().output()) {
      throw new IllegalArgumentException("point result bound exceeds its reserved output capacity");
    }
    this.maximumResultBytes = maximumResultBytes;
  }

  @Override
  public CompletableFuture<PointResult> read(ByteKey key, long upper, ReadProjection projection) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(projection, "projection");
    if (upper <= 0) return CompletableFuture.failedFuture(new IllegalArgumentException("revision must be positive"));
    BudgetManager.Lease output;
    try {
      output = account.tryRetain(BudgetManager.Memory.OUTPUT, maximumResultBytes)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    } catch (BackendFailure failure) {
      return CompletableFuture.failedFuture(failure);
    }
    var owner = new CompletableFuture<PointResult>();
    var response = owner.copy();
    owner.thenAccept(actual -> response.whenComplete((delivered, failure) -> {
      // Cancellation, timeout or a caller-supplied fallback may win before the real result arrives.
      if (actual != delivered) actual.close();
    }));
    CompletableFuture<Optional<StoredRecord>> operation;
    try { operation = PointRead.get(store, key, upper, projection, account); }
    catch (Error fatal) {
      output.close();
      owner.completeExceptionally(fatal);
      throw fatal;
    }
    operation.whenComplete((record, failure) -> {
      PointResult result = null;
      try {
        if (failure != null) { output.close(); owner.completeExceptionally(failure); return; }
        validate(record, key, upper, projection);
        account.requireActive();
        result = new PointResult(record, output, account.scope());
        account.requireActive();
        owner.complete(result);
      } catch (BackendFailure | RuntimeException invalid) {
        if (result != null) result.close();
        else output.close();
        owner.completeExceptionally(invalid);
      } catch (Error fatal) {
        if (result != null) result.close();
        else output.close();
        owner.completeExceptionally(fatal);
        throw fatal;
      }
    });
    return response;
  }

  private void validate(Optional<StoredRecord> record, ByteKey key, long upper, ReadProjection projection) throws BackendFailure {
    if (record.isEmpty()) return;
    var value = record.get();
    if (value.retainedBytes() > maximumResultBytes) throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
    if (!value.key().equals(key) || value.metadata().modificationRevision() > upper
        || projection == ReadProjection.METADATA && value.hasValue()
        || projection == ReadProjection.FULL && value.metadata().state() == StorageMetadata.State.LIVE && !value.hasValue()) {
      throw new IllegalStateException("point result violates its requested version or projection");
    }
  }
}
