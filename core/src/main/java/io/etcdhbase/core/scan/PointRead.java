package io.etcdhbase.core.scan;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Physical point-read accounting. Callers separately own any retained output allocation. */
final class PointRead {
  private PointRead() {}

  static CompletableFuture<Optional<StoredRecord>> get(RecordStore store, ByteKey key, long upper,
      ReadProjection projection, BudgetManager.Account account) {
    BudgetManager.Lease io = null;
    BudgetManager.Lease work = null;
    var result = new CompletableFuture<Optional<StoredRecord>>();
    try {
      io = account.tryRetain(BudgetManager.Memory.IO, account.reservation().io()).orElseThrow(PointRead::resourceLimit);
      work = account.tryAcquire(new BudgetManager.Work(0, 1, 0, 0, 0)).orElseThrow(PointRead::resourceLimit);
      account.chargePoint();
      var ownedIo = io;
      var ownedWork = work;
      store.get(key, upper, projection).whenComplete((record, failure) ->
          complete(result, record, failure, ownedWork, ownedIo, account));
    } catch (BackendFailure | RuntimeException failure) {
      complete(result, null, failure, work, io, account);
    } catch (Error fatal) {
      complete(result, null, fatal, work, io, account);
      throw fatal;
    }
    // A dependent cancellation never settles or cancels the backend producer.
    return result.copy();
  }

  private static void complete(CompletableFuture<Optional<StoredRecord>> result, Optional<StoredRecord> record,
      Throwable failure, BudgetManager.Lease work, BudgetManager.Lease io, BudgetManager.Account account) {
    Throwable terminal = failure;
    Error fatal = null;
    for (var lease : new BudgetManager.Lease[] {work, io}) {
      try { if (lease != null) lease.close(); }
      catch (RuntimeException | Error cleanup) {
        if (terminal == null) terminal = cleanup;
        else if (terminal != cleanup) terminal.addSuppressed(cleanup);
        if (cleanup instanceof Error error) fatal = error;
      }
    }
    if (terminal == null) {
      try { account.requireActive(); }
      catch (BackendFailure inactive) { terminal = inactive; }
    }
    if (terminal != null) result.completeExceptionally(terminal);
    else if (record == null) result.completeExceptionally(new IllegalStateException("point read returned no projection"));
    else result.complete(record);
    if (fatal != null) throw fatal;
  }

  private static BackendFailure resourceLimit() { return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT); }
}
