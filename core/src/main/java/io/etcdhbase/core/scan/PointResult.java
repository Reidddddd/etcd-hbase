package io.etcdhbase.core.scan;

import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Optional;

/** Owns a materialized point projection and its output capacity. */
public final class PointResult implements AutoCloseable {
  private Optional<StoredRecord> record;
  private final BudgetManager.Lease lease;
  private final RequestScope.ScopedCleanup registration;
  private boolean closed;

  PointResult(Optional<StoredRecord> record, BudgetManager.Lease lease, RequestScope scope) {
    this.record = record;
    this.lease = lease;
    registration = scope.registerScopedCleanup(this);
  }

  public synchronized Optional<StoredRecord> record() {
    if (closed) throw new IllegalStateException("point result is closed");
    return record;
  }

  @Override
  public void close() {
    synchronized (this) {
      if (closed) return;
      closed = true;
      record = Optional.empty();
    }
    try { lease.close(); }
    finally {
      try { if (registration != null) registration.close(); }
      catch (Exception failure) { throw new IllegalStateException("point result cleanup failed", failure); }
    }
  }
}
