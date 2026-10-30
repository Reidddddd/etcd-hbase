package io.etcdhbase.core.scan;

import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.StoragePage;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.List;

/** Owns the final response allocation until the consumer or request closes it. */
public final class RangePage implements AutoCloseable {
  private List<StoredRecord> records;
  private List<BudgetManager.Lease> leases;
  private final int count;
  private final boolean more;
  private final StoragePage.Cost cost;
  private final long pointReads;
  private final RequestScope.ScopedCleanup registration;
  private boolean closed;

  RangePage(List<StoredRecord> records, List<BudgetManager.Lease> leases, boolean more,
      StoragePage.Cost cost, long pointReads, RequestScope scope) {
    this.records = List.copyOf(records);
    this.leases = List.copyOf(leases);
    count = records.size();
    this.more = more;
    this.cost = cost;
    this.pointReads = pointReads;
    registration = scope.registerScopedCleanup(this);
  }

  public synchronized List<StoredRecord> records() {
    if (closed) throw new IllegalStateException("range response is closed");
    return records;
  }
  public int count() { return count; }
  public boolean more() { return more; }
  public StoragePage.Cost cost() { return cost; }
  public long pointReads() { return pointReads; }

  @Override
  public void close() {
    List<BudgetManager.Lease> owned;
    synchronized (this) {
      if (closed) return;
      closed = true;
      records = List.of();
      owned = leases;
      leases = List.of();
    }
    for (var lease : owned) lease.close();
    try {
      if (registration != null) registration.close();
    } catch (Exception failure) {
      throw new IllegalStateException("response registration cleanup failed", failure);
    }
  }
}
