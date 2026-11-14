package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Owns request budget claims until the request's physical backend work has drained. */
final class ScopedBudgetLeases implements AutoCloseable {
  private final Set<BudgetManager.Lease> leases =
      java.util.Collections.newSetFromMap(new IdentityHashMap<>());
  private boolean closed;

  private ScopedBudgetLeases() {}

  static ScopedBudgetLeases register(RequestScope scope, BudgetManager.Account account) {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    if (!account.belongsTo(scope)) {
      throw new IllegalArgumentException("budget account belongs to another request scope");
    }
    var owner = new ScopedBudgetLeases();
    var released = new CompletableFuture<Void>();
    try {
      scope.registerScopedCleanup(() -> scope.physicalProducersCompletion().whenComplete((ignored, failure) -> {
        try {
          owner.close();
          released.complete(null);
        } catch (RuntimeException | Error cleanupFailure) {
          released.completeExceptionally(cleanupFailure);
        }
      }), released);
      return owner;
    } catch (RuntimeException | Error failure) {
      try {
        owner.close();
      } catch (RuntimeException | Error cleanupFailure) {
        if (failure != cleanupFailure) failure.addSuppressed(cleanupFailure);
      }
      throw failure;
    }
  }

  BudgetManager.Lease retain(BudgetManager.Account account, BudgetManager.Memory lane, long bytes)
      throws BackendFailure {
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(lane, "lane");
    BudgetManager.Lease lease = account.tryRetain(lane, bytes)
        .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    track(lease);
    return lease;
  }

  BudgetManager.Lease acquire(BudgetManager.Account account, BudgetManager.Work work)
      throws BackendFailure {
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(work, "work");
    BudgetManager.Lease lease = account.tryAcquire(work)
        .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    track(lease);
    return lease;
  }

  void release(BudgetManager.Lease lease) {
    Objects.requireNonNull(lease, "lease");
    boolean owned;
    synchronized (this) {
      owned = leases.remove(lease);
    }
    if (owned) lease.close();
  }

  private synchronized void track(BudgetManager.Lease lease) {
    if (closed) {
      lease.close();
      throw new IllegalStateException("request budget owner is already closed");
    }
    leases.add(lease);
  }

  @Override
  public void close() {
    List<BudgetManager.Lease> releasing;
    synchronized (this) {
      if (closed) return;
      closed = true;
      releasing = new ArrayList<>(leases);
      leases.clear();
    }
    RuntimeException runtimeFailure = null;
    Error fatalFailure = null;
    for (int index = releasing.size() - 1; index >= 0; index--) {
      try {
        releasing.get(index).close();
      } catch (RuntimeException failure) {
        if (runtimeFailure == null) runtimeFailure = failure;
        else if (runtimeFailure != failure) runtimeFailure.addSuppressed(failure);
      } catch (Error failure) {
        if (fatalFailure == null) fatalFailure = failure;
        else if (fatalFailure != failure) fatalFailure.addSuppressed(failure);
      }
    }
    if (fatalFailure != null) {
      if (runtimeFailure != null) fatalFailure.addSuppressed(runtimeFailure);
      throw fatalFailure;
    }
    if (runtimeFailure != null) throw runtimeFailure;
  }
}
