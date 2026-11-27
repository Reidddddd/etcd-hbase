package io.etcdhbase.server.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.util.concurrent.CompletableFuture;

/** Retains a maintenance page's admission until every actual backend producer has drained. */
final class GcPageAdmission {
  private GcPageAdmission() {}

  static BudgetManager.Account admit(BudgetManager budgets, String group, RequestScope scope,
      BudgetManager.Reservation reservation, BudgetManager.ReadLimits limits) throws BackendFailure {
    return admit(budgets, group, scope, reservation, limits, new BudgetManager.Work(0, 1, 1, 1, 0));
  }

  static BudgetManager.Account admit(BudgetManager budgets, String group, RequestScope scope,
      BudgetManager.Reservation reservation, BudgetManager.ReadLimits limits, BudgetManager.Work work)
      throws BackendFailure {
    var account = budgets.admit(group, scope, reservation, limits);
    var slot = account.tryAcquire(work)
        .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    var released = new CompletableFuture<Void>();
    scope.registerScopedCleanup(() -> scope.physicalProducersCompletion().whenComplete((ignored, failure) -> {
      try { slot.close(); released.complete(null); }
      catch (RuntimeException | Error cleanup) { released.completeExceptionally(cleanup); }
    }), released);
    return account;
  }
}
