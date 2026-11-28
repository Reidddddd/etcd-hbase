package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.server.commit.LeaseBudgetProfile;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.M3MemoryReservations;
import io.etcdhbase.server.config.RuntimeConfiguration;
import java.util.List;
import org.junit.jupiter.api.Test;

class M3RuntimeBudgetsTest {
  @Test
  void m4MinimumAccountsForEveryGroupAndConcurrentLeaseOwner() throws Exception {
    int maximumRequestBytes = 1_572_864;
    var resources = new RuntimeConfiguration.ResourceBudgets(65_536, 2, 30_000,
        4 << 20, 4 << 20, 2, 0);
    var configuration = new RuntimeConfiguration(1, List.of(
        new RuntimeConfiguration.Listener("first", "127.0.0.1", 0, "a"),
        new RuntimeConfiguration.Listener("second", "127.0.0.1", 0, "b")),
        List.of(), List.of(), List.of(), resources);
    long admittedMinimum = M3MemoryReservations.minimumLeaseMemory(2, maximumRequestBytes);
    long reservedMemory = LeaseBudgetProfile.capacityForGroups(2, maximumRequestBytes).memoryBytes();
    long startupMemory = Math.addExact(Math.multiplyExact(2L, LeaseBudgetProfile.RECOVERY_MEMORY.total()),
        LeaseBudgetProfile.startupLeaseScanMemory().total());
    long regularMinimum = Math.max(admittedMinimum, startupMemory);
    long minimum = Math.addExact(reservedMemory, regularMinimum);
    assertThrows(IllegalArgumentException.class,
        () -> M3RuntimeBudgets.create(configuration, leaseActivation(minimum - 1)));
    var budgets = M3RuntimeBudgets.create(configuration, leaseActivation(minimum));
    try (var recoveryA = RequestScope.withoutDeadline(); var recoveryB = RequestScope.withoutDeadline();
         var gateway = RequestScope.withoutDeadline(); var owner = RequestScope.withoutDeadline();
         var a = budgets.admit("a", recoveryA, M3MemoryReservations.RECOVERY);
         var b = budgets.admit("b", recoveryB, M3MemoryReservations.RECOVERY);
         var read = budgets.admit("a", gateway,
             M3MemoryReservations.leaseBindingRead(M3MemoryReservations.LEASE, maximumRequestBytes));
         var mutation = budgets.admit("a", owner, M3MemoryReservations.LEASE)) {
      assertEquals(admittedMinimum, budgets.snapshot().reservedBytes());
    }
    assertEquals(0, budgets.snapshot().reservedBytes());
  }

  @Test
  void leaseActivationRejectsMemoryThatCannotAdmitACompleteBackfillCycle() {
    int maximumRequestBytes = 1_572_864;
    var resources = new RuntimeConfiguration.ResourceBudgets(65_536, 2, 30_000,
        4 << 20, 4 << 20, 2, 0);
    var configuration = new RuntimeConfiguration(1, List.of(
        new RuntimeConfiguration.Listener("first", "127.0.0.1", 0, "a")),
        List.of(), List.of(), List.of(), resources);
    long startupMemory = Math.addExact(LeaseBudgetProfile.RECOVERY_MEMORY.total(),
        LeaseBudgetProfile.startupLeaseScanMemory().total());
    long regularMemory = Math.max(
        M3MemoryReservations.minimumLeaseMemory(1, maximumRequestBytes), startupMemory);
    long minimum = Math.addExact(
        LeaseBudgetProfile.capacityForGroups(1, maximumRequestBytes).memoryBytes(), regularMemory);

    assertThrows(IllegalArgumentException.class,
        () -> M3RuntimeBudgets.create(configuration, leaseActivation(minimum - 1), 0));
    M3RuntimeBudgets.create(configuration, leaseActivation(minimum), 0);
    assertThrows(IllegalArgumentException.class,
        () -> M3RuntimeBudgets.create(configuration, leaseActivation(minimum), 1));
  }

  private static M3RuntimeConfiguration leaseActivation(long memoryBytes) {
    return new M3RuntimeConfiguration("1", 6_000, 1_000, 30_000, 30_000,
        memoryBytes, 1_572_864, 2, 16, 60_000, false, true, 5, 1_000,
        1_000, 1024, 4L << 20, 32L << 20, null, null,
        M3RuntimeConfiguration.ActivationProfile.PUBLIC_TEST, java.time.Duration.ofMinutes(30).toMillis());
  }

  @Test
  void leaseActivationAllowsEveryGroupToAdmitItsNestedBackfillAccounts() throws Exception {
    int maximumRequestBytes = 1_572_864;
    var resources = new RuntimeConfiguration.ResourceBudgets(65_536, 2, 30_000,
        4 << 20, 4 << 20, 2, 0);
    var configuration = new RuntimeConfiguration(1, List.of(
        new RuntimeConfiguration.Listener("first", "127.0.0.1", 0, "a"),
        new RuntimeConfiguration.Listener("second", "127.0.0.1", 0, "b")),
        List.of(), List.of(), List.of(), resources);
    long startupMemory = Math.addExact(Math.multiplyExact(2L, LeaseBudgetProfile.RECOVERY_MEMORY.total()),
        LeaseBudgetProfile.startupLeaseScanMemory().total());
    long regularMemory = Math.max(M3MemoryReservations.minimumLeaseMemory(2, maximumRequestBytes), startupMemory);
    long minimum = Math.addExact(
        LeaseBudgetProfile.capacityForGroups(2, maximumRequestBytes).memoryBytes(), regularMemory);
    assertThrows(IllegalArgumentException.class,
        () -> M3RuntimeBudgets.create(configuration, leaseActivation(minimum - 1), 0));
    var budgets = M3RuntimeBudgets.create(configuration, leaseActivation(minimum), 0);
    long accountBytes = LeaseBudgetProfile.eventBackfillMinimumMemory() / 2;
    var backfillAccount = new BudgetManager.Reservation(0, 1, 1, 0, accountBytes - 2);
    try (var scope = RequestScope.withoutDeadline();
         var recoveryA = budgets.admit("a", scope, LeaseBudgetProfile.RECOVERY_MEMORY);
         var recoveryB = budgets.admit("b", scope, LeaseBudgetProfile.RECOVERY_MEMORY);
         var startupScan = budgets.admit("b", scope, LeaseBudgetProfile.startupLeaseScanMemory())) {
      long remainingRegularMemory = regularMemory - startupMemory;
      assertThrows(BackendFailure.class, () -> budgets.admit("a", scope,
          new BudgetManager.Reservation(0, 1, 1, 0, Math.addExact(remainingRegularMemory, 1))));
      try (var windowA = budgets.admitReserved("a", scope, backfillAccount);
           var windowB = budgets.admitReserved("b", scope, backfillAccount);
           var stepA = budgets.admitReserved("a", scope, backfillAccount);
           var stepB = budgets.admitReserved("b", scope, backfillAccount);
           var expiryA = budgets.admitReserved("a", scope, LeaseBudgetProfile.EXPIRY_WORKER_MEMORY);
           var expiryB = budgets.admitReserved("b", scope, LeaseBudgetProfile.EXPIRY_WORKER_MEMORY)) {
        assertEquals(2 * LeaseBudgetProfile.RECOVERY_MEMORY.total()
            + LeaseBudgetProfile.startupLeaseScanMemory().total()
            + 2 * (LeaseBudgetProfile.eventBackfillMinimumMemory()
                + LeaseBudgetProfile.EXPIRY_WORKER_MEMORY.total()), budgets.snapshot().reservedBytes());
      }
    }
    assertEquals(0, budgets.snapshot().reservedBytes());
  }

  @Test
  void tableGroupsShareTheSameGlobalMemoryLimit() throws Exception {
    var resources = new RuntimeConfiguration.ResourceBudgets(65_536, 2, 30_000,
        4 << 20, 4 << 20, 2, 0);
    var configuration = new RuntimeConfiguration(1, List.of(
        new RuntimeConfiguration.Listener("first", "127.0.0.1", 0, "a"),
        new RuntimeConfiguration.Listener("second", "127.0.0.1", 0, "b")),
        List.of(), List.of(), List.of(), resources);
    var activation = new M3RuntimeConfiguration("1", 6_000, 1_000, 30_000, 30_000,
        256L << 20, 1_572_864, 2, 16, 60_000);
    var budgets = M3RuntimeBudgets.create(configuration, activation);
    var reservation = new BudgetManager.Reservation(0, 1, 1, 0, 160L << 20);
    try (var first = RequestScope.withoutDeadline(); var second = RequestScope.withoutDeadline();
         var admitted = budgets.admit("a", first, reservation)) {
      var failure = assertThrows(BackendFailure.class, () -> budgets.admit("b", second, reservation));
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
    }
    assertEquals(0, budgets.snapshot().reservedBytes());
    try (var scope = RequestScope.withoutDeadline();
         var account = budgets.admit("a", scope, new BudgetManager.Reservation(0, 1, 1, 0, 0));
         var queued = account.tryAcquire(BudgetManager.Work.queued(256)).orElseThrow()) {
      assertTrue(budgets.snapshot().queued() > 0, "internal scan scheduling needs a bounded queue");
    }
    assertEquals(0, budgets.snapshot().queued());
  }
}
