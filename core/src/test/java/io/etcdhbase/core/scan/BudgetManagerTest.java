package io.etcdhbase.core.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.StoragePage;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class BudgetManagerTest {
  private static final BudgetManager.Limits GLOBAL = new BudgetManager.Limits(4, 4, 2, 1, 2, 4096, 8192);
  private static final BudgetManager.Limits GROUP = new BudgetManager.Limits(2, 2, 1, 1, 1, 2048, 4096);
  private static final BudgetManager.Limits REQUEST = new BudgetManager.Limits(1, 2, 1, 1, 1, 1024, 2048);
  private static final BudgetManager.Reservation MEMORY = new BudgetManager.Reservation(512, 512, 512, 512);

  @Test
  void invalidReadLimitsCannotConsumeAdmissionOrMemory() throws Exception {
    var manager = new BudgetManager(GLOBAL, Map.of("alpha", GROUP), REQUEST);
    try (var scope = RequestScope.withoutDeadline()) {
      assertThrows(NullPointerException.class, () -> manager.admit("alpha", scope, MEMORY, null));
      assertEquals(0, manager.snapshot().requests());
      assertEquals(0, manager.snapshot().reservedBytes());
      try (var account = manager.admit("alpha", scope, MEMORY)) {
        assertTrue(account.isActive());
        assertEquals(1, manager.snapshot().requests());
      }
    }
    assertEquals(0, manager.snapshot().requests());
    assertEquals(0, manager.snapshot().reservedBytes());
  }

  @Test
  void readCompletionChecksAccountStateUnderTheChargingLock() throws Exception {
    var manager = new BudgetManager(GLOBAL, Map.of("alpha", GROUP), REQUEST);
    try (var scope = RequestScope.withoutDeadline()) {
      var account = manager.admit("alpha", scope, MEMORY);
      var observedActive = new CompletableFuture<Boolean>();
      var finishCharge = new CountDownLatch(1);
      var completion = new CompletableFuture<BackendFailure>();
      Thread worker = Thread.ofPlatform().daemon().start(() -> {
        observedActive.complete(account.isActive());
        try {
          finishCharge.await();
          account.charge(new StoragePage.Cost(1, 32, 1));
          completion.complete(null);
        } catch (BackendFailure failure) {
          completion.complete(failure);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          completion.completeExceptionally(interrupted);
        }
      });
      try {
        assertTrue(observedActive.get(5, TimeUnit.SECONDS));
        account.close();
        finishCharge.countDown();
        BackendFailure failure = completion.get(5, TimeUnit.SECONDS);
        assertNotNull(failure, "an earlier active observation cannot authorize a later charge");
        assertEquals(BackendFailure.Category.CANCELLED, failure.category());
      } finally {
        finishCharge.countDown();
        worker.join(5000);
        assertFalse(worker.isAlive());
      }
      assertEquals(0, manager.snapshot().requests());
      assertEquals(0, manager.snapshot().reservedBytes());
    }
  }

  @Test
  void admissionIsAtomicAcrossGlobalGroupAndRequestLimits() throws Exception {
    var manager = new BudgetManager(GLOBAL, Map.of("alpha", GROUP, "beta", GROUP), REQUEST);
    try (var scope = RequestScope.withoutDeadline(); var first = manager.admit("alpha", scope, MEMORY);
        var otherScope = RequestScope.withoutDeadline(); var second = manager.admit("alpha", otherScope, MEMORY)) {
      assertThrows(BackendFailure.class, () -> manager.admit("alpha", scope, MEMORY));
      assertEquals(2, manager.snapshot().requests());
      assertEquals(4096, manager.snapshot().reservedBytes());
      try (var permit = first.tryAcquire(BudgetManager.Work.scan()).orElseThrow()) {
        assertTrue(second.tryAcquire(BudgetManager.Work.scan()).isEmpty());
        assertEquals(1, manager.snapshot().scanners());
      }
      assertEquals(0, manager.snapshot().scanners());
      assertThrows(IllegalArgumentException.class, () -> manager.admit("unknown", scope, MEMORY));
    }
    assertEquals(0, manager.snapshot().requests());
    assertEquals(0, manager.snapshot().reservedBytes());
  }

  @Test
  void headAndOutputCannotConsumeTheReservedProgressPage() throws Exception {
    var manager = new BudgetManager(GLOBAL, Map.of("alpha", GROUP), REQUEST);
    try (var scope = RequestScope.withoutDeadline(); var account = manager.admit("alpha", scope, MEMORY);
        var heads = account.tryRetain(BudgetManager.Memory.HEAD, 512).orElseThrow();
        var output = account.tryRetain(BudgetManager.Memory.OUTPUT, 512).orElseThrow()) {
      assertTrue(account.tryRetain(BudgetManager.Memory.HEAD, 1).isEmpty());
      try (var page = account.tryRetain(BudgetManager.Memory.PAGE, 512).orElseThrow()) {
        assertEquals(1536, manager.snapshot().allocatedBytes());
      }
    }
    assertEquals(0, manager.snapshot().allocatedBytes());
  }

  @Test
  void cancellationKeepsAdmissionAndIoReservedUntilTheirActualOwnerReleases() throws Exception {
    var manager = new BudgetManager(GLOBAL, Map.of("alpha", GROUP), REQUEST);
    try (var scope = RequestScope.withoutDeadline()) {
      var account = manager.admit("alpha", scope, MEMORY);
      var operation = account.tryAcquire(BudgetManager.Work.scan()).orElseThrow();
      var bytes = account.tryRetain(BudgetManager.Memory.IO, 512).orElseThrow();
      scope.cancel();
      assertFalse(account.isActive());
      assertEquals(1, manager.snapshot().requests());
      assertEquals(1, manager.snapshot().scanners());
      operation.close();
      operation.close();
      assertEquals(1, manager.snapshot().requests());
      bytes.close();
      assertEquals(0, manager.snapshot().requests());
    }
  }

  @Test
  void rejectedReservationsCannotWrapOrAllocateAnUnboundedPool() {
    assertThrows(IllegalArgumentException.class, () -> new BudgetManager.Reservation(Long.MAX_VALUE, 1, 1, 1));
    var manager = new BudgetManager(GLOBAL, Map.of("alpha", GROUP), REQUEST);
    try (var scope = RequestScope.withoutDeadline()) {
      assertThrows(BackendFailure.class, () -> manager.admit("alpha", scope,
          new BudgetManager.Reservation(2048, 1, 1, 1)));
      assertEquals(0, manager.snapshot().reservedBytes());
    }
  }
}
