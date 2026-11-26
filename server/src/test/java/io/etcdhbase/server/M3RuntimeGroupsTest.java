package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class M3RuntimeGroupsTest {
  @Test
  void secureGroupWaitsForRecoveryAndDrainsItsProducerBeforeClosing() throws Exception {
    var resources = new M3RuntimeGroups.GroupResources();
    var started = new CountDownLatch(1);
    var logical = new CompletableFuture<Void>();
    var physical = new CompletableFuture<Void>();
    resources.configureRecovery(scope -> {
      scope.trackPhysicalProducer(physical);
      started.countDown();
      return logical;
    });
    assertFalse(resources.recoveryReady());
    resources.startRecovery(5_000);
    assertTrue(started.await(2, TimeUnit.SECONDS));
    assertFalse(resources.recoveryReady());
    assertFalse(resources.initialRecoveryReadiness().isDone());
    resources.stopRecovery();
    logical.complete(null);
    assertFalse(resources.recoveryDrained().isDone());
    assertThrows(IllegalStateException.class, resources::close);
    physical.complete(null);
    resources.recoveryDrained().get(2, TimeUnit.SECONDS);
    resources.close();
    assertFalse(resources.recoveryReady());
  }

  @Test
  void backendFactoryWaitsForRuntimeScannerAndRequestDrain() throws Exception {
    var runtime = new CompletableFuture<Void>();
    var scans = new CompletableFuture<Void>();
    var requests = new CompletableFuture<Void>();
    var closed = new AtomicInteger();
    var completion = M3RuntimeGroups.closeWhenDrained(runtime, scans, requests, closed::incrementAndGet);
    runtime.complete(null);
    scans.complete(null);
    assertFalse(completion.isDone());
    assertEquals(0, closed.get());
    requests.complete(null);
    completion.get(2, TimeUnit.SECONDS);
    assertEquals(1, closed.get());

    var failed = new CompletableFuture<Void>();
    var unsafeClose = new AtomicInteger();
    var rejected = M3RuntimeGroups.closeWhenDrained(CompletableFuture.completedFuture(null),
        failed, CompletableFuture.completedFuture(null), unsafeClose::incrementAndGet);
    failed.completeExceptionally(new IllegalStateException("scan cleanup failed"));
    assertThrows(CompletionException.class, rejected::join);
    assertEquals(0, unsafeClose.get());
  }
}
