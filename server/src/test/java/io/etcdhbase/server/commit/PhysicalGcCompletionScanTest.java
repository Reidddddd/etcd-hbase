package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CompactionOperation;
import io.etcdhbase.core.commit.GcCheckpoint;
import io.etcdhbase.core.commit.GcCheckpointStore;
import io.etcdhbase.core.commit.HistoryGcCheckpoint;
import io.etcdhbase.core.commit.HistoryGcCheckpointStore;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.SystemRecordClosure.Domain;
import io.etcdhbase.core.request.RequestScope;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class PhysicalGcCompletionScanTest {
  private static final GcCheckpoint.RegistryBinding REGISTRIES = new GcCheckpoint.RegistryBinding(
      bytes(32, 1), bytes(16, 2), bytes(16, 3), bytes(16, 4));

  @Test
  void kvCompletionAloneCannotCompleteAnySystemDomain() {
    var fixture = new Fixture();
    try (var scope = RequestScope.withoutDeadline()) {
      assertEquals(PhysicalCompactionWaiter.Progress.ADVANCED, fixture.scan.readNext(target(10, 20), scope).join());
      verifyNoInteractions(fixture.system);
      assertEquals(PhysicalCompactionWaiter.Progress.ADVANCED, fixture.scan.readNext(target(10, 20), scope).join());
      assertEquals(PhysicalCompactionWaiter.Progress.WAITING, fixture.scan.readNext(target(10, 20), scope).join());
      verify(fixture.system).read("group", Domain.INVOCATION, 0, scope);
    }
  }

  @Test
  void everyDomainAndBucketNeedsBothTargetBounds() {
    for (var blocked : Domain.values()) {
      for (int bucket = 0; bucket < 2; bucket++) {
        var fixture = new Fixture();
        int missingBucket = bucket;
        var observed = new HashSet<String>();
        when(fixture.system.read(anyString(), any(), anyInt(), any())).thenAnswer(call -> {
          Domain domain = call.getArgument(1);
          int selectedBucket = call.getArgument(2);
          observed.add(domain + ":" + selectedBucket);
          var completion = domain == blocked && selectedBucket == missingBucket
              ? new GcCheckpoint.Completion(10, 19, 1) : new GcCheckpoint.Completion(10, 20, 1);
          return CompletableFuture.completedFuture(Optional.of(checkpoint(domain, selectedBucket, 2, REGISTRIES, completion)));
        });
        try (var scope = RequestScope.withoutDeadline()) {
          var target = target(10, 20);
          assertEquals(PhysicalCompactionWaiter.Progress.ADVANCED, fixture.scan.readNext(target, scope).join());
          assertEquals(PhysicalCompactionWaiter.Progress.ADVANCED, fixture.scan.readNext(target, scope).join());
          PhysicalCompactionWaiter.Progress progress;
          do { progress = fixture.scan.readNext(target, scope).join(); }
          while (progress == PhysicalCompactionWaiter.Progress.ADVANCED);
          assertEquals(PhysicalCompactionWaiter.Progress.WAITING, progress, blocked + ":" + bucket);
          fixture.completeSystem(observed);
          for (int shard = 0; shard < 14; shard++) {
            assertEquals(shard == 13 ? PhysicalCompactionWaiter.Progress.COMPLETE
                : PhysicalCompactionWaiter.Progress.ADVANCED, fixture.scan.readNext(target, scope).join());
          }
          assertEquals(14, observed.size());
        }
      }
    }
  }

  @Test
  void eofAndDefaultZeroBoundsAreNotCompletionReceipts() {
    var fixture = new Fixture();
    var completion = new AtomicReference<>(GcCheckpoint.Completion.none());
    when(fixture.system.read(anyString(), any(), anyInt(), any())).thenAnswer(call ->
        CompletableFuture.completedFuture(Optional.of(checkpoint(call.getArgument(1), call.getArgument(2),
            2, REGISTRIES, completion.get()))));
    try (var scope = RequestScope.withoutDeadline()) {
      var target = target(0, 0);
      fixture.scan.readNext(target, scope).join();
      fixture.scan.readNext(target, scope).join();
      assertEquals(PhysicalCompactionWaiter.Progress.WAITING, fixture.scan.readNext(target, scope).join());
      completion.set(new GcCheckpoint.Completion(0, 0, 1));
      for (int shard = 0; shard < 13; shard++) fixture.scan.readNext(target, scope).join();
      assertEquals(PhysicalCompactionWaiter.Progress.COMPLETE, fixture.scan.readNext(target, scope).join());
    }
  }

  @Test
  void profileRegistriesNamespaceAndTopologyCannotTransferCompletion() {
    var mismatches = java.util.List.of(
        new GcCheckpoint.RegistryBinding(bytes(32, 9), REGISTRIES.kvIdentity(), REGISTRIES.eventIdentity(), REGISTRIES.eventNamespace()),
        new GcCheckpoint.RegistryBinding(REGISTRIES.profile(), bytes(16, 9), REGISTRIES.eventIdentity(), REGISTRIES.eventNamespace()),
        new GcCheckpoint.RegistryBinding(REGISTRIES.profile(), REGISTRIES.kvIdentity(), bytes(16, 9), REGISTRIES.eventNamespace()),
        new GcCheckpoint.RegistryBinding(REGISTRIES.profile(), REGISTRIES.kvIdentity(), REGISTRIES.eventIdentity(), bytes(16, 9)),
        REGISTRIES);
    for (var mismatch : mismatches) {
      var fixture = new Fixture();
      when(fixture.system.read(anyString(), any(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(
          Optional.of(checkpoint(Domain.INVOCATION, 0, mismatch == REGISTRIES ? 3 : 2, mismatch,
              new GcCheckpoint.Completion(10, 20, 1)))));
      try (var scope = RequestScope.withoutDeadline()) {
        fixture.scan.readNext(target(10, 20), scope).join();
        fixture.scan.readNext(target(10, 20), scope).join();
        assertInstanceOf(IllegalArgumentException.class, assertThrows(CompletionException.class,
            () -> fixture.scan.readNext(target(10, 20), scope).join()).getCause());
      }
    }
  }

  @Test
  void durableOlderReceiptSurvivesANewerPendingSweepAndOwnerTakeover() {
    var fixture = new Fixture();
    fixture.completeSystem(new HashSet<>());
    try (var scope = RequestScope.withoutDeadline()) {
      var target = target(10, 20);
      for (int shard = 0; shard < 15; shard++) {
        assertEquals(PhysicalCompactionWaiter.Progress.ADVANCED, fixture.scan.readNext(target, scope).join());
      }
      assertEquals(PhysicalCompactionWaiter.Progress.COMPLETE, fixture.scan.readNext(target, scope).join());
    }
  }

  @Test
  void storageFailureAndForeignTargetCannotBecomeCompletion() {
    var fixture = new Fixture();
    when(fixture.system.read(anyString(), any(), anyInt(), any())).thenReturn(
        CompletableFuture.failedFuture(new IllegalStateException("read failed")));
    try (var scope = RequestScope.withoutDeadline()) {
      fixture.scan.readNext(target(10, 20), scope).join();
      fixture.scan.readNext(target(10, 20), scope).join();
      assertInstanceOf(IllegalStateException.class, assertThrows(CompletionException.class,
          () -> fixture.scan.readNext(target(10, 20), scope).join()).getCause());
      var foreign = target(10, 20);
      when(foreign.profile()).thenReturn(bytes(32, 9));
      assertInstanceOf(IllegalArgumentException.class, assertThrows(CompletionException.class,
          () -> fixture.scan.readNext(foreign, scope).join()).getCause());
    }
  }

  private static final class Fixture {
    final HistoryGcCheckpointStore history = mock(HistoryGcCheckpointStore.class);
    final GcCheckpointStore system = mock(GcCheckpointStore.class);
    final PhysicalGcCompletionScan scan = new PhysicalGcCompletionScan("group", history, system, REGISTRIES, 2);

    Fixture() {
      var receipt = mock(HistoryGcCheckpoint.class);
      when(receipt.completedGeneration()).thenReturn(1L);
      when(receipt.completedRevision()).thenReturn(10L);
      when(receipt.completedSequence()).thenReturn(20L);
      when(history.read(anyInt(), any())).thenReturn(CompletableFuture.completedFuture(Optional.of(receipt)));
      when(system.read(anyString(), any(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    }

    void completeSystem(java.util.Set<String> observed) {
      doAnswer(call -> {
        Domain domain = call.getArgument(1);
        int bucket = call.getArgument(2);
        observed.add(domain + ":" + bucket);
        return CompletableFuture.completedFuture(Optional.of(checkpoint(domain, bucket, 2, REGISTRIES,
            new GcCheckpoint.Completion(10, 20, 1))));
      }).when(system).read(anyString(), any(), anyInt(), any());
    }
  }

  private static GcCheckpoint checkpoint(Domain domain, int bucket, int buckets,
      GcCheckpoint.RegistryBinding registries, GcCheckpoint.Completion completion) {
    return new GcCheckpoint("group", domain, 7, bucket, buckets,
        new GcCheckpoint.Plan(InvocationId.create(), 9, 200, 100, registries),
        GcCheckpoint.Position.start(), GcCheckpoint.Position.start(), GcCheckpoint.Phase.SCANNING,
        2, 1, true, 1, completion);
  }

  private static CompactionOperation target(long revision, long sequence) {
    var operation = mock(CompactionOperation.class);
    when(operation.revision()).thenReturn(revision);
    when(operation.baseSequence()).thenReturn(sequence);
    when(operation.profile()).thenReturn(REGISTRIES.profile());
    return operation;
  }

  private static ByteKey bytes(int size, int value) {
    var bytes = new byte[size];
    java.util.Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }
}
