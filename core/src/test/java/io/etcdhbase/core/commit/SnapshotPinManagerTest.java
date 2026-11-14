package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SnapshotPinManagerTest {
  private static final ByteKey PROFILE = digest(3);
  private static final ByteKey AUTHORIZATION = digest(9);

  @Test
  void durableFloorsProtectExistingViewsAndRejectNewOlderViews() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(3, 2, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());
    ReadView oldView = stableView(5);

    try (RequestScope scope = scope()) {
      ReadViewPinRegistry.Pin pin = manager.register(oldView, scope).join();
      SnapshotGcBoundary advanced = manager.advanceRegistrationFloors(8, 4, scope).join();
      assertEquals(5, advanced.safeRevision());
      assertEquals(4, advanced.safeSequence());
      assertEquals(1, advanced.protections().size());
      assertEquals(5, advanced.protections().getFirst().requiredRevision());

      SnapshotPinManager reopened = SnapshotPinManager.open(records,
          new SnapshotPinConfiguration(3, 2, Duration.ofSeconds(2)), scope).join();
      SnapshotGcBoundary afterReopen = reopened.readBoundary(scope).join();
      assertEquals(5, afterReopen.safeRevision());
      assertEquals(4, afterReopen.safeSequence());
      assertEquals(advanced.protections(), afterReopen.protections(),
          "reopening must preserve active pins and durable floors");

      Throwable rejected = failure(() -> manager.register(stableView(7), scope).join());
      assertInstanceOf(IllegalStateException.class, rejected);

      pin.complete(scope).join();
      SnapshotGcBoundary afterCompletion = reopened.readBoundary(scope).join();
      assertEquals(8, afterCompletion.safeRevision());
      assertEquals(4, afterCompletion.safeSequence());
      assertTrue(afterCompletion.protections().isEmpty());
    }
  }

  @Test
  void selectedMetadataDecisionUsesItsOwnSequenceBoundaryAndReference() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());
    ReadView selected = selectedMetadataView(5);

    try (RequestScope scope = scope()) {
      ReadViewPinRegistry.Pin pin = manager.register(selected, scope).join();
      SnapshotGcBoundary boundary = manager.advanceRegistrationFloors(8, 3, scope).join();
      assertEquals(5, boundary.safeRevision());
      assertEquals(1, boundary.safeSequence());
      assertEquals(1, boundary.protections().size());
      SnapshotGcBoundary.Protection protection = boundary.protections().getFirst();
      assertEquals(1, protection.requiredSequence());
      assertTrue(protection.transactionId().isPresent());
      assertTrue(protection.rootIdentity().isPresent());
      assertTrue(protection.rootDigest().isPresent());

      Throwable rejected = failure(() -> manager.register(selectedMetadataView(5), scope).join());
      assertInstanceOf(IllegalStateException.class, rejected);
      pin.close();
      pin.releaseCompletion().toCompletableFuture().join();
    }
  }

  @Test
  void auxiliaryPinRetainsStableSequenceWithoutASelectedRootAcrossReopen() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());
    var stable = new ReadView(PROFILE, AUTHORIZATION, 3, 3, 5, 5, 0,
        Optional.empty(), Optional.empty(), 0);
    try (RequestScope scope = scope()) {
      ReadViewPinRegistry.Pin pin = manager.registerAuxiliary(stable, scope).join();
      SnapshotGcBoundary boundary = manager.advanceRegistrationFloors(8, 5, scope).join();
      assertEquals(3, boundary.safeSequence());
      assertEquals(3, boundary.protections().getFirst().requiredSequence());
      assertTrue(boundary.protections().getFirst().transactionId().isEmpty());
      var reopened = SnapshotPinManager.open(records, configuration, scope).join();
      assertEquals(boundary, reopened.readBoundary(scope).join());
      assertInstanceOf(IllegalStateException.class,
          failure(() -> reopened.registerAuxiliary(stable, scope).join()));
      pin.complete(scope).join();
      assertEquals(5, reopened.readBoundary(scope).join().safeSequence());
    }
  }

  @Test
  void auxiliaryPinProtectsStableBaseAndSelectedRootTogether() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());
    TransactionId transaction = TransactionId.create();
    ManifestRoot root = ManifestRoot.create(transaction, 1, 3, 5, 0, 0, Optional.empty());
    var decision = new CommitDecision(transaction, InvocationId.create(), 4, 5, 5,
        root.systemIdentity(), root.digest(), false);
    var selected = new ReadView(PROFILE, AUTHORIZATION, 4, 3, 5, 5, 0,
        Optional.of(decision), Optional.of(root), 0);
    try (RequestScope scope = scope()) {
      ReadViewPinRegistry.Pin pin = manager.registerAuxiliary(selected, scope).join();
      var boundary = manager.advanceRegistrationFloors(8, 6, scope).join();
      assertEquals(3, boundary.safeSequence());
      assertEquals(transaction, boundary.protections().getFirst().transactionId().orElseThrow());
      assertEquals(root.systemIdentity(), boundary.protections().getFirst().rootIdentity().orElseThrow());
      pin.complete(scope).join();
    }
  }

  @Test
  void shardCodecKeepsLegacyFormatUnlessSequenceHasNoSelectedRoot() {
    var empty = new SnapshotPinShard(0, 1, 0, 0, java.util.List.of(SnapshotPinShard.Slot.empty()));
    var legacy = SnapshotPinRecordCodec.encodeShard(empty);
    assertEquals(1, java.nio.ByteBuffer.wrap(legacy.bytes()).getInt(4));
    assertEquals(empty, SnapshotPinRecordCodec.decodeShard(legacy));
    var slot = SnapshotPinShard.Slot.active(1, ByteKey.copyOf(new byte[16]), 5, 3,
        digest(1), 1000, Optional.empty(), Optional.empty(), Optional.empty());
    var auxiliary = new SnapshotPinShard(0, 2, 0, 0, java.util.List.of(slot));
    var encoded = SnapshotPinRecordCodec.encodeShard(auxiliary);
    assertEquals(2, java.nio.ByteBuffer.wrap(encoded.bytes()).getInt(4));
    assertEquals(auxiliary, SnapshotPinRecordCodec.decodeShard(encoded));
  }

  @Test
  void expiryAloneDoesNotReleaseProtectionAndRevocationFencesTheOldLease() {
    var records = new MemoryRecords();
    var clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofSeconds(1));
    SnapshotPinManager manager = open(records, configuration, clock);

    try (RequestScope scope = scope()) {
      ReadViewPinRegistry.Pin pin = manager.register(stableView(5), scope).join();
      manager.advanceRegistrationFloors(9, 4, scope).join();
      clock.advance(Duration.ofSeconds(2));
      SnapshotGcBoundary expiredButNotRevoked = manager.readBoundary(scope).join();
      assertEquals(5, expiredButNotRevoked.safeRevision());
      assertEquals(4, expiredButNotRevoked.safeSequence());
      assertEquals(1, expiredButNotRevoked.protections().size());

      SnapshotGcBoundary revoked = manager.revokeExpiredPins(scope).join();
      assertTrue(revoked.protections().isEmpty());
      assertEquals(9, revoked.safeRevision());
      assertEquals(4, revoked.safeSequence());
      assertInstanceOf(BackendFailure.class, failure(() -> pin.renew(scope).join()));
    }
  }

  @Test
  void terminalSlotCanBeReusedOnlyWithANewerFencingGeneration() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 1, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());

    try (RequestScope scope = scope()) {
      ReadViewPinRegistry.Pin first = manager.register(stableView(5), scope).join();
      Throwable capacity = failure(() -> manager.register(stableView(6), scope).join());
      assertInstanceOf(BackendFailure.class, capacity);
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
          ((BackendFailure) capacity).category());

      first.complete(scope).join();
      ReadViewPinRegistry.Pin second = manager.register(stableView(6), scope).join();
      assertTrue(second.generation() > first.generation());
      assertInstanceOf(BackendFailure.class, failure(() -> first.renew(scope).join()));
      second.complete(scope).join();
    }
  }

  @Test
  void unknownCasRepliesAreResolvedFromTheDurableSlot() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());

    try (RequestScope scope = scope()) {
      records.loseNextCasReplyAfterWrite();
      ReadViewPinRegistry.Pin pin = manager.register(stableView(5), scope).join();
      assertEquals(1, manager.readBoundary(scope).join().protections().size());

      records.loseNextCasReplyAfterWrite();
      pin.complete(scope).join();
      assertTrue(manager.readBoundary(scope).join().protections().isEmpty());
      pin.releaseCompletion().toCompletableFuture().join();
    }
  }

  @Test
  void registrationRacesFloorAdvanceOnTheSameShardCas() throws Exception {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 2, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());
    records.pauseNextShardReads(2);

    CompletableFuture<Optional<ReadViewPinRegistry.Pin>> registration = CompletableFuture.supplyAsync(
        () -> registerUnlessFloorAdvanced(manager));
    CompletableFuture<SnapshotGcBoundary> floorAdvance = CompletableFuture.supplyAsync(() -> {
      try (RequestScope scope = scope()) {
        return manager.advanceRegistrationFloors(6, 0, scope).join();
      }
    });

    SnapshotGcBoundary boundary = floorAdvance.get(5, TimeUnit.SECONDS);
    Optional<ReadViewPinRegistry.Pin> pin = registration.get(5, TimeUnit.SECONDS);
    if (pin.isPresent()) {
      assertEquals(5, boundary.safeRevision());
      assertEquals(1, boundary.protections().size());
      pin.orElseThrow().close();
      pin.orElseThrow().releaseCompletion().toCompletableFuture().join();
    } else {
      assertEquals(6, boundary.safeRevision());
      assertTrue(boundary.protections().isEmpty());
    }
  }

  private static Optional<ReadViewPinRegistry.Pin> registerUnlessFloorAdvanced(
      SnapshotPinManager manager) {
    try (RequestScope scope = scope()) {
      try {
        return Optional.of(manager.register(stableView(5), scope).join());
      } catch (CompletionException rejected) {
        Throwable cause = CompletionFailures.unwrapCompletion(rejected);
        if (cause instanceof IllegalStateException
            && "read view is below the durable snapshot registration floor".equals(cause.getMessage())) {
          return Optional.empty();
        }
        throw rejected;
      }
    }
  }

  @Test
  void storedConfigurationIsImmutableAndShardRowsMustBeComplete() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(2, 2, Duration.ofSeconds(2));
    open(records, configuration, Clock.systemUTC());

    try (RequestScope scope = scope()) {
      Throwable mismatch = failure(() -> SnapshotPinManager.open(records,
          new SnapshotPinConfiguration(2, 3, Duration.ofSeconds(2)), scope).join());
      assertInstanceOf(IllegalStateException.class, mismatch);
      SnapshotPinManager manager = SnapshotPinManager.open(records, configuration, scope).join();
      assertEquals(0, manager.readBoundary(scope).join().safeRevision());
      ByteKey missingShard = activeShardIdentity(records, 1);
      records.remove(SystemRecordType.SNAPSHOT_PIN, missingShard);
      assertInstanceOf(IllegalStateException.class,
          failure(() -> manager.readBoundary(scope).join()));
      assertInstanceOf(IllegalStateException.class,
          failure(() -> SnapshotPinManager.open(records, configuration, scope).join()));
      assertFalse(records.contains(SystemRecordType.SNAPSHOT_PIN, missingShard),
          "reopen must not replace a missing configured shard with an empty row");
    }
  }

  @Test
  void interruptedInitialShardCreationCanResumeBeforeTheConfigurationReadyMarker() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(2, 2, Duration.ofSeconds(2));
    records.failNextShardCreate(1);

    try (RequestScope scope = scope()) {
      assertInstanceOf(IllegalStateException.class,
          failure(() -> SnapshotPinManager.initializeFresh(records, configuration, scope).join()));
      assertTrue(records.containsAnyShard(0));
      assertFalse(records.contains(SystemRecordType.SNAPSHOT_PIN, configurationIdentity()),
          "the ready marker is written only after every initial shard exists");
      assertFalse(records.containsAnyShard(1));

      SnapshotPinManager reopened = SnapshotPinManager.open(records, configuration, scope).join();
      SnapshotGcBoundary boundary = reopened.readBoundary(scope).join();
      assertEquals(0, boundary.safeRevision());
      assertEquals(0, boundary.safeSequence());
      assertTrue(records.contains(SystemRecordType.SNAPSHOT_PIN, configurationIdentity()));
    }
  }

  @Test
  void unknownInitializationReservationCanBeRetried() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 1, Duration.ofSeconds(2));
    records.loseNextCasReplyAfterWrite();

    try (RequestScope scope = scope()) {
      BackendFailure unknown = assertInstanceOf(BackendFailure.class,
          failure(() -> SnapshotPinManager.initializeFresh(records, configuration, scope).join()));
      assertEquals(BackendFailure.Category.UNKNOWN, unknown.category());
      assertEquals(0, SnapshotPinManager.open(records, configuration, scope).join()
          .readBoundary(scope).join().safeRevision());
    }
  }

  @Test
  void unknownInitializationSealIsResolvedFromTheDurableWitness() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 1, Duration.ofSeconds(2));
    records.loseNextSealReplyAfterWrite();

    try (RequestScope scope = scope()) {
      SnapshotPinManager manager = SnapshotPinManager.initializeFresh(records, configuration, scope).join();
      assertEquals(0, manager.readBoundary(scope).join().safeRevision());
      assertEquals(0, SnapshotPinManager.open(records, configuration, scope).join()
          .readBoundary(scope).join().safeRevision());
    }
  }

  @Test
  void definiteInitializationSealFailureKeepsItsCategory() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 1, Duration.ofSeconds(2));
    records.failNextSealBeforeWrite(BackendFailure.Category.PERMISSION);

    try (RequestScope scope = scope()) {
      BackendFailure denied = assertInstanceOf(BackendFailure.class,
          failure(() -> SnapshotPinManager.initializeFresh(records, configuration, scope).join()));
      assertEquals(BackendFailure.Category.PERMISSION, denied.category());
      assertEquals(0, SnapshotPinManager.open(records, configuration, scope).join()
          .readBoundary(scope).join().safeRevision());
    }
  }

  @Test
  void cancelledOpenDoesNotCreateInitializationState() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 1, Duration.ofSeconds(2));
    RequestScope scope = scope();
    scope.close();

    BackendFailure cancelled = assertInstanceOf(BackendFailure.class,
        failure(() -> SnapshotPinManager.open(records, configuration, scope).join()));
    assertEquals(BackendFailure.Category.CANCELLED, cancelled.category());
    assertFalse(records.contains(SystemRecordType.SNAPSHOT_PIN, initializationIdentity()));
  }

  @Test
  void emptyStoreRequiresExplicitBootstrap() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 1, Duration.ofSeconds(2));

    try (RequestScope scope = scope()) {
      assertInstanceOf(IllegalStateException.class,
          failure(() -> SnapshotPinManager.open(records, configuration, scope).join()));
      assertFalse(records.contains(SystemRecordType.SNAPSHOT_PIN, initializationIdentity()));
      assertEquals(0, SnapshotPinManager.initializeFresh(records, configuration, scope).join()
          .readBoundary(scope).join().safeRevision());
    }
  }

  @Test
  void abandonedInitializationGenerationsAreBounded() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(2, 1, Duration.ofSeconds(2));

    try (RequestScope scope = scope()) {
      for (int attempt = 0; attempt < 32; attempt++) {
        records.failNextShardCreate(1);
        assertInstanceOf(IllegalStateException.class,
            failure(() -> SnapshotPinManager.initializeFresh(records, configuration, scope).join()));
      }
      assertEquals(32, records.countShardRows(0));
      BackendFailure exhausted = assertInstanceOf(BackendFailure.class,
          failure(() -> SnapshotPinManager.open(records, configuration, scope).join()));
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT, exhausted.category());
      assertEquals(32, records.countShardRows(0));
      assertFalse(records.contains(SystemRecordType.SNAPSHOT_PIN, configurationIdentity()));
    }
  }

  @Test
  void legacyShardWithoutConfigurationFailsClosed() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 1, Duration.ofSeconds(2));
    ByteKey legacy = ByteKey.copyOf(java.nio.ByteBuffer.allocate(Integer.BYTES * 2)
        .putInt(1).putInt(0).array());

    try (RequestScope scope = scope()) {
      records.createIfAbsent(SystemRecordType.SNAPSHOT_PIN, legacy,
          ByteKey.copyOf(new byte[] {1}), scope).join();
      assertInstanceOf(IllegalStateException.class,
          failure(() -> SnapshotPinManager.initializeFresh(records, configuration, scope).join()));
      assertFalse(records.contains(SystemRecordType.SNAPSHOT_PIN, configurationIdentity()));
      assertFalse(records.contains(SystemRecordType.SNAPSHOT_PIN, initializationIdentity()));
    }
  }

  @Test
  void legacyConfigurationRequiresExplicitMigration() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 1, Duration.ofSeconds(2));
    ByteKey legacy = ByteKey.copyOf(java.util.HexFormat.of().parseHex(
        "5350433100000001000000010000000100000000000007d0"
            + "eadf97e14e5dce29fc77ab614c34877e6e882de742275f7bc8a0fea717da58bc"));

    try (RequestScope scope = scope()) {
      records.createIfAbsent(SystemRecordType.SNAPSHOT_PIN, configurationIdentity(), legacy, scope).join();
      assertInstanceOf(IllegalArgumentException.class,
          failure(() -> SnapshotPinManager.open(records, configuration, scope).join()));
      assertEquals(legacy, records.value(SystemRecordType.SNAPSHOT_PIN, configurationIdentity()));
      assertFalse(records.contains(SystemRecordType.SNAPSHOT_PIN, initializationIdentity()));
    }
  }

  @Test
  void missingInitializationWitnessCannotReopenConfiguredShards() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(1, 1, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());

    try (RequestScope scope = scope()) {
      manager.advanceRegistrationFloors(8, 4, scope).join();
      records.remove(SystemRecordType.SNAPSHOT_PIN, initializationIdentity());
      assertInstanceOf(IllegalStateException.class,
          failure(() -> SnapshotPinManager.open(records, configuration, scope).join()));
      assertEquals(8, manager.readBoundary(scope).join().safeRevision());
      assertFalse(records.contains(SystemRecordType.SNAPSHOT_PIN, initializationIdentity()));
    }
  }

  @Test
  void missingBothMetadataRecordsDoesNotBootstrapAnActiveRegistry() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(2, 2, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());

    try (RequestScope scope = scope()) {
      manager.advanceRegistrationFloors(8, 4, scope).join();
      records.remove(SystemRecordType.SNAPSHOT_PIN, configurationIdentity());
      records.remove(SystemRecordType.SNAPSHOT_PIN, initializationIdentity());

      assertInstanceOf(IllegalStateException.class,
          failure(() -> SnapshotPinManager.open(records, configuration, scope).join()));
      assertFalse(records.contains(SystemRecordType.SNAPSHOT_PIN, configurationIdentity()));
      assertFalse(records.contains(SystemRecordType.SNAPSHOT_PIN, initializationIdentity()));
      assertEquals(8, manager.readBoundary(scope).join().safeRevision());
    }
  }

  @Test
  void missingConfigurationCannotReinitializeNonEmptyShardState() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(2, 2, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());

    try (RequestScope scope = scope()) {
      manager.advanceRegistrationFloors(8, 4, scope).join();
      records.remove(SystemRecordType.SNAPSHOT_PIN, configurationIdentity());

      assertInstanceOf(IllegalStateException.class,
          failure(() -> SnapshotPinManager.open(records, configuration, scope).join()));
      assertFalse(records.contains(SystemRecordType.SNAPSHOT_PIN, configurationIdentity()),
          "a missing ready marker cannot legitimize non-empty shard state");
      SnapshotGcBoundary retained = manager.readBoundary(scope).join();
      assertEquals(8, retained.safeRevision());
      assertEquals(4, retained.safeSequence());
    }
  }

  @Test
  void missingConfigurationAndShardCannotChangeAnActiveRegistryBeforeOpenFails() {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(2, 2, Duration.ofSeconds(2));
    SnapshotPinManager manager = open(records, configuration, Clock.systemUTC());

    try (RequestScope scope = scope()) {
      ReadViewPinRegistry.Pin pin = manager.register(stableView(5), scope).join();
      try {
        SnapshotGcBoundary before = manager.advanceRegistrationFloors(8, 4, scope).join();
        int retainedIndex = before.protections().getFirst().shard();
        ByteKey retainedShard = activeShardIdentity(records, retainedIndex);
        ByteKey retainedValue = records.value(SystemRecordType.SNAPSHOT_PIN, retainedShard);
        ByteKey missingShard = activeShardIdentity(records, 1 - retainedIndex);
        records.remove(SystemRecordType.SNAPSHOT_PIN, configurationIdentity());
        records.remove(SystemRecordType.SNAPSHOT_PIN, missingShard);

        assertInstanceOf(IllegalStateException.class,
            failure(() -> SnapshotPinManager.open(records, configuration, scope).join()));
        assertFalse(records.contains(SystemRecordType.SNAPSHOT_PIN, missingShard),
            "a failed reopen must not recreate the missing shard before finding durable state");
        assertEquals(retainedValue, records.value(SystemRecordType.SNAPSHOT_PIN, retainedShard));
        assertInstanceOf(IllegalStateException.class, failure(() -> manager.readBoundary(scope).join()));
        assertOlderViewsRejected(manager, scope);
      } finally {
        pin.close();
        pin.releaseCompletion().toCompletableFuture().join();
      }
    }
  }

  @Test
  void lateInitialCreateCannotRestoreAMissingShardOfAnEstablishedRegistry() throws Exception {
    var records = new MemoryRecords();
    var configuration = new SnapshotPinConfiguration(2, 2, Duration.ofSeconds(2));

    try (RequestScope scope = scope()) {
      records.pauseNextShardCreates(2);
      CompletableFuture<SnapshotPinManager> delayed =
          SnapshotPinManager.initializeFresh(records, configuration, scope);
      try {
        records.createEntered().get(5, TimeUnit.SECONDS);
        SnapshotPinManager established = SnapshotPinManager.open(records, configuration, scope).join();
        ReadViewPinRegistry.Pin pin = established.register(stableView(5), scope).join();
        try {
          SnapshotGcBoundary advanced = established.advanceRegistrationFloors(8, 4, scope).join();
          assertEquals(5, advanced.safeRevision());
          assertEquals(4, advanced.safeSequence());
          int retainedIndex = advanced.protections().getFirst().shard();
          ByteKey missingShard = activeShardIdentity(records, 1 - retainedIndex);
          ByteKey retainedShard = activeShardIdentity(records, retainedIndex);
          ByteKey retainedValue = records.value(SystemRecordType.SNAPSHOT_PIN, retainedShard);
          records.remove(SystemRecordType.SNAPSHOT_PIN, missingShard);
          records.releaseCreate();

          assertFalse(records.contains(SystemRecordType.SNAPSHOT_PIN, missingShard),
              "a delayed first-initialization write must not recreate the established shard");
          assertEquals(retainedValue, records.value(SystemRecordType.SNAPSHOT_PIN, retainedShard));
          assertInstanceOf(IllegalStateException.class,
              failure(() -> established.readBoundary(scope).join()));
          assertInstanceOf(IllegalStateException.class, failure(delayed::join));
          assertOlderViewsRejected(established, scope);
        } finally {
          pin.close();
          pin.releaseCompletion().toCompletableFuture().join();
        }
      } finally {
        records.releaseCreate();
      }
    }
  }

  @Test
  void corruptedDurableShardFailsClosed() {
    var records = new MemoryRecords();
    SnapshotPinManager manager = open(records,
        new SnapshotPinConfiguration(1, 2, Duration.ofSeconds(2)), Clock.systemUTC());
    records.corrupt(SystemRecordType.SNAPSHOT_PIN, activeShardIdentity(records, 0));

    try (RequestScope scope = scope()) {
      assertInstanceOf(IllegalArgumentException.class,
          failure(() -> manager.readBoundary(scope).join()));
    }
  }

  @Test
  void completionCasLosingToDurableRevocationCannotReportSuccess() throws Exception {
    var records = new MemoryRecords();
    var clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
    SnapshotPinManager manager = open(records,
        new SnapshotPinConfiguration(1, 1, Duration.ofSeconds(1)), clock);

    try (RequestScope scope = scope()) {
      ReadViewPinRegistry.Pin pin = manager.register(stableView(5), scope).join();
      records.pauseNextCompareAndSet();
      CompletableFuture<Void> casEntered = records.casEntered();
      CompletableFuture<Void> completion = pin.complete(scope);
      casEntered.get(5, TimeUnit.SECONDS);

      clock.advance(Duration.ofSeconds(2));
      SnapshotGcBoundary revoked = manager.revokeExpiredPins(scope).join();
      assertTrue(revoked.protections().isEmpty());

      records.releaseCompareAndSet();
      BackendFailure rejected = assertInstanceOf(BackendFailure.class,
          failure(() -> completion.join()));
      assertEquals(BackendFailure.Category.CANCELLED, rejected.category());
      assertTrue(manager.readBoundary(scope).join().protections().isEmpty());

      pin.close();
      pin.releaseCompletion().toCompletableFuture().join();
    }
  }

  private static SnapshotPinManager open(MemoryRecords records,
      SnapshotPinConfiguration configuration, Clock clock) {
    try (RequestScope scope = scope()) {
      return SnapshotPinManager.initializeFresh(records, configuration, clock, scope).join();
    }
  }

  private static ReadView stableView(long revision) {
    return ReadView.capture(CommitState.initial(PROFILE, revision), revision, AUTHORIZATION,
        Optional.empty(), Optional.empty());
  }

  private static ReadView selectedMetadataView(long revision) {
    TransactionId transaction = TransactionId.create();
    ManifestRoot root = ManifestRoot.create(transaction, 1, 0, revision, 0, 0, Optional.empty());
    PublicationSlot slot = new PublicationSlot(transaction, InvocationId.create(), 1,
        0, revision, 1, revision, false, root.systemIdentity(), root.digest());
    CommitState selected = CommitState.initial(PROFILE, revision)
        .claimOwner("pin-test-owner", 1, 10)
        .publish(slot);
    return ReadView.capture(selected, revision, AUTHORIZATION,
        Optional.of(CommitDecision.from(slot)), Optional.of(root));
  }

  private static RequestScope scope() { return RequestScope.withTimeout(Duration.ofSeconds(5)); }

  private static void assertOlderViewsRejected(SnapshotPinManager manager, RequestScope scope) {
    for (int attempt = 0; attempt < 8; attempt++) {
      assertInstanceOf(IllegalStateException.class,
          failure(() -> manager.register(stableView(5), scope).join()));
    }
  }

  private static ByteKey digest(int value) {
    byte[] bytes = new byte[32];
    java.util.Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }

  private static Throwable failure(Runnable action) {
    try {
      action.run();
    } catch (CompletionException failure) {
      Throwable cause = failure.getCause();
      return cause == null ? failure : cause;
    }
    throw new AssertionError("expected operation to fail");
  }

  private static final class MemoryRecords implements SystemRecordStore {
    private final Map<RecordKey, ByteKey> rows = new HashMap<>();
    private volatile CyclicBarrier readBarrier;
    private final AtomicInteger pausedReads = new AtomicInteger();
    private final AtomicBoolean loseCasReplyAfterWrite = new AtomicBoolean();
    private final AtomicBoolean loseSealReplyAfterWrite = new AtomicBoolean();
    private BackendFailure.Category failSealBeforeWrite;
    private Integer nextFailedShard;
    private int pausedShardCreates;
    private CompletableFuture<Void> createEntered;
    private CompletableFuture<Void> releaseCreate;
    private volatile CompletableFuture<Void> casEntered;
    private volatile CompletableFuture<Void> releaseCas;

    @Override
    public CompletableFuture<Optional<ByteKey>> read(
        SystemRecordType type, ByteKey identity, RequestScope scope) {
      Optional<ByteKey> value;
      synchronized (this) {
        value = Optional.ofNullable(rows.get(new RecordKey(type, identity)));
      }
      if (type == SystemRecordType.SNAPSHOT_PIN && pausedReads.getAndUpdate(count ->
          Math.max(0, count - 1)) > 0) {
        try {
          readBarrier.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          return CompletableFuture.failedFuture(interrupted);
        } catch (BrokenBarrierException | TimeoutException failure) {
          return CompletableFuture.failedFuture(failure);
        }
      }
      return CompletableFuture.completedFuture(value);
    }

    @Override
    public int bucket(SystemRecordType type, ByteKey identity) { return 0; }

    @Override
    public CompletableFuture<CreateResult> createIfAbsent(
        SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope) {
      RecordKey key = new RecordKey(type, identity);
      CompletableFuture<Void> release;
      synchronized (this) {
        if (pausedShardCreates == 0 || shardIndex(key) < 0) return createNow(key, value);
        pausedShardCreates--;
        release = releaseCreate;
        if (pausedShardCreates == 0) createEntered.complete(null);
      }
      return release.thenCompose(ignored -> createNow(key, value));
    }

    private synchronized CompletableFuture<CreateResult> createNow(RecordKey key, ByteKey value) {
      if (matchesShard(key, nextFailedShard)) {
        nextFailedShard = null;
        return CompletableFuture.failedFuture(new IllegalStateException("synthetic create failure"));
      }
      ByteKey existing = rows.get(key);
      if (existing == null) {
        rows.put(key, value);
        return CompletableFuture.completedFuture(CreateResult.CREATED);
      }
      return CompletableFuture.completedFuture(existing.equals(value)
          ? CreateResult.ALREADY_PRESENT : CreateResult.CONFLICT);
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(SystemRecordType type,
        ByteKey identity, Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) {
      CompletableFuture<Void> entered = casEntered;
      CompletableFuture<Void> release = releaseCas;
      if (entered != null && casEntered == entered) {
        casEntered = null;
        entered.complete(null);
        return release.thenCompose(ignored ->
            compareAndSetNow(type, identity, expected, replacement));
      }
      return compareAndSetNow(type, identity, expected, replacement);
    }

    private synchronized CompletableFuture<Boolean> compareAndSetNow(SystemRecordType type, ByteKey identity,
        Optional<ByteKey> expected, ByteKey replacement) {
      RecordKey key = new RecordKey(type, identity);
      ByteKey existing = rows.get(key);
      if (!expected.equals(Optional.ofNullable(existing))) return CompletableFuture.completedFuture(false);
      boolean seal = identity.equals(initializationIdentity())
          && SnapshotPinRecordCodec.decodeInitialization(replacement).sealedRegistryId().isPresent();
      if (seal && failSealBeforeWrite != null) {
        BackendFailure.Category category = failSealBeforeWrite;
        failSealBeforeWrite = null;
        return CompletableFuture.failedFuture(new BackendFailure(category));
      }
      rows.put(key, replacement);
      boolean lostSeal = seal && loseSealReplyAfterWrite.compareAndSet(true, false);
      if (loseCasReplyAfterWrite.compareAndSet(true, false) || lostSeal) {
        return CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(true);
    }

    private void pauseNextShardReads(int count) {
      readBarrier = new CyclicBarrier(count);
      pausedReads.set(count);
    }

    private void loseNextCasReplyAfterWrite() { loseCasReplyAfterWrite.set(true); }
    private void loseNextSealReplyAfterWrite() { loseSealReplyAfterWrite.set(true); }
    private synchronized void failNextSealBeforeWrite(BackendFailure.Category category) {
      failSealBeforeWrite = category;
    }

    private void pauseNextCompareAndSet() {
      casEntered = new CompletableFuture<>();
      releaseCas = new CompletableFuture<>();
    }

    private CompletableFuture<Void> casEntered() { return casEntered; }
    private void releaseCompareAndSet() { releaseCas.complete(null); }

    private synchronized void corrupt(SystemRecordType type, ByteKey identity) {
      RecordKey key = new RecordKey(type, identity);
      byte[] value = rows.get(key).bytes();
      value[value.length - 1] ^= 0x1;
      rows.put(key, ByteKey.copyOf(value));
    }

    private synchronized void remove(SystemRecordType type, ByteKey identity) {
      rows.remove(new RecordKey(type, identity));
    }

    private synchronized boolean contains(SystemRecordType type, ByteKey identity) {
      return rows.containsKey(new RecordKey(type, identity));
    }

    private synchronized void failNextShardCreate(int shard) {
      nextFailedShard = shard;
    }

    private synchronized void pauseNextShardCreates(int count) {
      pausedShardCreates = count;
      createEntered = new CompletableFuture<>();
      releaseCreate = new CompletableFuture<>();
    }

    private synchronized boolean containsAnyShard(int shard) {
      return rows.keySet().stream().anyMatch(key -> matchesShard(key, shard));
    }

    private synchronized long countShardRows(int shard) {
      return rows.keySet().stream().filter(key -> matchesShard(key, shard)).count();
    }

    private synchronized ByteKey value(SystemRecordType type, ByteKey identity) {
      return java.util.Objects.requireNonNull(rows.get(new RecordKey(type, identity)));
    }

    private static boolean matchesShard(RecordKey key, Integer shard) {
      return shard != null && shardIndex(key) == shard;
    }

    private static int shardIndex(RecordKey key) {
      if (key.type() != SystemRecordType.SNAPSHOT_PIN) return -1;
      byte[] identity = key.identity().bytes();
      if (identity.length != Integer.BYTES * 2 + 16
          || java.nio.ByteBuffer.wrap(identity).getInt() != 2) return -1;
      return java.nio.ByteBuffer.wrap(identity).getInt(identity.length - Integer.BYTES);
    }

    private CompletableFuture<Void> createEntered() { return createEntered; }
    private void releaseCreate() { releaseCreate.complete(null); }
  }

  private record RecordKey(SystemRecordType type, ByteKey identity) {}

  private static ByteKey activeShardIdentity(MemoryRecords records, int shard) {
    ByteKey encoded = records.value(SystemRecordType.SNAPSHOT_PIN, configurationIdentity());
    ByteKey registryId = SnapshotPinRecordCodec.decodeConfiguration(encoded).registryId();
    return ByteKey.copyOf(java.nio.ByteBuffer.allocate(Integer.BYTES * 2 + 16)
        .putInt(2).put(registryId.bytes()).putInt(shard).array());
  }

  private static ByteKey configurationIdentity() {
    return ByteKey.copyOf(new byte[] {0x53, 0x50, 0x49, 0x4e, 0x01, 0x00});
  }

  private static ByteKey initializationIdentity() {
    return ByteKey.copyOf(new byte[] {0x53, 0x50, 0x49, 0x4e, 0x01, 0x01});
  }

  private static final class MutableClock extends Clock {
    private Instant current;

    private MutableClock(Instant current) { this.current = current; }
    private void advance(Duration duration) { current = current.plus(duration); }
    @Override public ZoneId getZone() { return ZoneId.of("UTC"); }
    @Override public Clock withZone(ZoneId zone) { return this; }
    @Override public Instant instant() { return current; }
  }
}
