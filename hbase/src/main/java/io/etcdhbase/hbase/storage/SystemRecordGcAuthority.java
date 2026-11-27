package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.commit.CompactionOperation;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.SnapshotGcBoundary;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Independently reads this store's actual control, selected Compact and sealed protection registries. */
final class SystemRecordGcAuthority {
  private final HBaseSystemRecordStore records;
  private final HBaseCommitControlStore control;
  private final Clock clock;

  SystemRecordGcAuthority(HBaseSystemRecordStore records) {
    this.records = records;
    control = records.controlStore();
    clock = Clock.systemUTC();
  }

  CompletableFuture<List<SnapshotGcBoundary.Protection>> validate(
      SystemRecordRetirement.Permit permit, RequestScope scope) {
    permit.requireUsable(records.profileFingerprint(), scope);
    return records.requireCurrentGcFencing(scope).thenCompose(ignored -> control.read(scope)).thenCompose(found -> {
      var state = found.orElseThrow(() -> new IllegalStateException("missing GC control")).state();
      requireControl(state, permit);
      return records.read(SystemRecordType.COMPACTION, ByteKey.copyOf(permit.operationId().bytes()), scope)
          .thenCompose(operation -> {
            requireOperation(CompactionOperation.decode(operation.orElseThrow(
                () -> new IllegalStateException("missing Compact operation"))), state, permit);
            return registryProtections(permit, scope);
          }).thenCompose(protections -> requireBackfill(state, permit, scope)
              .thenCompose(ignored -> control.read(scope).thenApply(latest -> {
                requireControl(latest.orElseThrow(() -> new IllegalStateException("missing GC control")).state(), permit);
                return protections;
              })));
    });
  }

  private void requireControl(CommitState state, SystemRecordRetirement.Permit permit) {
    var identity = permit.authority();
    var owner = identity.owner();
    if (!state.profileFingerprint().equals(permit.profile()) || state.ownerEpoch() != owner.epoch()
        || state.ownerInstance().filter(owner.instance()::equals).isEmpty() || state.ownerLeaseExpired(clock.millis())
        || !state.supportsAuthorizationBarrier() || state.publication().isPresent()
        || state.authorizationBarrierGeneration() != identity.authorizationGeneration()
        || !state.eventRegistryNamespace().equals(Optional.of(identity.eventNamespace()))
        || !state.compactOperationId().equals(Optional.of(permit.operationId()))
        || state.compactRevision() < permit.safeRevision() || state.finalizedSequence() < permit.safeSequence()) {
      throw new IllegalStateException("retirement permit differs from current durable authority");
    }
  }

  private static void requireOperation(CompactionOperation operation, CommitState state,
      SystemRecordRetirement.Permit permit) {
    if (operation.stage() != CompactionOperation.Stage.LOGICAL || !operation.id().equals(permit.operationId())
        || !operation.profile().equals(permit.profile()) || operation.revision() != state.compactRevision()
        || Math.addExact(operation.controlVersion(), 1) != permit.generation()) {
      throw new IllegalStateException("retirement permit has no selected logical Compact operation");
    }
  }

  private CompletableFuture<List<SnapshotGcBoundary.Protection>> registryProtections(
      SystemRecordRetirement.Permit permit, RequestScope scope) {
    return SnapshotPinManager.openSealed(records, permit.authority().kvConfiguration(), clock, scope)
        .thenCompose(kv -> {
          if (!kv.registryIdentity().equals(permit.registryIdentity())) {
            throw new IllegalStateException("KV registry identity changed");
          }
          return kv.readBoundary(scope);
        }).thenCompose(kvBoundary -> eventBoundary(permit, scope).thenApply(eventBoundary -> {
          requireBoundary(kvBoundary, permit);
          requireBoundary(eventBoundary, permit);
          var protections = new ArrayList<>(permit.protections());
          protections.addAll(kvBoundary.protections());
          protections.addAll(eventBoundary.protections());
          return List.copyOf(protections);
        }));
  }

  private CompletableFuture<SnapshotGcBoundary> eventBoundary(
      SystemRecordRetirement.Permit permit, RequestScope scope) {
    var identity = permit.authority();
    return EventHistoryPinManager.openSelected(records, control, identity.eventConfiguration(), clock, scope)
        .thenCompose(events -> {
          if (!events.registryIdentity().equals(identity.eventRegistryIdentity())
              || !events.namespace().equals(Optional.of(identity.eventNamespace()))) {
            throw new IllegalStateException("event registry identity changed");
          }
          return events.readBoundary(scope);
        });
  }

  private static void requireBoundary(SnapshotGcBoundary boundary, SystemRecordRetirement.Permit permit) {
    if (boundary.safeRevision() < permit.safeRevision() || boundary.safeSequence() < permit.safeSequence()) {
      throw new IllegalStateException("retirement permit exceeds durable protection boundary");
    }
  }

  private CompletableFuture<Void> requireBackfill(CommitState state,
      SystemRecordRetirement.Permit permit, RequestScope scope) {
    // A pending publication is rejected by requireControl, so only finalized stable progress applies.
    return records.latestAtOrBefore(SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT,
        EventIndexBackfillCheckpoint.routePrefix(), state.finalizedSequence(), scope).thenAccept(found -> {
          if (found.isEmpty()) throw new IllegalStateException("event backfill checkpoint is missing");
          var entry = found.orElseThrow();
          var checkpoint = EventIndexBackfillCheckpoint.decode(entry.value());
          if (EventIndexBackfillCheckpoint.sequence(entry.identity()) != checkpoint.sequence()
              || !checkpoint.registryIdentity().equals(permit.authority().eventRegistryIdentity())
              || checkpoint.sequence() > state.finalizedSequence()) {
            throw new IllegalStateException("event backfill checkpoint differs from durable authority");
          }
          long firstRevision = checkpoint.firstBaseRevision() == Long.MAX_VALUE
              ? Long.MAX_VALUE : checkpoint.firstBaseRevision() + 1;
          if (!checkpoint.complete()
              && (permit.safeRevision() > firstRevision
                  || permit.safeSequence() > checkpoint.firstSequence())) {
            throw new IllegalStateException("retirement permit exceeds pending event backfill");
          }
        });
  }
}
