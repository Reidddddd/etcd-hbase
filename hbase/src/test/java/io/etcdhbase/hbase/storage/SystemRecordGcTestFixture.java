package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.CompactionOperation;
import io.etcdhbase.core.commit.CompactionService;
import io.etcdhbase.core.commit.EventHistoryRegistryUpgrade;
import io.etcdhbase.core.commit.EventHistoryRetention;
import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.EventIndexBackfillReader;
import io.etcdhbase.core.commit.GcCoordinator;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.PublicationSlot;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.request.RequestScope;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.client.TableDescriptor;

/** Builds real selected control/Compact and sealed registries; no caller-issued test permits. */
final class SystemRecordGcTestFixture {
  static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);
  static final SnapshotPinConfiguration CONFIGURATION = new SnapshotPinConfiguration(1, 16, Duration.ofMinutes(3));

  private SystemRecordGcTestFixture() {}

  static TableDescriptor descriptor(SystemTableBinding binding, TableDescriptor descriptor) {
    return binding.withCompactionCapability(binding.withEventRevisionIndexCapability(descriptor));
  }

  static SystemRecordRetirement.Permit authorize(HBaseSystemRecordStore records, RequestScope scope)
      throws io.etcdhbase.core.error.BackendFailure {
    var control = records.controlStore();
    var clock = Clock.systemUTC();
    assertTrue(control.initializeIfAbsent(CommitState.initial(records.profileFingerprint(), 1), scope).join());
    update(control, scope, state -> state.claimOwner("gc-owner", clock.millis(), clock.millis() + 180_000));
    update(control, scope, state -> state.upgradeAuxiliaryFormat("gc-owner", state.ownerEpoch(), clock.millis()));
    update(control, scope, state -> state.upgradeLeaseBindingFormat("gc-owner", state.ownerEpoch(), clock.millis()));
    update(control, scope, state -> state.upgradeLeaseExpiryFormat("gc-owner", state.ownerEpoch(), clock.millis()));
    update(control, scope, state -> state.upgradeEventIndexFormat("gc-owner", state.ownerEpoch(), clock.millis()));
    var owner = owner(control, scope);
    var events = new EventHistoryRegistryUpgrade(records, control, CONFIGURATION, clock).upgrade(owner, scope).join();
    owner = events.owner();
    update(control, scope, state -> state.enableAuthorizationBarrier("gc-owner", state.ownerEpoch(), clock.millis()));
    owner = owner(control, scope);
    var pins = SnapshotPinManager.initializeFresh(records, CONFIGURATION, scope).join();
    for (int index = 0; index < 20; index++) append(control, scope);
    var checkpoint = new EventIndexBackfillCheckpoint(owner.epoch(), 20, 1, 19, 20, 1, 20, 20, 0,
        events.registry().registryIdentity());
    assertEquals(SystemRecordStore.CreateResult.CREATED, records.createIfAbsent(
        SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT, EventIndexBackfillCheckpoint.versionIdentity(20),
        checkpoint.encode(), scope).join());
    var compactions = selectCompact(records, control, owner, clock, scope);
    var manifests = new ManifestCatalog(new HBaseManifestRepository(records));
    var views = new ReadViewService(control, manifests, pins, records.profileFingerprint());
    var retention = new EventHistoryRetention(events.registry(), views, new EventIndexBackfillReader(records, manifests));
    var limits = new BudgetManager.Limits(2, 2, 2, 2, 2, 1_000_000, 256L << 20);
    var budgets = new BudgetManager(limits, Map.of("public", limits), limits);
    try (var account = budgets.admit("public", scope,
        new BudgetManager.Reservation(8192, 32768, 128L << 20, 2L << 20, 4L << 20),
        new BudgetManager.ReadLimits(1000, 8L << 30, 1000, 1000, 1000))) {
      return new GcCoordinator(control, compactions, pins, clock).authorizeSystemRecords(owner, retention, DIGEST,
          new ManifestCatalog.ProofReadBudget(scope, account, 100, 8L << 30), scope).join();
    }
  }

  private static CompactionService selectCompact(HBaseSystemRecordStore records, HBaseCommitControlStore control,
      CommitOwnerService.OwnerToken owner, Clock clock, RequestScope scope) {
    var compactions = new CompactionService(control, records, clock);
    var state = control.read(scope).join().orElseThrow().state();
    var invocation = InvocationBinding.begin(InvocationId.create(), DIGEST, DIGEST, TransactionId.create(),
        owner.epoch(), state.publishedSequence(), state.publishedRevision());
    var proof = CommitAuthorizationProof.capture(DIGEST, state.authorizationBarrierGeneration(),
        (identity, generation, checked) -> CompletableFuture.completedFuture(null));
    assertEquals(CompactionOperation.Stage.LOGICAL, compactions.compact(16, invocation, owner, proof, scope).join().stage());
    return compactions;
  }

  private static CommitOwnerService.OwnerToken owner(HBaseCommitControlStore control, RequestScope scope) {
    var state = control.read(scope).join().orElseThrow().state();
    return new CommitOwnerService.OwnerToken(state.ownerInstance().orElseThrow(), state.ownerEpoch());
  }

  private static void append(HBaseCommitControlStore control, RequestScope scope) {
    var observed = control.read(scope).join().orElseThrow();
    var state = observed.state();
    var transaction = TransactionId.create();
    var root = ManifestRoot.create(transaction, state.ownerEpoch(), state.publishedSequence(),
        state.publishedRevision(), 0, 0, Optional.empty());
    var slot = new PublicationSlot(transaction, InvocationId.create(), state.ownerEpoch(),
        state.publishedSequence(), state.publishedRevision(), state.publishedSequence() + 1,
        state.publishedRevision() + 1, true, root.systemIdentity(), root.digest());
    assertTrue(control.compareAndSet(observed, state.publish(slot), scope).join());
    update(control, scope, selected -> selected.finalizePublication(transaction));
  }

  private static void update(HBaseCommitControlStore control, RequestScope scope,
      java.util.function.UnaryOperator<CommitState> change) {
    var observed = control.read(scope).join().orElseThrow();
    assertTrue(control.compareAndSet(observed, change.apply(observed.state()), scope).join());
  }
}
