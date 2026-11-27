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
public final class SystemRecordGcTestFixture {
  public static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);
  static final SnapshotPinConfiguration CONFIGURATION = new SnapshotPinConfiguration(1, 16, Duration.ofMinutes(3));

  private SystemRecordGcTestFixture() {}

  public static TableDescriptor descriptor(SystemTableBinding binding, TableDescriptor descriptor) {
    return binding.withGcFencingCapability(
        binding.withCompactionCapability(binding.withEventRevisionIndexCapability(descriptor)));
  }

  static SystemRecordRetirement.Permit authorize(HBaseSystemRecordStore records, RequestScope scope)
      throws io.etcdhbase.core.error.BackendFailure {
    return prepare(records, scope).authorize(scope);
  }

  public static Authority prepare(HBaseSystemRecordStore records, RequestScope scope) {
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
    return new Authority(control, new GcCoordinator(control, compactions, pins, clock), retention,
        owner, pins, events.registry());
  }

  public record Authority(io.etcdhbase.core.commit.CommitControlStore control, GcCoordinator coordinator,
      EventHistoryRetention retention, CommitOwnerService.OwnerToken owner, SnapshotPinManager pins,
      io.etcdhbase.core.commit.EventHistoryPinManager eventPins) {
    public SystemRecordRetirement.Permit authorize(RequestScope scope) throws io.etcdhbase.core.error.BackendFailure {
      var limits = new BudgetManager.Limits(2, 2, 2, 2, 2, 1_000_000, 256L << 20);
      var budgets = new BudgetManager(limits, Map.of("public", limits), limits);
      try (var account = budgets.admit("public", scope,
          new BudgetManager.Reservation(8192, 32768, 128L << 20, 2L << 20, 4L << 20),
          new BudgetManager.ReadLimits(1000, 8L << 30, 1000, 1000, 1000))) {
        return coordinator.authorizeSystemRecords(owner, retention, DIGEST,
            new ManifestCatalog.ProofReadBudget(scope, account, 100, 8L << 30), scope).join();
      }
    }
  }

  /** Keeps the package-private MiniCluster fixture and real authority together for server tests. */
  public static Environment open(int buckets, RequestScope scope) throws Exception {
    var environment = M1TestEnvironment.start(1);
    try {
      String suffix = java.util.UUID.randomUUID().toString().replace("-", "");
      var salt = new io.etcdhbase.spi.salt.HashSaltStrategy(new io.etcdhbase.spi.salt.SaltProfile(
          io.etcdhbase.spi.salt.HashSaltStrategy.STRATEGY_ID, 1, buckets, 2));
      var table = org.apache.hadoop.hbase.TableName.valueOf("gc_worker_" + suffix);
      var binding = new SystemTableBinding(table, new byte[] {'s'}, "gc-worker-" + suffix, "gc-salt-" + suffix, salt);
      var descriptor = org.apache.hadoop.hbase.client.TableDescriptorBuilder.newBuilder(table)
          .setColumnFamily(org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.newBuilder(new byte[] {'s'})
              .setTimeToLive(org.apache.hadoop.hbase.HConstants.FOREVER).setMaxVersions(1).build())
          .setDurability(org.apache.hadoop.hbase.client.Durability.SYNC_WAL)
          .setValue("etcdhbase.m0.table-group-id", binding.tableGroupId())
          .setValue("etcdhbase.m0.salt-profile-id", "gc-salt-" + suffix)
          .setValue("etcdhbase.m0.salt-strategy-id", salt.profile().strategyId())
          .setValue("etcdhbase.m0.salt-encoding-version", "1")
          .setValue("etcdhbase.m0.salt-bucket-count", Integer.toString(buckets))
          .setValue("etcdhbase.m0.salt-width-bytes", "2")
          .setValue("etcdhbase.m0.record-categories", String.join(",", "CONTROL", "INVOCATION", "CANDIDATE",
              "MANIFEST", "DECISION", "SNAPSHOT_PIN", "RESULT", "TABLE_GROUP_IDENTITY", "WORKSET",
              "LEASE_STATE", "LEASE_ENUMERATION", "EXPIRY_CANDIDATE", "AUXILIARY", "LEASE_RESULT",
              "LEASE_BINDING", "LEASE_EXPIRY_CHECKPOINT"))
          .setValue("etcdhbase.m4.auxiliary-format", "3").build();
      environment.create(table, descriptor(binding, descriptor), salt.splitKeys().toArray(byte[][]::new));
      var records = HBaseSystemRecordStore.open(environment.factory(), environment.identity(), binding, scope).join();
      return new Environment(environment, records, binding.tableGroupId(), prepare(records, scope));
    } catch (Exception | Error failure) {
      try { environment.close(); } catch (Exception | Error cleanup) { failure.addSuppressed(cleanup); }
      throw failure;
    }
  }

  public static final class Environment implements AutoCloseable {
    private final M1TestEnvironment environment;
    private final HBaseSystemRecordStore records;
    private final String group;
    private final Authority authority;
    private Environment(M1TestEnvironment environment, HBaseSystemRecordStore records, String group, Authority authority) {
      this.environment = environment;
      this.records = records;
      this.group = group;
      this.authority = authority;
    }
    public HBaseSystemRecordStore records() { return records; }
    public String group() { return group; }
    public Authority authority() { return authority; }
    @Override public void close() throws Exception { environment.close(); }
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
