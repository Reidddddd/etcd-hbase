package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.AuxiliaryFormatUpgrade;
import io.etcdhbase.core.commit.AuxiliaryLeasePlanVerifier;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.AuthorizationBarrierUpgrade;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.commit.CommitPlanVerifier;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.EventHistoryRegistryUpgrade;
import io.etcdhbase.core.commit.EventIndexBackfillCapture;
import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.EventIndexBackfillPlanner;
import io.etcdhbase.core.commit.EventIndexBackfillStep;
import io.etcdhbase.core.commit.EventIndexFormatUpgrade;
import io.etcdhbase.core.commit.EventRevisionIndex;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.LeaseBindingFormatUpgrade;
import io.etcdhbase.core.commit.LeaseExpiryFormatUpgrade;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.commit.SelectedSequenceReader;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.hbase.codec.LeaseBindingMessages;
import io.etcdhbase.hbase.codec.LeaseStateMessages;
import io.etcdhbase.hbase.connection.HBaseRequestContext;
import io.etcdhbase.hbase.storage.M3TestEnvironment;
import io.etcdhbase.hbase.storage.HBaseEventRevisionIndexRepository;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Exercises actual checkpoint selection and materialization on temporary HBase. */
class EventBackfillPublicationIT {
  private static final Duration TIMEOUT = Duration.ofMinutes(3);
  private static final ByteKey AUTHORIZATION = ByteKey.copyOf(new byte[32]);
  private static final SnapshotPinConfiguration PINS = new SnapshotPinConfiguration(2, 16, Duration.ofMinutes(1));

  @Test
  void resumesBackfillCycleAfterReopeningBothLayouts() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (var fixture = M3TestEnvironment.start(layout);
           var scope = RequestScope.withTimeout(TIMEOUT)) {
        var original = TableGroupStores.open(fixture.factory(), fixture.identity(), fixture.system(),
            fixture.primary(), fixture.current(), PINS, scope).join();
        long now = System.currentTimeMillis();
        var owner = new CommitOwnerService(original.control())
            .claim("backfill-cycle-test-owner", now, now + TIMEOUT.toMillis(), scope).join().orElseThrow();
        owner = new AuthorizationBarrierUpgrade(original.control()).enable(owner, now, scope).join();
        writeLegacyPut(fixture, original, owner);
        enableCapabilities(fixture, scope);
        var stores = TableGroupStores.open(fixture.factory(), fixture.identity(), fixture.system(),
            fixture.primary(), fixture.current(), PINS, scope).join();
        owner = new AuxiliaryFormatUpgrade(stores.control()).upgrade(owner, now, scope).join();
        owner = new LeaseBindingFormatUpgrade(stores.control()).upgrade(owner, now, scope).join();
        owner = new LeaseExpiryFormatUpgrade(stores.control()).upgrade(owner, now, scope).join();
        owner = new EventIndexFormatUpgrade(stores.control()).upgrade(owner, now, scope).join();
        var selected = new EventHistoryRegistryUpgrade(stores.records(), stores.control(), PINS, Clock.systemUTC())
            .upgrade(owner, scope).join();
        owner = selected.owner();
        owner = new AuthorizationBarrierUpgrade(stores.control()).enable(owner, now, scope).join();

        var limits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1_000_000, 1024L << 20);
        var tableGroup = fixture.system().tableGroupId();
        var reserved = LeaseBudgetProfile.perGroupCapacity(1_572_864);
        var budgets = new BudgetManager(limits, Map.of(tableGroup, limits), limits, reserved,
            Map.of(tableGroup, reserved));
        var handoff = new BoundedPublicationHandoff(1, 1, TIMEOUT);
        var ownerLane = new SerialMutationQueue(0, 1024, 1,
            LeaseBudgetProfile.RESERVED_OWNER_QUEUE_BYTES, TIMEOUT);
        try {
          var cycle = backfillCycle(fixture, stores, selected.registry(), budgets, handoff, ownerLane);
          var first = cycle.run(owner, scope).join();
          assertTrue(first.progressed());
          assertFalse(first.complete());
          var state = stores.control().read(scope).join().orElseThrow().state();
          assertEquals(2, state.finalizedSequence());
          assertEquals(2, state.finalizedRevision());
          assertTrue(new HBaseEventRevisionIndexRepository(stores.records()).read(2, scope).join().isEmpty());

          var reopened = TableGroupStores.open(fixture.factory(), fixture.identity(), fixture.system(),
              fixture.primary(), fixture.current(), PINS, scope).join();
          var pins = io.etcdhbase.core.commit.EventHistoryPinManager.openSelected(
              reopened.records(), reopened.control(), PINS, Clock.systemUTC(), scope).join();
          cycle = backfillCycle(fixture, reopened, pins, budgets, handoff, ownerLane);
          var resumed = cycle.run(owner, scope).join();
          assertTrue(resumed.progressed());
          assertTrue(resumed.complete());
          state = reopened.control().read(scope).join().orElseThrow().state();
          assertEquals(3, state.finalizedSequence());
          assertEquals(2, state.finalizedRevision());
          var alreadyComplete = cycle.run(owner, scope).join();
          assertFalse(alreadyComplete.progressed());
          assertTrue(alreadyComplete.complete());
          assertEquals(3, reopened.control().read(scope).join().orElseThrow().state().finalizedSequence());
          var index = new HBaseEventRevisionIndexRepository(reopened.records()).read(2, scope).join().orElseThrow();
          var decision = reopened.decisions().read(1, scope).join().orElseThrow();
          var root = reopened.manifests().readRoot(decision.manifestRoot(), scope).join().orElseThrow();
          assertEquals(EventRevisionIndex.fromSelected(decision, root), index);
        } finally {
          handoff.close();
          handoff.drained().join();
          ownerLane.close();
          ownerLane.drained().join();
          assertTrue(ownerLane.awaitTermination(Duration.ofSeconds(10)));
        }
      }
    }
  }

  private static EventBackfillCycle backfillCycle(M3TestEnvironment fixture, TableGroupStores stores,
      io.etcdhbase.core.commit.EventHistoryPinManager pins, BudgetManager budgets,
      BoundedPublicationHandoff handoff, SerialMutationQueue ownerLane) {
    var invocations = new InvocationService(stores.invocations());
    var preparation = new EventBackfillPreparation(new LeaseAuxiliaryMutationPreparation(
        new ResultPreparation(stores.resultRepository()), stores.results(),
        new ManifestPreparation(stores.manifestRepository(), new ManifestPreparation.Limits(0, 0, 0, 0)),
        invocations));
    var publisher = new CommitPublisher(stores.control(), stores.outcomes(),
        new CommitPlanVerifier(stores.manifests(), stores.materialization(), stores.results(),
            stores.auxiliaryMaterialization(), new AuxiliaryLeasePlanVerifier(
                LeaseStateMessages::decode, LeaseBindingMessages::decode)),
        stores.invocations(), handoff, stores.finalizer());
    return new EventBackfillCycle(fixture.system().tableGroupId(), ownerLane, stores, pins, invocations,
        preparation, publisher, budgets, fixture.identity(), AUTHORIZATION, Clock.systemUTC());
  }

  @Test
  void publishesAndReopensLegacyBackfillOnBothLayouts() throws Exception {
    for (var layout : List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
      try (var fixture = M3TestEnvironment.start(layout);
           var scope = RequestScope.withTimeout(TIMEOUT)) {
        var original = TableGroupStores.open(fixture.factory(), fixture.identity(), fixture.system(),
            fixture.primary(), fixture.current(), PINS, scope).join();
        long now = System.currentTimeMillis();
        var owner = new CommitOwnerService(original.control())
            .claim("backfill-test-owner", now, now + TIMEOUT.toMillis(), scope).join().orElseThrow();
        owner = new AuthorizationBarrierUpgrade(original.control()).enable(owner, now, scope).join();
        writeLegacyPut(fixture, original, owner);
        enableCapabilities(fixture, scope);
        var stores = TableGroupStores.open(fixture.factory(), fixture.identity(), fixture.system(),
            fixture.primary(), fixture.current(), PINS, scope).join();
        owner = new AuxiliaryFormatUpgrade(stores.control()).upgrade(owner, now, scope).join();
        owner = new LeaseBindingFormatUpgrade(stores.control()).upgrade(owner, now, scope).join();
        owner = new LeaseExpiryFormatUpgrade(stores.control()).upgrade(owner, now, scope).join();
        owner = new EventIndexFormatUpgrade(stores.control()).upgrade(owner, now, scope).join();
        var selected = new EventHistoryRegistryUpgrade(stores.records(), stores.control(), PINS, Clock.systemUTC())
            .upgrade(owner, scope).join();
        owner = selected.owner();
        owner = new AuthorizationBarrierUpgrade(stores.control()).enable(owner, now, scope).join();
        var pins = selected.registry();

        var limits = new BudgetManager.Limits(2, 2, 2, 2, 2, 1_000_000, 1024L << 20);
        var budgets = new BudgetManager(limits, Map.of(fixture.system().tableGroupId(), limits), limits);
        var account = budgets.admit(fixture.system().tableGroupId(), scope,
            new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8L << 20),
            new BudgetManager.ReadLimits(1000, 8L << 30, 1000, 1000, 1000));
        var budget = new ManifestCatalog.ProofReadBudget(scope, account, 64, 4L << 30);
        var checkpoint = new EventIndexBackfillPlanner(stores.control(), stores.decisions())
            .plan(owner, pins.namespace().orElseThrow(), pins.registryIdentity(), scope, budget).join();
        var capture = new EventIndexBackfillCapture(pins, stores.control(), stores.decisions());
        var window = capture.open(checkpoint, AUTHORIZATION, scope, budget).join();
        var invocations = new InvocationService(stores.invocations());
        var invocation = InvocationBinding.begin(InvocationId.create(), AUTHORIZATION, AUTHORIZATION,
            TransactionId.create(), owner.epoch(), checkpoint.sequence() - 1, checkpoint.upperRevision());
        invocation = invocations.bindOrJoin(invocation, scope).join();
        var preparation = new EventBackfillPreparation(new LeaseAuxiliaryMutationPreparation(
            new ResultPreparation(stores.resultRepository()), stores.results(),
            new ManifestPreparation(stores.manifestRepository(), new ManifestPreparation.Limits(0, 0, 0, 0)),
            invocations));
        var handoff = new BoundedPublicationHandoff(1, 1, TIMEOUT);
        try {
          var publisher = new CommitPublisher(stores.control(), stores.outcomes(),
              new CommitPlanVerifier(stores.manifests(), stores.materialization(), stores.results(),
                  stores.auxiliaryMaterialization(), new AuxiliaryLeasePlanVerifier(
                      LeaseStateMessages::decode, LeaseBindingMessages::decode)),
              stores.invocations(), handoff, stores.finalizer());
          var result = new EventBackfillExecutor(stores.control(), preparation, publisher, Clock.systemUTC())
              .publish(invocation, owner, checkpoint, window,
                  serviceProof(fixture, stores, scope), scope, account, budget).join();
          assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, result.state());
          var state = stores.control().read(scope).join().orElseThrow().state();
          assertEquals(2, state.finalizedSequence());
          assertEquals(2, state.finalizedRevision());
          assertFalse(checkpoint.complete());
          assertTrue(new HBaseEventRevisionIndexRepository(stores.records()).read(2, scope).join().isEmpty());
          var advanced = verifyLegacyAndPublish(fixture, stores, pins, owner, window, checkpoint,
              preparation, publisher, budgets, account, budget, scope);
          state = stores.control().read(scope).join().orElseThrow().state();
          assertEquals(3, state.finalizedSequence());
          assertEquals(2, state.finalizedRevision());
          var reopened = TableGroupStores.open(fixture.factory(), fixture.identity(), fixture.system(),
              fixture.primary(), fixture.current(), PINS, scope).join();
          var durable = reopened.records().latestAtOrBefore(SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT,
              EventIndexBackfillCheckpoint.routePrefix(), state.finalizedSequence(), scope).join().orElseThrow();
          assertEquals(advanced, EventIndexBackfillCheckpoint.decode(durable.value()));
          assertTrue(advanced.complete());
          var index = new HBaseEventRevisionIndexRepository(reopened.records()).read(2, scope).join().orElseThrow();
          var decision = reopened.decisions().read(1, scope).join().orElseThrow();
          var root = reopened.manifests().readRoot(decision.manifestRoot(), scope).join().orElseThrow();
          assertEquals(EventRevisionIndex.fromSelected(decision, root), index);
        } finally {
          handoff.close();
          handoff.drained().join();
          window.close();
          window.releaseCompletion().toCompletableFuture().join();
        }
      }
    }
  }

  // Legacy here means no event-index capability; new publication still requires the auth fence.
  private static void writeLegacyPut(M3TestEnvironment fixture, TableGroupStores stores,
      CommitOwnerService.OwnerToken owner) throws Exception {
    var limits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1L << 20, 256L << 20);
    var budgets = new BudgetManager(limits, Map.of(fixture.system().tableGroupId(), limits), limits);
    var worker = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(4));
    var handoff = new BoundedPublicationHandoff(1, 1, TIMEOUT);
    try (var scope = RequestScope.withTimeout(TIMEOUT)) {
      var account = budgets.admit(fixture.system().tableGroupId(), scope,
          new BudgetManager.Reservation(64L << 10, 64L << 10, 2L << 20, 16L << 20, 64L << 20));
      var invocations = new InvocationService(stores.invocations());
      var binding = InvocationBinding.begin(InvocationId.create(), AUTHORIZATION, AUTHORIZATION,
          TransactionId.create(), owner.epoch(), 0, 1);
      binding = invocations.bindOrJoin(binding, scope).join();
      var preparation = new PutCommitPreparation(new ResultPreparation(stores.resultRepository()), stores.results(),
          new ManifestPreparation(stores.manifestRepository(),
              new ManifestPreparation.Limits(1, 4L << 20, 1, 8L << 20)), invocations);
      var publisher = new CommitPublisher(stores.control(), stores.outcomes(),
          new CommitPlanVerifier(stores.manifests(), stores.materialization(), stores.results()),
          stores.invocations(), handoff, stores.finalizer());
      var executor = new PutMutationExecutor(fixture.system().tableGroupId(), fixture.system().profileFingerprint(),
          stores.control(), stores.views(), stores.manifests(), fixture.factory(), fixture.primary(), worker,
          preparation, publisher, 5L << 20, 8L << 20);
      var command = new PutCommand(ByteKey.copyOf(new byte[] {1, 0, (byte) 0xff}),
          new byte[] {4, 2}, 0, false, false, false);
      assertEquals(binding.invocationId(), executor.executeNew(command, binding, owner,
          fixture.identity(), serviceProof(fixture, stores, scope), scope, account).join());
    } finally {
      handoff.close();
      handoff.drained().join();
      worker.shutdown();
      if (!worker.awaitTermination(30, TimeUnit.SECONDS)) {
        throw new IllegalStateException("legacy Put worker did not stop");
      }
    }
  }

  private static EventIndexBackfillCheckpoint verifyLegacyAndPublish(M3TestEnvironment fixture,
      TableGroupStores stores, io.etcdhbase.core.commit.EventHistoryPinManager pins,
      CommitOwnerService.OwnerToken owner, io.etcdhbase.core.commit.EventBackfillWindow window,
      EventIndexBackfillCheckpoint checkpoint, EventBackfillPreparation preparation,
      CommitPublisher publisher, BudgetManager budgets, BudgetManager.Account account,
      ManifestCatalog.ProofReadBudget windowBudget, RequestScope scope) throws Exception {
    var source = new SelectedSequenceReader(pins, stores.control(), stores.decisions(),
        stores.manifestRepository(), new HBaseEventRevisionIndexRepository(stores.records()));
    EventIndexBackfillStep.Prepared verified;
    try (var stepScope = RequestScope.withTimeout(TIMEOUT)) {
      var stepAccount = budgets.admit(fixture.system().tableGroupId(), stepScope,
          new BudgetManager.Reservation(8192, 8192, 256L << 20, 8192, 8192),
          new BudgetManager.ReadLimits(1000, 8L << 30, 1000, 1000, 1000));
      var stepBudget = new ManifestCatalog.ProofReadBudget(stepScope, stepAccount, 64, 4L << 30);
      try (var step = EventIndexBackfillStep.open(source, stores.control(), window, checkpoint,
          AUTHORIZATION, owner.epoch(), 3, stepScope, stepBudget, windowBudget).join()) {
        Optional<EventIndexBackfillStep.Prepared> result = Optional.empty();
        for (int event = 0; event < 8 && result.isEmpty(); event++) {
          result = step.next(stepBudget, windowBudget).join();
        }
        verified = result.orElseThrow(() -> new IllegalStateException("legacy event did not finish"));
      }
    }
    var advanced = verified.checkpoint();
    assertTrue(verified.index().isPresent());
    var invocations = new InvocationService(stores.invocations());
    var proposed = InvocationBinding.begin(InvocationId.create(), AUTHORIZATION, AUTHORIZATION,
        TransactionId.create(), owner.epoch(), 2, 2);
    var invocation = invocations.bindOrJoin(proposed, scope).join();
    var result = new EventBackfillExecutor(stores.control(), preparation, publisher, Clock.systemUTC())
        .publish(invocation, owner, advanced, window,
            serviceProof(fixture, stores, scope), scope, account, windowBudget).join();
    assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, result.state());
    return advanced;
  }

  private static void enableCapabilities(M3TestEnvironment fixture, RequestScope scope) {
    var request = new HBaseRequestContext(scope);
    var connection = request.borrowConnection(fixture.factory(), fixture.identity()).join();
    var admin = connection.getAdmin();
    var descriptor = request.execute(() -> admin.getDescriptor(fixture.system().table())).join();
    descriptor = fixture.system().withAuxiliaryCapability(descriptor);
    descriptor = fixture.system().withLeaseBindingCapability(descriptor);
    descriptor = fixture.system().withLeaseExpiryCheckpointCapability(descriptor);
    descriptor = fixture.system().withEventRevisionIndexCapability(descriptor);
    var replacement = descriptor;
    request.execute(() -> admin.modifyTable(replacement)).join();
  }

  private static CommitAuthorizationProof serviceProof(
      M3TestEnvironment fixture, TableGroupStores stores, RequestScope scope) {
    var state = stores.control().read(scope).join().orElseThrow().state();
    return ServiceAuthorizationContextReference.publicationProof(
        fixture.identity(), AUTHORIZATION, state.authorizationBarrierGeneration());
  }
}
