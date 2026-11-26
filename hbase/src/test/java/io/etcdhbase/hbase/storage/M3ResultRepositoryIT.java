package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitFinalizationService;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPlanVerifier;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.commit.InvocationRepository;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.MaterializationStore;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.PublicationHandoff;
import io.etcdhbase.core.commit.ResultCatalog;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.ResultPage;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TableGroupIdentityService;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.kv.PutResult;
import io.etcdhbase.hbase.codec.ResultMessages;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

class M3ResultRepositoryIT {
  private static final byte[] FAMILY = {'s'};
  private static final String GROUP = "m3-result";
  private static final String PROFILE = "m3-result-salt";
  private static final HashSaltStrategy SALT = new HashSaltStrategy();

  @Test
  void noChangeResultSurvivesLostCasAcknowledgementAndRejectsLateSeal() throws Exception {
    TableName table = TableName.valueOf("j31_nochange_" + UUID.randomUUID().toString().replace("-", ""));
    var binding = new SystemTableBinding(table, FAMILY, GROUP, PROFILE, SALT);
    try (var environment = M1TestEnvironment.start(1);
         var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      environment.create(table, tableDescriptor(table, GROUP, true), SALT.splitKeys().toArray(byte[][]::new));
      var records = HBaseSystemRecordStore.open(environment.factory(), environment.identity(), binding, scope).join();
      var control = HBaseCommitControlStore.open(environment.factory(), environment.identity(), binding, scope).join();
      assertTrue(control.initializeIfAbsent(CommitState.initial(binding.profileFingerprint(), 1), scope).join());
      var initial = control.read(scope).join().orElseThrow();
      assertTrue(control.compareAndSet(initial, initial.state().claimOwner("j31-fixture-owner", 0, Long.MAX_VALUE),
          scope).join());
      var rawInvocations = new HBaseInvocationRepository(records);
      InvocationRepository acknowledgementLost = loseNoChangesAcknowledgement(rawInvocations);
      var invocations = new InvocationService(acknowledgementLost);
      var prepared = prepareNoChanges(records, rawInvocations, invocations, scope);
      verifyNoChangesRecovery(records, control, invocations, prepared, scope);
    }
  }

  private record NoChangesFixture(InvocationBinding preparing, InvocationBinding completed, ResultCatalog results) {}

  private static NoChangesFixture prepareNoChanges(SystemRecordStore records,
      HBaseInvocationRepository rawInvocations, InvocationService invocations, RequestScope scope) {
    var preparing = InvocationBinding.begin(InvocationId.create(), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1);
    preparing = invocations.bindOrJoin(preparing, scope).join();
    var resultRepository = new HBaseResultRepository(records);
    var identity = ResultIdentity.from(preparing);
    var page = new ResultPage(identity, 0, new byte[] {1, 2, 3});
    var reference = new ResultPreparation(resultRepository).prepare(identity, List.of(page), scope).join();
    var results = new ResultCatalog(new HBaseResultRepository(records));
    var completed = invocations.completeWithoutChanges(preparing, reference, results,
        () -> CompletableFuture.completedFuture(null), scope).join();
    assertEquals(InvocationBinding.State.NO_CHANGES, completed.state());
    assertEquals(InvocationBinding.State.NO_CHANGES,
        rawInvocations.read(preparing.invocationId(), scope).join().orElseThrow().state());
    var latePreparing = preparing;
    assertThrows(CompletionException.class, () -> invocations.transition(latePreparing,
        latePreparing.seal(ByteKey.copyOf(new byte[] {7})), scope).join());

    return new NoChangesFixture(preparing, completed, results);
  }

  private static void verifyNoChangesRecovery(SystemRecordStore records, HBaseCommitControlStore control,
      InvocationService invocations, NoChangesFixture prepared, RequestScope scope) throws BackendFailure {
    var decisions = new HBaseCommitDecisionRepository(records);
    var resolver = new InvocationResultResolver(new HBaseInvocationRepository(records),
        new CommitOutcomeResolver(control, decisions), new ManifestCatalog(new HBaseManifestRepository(records)),
        prepared.results());
    var limits = new BudgetManager.Limits(2, 0, 2, 1, 1, 0, 2_000_000);
    var budgets = new BudgetManager(limits, Map.of("group", limits), limits);
    try (var account = budgets.admit("group", scope, new BudgetManager.Reservation(0, 1, 1_000_000, 100));
         var recovered = resolver.resolveNoChanges(prepared.preparing().invocationId(), prepared.preparing().requestDigest(),
             prepared.preparing().authorizationScopeDigest(), scope, account).join()) {
      byte[] bytes = new byte[recovered.payload().buffer().remaining()];
      recovered.payload().buffer().get(bytes);
      assertTrue(java.util.Arrays.equals(new byte[] {1, 2, 3}, bytes));
      assertEquals(1, recovered.revision());
    }
    assertEquals(0, control.read(scope).join().orElseThrow().state().publishedSequence());
    var closed = invocations.transition(prepared.completed(), prepared.completed().closeQueryWindow(), scope).join();
    assertEquals(InvocationBinding.State.WINDOW_CLOSED, closed.state());
    assertEquals(InvocationBinding.State.WINDOW_CLOSED, invocations.bindOrJoin(prepared.preparing(), scope).join().state());
  }

  private static InvocationRepository loseNoChangesAcknowledgement(InvocationRepository delegate) {
    var loseAcknowledgement = new AtomicBoolean(true);
    return new InvocationRepository() {
      @Override public CompletableFuture<Optional<InvocationBinding>> read(InvocationId id, RequestScope request) {
        return delegate.read(id, request);
      }
      @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
          InvocationBinding value, RequestScope request) {
        return delegate.createIfAbsent(value, request);
      }
      @Override public CompletableFuture<Boolean> compareAndSet(
          InvocationBinding expected, InvocationBinding replacement, RequestScope request) {
        return delegate.compareAndSet(expected, replacement, request).thenCompose(updated -> {
          if (updated && replacement.state() == InvocationBinding.State.NO_CHANGES
              && loseAcknowledgement.compareAndSet(true, false)) {
            return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
          }
          return CompletableFuture.completedFuture(updated);
        });
      }
    };
  }

  @Test
  void persistsVerifiesAndRejectsDamagedResultOnDisposableHBase() throws Exception {
    TableName table = TableName.valueOf("m3_result_" + UUID.randomUUID().toString().replace("-", ""));
    SystemTableBinding binding = new SystemTableBinding(table, FAMILY, GROUP, PROFILE, SALT);
    try (var environment = M1TestEnvironment.start(1);
         var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      environment.create(table, tableDescriptor(table, GROUP, true), SALT.splitKeys().toArray(byte[][]::new));
      SystemRecordStore records = openInitializedGroup(environment, binding, scope);
      verifyClusterIdentityIsolation(environment, scope, records);
      HBaseResultRepository first = new HBaseResultRepository(records);
      HBaseResultRepository reopened = new HBaseResultRepository(records);
      InvocationBinding invocation = beginInvocation();
      ByteKey logicalKey = ByteKey.copyOf(new byte[] {0, (byte) 0xff});
      ResultPage page = resultPage(invocation, logicalKey);
      verifyLegacySchemaRejected(environment, scope, page);
      PersistedResult persisted = persistAndVerifyResult(records, first, reopened, invocation, page, scope);
      verifyRecoveryThroughRealControlAndInvocationRows(
          environment, binding, records, persisted, logicalKey, scope);
      verifyIdempotencyAndDamagedResult(records, reopened, persisted, page, scope);
    }
  }

  private static SystemRecordStore openInitializedGroup(
      M1TestEnvironment environment, SystemTableBinding binding, RequestScope scope) {
    var records = HBaseSystemRecordStore.open(
        environment.factory(), environment.identity(), binding, scope).join();
    long clusterId = new TableGroupIdentityService(records).initialize(scope).join();
    assertNotEquals(0, clusterId);
    assertEquals(clusterId, new TableGroupIdentityService(records).readExisting(scope).join());
    return records;
  }

  private static void verifyClusterIdentityIsolation(
      M1TestEnvironment environment, RequestScope scope, SystemRecordStore records) throws Exception {
    long clusterId = new TableGroupIdentityService(records).readExisting(scope).join();
    TableName otherTable = TableName.valueOf("m3_other_" + UUID.randomUUID().toString().replace("-", ""));
    String otherGroup = GROUP + "-other";
    environment.create(otherTable, tableDescriptor(otherTable, otherGroup, true),
        SALT.splitKeys().toArray(byte[][]::new));
    var otherBinding = new SystemTableBinding(otherTable, FAMILY, otherGroup, PROFILE, SALT);
    var otherRecords = HBaseSystemRecordStore.open(
        environment.factory(), environment.identity(), otherBinding, scope).join();
    assertNotEquals(clusterId, new TableGroupIdentityService(otherRecords).initialize(scope).join());
  }

  private static InvocationBinding beginInvocation() {
    return InvocationBinding.begin(InvocationId.create(), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), TransactionId.create(), 2, 0, 1);
  }

  private static ResultPage resultPage(InvocationBinding invocation, ByteKey logicalKey) {
    return new ResultPage(ResultIdentity.from(invocation), 0,
        ResultMessages.encode(new PutResult(logicalKey, 1, Optional.empty())));
  }

  private static void verifyLegacySchemaRejected(
      M1TestEnvironment environment, RequestScope scope, ResultPage page) throws Exception {
    TableName legacyTable = TableName.valueOf("m3_legacy_" + UUID.randomUUID().toString().replace("-", ""));
    environment.create(legacyTable, tableDescriptor(legacyTable, GROUP, false),
        SALT.splitKeys().toArray(byte[][]::new));
    var legacyBinding = new SystemTableBinding(legacyTable, FAMILY, GROUP, PROFILE, SALT);
    var legacyRecords = HBaseSystemRecordStore.open(
        environment.factory(), environment.identity(), legacyBinding, scope).join();
    assertTrue(legacyRecords.read(SystemRecordType.CONTROL, ByteKey.copyOf(new byte[] {1}), scope)
        .join().isEmpty());
    assertThrows(IllegalStateException.class,
        () -> new HBaseResultRepository(legacyRecords).createPage(page, scope));
    assertThrows(IllegalStateException.class,
        () -> legacyRecords.read(SystemRecordType.WORKSET, ByteKey.copyOf(new byte[] {1}), scope));
  }

  private static PersistedResult persistAndVerifyResult(SystemRecordStore records,
      HBaseResultRepository first, HBaseResultRepository reopened,
      InvocationBinding invocation, ResultPage page, RequestScope scope) {
    ResultIdentity identity = ResultIdentity.from(invocation);
    var reference = new ResultPreparation(first).prepare(identity, List.of(page), scope).join();
    var manifestStore = new HBaseManifestRepository(records);
    var manifests = new ManifestCatalog(manifestStore);
    var results = new ResultCatalog(reopened);
    var root = new ManifestPreparation(manifestStore, new ManifestPreparation.Limits(0, 0, 0, 0))
        .prepareWithResult(invocation, List.<PreparedCandidate>of().iterator(),
            List.<PreparedEvent>of().iterator(), reference, results,
            () -> CompletableFuture.completedFuture(null), scope).join();
    var sealed = invocation.seal(root.systemIdentity());
    MaterializationStore materialization = noOpMaterialization();
    var verifier = new CommitPlanVerifier(manifests, materialization, results);
    var persisted = new PersistedResult(invocation, identity, page, reference, root, sealed,
        manifests, results, materialization, verifier);

    assertEquals(reference, results.verify(reference, scope).join().reference());
    assertEquals(reference, manifestStore.readRoot(root.systemIdentity(), scope)
        .join().orElseThrow().resultReference().orElseThrow());
    assertEquals(root.digest(), verifier.verify(sealed, root, scope).join().digest());
    return persisted;
  }

  private static MaterializationStore noOpMaterialization() {
    return new MaterializationStore() {
      @Override public CompletableFuture<ApplyResult> applySelected(
          CommitDecision decision, PreparedCandidate candidate, RequestScope ignored) {
        return CompletableFuture.completedFuture(ApplyResult.APPLIED);
      }
      @Override public CompletableFuture<Void> preflight(PreparedCandidate candidate, RequestScope ignored) {
        return CompletableFuture.completedFuture(null);
      }
    };
  }

  private static void verifyIdempotencyAndDamagedResult(SystemRecordStore records,
      HBaseResultRepository repository, PersistedResult persisted, ResultPage page, RequestScope scope) {
    assertEquals(SystemRecordStore.CreateResult.CONFLICT,
        repository.createPage(new ResultPage(persisted.identity(), 0, new byte[] {7}), scope).join());
    assertEquals(persisted.identity(), repository.readPage(persisted.identity(), 0, scope)
        .join().orElseThrow().identity());

    // Fault injection after successful verification: later readers must not trust the directory alone.
    ByteKey oldBytes = ByteKey.copyOf(ResultMessages.encode(page));
    ByteKey replacement = ByteKey.copyOf(new byte[] {1});
    assertTrue(records.compareAndSet(SystemRecordType.RESULT, page.systemIdentity(),
        Optional.of(oldBytes), replacement, scope).join());
    assertThrows(CompletionException.class,
        () -> persisted.results().verify(persisted.reference(), scope).join());
    assertThrows(CompletionException.class,
        () -> persisted.verifier().verify(persisted.sealed(), persisted.root(), scope).join());
  }

  private record PersistedResult(InvocationBinding invocation, ResultIdentity identity, ResultPage page,
      io.etcdhbase.core.commit.ResultReference reference,
      io.etcdhbase.core.commit.ManifestRoot root, InvocationBinding sealed,
      ManifestCatalog manifests, ResultCatalog results, MaterializationStore materialization,
      CommitPlanVerifier verifier) {}

  private static void verifyRecoveryThroughRealControlAndInvocationRows(
      M1TestEnvironment environment, SystemTableBinding binding, SystemRecordStore records,
      PersistedResult persisted, ByteKey logicalKey, RequestScope scope) {
    var control = initializeControl(environment, binding, scope);
    var preparing = persisted.invocation();
    var invocationRepository = new HBaseInvocationRepository(records);
    var invocations = new InvocationService(invocationRepository);
    invocations.bindOrJoin(preparing, scope).join();
    invocations.transition(preparing, persisted.sealed(), scope).join();
    publishPreparedResult(records, control, persisted, scope);
    verifyRecoveredInvocation(records, control, invocationRepository, preparing, logicalKey, scope);
  }

  private static HBaseCommitControlStore initializeControl(
      M1TestEnvironment environment, SystemTableBinding binding, RequestScope scope) {
    var control = HBaseCommitControlStore.open(
        environment.factory(), environment.identity(), binding, scope).join();
    assertTrue(control.initializeIfAbsent(CommitState.initial(binding.profileFingerprint(), 1), scope).join());
    var initial = control.read(scope).join().orElseThrow();
    assertTrue(control.compareAndSet(initial, initial.state().claimOwner("fixture-owner", 0, Long.MAX_VALUE), scope).join());
    new io.etcdhbase.core.commit.AuthorizationBarrierUpgrade(control)
        .enable(new CommitOwnerService.OwnerToken("fixture-owner", 1), 0, scope).join();
    return control;
  }

  private static void publishPreparedResult(SystemRecordStore records,
      HBaseCommitControlStore control, PersistedResult persisted, RequestScope scope) {
    var decisions = new HBaseCommitDecisionRepository(records);
    var outcomes = new CommitOutcomeResolver(control, decisions);
    var finalizer = new CommitFinalizationService(control, outcomes, decisions,
        persisted.manifests(), persisted.materialization(), persisted.results());
    PublicationHandoff handoff = new PublicationHandoff() {
      @Override public <T> CompletableFuture<T> transfer(InvocationBinding invocation,
          RequestScope requestScope, java.util.function.Function<RequestScope, CompletableFuture<T>> work) {
        return work.apply(requestScope);
      }
    };
    var published = new CommitPublisher(control, outcomes, persisted.verifier(),
        new HBaseInvocationRepository(records), handoff, finalizer)
        .publish(persisted.sealed(), new CommitOwnerService.OwnerToken("fixture-owner", 2),
            persisted.root(), scope, io.etcdhbase.core.commit.CommitAuthorizationProof.capture(
                persisted.sealed().authorizationScopeDigest(),
                control.read(scope).join().orElseThrow().state().authorizationBarrierGeneration(),
                (identity, generation, request) -> CompletableFuture.completedFuture(null))).join();
    assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, published.state());
  }

  private static void verifyRecoveredInvocation(SystemRecordStore records,
      HBaseCommitControlStore control, HBaseInvocationRepository invocationRepository,
      InvocationBinding preparing, ByteKey logicalKey, RequestScope scope) {
    var reopenedResults = new ResultCatalog(new HBaseResultRepository(records));
    var reopened = new InvocationResultResolver(new HBaseInvocationRepository(records),
        new CommitOutcomeResolver(control, new HBaseCommitDecisionRepository(records)),
        new ManifestCatalog(new HBaseManifestRepository(records)), reopenedResults);
    var limits = new BudgetManager.Limits(2, 0, 2, 1, 1, 0, 2_000_000);
    var budgets = new BudgetManager(limits, Map.of("group", limits), limits);
    BudgetManager.Account account;
    try {
      account = budgets.admit("group", scope, new BudgetManager.Reservation(0, 1, 1_000_000, 100));
    } catch (io.etcdhbase.core.error.BackendFailure failure) {
      throw new AssertionError(failure);
    }
    try (var recovered = reopened.resolve(preparing.invocationId(), preparing.requestDigest(),
        preparing.authorizationScopeDigest(), scope, account).join()) {
      PutResult decoded = ResultMessages.decodePut(recovered.payload().buffer());
      assertEquals(logicalKey, decoded.key());
      assertTrue(decoded.previous().isEmpty());
      assertEquals(1, recovered.decision().revision());
    }
    assertEquals(InvocationBinding.State.SELECTED,
        invocationRepository.read(preparing.invocationId(), scope).join().orElseThrow().state());
    assertEquals(0, budgets.snapshot().allocatedBytes());
    account.close();
  }

  private static TableDescriptor tableDescriptor(TableName table, String group, boolean results) {
    return TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", group)
        .setValue("etcdhbase.m0.salt-profile-id", PROFILE)
        .setValue("etcdhbase.m0.salt-strategy-id", SALT.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version", Integer.toString(SALT.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count", Integer.toString(SALT.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes", Integer.toString(SALT.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories",
            "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION,SNAPSHOT_PIN" + (results ? ",RESULT" : ""))
        .build();
  }
}
