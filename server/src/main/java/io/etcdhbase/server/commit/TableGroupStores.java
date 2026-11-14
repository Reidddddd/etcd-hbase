package io.etcdhbase.server.commit;

import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.core.commit.CommitFinalizationService;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.AuxiliaryMaterializationStore;
import io.etcdhbase.core.commit.InvocationResultResolver;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.ResultCatalog;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.commit.SystemRecordAuxiliaryMaterializationStore;
import io.etcdhbase.core.commit.TableGroupIdentityService;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.codec.LeaseBindingMessages;
import io.etcdhbase.hbase.codec.LeaseStateMessages;
import io.etcdhbase.hbase.connection.HBaseRequestContext;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.hbase.storage.HBaseCommitDecisionRepository;
import io.etcdhbase.hbase.storage.HBaseInvocationRepository;
import io.etcdhbase.hbase.storage.HBaseManifestRepository;
import io.etcdhbase.hbase.storage.HBaseMaterializationStore;
import io.etcdhbase.hbase.storage.HBaseResultRepository;
import io.etcdhbase.hbase.storage.HBaseSystemRecordStore;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.hbase.storage.SystemTableBinding;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.ClusterMetrics;

/** Opens the established HBase components of one table group without creating or adopting tables. */
public final class TableGroupStores {
  private final HBaseSystemRecordStore records;
  private final HBaseCommitControlStore control;
  private final HBaseInvocationRepository invocations;
  private final HBaseManifestRepository manifestRepository;
  private final ManifestCatalog manifests;
  private final HBaseResultRepository resultRepository;
  private final ResultCatalog results;
  private final HBaseCommitDecisionRepository decisions;
  private final CommitOutcomeResolver outcomes;
  private final HBaseMaterializationStore materialization;
  private final AuxiliaryMaterializationStore auxiliaryMaterialization;
  private final CommitFinalizationService finalizer;
  private final SnapshotPinManager pins;
  private final ReadViewService views;
  private final InvocationResultResolver invocationResults;
  private final long clusterId;

  private TableGroupStores(HBaseSystemRecordStore records, HBaseCommitControlStore control,
      HBaseInvocationRepository invocations, HBaseManifestRepository manifestRepository,
      ManifestCatalog manifests, HBaseResultRepository resultRepository, ResultCatalog results,
      HBaseCommitDecisionRepository decisions, CommitOutcomeResolver outcomes,
      HBaseMaterializationStore materialization,
      AuxiliaryMaterializationStore auxiliaryMaterialization,
      CommitFinalizationService finalizer,
      SnapshotPinManager pins, ReadViewService views, InvocationResultResolver invocationResults,
      long clusterId) {
    if (clusterId == 0) throw new IllegalArgumentException("table-group cluster ID must not be zero");
    this.records = records;
    this.control = control;
    this.invocations = invocations;
    this.manifestRepository = manifestRepository;
    this.manifests = manifests;
    this.resultRepository = resultRepository;
    this.results = results;
    this.decisions = decisions;
    this.outcomes = outcomes;
    this.materialization = materialization;
    this.auxiliaryMaterialization = auxiliaryMaterialization;
    this.finalizer = finalizer;
    this.pins = pins;
    this.views = views;
    this.invocationResults = invocationResults;
    this.clusterId = clusterId;
  }

  public HBaseSystemRecordStore records() { return records; }
  public HBaseCommitControlStore control() { return control; }
  public HBaseInvocationRepository invocations() { return invocations; }
  public HBaseManifestRepository manifestRepository() { return manifestRepository; }
  public ManifestCatalog manifests() { return manifests; }
  public HBaseResultRepository resultRepository() { return resultRepository; }
  public ResultCatalog results() { return results; }
  public HBaseCommitDecisionRepository decisions() { return decisions; }
  public CommitOutcomeResolver outcomes() { return outcomes; }
  public HBaseMaterializationStore materialization() { return materialization; }
  public AuxiliaryMaterializationStore auxiliaryMaterialization() { return auxiliaryMaterialization; }
  public CommitFinalizationService finalizer() { return finalizer; }
  public SnapshotPinManager pins() { return pins; }
  public ReadViewService views() { return views; }
  public InvocationResultResolver invocationResults() { return invocationResults; }
  public long clusterId() { return clusterId; }

  /** Opens a configured group only when its provisioning plan and actual HBase cluster agree. */
  public static CompletableFuture<TableGroupStores> openConfigured(HBaseConnectionFactory factory,
      AuthorizedBackendIdentity identity, M3TableGroupPlan plan,
      SnapshotPinConfiguration pinConfiguration, RequestScope scope) {
    Objects.requireNonNull(factory, "factory");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(plan, "plan");
    Objects.requireNonNull(pinConfiguration, "pinConfiguration");
    Objects.requireNonNull(scope, "scope");
    var source = plan.source();
    var expected = new AuthenticationScope(source.tableGroupId(), source.hbaseProfile().id());
    if (!expected.equals(identity.scope()) || !source.identityProviderId().equals(identity.providerId())
        || !identity.allowedTableGroupIds().contains(source.tableGroupId())) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
    }
    var request = new HBaseRequestContext(scope);
    return request.borrowConnection(factory, identity).thenCompose(connection -> {
      var admin = connection.getAdmin();
      var descriptor = request.execute(() -> admin.getDescriptor(plan.system().table()));
      var metrics = request.execute(() ->
          admin.getClusterMetrics(EnumSet.of(ClusterMetrics.Option.CLUSTER_ID)));
      return descriptor.thenCombine(metrics, (table, cluster) -> {
        String actualCluster = cluster.getClusterId();
        if (actualCluster == null || actualCluster.isBlank()
            || !actualCluster.equals(table.getValue("etcdhbase.m0.backend-cluster-id"))
            || !source.fingerprint().equals(table.getValue("etcdhbase.m0.plan-fingerprint"))) {
          throw new IllegalArgumentException("table group provisioning or backend identity differs");
        }
        return Boolean.TRUE;
      }).thenCompose(ignored -> open(factory, identity, plan.system(), plan.primary(), plan.current(),
          pinConfiguration, scope));
    });
  }

  /**
   * The caller owns the factory and identity. Missing control or pin metadata is an error; this
   * method never infers freshness from an empty table and never initializes M2 metadata.
   */
  public static CompletableFuture<TableGroupStores> open(HBaseConnectionFactory factory,
      AuthorizedBackendIdentity identity, SystemTableBinding system, StorageTableBinding primary,
      Optional<StorageTableBinding> current, SnapshotPinConfiguration pinConfiguration,
      RequestScope scope) {
    Objects.requireNonNull(factory, "factory");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(system, "system");
    Objects.requireNonNull(primary, "primary");
    Objects.requireNonNull(current, "current");
    Objects.requireNonNull(pinConfiguration, "pinConfiguration");
    Objects.requireNonNull(scope, "scope");
    if (!identity.scope().tableGroupId().equals(system.tableGroupId())
        || !identity.allowedTableGroupIds().contains(system.tableGroupId())
        || !system.saltProfile().equals(primary.codec().saltProfile())
        || current.filter(value -> !system.saltProfile().equals(value.codec().saltProfile())).isPresent()
        || system.table().equals(primary.table())
        || current.filter(value -> value.table().equals(system.table())
            || value.table().equals(primary.table())).isPresent()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "table-group identity, salt, or physical bindings differ"));
    }
    return HBaseSystemRecordStore.open(factory, identity, system, scope).thenCompose(records ->
        openControlAndVerifyReadiness(factory, identity, system, primary, current,
            pinConfiguration, scope, records));
  }

  private static CompletableFuture<TableGroupStores> openControlAndVerifyReadiness(
      HBaseConnectionFactory factory, AuthorizedBackendIdentity identity, SystemTableBinding system,
      StorageTableBinding primary, Optional<StorageTableBinding> current,
      SnapshotPinConfiguration pinConfiguration, RequestScope scope, HBaseSystemRecordStore records) {
    return HBaseCommitControlStore.open(factory, identity, system, scope).thenCompose(control ->
        control.read(scope).thenCompose(found -> {
          var state = found.orElseThrow(() ->
              new IllegalStateException("table-group control state is not initialized")).state();
          if (!state.profileFingerprint().equals(system.profileFingerprint())) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("table-group control profile differs"));
          }
          return openReadyStores(factory, identity, system, primary, current,
              pinConfiguration, scope, records, control);
        }));
  }

  private static CompletableFuture<TableGroupStores> openReadyStores(
      HBaseConnectionFactory factory, AuthorizedBackendIdentity identity, SystemTableBinding system,
      StorageTableBinding primary, Optional<StorageTableBinding> current,
      SnapshotPinConfiguration pinConfiguration, RequestScope scope,
      HBaseSystemRecordStore records, HBaseCommitControlStore control) {
    return SnapshotPinManager.open(records, pinConfiguration, scope).thenCompose(pins ->
        HBaseMaterializationStore.openForTableGroup(
            factory, identity, system, primary, current, scope).thenCompose(materialization ->
                readClusterIdentity(records, scope).thenApply(clusterId -> assemble(records, control,
                    materialization, pins, system, clusterId))));
  }

  private static CompletableFuture<Long> readClusterIdentity(
      HBaseSystemRecordStore records, RequestScope scope) {
    return new TableGroupIdentityService(records).readExisting(scope);
  }

  private static TableGroupStores assemble(HBaseSystemRecordStore records,
      HBaseCommitControlStore control, HBaseMaterializationStore materialization,
      SnapshotPinManager pins, SystemTableBinding system, long clusterId) {
    var invocations = new HBaseInvocationRepository(records);
    var manifestRepository = new HBaseManifestRepository(records);
    var manifests = new ManifestCatalog(manifestRepository);
    var resultRepository = new HBaseResultRepository(records);
    var results = new ResultCatalog(resultRepository);
    var decisions = new HBaseCommitDecisionRepository(records);
    var outcomes = new CommitOutcomeResolver(control, decisions);
    var auxiliaryMaterialization = new SystemRecordAuxiliaryMaterializationStore(records,
        LeaseStateMessages::decode, LeaseBindingMessages::decode);
    var finalizer = new CommitFinalizationService(control, outcomes, decisions, manifests,
        materialization, results, auxiliaryMaterialization);
    var views = new ReadViewService(control, manifests, pins, system.profileFingerprint());
    var invocationResults = new InvocationResultResolver(invocations, outcomes, manifests, results);
    return new TableGroupStores(records, control, invocations, manifestRepository, manifests,
        resultRepository, results, decisions, outcomes, materialization, auxiliaryMaterialization,
        finalizer, pins, views, invocationResults, clusterId);
  }
}
