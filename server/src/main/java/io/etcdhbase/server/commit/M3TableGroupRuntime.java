package io.etcdhbase.server.commit;

import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitPlanVerifier;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.AuxiliaryFormatUpgrade;
import io.etcdhbase.core.commit.AuxiliaryLeasePlanVerifier;
import io.etcdhbase.core.commit.EventIndexFormatUpgrade;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventHistoryRegistryUpgrade;
import io.etcdhbase.core.commit.LeaseBindingFormatUpgrade;
import io.etcdhbase.core.commit.LeaseExpiryFormatUpgrade;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.LeaseBindingReader;
import io.etcdhbase.core.commit.LeaseBindingReadView;
import io.etcdhbase.core.commit.LeaseEnumerationReader;
import io.etcdhbase.core.commit.LeaseReadView;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.commit.RecoveryManager;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.commit.SharedEventTailer;
import io.etcdhbase.core.commit.SelectedSequenceReader;
import io.etcdhbase.core.commit.UnchangedRangeProof;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseIdAllocator;
import io.etcdhbase.core.lease.LeaseCostAdmission;
import io.etcdhbase.core.lease.LeaseTimeRules;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.kv.RangeReadCache;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.grpc.service.KvService;
import io.etcdhbase.grpc.service.LeaseService;
import io.etcdhbase.grpc.service.WatchService;
import io.etcdhbase.grpc.service.AuthService;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.codec.LeaseStateMessages;
import io.etcdhbase.hbase.codec.LeaseBindingMessages;
import io.etcdhbase.hbase.connection.HBaseRequestContext;
import io.etcdhbase.hbase.error.HBaseFailures;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.server.config.M3MemoryReservations;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.server.auth.AuthenticateRpcHandler;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Duration;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.LongFunction;

/** Owns the local KV runtime for one validated table group; the caller owns its HBase factory. */
public final class M3TableGroupRuntime implements AutoCloseable {
  private static final BudgetManager.Reservation REQUEST_MEMORY =
      M3MemoryReservations.REQUEST;
  // Lease requests retain proof-read headroom separately from ordinary KV requests.
  private static final BudgetManager.Reservation LEASE_REQUEST_MEMORY =
      LeaseBudgetProfile.LEASE_REQUEST_MEMORY;
  private static final BudgetManager.Reservation RECOVERY_MEMORY =
      LeaseBudgetProfile.RECOVERY_MEMORY;
  private static final Executor CLEANUP = task -> Thread.ofVirtual().name("m3-runtime-cleanup").start(task);
  private static final BiFunction<Long, Integer, LeaseTimeSource> SYSTEM_LEASE_TIME_SOURCE =
      (uncertaintyMillis, maximumDriftPpm) -> new BoundedSystemLeaseTimeSource(
          Clock.systemUTC(), uncertaintyMillis, maximumDriftPpm);

  private final Pending resources;
  private final KvService kvService;
  private final LeaseService leaseService;
  private final WatchService watchService;
  private final AuthService authService;
  private final Optional<LeaseTimeSource> leaseTime;
  private final long clusterId;
  private final long memberId;
  private final RequestMetrics metrics;

  private M3TableGroupRuntime(Pending resources, KvService kvService, LeaseService leaseService,
      WatchService watchService, AuthService authService,
      Optional<LeaseTimeSource> leaseTime, long clusterId, long memberId, RequestMetrics metrics) {
    this.resources = resources;
    this.kvService = kvService;
    this.leaseService = leaseService;
    this.watchService = watchService;
    this.authService = Objects.requireNonNull(authService, "authService");
    this.leaseTime = Objects.requireNonNull(leaseTime, "leaseTime");
    this.clusterId = clusterId;
    this.memberId = memberId;
    this.metrics = metrics;
  }

  /**
   * Opens an already initialized group and completes recovery before returning. The caller owns
   * the startup scope and factory and must not close the latter until startup and runtime physical
   * producers have drained, including when startup fails.
   */
  public static M3TableGroupRuntime open(M3TableGroupPlan plan, M3RuntimeConfiguration activation,
      RuntimeConfiguration.ResourceBudgets limits, HBaseConnectionFactory connections,
      AuthorizedBackendIdentity identity, BudgetManager budgets, ScanScheduler scans,
      RequestScope startup) throws Exception {
    return openInternal(plan, activation, limits, connections, identity, budgets, scans, startup,
        Optional.empty(), SYSTEM_LEASE_TIME_SOURCE, null, null, null);
  }

  /** Opens the data path with durable session resolution for requests and owner forwarding. */
  public static M3TableGroupRuntime openAuthenticated(M3TableGroupPlan plan,
      M3RuntimeConfiguration activation, RuntimeConfiguration.ResourceBudgets limits,
      HBaseConnectionFactory connections, AuthorizedBackendIdentity identity,
      BudgetManager budgets, ScanScheduler scans, RequestScope startup,
      OwnerForwardingRuntime ownerForwarding, ScheduledExecutorService leaseTimers,
      LongFunction<AuthSessionService> authSessionFactory) throws Exception {
    return openInternal(plan, activation, limits, connections, identity, budgets, scans, startup,
        Optional.empty(), SYSTEM_LEASE_TIME_SOURCE, ownerForwarding, leaseTimers,
        Objects.requireNonNull(authSessionFactory, "authSessionFactory"));
  }

  /** Uses an explicit time source for runtime readiness and lease-boundary tests. */
  public static M3TableGroupRuntime openWithLeaseTimeSource(M3TableGroupPlan plan,
      M3RuntimeConfiguration activation, RuntimeConfiguration.ResourceBudgets limits,
      HBaseConnectionFactory connections, AuthorizedBackendIdentity identity, BudgetManager budgets,
      ScanScheduler scans, RequestScope startup,
      LongFunction<LeaseTimeSource> leaseTimeSourceFactory) throws Exception {
    return openWithLeaseTimeSource(plan, activation, limits, connections, identity, budgets, scans,
        startup, leaseTimeSourceFactory, null, null);
  }

  /** Uses an explicit Lease clock while retaining the process-shared owner and timer services. */
  public static M3TableGroupRuntime openWithLeaseTimeSource(M3TableGroupPlan plan,
      M3RuntimeConfiguration activation, RuntimeConfiguration.ResourceBudgets limits,
      HBaseConnectionFactory connections, AuthorizedBackendIdentity identity, BudgetManager budgets,
      ScanScheduler scans, RequestScope startup,
      LongFunction<LeaseTimeSource> leaseTimeSourceFactory, OwnerForwardingRuntime ownerForwarding,
      ScheduledExecutorService leaseTimers) throws Exception {
    return openInternal(plan, activation, limits, connections, identity, budgets, scans, startup,
        Optional.empty(), (uncertaintyMillis, ignoredDriftPpm) ->
            leaseTimeSourceFactory.apply(uncertaintyMillis), ownerForwarding, leaseTimers, null);
  }

  /** Internal assembly path; public configuration still rejects cache activation. */
  static M3TableGroupRuntime open(M3TableGroupPlan plan, M3RuntimeConfiguration activation,
      RuntimeConfiguration.ResourceBudgets limits, HBaseConnectionFactory connections,
      AuthorizedBackendIdentity identity, BudgetManager budgets, ScanScheduler scans,
      RequestScope startup, Optional<CacheLimits> cacheLimits) throws Exception {
    return openInternal(plan, activation, limits, connections, identity, budgets, scans, startup,
        cacheLimits, SYSTEM_LEASE_TIME_SOURCE, null, null, null);
  }

  public static M3TableGroupRuntime open(M3TableGroupPlan plan, M3RuntimeConfiguration activation,
      RuntimeConfiguration.ResourceBudgets limits, HBaseConnectionFactory connections,
      AuthorizedBackendIdentity identity, BudgetManager budgets, ScanScheduler scans,
      RequestScope startup, OwnerForwardingRuntime ownerForwarding) throws Exception {
    return openInternal(plan, activation, limits, connections, identity, budgets, scans, startup,
        Optional.empty(), SYSTEM_LEASE_TIME_SOURCE, ownerForwarding, null, null);
  }

  /** Opens one table-group runtime with process-shared owner routing and Lease stream timers. */
  public static M3TableGroupRuntime open(M3TableGroupPlan plan, M3RuntimeConfiguration activation,
      RuntimeConfiguration.ResourceBudgets limits, HBaseConnectionFactory connections,
      AuthorizedBackendIdentity identity, BudgetManager budgets, ScanScheduler scans,
      RequestScope startup, OwnerForwardingRuntime ownerForwarding,
      ScheduledExecutorService leaseTimers) throws Exception {
    return openInternal(plan, activation, limits, connections, identity, budgets, scans, startup,
        Optional.empty(), SYSTEM_LEASE_TIME_SOURCE, ownerForwarding, leaseTimers, null);
  }

  private static M3TableGroupRuntime openInternal(M3TableGroupPlan plan,
      M3RuntimeConfiguration activation, RuntimeConfiguration.ResourceBudgets limits,
      HBaseConnectionFactory connections, AuthorizedBackendIdentity identity, BudgetManager budgets,
      ScanScheduler scans, RequestScope startup, Optional<CacheLimits> cacheLimits,
      BiFunction<Long, Integer, LeaseTimeSource> leaseTimeSourceFactory,
      OwnerForwardingRuntime ownerForwarding,
      ScheduledExecutorService leaseTimers,
      LongFunction<AuthSessionService> authSessionFactory) throws Exception {
    Objects.requireNonNull(plan, "plan");
    Objects.requireNonNull(activation, "activation");
    Objects.requireNonNull(limits, "limits");
    Objects.requireNonNull(connections, "connections");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(budgets, "budgets");
    Objects.requireNonNull(scans, "scans");
    Objects.requireNonNull(startup, "startup");
    Objects.requireNonNull(cacheLimits, "cacheLimits");
    Objects.requireNonNull(leaseTimeSourceFactory, "leaseTimeSourceFactory");
    if (limits.maxConcurrentRequests() < 1 || limits.maxConcurrentRequests() > 100_000
        || limits.requestDeadlineMs() < 1 || limits.requestDeadlineMs() > 600_000
        || limits.maxScannersPerTable() < 1 || limits.maxScannersPerTable() > 1024
        || activation.maxEtcdRequestBytes() > limits.maxRpcBytes()
        || limits.maxRpcBytes() > limits.maxRequestBytes() || limits.maxQueuedRequests() != 0) {
      throw new IllegalArgumentException("M3 transport or queue limits are incompatible");
    }
    Pending pending = null;
    try {
      var stores = await(TableGroupStores.openConfigured(connections, identity, plan,
          activation.pinConfiguration(), startup), startup);
      AuthSessionService authSessions = authSessionFactory == null ? null
          : Objects.requireNonNull(authSessionFactory.apply(stores.clusterId()),
              "authenticated session service");
      if (!activation.m4LeaseApiEnabled()) {
        verifyLeaseDisabledFormat(stores, startup);
      }
      pending = new Pending(startup, leaseTimers);
      pending.initialize(plan, activation, limits, budgets);
      var invocations = new InvocationService(stores.invocations());
      var owners = new CommitOwnerService(stores.control());
      var recovery = new RecoveryManager(plan.system().profileFingerprint(), stores.control(), owners,
          stores.invocations(), stores.outcomes(), stores.finalizer());
      String instance = activation.ownerInstanceId() == null
          ? CommitOwnerService.newInstanceId() : OwnerIncarnation.newId(activation.ownerInstanceId());
      Runnable ownerLost = activation.ownerForwarding() == null ? pending::ownerLost : () -> {};
      var ownerFuture = activation.ownerForwarding() == null
          ? M3OwnerLease.acquire(recovery, owners, instance,
              Duration.ofMillis(activation.ownerLeaseMs()), Duration.ofMillis(activation.heartbeatMs()),
              Duration.ofMillis(2L * activation.heartbeatMs()), startup, ownerLost)
          : M3OwnerLease.acquireOrStandby(recovery, owners, instance,
              Duration.ofMillis(activation.ownerLeaseMs()), Duration.ofMillis(activation.heartbeatMs()),
              Duration.ofMillis(2L * activation.heartbeatMs()), startup, ownerLost);
      pending.trackOwner(ownerFuture);
      var owner = await(ownerFuture, startup);
      if (activation.m4LeaseApiEnabled()) {
        if (owner.isOwner()) {
          ensureM4LeaseBindingFormat(plan, connections, identity, stores, owner, startup);
          ensureM4LeaseExpiryFormat(plan, connections, identity, owner, startup);
          ensureM4EventIndexFormat(plan, connections, identity, owner, startup);
          stores = await(TableGroupStores.openConfigured(connections, identity, plan,
              activation.pinConfiguration(), startup), startup);
          verifyLeaseExpiryFormat(stores, startup);
          verifyEventIndexFormat(stores, startup);
          invocations = new InvocationService(stores.invocations());
          owners = new CommitOwnerService(stores.control());
          recovery = new RecoveryManager(plan.system().profileFingerprint(), stores.control(), owners,
              stores.invocations(), stores.outcomes(), stores.finalizer());
          await(owner.refreshServices(owners, recovery, startup), startup);
        } else {
          verifyLeaseExpiryFormat(stores, startup);
          verifyEventIndexFormat(stores, startup);
        }
        pending.eventPins = openEventRegistry(stores, activation, owner, startup);
      }
      ByteKey authorization = authorizationDigest(identity);
      var business = assembleBusiness(plan, activation, connections, budgets, scans, pending, stores,
          invocations, owner, cacheLimits, leaseTimeSourceFactory);
      Optional<LeaseTimeSource> leaseTime = business.lease().map(LeaseComponents::time);
      if (business.lease().isPresent()) {
        verifyExistingLeaseLimits(plan, activation, budgets, stores, authorization,
            business.lease().orElseThrow(), startup);
      }
      ServiceComponents services = assembleService(plan, activation, limits, identity, authorization, budgets,
          pending, stores, invocations, owner, business, ownerForwarding, authSessions);
      await(pending.initialReadiness(), startup);
      startup.closeAsync(CLEANUP);
      await(CompletableFuture.allOf(startup.cleanupCompletion().toCompletableFuture(),
          startup.physicalProducersCompletion().toCompletableFuture()), startup);
      if (!runtimeReady(owner.isReady(), leaseTime) || !pending.isReady()) {
        throw new java.util.concurrent.CompletionException(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      return new M3TableGroupRuntime(pending, services.kvService(), services.leaseService(),
          services.watchService(), services.authService(),
          leaseTime, stores.clusterId(), activation.memberIdBits(), business.metrics());
    } catch (Exception failure) {
      throw new StartupException(failure, abort(startup, pending, failure));
    } catch (Error fatal) {
      fatal.addSuppressed(new StartupCleanup(abort(startup, pending, fatal)));
      throw fatal;
    }
  }

  private static void verifyLeaseDisabledFormat(TableGroupStores stores, RequestScope startup)
      throws Exception {
    CommitState state = await(stores.control().read(startup), startup)
        .orElseThrow(() -> new IllegalStateException("table-group control state is not initialized"))
        .state();
    if (state.formatVersion() != CommitState.FORMAT_VERSION) {
      throw new IllegalStateException(
          "M4 Lease activation must remain enabled for a table group with auxiliary state");
    }
  }

  private static void ensureM4LeaseBindingFormat(M3TableGroupPlan plan,
      HBaseConnectionFactory connections, AuthorizedBackendIdentity identity,
      TableGroupStores stores, M3OwnerLease ownerLease, RequestScope startup) throws Exception {
    if (!ownerLease.isReady()) {
      throw new BackendFailure(BackendFailure.Category.UNAVAILABLE);
    }
    CommitState initial = await(stores.control().read(startup), startup)
        .orElseThrow(() -> new IllegalStateException("table-group control state is not initialized"))
        .state();
    if (!initial.supportsAuxiliaryRecords() && initial.formatVersion() != CommitState.FORMAT_VERSION) {
      throw new IllegalStateException("table-group control format cannot be activated for M4");
    }

    // Descriptor and control-format capabilities advance separately. Publish the descriptor
    // marker first, then reopen the CAS adapter so it validates the capability snapshot it uses.
    await(ensureAuxiliaryDescriptor(plan, connections, identity, startup), startup);
    HBaseCommitControlStore auxiliaryControl = await(
        HBaseCommitControlStore.open(connections, identity, plan.system(), startup), startup);
    if (!auxiliaryControl.supportsAuxiliary()) {
      throw new IllegalStateException("M4 auxiliary descriptor capability is not available");
    }
    CommitOwnerService auxiliaryOwners = new CommitOwnerService(auxiliaryControl);
    CommitState auxiliaryState = await(auxiliaryControl.read(startup), startup)
        .orElseThrow(() -> new IllegalStateException("table-group control state is not initialized"))
        .state();
    if (auxiliaryState.formatVersion() == CommitState.FORMAT_VERSION) {
      await(ownerLease.transitionFence(auxiliaryOwners,
          owner -> new AuxiliaryFormatUpgrade(auxiliaryControl)
              .upgrade(owner, Clock.systemUTC().millis(), startup), startup), startup);
    } else if (auxiliaryState.supportsAuxiliaryRecords()) {
      await(ownerLease.transitionFence(auxiliaryOwners,
          CompletableFuture::completedFuture, startup), startup);
    } else {
      throw new IllegalStateException("table-group control format cannot be activated for M4");
    }

    await(ensureLeaseBindingDescriptor(plan, connections, identity, startup), startup);
    HBaseCommitControlStore bindingControl = await(
        HBaseCommitControlStore.open(connections, identity, plan.system(), startup), startup);
    if (!bindingControl.supportsAuxiliary() || !bindingControl.supportsLeaseBindings()) {
      throw new IllegalStateException("J41 binding descriptor capability is not available");
    }
    CommitOwnerService bindingOwners = new CommitOwnerService(bindingControl);
    CommitState bindingState = await(bindingControl.read(startup), startup)
        .orElseThrow(() -> new IllegalStateException("table-group control state is not initialized"))
        .state();
    if (bindingState.formatVersion() == CommitState.AUXILIARY_FORMAT_VERSION) {
      await(ownerLease.transitionFence(bindingOwners,
          owner -> new LeaseBindingFormatUpgrade(bindingControl)
              .upgrade(owner, Clock.systemUTC().millis(), startup), startup), startup);
    } else if (bindingState.supportsLeaseBindings()) {
      await(ownerLease.transitionFence(bindingOwners,
          CompletableFuture::completedFuture, startup), startup);
    } else {
      throw new IllegalStateException("table-group control format cannot be activated for J41");
    }

    CommitState verified = await(bindingControl.read(startup), startup)
        .orElseThrow(() -> new IllegalStateException("table-group control state is not initialized"))
        .state();
    CommitOwnerService.OwnerToken owner = ownerLease.token();
    if (!verified.supportsLeaseBindings()
        || verified.ownerInstance().filter(owner.instance()::equals).isEmpty()
        || verified.ownerEpoch() != owner.epoch()
        || verified.ownerLeaseExpired(Clock.systemUTC().millis()) || !ownerLease.isReady()) {
      throw new IllegalStateException("J41 control format upgrade lost its owner fence");
    }
  }

  private static CompletableFuture<Void> ensureAuxiliaryDescriptor(M3TableGroupPlan plan,
      HBaseConnectionFactory connections, AuthorizedBackendIdentity identity, RequestScope scope) {
    var request = new HBaseRequestContext(scope);
    return request.borrowConnection(connections, identity).thenCompose(connection -> {
      var admin = connection.getAdmin();
      return request.execute(() -> admin.getDescriptor(plan.system().table())).thenCompose(descriptor -> {
        if (plan.system().supportsAuxiliary(descriptor)) return CompletableFuture.completedFuture(null);
        var replacement = plan.system().withAuxiliaryCapability(descriptor);
        return request.execute(() -> admin.modifyTable(replacement))
            .thenCompose(ignored -> request.execute(() -> admin.getDescriptor(plan.system().table())))
            .thenAccept(verified -> {
              if (!plan.system().supportsAuxiliary(verified)) {
                throw new IllegalStateException("M4 system-table capability was not persisted");
              }
            });
      });
    }).exceptionallyCompose(failure -> CompletableFuture.failedFuture(HBaseFailures.classify(failure)));
  }

  private static CompletableFuture<Void> ensureLeaseBindingDescriptor(M3TableGroupPlan plan,
      HBaseConnectionFactory connections, AuthorizedBackendIdentity identity, RequestScope scope) {
    var request = new HBaseRequestContext(scope);
    return request.borrowConnection(connections, identity).thenCompose(connection -> {
      var admin = connection.getAdmin();
      return request.execute(() -> admin.getDescriptor(plan.system().table())).thenCompose(descriptor -> {
        if (plan.system().supportsLeaseBindings(descriptor)) return CompletableFuture.completedFuture(null);
        var replacement = plan.system().withLeaseBindingCapability(descriptor);
        return request.execute(() -> admin.modifyTable(replacement))
            .thenCompose(ignored -> request.execute(() -> admin.getDescriptor(plan.system().table())))
            .thenAccept(verified -> {
              if (!plan.system().supportsLeaseBindings(verified)) {
                throw new IllegalStateException("J41 system-table capability was not persisted");
              }
            });
      });
    }).exceptionallyCompose(failure -> CompletableFuture.failedFuture(HBaseFailures.classify(failure)));
  }

  private static void ensureM4LeaseExpiryFormat(M3TableGroupPlan plan,
      HBaseConnectionFactory connections, AuthorizedBackendIdentity identity,
      M3OwnerLease ownerLease, RequestScope startup) throws Exception {
    if (!ownerLease.isReady()) {
      throw new BackendFailure(BackendFailure.Category.UNAVAILABLE);
    }
    await(ensureLeaseExpiryDescriptor(plan, connections, identity, startup), startup);
    HBaseCommitControlStore expiryControl = await(
        HBaseCommitControlStore.open(connections, identity, plan.system(), startup), startup);
    if (!expiryControl.supportsAuxiliary() || !expiryControl.supportsLeaseBindings()
        || !expiryControl.supportsLeaseExpiryCheckpoints()) {
      throw new IllegalStateException("J43 expiry checkpoint descriptor capability is not available");
    }
    CommitOwnerService expiryOwners = new CommitOwnerService(expiryControl);
    CommitState expiryState = await(expiryControl.read(startup), startup)
        .orElseThrow(() -> new IllegalStateException("table-group control state is not initialized"))
        .state();
    if (expiryState.formatVersion() == CommitState.LEASE_BINDING_FORMAT_VERSION) {
      await(ownerLease.transitionFence(expiryOwners,
          owner -> new LeaseExpiryFormatUpgrade(expiryControl)
              .upgrade(owner, Clock.systemUTC().millis(), startup), startup), startup);
    } else if (expiryState.supportsLeaseExpiryCheckpoints()) {
      await(ownerLease.transitionFence(expiryOwners,
          CompletableFuture::completedFuture, startup), startup);
    } else {
      throw new IllegalStateException("table-group control format cannot be activated for J43");
    }

    CommitState verified = await(expiryControl.read(startup), startup)
        .orElseThrow(() -> new IllegalStateException("table-group control state is not initialized"))
        .state();
    CommitOwnerService.OwnerToken owner = ownerLease.token();
    if (!verified.supportsLeaseExpiryCheckpoints()
        || verified.ownerInstance().filter(owner.instance()::equals).isEmpty()
        || verified.ownerEpoch() != owner.epoch()
        || verified.ownerLeaseExpired(Clock.systemUTC().millis()) || !ownerLease.isReady()) {
      throw new IllegalStateException("J43 control format upgrade lost its owner fence");
    }
  }

  private static CompletableFuture<Void> ensureLeaseExpiryDescriptor(M3TableGroupPlan plan,
      HBaseConnectionFactory connections, AuthorizedBackendIdentity identity, RequestScope scope) {
    var request = new HBaseRequestContext(scope);
    return request.borrowConnection(connections, identity).thenCompose(connection -> {
      var admin = connection.getAdmin();
      return request.execute(() -> admin.getDescriptor(plan.system().table())).thenCompose(descriptor -> {
        if (plan.system().supportsLeaseExpiryCheckpoints(descriptor)) {
          return CompletableFuture.completedFuture(null);
        }
        var replacement = plan.system().withLeaseExpiryCheckpointCapability(descriptor);
        return request.execute(() -> admin.modifyTable(replacement))
            .thenCompose(ignored -> request.execute(() -> admin.getDescriptor(plan.system().table())))
            .thenAccept(verified -> {
              if (!plan.system().supportsLeaseExpiryCheckpoints(verified)) {
                throw new IllegalStateException("J43 system-table capability was not persisted");
              }
            });
      });
    }).exceptionallyCompose(failure -> CompletableFuture.failedFuture(HBaseFailures.classify(failure)));
  }

  private static void verifyLeaseExpiryFormat(TableGroupStores stores, RequestScope scope) throws Exception {
    CommitState state = await(stores.control().read(scope), scope)
        .orElseThrow(() -> new IllegalStateException("table-group control state is not initialized")).state();
    if (!state.supportsLeaseExpiryCheckpoints()
        || !stores.control().supportsAuxiliary() || !stores.control().supportsLeaseBindings()
        || !stores.control().supportsLeaseExpiryCheckpoints()
        || !stores.records().supportsAuxiliary() || !stores.records().supportsLeaseBindings()
        || !stores.records().supportsLeaseExpiryCheckpoints()) {
      throw new IllegalStateException("J43 Lease expiry checkpoint format is not fenced and available");
    }
  }

  /** The owner fences old writers before finalizers require an index for each new KV revision. */
  private static void ensureM4EventIndexFormat(M3TableGroupPlan plan,
      HBaseConnectionFactory connections, AuthorizedBackendIdentity identity,
      M3OwnerLease ownerLease, RequestScope startup) throws Exception {
    if (!ownerLease.isReady()) throw new BackendFailure(BackendFailure.Category.UNAVAILABLE);
    await(ensureEventIndexDescriptor(plan, connections, identity, startup), startup);
    HBaseCommitControlStore control = await(
        HBaseCommitControlStore.open(connections, identity, plan.system(), startup), startup);
    if (!control.supportsEventRevisionIndex()) {
      throw new IllegalStateException("J44 event index descriptor capability is not available");
    }
    var owners = new CommitOwnerService(control);
    CommitState state = await(control.read(startup), startup)
        .orElseThrow(() -> new IllegalStateException("table-group control state is not initialized"))
        .state();
    if (state.formatVersion() == CommitState.LEASE_EXPIRY_FORMAT_VERSION) {
      await(ownerLease.transitionFence(owners,
          owner -> new EventIndexFormatUpgrade(control)
              .upgrade(owner, Clock.systemUTC().millis(), startup), startup), startup);
    } else if (state.supportsEventRevisionIndex()) {
      await(ownerLease.transitionFence(owners, CompletableFuture::completedFuture, startup), startup);
    } else {
      throw new IllegalStateException("table-group control format cannot be activated for J44");
    }
    CommitState verified = await(control.read(startup), startup)
        .orElseThrow(() -> new IllegalStateException("table-group control state is not initialized"))
        .state();
    CommitOwnerService.OwnerToken owner = ownerLease.token();
    if (!verified.supportsEventRevisionIndex()
        || verified.ownerInstance().filter(owner.instance()::equals).isEmpty()
        || verified.ownerEpoch() != owner.epoch()
        || verified.ownerLeaseExpired(Clock.systemUTC().millis()) || !ownerLease.isReady()) {
      throw new IllegalStateException("J44 event index format upgrade lost its owner fence");
    }
  }

  private static CompletableFuture<Void> ensureEventIndexDescriptor(M3TableGroupPlan plan,
      HBaseConnectionFactory connections, AuthorizedBackendIdentity identity, RequestScope scope) {
    var request = new HBaseRequestContext(scope);
    return request.borrowConnection(connections, identity).thenCompose(connection -> {
      var admin = connection.getAdmin();
      return request.execute(() -> admin.getDescriptor(plan.system().table())).thenCompose(descriptor -> {
        if (plan.system().supportsEventRevisionIndex(descriptor)) return CompletableFuture.completedFuture(null);
        var replacement = plan.system().withEventRevisionIndexCapability(descriptor);
        return request.execute(() -> admin.modifyTable(replacement))
            .thenCompose(ignored -> request.execute(() -> admin.getDescriptor(plan.system().table())))
            .thenAccept(verified -> {
              if (!plan.system().supportsEventRevisionIndex(verified)) {
                throw new IllegalStateException("J44 event index descriptor was not persisted");
              }
            });
      });
    }).exceptionallyCompose(failure -> CompletableFuture.failedFuture(HBaseFailures.classify(failure)));
  }

  private static void verifyEventIndexFormat(TableGroupStores stores, RequestScope scope) throws Exception {
    CommitState state = await(stores.control().read(scope), scope)
        .orElseThrow(() -> new IllegalStateException("table-group control state is not initialized")).state();
    if (!state.supportsEventRevisionIndex() || !stores.control().supportsEventRevisionIndex()
        || !stores.records().supportsEventRevisionIndex()) {
      throw new IllegalStateException("J44 event index format is not fenced and available");
    }
  }

  /** Selects a fully staged protection registry before the runtime can consume event history. */
  private static EventHistoryPinManager openEventRegistry(TableGroupStores stores,
      M3RuntimeConfiguration activation, M3OwnerLease owner, RequestScope scope) throws Exception {
    CommitState state = await(stores.control().read(scope), scope)
        .orElseThrow(() -> new IllegalStateException("table-group control state is not initialized")).state();
    if (state.eventRegistryNamespace().isEmpty()) {
      if (!owner.isOwner()) throw new BackendFailure(BackendFailure.Category.UNAVAILABLE);
      var owners = new CommitOwnerService(stores.control());
      var upgrade = new EventHistoryRegistryUpgrade(stores.records(), stores.control(),
          activation.pinConfiguration(), Clock.systemUTC());
      await(owner.transitionFence(owners,
          token -> upgrade.upgrade(token, scope).thenApply(EventHistoryRegistryUpgrade.Selection::owner),
          scope), scope);
    }
    return await(EventHistoryPinManager.openSelected(stores.records(), stores.control(),
        activation.pinConfiguration(), Clock.systemUTC(), scope), scope);
  }

  private record BusinessComponents(PutOwnerProcessor putProcessor, TxnOwnerProcessor txnProcessor,
      TxnReadExecutor txnReads, RequestMetrics metrics, Optional<LeaseComponents> lease,
      CommitPublisher publisher) {}

  private record LeaseComponents(LeaseGrantOwnerProcessor grantProcessor,
      LeaseRenewalOwnerProcessor renewalProcessor,
      LeaseRevocationOwnerProcessor revocationProcessor,
      LeaseExpiryStepOwnerProcessor expiryStepProcessor,
      LeaseReadView readView, LeaseEnumerationReader enumeration, LeaseBindingReader bindings,
      LeaseTimeSource time, LeaseBindingMutationPlanner bindingPlanner) {}

  private record ServiceComponents(KvService kvService, LeaseService leaseService,
      WatchService watchService, AuthService authService) {}

  record CacheLimits(long maximumBytes, int maximumEntries, int maximumRecords,
      int maximumDecisions, long maximumCandidateBytes, BudgetManager.Reservation proofReservation,
      BudgetManager.ReadLimits proofReads, int maximumProofPages, long maximumProofBytes,
      int maximumEntriesExamined, long maximumProofNanos) {}

  private static BusinessComponents assembleBusiness(M3TableGroupPlan plan, M3RuntimeConfiguration activation,
      HBaseConnectionFactory connections, BudgetManager budgets, ScanScheduler scans, Pending pending,
      TableGroupStores stores, InvocationService invocations, M3OwnerLease owner,
      Optional<CacheLimits> cacheLimits,
      BiFunction<Long, Integer, LeaseTimeSource> leaseTimeSourceFactory)
      throws BackendFailure {
    var preparation = new PutCommitPreparation(new ResultPreparation(stores.resultRepository()),
        stores.results(), new ManifestPreparation(stores.manifestRepository(),
            new ManifestPreparation.Limits(1, 4L << 20, 1, 8L << 20)), invocations);
    var publisher = new CommitPublisher(stores.control(), stores.outcomes(),
        new CommitPlanVerifier(stores.manifests(), stores.materialization(), stores.results(),
            stores.auxiliaryMaterialization(), new AuxiliaryLeasePlanVerifier(
                LeaseStateMessages::decode, LeaseBindingMessages::decode)),
        stores.invocations(), pending.handoff, stores.finalizer());
    var metrics = new RequestMetrics();
    Optional<LeaseComponents> lease = activation.m4LeaseApiEnabled()
        ? Optional.of(assembleLease(
            plan, activation, budgets, connections, pending, stores, invocations, owner, publisher,
            metrics, leaseTimeSourceFactory))
        : Optional.empty();
    PutMutationExecutor executor = lease.isPresent()
        ? new PutMutationExecutor(plan.source().tableGroupId(), plan.system().profileFingerprint(),
            stores.control(), stores.views(), stores.manifests(), connections, plan.primary(), pending.worker,
            preparation, publisher, 5L << 20, 8L << 20,
            lease.orElseThrow().bindingPlanner(), metrics)
        : new PutMutationExecutor(plan.source().tableGroupId(), plan.system().profileFingerprint(),
            stores.control(), stores.views(), stores.manifests(), connections, plan.primary(), pending.worker,
            preparation, publisher, 5L << 20, 8L << 20, metrics);
    BudgetManager.Reservation mutationReservation = lease.isPresent()
        ? LEASE_REQUEST_MEMORY : REQUEST_MEMORY;
    var putProcessor = new PutOwnerProcessor(plan.source().tableGroupId(), owner::currentToken, budgets,
        mutationReservation, executor, stores.outcomes());
    var txnLimits = new TxnExecutionLimits(5L << 20, 8L << 20, 32L << 20, 4096, 4096);
    var txnPreparation = new TxnCommitPreparation(new ResultPreparation(stores.resultRepository()),
        stores.results(), new ManifestPreparation(stores.manifestRepository(),
            new ManifestPreparation.Limits(128, 4L << 20, 128, 8L << 20)), invocations, 8L << 20);
    TxnMutationExecutor txnExecutor = lease.isPresent()
        ? new TxnMutationExecutor(plan.source().tableGroupId(), plan.system().profileFingerprint(),
            stores.control(), stores.views(), stores.manifests(), stores.records(), stores.manifestRepository(),
            connections, plan.primary(), pending.worker, scans, txnPreparation, publisher,
            txnLimits, lease.orElseThrow().bindingPlanner(), metrics)
        : new TxnMutationExecutor(plan.source().tableGroupId(), plan.system().profileFingerprint(),
            stores.control(), stores.views(), stores.manifests(), stores.records(), stores.manifestRepository(),
            connections, plan.primary(), pending.worker, scans, txnPreparation, publisher, txnLimits, metrics);
    var txnProcessor = new TxnOwnerProcessor(plan.source().tableGroupId(), owner::currentToken, budgets,
        mutationReservation, txnExecutor, stores.outcomes(), activation.maxEtcdRequestBytes());
    if (cacheLimits.isPresent()) pending.createCache(plan, stores, budgets, cacheLimits.orElseThrow());
    var txnReads = new TxnReadExecutor(plan.source().tableGroupId(), stores.views(), stores.manifests(),
        connections, plan.primary(), pending.worker, scans, txnLimits, metrics, Optional.ofNullable(pending.cache));
    return new BusinessComponents(putProcessor, txnProcessor, txnReads, metrics, lease, publisher);
  }

  private static LeaseComponents assembleLease(M3TableGroupPlan plan,
      M3RuntimeConfiguration activation, BudgetManager budgets,
      HBaseConnectionFactory connections, Pending pending, TableGroupStores stores,
      InvocationService invocations, M3OwnerLease owner, CommitPublisher publisher,
      RequestMetrics metrics, BiFunction<Long, Integer, LeaseTimeSource> leaseTimeSourceFactory) {
    int maximumMutationRecords = LeaseBindingMutationPlanner.MAX_AUXILIARY_RECORDS_PER_MUTATION;
    var leaseReads = new LeaseReadView(stores.records(), stores.manifests(), LeaseStateMessages::decode,
        maximumMutationRecords, 64L << 20);
    var enumeration = new LeaseEnumerationReader(stores.records(), stores.manifests(),
        LeaseStateMessages::decode, plan.primary().codec().saltProfile().bucketCount(),
        64, 8L << 10, 100_000, 100_000, maximumMutationRecords, 64L << 20);
    var bindings = new LeaseBindingReader(stores.records(), stores.manifests(),
        LeaseBindingMessages::decode, LeaseGateway.BINDING_PAGE_ENTRIES,
        LeaseGateway.bindingPageBytes(activation.maxEtcdRequestBytes()), 100_000, 64L << 20,
        100_000, activation.leaseCostLimits().maximumKeyBytes(), maximumMutationRecords,
        64L << 20);
    LeaseTimeSource time = Objects.requireNonNull(
        leaseTimeSourceFactory.apply(activation.leaseClockUncertaintyMs(),
            activation.leaseClockMaxDriftPpm()), "Lease time source");
    var bindingReadView = new LeaseBindingReadView(stores.records(), stores.manifests(),
        LeaseBindingMessages::decode, maximumMutationRecords, 64L << 20);
    var leaseCosts = new LeaseCostAdmission(activation.leaseCostLimits());
    BudgetManager.Reservation revocationReservation =
        LeaseBudgetProfile.ownerMutationReservation(activation.maxEtcdRequestBytes());
    var bindingPlanner = new LeaseBindingMutationPlanner(leaseReads, bindingReadView, time,
        leaseCosts, plan.primary().codec().saltProfile().bucketCount());
    var allocator = new LeaseIdAllocator(
        () -> ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE), 16);
    LeaseCostAdmission.Limits leaseLimits = activation.leaseCostLimits();
    // Lease-only mutations may seal one tombstone and DELETE event per admitted binding.
    var mutationPreparation = new LeaseAuxiliaryMutationPreparation(
        new ResultPreparation(stores.resultRepository()), stores.results(),
        new ManifestPreparation(stores.manifestRepository(),
            new ManifestPreparation.Limits(leaseLimits.maximumBindings(), 0,
                leaseLimits.maximumBindings(), leaseLimits.maximumRevokeCostBytes())), invocations);
    var preparation = new LeaseGrantPreparation(mutationPreparation);
    var executor = new LeaseGrantExecutor(plan.source().tableGroupId(),
        plan.system().profileFingerprint(), stores.control(), stores.views(), leaseReads, allocator,
        time, activation.leaseMinTtlSeconds(), LeaseTimeRules.MAX_TTL_SECONDS, Clock.systemUTC(),
        preparation, publisher, plan.primary().codec().saltProfile().bucketCount());
    var renewalPreparation = new LeaseRenewalPreparation(mutationPreparation);
    var processor = new LeaseGrantOwnerProcessor(plan.source().tableGroupId(), owner::currentToken,
        budgets, LEASE_REQUEST_MEMORY, executor, stores.outcomes(), invocations, Clock.systemUTC());
    var revocationExecutor = new LeaseRevocationExecutor(plan.source().tableGroupId(),
        plan.system().profileFingerprint(), stores.clusterId(), activation.memberIdBits(),
        stores.control(), stores.views(), stores.records(), leaseReads, bindings, bindingPlanner, leaseCosts,
        mutationPreparation, publisher, connections, plan.primary(), pending.worker,
        Clock.systemUTC(), time, metrics);
    var renewalExecutor = new LeaseRenewalExecutor(plan.source().tableGroupId(),
        plan.system().profileFingerprint(), stores.clusterId(), activation.memberIdBits(),
        stores.control(), stores.views(), leaseReads, time, Clock.systemUTC(),
        renewalPreparation, revocationExecutor, publisher, metrics,
        plan.primary().codec().saltProfile().bucketCount());
    var renewalProcessor = new LeaseRenewalOwnerProcessor(plan.source().tableGroupId(), owner::currentToken,
        budgets, revocationReservation, renewalExecutor, stores.outcomes(), invocations, Clock.systemUTC());
    var revocationProcessor = new LeaseRevocationOwnerProcessor(plan.source().tableGroupId(),
        owner::currentToken, budgets, revocationReservation, revocationExecutor, stores.outcomes(),
        invocations, Clock.systemUTC());
    var expiryStepExecutor = new LeaseExpiryStepExecutor(plan.source().tableGroupId(),
        plan.system().profileFingerprint(), plan.system().saltProfile().bucketCount(),
        stores.control(), stores.views(), leaseReads, stores.records(), time, Clock.systemUTC(),
        mutationPreparation, publisher, revocationExecutor, metrics);
    var expiryStepProcessor = new LeaseExpiryStepOwnerProcessor(plan.source().tableGroupId(),
        owner::currentToken, budgets, revocationReservation, expiryStepExecutor, stores.outcomes(),
        invocations, Clock.systemUTC());
    return new LeaseComponents(processor, renewalProcessor, revocationProcessor,
        expiryStepProcessor, leaseReads, enumeration, bindings,
        time, bindingPlanner);
  }

  /** Refuses a stricter restart configuration that could strand a previously accepted Lease. */
  private static void verifyExistingLeaseLimits(M3TableGroupPlan plan,
      M3RuntimeConfiguration activation, BudgetManager budgets, TableGroupStores stores,
      ByteKey authorization, LeaseComponents lease, RequestScope startup) throws Exception {
    var account = budgets.admit(plan.source().tableGroupId(), startup, LeaseBudgetProfile.startupLeaseScanMemory(),
        new BudgetManager.ReadLimits(100_000, 64L << 20, 100_000, 100_000, 100_000));
    try {
      var proof = new ManifestCatalog.ProofReadBudget(startup, account, 100_000, 64L << 20);
      var pin = await(stores.views().openAuxiliary(authorization, startup), startup);
      var existing = await(lease.enumeration().read(pin, startup, account, proof), startup);
      var admission = new LeaseCostAdmission(activation.leaseCostLimits());
      for (var state : existing) admission.requireExistingStateFits(state);
      await(lease.readView().finish(pin, startup), startup);
    } finally {
      account.close();
    }
  }

  private static ServiceComponents assembleService(M3TableGroupPlan plan,
      M3RuntimeConfiguration activation, RuntimeConfiguration.ResourceBudgets limits,
      AuthorizedBackendIdentity identity, ByteKey authorization, BudgetManager budgets, Pending pending,
      TableGroupStores stores, InvocationService invocations, M3OwnerLease owner,
      BusinessComponents business, OwnerForwardingRuntime ownerForwarding,
      AuthSessionService authSessions) {
    java.util.function.Predicate<String> trustedPeer = peer -> ownerForwarding == null
        ? owner.currentToken().map(token -> token.instance().equals(peer)).orElse(false)
        : ownerForwarding.isTrustedPeerIdentity(peer);
    MutationForwardReceiver.AuthorizationResolver authorizationResolver = authSessions == null
        ? (peer, request, scope) -> {
          if (!trustedPeer.test(peer) || !request.tableGroupId().equals(plan.source().tableGroupId())
              || !authorization.equals(request.authorizationScopeDigest())
              || !ServiceAuthorizationContextReference.matches(identity, authorization,
                  request.authorizationContextReference())) {
            return CompletableFuture.failedFuture(
                new BackendFailure(BackendFailure.Category.PERMISSION));
          }
          return CompletableFuture.completedFuture(
              new MutationForwardReceiver.VerifiedAuthorization(identity, authorization));
        }
        : new SessionAuthorizationResolver(plan.source().tableGroupId(), authSessions, trustedPeer);
    var receiver = new MutationForwardReceiver(stores.control(), invocations,
        authorizationResolver,
        (request, binding, principal, scope) -> switch (MutationOperationEnvelope.kind(request.operationBytes())) {
          case PUT -> business.putProcessor().process(request, binding, principal, scope);
          case TRANSACTION, DELETE_RANGE -> business.txnProcessor().process(request, binding, principal, scope);
          case LEASE_GRANT -> business.lease().isPresent()
              ? business.lease().orElseThrow().grantProcessor().process(request, binding, principal, scope)
              : CompletableFuture.completedFuture(
                  new OwnerForwarding.Unknown(BackendFailure.Category.UNAVAILABLE));
          case LEASE_KEEP_ALIVE -> business.lease().isPresent()
              ? business.lease().orElseThrow().renewalProcessor().process(request, binding, principal, scope)
              : CompletableFuture.completedFuture(
                  new OwnerForwarding.Unknown(BackendFailure.Category.UNAVAILABLE));
          case LEASE_REVOKE -> business.lease().isPresent()
              ? business.lease().orElseThrow().revocationProcessor().process(request, binding, principal, scope)
              : CompletableFuture.completedFuture(
                  new OwnerForwarding.Unknown(BackendFailure.Category.UNAVAILABLE));
          case LEASE_EXPIRY_STEP -> business.lease().isPresent()
              ? business.lease().orElseThrow().expiryStepProcessor().process(request, binding, principal, scope)
              : CompletableFuture.completedFuture(
                  new OwnerForwarding.Unknown(BackendFailure.Category.UNAVAILABLE));
        });
    var lane = new MutationOwnerLane(plan.source().tableGroupId(), pending.queue, receiver, business.metrics());
    MutationRelay relay;
    if (ownerForwarding != null) {
      ownerForwarding.register(plan.source().tableGroupId(), lane);
      relay = new MutationRelay(stores.control(), ownerForwarding, ownerForwarding);
    } else {
      relay = localOwnerRelay(plan.source().tableGroupId(), stores.control(), owner, lane);
    }
    var gateway = new PutGateway(plan.source().tableGroupId(), identity, authorization,
        stores.clusterId(), activation.memberIdBits(), activation.maxEtcdRequestBytes(),
        stores.control(), relay, stores.invocationResults(), budgets, REQUEST_MEMORY, pending.worker,
        business.metrics(), authSessions);
    var txnGateway = new TxnGateway(plan.source().tableGroupId(), identity, authorization,
        stores.clusterId(), activation.memberIdBits(), activation.maxEtcdRequestBytes(),
        stores.control(), relay, stores.invocationResults(), business.txnReads(), budgets,
        REQUEST_MEMORY, pending.worker, business.metrics(), authSessions);
    LeaseService leaseService = new LeaseService();
    WatchService watchService = new WatchService();
    if (business.lease().isPresent()) {
      LeaseComponents lease = business.lease().orElseThrow();
      var leaseGateway = new LeaseGateway(plan.source().tableGroupId(), identity, authorization,
          stores.clusterId(), activation.memberIdBits(), activation.maxEtcdRequestBytes(),
          stores.control(), relay, stores.invocationResults(), stores.views(), lease.readView(),
          lease.enumeration(), lease.bindings(), lease.time(), budgets, LEASE_REQUEST_MEMORY,
          pending.worker, pending.leaseTimers, business.metrics());
      leaseService = new LeaseService(leaseGateway.grantHandler(),
          leaseGateway.timeToLiveHandler(), leaseGateway.leasesHandler(),
          leaseGateway.keepAliveHandler(), leaseGateway.revokeHandler());
      var expiryWorker = new LeaseExpiryWorker(plan.source().tableGroupId(), authorization,
          plan.system().saltProfile().bucketCount(), 30_000,
          stores.control(), stores.records(), owner, relay, identity, lease.time(), budgets,
          LeaseBudgetProfile.EXPIRY_WORKER_MEMORY, pending.worker, pending.leaseTimers, Clock.systemUTC(),
          business.metrics());
      pending.startExpiryWorker(expiryWorker);
      var backfillPreparation = new EventBackfillPreparation(new LeaseAuxiliaryMutationPreparation(
          new ResultPreparation(stores.resultRepository()), stores.results(),
          new ManifestPreparation(stores.manifestRepository(), new ManifestPreparation.Limits(0, 0, 0, 0)),
          invocations));
      var backfill = new EventBackfillCycle(plan.source().tableGroupId(), pending.queue, stores, pending.eventPins,
          invocations, backfillPreparation, business.publisher(), budgets, authorization, Clock.systemUTC());
      var tailer = new EventTailerCycle(plan.source().tableGroupId(), pending.queue,
          owner::currentToken, stores, pending.eventPins, budgets, authorization,
          pending, business.metrics());
      var history = new EventHistoryCycle(backfill, tailer, business.metrics());
      pending.installEventHistoryWorker(new EventHistoryWorker(
          pending.leaseTimers, owner::currentToken, history::run, business.metrics()));
      var selectedEvents = new SelectedSequenceReader(pending.eventPins, stores.control(),
          stores.decisions(), stores.manifestRepository(), stores.eventIndexes());
      var watchGateway = new WatchGateway(plan.source().tableGroupId(), pending,
          stores.control(), budgets, pending.worker, pending.leaseTimers,
          stores.clusterId(), activation.memberIdBits(), limits.maxRpcBytes(), pending.eventPins,
          selectedEvents, authorization, business.metrics());
      watchService = new WatchService(watchGateway.handler());
    }
    AuthService authService = authSessions == null ? new AuthService()
        : new AuthService(new AuthenticateRpcHandler(authSessions));
    return new ServiceComponents(new KvService(gateway, txnGateway, txnGateway::deleteRange,
        txnGateway::range), leaseService, watchService, authService);
  }

  private static MutationRelay localOwnerRelay(String tableGroupId, CommitControlStore control,
      M3OwnerLease owner, MutationOwnerLane lane) {
    return new MutationRelay(control,
        (group, instanceId, epoch) -> owner.currentToken()
            .filter(token -> group.equals(tableGroupId) && token.instance().equals(instanceId)
                && token.epoch() == epoch)
            .<CompletableFuture<OwnerForwarding.Route>>map(token -> CompletableFuture.completedFuture(
                new OwnerForwarding.Route(token.instance(), token.epoch(), token.instance(), token.instance())))
            .orElseGet(() -> CompletableFuture.failedFuture(
                new BackendFailure(BackendFailure.Category.UNAVAILABLE))),
        (route, request, scope) -> owner.currentToken()
            .filter(token -> route.ownerInstance().equals(token.instance())
                && route.ownerEpoch() == token.epoch())
            .<CompletableFuture<OwnerForwarding.Reply>>map(token ->
                lane.receive(token.instance(), request, scope))
            .orElseGet(() -> CompletableFuture.failedFuture(
                new BackendFailure(BackendFailure.Category.UNAVAILABLE))));
  }

  public KvService kvService() { return kvService; }
  public LeaseService leaseService() { return leaseService; }
  public WatchService watchService() { return watchService; }
  public AuthService authService() { return authService; }
  public CompletableFuture<SharedEventTailer> eventTailerReady() { return resources.eventTailerReady(); }
  public Optional<SharedEventTailer> eventTailer() { return resources.eventTailer(); }
  public long clusterId() { return clusterId; }
  public long memberId() { return memberId; }
  public RequestMetrics metrics() { return metrics; }
  long cacheHitCount() { return resources.cache == null ? 0 : resources.cache.hitCount(); }
  static boolean runtimeReady(boolean ownerReady, Optional<LeaseTimeSource> leaseTime) {
    Objects.requireNonNull(leaseTime, "leaseTime");
    return ownerReady && leaseTime.map(source -> source.observe().healthy()).orElse(true);
  }
  static boolean eventHistoryReady(boolean eventPinsReady, EventHistoryWorker history,
      SharedEventTailer tailer) {
    return eventPinsReady && history != null && history.isReady()
        && tailer != null && !tailer.isClosed();
  }

  public boolean isReady() { return runtimeReady(resources.isReady(), leaseTime); }
  public CompletableFuture<Void> drained() { return resources.drained.copy(); }
  @Override public void close() { resources.close(); }

  private static CompletableFuture<Void> abort(RequestScope startup, Pending pending, Throwable failure) {
    try { startup.closeAsync(CLEANUP); }
    catch (RuntimeException | Error cleanup) { if (failure != cleanup) failure.addSuppressed(cleanup); }
    if (pending != null) {
      pending.resolveNoOwnerIfUnstarted();
      try { pending.close(); }
      catch (RuntimeException | Error cleanup) { if (failure != cleanup) failure.addSuppressed(cleanup); }
      return pending.drained;
    }
    return CompletableFuture.allOf(startup.cleanupCompletion().toCompletableFuture(),
        startup.physicalProducersCompletion().toCompletableFuture());
  }

  /** Startup failed; callers retain the factory until this completion has settled successfully. */
  public static final class StartupException extends Exception {
    private static final long serialVersionUID = 1L;
    private final CompletableFuture<Void> cleanup;

    private StartupException(Throwable cause, CompletableFuture<Void> cleanup) {
      super("M3 table-group startup failed", cause);
      this.cleanup = cleanup;
    }

    public CompletionStage<Void> cleanup() { return cleanup.minimalCompletionStage(); }
  }

  /** Carries the same ownership completion when a fatal startup error must be rethrown. */
  public static final class StartupCleanup extends Exception {
    private static final long serialVersionUID = 1L;
    private final CompletableFuture<Void> cleanup;

    private StartupCleanup(CompletableFuture<Void> cleanup) {
      super("M3 startup cleanup is still owned", null, false, false);
      this.cleanup = cleanup;
    }

    public CompletionStage<Void> cleanup() { return cleanup.minimalCompletionStage(); }
  }

  private static ByteKey authorizationDigest(AuthorizedBackendIdentity identity) {
    return AuthorizationScopeDigest.fromIdentity(identity);
  }

  private static <T> T await(CompletableFuture<T> future, RequestScope scope) throws Exception {
    long remaining = scope.remainingDeadlineNanos();
    if (remaining <= 0) throw new TimeoutException("M3 startup deadline expired");
    try {
      return future.get(remaining, TimeUnit.NANOSECONDS);
    } catch (ExecutionException failure) {
      Throwable cause = failure.getCause();
      if (cause instanceof Exception exception) throw exception;
      if (cause instanceof Error fatal) throw fatal;
      throw new IllegalStateException("M3 startup failed", cause);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw interrupted;
    }
  }

  /** Owns all lanes until their real producers, timers, workers, and reserved recovery capacity drain. */
  static final class Pending {
    private final RequestScope startup;
    private final RequestScope recoveryScope = RequestScope.withoutDeadline();
    private final CompletableFuture<Void> ownerResolved = new CompletableFuture<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private final AtomicBoolean closing = new AtomicBoolean();
    private volatile M3OwnerLease owner;
    private volatile Throwable shutdownFailure;
    private boolean ownerStarted;
    private SerialMutationQueue queue;
    private BoundedPublicationHandoff handoff;
    private ThreadPoolExecutor worker;
    private RangeReadCache cache;
    private volatile EventHistoryPinManager eventPins;
    private volatile BudgetManager.Account eventTailerMemory;
    private volatile SharedEventTailer eventTailer;
    private CompletableFuture<SharedEventTailer> eventTailerReady = new CompletableFuture<>();
    private ScheduledExecutorService leaseTimers;
    private ScheduledThreadPoolExecutor ownedLeaseTimers;
    private volatile LeaseExpiryWorker expiryWorker;
    private volatile EventHistoryWorker eventHistoryWorker;
    private volatile boolean leaseEnabled;
    private boolean ownerLost;

    Pending(RequestScope startup, ScheduledExecutorService leaseTimers) {
      this(startup, leaseTimers, false);
    }

    Pending(RequestScope startup, ScheduledExecutorService leaseTimers, boolean leaseEnabled) {
      this.startup = startup;
      this.leaseTimers = leaseTimers;
      this.leaseEnabled = leaseEnabled;
    }

    private void createCache(M3TableGroupPlan plan, TableGroupStores stores, BudgetManager budgets,
        CacheLimits limits) throws BackendFailure {
      var prover = new UnchangedRangeProof(stores.decisions(), stores.manifests(),
          plan.primary().codec().saltProfile().bucketCount(), limits.maximumDecisions(),
          limits.maximumCandidateBytes(), plan.primary().codec()::bucket);
      var proof = new RangeReadCache.ProofOptions(prover, limits.proofReservation(), limits.proofReads(),
          limits.maximumProofPages(), limits.maximumProofBytes(), limits.maximumEntriesExamined(),
          limits.maximumProofNanos());
      cache = new RangeReadCache(stores.clusterId(), plan.system().profileFingerprint(), limits.maximumBytes(),
          limits.maximumEntries(), limits.maximumRecords(), budgets, plan.source().tableGroupId(),
          recoveryScope, Optional.of(proof));
    }

    private void initialize(M3TableGroupPlan plan, M3RuntimeConfiguration activation,
        RuntimeConfiguration.ResourceBudgets limits, BudgetManager budgets) throws BackendFailure {
      leaseEnabled = activation.m4LeaseApiEnabled();
      if (activation.m4LeaseApiEnabled() && leaseTimers == null) {
        ownedLeaseTimers = new ScheduledThreadPoolExecutor(1,
            Thread.ofPlatform().daemon().name("m4-lease-ready-timeout-", 0).factory());
        ownedLeaseTimers.setRemoveOnCancelPolicy(true);
        ownedLeaseTimers.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        leaseTimers = ownedLeaseTimers;
      }
      var account = budgets.admit(plan.source().tableGroupId(), recoveryScope, RECOVERY_MEMORY);
      var slot = account.tryAcquire(new BudgetManager.Work(0, 1, 0, 1, 0))
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      var released = new CompletableFuture<Void>();
      try {
        recoveryScope.registerScopedCleanup(() -> startup.physicalProducersCompletion().whenComplete((ignored, failure) -> {
          try { slot.close(); released.complete(null); }
          catch (RuntimeException | Error cleanup) { released.completeExceptionally(cleanup); }
        }), released);
      } catch (RuntimeException | Error failure) {
        try { slot.close(); } catch (RuntimeException | Error cleanup) { if (failure != cleanup) failure.addSuppressed(cleanup); }
        throw failure;
      }
      Duration deadline = Duration.ofMillis(Math.min(limits.requestDeadlineMs(), 300_000));
      long queueBytes = Math.addExact(8192L, Math.multiplyExact(8L, activation.maxEtcdRequestBytes()));
      queue = new SerialMutationQueue(0, queueBytes, 1,
          LeaseBudgetProfile.RESERVED_OWNER_QUEUE_BYTES, deadline);
      handoff = new BoundedPublicationHandoff(1, 1, deadline);
      int queuedWorkers = (int) Math.min(1024L, Math.max(8L, (long) limits.maxConcurrentRequests() * 2));
      worker = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queuedWorkers),
          Thread.ofPlatform().daemon().name("m3-backend-", 0).factory());
    }

    private void trackOwner(CompletableFuture<M3OwnerLease> future) {
      ownerStarted = true;
      future.whenComplete((value, failure) -> {
        owner = value;
        ownerResolved.complete(null);
      });
    }

    private void resolveNoOwnerIfUnstarted() {
      if (!ownerStarted) ownerResolved.complete(null);
    }

    synchronized void startExpiryWorker(LeaseExpiryWorker worker) {
      Objects.requireNonNull(worker, "worker");
      if (closing.get() || ownerLost || expiryWorker != null) {
        throw new IllegalStateException("Lease expiry worker cannot be installed now");
      }
      expiryWorker = worker;
      worker.start();
    }

    synchronized void installEventHistoryWorker(EventHistoryWorker worker) {
      Objects.requireNonNull(worker, "worker");
      if (closing.get() || ownerLost || eventHistoryWorker != null) {
        throw new IllegalStateException("event history worker cannot be installed now");
      }
      eventHistoryWorker = worker;
      try {
        worker.start();
      } catch (RuntimeException | Error failure) {
        eventHistoryWorker = null;
        throw failure;
      }
    }

    synchronized BudgetManager.Account eventTailerMemory(BudgetManager budgets, String tableGroup)
        throws BackendFailure {
      if (closing.get() || ownerLost || !leaseEnabled) {
        throw new BackendFailure(BackendFailure.Category.UNAVAILABLE);
      }
      if (eventTailerMemory == null) {
        eventTailerMemory = budgets.admitReserved(tableGroup, recoveryScope,
            LeaseBudgetProfile.EVENT_TAILER_MEMORY, LeaseBudgetProfile.EVENT_TAILER_READ_LIMITS);
      }
      return eventTailerMemory;
    }

    synchronized SharedEventTailer installEventTailer(SharedEventTailer candidate) {
      Objects.requireNonNull(candidate, "candidate");
      if (closing.get() || ownerLost || !leaseEnabled) {
        candidate.close();
        throw new java.util.concurrent.CompletionException(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      if (eventTailer != null) {
        candidate.close();
        return eventTailer;
      }
      eventTailer = candidate;
      eventTailerReady.complete(candidate);
      return candidate;
    }

    synchronized Optional<SharedEventTailer> eventTailer() {
      return Optional.ofNullable(eventTailer);
    }

    synchronized boolean detachClosedEventTailer(SharedEventTailer expected) {
      Objects.requireNonNull(expected, "expected");
      if (closing.get() || ownerLost || eventTailer != expected || !expected.isClosed()) return false;
      eventTailer = null;
      eventTailerReady = new CompletableFuture<>();
      return true;
    }

    synchronized CompletableFuture<SharedEventTailer> eventTailerReady() {
      return eventTailerReady.copy();
    }

    CompletableFuture<Void> initialReadiness() {
      if (!leaseEnabled) return CompletableFuture.completedFuture(null);
      LeaseExpiryWorker current = expiryWorker;
      if (current == null) {
        return CompletableFuture.failedFuture(
            new IllegalStateException("Lease expiry worker was not installed"));
      }
      EventHistoryWorker history = eventHistoryWorker;
      if (history == null) {
        return CompletableFuture.failedFuture(
            new IllegalStateException("event history worker was not installed"));
      }
      CompletableFuture<Void> historyReady = history.initialReadiness();
      historyReady.whenComplete((ignored, failure) -> {
        if (failure != null) eventTailerReady.completeExceptionally(failure);
      });
      CompletableFuture<Void> tailerReady = eventTailerReady.thenApply(tailer -> {
        if (tailer.isClosed()) {
          throw new java.util.concurrent.CompletionException(
              new BackendFailure(BackendFailure.Category.UNAVAILABLE));
        }
        return null;
      });
      return CompletableFuture.allOf(current.initialReadiness(), historyReady, tailerReady);
    }

    synchronized void ownerLost() {
      ownerLost = true;
      LeaseExpiryWorker current = expiryWorker;
      if (current != null) current.stop();
      EventHistoryWorker history = eventHistoryWorker;
      if (history != null) history.stop();
      SharedEventTailer tailer = eventTailer;
      if (tailer != null) tailer.close();
      else eventTailerReady.completeExceptionally(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      SerialMutationQueue currentQueue = queue;
      if (currentQueue != null) currentQueue.close();
    }

    private boolean isReady() {
      M3OwnerLease current = owner;
      if (closing.get() || current == null || !current.isReady()) return false;
      LeaseExpiryWorker expiry = expiryWorker;
      if (!leaseEnabled) return true;
      EventHistoryWorker history = eventHistoryWorker;
      return expiry != null && expiry.isReady()
          && eventHistoryReady(eventPins != null, history, eventTailer);
    }

    private void close() {
      synchronized (this) {
        if (!closing.compareAndSet(false, true)) return;
        try {
          if (expiryWorker != null) expiryWorker.stop();
        } catch (RuntimeException | Error failure) {
          shutdownFailure = failure;
        }
        try {
          if (eventHistoryWorker != null) eventHistoryWorker.stop();
        } catch (RuntimeException | Error failure) {
          shutdownFailure = merge(shutdownFailure, failure);
        }
        if (eventTailer == null) {
          eventTailerReady.completeExceptionally(new BackendFailure(BackendFailure.Category.CANCELLED));
        }
        try {
          if (queue != null) queue.close();
        } catch (RuntimeException | Error failure) {
          shutdownFailure = merge(shutdownFailure, failure);
        }
      }
      try {
        Thread.ofVirtual().name("m3-runtime-drain").start(this::drain);
      } catch (RuntimeException | Error failure) {
        shutdownFailure = merge(shutdownFailure, failure);
        drain();
        throw failure;
      }
    }

    private void drain() {
      Throwable failure = shutdownFailure;
      try {
        if (expiryWorker != null) expiryWorker.drained().join();
      } catch (RuntimeException | Error problem) { failure = merge(failure, problem); }
      try {
        if (eventHistoryWorker != null) eventHistoryWorker.drained().join();
      } catch (RuntimeException | Error problem) { failure = merge(failure, problem); }
      try {
        SharedEventTailer tailer = eventTailer;
        if (tailer != null) tailer.close();
      } catch (RuntimeException | Error problem) { failure = merge(failure, problem); }
      try {
        if (queue != null) queue.drained().join();
      } catch (RuntimeException | Error problem) { failure = merge(failure, problem); }
      try {
        if (handoff != null) { handoff.close(); handoff.drained().join(); }
      } catch (RuntimeException | Error problem) { failure = merge(failure, problem); }
      try {
        ownerResolved.join();
        if (owner != null) {
          owner.close();
          owner.drained().join();
        }
      } catch (RuntimeException | Error problem) { failure = merge(failure, problem); }
      try {
        if (owner != null) while (!owner.awaitTermination(Duration.ofSeconds(30))) { /* wait for the owned timer */ }
        if (queue != null) while (!queue.awaitTermination(Duration.ofSeconds(30))) { /* wait for the owned lane */ }
        if (handoff != null) while (!handoff.awaitTermination(Duration.ofSeconds(30))) { /* wait for the publication lane */ }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        failure = merge(failure, interrupted);
      }
      if (worker != null) {
        worker.shutdown();
        try {
          while (!worker.awaitTermination(30, TimeUnit.SECONDS)) { /* wait for owned backend work */ }
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          failure = merge(failure, interrupted);
        }
      }
      try {
        recoveryScope.closeAsync(CLEANUP);
        CompletableFuture.allOf(recoveryScope.cleanupCompletion().toCompletableFuture(),
            recoveryScope.physicalProducersCompletion().toCompletableFuture()).join();
      } catch (RuntimeException | Error problem) { failure = merge(failure, problem); }
      if (ownedLeaseTimers != null) {
        ownedLeaseTimers.shutdown();
        try {
          while (!ownedLeaseTimers.awaitTermination(30, TimeUnit.SECONDS)) {
            /* wait for owned response timeout tasks to stop */
          }
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          failure = merge(failure, interrupted);
        }
      }
      if (failure == null) drained.complete(null);
      else drained.completeExceptionally(failure);
    }

    private static Throwable merge(Throwable first, Throwable next) {
      if (first == null) return next;
      if (first != next) first.addSuppressed(next);
      return first;
    }
  }
}
