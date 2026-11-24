package io.etcdhbase.server;

import static io.etcdhbase.spi.identity.BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID;

import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.admin.TableGroupInitializationPlan;
import io.etcdhbase.admin.TableGroupPlanFactory;
import io.etcdhbase.admin.config.DevelopmentHBaseConfiguration;
import io.etcdhbase.admin.config.HBaseQuorum;
import io.etcdhbase.admin.config.InitializationConfiguration.AuthenticationMode;
import io.etcdhbase.admin.config.InitializationConfiguration.HBaseProfile;
import io.etcdhbase.core.auth.CredentialEnvelopeProtector;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.scan.ScanScheduler;
import io.etcdhbase.grpc.service.KvService;
import io.etcdhbase.grpc.service.LeaseService;
import io.etcdhbase.grpc.service.WatchService;
import io.etcdhbase.grpc.service.AuthService;
import io.etcdhbase.hbase.api.HBaseAccessVerifier;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.api.HBaseCredentialAuthenticator;
import io.etcdhbase.hbase.connection.HBaseConnectionRouter;
import io.etcdhbase.hbase.identity.HBaseAccessVerifiers;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.identity.KerberosUserContext;
import io.etcdhbase.hbase.storage.AuthControlBinding;
import io.etcdhbase.hbase.storage.HBaseAuthSessionStore;
import io.etcdhbase.server.commit.M3TableGroupRuntime;
import io.etcdhbase.server.commit.OwnerForwardingRuntime;
import io.etcdhbase.server.auth.AesGcmCredentialEnvelopeProtector;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.server.auth.CredentialKeyProvider;
import io.etcdhbase.server.auth.CredentialReferenceResolver;
import io.etcdhbase.server.auth.FileCredentialKeyProvider;
import io.etcdhbase.server.auth.ProviderSessionIdentityManager;
import io.etcdhbase.server.config.M3AuthenticationConfiguration;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.AuthControl;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.BackgroundIdentity;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.ClientProfile;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.CredentialSource;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.observability.RequestMetrics;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Clock;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.security.UserGroupInformation;

/** Owns one activated runtime group and every provider, route, and auth resource it depends on. */
final class M3RuntimeGroups implements AutoCloseable {
  private static final Executor CLEANUP = task -> Thread.ofVirtual().name("m3-group-cleanup").start(task);
  private final Map<String, ManagedGroup> groups;
  private final ScanScheduler scans;
  private final OwnerForwardingRuntime ownerForwarding;
  private final ScheduledThreadPoolExecutor leaseTimers;
  private final AtomicBoolean closing = new AtomicBoolean();
  private final CompletableFuture<Void> drained = new CompletableFuture<>();

  private M3RuntimeGroups(Map<String, ManagedGroup> groups, ScanScheduler scans,
      OwnerForwardingRuntime ownerForwarding, ScheduledThreadPoolExecutor leaseTimers) {
    this.groups = Map.copyOf(groups);
    this.scans = scans;
    this.ownerForwarding = ownerForwarding;
    this.leaseTimers = leaseTimers;
  }

  static M3RuntimeGroups open(RuntimeConfiguration configuration, M3RuntimeConfiguration activation)
      throws IOException {
    return openInternal(configuration, activation, Optional.empty());
  }

  static M3RuntimeGroups openWithLeaseTimeSource(RuntimeConfiguration configuration,
      M3RuntimeConfiguration activation, LongFunction<LeaseTimeSource> leaseTimeSourceFactory)
      throws IOException {
    return openInternal(configuration, activation,
        Optional.of(Objects.requireNonNull(leaseTimeSourceFactory, "leaseTimeSourceFactory")));
  }

  private static M3RuntimeGroups openInternal(RuntimeConfiguration configuration,
      M3RuntimeConfiguration activation,
      Optional<LongFunction<LeaseTimeSource>> leaseTimeSourceFactory) throws IOException {
    OwnerForwardingRuntime.RequestLimits ownerRequestLimits = activation.ownerForwarding() == null ? null
        : OwnerForwardingRuntime.requestLimits(activation.memoryBytes(), activation.maxEtcdRequestBytes(),
            configuration.resources().maxConcurrentRequests());
    long ownerTransportMemory = ownerRequestLimits == null ? 0 : ownerRequestLimits.reservedMemoryBytes();
    var budgets = M3RuntimeBudgets.create(configuration, activation, ownerTransportMemory);
    var scans = new ScanScheduler(budgets, Duration.ofMillis(configuration.resources().requestDeadlineMs()));
    ScheduledThreadPoolExecutor leaseTimers = activation.m4LeaseApiEnabled()
        ? leaseTimers() : null;
    Map<String, ManagedGroup> opened = new LinkedHashMap<>();
    OwnerForwardingRuntime ownerForwarding = null;
    try {
      if (activation.ownerForwarding() != null) {
        ownerForwarding = new OwnerForwardingRuntime(activation.ownerInstanceId(),
            activation.ownerForwarding(), ownerRequestLimits,
            configuration.resources().requestDeadlineMs(), activation.shutdownTimeoutMs());
      }
      for (String group : configuration.listeners().stream()
          .map(RuntimeConfiguration.Listener::tableGroupId).distinct().toList()) {
        var source = TableGroupPlanFactory.create(configuration.initialization(), group, null);
        var plan = M3TableGroupPlan.from(source);
        ManagedGroup managed = openGroup(plan, configuration, activation, budgets, scans,
            leaseTimeSourceFactory, ownerForwarding, leaseTimers);
        try {
          opened.put(group, managed);
        } catch (RuntimeException | Error failure) {
          try { managed.runtime().close(); }
          catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
          awaitCleanup(closeAfter(managed.runtime().drained(), managed.resources()),
              activation.shutdownTimeoutMs(), failure);
          throw failure;
        }
      }
      if (ownerForwarding != null) ownerForwarding.start();
      return new M3RuntimeGroups(opened, scans, ownerForwarding, leaseTimers);
    } catch (Exception | Error failure) {
      var rollback = new M3RuntimeGroups(opened, scans, ownerForwarding, leaseTimers);
      try { rollback.close(); }
      catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
      awaitCleanup(rollback.drained, activation.shutdownTimeoutMs(), failure);
      if (failure instanceof Error fatal) throw fatal;
      if (failure instanceof RuntimeException invalid) throw invalid;
      if (failure instanceof IOException io) throw io;
      throw new IOException("M3 table-group activation failed", failure);
    }
  }

  private static ManagedGroup openGroup(M3TableGroupPlan plan, RuntimeConfiguration configuration,
      M3RuntimeConfiguration activation, BudgetManager budgets, ScanScheduler scans,
      Optional<LongFunction<LeaseTimeSource>> leaseTimeSourceFactory,
      OwnerForwardingRuntime ownerForwarding, ScheduledThreadPoolExecutor leaseTimers) throws Exception {
    var resources = new GroupResources();
    boolean providerCloseDeferred = false;
    try {
      M3TableGroupRuntime runtime;
      try (var startup = RequestScope.withTimeout(Duration.ofMillis(activation.startupTimeoutMs()))) {
        IdentityActivation identity = activateIdentity(plan, configuration, activation, startup,
            resources);
        runtime = startRuntime(plan, configuration, activation, budgets, scans,
            identity.connections(), identity.identity(), startup, leaseTimeSourceFactory,
            ownerForwarding, leaseTimers, identity.sessionFactory());
      } catch (M3TableGroupRuntime.StartupException failure) {
        providerCloseDeferred = true;
        awaitCleanup(closeAfter(failure.cleanup(), resources), activation.shutdownTimeoutMs(), failure);
        throw failure;
      } catch (Error fatal) {
        providerCloseDeferred = awaitFatalStartupCleanup(fatal, resources, activation.shutdownTimeoutMs());
        throw fatal;
      }
      try {
        return new ManagedGroup(runtime, resources);
      } catch (RuntimeException | Error failure) {
        providerCloseDeferred = true;
        closeRuntimeAfterDrain(runtime, resources, activation.shutdownTimeoutMs(), failure);
        throw failure;
      }
    } catch (Exception | Error failure) {
      if (!providerCloseDeferred) closeResources(resources, failure);
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      throw failure;
    }
  }

  private static IdentityActivation activateIdentity(M3TableGroupPlan plan,
      RuntimeConfiguration configuration, M3RuntimeConfiguration activation,
      RequestScope startup, GroupResources resources) throws Exception {
    TableGroupInitializationPlan source = plan.source();
    if (source.hbaseProfile().authenticationMode() == AuthenticationMode.DEV_NO_AUTH) {
      if (!source.identityProviderId().equals(DEVELOPMENT_NO_AUTH_PROVIDER_ID)) {
        throw new IllegalArgumentException("development identity provider binding is invalid");
      }
      HBaseBackendIdentityProvider provider = createDevelopmentProvider(source, activation);
      resources.backendProvider = provider;
      return new IdentityActivation(provider, authenticate(provider, source, activation), null);
    }

    M3AuthenticationConfiguration auth = configuration.authentication();
    if (auth == null) throw new IllegalArgumentException("secure identity activation is not configured");
    String group = source.tableGroupId();
    var scope = new AuthenticationScope(group, source.hbaseProfile().id());
    var endpoint = auth.endpointBindings().stream()
        .filter(binding -> binding.tableGroupId().equals(group)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException("secure endpoint identity binding is missing"));
    BackgroundIdentity background = auth.backgroundIdentities().stream()
        .filter(binding -> binding.tableGroupId().equals(group)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException("secure background identity is missing"));
    if (!background.allowedTableGroupIds().equals(List.of(group))) {
      throw new IllegalArgumentException("background identity must be isolated to one table group");
    }

    List<HBaseAccessVerifiers.TableAccess> dataAccess = dataTableAccess(plan);
    ClientProfile backgroundClient = clientProfile(auth, background.clientProfileId());
    HBaseBackendIdentityProvider backend = createSecureProvider(configuration, backgroundClient,
        background.providerId(), scope, dataAccess, activation);
    resources.backendProvider = backend;
    AuthorizedBackendIdentity serviceIdentity = activateConfiguredIdentity(backend,
        background.identity(), auth.secretDirectory(), scope, activation);

    HBaseConnectionRouter router = new HBaseConnectionRouter(backend);
    resources.router = router;
    ClientProfile endpointClient = clientProfile(auth, endpoint.clientProfileId());
    ProviderSessionIdentityManager identities = new ProviderSessionIdentityManager(
        () -> createSecureProvider(configuration, endpointClient, endpoint.providerId(),
            scope, dataAccess, activation), router, scope, endpoint.providerId(),
        auth.maximumIdentityContainers());
    resources.sessionIdentities = identities;

    AuthControl configuredControl = auth.authControl();
    HBaseProfile controlProfile = hbaseProfile(configuration, configuredControl.hbaseProfileId());
    ClientProfile controlClient = clientProfile(auth, configuredControl.clientProfileId());
    var controlScope = new AuthenticationScope("auth-control", configuredControl.hbaseProfileId());
    var saltConfiguration = configuration.saltProfiles().stream()
        .filter(profile -> profile.id().equals(configuredControl.saltProfileId())).findFirst()
        .orElseThrow(() -> new IllegalArgumentException("auth-control salt profile is missing"));
    var salt = new SaltProfile(saltConfiguration.strategyId(), saltConfiguration.encodingVersion(),
        saltConfiguration.bucketCount(), saltConfiguration.saltWidthBytes());
    var binding = new AuthControlBinding(
        TableName.valueOf(controlProfile.namespace(), configuredControl.tableName()),
        auth.trustDomain(), new HashSaltStrategy(salt));
    var controlAccess = List.of(new HBaseAccessVerifiers.TableAccess(binding.table(),
        Set.of(Action.READ, Action.WRITE, Action.ADMIN)));
    HBaseBackendIdentityProvider controlProvider = createSecureProvider(configuration,
        controlClient, configuredControl.providerId(), controlScope, controlAccess, activation);
    resources.authControlProvider = controlProvider;
    AuthorizedBackendIdentity controlIdentity = activateConfiguredIdentity(controlProvider,
        configuredControl.identity(), auth.secretDirectory(), controlScope, activation);
    HBaseAuthSessionStore store = HBaseAuthSessionStore.open(controlProvider, controlIdentity,
        binding, startup).get(activation.startupTimeoutMs(), TimeUnit.MILLISECONDS);

    Map<String, Path> keyFiles = new LinkedHashMap<>();
    for (var key : auth.credentialKeyRing().keys()) {
      keyFiles.put(key.keyId(), CredentialReferenceResolver.resolvePrivateSecretFile(
          key.secretReference(), auth.secretDirectory()));
    }
    CredentialKeyProvider keys = new FileCredentialKeyProvider(
        auth.credentialKeyRing().activeKeyId(), keyFiles);
    resources.credentialKeys = keys;
    for (var key : auth.credentialKeyRing().keys()) {
      byte[] material = keys.keyBytes(key.keyId());
      try {
        if (material.length != FileCredentialKeyProvider.KEY_BYTES) {
          throw new IllegalArgumentException("credential encryption key has an invalid size");
        }
      } finally {
        java.util.Arrays.fill(material, (byte) 0);
      }
    }
    CredentialEnvelopeProtector protector = new AesGcmCredentialEnvelopeProtector(
        keys, new SecureRandom());
    LongFunction<AuthSessionService> sessionFactory = clusterId -> new AuthSessionService(
        store, protector, identities, auth.trustDomain(), endpoint.providerId(), scope,
        datasetDigest(source.fingerprint(), auth.trustDomain(), clusterId),
        Duration.ofMillis(auth.sessionTtlMillis()), auth.sessionSlots(), Clock.systemUTC(),
        new SecureRandom());
    return new IdentityActivation(router, serviceIdentity, sessionFactory);
  }

  private static HBaseBackendIdentityProvider createDevelopmentProvider(
      TableGroupInitializationPlan source, M3RuntimeConfiguration activation) {
    var scope = new AuthenticationScope(source.tableGroupId(), source.hbaseProfile().id());
    var timeout = Duration.ofMillis(Math.min(activation.startupTimeoutMs(), 300_000));
    return new HBaseBackendIdentityProvider(source.identityProviderId(), scope,
        DevelopmentHBaseConfiguration.create(source.hbaseProfile()), HBaseAuthenticators.simpleDevelopment(),
        (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
        1, 1, timeout);
  }

  private static AuthorizedBackendIdentity authenticate(HBaseBackendIdentityProvider provider,
      TableGroupInitializationPlan source, M3RuntimeConfiguration activation) throws Exception {
    var scope = new AuthenticationScope(source.tableGroupId(), source.hbaseProfile().id());
    String principal = UserGroupInformation.getCurrentUser().getShortUserName();
    try (var credentials = new BackendCredentials(principal, new char[0])) {
      return provider.authenticate(credentials, scope).toCompletableFuture()
          .get(activation.startupTimeoutMs(), TimeUnit.MILLISECONDS);
    }
  }

  private static M3TableGroupRuntime startRuntime(M3TableGroupPlan plan,
      RuntimeConfiguration configuration, M3RuntimeConfiguration activation,
      BudgetManager budgets, ScanScheduler scans, HBaseConnectionFactory connections,
      AuthorizedBackendIdentity identity, RequestScope startup,
      Optional<LongFunction<LeaseTimeSource>> leaseTimeSourceFactory,
      OwnerForwardingRuntime ownerForwarding, ScheduledThreadPoolExecutor leaseTimers,
      LongFunction<AuthSessionService> authSessionFactory) throws Exception {
    if (authSessionFactory != null) {
      if (leaseTimeSourceFactory.isPresent()) {
        throw new IllegalArgumentException("authenticated M4 test clock activation is not configured");
      }
      return M3TableGroupRuntime.openAuthenticated(plan, activation, configuration.resources(),
          connections, identity, budgets, scans, startup, ownerForwarding, leaseTimers,
          authSessionFactory);
    }
    if (leaseTimeSourceFactory.isPresent()) {
      return M3TableGroupRuntime.openWithLeaseTimeSource(plan, activation, configuration.resources(),
          connections, identity, budgets, scans, startup, leaseTimeSourceFactory.orElseThrow(),
          ownerForwarding, leaseTimers);
    }
    return M3TableGroupRuntime.open(plan, activation, configuration.resources(),
        connections, identity, budgets, scans, startup, ownerForwarding, leaseTimers);
  }

  private static HBaseBackendIdentityProvider createSecureProvider(
      RuntimeConfiguration runtime, ClientProfile client, String providerId,
      AuthenticationScope scope, List<HBaseAccessVerifiers.TableAccess> access,
      M3RuntimeConfiguration activation) {
    HBaseProfile profile = hbaseProfile(runtime, client.hbaseProfileId());
    if (profile.authenticationMode() != AuthenticationMode.CREDENTIAL_REFERENCE) {
      throw new IllegalArgumentException("secure client profile does not select a credentialed backend");
    }
    Configuration configuration = secureHBaseConfiguration(profile, client);
    HBaseCredentialAuthenticator authenticator = authenticator(client);
    HBaseAccessVerifier permissions = HBaseAccessVerifiers.tablePermissions(access);
    HBaseAccessVerifier verifiedAccess = (connection, requestedScope) ->
        permissions.verify(connection, requestedScope).thenCompose(ignored -> {
          if (client.clusterId() == null) return CompletableFuture.completedFuture(null);
          return connection.getAdmin().getClusterMetrics(
              EnumSet.of(ClusterMetrics.Option.CLUSTER_ID)).thenCompose(metrics -> {
                if (!client.clusterId().equals(metrics.getClusterId())) {
                  return CompletableFuture.<Void>failedFuture(new BackendIdentityException(
                      BackendIdentityException.Reason.INSUFFICIENT_PERMISSION));
                }
                return CompletableFuture.completedFuture(null);
              });
        });
    var timeout = Duration.ofMillis(Math.min(activation.startupTimeoutMs(), 300_000));
    return new HBaseBackendIdentityProvider(providerId, scope, configuration, authenticator,
        verifiedAccess, 1, 1, timeout);
  }

  private static Configuration secureHBaseConfiguration(HBaseProfile profile,
      ClientProfile client) {
    Path siteFile = Path.of(client.hbaseSiteFile());
    if (!Files.isRegularFile(siteFile) || !Files.isReadable(siteFile)) {
      throw new IllegalArgumentException("configured secure HBase client configuration is unavailable");
    }
    Configuration configuration = HBaseConfiguration.create();
    configuration.addResource(new org.apache.hadoop.fs.Path(siteFile.toUri()));
    String quorum = HBaseQuorum.connectionString(profile);
    configuration.set("hbase.zookeeper.quorum", quorum);
    configuration.set("hbase.client.zookeeper.quorum", quorum);
    configuration.setInt("hbase.zookeeper.property.clientPort", profile.clientPort());
    configuration.setInt("hbase.client.zookeeper.property.clientPort", profile.clientPort());
    configuration.set("hbase.client.registry.impl",
        "org.apache.hadoop.hbase.client.ZKConnectionRegistry");
    configuration.setBoolean(HBaseCredentialAuthenticator.SIMPLE_FALLBACK, false);
    return configuration;
  }

  private static HBaseCredentialAuthenticator authenticator(ClientProfile client) {
    return switch (client.authenticatorId()) {
      case "kerberos-password" -> HBaseAuthenticators.kerberosPassword();
      case "delegation-token" -> HBaseAuthenticators.delegationToken(client.clusterId());
      case "digest-password" -> HBaseAuthenticators.digestPassword(client.clusterId());
      default -> throw new IllegalArgumentException("configured HBase authenticator is unavailable");
    };
  }

  private static AuthorizedBackendIdentity activateConfiguredIdentity(
      HBaseBackendIdentityProvider provider,
      M3AuthenticationConfiguration.IdentityCredential identity,
      String secretDirectory, AuthenticationScope scope,
      M3RuntimeConfiguration activation) throws Exception {
    return switch (identity.source()) {
      case CREDENTIAL_REFERENCE -> {
        try (var credentials = CredentialReferenceResolver.resolveIdentity(identity,
            secretDirectory, System.getenv())) {
          yield provider.authenticate(credentials, scope).toCompletableFuture()
              .get(activation.startupTimeoutMs(), TimeUnit.MILLISECONDS);
        }
      }
      case KERBEROS_KEYTAB -> {
        Path keytab = CredentialReferenceResolver.resolvePrivateSecretFile(
            identity.credentialReference(), secretDirectory);
        yield provider.activateTrusted(() -> KerberosUserContext.fromKeytab(
            identity.principal(), keytab)).toCompletableFuture()
            .get(activation.startupTimeoutMs(), TimeUnit.MILLISECONDS);
      }
      case KERBEROS_TICKET_CACHE -> {
        Path ticketCache = CredentialReferenceResolver.resolvePrivateSecretFile(
            identity.credentialReference(), secretDirectory);
        yield provider.activateTrusted(() -> KerberosUserContext.fromTicketCache(
            identity.principal(), ticketCache)).toCompletableFuture()
            .get(activation.startupTimeoutMs(), TimeUnit.MILLISECONDS);
      }
    };
  }

  private static List<HBaseAccessVerifiers.TableAccess> dataTableAccess(M3TableGroupPlan plan) {
    var tables = new java.util.LinkedHashSet<TableName>();
    tables.add(plan.system().table());
    tables.add(plan.primary().table());
    plan.current().ifPresent(current -> tables.add(current.table()));
    return tables.stream().map(table -> new HBaseAccessVerifiers.TableAccess(table,
        Set.of(Action.READ, Action.WRITE))).toList();
  }

  private static HBaseProfile hbaseProfile(RuntimeConfiguration configuration, String profileId) {
    return configuration.hbaseProfiles().stream()
        .filter(profile -> profile.id().equals(profileId)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException("configured HBase profile is missing"));
  }

  private static ClientProfile clientProfile(M3AuthenticationConfiguration authentication,
      String profileId) {
    return authentication.clientProfiles().stream()
        .filter(profile -> profile.id().equals(profileId)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException("configured secure client profile is missing"));
  }

  private static ByteKey datasetDigest(String planFingerprint, String trustDomain,
      long clusterId) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update("etcd-hbase/m5/auth-dataset/v1".getBytes(StandardCharsets.US_ASCII));
      updateDigestField(digest, trustDomain.getBytes(StandardCharsets.UTF_8));
      updateDigestField(digest, planFingerprint.getBytes(StandardCharsets.US_ASCII));
      digest.update(ByteBuffer.allocate(Long.BYTES).putLong(clusterId).array());
      return ByteKey.copyOf(digest.digest());
    } catch (NoSuchAlgorithmException impossible) {
      throw new AssertionError("the JDK must provide SHA-256", impossible);
    }
  }

  private static void updateDigestField(MessageDigest digest, byte[] value) {
    digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value.length).array());
    digest.update(value);
  }

  private static ScheduledThreadPoolExecutor leaseTimers() {
    var timers = new ScheduledThreadPoolExecutor(1,
        Thread.ofPlatform().daemon().name("m4-lease-ready-timeout-", 0).factory());
    timers.setRemoveOnCancelPolicy(true);
    timers.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    return timers;
  }

  private static boolean awaitFatalStartupCleanup(Error fatal, GroupResources resources,
      int timeoutMs) {
    for (Throwable suppressed : fatal.getSuppressed()) {
      if (suppressed instanceof M3TableGroupRuntime.StartupCleanup cleanup) {
        awaitCleanup(closeAfter(cleanup.cleanup(), resources), timeoutMs, fatal);
        return true;
      }
    }
    return false;
  }

  private static void closeRuntimeAfterDrain(M3TableGroupRuntime runtime,
      GroupResources resources, int timeoutMs, Throwable failure) {
    try { runtime.close(); }
    catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
    awaitCleanup(closeAfter(runtime.drained(), resources), timeoutMs, failure);
  }

  private static void closeResources(GroupResources resources, Throwable failure) {
    try { resources.close(); }
    catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
  }

  KvService service(String group) {
    ManagedGroup managed = groups.get(group);
    if (managed == null || closing.get()) throw new IllegalArgumentException("table group is not active");
    return managed.runtime().kvService();
  }

  LeaseService leaseService(String group) {
    ManagedGroup managed = groups.get(group);
    if (managed == null || closing.get()) throw new IllegalArgumentException("table group is not active");
    return managed.runtime().leaseService();
  }

  WatchService watchService(String group) {
    ManagedGroup managed = groups.get(group);
    if (managed == null || closing.get()) throw new IllegalArgumentException("table group is not active");
    return managed.runtime().watchService();
  }

  AuthService authService(String group) {
    ManagedGroup managed = groups.get(group);
    if (managed == null || closing.get()) throw new IllegalArgumentException("table group is not active");
    return managed.runtime().authService();
  }

  RequestMetrics metrics(String group) {
    ManagedGroup managed = groups.get(group);
    if (managed == null || closing.get()) throw new IllegalArgumentException("table group is not active");
    return managed.runtime().metrics();
  }

  M3TableGroupRuntime runtime(String group) {
    ManagedGroup managed = groups.get(group);
    if (managed == null || closing.get()) throw new IllegalArgumentException("table group is not active");
    return managed.runtime();
  }

  boolean isReady() {
    return !closing.get() && !groups.isEmpty() && groups.values().stream().allMatch(value -> value.runtime().isReady());
  }

  CompletableFuture<Void> drained() { return drained.copy(); }

  @Override public void close() { closeAfterRequests(CompletableFuture.completedFuture(null)); }

  void closeAfterRequests(CompletionStage<Void> requestsDrained) {
    Objects.requireNonNull(requestsDrained, "requestsDrained");
    if (!closing.compareAndSet(false, true)) return;
    List<CompletableFuture<Void>> cleanup = new ArrayList<>();
    for (ManagedGroup group : groups.values()) {
      try {
        group.runtime().close();
      } catch (RuntimeException | Error failure) {
        cleanup.add(CompletableFuture.failedFuture(failure));
      }
    }
    try { scans.close(); }
    catch (RuntimeException | Error failure) { cleanup.add(CompletableFuture.failedFuture(failure)); }
    var scansDrained = scans.termination().toCompletableFuture();
    cleanup.add(scansDrained);
    var runtimesDrained = CompletableFuture.allOf(groups.values().stream()
        .map(group -> group.runtime().drained())
        .toArray(CompletableFuture[]::new));
    if (ownerForwarding != null) {
      cleanup.add(closeWhenDrained(runtimesDrained, scansDrained, requestsDrained, ownerForwarding::close));
    }
    if (leaseTimers != null) {
      cleanup.add(closeWhenDrained(runtimesDrained, scansDrained, requestsDrained,
          () -> closeLeaseTimers(leaseTimers)));
    }
    for (ManagedGroup group : groups.values()) {
      cleanup.add(closeWhenDrained(group.runtime().drained(), scansDrained, requestsDrained,
          group.resources()::close));
    }
    CompletableFuture.allOf(cleanup.toArray(CompletableFuture[]::new)).whenComplete((ignored, failure) -> {
      if (failure == null) drained.complete(null);
      else drained.completeExceptionally(failure);
    });
  }

  static CompletableFuture<Void> closeWhenDrained(CompletionStage<Void> runtime,
      CompletionStage<Void> scans, CompletionStage<Void> requests, Runnable close) {
    return CompletableFuture.allOf(runtime.toCompletableFuture(), scans.toCompletableFuture(),
        requests.toCompletableFuture()).thenRunAsync(close, CLEANUP);
  }

  private static void closeLeaseTimers(ScheduledThreadPoolExecutor timers) {
    timers.shutdown();
    try {
      if (!timers.awaitTermination(30, TimeUnit.SECONDS)) {
        throw new IllegalStateException("M4 lease response timers did not terminate");
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("M4 lease response timer shutdown interrupted", interrupted);
    }
  }

  private static CompletableFuture<Void> closeAfter(CompletionStage<Void> completion,
      GroupResources resources) {
    return completion.toCompletableFuture().thenRunAsync(resources::close, CLEANUP);
  }

  private static void awaitCleanup(CompletableFuture<Void> cleanup, int timeoutMs, Throwable primary) {
    try {
      cleanup.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception failure) {
      if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
      if (failure != primary) primary.addSuppressed(failure);
    }
  }

  private record ManagedGroup(M3TableGroupRuntime runtime, GroupResources resources) {}

  private record IdentityActivation(HBaseConnectionFactory connections,
      AuthorizedBackendIdentity identity,
      LongFunction<AuthSessionService> sessionFactory) {}

  private static final class GroupResources implements AutoCloseable {
    private HBaseBackendIdentityProvider backendProvider;
    private HBaseBackendIdentityProvider authControlProvider;
    private ProviderSessionIdentityManager sessionIdentities;
    private HBaseConnectionRouter router;
    private CredentialKeyProvider credentialKeys;
    private boolean closed;

    @Override
    public synchronized void close() {
      if (closed) return;
      Throwable first = null;
      first = closeComponent(first, sessionIdentities == null ? null : sessionIdentities::close);
      first = closeComponent(first, router == null ? null : router::close);
      first = closeComponent(first, authControlProvider == null ? null : authControlProvider::close);
      first = closeComponent(first, credentialKeys == null ? null : credentialKeys::close);
      first = closeComponent(first, backendProvider == null ? null : backendProvider::close);
      if (first == null) closed = true;
      if (first instanceof Error fatal) throw fatal;
      if (first instanceof RuntimeException failure) throw failure;
      if (first != null) throw new IllegalStateException("M5 runtime identity cleanup failed", first);
    }

    private static Throwable closeComponent(Throwable first, Runnable close) {
      if (close == null) return first;
      try {
        close.run();
      } catch (RuntimeException | Error failure) {
        if (first == null) return failure;
        if (first != failure) first.addSuppressed(failure);
      }
      return first;
    }
  }
}
