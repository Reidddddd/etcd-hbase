package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.core.auth.AuthToken;
import io.etcdhbase.core.auth.AuthorizationRevocation;
import io.etcdhbase.core.auth.SubjectAuthority;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.identity.KerberosUserContext;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.hbase.storage.AuthControlBinding;
import io.etcdhbase.hbase.storage.HBaseAuthControlInitializer;
import io.etcdhbase.hbase.storage.HBaseAuthSessionStore;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.security.token.ClientTokenUtil;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Proves persisted sessions activate real scoped HBase identities in independent JVMs. */
@Timeout(600)
class M5AuthProcessIT {
  private static final AuthenticationScope SCOPE = M5AuthProcessControl.SCOPE;
  private static final String KEY_ID = "process-test-key";
  private static final String TRUST_DOMAIN = M5AuthProcessControl.TRUST_DOMAIN;
  private static final byte[] FAMILY = {'d'};
  private static final Set<PosixFilePermission> PRIVATE_FILE =
      EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);

  @TempDir Path temporary;

  @Test
  void sessionsActivateAllRegisteredHBaseMechanismsAcrossProcessesAndFullRestart()
      throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.startSecure()) {
      var configuration = cluster.clientConfiguration();
      String realm = configuration.get("etcdhbase.test.security.realm");
      String adminPrincipal = "admin@" + realm;
      Path adminKeytab = Path.of(configuration.get("etcdhbase.test.security.admin.keytab"));
      TableName dataTable = TableName.valueOf("m5proc", "kv_" + shortId());
      TableName authTable = TableName.valueOf("m5proc", "auth_" + shortId());
      TableName systemTable = TableName.valueOf("m5proc", "system_" + shortId());
      configuration.set(M5AuthProcessControl.SYSTEM_TABLE_PROPERTY, systemTable.getNameAsString());
      Path configurationFile = temporary.resolve("hbase-site.xml");
      Path keyFile = temporary.resolve("credential.key");
      byte[] keyMaterial = new byte[32];
      new SecureRandom().nextBytes(keyMaterial);
      Files.write(keyFile, keyMaterial);
      java.util.Arrays.fill(keyMaterial, (byte) 0);
      Files.setPosixFilePermissions(keyFile, PRIVATE_FILE);
      String dataset = datasetDigest(configuration.get("hbase.zookeeper.quorum"),
          configuration.getInt("hbase.zookeeper.property.clientPort", -1));

      String clusterId = createTablesAndGrantUsers(configuration, adminPrincipal,
          adminKeytab, dataTable, authTable, systemTable, ByteKey.copyOf(java.util.HexFormat.of().parseHex(dataset)));
      configuration.unset("etcdhbase.test.security.admin.keytab");
      try (var output = Files.newOutputStream(configurationFile)) {
        configuration.writeXml(output);
      }
      String servicePrincipal = "keytab-user@" + realm;
      Path logDirectory = Files.createDirectories(temporary.resolve("process-logs"));
      String scopeId = SCOPE.tableGroupId();
      String profileId = SCOPE.connectionProfileId();
      String authControl = authTable.getNameAsString();
      ProcessNode first = startNode(logDirectory.resolve("a.log"), configurationFile,
          scopeId, profileId, dataTable.getNameAsString(), authControl, keyFile,
          dataset, servicePrincipal, "kerberos-password", clusterId);
      ProcessNode second = null;
      String aliceToken;
      String readerToken;
      try {
        assertNotEquals(ProcessHandle.current().pid(), first.pid());
        aliceToken = first.authenticate("alice@" + realm, fixturePassword());
        readerToken = first.authenticate("reader@" + realm, fixturePassword());
        var registryAfterLogin = verifyDeploymentProgress(configuration, adminPrincipal, adminKeytab,
            authTable, systemTable, readerToken);

        second = startNode(logDirectory.resolve("b.log"), configurationFile,
            scopeId, profileId, dataTable.getNameAsString(), authControl, keyFile,
            dataset, servicePrincipal, "kerberos-password", clusterId);
        assertNotEquals(first.pid(), second.pid());
        assertIdentity(second.resolve(aliceToken), "alice", "KERBEROS");
        assertIdentity(second.resolve(readerToken), "reader", "KERBEROS");

        // Keep the original activation alive in both JVMs while the backend ACL changes.
        first.hold(aliceToken);
        second.hold(aliceToken);
        setUserAccess(configuration, adminPrincipal, adminKeytab, dataTable, "alice", false);
        assertEquals("PERMISSION_DENIED", first.resolveFailure(aliceToken));
        assertEquals("PERMISSION_DENIED", second.resolveFailure(aliceToken));
        first.assertHeldConnectionOpen();
        second.assertHeldConnectionOpen();
        assertIdentity(second.resolve(readerToken), "reader", "KERBEROS");
        first.releaseHeld();
        second.releaseHeld();
        setUserAccess(configuration, adminPrincipal, adminKeytab, dataTable, "alice", true);

        try (ProcessNode wrongScope = startNode(logDirectory.resolve("wrong-scope.log"),
            configurationFile, "m5-other-process", profileId, dataTable.getNameAsString(),
            authControl, keyFile, dataset, servicePrincipal, "kerberos-password", clusterId)) {
          assertEquals("INVALID_TOKEN", wrongScope.resolveFailure(aliceToken));
          assertEquals("DEPENDENCY_UNAVAILABLE", wrongScope.authenticateFailure("alice@" + realm, fixturePassword()));
          assertEquals(registryAfterLogin, verifyDeploymentProgress(configuration, adminPrincipal,
              adminKeytab, authTable, systemTable, readerToken));
        }
        try (ProcessNode wrongProfile = startNode(logDirectory.resolve("wrong-profile.log"),
            configurationFile, scopeId, "other-profile", dataTable.getNameAsString(),
            authControl, keyFile, dataset, servicePrincipal, "kerberos-password", clusterId)) {
          assertEquals("INVALID_TOKEN", wrongProfile.resolveFailure(aliceToken));
          assertEquals("DEPENDENCY_UNAVAILABLE", wrongProfile.authenticateFailure("alice@" + realm, fixturePassword()));
          assertEquals(registryAfterLogin, verifyDeploymentProgress(configuration, adminPrincipal,
              adminKeytab, authTable, systemTable, readerToken));
        }
      } finally {
        if (second != null) second.close();
        first.close();
      }

      // No application JVM remains. A fresh process must reload auth-control and reactivate both
      // users from the persisted envelopes without relying on A/B's provider handles.
      try (ProcessNode restarted = startNode(logDirectory.resolve("restarted.log"),
          configurationFile, scopeId, profileId, dataTable.getNameAsString(), authControl,
          keyFile, dataset, servicePrincipal, "kerberos-password", clusterId)) {
        assertIdentity(restarted.resolve(aliceToken), "alice", "KERBEROS");
        assertIdentity(restarted.resolve(readerToken), "reader", "KERBEROS");
      }

      String delegationToken = acquireDelegationToken(configuration, realm, clusterId);
      verifySessionRestartForAdapter(logDirectory, configurationFile, scopeId, profileId,
          dataTable.getNameAsString(), authControl, keyFile, dataset, servicePrincipal,
          clusterId, "alice@" + realm, "delegation-token", delegationToken, "TOKEN");
      verifySessionRestartForAdapter(logDirectory, configurationFile, scopeId, profileId,
          dataTable.getNameAsString(), authControl, keyFile, dataset, servicePrincipal,
          clusterId, "alice@" + realm, "password-sasl", fixturePassword(), "TOKEN");
    }
  }

  private void setUserAccess(org.apache.hadoop.conf.Configuration configuration,
      String adminPrincipal, Path adminKeytab, TableName table, String userName,
      boolean granted) throws Exception {
    try (var user = KerberosUserContext.fromKeytab(adminPrincipal, adminKeytab);
        var connection = ConnectionFactory.createAsyncConnection(configuration, user.user())
            .get(30, TimeUnit.SECONDS)) {
      var admin = connection.getAdmin();
      var permission = Permission.newBuilder(table)
          .withActions(Action.READ, Action.WRITE, Action.EXEC).build();
      var entry = new UserPermission(userName, permission);
      if (granted) admin.grant(entry, false).get(30, TimeUnit.SECONDS);
      else admin.revoke(entry).get(30, TimeUnit.SECONDS);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (System.nanoTime() < deadline) {
        if (admin.hasUserPermissions(userName, List.of(permission))
            .get(30, TimeUnit.SECONDS).equals(List.of(granted))) return;
        Thread.sleep(50);
      }
      throw new IllegalStateException("temporary HBase ACL change did not become visible");
    }
  }

  private String createTablesAndGrantUsers(org.apache.hadoop.conf.Configuration configuration,
      String adminPrincipal, Path adminKeytab,
      TableName dataTable, TableName authTable, TableName systemTable, ByteKey dataset) throws Exception {
    try (var user = KerberosUserContext.fromKeytab(adminPrincipal, adminKeytab);
        var connection = ConnectionFactory.createAsyncConnection(configuration, user.user())
            .get(30, TimeUnit.SECONDS)) {
      var admin = connection.getAdmin();
      String clusterId = admin.getClusterMetrics().get(30, TimeUnit.SECONDS).getClusterId();
      admin.createNamespace(NamespaceDescriptor.create("m5proc").build())
          .get(30, TimeUnit.SECONDS);
      admin.createTable(TableDescriptorBuilder.newBuilder(dataTable)
          .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
              .setMaxVersions(1).build())
          .setDurability(Durability.SYNC_WAL).build()).get(30, TimeUnit.SECONDS);
      admin.createTable(M5AuthProcessControl.descriptor(systemTable)).get(30, TimeUnit.SECONDS);
      for (String userName : List.of("alice", "reader")) {
        grantTableAccess(admin, userName, dataTable, Action.READ, Action.WRITE, Action.EXEC);
      }
      initializeControl(configuration, adminPrincipal, adminKeytab, authTable, systemTable, dataset);
      // HBase requires table-scoped CREATE for descriptor reads; children perform no DDL.
      for (TableName table : List.of(authTable, systemTable)) {
        grantTableAccess(admin, "keytab-user", table, Action.READ, Action.WRITE, Action.EXEC, Action.CREATE);
      }
      return clusterId;
    }
  }

  private void grantTableAccess(org.apache.hadoop.hbase.client.AsyncAdmin admin, String userName,
      TableName table, Action... actions) throws Exception {
    var permission = Permission.newBuilder(table).withActions(actions).build();
    admin.grant(new UserPermission(userName, permission), false).get(30, TimeUnit.SECONDS);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < deadline && !admin.hasUserPermissions(userName, List.of(permission))
        .get(30, TimeUnit.SECONDS).equals(List.of(true))) Thread.sleep(50);
    assertEquals(List.of(true), admin.hasUserPermissions(userName, List.of(permission)).get(30, TimeUnit.SECONDS));
  }

  private void initializeControl(org.apache.hadoop.conf.Configuration configuration,
      String adminPrincipal, Path adminKeytab, TableName authTable, TableName systemTable,
      ByteKey dataset) throws Exception {
    var scope = new AuthenticationScope("auth-control", SCOPE.connectionProfileId());
    try (var provider = new HBaseBackendIdentityProvider("m5-process-bootstrap", scope,
        configuration, HBaseAuthenticators.kerberosPassword(),
        (backend, ignored) -> backend.getAdmin().listNamespaces().thenApply(names -> null),
        2, 1, Duration.ofSeconds(30));
        var request = RequestScope.withTimeout(Duration.ofSeconds(60))) {
      var operator = provider.activateTrusted(() ->
          KerberosUserContext.fromKeytab(adminPrincipal, adminKeytab))
          .toCompletableFuture().get(30, TimeUnit.SECONDS);
      var binding = new AuthControlBinding(authTable, TRUST_DOMAIN,
          new HashSaltStrategy(new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2)));
      HBaseAuthControlInitializer.initializeFresh(provider, operator, binding, request)
          .get(30, TimeUnit.SECONDS);
      var sessions = HBaseAuthSessionStore.open(provider, operator, binding, request).get(30, TimeUnit.SECONDS);
      M5AuthProcessControl.initialize(provider, operator, sessions, systemTable, dataset, request);
    }
  }

  private AuthDeploymentRegistry verifyDeploymentProgress(org.apache.hadoop.conf.Configuration configuration,
      String adminPrincipal, Path adminKeytab, TableName authTable, TableName systemTable, String token)
      throws Exception {
    var scope = new AuthenticationScope("auth-control", SCOPE.connectionProfileId());
    try (var provider = new HBaseBackendIdentityProvider("m5-progress-reader", scope,
        configuration, HBaseAuthenticators.kerberosPassword(),
        (backend, ignored) -> backend.getAdmin().listNamespaces().thenApply(names -> null),
        2, 1, Duration.ofSeconds(30));
        var request = RequestScope.withTimeout(Duration.ofSeconds(60));
        var parsed = AuthToken.parse(token)) {
      var operator = provider.activateTrusted(() -> KerberosUserContext.fromKeytab(adminPrincipal, adminKeytab))
          .toCompletableFuture().get(30, TimeUnit.SECONDS);
      var binding = new AuthControlBinding(authTable, TRUST_DOMAIN,
          new HashSaltStrategy(new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2)));
      var sessions = HBaseAuthSessionStore.open(provider, operator, binding, request).get(30, TimeUnit.SECONDS);
      var control = HBaseCommitControlStore.open(provider, operator,
          M5AuthProcessControl.binding(systemTable), request).get(30, TimeUnit.SECONDS);
      var registry = sessions.readRegistry(request).toCompletableFuture().get(30, TimeUnit.SECONDS).orElseThrow();
      var operation = registry.revocation();
      assertEquals(AuthorizationRevocation.State.COMPLETED, operation.state());
      assertEquals(1, operation.configurationGeneration());
      assertEquals(2, operation.targets().getFirst().barrierGeneration());
      assertTrue(operation.targets().getFirst().installed());
      assertTrue(operation.targets().getFirst().released());
      assertEquals(2, control.read(request).get(30, TimeUnit.SECONDS).orElseThrow()
          .state().authorizationBarrierGeneration());
      var session = sessions.readSession(parsed.digest(), request).toCompletableFuture()
          .get(30, TimeUnit.SECONDS).orElseThrow();
      var authority = sessions.readAuthority(session.subjectDigest(), request).toCompletableFuture()
          .get(30, TimeUnit.SECONDS).orElseThrow();
      assertEquals(operation.targets().getFirst().identity().authorityDigest(), session.subjectDigest());
      assertEquals(SubjectAuthority.State.ACTIVE, authority.state());
      assertEquals(operation.operationId(), authority.revocationOperationId());
      assertEquals(operation.operationSequence(), authority.revocationOperationSequence());
      assertEquals(registry.configurationGeneration(), authority.revocationConfigurationGeneration());
      assertEquals(2, session.authorizationGeneration());
      return registry;
    }
  }

  private ProcessNode startNode(Path log, Path configuration, String tableGroup,
      String profile, String dataTable, String authTable, Path keyFile,
      String datasetDigest, String servicePrincipal, String userAdapter, String clusterId)
      throws Exception {
    String realmConfiguration = System.getProperty("java.security.krb5.conf");
    if (realmConfiguration == null || realmConfiguration.isBlank()) {
      throw new IllegalStateException("secure HBase fixture did not publish its Kerberos configuration");
    }
    var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-Djava.security.krb5.conf=" + realmConfiguration,
        "-cp", System.getProperty("java.class.path"), M5AuthProcessNode.class.getName(),
        configuration.toAbsolutePath().toString(), tableGroup, profile, dataTable, authTable,
        KEY_ID, keyFile.toAbsolutePath().toString(), datasetDigest, servicePrincipal,
        userAdapter, clusterId)
        .redirectError(log.toFile()).start();
    var node = new ProcessNode(process, log);
    if (process.pid() == ProcessHandle.current().pid()) {
      node.close();
      throw new AssertionError("auth fixture did not start an independent JVM");
    }
    assertEquals("READY", node.read(), "auth child failed to initialize; log=" + log);
    return node;
  }

  private void verifySessionRestartForAdapter(Path logDirectory, Path configurationFile,
      String tableGroup, String profile, String dataTable, String authTable, Path keyFile,
      String dataset, String servicePrincipal, String clusterId, String principal, String adapter,
      String secret, String expectedAuthMethod) throws Exception {
    String token;
    try (ProcessNode first = startNode(logDirectory.resolve(adapter + "-a.log"),
        configurationFile, tableGroup, profile, dataTable, authTable, keyFile, dataset,
        servicePrincipal, adapter, clusterId)) {
      token = first.authenticate(principal, secret);
      try (ProcessNode second = startNode(logDirectory.resolve(adapter + "-b.log"),
          configurationFile, tableGroup, profile, dataTable, authTable, keyFile, dataset,
          servicePrincipal, adapter, clusterId)) {
        assertIdentity(second.resolve(token), "alice", expectedAuthMethod);
      }
    }
    try (ProcessNode restarted = startNode(logDirectory.resolve(adapter + "-restarted.log"),
        configurationFile, tableGroup, profile, dataTable, authTable, keyFile, dataset,
        servicePrincipal, adapter, clusterId)) {
      assertIdentity(restarted.resolve(token), "alice", expectedAuthMethod);
    }
  }

  private String acquireDelegationToken(org.apache.hadoop.conf.Configuration configuration,
      String realm, String clusterId) throws Exception {
    try (var credentials = new BackendCredentials("alice@" + realm,
            fixturePassword().toCharArray());
        var user = KerberosUserContext.fromPassword(credentials);
        var connection = ConnectionFactory.createConnection(configuration, user.user())) {
      var token = ClientTokenUtil.obtainToken(connection);
      assertEquals(clusterId, token.getService().toString());
      return token.encodeToUrlString();
    }
  }

  private static void assertIdentity(Observation observation, String expectedUser,
      String expectedAuthMethod) {
    assertEquals(expectedUser, observation.user());
    assertEquals(expectedAuthMethod, observation.authMethod());
    assertTrue(observation.dataRoundTrips());
    assertTrue(observation.authControlDenied());
  }

  private static String fixturePassword() {
    return io.etcdhbase.hbase.probe.auth.PasswordDigestSaslServerFixture.FIXTURE_PASSWORD;
  }

  private static String shortId() {
    return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
  }

  private static String datasetDigest(String quorum, int clientPort) throws Exception {
    byte[] source = (quorum + ":" + clientPort).getBytes(StandardCharsets.UTF_8);
    return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(source));
  }

  private static final class ProcessNode implements AutoCloseable {
    private final Process process;
    private final Path log;
    private final DataInputStream input;
    private final DataOutputStream output;

    private ProcessNode(Process process, Path log) {
      this.process = process;
      this.log = log;
      input = new DataInputStream(new BufferedInputStream(process.getInputStream()));
      output = new DataOutputStream(new BufferedOutputStream(process.getOutputStream()));
    }

    private long pid() { return process.pid(); }

    private String authenticate(String principal, String password) throws Exception {
      output.writeUTF("AUTH");
      output.writeUTF(principal);
      output.writeUTF(password);
      output.flush();
      String[] response = read().split("\t", -1);
      assertEquals("TOKEN", response[0], "authentication process result; log=" + log);
      assertEquals(2, response.length);
      assertEquals(43, response[1].length(), "opaque token encoding length");
      return response[1];
    }

    private Observation resolve(String token) throws Exception {
      output.writeUTF("RESOLVE");
      output.writeUTF(token);
      output.flush();
      String[] response = read().split("\t", -1);
      assertEquals("IDENTITY", response[0], "identity process result; log=" + log);
      assertEquals(5, response.length);
      return new Observation(response[1], response[2], Boolean.parseBoolean(response[3]),
          Boolean.parseBoolean(response[4]));
    }

    private String authenticateFailure(String principal, String password) throws Exception {
      output.writeUTF("AUTH");
      output.writeUTF(principal);
      output.writeUTF(password);
      output.flush();
      String[] response = read().split("\t", -1);
      assertEquals("ERROR", response[0], "mismatched binding unexpectedly issued a token");
      assertEquals(2, response.length);
      return response[1];
    }

    private String resolveFailure(String token) throws Exception {
      output.writeUTF("RESOLVE");
      output.writeUTF(token);
      output.flush();
      String[] response = read().split("\t", -1);
      assertEquals("ERROR", response[0], "mismatched scope unexpectedly resolved the token");
      assertEquals(2, response.length);
      return response[1];
    }

    private void hold(String token) throws Exception {
      output.writeUTF("HOLD");
      output.writeUTF(token);
      output.flush();
      assertEquals("HELD", read(), "identity was not held; log=" + log);
    }

    private void releaseHeld() throws Exception {
      output.writeUTF("RELEASE");
      output.flush();
      assertEquals("RELEASED", read(), "identity was not released; log=" + log);
    }

    private void assertHeldConnectionOpen() throws Exception {
      output.writeUTF("HELD_OPEN");
      output.flush();
      assertEquals("OPEN", read(), "held connection closed before drain; log=" + log);
    }

    private String read() throws Exception {
      var response = new java.util.concurrent.FutureTask<>(input::readUTF);
      Thread.ofVirtual().start(response);
      try {
        return response.get(90, TimeUnit.SECONDS);
      } catch (Exception failure) {
        throw new AssertionError("auth child did not return a bounded response; log=" + log, failure);
      }
    }

    @Override
    public void close() throws Exception {
      if (process.isAlive()) {
        output.writeUTF("STOP");
        output.flush();
        assertEquals("STOPPED", read(), "auth child failed graceful stop; log=" + log);
        assertTrue(process.waitFor(60, TimeUnit.SECONDS), "auth child did not exit; log=" + log);
        assertEquals(0, process.exitValue(), "auth child failed during close; log=" + log);
      }
    }
  }

  private record Observation(String user, String authMethod,
      boolean dataRoundTrips, boolean authControlDenied) {}
}
