package io.etcdhbase.hbase.probe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.hbase.api.HBaseCredentialAuthenticator;
import io.etcdhbase.hbase.api.HBaseUserContext;
import io.etcdhbase.hbase.identity.HBaseAccessVerifiers;
import io.etcdhbase.hbase.identity.HBaseAccessVerifiers.TableAccess;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.identity.KerberosUserContext;
import io.etcdhbase.hbase.probe.auth.PasswordDigestSaslClientFixture;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.identity.BackendIdentityException.Reason;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.AuthenticationService;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.WhoAmIRequest;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.WhoAmIResponse;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.ClientTokenUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

/** Real HBase RPC authentication and ACL checks; no target environment credentials are used. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(300)
class SecureHBaseAuthenticationIT {
  private static final AuthenticationScope SCOPE = new AuthenticationScope("secure-test", "local");
  private TemporaryHBaseClusterProcess cluster;
  private Configuration configuration;
  private KerberosUserContext adminUser;
  private AsyncConnection adminConnection;
  private TableName table;
  private String namespace;
  private String realm;
  private String clusterId;

  @BeforeAll
  void startSecureClusterAndGrantExplicitTablePermissions() throws Exception {
    cluster = TemporaryHBaseClusterProcess.startSecure();
    configuration = cluster.clientConfiguration();
    configuration.setInt("hbase.client.retries.number", 1);
    configuration.setInt("hbase.rpc.timeout", 5000);
    configuration.setInt("hbase.client.operation.timeout", 15_000);
    configuration.setInt("hbase.client.meta.operation.timeout", 15_000);
    assertEquals("kerberos", configuration.get("hbase.security.authentication"));
    assertTrue(configuration.getBoolean("hbase.security.authorization", false));
    assertFalse(configuration.getBoolean(HBaseCredentialAuthenticator.SIMPLE_FALLBACK, true));
    realm = configuration.get(TemporaryKerberosSecurity.PREFIX + "realm");
    adminUser =
        KerberosUserContext.fromKeytab(
            "admin@" + realm,
            Path.of(configuration.get(TemporaryKerberosSecurity.PREFIX + "admin.keytab")));
    adminConnection =
        await(ConnectionFactory.createAsyncConnection(configuration, adminUser.user()));
    clusterId = await(adminConnection.getAdmin().getClusterMetrics()).getClusterId();
    namespace = "m0_secure_" + UUID.randomUUID().toString().replace("-", "");
    table = TableName.valueOf(namespace, "probe");
    await(
        adminConnection.getAdmin().createNamespace(NamespaceDescriptor.create(namespace).build()));
    await(
        adminConnection
            .getAdmin()
            .createTable(
                TableDescriptorBuilder.newBuilder(table)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("d"))
                    .setDurability(Durability.SYNC_WAL)
                    .build()));
    for (String user : List.of("alice", "keytab-user")) {
      grantAndAwait(user, Set.of(Action.READ, Action.WRITE, Action.EXEC));
    }
    grantAndAwait("reader", Set.of(Action.READ, Action.EXEC));
  }

  @AfterAll
  void deleteOwnedResourcesAndCloseAllSecurityContexts() throws Exception {
    Exception failure = null;
    if (adminConnection != null && table != null) {
      try {
        if (await(adminConnection.getAdmin().tableExists(table))) {
          await(adminConnection.getAdmin().disableTable(table));
          await(adminConnection.getAdmin().deleteTable(table));
        }
        await(adminConnection.getAdmin().deleteNamespace(namespace));
      } catch (Exception cleanupFailure) {
        failure = cleanupFailure;
      }
    }
    for (AutoCloseable resource : new AutoCloseable[] {adminConnection, adminUser, cluster}) {
      if (resource != null) {
        try {
          resource.close();
        } catch (Exception cleanupFailure) {
          if (failure == null) failure = cleanupFailure;
          else failure.addSuppressed(cleanupFailure);
        }
      }
    }
    if (failure != null) throw failure;
  }

  @Test
  void kerberosPasswordPerformsRealOperationsAndRejectsWrongCredentialsAndInsufficientPermissions()
      throws Exception {
    try (var provider = provider(HBaseAuthenticators.kerberosPassword())) {
      AuthorizedBackendIdentity identity =
          authenticate(
              provider, "alice@" + realm, TemporaryKerberosSecurity.PASSWORD.toCharArray());
      verifyOperations(provider, identity, "kerberos-password", "alice", "KERBEROS");
    }
    assertAuthenticationFailure(
        HBaseAuthenticators.kerberosPassword(),
        "alice@" + realm,
        "wrong-fixture-password".toCharArray(),
        Reason.INVALID_CREDENTIALS);
    assertAuthenticationFailure(
        HBaseAuthenticators.kerberosPassword(),
        "reader@" + realm,
        TemporaryKerberosSecurity.PASSWORD.toCharArray(),
        Reason.INSUFFICIENT_PERMISSION);
    assertAuthenticationFailure(
        HBaseAuthenticators.kerberosPassword(),
        "denied@" + realm,
        TemporaryKerberosSecurity.PASSWORD.toCharArray(),
        Reason.INSUFFICIENT_PERMISSION);
    System.out.println("secure-hbase negative=kerberos-password-and-permissions status=passed");
  }

  @Test
  void revokedPermissionDeniesLiveBackendReadsAndFreshAuthentication() throws Exception {
    String user = "revocation-user";
    String principal = user + "@" + realm;
    var actions = Set.of(Action.READ, Action.WRITE, Action.EXEC);
    var permission = Permission.newBuilder(table).withActions(actions.toArray(Action[]::new)).build();
    grantAndAwait(user, actions);
    var original = provider(HBaseAuthenticators.kerberosPassword());
    AuthorizedBackendIdentity identity;
    try {
      identity = authenticate(original, principal, TemporaryKerberosSecurity.PASSWORD.toCharArray());
      var held = original.borrow(identity);
      var connection = await(original.connectionFor(identity).toCompletableFuture());
      try {
        await(original.verifyCurrentAccess(identity).toCompletableFuture());
        var get = new Get(new byte[] {'r'}).addFamily(new byte[] {'d'});
        await(connection.getTable(table).get(get));
        await(adminConnection.getAdmin().revoke(new UserPermission(user, permission)));
        awaitBackendReadDenial(connection, get);
        var recheckFailure = assertThrows(ExecutionException.class,
            () -> original.verifyCurrentAccess(identity).toCompletableFuture().get(30, TimeUnit.SECONDS));
        assertEquals(Reason.INSUFFICIENT_PERMISSION,
            assertInstanceOf(BackendIdentityException.class, recheckFailure.getCause()).reason());
        assertThrows(BackendIdentityException.class, () -> original.borrow(identity));
        assertFalse(connection.isClosed());
      } finally {
        held.release().toCompletableFuture().get(30, TimeUnit.SECONDS);
      }
      original.retire(identity).toCompletableFuture().get(30, TimeUnit.SECONDS);
      assertTrue(connection.isClosed());
      assertAuthenticationFailure(HBaseAuthenticators.kerberosPassword(), principal,
          TemporaryKerberosSecurity.PASSWORD.toCharArray(), Reason.INSUFFICIENT_PERMISSION);
    } finally {
      original.close();
    }
    var closed = assertThrows(ExecutionException.class,
        () -> original.connectionFor(identity).toCompletableFuture().get(30, TimeUnit.SECONDS));
    assertEquals(Reason.BACKEND_UNAVAILABLE,
        assertInstanceOf(BackendIdentityException.class, closed.getCause()).reason());
  }

  private void awaitBackendReadDenial(AsyncConnection connection, Get get) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < deadline) {
      try {
        await(connection.getTable(table).get(get));
      } catch (ExecutionException failure) {
        if (isAccessDenied(failure)) return;
        throw failure;
      }
      Thread.sleep(50);
    }
    throw new IllegalStateException("revoked HBase permission remained effective");
  }

  private boolean isAccessDenied(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof org.apache.hadoop.hbase.security.AccessDeniedException) return true;
    }
    return false;
  }

  @Test
  void keytabAndTicketCacheIdentitiesPerformRealOperations() throws Exception {
    verifyTrustedSource(
        "kerberos-keytab",
        "keytab-user",
        () ->
            KerberosUserContext.fromKeytab(
                "keytab-user@" + realm,
                Path.of(configuration.get(TemporaryKerberosSecurity.PREFIX + "user.keytab"))));
    verifyTrustedSource(
        "kerberos-ticket-cache",
        "alice",
        () ->
            KerberosUserContext.fromTicketCache(
                "alice@" + realm,
                Path.of(configuration.get(TemporaryKerberosSecurity.PREFIX + "ticket.cache"))));
  }

  @Test
  void serverIssuedDelegationTokenPerformsRealOperationsAndTamperingIsRejected() throws Exception {
    Token<AuthenticationTokenIdentifier> token;
    try (var credentials =
            new BackendCredentials(
                "alice@" + realm, TemporaryKerberosSecurity.PASSWORD.toCharArray());
        var user = KerberosUserContext.fromPassword(credentials);
        var connection = ConnectionFactory.createConnection(configuration, user.user())) {
      token = ClientTokenUtil.obtainToken(connection);
    }
    assertEquals(clusterId, token.getService().toString());
    HBaseCredentialAuthenticator authenticator =
        (credentials, scope) -> {
          var context =
              HBaseAuthenticators.delegationToken(clusterId).authenticate(credentials, scope);
          assertFalse(
              context.user().getUGI().hasKerberosCredentials(),
              "token authentication must not silently use a Kerberos TGT");
          return context;
        };
    try (var provider = provider(authenticator)) {
      AuthorizedBackendIdentity identity =
          authenticate(provider, "alice@" + realm, token.encodeToUrlString().toCharArray());
      verifyOperations(provider, identity, "delegation-token", "alice", "TOKEN");
    }
    token.getPassword()[0] ^= 1;
    assertAuthenticationFailure(
        authenticator,
        "alice@" + realm,
        token.encodeToUrlString().toCharArray(),
        Reason.INVALID_CREDENTIALS);
    Arrays.fill(token.getPassword(), (byte) 0);
    System.out.println("secure-hbase negative=tampered-delegation-token status=passed");
  }

  @Test
  void secureServerRejectsASimpleClientWithoutFallback() throws Exception {
    var simple = new Configuration(configuration);
    simple.set("hbase.security.authentication", "simple");
    simple.set("hadoop.security.authentication", "simple");
    simple.setBoolean("hbase.security.authorization", false);
    var backendRejection = new AtomicReference<Throwable>();
    try (var provider =
        new HBaseBackendIdentityProvider(
            "simple-attempt",
            SCOPE,
            simple,
            HBaseAuthenticators.simpleDevelopment(),
            (connection, scope) ->
                connection
                    .getAdmin()
                    .listNamespaces()
                    .whenComplete((ignored, failure) -> backendRejection.set(failure))
                    .thenApply(ignored -> null),
            1,
            1,
            Duration.ofSeconds(20))) {
      var failure =
          assertThrows(
              ExecutionException.class, () -> authenticate(provider, "alice", new char[0]));
      Throwable backend = backendRejection.get();
      assertInstanceOf(org.apache.hadoop.hbase.client.RetriesExhaustedException.class, backend);
      while (backend.getCause() != null) {
        backend = backend.getCause();
      }
      assertInstanceOf(ConnectionClosedException.class, backend);
      // This client loses the server's preamble AccessDeniedException and exposes only a closed
      // connection. Preserve that transport classification rather than guessing from message text.
      assertEquals(
          Reason.BACKEND_UNAVAILABLE,
          assertInstanceOf(BackendIdentityException.class, failure.getCause()).reason());
    }
    System.out.println("secure-hbase negative=simple-on-secure-server status=passed");
  }

  @Test
  void customPasswordDigestExtensionPerformsRealOperationsAndRejectsBadCredentials()
      throws Exception {
    HBaseCredentialAuthenticator authenticator =
        (credentials, scope) -> {
          var context =
              HBaseAuthenticators.digestPassword(clusterId).authenticate(credentials, scope);
          assertFalse(context.user().getUGI().hasKerberosCredentials());
          context
              .user()
              .getTokens()
              .forEach(
                  token -> token.setKind(new Text(PasswordDigestSaslClientFixture.TOKEN_KIND)));
          return context;
        };
    try (var provider = provider(authenticator)) {
      var identity =
          authenticate(
              provider, "alice@" + realm, TemporaryKerberosSecurity.PASSWORD.toCharArray());
      verifyOperations(provider, identity, "custom-password-digest", "alice", "TOKEN");
    }
    assertAuthenticationFailure(
        authenticator,
        "alice@" + realm,
        "wrong-fixture-password".toCharArray(),
        Reason.INVALID_CREDENTIALS);
    assertAuthenticationFailure(
        authenticator,
        "denied@" + realm,
        TemporaryKerberosSecurity.PASSWORD.toCharArray(),
        Reason.INSUFFICIENT_PERMISSION);
    System.out.println("secure-hbase negative=custom-password-and-permissions status=passed");
  }

  @Test
  void accountPasswordDigestCannotImpersonateAStandardDelegationToken() throws Exception {
    assertAuthenticationFailure(
        HBaseAuthenticators.digestPassword(clusterId),
        "alice@" + realm,
        TemporaryKerberosSecurity.PASSWORD.toCharArray(),
        Reason.INVALID_CREDENTIALS);
    System.out.println("secure-hbase negative=password-digest-on-standard-server status=passed");
  }

  private void verifyTrustedSource(
      String mechanism, String expectedUser, Callable<? extends HBaseUserContext> source)
      throws Exception {
    try (var provider = provider(HBaseAuthenticators.kerberosPassword())) {
      var identity = await(provider.activateTrusted(source).toCompletableFuture());
      verifyOperations(provider, identity, mechanism, expectedUser, "KERBEROS");
    }
  }

  private void verifyOperations(
      HBaseBackendIdentityProvider provider,
      AuthorizedBackendIdentity identity,
      String mechanism,
      String expectedUser,
      String expectedAuthentication)
      throws Exception {
    AsyncConnection connection = await(provider.connectionFor(identity).toCompletableFuture());
    WhoAmIResponse observed =
        await(
            connection
                .getTable(table)
                .<AuthenticationService.Interface, WhoAmIResponse>coprocessorService(
                    AuthenticationService::newStub,
                    (service, controller, callback) ->
                        service.whoAmI(controller, WhoAmIRequest.getDefaultInstance(), callback),
                    new byte[0]));
    assertEquals(expectedUser, observed.getUsername());
    assertEquals(expectedAuthentication, observed.getAuthMethod());
    String runId = mechanism + UUID.randomUUID().toString().replace("-", "");
    var result = BackendProbe.execute(connection, table, runId);
    assertTrue(result.rawBytesRoundTrip());
    assertTrue(result.compareAndMutateSucceeded());
    assertTrue(result.staleCompareAndMutateRejected());
    assertEquals(2, result.scannedRows());
    BackendProbe.verifyPersistedValue(adminConnection, table, runId);
    BackendProbe.cleanup(connection, table, runId);
    System.out.printf(
        "secure-hbase authentication=%s observed=%s operations=put,get,cas,scan"
            + " independentRead=true permissions=read-write status=passed%n",
        mechanism, observed.getAuthMethod());
  }

  private HBaseBackendIdentityProvider provider(HBaseCredentialAuthenticator authenticator) {
    return new HBaseBackendIdentityProvider(
        "secure-backend",
        SCOPE,
        configuration,
        authenticator,
        HBaseAccessVerifiers.tablePermissions(
            List.of(new TableAccess(table, Set.of(Action.READ, Action.WRITE)))),
        1,
        1,
        Duration.ofSeconds(20));
  }

  private AuthorizedBackendIdentity authenticate(
      HBaseBackendIdentityProvider provider, String principal, char[] secret) throws Exception {
    try (var credentials = new BackendCredentials(principal, secret)) {
      return await(provider.authenticate(credentials, SCOPE).toCompletableFuture());
    } finally {
      Arrays.fill(secret, '\0');
    }
  }

  private void assertAuthenticationFailure(
      HBaseCredentialAuthenticator authenticator, String principal, char[] secret, Reason expected)
      throws Exception {
    try (var provider = provider(authenticator)) {
      var failure =
          assertThrows(ExecutionException.class, () -> authenticate(provider, principal, secret));
      assertEquals(
          expected, assertInstanceOf(BackendIdentityException.class, failure.getCause()).reason());
    }
  }

  private void grantAndAwait(String user, Set<Action> actions) throws Exception {
    var permission =
        Permission.newBuilder(table).withActions(actions.toArray(Action[]::new)).build();
    await(adminConnection.getAdmin().grant(new UserPermission(user, permission), false));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < deadline) {
      if (await(adminConnection.getAdmin().hasUserPermissions(user, List.of(permission)))
          .equals(List.of(true))) return;
      Thread.sleep(50);
    }
    throw new IllegalStateException("temporary HBase permission cache did not become ready");
  }

  private static <T> T await(CompletableFuture<T> future) throws Exception {
    return future.get(30, TimeUnit.SECONDS);
  }
}
