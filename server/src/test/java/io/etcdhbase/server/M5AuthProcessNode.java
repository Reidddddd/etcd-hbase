package io.etcdhbase.server;

import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseCredentialAuthenticator;
import io.etcdhbase.hbase.api.HBaseUserContext;
import io.etcdhbase.hbase.connection.HBaseConnectionRouter;
import io.etcdhbase.hbase.identity.HBaseAccessVerifiers;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.identity.KerberosUserContext;
import io.etcdhbase.hbase.probe.auth.PasswordDigestSaslClientFixture;
import io.etcdhbase.hbase.storage.AuthControlBinding;
import io.etcdhbase.hbase.storage.HBaseAuthSessionStore;
import io.etcdhbase.server.auth.AesGcmCredentialEnvelopeProtector;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.server.auth.CredentialKeyProvider;
import io.etcdhbase.server.auth.FileCredentialKeyProvider;
import io.etcdhbase.server.auth.ProviderSessionIdentityManager;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.AuthenticationService;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.WhoAmIRequest;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.WhoAmIResponse;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hbase.client.AsyncConnection;

/** Test subprocess owning an independent J50 auth session store and provider identity pool. */
public final class M5AuthProcessNode implements AutoCloseable {
  private static final String CONTROL_PROVIDER_ID = "m5-control-process";
  private static final String USER_PROVIDER_ID = "m5-user-process";
  private static final String TRUST_DOMAIN = "m5-process-test";
  private static final String KEY_QUALIFIER = "v";
  private static final String KEY_FAMILY = "d";
  private static final byte[] PAYLOAD = "m5-process-value".getBytes(StandardCharsets.UTF_8);

  private final AuthenticationScope scope;
  private final TableName dataTable;
  private final TableName controlTable;
  private final AuthSessionService sessions;
  private final ProviderSessionIdentityManager identities;
  private final HBaseConnectionRouter connections;
  private final HBaseBackendIdentityProvider controlProvider;
  private final CredentialKeyProvider keys;

  private M5AuthProcessNode(AuthenticationScope scope, TableName dataTable,
      TableName controlTable, AuthSessionService sessions,
      ProviderSessionIdentityManager identities, HBaseConnectionRouter connections,
      HBaseBackendIdentityProvider controlProvider, CredentialKeyProvider keys) {
    this.scope = scope;
    this.dataTable = dataTable;
    this.controlTable = controlTable;
    this.sessions = sessions;
    this.identities = identities;
    this.connections = connections;
    this.controlProvider = controlProvider;
    this.keys = keys;
  }

  public static void main(String[] arguments) throws Exception {
    if (arguments.length != 11) throw new IllegalArgumentException("invalid process fixture arguments");
    Configuration configuration = HBaseConfiguration.create();
    configuration.addResource(new org.apache.hadoop.fs.Path(Path.of(arguments[0]).toUri()));
    UserGroupInformation.setConfiguration(configuration);
    AuthenticationScope scope = new AuthenticationScope(arguments[1], arguments[2]);
    TableName dataTable = TableName.valueOf(arguments[3]);
    TableName controlTable = TableName.valueOf(arguments[4]);
    String keyId = arguments[5];
    Path keyFile = Path.of(arguments[6]);
    ByteKey datasetDigest = ByteKey.copyOf(HexFormat.of().parseHex(arguments[7]));
    String adminPrincipal = arguments[8];
    String userAdapter = arguments[9];
    String clusterId = arguments[10];
    String adminKeytab = configuration.get("etcdhbase.test.security.admin.keytab");
    String krb5 = configuration.get("etcdhbase.test.security.krb5");
    if (adminKeytab == null || krb5 == null) {
      throw new IllegalStateException("secure process fixture material is unavailable");
    }
    System.setProperty("java.security.krb5.conf", krb5);

    try (M5AuthProcessNode node = open(configuration, scope, dataTable, controlTable,
        keyId, keyFile, datasetDigest, adminPrincipal, Path.of(adminKeytab), userAdapter,
        clusterId);
        var input = new DataInputStream(new BufferedInputStream(System.in));
        var output = new DataOutputStream(new BufferedOutputStream(System.out))) {
      output.writeUTF("READY");
      output.flush();
      node.serve(input, output);
    }
  }

  private static M5AuthProcessNode open(Configuration configuration, AuthenticationScope scope,
      TableName dataTable, TableName controlTable, String keyId, Path keyFile,
      ByteKey datasetDigest, String adminPrincipal, Path adminKeytab, String userAdapter,
      String clusterId) throws Exception {
    var controlScope = new AuthenticationScope("auth-control", scope.connectionProfileId());
    var controlProvider = new HBaseBackendIdentityProvider(CONTROL_PROVIDER_ID, controlScope,
        configuration, HBaseAuthenticators.kerberosPassword(),
        (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
        2, 1, Duration.ofSeconds(30));
    var controlIdentity = controlProvider.activateTrusted(() ->
        KerberosUserContext.fromKeytab(adminPrincipal, adminKeytab))
        .toCompletableFuture().get(30, TimeUnit.SECONDS);
    var connections = new HBaseConnectionRouter(controlProvider);
    var profile = new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2);
    var binding = new AuthControlBinding(controlTable, TRUST_DOMAIN,
        new HashSaltStrategy(profile));
    CredentialKeyProvider keys = new FileCredentialKeyProvider(keyId, Map.of(keyId, keyFile));
    var identityManager = new ProviderSessionIdentityManager(
        () -> new HBaseBackendIdentityProvider(USER_PROVIDER_ID, scope, configuration,
            userAuthenticator(userAdapter, clusterId),
            HBaseAccessVerifiers.tablePermissions(List.of(new HBaseAccessVerifiers.TableAccess(
                dataTable, Set.of(Action.READ, Action.WRITE, Action.EXEC)))),
            4, 1, Duration.ofSeconds(30)),
        connections, scope, USER_PROVIDER_ID, 8);
    RequestScope openScope = RequestScope.withTimeout(Duration.ofSeconds(45));
    try {
      var store = HBaseAuthSessionStore.open(controlProvider, controlIdentity, binding, openScope)
          .get(30, TimeUnit.SECONDS);
      var protector = new AesGcmCredentialEnvelopeProtector(keys, new SecureRandom());
      var sessions = new AuthSessionService(store, protector, identityManager, TRUST_DOMAIN,
          USER_PROVIDER_ID, scope, datasetDigest, Duration.ofMinutes(15), 16,
          Clock.systemUTC(), new SecureRandom());
      return new M5AuthProcessNode(scope, dataTable, controlTable, sessions,
          identityManager, connections, controlProvider, keys);
    } catch (Exception | Error failure) {
      try { identityManager.close(); } catch (RuntimeException | Error cleanup) {
        if (cleanup != failure) failure.addSuppressed(cleanup);
      }
      try { connections.close(); } catch (RuntimeException | Error cleanup) {
        if (cleanup != failure) failure.addSuppressed(cleanup);
      }
      try { controlProvider.close(); } catch (RuntimeException | Error cleanup) {
        if (cleanup != failure) failure.addSuppressed(cleanup);
      }
      keys.close();
      throw failure;
    } finally {
      openScope.close();
      openScope.cleanupCompletion().toCompletableFuture().get(30, TimeUnit.SECONDS);
    }
  }

  private static HBaseCredentialAuthenticator userAuthenticator(String adapter, String clusterId) {
    return switch (adapter) {
      case "kerberos-password" -> HBaseAuthenticators.kerberosPassword();
      case "delegation-token" -> HBaseAuthenticators.delegationToken(clusterId);
      case "password-sasl" -> passwordSaslAuthenticator(clusterId);
      default -> throw new IllegalArgumentException("unsupported test authentication adapter");
    };
  }

  private static HBaseCredentialAuthenticator passwordSaslAuthenticator(String clusterId) {
    HBaseCredentialAuthenticator digest = HBaseAuthenticators.digestPassword(clusterId);
    return (credentials, scope) -> {
      HBaseUserContext context = digest.authenticate(credentials, scope);
      try {
        context.user().getTokens().forEach(token -> token.setKind(
            new Text(PasswordDigestSaslClientFixture.TOKEN_KIND)));
        return context;
      } catch (RuntimeException | Error failure) {
        context.close();
        throw failure;
      }
    };
  }

  private void serve(DataInputStream input, DataOutputStream output) throws Exception {
    while (true) {
      String command = input.readUTF();
      if (command.equals("STOP")) {
        output.writeUTF("STOPPED");
        output.flush();
        return;
      }
      try {
        switch (command) {
          case "AUTH" -> authenticate(input, output);
          case "RESOLVE" -> resolve(input, output);
          default -> writeError(output, "INVALID_COMMAND");
        }
      } catch (Exception failure) {
        writeError(output, errorCode(failure));
      }
      output.flush();
    }
  }

  private void authenticate(DataInputStream input, DataOutputStream output) throws Exception {
    String principal = input.readUTF();
    char[] secret = input.readUTF().toCharArray();
    try (var credentials = new BackendCredentials(principal, secret);
        RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(60))) {
      AuthSessionService.IssuedToken issued = sessions.authenticate(credentials, request)
          .toCompletableFuture().get(60, TimeUnit.SECONDS);
      output.writeUTF("TOKEN\t" + issued.tokenForResponse());
    } finally {
      Arrays.fill(secret, '\0');
    }
  }

  private void resolve(DataInputStream input, DataOutputStream output) throws Exception {
    String token = input.readUTF();
    try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(60))) {
      AuthSessionService.ResolvedIdentity resolved = sessions.resolve(token, request)
          .toCompletableFuture().get(60, TimeUnit.SECONDS);
      try {
        AsyncConnection connection = connections.connectionFor(resolved.identity())
            .toCompletableFuture().get(30, TimeUnit.SECONDS);
        WhoAmIResponse whoAmI = connection.getTable(dataTable)
            .<AuthenticationService.Interface, WhoAmIResponse>coprocessorService(
                AuthenticationService::newStub,
                (service, controller, callback) -> service.whoAmI(
                    controller, WhoAmIRequest.getDefaultInstance(), callback), new byte[0])
            .get(30, TimeUnit.SECONDS);
        byte[] row = ("m5-process-" + whoAmI.getUsername()).getBytes(StandardCharsets.UTF_8);
        connection.getTable(dataTable).put(new Put(row)
            .addColumn(KEY_FAMILY.getBytes(StandardCharsets.UTF_8),
                KEY_QUALIFIER.getBytes(StandardCharsets.UTF_8), PAYLOAD))
            .get(30, TimeUnit.SECONDS);
        var result = connection.getTable(dataTable).get(new Get(row)
            .addColumn(KEY_FAMILY.getBytes(StandardCharsets.UTF_8),
                KEY_QUALIFIER.getBytes(StandardCharsets.UTF_8)))
            .get(30, TimeUnit.SECONDS);
        boolean userValueRoundTrips = Arrays.equals(PAYLOAD,
            result.getValue(KEY_FAMILY.getBytes(StandardCharsets.UTF_8),
                KEY_QUALIFIER.getBytes(StandardCharsets.UTF_8)));
        boolean controlTableDenied = assertControlTableDenied(connection);
        output.writeUTF("IDENTITY\t" + whoAmI.getUsername() + "\t"
            + whoAmI.getAuthMethod() + "\t" + userValueRoundTrips + "\t" + controlTableDenied);
      } finally {
        resolved.release().toCompletableFuture().get(30, TimeUnit.SECONDS);
      }
    }
  }

  private boolean assertControlTableDenied(AsyncConnection connection) throws Exception {
    try {
      connection.getTable(controlTable).get(new Get("blocked".getBytes(StandardCharsets.UTF_8)))
          .get(30, TimeUnit.SECONDS);
      return false;
    } catch (Exception failure) {
      for (Throwable cause = unwrap(failure); cause != null; cause = cause.getCause()) {
        if (cause instanceof AccessDeniedException) return true;
      }
      throw failure;
    }
  }

  private static String errorCode(Exception failure) {
    Throwable cause = unwrap(failure);
    if (cause instanceof AuthSessionException session) return session.reason().name();
    return cause.getClass().getSimpleName();
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable current = failure;
    while ((current instanceof CompletionException
        || current instanceof java.util.concurrent.ExecutionException)
        && current.getCause() != null) current = current.getCause();
    return current;
  }

  private static void writeError(DataOutputStream output, String code) throws IOException {
    output.writeUTF("ERROR\t" + code);
  }

  @Override
  public void close() {
    Throwable failure = null;
    try { identities.close(); } catch (RuntimeException | Error closeFailure) { failure = closeFailure; }
    try { connections.close(); } catch (RuntimeException | Error closeFailure) {
      if (failure == null) failure = closeFailure;
      else failure.addSuppressed(closeFailure);
    }
    try { controlProvider.close(); } catch (RuntimeException | Error closeFailure) {
      if (failure == null) failure = closeFailure;
      else failure.addSuppressed(closeFailure);
    }
    keys.close();
    if (failure instanceof Error fatal) throw fatal;
    if (failure instanceof RuntimeException closeFailure) throw closeFailure;
  }
}
