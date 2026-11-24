package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import etcdserverpb.AuthGrpc;
import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.admin.M3TableGroupInitializer;
import io.etcdhbase.admin.M3TableGroupPlan;
import io.etcdhbase.admin.TableGroupPlanFactory;
import io.etcdhbase.admin.config.InitializationConfiguration;
import io.etcdhbase.admin.config.InitializationConfigurationLoader;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.EtcdAuthTokenMetadata;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.identity.KerberosUserContext;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.hbase.storage.AuthControlBinding;
import io.etcdhbase.hbase.storage.HBaseAuthControlInitializer;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.server.config.M3RuntimeConfiguration;
import io.etcdhbase.server.config.M3RuntimeConfiguration.OwnerPeer;
import io.etcdhbase.server.config.RuntimeConfigurationLoader;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.stub.MetadataUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Exercises real cross-process Authenticate, KV gateways, owner forwarding, and reactivation. */
@Timeout(900)
class M5AuthenticateGatewayProcessIT {
  private static final String TABLE_GROUP = "m5-auth";
  private static final String HBASE_PROFILE = "m5-secure-hbase";
  private static final String ENDPOINT_PROVIDER = "m5-endpoint";
  private static final String TRUST_DOMAIN = "m5-gateway-test";
  private static final String AUTH_CONTROL_TABLE = "auth_sessions";
  private static final String API_ID = "localhost";
  private static final String OWNER_A = "m5-owner-a";
  private static final String OWNER_B = "m5-owner-b";
  private static final String OWNER_C = "m5-owner-c";
  private static final int OWNER_LEASE_MS = 12_000;
  private static final int HEARTBEAT_MS = 1_000;
  private static final Set<PosixFilePermission> PRIVATE_FILE = EnumSet.of(
      PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);

  @TempDir Path temporary;

  @Test
  void authenticatesOnAThenUsesBGetPutTxnAndReactivatesAfterFullStop() throws Exception {
    Path repositoryRoot = WireProcesses.repositoryRoot();
    try (var cluster = TemporaryHBaseClusterProcess.startSecure();
         var apiCertificate = TestTlsCertificate.createWithDnsSans(
             temporary, API_ID, List.of(API_ID));
         var ownerACertificate = TestTlsCertificate.create(temporary, OWNER_A);
         var ownerBCertificate = TestTlsCertificate.create(temporary, OWNER_B);
         var ownerCCertificate = TestTlsCertificate.create(temporary, OWNER_C)) {
      Configuration hbase = cluster.clientConfiguration();
      String realm = required(hbase, "etcdhbase.test.security.realm");
      String adminPrincipal = "admin@" + realm;
      Path adminKeytab = Path.of(required(hbase, "etcdhbase.test.security.admin.keytab"));
      Path krb5Configuration = Path.of(required(hbase, "etcdhbase.test.security.krb5"));
      Path hbaseSite = writeHBaseSite(hbase);
      SecretFiles secrets = writeSecrets(adminKeytab);

      String namespace = "m5auth" + UUID.randomUUID().toString().replace("-", "");
      InitializationConfiguration initialization = initialization(
          repositoryRoot, namespace, hbase.getInt("hbase.zookeeper.property.clientPort", -1));
      M3TableGroupPlan plan = M3TableGroupPlan.from(
          TableGroupPlanFactory.create(initialization, TABLE_GROUP, null));
      M3RuntimeConfiguration bootstrapActivation = activation("51");
      String hbaseClusterId = initializeTableGroup(
          hbase, plan, adminPrincipal, adminKeytab, bootstrapActivation);
      grantUserDataAccess(hbase, plan, adminPrincipal, adminKeytab);
      initializeAuthControl(hbase, plan, adminPrincipal, adminKeytab);

      int apiPortA = freePort();
      int apiPortB = freePort();
      int apiPortC = freePort();
      Path apiServerFile = writeRuntimeConfiguration(initialization, hbaseClusterId,
          adminPrincipal, secrets.directory(), hbaseSite, apiCertificate, apiPortA);
      Path apiServerBFile = writeRuntimeConfiguration(initialization, hbaseClusterId,
          adminPrincipal, secrets.directory(), hbaseSite, apiCertificate, apiPortB);
      Path apiServerCFile = writeRuntimeConfiguration(initialization, hbaseClusterId,
          adminPrincipal, secrets.directory(), hbaseSite, apiCertificate, apiPortC);

      int ownerAPort = freePort();
      int ownerBPort = freePort();
      int ownerCPort = freePort();
      Path trustedPeers = writeTrustedPeerCertificates(
          ownerACertificate, ownerBCertificate, ownerCCertificate);
      List<OwnerPeer> peers = List.of(
          new OwnerPeer(OWNER_A, "127.0.0.1", ownerAPort, OWNER_A),
          new OwnerPeer(OWNER_B, "127.0.0.1", ownerBPort, OWNER_B),
          new OwnerPeer(OWNER_C, "127.0.0.1", ownerCPort, OWNER_C));
      Path activationA = writeActivation(repositoryRoot, OWNER_A, "51", ownerAPort,
          ownerACertificate, trustedPeers, peers);
      Path activationB = writeActivation(repositoryRoot, OWNER_B, "52", ownerBPort,
          ownerBCertificate, trustedPeers, peers);
      Path activationC = writeActivation(repositoryRoot, OWNER_C, "53", ownerCPort,
          ownerCCertificate, trustedPeers, peers);

      ProcessNode processA = null;
      ProcessNode processB = null;
      ProcessNode processC = null;
      try (var ownerControl = openOwnerControl(hbase, plan, adminPrincipal, adminKeytab)) {
        assertTrue(ownerControl.read().ownerInstance().isEmpty(),
            "the unique test table group must have no owner before A starts");
        try {
          processA = startRuntime(apiServerFile, activationA, hbaseSite, krb5Configuration,
              "runtime-a.log", apiPortA);
          CommitState ownerAfterAStarts = ownerControl.read();
          assertTrue(ownerAfterAStarts.ownerInstance().isPresent(),
              "A must acquire the owner lease before B starts");
          assertTrue(ownerAfterAStarts.ownerLeaseDeadlineEpochMillis() > System.currentTimeMillis(),
              "A's persisted owner lease must still be active");

          processB = startRuntime(apiServerBFile, activationB, hbaseSite, krb5Configuration,
              "runtime-b.log", apiPortB);
          assertNotEquals(ProcessHandle.current().pid(), processA.pid());
          assertNotEquals(ProcessHandle.current().pid(), processB.pid());
          assertNotEquals(processA.pid(), processB.pid());
          assertSameOwner(ownerAfterAStarts, ownerControl.read(), "after B starts");

          String token = verifyAuthenticateOnAAndKvThroughB(processA.port(), processB.port(),
              apiCertificate.certificate(), realm, ownerControl, ownerAfterAStarts);

          // Stop the standby first so it cannot take over while A releases its owner lease.
          processB.close();
          processA.close();
          processB = null;
          processA = null;
          Thread.sleep(OWNER_LEASE_MS + HEARTBEAT_MS);

          processC = startRuntime(apiServerCFile, activationC, hbaseSite, krb5Configuration,
              "runtime-c.log", apiPortC);
          verifyPersistedTokenOnRestartedReplica(
              processC.port(), apiCertificate.certificate(), token);
        } finally {
          closeProcess(processC);
          closeProcess(processB);
          closeProcess(processA);
        }
      }
    }
  }

  private String verifyAuthenticateOnAAndKvThroughB(int portA, int portB,
      Path apiCertificate, String realm, OwnerControlReader ownerControl,
      CommitState expectedOwner) throws Exception {
    ManagedChannel channelA = tlsChannel(portA, apiCertificate);
    ManagedChannel channelB = tlsChannel(portB, apiCertificate);
    try {
      String token = AuthGrpc.newBlockingStub(channelA).withDeadlineAfter(60, TimeUnit.SECONDS)
          .authenticate(Rpc.AuthenticateRequest.newBuilder()
              .setName("alice@" + realm)
              .setPassword(fixturePassword())
              .build())
          .getToken();
      assertEquals(43, token.length(), "Authenticate must return only an opaque token");

      KVGrpc.KVBlockingStub localKv = authenticatedKv(channelA, token);
      KVGrpc.KVBlockingStub kv = authenticatedKv(channelB, token);
      ByteString putKey = ByteString.copyFromUtf8("j50-cross-process-put");
      ByteString putValue = ByteString.copyFromUtf8("authenticated-through-b");
      Rpc.RangeResponse localMissing = localKv.withDeadlineAfter(60, TimeUnit.SECONDS)
          .range(Rpc.RangeRequest.newBuilder().setKey(putKey).build());
      assertEquals(0, localMissing.getKvsCount(),
          "the newly issued token must work on its authenticating replica");
      Rpc.RangeResponse missing = kv.withDeadlineAfter(60, TimeUnit.SECONDS)
          .range(Rpc.RangeRequest.newBuilder().setKey(putKey).build());
      assertEquals(0, missing.getKvsCount());

      CommitState beforePut = ownerControl.read();
      assertSameOwner(expectedOwner, beforePut, "before B's Put");
      Rpc.PutResponse put = kv.withDeadlineAfter(60, TimeUnit.SECONDS)
          .put(Rpc.PutRequest.newBuilder().setKey(putKey).setValue(putValue).build());
      assertEquals(52, put.getHeader().getMemberId(),
          "the response header must identify B, which received the Put request");
      CommitState afterPut = ownerControl.read();
      assertSameOwner(expectedOwner, afterPut, "after B's Put");
      assertEquals(beforePut.publishedSequence() + 1, afterPut.publishedSequence(),
          "B's Put must publish one commit while A remains the owner");
      Rpc.RangeResponse readBack = kv.withDeadlineAfter(60, TimeUnit.SECONDS)
          .range(Rpc.RangeRequest.newBuilder().setKey(putKey).build());
      assertEquals(1, readBack.getKvsCount());
      assertEquals(putValue, readBack.getKvs(0).getValue());

      ByteString txnKey = ByteString.copyFromUtf8("j50-cross-process-txn");
      ByteString txnValue = ByteString.copyFromUtf8("authenticated-txn-through-b");
      CommitState beforeTxn = ownerControl.read();
      assertSameOwner(expectedOwner, beforeTxn, "before B's transaction");
      Rpc.TxnResponse txn = kv.withDeadlineAfter(60, TimeUnit.SECONDS)
          .txn(Rpc.TxnRequest.newBuilder()
              .addCompare(Rpc.Compare.newBuilder().setKey(txnKey)
                  .setTarget(Rpc.Compare.CompareTarget.VERSION)
                  .setResult(Rpc.Compare.CompareResult.EQUAL).setVersion(0).build())
              .addSuccess(Rpc.RequestOp.newBuilder().setRequestPut(Rpc.PutRequest.newBuilder()
                  .setKey(txnKey).setValue(txnValue).build()))
              .build());
      assertTrue(txn.getSucceeded());
      assertEquals(52, txn.getHeader().getMemberId(),
          "the response header must identify B, which received the transaction");
      CommitState afterTxn = ownerControl.read();
      assertSameOwner(expectedOwner, afterTxn, "after B's transaction");
      assertEquals(beforeTxn.publishedSequence() + 1, afterTxn.publishedSequence(),
          "B's transaction must publish one commit while A remains the owner");
      Rpc.RangeResponse txnReadBack = kv.withDeadlineAfter(60, TimeUnit.SECONDS)
          .range(Rpc.RangeRequest.newBuilder().setKey(txnKey).build());
      assertEquals(1, txnReadBack.getKvsCount());
      assertEquals(txnValue, txnReadBack.getKvs(0).getValue());

      CommitState beforeDelete = ownerControl.read();
      assertSameOwner(expectedOwner, beforeDelete, "before B's DeleteRange");
      Rpc.DeleteRangeResponse deleted = kv.withDeadlineAfter(60, TimeUnit.SECONDS)
          .deleteRange(Rpc.DeleteRangeRequest.newBuilder().setKey(txnKey).build());
      assertEquals(52, deleted.getHeader().getMemberId(),
          "the response header must identify B, which received DeleteRange");
      assertEquals(1, deleted.getDeleted(), "DeleteRange must remove the transaction's key");
      CommitState afterDelete = ownerControl.read();
      assertSameOwner(expectedOwner, afterDelete, "after B's DeleteRange");
      assertEquals(beforeDelete.publishedSequence() + 1, afterDelete.publishedSequence(),
          "B's DeleteRange must publish one commit while A remains the owner");
      Rpc.RangeResponse deletedReadBack = kv.withDeadlineAfter(60, TimeUnit.SECONDS)
          .range(Rpc.RangeRequest.newBuilder().setKey(txnKey).build());
      assertEquals(0, deletedReadBack.getKvsCount());
      return token;
    } finally {
      closeChannel(channelB);
      closeChannel(channelA);
    }
  }

  private static void assertSameOwner(CommitState expected, CommitState actual, String context) {
    assertTrue(expected.ownerInstance().isPresent(), "A must be the owner " + context);
    assertEquals(expected.ownerInstance(), actual.ownerInstance(),
        "the owner instance must remain A " + context);
    assertEquals(expected.ownerEpoch(), actual.ownerEpoch(),
        "the owner epoch must remain unchanged " + context);
    assertTrue(actual.ownerLeaseDeadlineEpochMillis() > System.currentTimeMillis(),
        "the persisted owner lease must remain active " + context);
  }

  private static OwnerControlReader openOwnerControl(Configuration configuration,
      M3TableGroupPlan plan, String adminPrincipal, Path adminKeytab) throws Exception {
    AuthenticationScope scope = new AuthenticationScope(
        plan.source().tableGroupId(), plan.source().hbaseProfile().id());
    var provider = new HBaseBackendIdentityProvider(plan.source().identityProviderId(), scope,
        configuration, HBaseAuthenticators.kerberosPassword(),
        (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
        2, 1, Duration.ofSeconds(30));
    try {
      var identity = provider.activateTrusted(() ->
          KerberosUserContext.fromKeytab(adminPrincipal, adminKeytab))
          .toCompletableFuture().get(60, TimeUnit.SECONDS);
      try (var request = RequestScope.withTimeout(Duration.ofSeconds(60))) {
        var control = HBaseCommitControlStore.open(provider, identity, plan.system(), request)
            .get(60, TimeUnit.SECONDS);
        return new OwnerControlReader(provider, control);
      }
    } catch (Exception | Error failure) {
      provider.close();
      throw failure;
    }
  }

  private void verifyPersistedTokenOnRestartedReplica(int port, Path apiCertificate, String token)
      throws Exception {
    ManagedChannel channel = tlsChannel(port, apiCertificate);
    try {
      // A/B are both stopped. C must decrypt and reactivate Alice's persisted session locally.
      KVGrpc.KVBlockingStub kv = authenticatedKv(channel, token);
      Rpc.RangeResponse put = kv.withDeadlineAfter(60, TimeUnit.SECONDS)
          .range(Rpc.RangeRequest.newBuilder()
              .setKey(ByteString.copyFromUtf8("j50-cross-process-put")).build());
      assertEquals(1, put.getKvsCount());
      assertEquals(ByteString.copyFromUtf8("authenticated-through-b"), put.getKvs(0).getValue());

      Rpc.PutResponse resumedWrite = kv.withDeadlineAfter(60, TimeUnit.SECONDS)
          .put(Rpc.PutRequest.newBuilder()
              .setKey(ByteString.copyFromUtf8("j50-after-full-stop"))
              .setValue(ByteString.copyFromUtf8("reactivated-on-c"))
              .build());
      assertEquals(53, resumedWrite.getHeader().getMemberId());
    } finally {
      closeChannel(channel);
    }
  }

  private static KVGrpc.KVBlockingStub authenticatedKv(ManagedChannel channel, String token) {
    var metadata = new Metadata();
    metadata.put(EtcdAuthTokenMetadata.key(), token);
    return KVGrpc.newBlockingStub(ClientInterceptors.intercept(channel,
        MetadataUtils.newAttachHeadersInterceptor(metadata)));
  }

  private static ManagedChannel tlsChannel(int port, Path apiCertificate) throws Exception {
    SslContext tls = GrpcSslContexts.configure(SslContextBuilder.forClient()
        .trustManager(apiCertificate.toFile())).build();
    return NettyChannelBuilder.forAddress("127.0.0.1", port)
        .sslContext(tls).overrideAuthority(API_ID).build();
  }

  private String fixturePassword() {
    return io.etcdhbase.hbase.probe.auth.PasswordDigestSaslServerFixture.FIXTURE_PASSWORD;
  }

  private static InitializationConfiguration initialization(Path repositoryRoot,
      String namespace, int clientPort) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Path template = repositoryRoot.resolve("config/examples/j05/native-cell-versions.json");
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(template));
    ((ObjectNode) tree.withArray("hbase_profiles").get(0))
        .put("id", HBASE_PROFILE)
        .put("quorum", "127.0.0.1")
        .put("client_port", clientPort)
        .put("namespace", namespace)
        .put("authentication_mode", "CREDENTIAL_REFERENCE")
        .put("credential_reference", "secret://admin.keytab");
    ObjectNode group = (ObjectNode) tree.withArray("table_groups").get(0);
    group.put("id", TABLE_GROUP)
        .put("hbase_profile_id", HBASE_PROFILE)
        .put("identity_provider_id", ENDPOINT_PROVIDER);
    ArrayNode categories = (ArrayNode) group.with("system_table").withArray("record_categories");
    if (java.util.stream.StreamSupport.stream(categories.spliterator(), false)
        .noneMatch(value -> value.asText().equals("RESULT"))) {
      categories.add("RESULT");
    }
    return new InitializationConfigurationLoader().load(
        new ByteArrayInputStream(mapper.writeValueAsBytes(tree)));
  }

  private static String initializeTableGroup(Configuration configuration, M3TableGroupPlan plan,
      String adminPrincipal, Path adminKeytab, M3RuntimeConfiguration activation) throws Exception {
    AuthenticationScope scope = new AuthenticationScope(
        plan.source().tableGroupId(), plan.source().hbaseProfile().id());
    try (var provider = new HBaseBackendIdentityProvider(plan.source().identityProviderId(), scope,
        configuration, HBaseAuthenticators.kerberosPassword(),
        (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
        2, 1, Duration.ofSeconds(30))) {
      var identity = provider.activateTrusted(() ->
          KerberosUserContext.fromKeytab(adminPrincipal, adminKeytab))
          .toCompletableFuture().get(60, TimeUnit.SECONDS);
      M3TableGroupInitializer.initializeFresh(plan, "sha256:" + plan.source().fingerprint(),
          identity, provider, activation.pinConfiguration());
    }
    try (var operator = KerberosUserContext.fromKeytab(adminPrincipal, adminKeytab);
         var connection = ConnectionFactory.createAsyncConnection(configuration, operator.user())
             .get(60, TimeUnit.SECONDS)) {
      return connection.getAdmin().getClusterMetrics()
          .get(60, TimeUnit.SECONDS).getClusterId();
    }
  }

  private static void grantUserDataAccess(Configuration configuration, M3TableGroupPlan plan,
      String adminPrincipal, Path adminKeytab) throws Exception {
    try (var operator = KerberosUserContext.fromKeytab(adminPrincipal, adminKeytab);
         var connection = ConnectionFactory.createAsyncConnection(configuration, operator.user())
             .get(60, TimeUnit.SECONDS)) {
      var admin = connection.getAdmin();
      for (var schema : plan.source().tables()) {
        TableName table = TableName.valueOf(plan.source().namespace(), schema.tableName());
        Permission permission = Permission.newBuilder(table)
            .withActions(Action.READ, Action.WRITE, Action.EXEC).build();
        admin.grant(new UserPermission("alice", permission), false).get(60, TimeUnit.SECONDS);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        while (System.nanoTime() < deadline
            && !admin.hasUserPermissions("alice", List.of(permission))
                .get(30, TimeUnit.SECONDS).equals(List.of(true))) {
          Thread.sleep(50);
        }
        assertEquals(List.of(true), admin.hasUserPermissions("alice", List.of(permission))
            .get(30, TimeUnit.SECONDS), "M3 table permission did not become visible: " + table);
      }
    }
  }

  private static void initializeAuthControl(Configuration configuration, M3TableGroupPlan plan,
      String adminPrincipal, Path adminKeytab) throws Exception {
    var controlScope = new AuthenticationScope("auth-control", plan.source().hbaseProfile().id());
    try (var provider = new HBaseBackendIdentityProvider("m5-control-bootstrap", controlScope,
        configuration, HBaseAuthenticators.kerberosPassword(),
        (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
        2, 1, Duration.ofSeconds(30))) {
      var operator = provider.activateTrusted(() ->
          KerberosUserContext.fromKeytab(adminPrincipal, adminKeytab))
          .toCompletableFuture().get(60, TimeUnit.SECONDS);
      var saltProfile = plan.source().saltProfile();
      var salt = new HashSaltStrategy(new SaltProfile(saltProfile.strategyId(),
          saltProfile.encodingVersion(), saltProfile.bucketCount(), saltProfile.saltWidthBytes()));
      var binding = new AuthControlBinding(
          TableName.valueOf(plan.source().namespace(), AUTH_CONTROL_TABLE), TRUST_DOMAIN, salt);
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(60))) {
        HBaseAuthControlInitializer.initializeFresh(provider, operator, binding, request)
            .get(60, TimeUnit.SECONDS);
      }
    }
  }

  private static ObjectNode authenticationConfiguration(ObjectMapper mapper,
      InitializationConfiguration initialization, String clusterId, String adminPrincipal,
      Path secretDirectory, Path hbaseSite) {
    ObjectNode auth = mapper.createObjectNode()
        .put("trust_domain", TRUST_DOMAIN)
        .put("session_ttl_millis", TimeUnit.MINUTES.toMillis(10))
        .put("session_slots", 16)
        .put("maximum_identity_containers", 8)
        .put("secret_directory", secretDirectory.toString());
    ObjectNode control = auth.putObject("auth_control")
        .put("hbase_profile_id", HBASE_PROFILE)
        .put("salt_profile_id", initialization.saltProfiles().getFirst().id())
        .put("table_name", AUTH_CONTROL_TABLE)
        .put("provider_id", "m5-control-provider")
        .put("client_profile_id", "m5-admin-client");
    addAdminIdentity(control.putObject("identity"), adminPrincipal);

    ArrayNode clients = auth.putArray("client_profiles");
    addClientProfile(clients.addObject(), "m5-admin-client", clusterId, hbaseSite);
    addClientProfile(clients.addObject(), "m5-endpoint-client", clusterId, hbaseSite);
    auth.putArray("endpoint_bindings").addObject()
        .put("table_group_id", TABLE_GROUP)
        .put("provider_id", ENDPOINT_PROVIDER)
        .put("client_profile_id", "m5-endpoint-client");
    ObjectNode background = auth.putArray("background_identities").addObject()
        .put("table_group_id", TABLE_GROUP)
        .put("provider_id", ENDPOINT_PROVIDER)
        .put("client_profile_id", "m5-admin-client");
    addAdminIdentity(background.putObject("identity"), adminPrincipal);
    background.putArray("allowed_table_group_ids").add(TABLE_GROUP);
    auth.putObject("credential_key_ring").put("active_key_id", "m5-test-key")
        .putArray("keys").addObject()
        .put("key_id", "m5-test-key")
        .put("secret_reference", "secret://credential.key");
    return auth;
  }

  private static void addAdminIdentity(ObjectNode identity, String adminPrincipal) {
    identity.put("source", "KERBEROS_KEYTAB")
        .put("principal", adminPrincipal)
        .put("credential_reference", "secret://admin.keytab");
  }

  private static void addClientProfile(ObjectNode profile, String id,
      String clusterId, Path hbaseSite) {
    profile.put("id", id)
        .put("hbase_profile_id", HBASE_PROFILE)
        .put("authenticator_id", "kerberos-password")
        .put("cluster_id", clusterId)
        .put("hbase_site_file", hbaseSite.toString());
  }

  private Path writeActivation(Path repositoryRoot, String instanceId, String memberId,
      int ownerPort, TestTlsCertificate certificate, Path trustedPeers,
      List<OwnerPeer> peers) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(
        repositoryRoot.resolve("config/examples/m3/activation.json")));
    tree.put("member_id", memberId)
        .put("owner_lease_ms", OWNER_LEASE_MS)
        .put("heartbeat_ms", HEARTBEAT_MS)
        .put("startup_timeout_ms", 90_000)
        .put("shutdown_timeout_ms", 30_000)
        .put("owner_instance_id", instanceId);
    ObjectNode forwarding = tree.putObject("owner_forwarding")
        .put("bind_host", "127.0.0.1")
        .put("port", ownerPort)
        .put("certificate_file", certificate.certificate().toString())
        .put("private_key_file", certificate.privateKey().toString())
        .put("trusted_ca_file", trustedPeers.toString());
    ArrayNode peerArray = forwarding.putArray("peers");
    for (OwnerPeer peer : peers) {
      peerArray.addObject().put("instance_id", peer.instanceId())
          .put("host", peer.host()).put("port", peer.port())
          .put("peer_identity", peer.peerIdentity());
    }
    Path output = temporary.resolve("activation-" + instanceId + ".json");
    Files.write(output, mapper.writeValueAsBytes(tree));
    try (var input = Files.newInputStream(output)) {
      new io.etcdhbase.server.config.M3RuntimeConfigurationLoader().load(input);
    }
    return output;
  }

  private ProcessNode startRuntime(Path runtime, Path activation, Path hbaseSite,
      Path krb5Configuration, String logName, int expectedPort) throws Exception {
    Path log = temporary.resolve(logName);
    var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-Djava.security.krb5.conf=" + krb5Configuration,
        "-cp", System.getProperty("java.class.path"),
        M5AuthenticateGatewayRuntimeProcess.class.getName(), runtime.toString(),
        activation.toString(), hbaseSite.toString(), krb5Configuration.toString())
        .directory(WireProcesses.repositoryRoot().toFile())
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    ProcessNode node = new ProcessNode(process, log, expectedPort);
    try {
      node.awaitReady();
      return node;
    } catch (Exception | Error failure) {
      node.terminateAfterStartupFailure();
      throw failure;
    }
  }

  private static String required(Configuration configuration, String property) {
    String value = configuration.get(property);
    if (value == null || value.isBlank()) throw new IllegalStateException("secure HBase fixture property is missing");
    return value;
  }

  private Path writeHBaseSite(Configuration configuration) throws IOException {
    Path destination = temporary.resolve("hbase-site.xml");
    try (var output = Files.newOutputStream(destination)) {
      configuration.writeXml(output);
    }
    return destination;
  }

  private SecretFiles writeSecrets(Path adminKeytab) throws Exception {
    Path directory = Files.createDirectory(temporary.resolve("secrets"));
    Path keytab = directory.resolve("admin.keytab");
    Files.copy(adminKeytab, keytab);
    makePrivate(keytab);
    Path credentialKey = directory.resolve("credential.key");
    byte[] key = new byte[32];
    try {
      new SecureRandom().nextBytes(key);
      Files.write(credentialKey, key);
      makePrivate(credentialKey);
    } finally {
      Arrays.fill(key, (byte) 0);
    }
    return new SecretFiles(directory);
  }

  private Path writeTrustedPeerCertificates(TestTlsCertificate... certificates) throws IOException {
    Path trustedPeers = temporary.resolve("trusted-owner-peers.pem");
    String material = String.join(System.lineSeparator(), Arrays.stream(certificates)
        .map(certificate -> {
          try {
            return Files.readString(certificate.certificate());
          } catch (IOException failure) {
            throw new java.io.UncheckedIOException(failure);
          }
        }).toList());
    Files.writeString(trustedPeers, material + System.lineSeparator(), StandardCharsets.US_ASCII);
    return trustedPeers;
  }

  private Path writeRuntimeConfiguration(InitializationConfiguration initialization,
      String clusterId, String adminPrincipal, Path secretDirectory, Path hbaseSite,
      TestTlsCertificate certificate, int port) throws Exception {
    Path repositoryRoot = WireProcesses.repositoryRoot();
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode tree = (ObjectNode) mapper.readTree(Files.readAllBytes(
        repositoryRoot.resolve("config/examples/m3/server.json")));
    ObjectMapper snakeMapper = new ObjectMapper()
        .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    ObjectNode init = (ObjectNode) snakeMapper.valueToTree(initialization);
    tree.set("hbase_profiles", init.withArray("hbase_profiles").deepCopy());
    tree.set("salt_profiles", init.withArray("salt_profiles").deepCopy());
    tree.set("table_groups", init.withArray("table_groups").deepCopy());
    ObjectNode listener = (ObjectNode) tree.withArray("listeners").get(0);
    listener.put("id", "m5-auth-listener")
        .put("bind_host", "127.0.0.1")
        .put("port", port)
        .put("table_group_id", TABLE_GROUP);
    listener.putObject("tls")
        .put("certificate_file", certificate.certificate().toString())
        .put("private_key_file", certificate.privateKey().toString());
    tree.set("authentication", authenticationConfiguration(mapper,
        initialization, clusterId, adminPrincipal, secretDirectory, hbaseSite));
    Path output = temporary.resolve("runtime-" + port + ".json");
    Files.write(output, mapper.writeValueAsBytes(tree));
    try (var input = Files.newInputStream(output)) {
      new RuntimeConfigurationLoader().load(input);
    }
    return output;
  }

  private static M3RuntimeConfiguration activation(String memberId) {
    return new M3RuntimeConfiguration(memberId, OWNER_LEASE_MS, HEARTBEAT_MS,
        90_000, 30_000, 512L << 20, 1_572_864, 2, 16, 60_000);
  }

  private static int freePort() throws IOException {
    try (var socket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
      return socket.getLocalPort();
    }
  }

  private static void closeChannel(ManagedChannel channel) throws InterruptedException {
    channel.shutdown();
    if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS), "test TLS channel did not terminate");
    }
  }

  private static void closeProcess(ProcessNode process) throws Exception {
    if (process != null) process.close();
  }

  private static void makePrivate(Path file) throws IOException {
    try {
      Files.setPosixFilePermissions(file, PRIVATE_FILE);
    } catch (UnsupportedOperationException ignored) {
      // The temporary directory is private on platforms without POSIX file permissions.
    }
  }

  private record SecretFiles(Path directory) {}

  private static final class OwnerControlReader implements AutoCloseable {
    private final HBaseBackendIdentityProvider provider;
    private final HBaseCommitControlStore control;

    private OwnerControlReader(HBaseBackendIdentityProvider provider,
        HBaseCommitControlStore control) {
      this.provider = provider;
      this.control = control;
    }

    CommitState read() throws Exception {
      try (var scope = RequestScope.withTimeout(Duration.ofSeconds(30))) {
        return control.read(scope).get(30, TimeUnit.SECONDS)
            .orElseThrow(() -> new IllegalStateException("M3 owner control row is missing")).state();
      }
    }

    @Override
    public void close() {
      provider.close();
    }
  }

  private static final class ProcessNode implements AutoCloseable {
    private final Process process;
    private final Path log;
    private final int expectedPort;
    private boolean closed;

    private ProcessNode(Process process, Path log, int expectedPort) {
      this.process = process;
      this.log = log;
      this.expectedPort = expectedPort;
    }

    long pid() { return process.pid(); }
    int port() { return expectedPort; }

    private void awaitReady() throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
      while (System.nanoTime() < deadline) {
        if (Files.isRegularFile(log)
            && Files.readString(log).contains("M5_GATEWAY_READY\t" + expectedPort)) return;
        if (!process.isAlive()) {
          throw new AssertionError("M5 runtime exited before readiness; log:\n" + readLog());
        }
        Thread.sleep(100);
      }
      throw new AssertionError("M5 runtime did not become ready; log:\n" + readLog());
    }

    private void terminateAfterStartupFailure() throws InterruptedException {
      process.descendants().forEach(ProcessHandle::destroyForcibly);
      process.destroyForcibly();
      process.waitFor(10, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {
      if (closed) return;
      closed = true;
      if (!process.isAlive()) {
        assertEquals(0, process.exitValue(), "M5 runtime exited unexpectedly; log:\n" + readLog());
        return;
      }
      process.getOutputStream().write("STOP\n".getBytes(StandardCharsets.UTF_8));
      process.getOutputStream().flush();
      if (!process.waitFor(120, TimeUnit.SECONDS)) {
        process.descendants().forEach(ProcessHandle::destroyForcibly);
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
        throw new AssertionError("M5 runtime did not stop after the graceful-stop command; log:\n" + readLog());
      }
      String output = readLog();
      assertTrue(output.contains("M5_GATEWAY_STOPPED"),
          "M5 runtime did not report completed cleanup; log:\n" + output);
      assertEquals(0, process.exitValue(), "M5 runtime failed during cleanup; log:\n" + output);
    }

    private String readLog() {
      try {
        return Files.readString(log);
      } catch (IOException unavailable) {
        return "runtime log is unavailable";
      }
    }
  }
}
