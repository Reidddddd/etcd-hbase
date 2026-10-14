package io.etcdhbase.hbase.probe;

import io.etcdhbase.hbase.connection.OfficialHBaseConnectionFactory;
import io.etcdhbase.hbase.identity.DevelopmentIdentityProvider;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalHBaseProbeIT {
  private static final long TIMEOUT_SECONDS = 60;
  private static final String PROFILE_ID = "local-hbase";
  private static final String TABLE_GROUP_ID = "local";

  private final DevelopmentIdentityProvider identityProvider = new DevelopmentIdentityProvider();
  private final List<OfficialHBaseConnectionFactory> connectionFactories = new ArrayList<>();
  private TemporaryHBaseClusterProcess cluster;
  private Configuration clientConfiguration;
  private String namespace;
  private TableName tableName;

  @BeforeEach
  void startUnauthenticatedTemporaryCluster() throws Exception {
    cluster = TemporaryHBaseClusterProcess.start();
    clientConfiguration = cluster.clientConfiguration();
    assertEquals("simple", clientConfiguration.get("hbase.security.authentication"));
    assertFalse(clientConfiguration.getBoolean("hbase.security.authorization", true));

    String runId = UUID.randomUUID().toString().replace("-", "");
    namespace = "etcd_hbase_m0_" + runId;
    tableName = TableName.valueOf(namespace, "probe");
  }

  @AfterEach
  void closeConnectionsAndTemporaryCluster() throws Exception {
    Exception cleanupFailure = null;
    if (cluster != null && namespace != null) {
      OfficialHBaseConnectionFactory cleanupFactory = null;
      try {
        AsyncConnection connection = openConnection();
        cleanupFactory = connectionFactories.getLast();
        AsyncAdmin admin = connection.getAdmin();
        if (await(admin.tableExists(tableName))) {
          if (!await(admin.isTableDisabled(tableName))) {
            await(admin.disableTable(tableName));
          }
          await(admin.deleteTable(tableName));
        }
        if (await(admin.listNamespaces()).contains(namespace)) {
          await(admin.deleteNamespace(namespace));
        }
        System.out.printf(
            "hbase-probe cleanup namespace=%s table=%s status=complete%n", namespace, tableName);
      } catch (Exception failure) {
        cleanupFailure = failure;
        System.err.printf(
            "probe-resource-cleanup-failed namespace=%s table=%s%n", namespace, tableName);
      } finally {
        if (cleanupFactory != null) {
          cleanupFactory.close();
        }
      }
    }

    for (OfficialHBaseConnectionFactory factory : connectionFactories) {
      factory.close();
    }
    identityProvider.close();
    if (cluster != null) {
      cluster.close();
    }
    if (cleanupFailure != null) {
      throw cleanupFailure;
    }
  }

  @Test
  void verifiesRawBytesCasScanIndependentReadAndConnectionRecreation() throws Exception {
    String runId = namespace.substring("etcd_hbase_m0_".length());
    OfficialHBaseConnectionFactory serviceFactory = newConnectionFactory();
    AsyncConnection serviceConnection = openConnection(serviceFactory);
    AsyncAdmin admin = serviceConnection.getAdmin();
    await(admin.createNamespace(NamespaceDescriptor.create(namespace).build()));
    await(
        admin.createTable(
            TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("d"))
                .build()));
    System.out.printf(
        "hbase-probe resource namespace=%s table=%s family=d auth=development-no-auth%n",
        namespace,
        tableName);

    BackendProbe.ProbeResult result = BackendProbe.execute(serviceConnection, tableName, runId);
    assertTrue(result.rawBytesRoundTrip());
    assertTrue(result.compareAndMutateSucceeded());
    assertTrue(result.staleCompareAndMutateRejected());
    assertEquals(2, result.scannedRows());

    OfficialHBaseConnectionFactory independentFactory = newConnectionFactory();
    BackendProbe.verifyPersistedValue(
        openConnection(independentFactory), tableName, runId);
    independentFactory.close();

    serviceFactory.close();
    OfficialHBaseConnectionFactory restartedFactory = newConnectionFactory();
    AsyncConnection restartedConnection = openConnection(restartedFactory);
    BackendProbe.verifyPersistedValue(restartedConnection, tableName, runId);

    System.out.printf(
        "hbase-probe profile=public-local authentication=simple authorization=false "
            + "hbase.version=%s hbase.jvm=%s client.jvm=%s "
            + "operations=put,get,cas,scan independentRead=true reconnectRead=true%n",
        cluster.hbaseVersion(),
        cluster.jvmVersion(),
        Runtime.version());
  }

  private AsyncConnection openConnection() throws Exception {
    return openConnection(newConnectionFactory());
  }

  private AsyncConnection openConnection(OfficialHBaseConnectionFactory factory) throws Exception {
    AuthorizedBackendIdentity identity;
    try (BackendCredentials credentials = new BackendCredentials("m0-local-probe", new char[0])) {
      identity =
          identityProvider
              .authenticate(
                  credentials,
                  new AuthenticationScope(TABLE_GROUP_ID, PROFILE_ID))
              .toCompletableFuture()
              .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    return factory
        .connectionFor(identity)
        .toCompletableFuture()
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  private OfficialHBaseConnectionFactory newConnectionFactory() {
    OfficialHBaseConnectionFactory factory =
        new OfficialHBaseConnectionFactory(clientConfiguration, PROFILE_ID, 1);
    connectionFactories.add(factory);
    return factory;
  }

  private static <T> T await(java.util.concurrent.CompletableFuture<T> future) throws Exception {
    return future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }
}
