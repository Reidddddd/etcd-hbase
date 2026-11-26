package io.etcdhbase.server;

import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.storage.HBaseAuthSessionStore;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.hbase.storage.SystemTableBinding;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/** Shared binding and administrator-only bootstrap for the independent secure session JVMs. */
final class M5AuthProcessControl {
  static final String USER_PROVIDER_ID = "m5-user-process";
  static final String TRUST_DOMAIN = "m5-process-test";
  static final String IDENTITY_DOMAIN = "m5-process-realm";
  static final String SYSTEM_TABLE_PROPERTY = "etcdhbase.test.auth.system-table";
  static final AuthenticationScope SCOPE = new AuthenticationScope("m5-process", "local");
  private static final String SALT_PROFILE = "m5-process-salt";
  private static final byte[] FAMILY = {'s'};

  private M5AuthProcessControl() {}

  static SystemTableBinding binding(TableName table) {
    return new SystemTableBinding(table, FAMILY, SCOPE.tableGroupId(), SALT_PROFILE,
        new HashSaltStrategy(new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2)));
  }

  static TableDescriptor descriptor(TableName table) {
    var salt = binding(table).saltProfile();
    return TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", SCOPE.tableGroupId())
        .setValue("etcdhbase.m0.salt-profile-id", SALT_PROFILE)
        .setValue("etcdhbase.m0.salt-strategy-id", salt.strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version", Integer.toString(salt.encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count", Integer.toString(salt.bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes", Integer.toString(salt.saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories", "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION,SNAPSHOT_PIN")
        .build();
  }

  static void initialize(HBaseConnectionFactory connections, AuthorizedBackendIdentity operator,
      HBaseAuthSessionStore sessions, TableName table, ByteKey dataset, RequestScope request) throws Exception {
    var binding = binding(table);
    var control = HBaseCommitControlStore.open(connections, operator, binding, request).get(30, TimeUnit.SECONDS);
    var initial = CommitState.initial(binding.profileFingerprint(), 1);
    if (!control.initializeIfAbsent(initial, request).get(30, TimeUnit.SECONDS)) {
      throw new IllegalStateException("fresh process fixture control already exists");
    }
    var observed = control.read(request).get(30, TimeUnit.SECONDS).orElseThrow();
    long now = System.currentTimeMillis();
    if (!control.compareAndSet(observed, initial.claimOwner("m5-process-owner", now,
        now + TimeUnit.HOURS.toMillis(1)), request).get(30, TimeUnit.SECONDS)) {
      throw new IllegalStateException("fresh process fixture owner could not be installed");
    }
    observed = control.read(request).get(30, TimeUnit.SECONDS).orElseThrow();
    if (!control.compareAndSet(observed, observed.state().enableAuthorizationBarrier(
        "m5-process-owner", observed.state().ownerEpoch(), now), request).get(30, TimeUnit.SECONDS)) {
      throw new IllegalStateException("fresh process fixture authorization barrier could not be enabled");
    }
    var registry = AuthDeploymentRegistry.initial(TRUST_DOMAIN, List.of(new AuthDeploymentRegistry.Group(
        SCOPE.tableGroupId(), USER_PROVIDER_ID, IDENTITY_DOMAIN, SCOPE.connectionProfileId(), dataset,
        binding.profileFingerprint(), true)));
    if (!sessions.createRegistryIfAbsent(registry, request).toCompletableFuture().get(30, TimeUnit.SECONDS)) {
      throw new IllegalStateException("fresh process fixture registry already exists");
    }
  }
}
