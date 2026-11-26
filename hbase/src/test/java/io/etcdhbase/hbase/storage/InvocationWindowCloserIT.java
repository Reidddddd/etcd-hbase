package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.ResultDirectory;
import io.etcdhbase.core.commit.ResultPageReference;
import io.etcdhbase.core.commit.ResultReference;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.InvocationMessages;
import io.etcdhbase.hbase.codec.ResultMessages;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.time.Duration;
import java.util.UUID;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/**
 * §6.2 requires an invocation's result window to close before its result is retired, and the
 * retained binding to keep the request digests so the same request can never run again.
 */
class InvocationWindowCloserIT {
  private static final byte[] FAMILY = {'s'};
  private static final int SALT_BUCKETS = 4;
  private static final HashSaltStrategy SALT = new HashSaltStrategy(
      new io.etcdhbase.spi.salt.SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, SALT_BUCKETS, 2));
  private static final String RECORD_CATEGORIES = String.join(",",
      "CONTROL", "INVOCATION", "CANDIDATE", "MANIFEST", "DECISION", "SNAPSHOT_PIN",
      "RESULT", "TABLE_GROUP_IDENTITY", "WORKSET", "LEASE_STATE", "LEASE_ENUMERATION",
      "EXPIRY_CANDIDATE", "AUXILIARY", "LEASE_RESULT", "LEASE_BINDING",
      "LEASE_EXPIRY_CHECKPOINT");

  @Test
  void closesTheWindowFirstAndKeepsTheDigestsNeededToRejectAReplay() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var store = open(environment, scope);
      var invocations = new InvocationService(new HBaseInvocationRepository(store));
      var binding = selectedInvocation(invocations, scope);
      var permit = SystemRecordRetirement.Permit.issue(store.profileFingerprint(),
          ByteKey.copyOf(new byte[32]), InvocationId.create(), 1, 16, 16, scope);
      var closer = new InvocationWindowCloser(store, invocations, store);

      var identity = ByteKey.copyOf(binding.invocationId().bytes());
      var resultIdentity = ResultIdentity.from(binding);
      var directory = ResultDirectory.create(resultIdentity,
          java.util.List.of(new ResultPageReference(resultIdentity, 0, 8, 1,
              ByteKey.copyOf(new byte[32]))));
      var resultRow = ResultDirectory.systemIdentity(resultIdentity);
      assertEquals(SystemRecordStore.CreateResult.CREATED,
          store.createIfAbsent(SystemRecordType.RESULT, resultRow,
              ByteKey.copyOf(ResultMessages.encode(directory)), scope).join());
      // Passing null lets the page derive its own digest, which the directory reference must match.
      var outcome = closer.close(identity, permit, scope).join();
      var closed = assertInstanceOf(InvocationWindowCloser.Closed.class, outcome);

      var stored = InvocationMessages.decode(store.read(SystemRecordType.INVOCATION, identity,
          scope).join().orElseThrow().bytes());
      assertEquals(InvocationBinding.State.WINDOW_CLOSED, stored.state());
      assertTrue(stored.resultReference().isEmpty(), "a closed window keeps no result reference");
      assertEquals(binding.requestDigest(), stored.requestDigest(),
          "the retained request digest is what refuses a duplicate write");
      assertEquals(binding.authorizationScopeDigest(), stored.authorizationScopeDigest());
      assertTrue(closed.resultRetired(), "the declared result is retired with its window");
      assertTrue(store.read(SystemRecordType.RESULT, resultRow, scope).join().isEmpty(),
          "a retired result must no longer be queryable");
      assertTrue(store.readTombstone(SystemRecordType.RESULT, resultRow, scope).join().isPresent(),
          "the result keeps a tombstone so it cannot be recreated");

      var again = assertInstanceOf(InvocationWindowCloser.Retained.class,
          closer.close(identity, permit, scope).join());
      assertEquals(InvocationWindowCloser.Skip.ALREADY_CLOSED, again.skip());
    }
  }

  @Test
  void anUnresolvedInvocationIsRetainedRatherThanClosed() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var store = open(environment, scope);
      var invocations = new InvocationService(new HBaseInvocationRepository(store));
      var preparing = proposedInvocation();
      invocations.bindOrJoin(preparing, scope).join();
      var permit = SystemRecordRetirement.Permit.issue(store.profileFingerprint(),
          ByteKey.copyOf(new byte[32]), InvocationId.create(), 1, 16, 16, scope);
      var closer = new InvocationWindowCloser(store, invocations, store);

      var identity = ByteKey.copyOf(preparing.invocationId().bytes());
      var outcome = assertInstanceOf(InvocationWindowCloser.Retained.class,
          closer.close(identity, permit, scope).join());
      assertEquals(InvocationWindowCloser.Skip.STILL_PREPARING, outcome.skip());
      var stored = InvocationMessages.decode(store.read(SystemRecordType.INVOCATION, identity,
          scope).join().orElseThrow().bytes());
      assertEquals(InvocationBinding.State.PREPARING, stored.state(),
          "an unresolved invocation keeps its window open");
      assertTrue(stored.requestDigest().equals(preparing.requestDigest()),
          "an unresolved invocation keeps its original binding untouched");
    }
  }

  private static InvocationBinding selectedInvocation(InvocationService invocations,
      RequestScope scope) {
    var prepared = proposedInvocation();
    var bound = invocations.bindOrJoin(prepared, scope).join();
    var sealed = invocations.transition(bound, bound.seal(ByteKey.copyOf(new byte[] {1, 1})), scope)
        .join();
    var resultIdentity = ResultIdentity.from(sealed);
    var directory = ResultDirectory.create(resultIdentity,
        java.util.List.of(new ResultPageReference(resultIdentity, 0, 8, 1,
            ByteKey.copyOf(new byte[32]))));
    return invocations.transition(sealed, sealed.selected(directory.reference().token()), scope)
        .join();
  }

  private static InvocationBinding proposedInvocation() {
    return InvocationBinding.begin(InvocationId.create(), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 1, 2);
  }

  private static HBaseSystemRecordStore open(M1TestEnvironment environment, RequestScope scope)
      throws Exception {
    return HBaseSystemRecordStore.open(environment.factory(), environment.identity(),
        createSystemTable(environment), scope).join();
  }

  private static SystemTableBinding createSystemTable(M1TestEnvironment environment)
      throws Exception {
    String suffix = UUID.randomUUID().toString().replace("-", "");
    String group = "j52-window-" + suffix;
    String saltProfileId = "j52-window-salt-" + suffix;
    TableName table = TableName.valueOf("j52_window_" + suffix);
    SystemTableBinding binding = new SystemTableBinding(table, FAMILY, group, saltProfileId, SALT);
    var descriptor = TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", group)
        .setValue("etcdhbase.m0.salt-profile-id", saltProfileId)
        .setValue("etcdhbase.m0.salt-strategy-id", SALT.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version",
            Integer.toString(SALT.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count", Integer.toString(SALT.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes", Integer.toString(SALT.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories", RECORD_CATEGORIES)
        .setValue("etcdhbase.m4.auxiliary-format", "1")
        .build();
    environment.create(table, descriptor, SALT.splitKeys().toArray(byte[][]::new));
    return binding;
  }
}
