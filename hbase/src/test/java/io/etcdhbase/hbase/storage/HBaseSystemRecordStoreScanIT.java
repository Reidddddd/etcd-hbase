package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.SystemRecordEntry;
import io.etcdhbase.core.commit.SystemRecordPage;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.LeaseBindingIdentity;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/** MiniCluster coverage for ordered, bounded M4 system-record scans. */
class HBaseSystemRecordStoreScanIT {
  private static final byte[] FAMILY = {'s'};
  private static final String RECORD_CATEGORIES = String.join(",",
      "CONTROL", "INVOCATION", "CANDIDATE", "MANIFEST", "DECISION", "SNAPSHOT_PIN",
      "RESULT", "TABLE_GROUP_IDENTITY", "WORKSET", "LEASE_STATE", "LEASE_ENUMERATION",
      "EXPIRY_CANDIDATE", "AUXILIARY", "LEASE_RESULT", "LEASE_BINDING");

  @Test
  void scansOneBucketInBoundedExclusiveResumePages() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      SystemTableBinding binding = createLeaseCapableSystemTable(environment);
      SystemRecordStore records = HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, scope).join();
      List<ByteKey> identities = identitiesInOneBucket(records, 5);
      int bucket = records.bucket(SystemRecordType.WORKSET, identities.getFirst());

      for (int index = 0; index < identities.size(); index++) {
        assertEquals(SystemRecordStore.CreateResult.CREATED,
            records.createIfAbsent(SystemRecordType.WORKSET, identities.get(index),
                ByteKey.copyOf(new byte[] {(byte) index, 7, 9, 11}), scope).join());
      }

      SystemRecordPage first = records.scanBucket(SystemRecordType.WORKSET, bucket,
          java.util.Optional.empty(), 2, 100, scope).join();
      assertEquals(2, first.entries().size());
      assertFalse(first.exhausted());
      assertEquals(24, first.encodedBytes());

      SystemRecordPage second = records.scanBucket(SystemRecordType.WORKSET, bucket,
          first.lastIdentity(), 2, 100, scope).join();
      assertEquals(2, second.entries().size());
      assertFalse(second.exhausted());
      assertTrue(first.lastIdentity().orElseThrow().compareTo(
          second.entries().getFirst().identity()) < 0);

      SystemRecordPage last = records.scanBucket(SystemRecordType.WORKSET, bucket,
          second.lastIdentity(), 2, 100, scope).join();
      assertEquals(1, last.entries().size());
      assertTrue(last.exhausted());
      assertEquals(12, last.encodedBytes());
    }
  }

  @Test
  void stopsBeforeCrossingTheByteBudgetAndResumesAfterTheLastIncludedRecord() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      SystemTableBinding binding = createLeaseCapableSystemTable(environment);
      SystemRecordStore records = HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, scope).join();
      List<ByteKey> identities = identitiesInOneBucket(records, 3);
      int bucket = records.bucket(SystemRecordType.WORKSET, identities.getFirst());
      for (int index = 0; index < identities.size(); index++) {
        assertEquals(SystemRecordStore.CreateResult.CREATED,
            records.createIfAbsent(SystemRecordType.WORKSET, identities.get(index),
                ByteKey.copyOf(new byte[] {(byte) index, 7, 9, 11}), scope).join());
      }

      SystemRecordPage first = records.scanBucket(SystemRecordType.WORKSET, bucket,
          java.util.Optional.empty(), 5, 20, scope).join();
      assertEquals(List.of(identities.getFirst()),
          first.entries().stream().map(SystemRecordEntry::identity).toList());
      assertEquals(12, first.encodedBytes());
      assertFalse(first.exhausted());

      SystemRecordPage second = records.scanBucket(SystemRecordType.WORKSET, bucket,
          first.lastIdentity(), 5, 20, scope).join();
      assertEquals(List.of(identities.get(1)),
          second.entries().stream().map(SystemRecordEntry::identity).toList());
      assertEquals(12, second.encodedBytes());
      assertFalse(second.exhausted());

      SystemRecordPage last = records.scanBucket(SystemRecordType.WORKSET, bucket,
          second.lastIdentity(), 5, 20, scope).join();
      assertEquals(List.of(identities.get(2)),
          last.entries().stream().map(SystemRecordEntry::identity).toList());
      assertTrue(last.exhausted());
    }
  }

  @Test
  void rejectsPagesThatExceedTheirByteBudgetAndSeeksVersionsAtTheBarrier() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      SystemTableBinding binding = createLeaseCapableSystemTable(environment);
      SystemRecordStore records = HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, scope).join();
      ByteKey identity = identitiesInOneBucket(records, 1).getFirst();
      int bucket = records.bucket(SystemRecordType.WORKSET, identity);
      records.createIfAbsent(SystemRecordType.WORKSET, identity,
          ByteKey.copyOf(new byte[] {1, 2, 3, 4}), scope).join();

      CompletionException budgetFailure = assertThrows(CompletionException.class,
          () -> records.scanBucket(SystemRecordType.WORKSET, bucket,
              java.util.Optional.empty(), 1, 11, scope).join());
      assertTrue(rootCause(budgetFailure) instanceof BackendFailure);
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
          ((BackendFailure) rootCause(budgetFailure)).category());

      ByteKey earlier = LeaseRecordIdentity.stateVersion(-7, 11);
      ByteKey later = LeaseRecordIdentity.stateVersion(-7, 29);
      records.createIfAbsent(SystemRecordType.LEASE_STATE, earlier,
          ByteKey.copyOf(new byte[] {11}), scope).join();
      records.createIfAbsent(SystemRecordType.LEASE_STATE, later,
          ByteKey.copyOf(new byte[] {29}), scope).join();

      SystemRecordEntry atBarrier = records.latestAtOrBefore(SystemRecordType.LEASE_STATE,
          LeaseRecordIdentity.routePrefix(-7), 19, scope).join().orElseThrow();
      assertEquals(earlier, atBarrier.identity());
      assertEquals(ByteKey.copyOf(new byte[] {11}), atBarrier.value());
      assertTrue(records.latestAtOrBefore(SystemRecordType.LEASE_STATE,
          LeaseRecordIdentity.routePrefix(-7), 5, scope).join().isEmpty());
    }
  }

  @Test
  void seeksOneKeyBindingHistoryWithoutCrossingIntoNeighborKeys() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      SystemTableBinding binding = createLeaseCapableSystemTable(environment, true);
      SystemRecordStore records = HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, scope).join();
      long leaseId = -17;
      long incarnation = 2;
      ByteKey key = ByteKey.copyOf(new byte[] {'r', (byte) 0xff});
      ByteKey neighboringKey = ByteKey.copyOf(new byte[] {'r', (byte) 0xff, 0});

      for (long sequence : new long[] {3, 8, 12}) {
        ByteKey identity = new LeaseBindingIdentity(leaseId, incarnation, key, sequence).encode();
        assertEquals(SystemRecordStore.CreateResult.CREATED,
            records.createIfAbsent(SystemRecordType.LEASE_BINDING, identity,
                ByteKey.copyOf(ByteBuffer.allocate(Long.BYTES).putLong(sequence).array()), scope).join());
      }
      ByteKey neighboring = new LeaseBindingIdentity(leaseId, incarnation, neighboringKey, 15).encode();
      assertEquals(SystemRecordStore.CreateResult.CREATED,
          records.createIfAbsent(SystemRecordType.LEASE_BINDING, neighboring,
              ByteKey.copyOf(new byte[] {99}), scope).join());

      SystemRecordEntry atTen = records.latestAtOrBefore(SystemRecordType.LEASE_BINDING,
          LeaseBindingIdentity.keyRoutePrefix(leaseId, incarnation, key), 10, scope).join().orElseThrow();
      assertEquals(new LeaseBindingIdentity(leaseId, incarnation, key, 8).encode(), atTen.identity());
      assertEquals(ByteKey.copyOf(ByteBuffer.allocate(Long.BYTES).putLong(8).array()), atTen.value());

      SystemRecordEntry afterHistory = records.latestAtOrBefore(SystemRecordType.LEASE_BINDING,
          LeaseBindingIdentity.keyRoutePrefix(leaseId, incarnation, key), 100, scope).join().orElseThrow();
      assertEquals(new LeaseBindingIdentity(leaseId, incarnation, key, 12).encode(), afterHistory.identity());
      assertTrue(records.latestAtOrBefore(SystemRecordType.LEASE_BINDING,
          LeaseBindingIdentity.keyRoutePrefix(leaseId, incarnation, key), 2, scope).join().isEmpty());
    }
  }

  @Test
  void rejectsBindingWritesUntilTheCapabilityIsPersistedAndTheStoreIsReopened() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      SystemTableBinding binding = createLeaseCapableSystemTable(environment);
      SystemRecordStore oldSnapshot = HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, scope).join();
      ByteKey identity = new LeaseBindingIdentity(17, 1,
          ByteKey.copyOf(new byte[] {'r', 1}), 1).encode();
      ByteKey value = ByteKey.copyOf(new byte[] {1});

      assertThrows(IllegalStateException.class, () -> oldSnapshot.createIfAbsent(
          SystemRecordType.LEASE_BINDING, identity, value, scope));

      var admin = environment.connection().getAdmin();
      var oldDescriptor = admin.getDescriptor(binding.table()).join();
      admin.modifyTable(binding.withLeaseBindingCapability(oldDescriptor)).join();

      assertThrows(IllegalStateException.class, () -> oldSnapshot.createIfAbsent(
          SystemRecordType.LEASE_BINDING, identity, value, scope),
          "an opened J40 record store must retain its conservative capability snapshot");
      SystemRecordStore reopened = HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, scope).join();
      assertTrue(binding.supportsLeaseBindings(admin.getDescriptor(binding.table()).join()));
      assertEquals(SystemRecordStore.CreateResult.CREATED, reopened.createIfAbsent(
          SystemRecordType.LEASE_BINDING, identity, value, scope).join());
    }
  }

  private static SystemTableBinding createLeaseCapableSystemTable(
      M1TestEnvironment environment) throws Exception {
    return createLeaseCapableSystemTable(environment, false);
  }

  private static SystemTableBinding createLeaseCapableSystemTable(
      M1TestEnvironment environment, boolean leaseBindings) throws Exception {
    String suffix = UUID.randomUUID().toString().replace("-", "");
    String group = "j40-scan-" + suffix;
    String saltProfileId = "j40-salt-" + suffix;
    HashSaltStrategy salt = new HashSaltStrategy();
    TableName table = TableName.valueOf("j40_system_" + suffix);
    SystemTableBinding binding = new SystemTableBinding(table, FAMILY, group, saltProfileId, salt);
    var descriptor = TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", group)
        .setValue("etcdhbase.m0.salt-profile-id", saltProfileId)
        .setValue("etcdhbase.m0.salt-strategy-id", salt.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version",
            Integer.toString(salt.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count", Integer.toString(salt.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes", Integer.toString(salt.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories", RECORD_CATEGORIES)
        .setValue("etcdhbase.m4.auxiliary-format", leaseBindings ? "2" : "1")
        .build();
    environment.create(table, descriptor, salt.splitKeys().toArray(byte[][]::new));
    return binding;
  }

  private static List<ByteKey> identitiesInOneBucket(SystemRecordStore records, int count) {
    var identities = new ArrayList<ByteKey>(count);
    int bucket = -1;
    for (long candidate = 1; identities.size() < count; candidate++) {
      ByteKey identity = ByteKey.copyOf(ByteBuffer.allocate(Long.BYTES).putLong(candidate).array());
      int candidateBucket = records.bucket(SystemRecordType.WORKSET, identity);
      if (bucket == -1) bucket = candidateBucket;
      if (candidateBucket == bucket) identities.add(identity);
    }
    return List.copyOf(identities);
  }

  private static Throwable rootCause(Throwable failure) {
    Throwable current = failure;
    while (current.getCause() != null) current = current.getCause();
    return current;
  }
}
