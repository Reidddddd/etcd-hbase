package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.EventPage;
import io.etcdhbase.core.commit.ManifestPage;
import io.etcdhbase.core.commit.ManifestCandidateReference;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.PreparedEventReference;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.ResultDirectory;
import io.etcdhbase.core.commit.ResultPage;
import io.etcdhbase.core.commit.ResultPageReference;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.codec.ResultMessages;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionException;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.SystemRecordClosure;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.CommitDecisionMessages;
import io.etcdhbase.hbase.codec.ManifestMessages;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/**
 * The closure must be conservative: a manifest that a surviving decision still references is
 * examined but never retired, and only an unreferenced manifest is actually collected.
 */
class HBaseSystemRecordClosureIT {
  private static final byte[] FAMILY = {'s'};
  private static final int SALT_BUCKETS = 1;
  private static final HashSaltStrategy SALT = new HashSaltStrategy(
      new io.etcdhbase.spi.salt.SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, SALT_BUCKETS, 2));
  private static final String RECORD_CATEGORIES = String.join(",",
      "CONTROL", "INVOCATION", "CANDIDATE", "MANIFEST", "DECISION", "SNAPSHOT_PIN",
      "RESULT", "TABLE_GROUP_IDENTITY", "WORKSET", "LEASE_STATE", "LEASE_ENUMERATION",
      "EXPIRY_CANDIDATE", "AUXILIARY", "LEASE_RESULT", "LEASE_BINDING",
      "LEASE_EXPIRY_CHECKPOINT");
  private static final ByteKey AUTHORIZATION_DIGEST = ByteKey.copyOf(new byte[32]);

  @Test
  void keepsAReferencedManifestAndCollectsAnUnreferencedOne() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      SystemTableBinding binding = createSystemTable(environment);
      HBaseSystemRecordStore store = HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, scope).join();
      var permit = SystemRecordRetirement.Permit.issue(store.profileFingerprint(),
          AUTHORIZATION_DIGEST, InvocationId.create(), 1, 16, 16, scope);

      var referenced = manifest(1);
      var orphan = manifest(2);
      var referencedIdentity = referenced.systemIdentity();
      var orphanIdentity = orphan.systemIdentity();
      create(store, SystemRecordType.MANIFEST, referencedIdentity, referenced, scope);
      create(store, SystemRecordType.MANIFEST, orphanIdentity, orphan, scope);

      var decision = new CommitDecision(TransactionId.create(), InvocationId.create(), 1, 1, 2,
          referencedIdentity, ByteKey.copyOf(new byte[32]), true);
      create(store, SystemRecordType.DECISION, ByteKey.copyOf(ByteBuffer.allocate(Long.BYTES).putLong(decision.sequence()).array()),
          CommitDecisionMessages.encode(decision), scope);

      var closure = new HBaseSystemRecordClosure(store, store, store.profileFingerprint(),
          SALT_BUCKETS);
      var page = closure.collect(SystemRecordClosure.Domain.MANIFEST, Optional.empty(), 16, permit,
          scope).join();

      assertEquals(2, page.examined(), "both manifests are examined");
      assertEquals(1, page.retired(), "only the unreferenced manifest is collected");
      assertTrue(store.read(SystemRecordType.MANIFEST, referencedIdentity, scope).join().isPresent(),
          "a still-referenced manifest must survive");
      assertTrue(store.read(SystemRecordType.MANIFEST, orphanIdentity, scope).join().isEmpty(),
          "the unreferenced manifest is actually retired");
      assertTrue(store.readTombstone(SystemRecordType.MANIFEST, orphanIdentity, scope)
          .join().isPresent(), "the retired manifest keeps its tombstone");
    }
  }

  @Test
  void undecodableReferenceRetainsDataInsteadOfCollectingIt() throws Exception {
    try (M1TestEnvironment environment = M1TestEnvironment.start(1);
        RequestScope scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      SystemTableBinding binding = createSystemTable(environment);
      HBaseSystemRecordStore store = HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, scope).join();
      var permit = SystemRecordRetirement.Permit.issue(store.profileFingerprint(),
          AUTHORIZATION_DIGEST, InvocationId.create(), 1, 16, 16, scope);

      var root = manifest(3);
      var target = root.systemIdentity();
      create(store, SystemRecordType.MANIFEST, target, root, scope);
      // A decision that cannot be decoded might be the reference; it must keep the manifest alive.
      create(store, SystemRecordType.DECISION, ByteKey.copyOf(new byte[] {6, 6}),
          new byte[] {9, 9, 9}, scope);

      var closure = new HBaseSystemRecordClosure(store, store, store.profileFingerprint(),
          SALT_BUCKETS);
      assertThrows(CompletionException.class, () -> closure.collect(SystemRecordClosure.Domain.MANIFEST,
          Optional.empty(), 16, permit, scope).join(), "a corrupt proof must not publish a checkpoint");
      assertTrue(store.read(SystemRecordType.MANIFEST, target, scope).join().isPresent());
    }
  }

  @Test
  void manifestPagesAndRootsProtectRealCandidateEventAndResultRecords() throws Exception {
    try (var environment = M1TestEnvironment.start(1);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      var store = HBaseSystemRecordStore.open(environment.factory(), environment.identity(),
          createSystemTable(environment), scope).join();
      var transaction = TransactionId.create();
      var record = StoredRecord.live(ByteKey.copyOf(new byte[] {7}),
          new StorageMetadata(2, 2, 1, 0, StorageMetadata.State.LIVE, 1), new byte[] {8});
      var candidate = new PreparedCandidate(transaction, 1, 0, 1, record);
      var candidates = ManifestPage.create(transaction, 0, List.of(ManifestCandidateReference.from(candidate,
          store.bucket(SystemRecordType.CANDIDATE, candidate.systemIdentity()))));
      var event = new PreparedEvent(transaction, 1, 0, 1, 0, PreparedEvent.Kind.PUT, record, Optional.empty());
      var events = EventPage.create(transaction, 0, List.of(PreparedEventReference.from(event)));
      var resultIdentity = new ResultIdentity(InvocationId.create(), transaction, AUTHORIZATION_DIGEST,
          AUTHORIZATION_DIGEST, 1, 0, 1);
      var resultPage = new ResultPage(resultIdentity, 0, new byte[] {1});
      var result = ResultDirectory.create(resultIdentity, List.of(ResultPageReference.from(resultPage)));
      var root = ManifestRoot.createWithResult(transaction, 1, 0, 1, 1, 1, Optional.of(candidates.reference()),
          1, events.eventBytes(), Optional.of(events.reference()), result.reference());
      create(store, SystemRecordType.CANDIDATE, candidate.systemIdentity(), ManifestMessages.encode(candidate), scope);
      create(store, SystemRecordType.EVENT, event.systemIdentity(), ManifestMessages.encode(event), scope);
      create(store, SystemRecordType.RESULT, result.systemIdentity(), ResultMessages.encode(result), scope);
      create(store, SystemRecordType.RESULT, resultPage.systemIdentity(), ResultMessages.encode(resultPage), scope);
      create(store, SystemRecordType.MANIFEST, candidates.systemIdentity(), ManifestMessages.encode(candidates), scope);
      create(store, SystemRecordType.MANIFEST, events.systemIdentity(), ManifestMessages.encode(events), scope);
      create(store, SystemRecordType.MANIFEST, root.systemIdentity(), root, scope);
      var closure = new HBaseSystemRecordClosure(store, store, store.profileFingerprint(), SALT_BUCKETS);
      var permit = SystemRecordRetirement.Permit.issue(store.profileFingerprint(), AUTHORIZATION_DIGEST,
          InvocationId.create(), 1, 16, 16, scope);
      for (var domain : List.of(SystemRecordClosure.Domain.CANDIDATE, SystemRecordClosure.Domain.EVENT,
          SystemRecordClosure.Domain.RESULT)) {
        var page = closure.collect(domain, Optional.empty(), 16, permit, scope).join();
        assertEquals(0, page.retired(), "durable references protect " + domain);
        assertTrue(page.examined() > 0);
      }
    }
  }

  private static ManifestRoot manifest(long sequence) {
    return ManifestRoot.create(TransactionId.create(), 2, sequence, sequence + 1, 0, 0,
        Optional.empty());
  }

  private static void create(SystemRecordStore store, SystemRecordType type, ByteKey identity,
      ManifestRoot root, RequestScope scope) {
    create(store, type, identity, ManifestMessages.encode(root), scope);
  }

  private static void create(SystemRecordStore store, SystemRecordType type, ByteKey identity,
      byte[] value, RequestScope scope) {
    assertEquals(SystemRecordStore.CreateResult.CREATED,
        store.createIfAbsent(type, identity, ByteKey.copyOf(value), scope).join());
  }

  private static SystemTableBinding createSystemTable(M1TestEnvironment environment)
      throws Exception {
    String suffix = UUID.randomUUID().toString().replace("-", "");
    String group = "j52-closure-" + suffix;
    String saltProfileId = "j52-closure-salt-" + suffix;
    HashSaltStrategy salt = SALT;
    TableName table = TableName.valueOf("j52_closure_" + suffix);
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
        .setValue("etcdhbase.m4.auxiliary-format", "3")
        .build();
    environment.create(table, binding.withEventRevisionIndexCapability(descriptor), salt.splitKeys().toArray(byte[][]::new));
    return binding;
  }
}
