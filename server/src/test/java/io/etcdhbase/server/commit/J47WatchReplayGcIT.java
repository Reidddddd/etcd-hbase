package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.EventRevisionIndex;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventHistoryRetention;
import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.EventIndexBackfillReader;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.SelectedSequenceReader;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordRead;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.hbase.storage.HBaseSystemRecordStore;
import io.etcdhbase.hbase.storage.SystemTableBinding;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;
import io.etcdhbase.hbase.storage.HBaseCommitDecisionRepository;
import io.etcdhbase.hbase.storage.HBaseEventRevisionIndexRepository;
import io.etcdhbase.hbase.storage.HBaseManifestRepository;

/** Exercises replay-owned durable protection against a real MiniCluster event collector. */
class J47WatchReplayGcIT {
  private static final String TABLE_GROUP = "j47-watch-pin";
  private static final ByteKey AUTHORIZATION = ByteKey.copyOf(new byte[32]);

  @Test
  void watchReplayPinPreventsPhysicalEventCollectionUntilReplaySettles() throws Exception {
    byte[] family = {'s'};
    var salt = new HashSaltStrategy();
    var table = TableName.valueOf("j47_pin_" + UUID.randomUUID().toString().replace("-", ""));
    var binding = new SystemTableBinding(table, family, TABLE_GROUP, "j47-salt", salt);
    var descriptor = TableDescriptorBuilder.newBuilder(table)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(family)
            .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
        .setDurability(Durability.SYNC_WAL)
        .setValue("etcdhbase.m0.table-group-id", TABLE_GROUP)
        .setValue("etcdhbase.m0.salt-profile-id", "j47-salt")
        .setValue("etcdhbase.m0.salt-strategy-id", salt.profile().strategyId())
        .setValue("etcdhbase.m0.salt-encoding-version", Integer.toString(salt.profile().encodingVersion()))
        .setValue("etcdhbase.m0.salt-bucket-count", Integer.toString(salt.profile().bucketCount()))
        .setValue("etcdhbase.m0.salt-width-bytes", Integer.toString(salt.profile().saltWidthBytes()))
        .setValue("etcdhbase.m0.record-categories",
            "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION,SNAPSHOT_PIN,RESULT")
        .build();
    descriptor = binding.withEventRevisionIndexCapability(binding.withLeaseExpiryCheckpointCapability(
        binding.withLeaseBindingCapability(binding.withAuxiliaryCapability(descriptor))));

    try (var environment = MiniClusterFixture.start(table, descriptor, salt.splitKeys().toArray(byte[][]::new));
         var setupScope = RequestScope.withTimeout(Duration.ofMinutes(3))) {
      SystemRecordStore records = HBaseSystemRecordStore.open(
          environment.factory(), environment.identity(), binding, setupScope).join();
      var pinConfiguration = new SnapshotPinConfiguration(1, 2, Duration.ofMinutes(1));
      var eventPins = EventHistoryPinManager.initializeFresh(
          records, pinConfiguration, Clock.systemUTC(), setupScope).join();
      var viewPins = SnapshotPinManager.initializeFresh(records, pinConfiguration, setupScope).join();
      var state = new AtomicReference<>(controlState(binding, 7, 7, 6, 0));
      var control = controlStore(state);
      var manifestRepository = new HBaseManifestRepository(records);
      var manifests = new ManifestCatalog(manifestRepository);
      var views = new ReadViewService(control, manifests, viewPins, binding.profileFingerprint());
      var retention = new EventHistoryRetention(eventPins, views, new EventIndexBackfillReader(records, manifests));
      byte[] eventValue = "watch-event".getBytes(StandardCharsets.UTF_8);
      var event = event(5, 4, 4, "j47-event-key", eventValue);
      var eventRoot = prepareEvent(manifestRepository, event, setupScope);
      var eventDecision = decision(event, eventRoot, 5);
      assertSelectedManifest(eventDecision, eventRoot);
      var decisions = new HBaseCommitDecisionRepository(records);
      decisions.createIfAbsent(eventDecision, setupScope).join();
      var eventIndexes = new HBaseEventRevisionIndexRepository(records);
      eventIndexes.createIfAbsent(EventRevisionIndex.fromSelected(eventDecision, eventRoot), setupScope).join();

      byte[] nextValue = "next-watch-event".getBytes(StandardCharsets.UTF_8);
      var nextEvent = event(6, 5, 5, "j47-next-event-key", nextValue);
      var nextRoot = prepareEvent(manifestRepository, nextEvent, setupScope);
      var nextDecision = decision(nextEvent, nextRoot, 6);
      assertSelectedManifest(nextDecision, nextRoot);
      decisions.createIfAbsent(nextDecision, setupScope).join();
      eventIndexes.createIfAbsent(EventRevisionIndex.fromSelected(nextDecision, nextRoot), setupScope).join();
      ByteKey eventIdentity = event.systemIdentity();
      writeBackfillCheckpoint(records, eventPins.registryIdentity(), true, setupScope);

      var budgets = replayBudgets();
      WatchHistoryReplay replay;
      try (var watchScope = RequestScope.withoutDeadline();
           BudgetManager.Account watchAccount = budgets.admit(TABLE_GROUP, watchScope,
               new BudgetManager.Reservation(0, 1, 1, 1024, 0))) {
        CountDownLatch eventReadStarted = new CountDownLatch(1);
        var replayWakeups = new LinkedBlockingQueue<Boolean>();
        CompletableFuture<Void> permitEventRead = new CompletableFuture<>();
        var gatedRecords = gateEventRead(records, eventIdentity, eventReadStarted, permitEventRead);
        var selected = new SelectedSequenceReader(eventPins, control,
            new HBaseCommitDecisionRepository(gatedRecords),
            new HBaseManifestRepository(gatedRecords),
            new HBaseEventRevisionIndexRepository(gatedRecords));
        var opened = WatchHistoryReplay.open(TABLE_GROUP, KeyRange.all(), 5,
            new ControlSnapshot(state.get(), new byte[] {1}), 7, AUTHORIZATION,
            eventPins, selected, budgets, watchAccount).join();
        replay = opened.replay().orElseThrow();
        replay.setWakeup(() -> replayWakeups.offer(Boolean.TRUE));
        AssertionError testAssertion = null;
        try {
          assertTrue(replay.poll().isEmpty(), "the first poll schedules the bounded historical read");
          assertTrue(eventReadStarted.await(30, TimeUnit.SECONDS),
              "the selected HBase event cursor reaches the deterministic read gate");
          assertFalse(collectIfSafe(environment, binding, retention, eventIdentity, 5));
          assertTrue(records.read(SystemRecordType.EVENT, eventIdentity, setupScope).join().isPresent(),
              "the event needed by replay remains physically present while its replay pin is active");
          state.set(controlState(binding, 8, 7, 6, 6));
          permitEventRead.complete(null);
          assertTrue(replayWakeups.poll(30, TimeUnit.SECONDS) != null,
              "the selected revision read finishes while replay protection remains active");
          WatchStream.Delivery delivery = replay.poll().orElseThrow(
              () -> new AssertionError("historical replay did not deliver the pinned event"));
          try {
            assertEquals(event.current().key(), delivery.event().current().key());
            assertEquals(event.current().value().orElseThrow().length,
                delivery.event().current().value().orElseThrow().length);
            assertEquals(5, delivery.revision());
          } finally {
            delivery.close();
          }
          assertTrue(replay.poll().isEmpty(), "the next revision starts after the committed event is delivered");
          assertTrue(replayWakeups.poll(30, TimeUnit.SECONDS) != null,
              "the next revision at the compact boundary completes its selected read");
          Optional<WatchStream.Delivery> nextDeliveryResult = replay.poll();
          assertTrue(nextDeliveryResult.isPresent(),
              "the revision equal to compactRevision remains deliverable; termination="
                  + replay.termination().map(WatchTermination::kind).orElse(null)
                  + ", failure=" + replay.termination()
                      .map(J47WatchReplayGcIT::safeFailureDiagnostic)
                      .orElse(null)
                  + ", replayComplete=" + replay.complete());
          WatchStream.Delivery nextDelivery = nextDeliveryResult.orElseThrow();
          try {
            assertEquals(nextEvent.current().key(), nextDelivery.event().current().key());
            assertEquals(6, nextDelivery.revision());
          } finally {
            nextDelivery.close();
          }
          assertTrue(replay.poll().isEmpty(), "the captured replay barrier is complete");
          assertTrue(replay.complete());
          assertTrue(replay.termination().isEmpty(),
              "compaction at the next revision does not cancel already selected history");
          replay.settled().toCompletableFuture().get(30, TimeUnit.SECONDS);
        } catch (AssertionError failure) {
          testAssertion = failure;
          throw failure;
        } finally {
          permitEventRead.complete(null);
          replay.close();
          try {
            replay.settled().toCompletableFuture().get(30, TimeUnit.SECONDS);
          } catch (java.util.concurrent.ExecutionException settlementFailure) {
            boolean terminalSettlement = replay.termination().isPresent()
                && settlementFailure.getCause() instanceof io.etcdhbase.core.error.BackendFailure backendFailure
                && backendFailure.category() == io.etcdhbase.core.error.BackendFailure.Category.UNAVAILABLE
                && backendFailure.getSuppressed().length == 0;
            if (testAssertion == null || !terminalSettlement) throw settlementFailure;
            Throwable terminationFailure = replay.termination()
                .map(WatchTermination::failure).orElse(null);
            if (terminationFailure != null) testAssertion.addSuppressed(terminationFailure);
          }
        }
        var released = eventPins.readBoundary(setupScope).join();
        assertFalse(released.protections().stream().anyMatch(protection -> protection.requiredSequence() == 5),
            "settled replay releases its durable event-history pins");

        assertTrue(collectIfSafe(environment, binding, retention, eventIdentity, 5));
        assertTrue(records.read(SystemRecordType.EVENT, eventIdentity, setupScope).join().isEmpty(),
            "the controlled collector physically removes the event after replay releases protection");
      }
    }
  }

  private static String safeFailureDiagnostic(WatchTermination termination) {
    Throwable failure = termination.failure();
    if (failure == null) return "none";
    StringBuilder diagnostic = new StringBuilder(failure.getClass().getSimpleName());
    if (failure instanceof BackendFailure backendFailure) {
      diagnostic.append("[category=").append(backendFailure.category())
          .append(", message=").append(backendFailure.safeMessage()).append(']');
    }
    Throwable cause = failure.getCause();
    if (cause != null && cause != failure) {
      diagnostic.append(" causedBy=").append(cause.getClass().getSimpleName());
    }
    return diagnostic.toString();
  }

  private static PreparedEvent event(long sequence, long baseSequence, long baseRevision,
      String key, byte[] value) {
    long revision = baseRevision + 1;
    return new PreparedEvent(new TransactionId(new UUID(1, sequence)), 1, baseSequence,
        baseRevision, 0, PreparedEvent.Kind.PUT,
        StoredRecord.live(ByteKey.copyOf(key.getBytes(StandardCharsets.UTF_8)),
            new StorageMetadata(1, revision, 1, 0, StorageMetadata.State.LIVE, value.length), value),
        Optional.empty());
  }

  private static io.etcdhbase.core.commit.ManifestRoot prepareEvent(
      HBaseManifestRepository manifests, PreparedEvent event, RequestScope scope) {
    PreparedCandidate candidate = new PreparedCandidate(event.transactionId(), event.ownerEpoch(),
        event.baseSequence(), event.baseRevision(), event.current());
    return new ManifestPreparation(manifests,
        new ManifestPreparation.Limits(1, 1L << 20, 1, 1L << 20))
        .prepare(event.transactionId(), event.ownerEpoch(), event.baseSequence(), event.baseRevision(),
            Collections.singleton(candidate).iterator(), java.util.List.of(event).iterator(), scope)
        .join();
  }

  private static CommitDecision decision(PreparedEvent event,
      io.etcdhbase.core.commit.ManifestRoot root, long sequence) {
    long baseRevision = root.baseRevision();
    return new CommitDecision(event.transactionId(), new InvocationId(new UUID(2, sequence)),
        sequence, baseRevision, baseRevision + 1, root.systemIdentity(), root.digest(), true);
  }

  private static void assertSelectedManifest(CommitDecision decision,
      io.etcdhbase.core.commit.ManifestRoot root) {
    assertTrue(root.containsKeyChanges(), "the sealed fixture manifest must contain its candidate");
    assertTrue(decision.containsKeyChanges(), "the fixture decision must select a KV-changing commit");
    assertEquals(root.transactionId(), decision.transactionId(), "decision and manifest transaction IDs");
    assertEquals(root.baseSequence() + 1, decision.sequence(), "decision sequence follows manifest base");
    assertEquals(root.baseRevision(), decision.baseRevision(), "decision and manifest base revisions");
    assertEquals(root.systemIdentity(), decision.manifestRoot(), "decision selects this manifest root");
    assertEquals(root.digest(), decision.manifestDigest(), "decision authenticates this manifest digest");
  }

  private static CommitControlStore controlStore(AtomicReference<CommitState> state) {
    return (CommitControlStore) Proxy.newProxyInstance(CommitControlStore.class.getClassLoader(),
        new Class<?>[] {CommitControlStore.class}, (proxy, method, arguments) -> {
          ControlSnapshot snapshot = new ControlSnapshot(state.get(), new byte[] {1});
          if (method.getName().equals("read")) {
            return CompletableFuture.completedFuture(Optional.of(snapshot));
          }
          if (method.getName().equals("readWithSize")) {
            return CompletableFuture.completedFuture(
                new SystemRecordRead<>(Optional.of(snapshot), 1));
          }
          throw new UnsupportedOperationException(method.getName());
        });
  }

  /** Delays one selected event read while retaining a test producer until the gate is released. */
  private static SystemRecordStore gateEventRead(SystemRecordStore delegate, ByteKey eventIdentity,
      CountDownLatch eventReadStarted, CompletableFuture<Void> permitEventRead) {
    return (SystemRecordStore) Proxy.newProxyInstance(SystemRecordStore.class.getClassLoader(),
        new Class<?>[] {SystemRecordStore.class}, (proxy, method, arguments) -> {
          if (!method.getName().equals("read") || arguments == null || arguments.length != 3
              || arguments[0] != SystemRecordType.EVENT || !eventIdentity.equals(arguments[1])) {
            return method.invoke(delegate, arguments);
          }
          RequestScope scope = (RequestScope) arguments[2];
          CompletableFuture<Void> producer = new CompletableFuture<>();
          scope.trackPhysicalProducer(producer);
          eventReadStarted.countDown();
          CompletableFuture<Object> gatedRead = permitEventRead.thenCompose(ignored ->
              invoke(delegate, method, arguments));
          gatedRead.whenComplete((ignored, failure) -> {
            if (failure == null) producer.complete(null);
            else producer.completeExceptionally(failure);
          });
          return gatedRead;
        });
  }

  private static CompletableFuture<Object> invoke(Object target, Method method, Object[] arguments) {
    try {
      Object result = method.invoke(target, arguments);
      if (result instanceof CompletionStage<?> stage) {
        return stage.thenApply(value -> (Object) value).toCompletableFuture();
      }
      return CompletableFuture.completedFuture(result);
    } catch (InvocationTargetException failure) {
      return CompletableFuture.failedFuture(failure.getCause());
    } catch (IllegalAccessException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private static void writeBackfillCheckpoint(SystemRecordStore records, ByteKey registryIdentity,
      boolean complete, RequestScope scope) {
    EventIndexBackfillCheckpoint checkpoint = complete
        ? new EventIndexBackfillCheckpoint(1, 6, 3, 4, 5, 3, 5, 5, 0, registryIdentity)
        : new EventIndexBackfillCheckpoint(1, 5, 3, 4, 4, 3, 4, 5, 0, registryIdentity);
    records.createIfAbsent(SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT,
        EventIndexBackfillCheckpoint.versionIdentity(checkpoint.sequence()), checkpoint.encode(), scope).join();
  }

  private static CommitState controlState(SystemTableBinding binding, long stateVersion,
      long sequence, long revision, long compactRevision) {
    return CommitState.restore(CommitState.EVENT_INDEX_FORMAT_VERSION, binding.profileFingerprint(),
        Optional.of("j47-owner"), 1, 100, stateVersion, sequence, revision, sequence, revision,
        Optional.empty(), compactRevision);
  }

  private static boolean collectIfSafe(MiniClusterFixture environment, SystemTableBinding binding,
      EventHistoryRetention retention, ByteKey identity, long sequence) throws Exception {
    var limits = new BudgetManager.Limits(2, 2, 2, 2, 2, 1_000_000, 256L << 20);
    var budgets = new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits);
    RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(30));
    try (scope;
         BudgetManager.Account account = budgets.admit(TABLE_GROUP, scope,
             new BudgetManager.Reservation(8192, 32768, 128L << 20, 2L << 20, 4L << 20),
             new BudgetManager.ReadLimits(1000, 8L << 30, 1000, 1000, 1000))) {
      var boundary = retention.advance(6, 7, AUTHORIZATION, scope,
          new ManifestCatalog.ProofReadBudget(scope, account, 100, 8L << 30)).join();
      if (boundary.safeSequence() <= sequence || boundary.protections().stream()
          .anyMatch(protection -> protection.requiredSequence() <= sequence
              || protection.rootIdentity().filter(identity::equals).isPresent())) {
        return false;
      }
      environment.connection().getTable(binding.table())
          .delete(new Delete(binding.row(SystemRecordType.EVENT, identity))).join();
      return true;
    } finally {
      scope.cleanupCompletion().toCompletableFuture().join();
    }
  }

  private static BudgetManager replayBudgets() {
    // The 80 MiB service reserve is separate from the regular 64 MiB replay proof read.
    long memory = 256L << 20;
    var limits = new BudgetManager.Limits(16, 16, 16, 8, 8, 1L << 20, memory);
    var reserved = new BudgetManager.ReservedCapacity(4, 4, 4, 4, 4, 1L << 20, 80L << 20);
    return new BudgetManager(limits, Map.of(TABLE_GROUP, limits), limits, reserved, Map.of(TABLE_GROUP, reserved));
  }

  private static final class MiniClusterFixture implements AutoCloseable {
    private final TemporaryHBaseClusterProcess cluster;
    private final HBaseBackendIdentityProvider provider;
    private final AuthorizedBackendIdentity identity;
    private final org.apache.hadoop.hbase.client.AsyncConnection connection;
    private final TableName table;

    private MiniClusterFixture(TemporaryHBaseClusterProcess cluster, HBaseBackendIdentityProvider provider,
        AuthorizedBackendIdentity identity, org.apache.hadoop.hbase.client.AsyncConnection connection,
        TableName table) {
      this.cluster = cluster;
      this.provider = provider;
      this.identity = identity;
      this.connection = connection;
      this.table = table;
    }

    static MiniClusterFixture start(TableName table, org.apache.hadoop.hbase.client.TableDescriptor descriptor,
        byte[][] splitKeys) throws Exception {
      TemporaryHBaseClusterProcess cluster = TemporaryHBaseClusterProcess.start(1);
      HBaseBackendIdentityProvider provider = null;
      try {
        var authenticationScope = new AuthenticationScope("j47", "public");
        provider = new HBaseBackendIdentityProvider("j47-fixture", authenticationScope,
            cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
            (client, ignored) -> client.getAdmin().listNamespaces().thenApply(names -> null),
            2, 2, Duration.ofSeconds(30));
        AuthorizedBackendIdentity identity;
        try (var credentials = new BackendCredentials("j47-fixture", new char[0])) {
          identity = provider.authenticate(credentials, authenticationScope).toCompletableFuture().join();
        }
        var connection = provider.connectionFor(identity).toCompletableFuture().join();
        connection.getAdmin().createTable(descriptor, splitKeys).join();
        return new MiniClusterFixture(cluster, provider, identity, connection, table);
      } catch (Exception | Error failure) {
        if (provider != null) provider.close();
        cluster.close();
        throw failure;
      }
    }

    HBaseConnectionFactory factory() { return provider; }
    AuthorizedBackendIdentity identity() { return identity; }
    org.apache.hadoop.hbase.client.AsyncConnection connection() { return connection; }

    @Override
    public void close() throws Exception {
      try {
        connection.getAdmin().disableTable(table).join();
        connection.getAdmin().deleteTable(table).join();
      } finally {
        try { provider.close(); }
        finally { cluster.close(); }
      }
    }
  }
}
