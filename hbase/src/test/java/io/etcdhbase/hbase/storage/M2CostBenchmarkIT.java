package io.etcdhbase.hbase.storage;

import static io.etcdhbase.hbase.storage.M1TestEnvironment.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecisionRepository;
import io.etcdhbase.core.commit.CommitFinalizationService;
import io.etcdhbase.core.commit.CommitFinalizer;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationRepository;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.ManifestRepository;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.MaterializationStore;
import io.etcdhbase.core.commit.PublicationHandoff;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.junit.jupiter.api.Test;

/** Public local MiniCluster cost profile; it is neither a target-capacity nor a p99 SLO test. */
class M2CostBenchmarkIT {
  private static final long SEED = 20261104L;
  private static final int BUCKETS = 4;
  private static final byte[] FAMILY = {'s'};
  private static final ByteKey AUTHORIZATION_DIGEST = digest("j29-public-auth-scope");
  private static final HashSaltStrategy SALT = new HashSaltStrategy(
      new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, BUCKETS, 2));

  @Test
  void reportsCommitReadAndSimulatedRenewalCostsForBothLayouts() throws Exception {
    String digest = workloadDigest();
    try (M1TestEnvironment environment = M1TestEnvironment.start(1)) {
      for (StorageTableBinding.Layout layout :
          List.of(StorageTableBinding.Layout.HISTORY, StorageTableBinding.Layout.NATIVE)) {
        Profile profile = Profile.open(environment, layout);
        profile.runCommitBaseline(digest);
        profile.runMixedQueue(digest);
        profile.runPendingReadProfile(digest);
        profile.printTotals(digest);
      }
    }
  }

  private static String workloadDigest() {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      for (String request : List.of("short-kv:1:64", "large-kv:16:1024",
          "pending-overlay:8:128", "mixed-queue:short=4,large=2,renewal=2")) {
        digest.update(request.getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 0);
      }
      digest.update(Long.toString(SEED).getBytes(StandardCharsets.US_ASCII));
      return java.util.HexFormat.of().formatHex(digest.digest());
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private static ByteKey digest(String value) {
    try {
      return ByteKey.copyOf(MessageDigest.getInstance("SHA-256")
          .digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private record CommitSample(String kind, int candidates, long bindNanos, long prepareNanos,
      long publishNanos, long finalizationNanos, long totalNanos) {}

  private static final class Profile {
    private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(120);
    private static final Duration SERVICE_TIMEOUT = Duration.ofSeconds(120);

    private final M1TestEnvironment environment;
    private final StorageTableBinding.Layout layout;
    private final StorageTableBinding primary;
    private final SystemTableBinding systemBinding;
    private final M2CostMetrics metrics;
    private final LongAdder finalizationNanos = new LongAdder();
    private final M2CostRuntimeSampler runtime = new M2CostRuntimeSampler();
    private final AtomicLong maxCandidatePayloadBytes = new AtomicLong();
    private final SystemRecordStore records;
    private final M2CostMetrics.Control control;
    private final InvocationRepository invocations;
    private final ManifestRepository manifests;
    private final CommitDecisionRepository decisions;
    private final MaterializationStore materialization;
    private final CommitOwnerService.OwnerToken owner;
    private final InvocationService invocationService;
    private final ManifestPreparation preparation;
    private final ManifestCatalog catalog;
    private final CommitOutcomeResolver outcomes;
    private final CommitFinalizer finalizer;
    private final CommitPublisher publisher;
    private int transactionNumber;

    private Profile(M1TestEnvironment environment, StorageTableBinding.Layout layout,
        StorageTableBinding primary, SystemTableBinding systemBinding, M2CostMetrics metrics, SystemRecordStore records,
        M2CostMetrics.Control control, InvocationRepository invocations,
        ManifestRepository manifests, CommitDecisionRepository decisions,
        MaterializationStore materialization, CommitOwnerService.OwnerToken owner) {
      this.environment = environment;
      this.layout = layout;
      this.primary = primary;
      this.systemBinding = systemBinding;
      this.metrics = metrics;
      this.records = records;
      this.control = control;
      this.invocations = invocations;
      this.manifests = manifests;
      this.decisions = decisions;
      this.materialization = materialization;
      this.owner = owner;
      this.invocationService = new InvocationService(invocations);
      this.preparation = new ManifestPreparation(manifests,
          new ManifestPreparation.Limits(128, 64L * 1024 * 1024));
      this.catalog = new ManifestCatalog(manifests);
      this.outcomes = new CommitOutcomeResolver(control, decisions);
      this.finalizer = timedFinalizer(new CommitFinalizationService(control, outcomes, decisions,
          catalog, materialization));
      this.publisher = new CommitPublisher(control, outcomes,
          new io.etcdhbase.core.commit.CommitPlanVerifier(catalog, materialization),
          invocations, serviceHandoff(), finalizer);
    }

    static Profile open(M1TestEnvironment environment, StorageTableBinding.Layout layout)
        throws Exception {
      String suffix = UUID.randomUUID().toString().replace("-", "");
      StorageTableBinding primary = new StorageTableBinding(
          TableName.valueOf("m2j29_" + layout.name().toLowerCase(Locale.ROOT) + "_" + suffix),
          new byte[] {'d'}, layout, SALT);
      Optional<StorageTableBinding> current = layout == StorageTableBinding.Layout.HISTORY
          ? Optional.of(new StorageTableBinding(TableName.valueOf("m2j29_current_" + suffix),
              new byte[] {'d'}, StorageTableBinding.Layout.CURRENT, SALT))
          : Optional.empty();
      String systemSuffix = UUID.randomUUID().toString().replace("-", "");
      SystemTableBinding system = new SystemTableBinding(TableName.valueOf("m2j29_system_" + systemSuffix),
          FAMILY, "m2j29-group-" + systemSuffix, "m2j29-profile", SALT);
      environment.create(system.table(), systemDescriptor(system), SALT.splitKeys().toArray(byte[][]::new));
      environment.create(primary, SALT.splitKeys().toArray(byte[][]::new));
      current.ifPresent(binding -> createStorage(environment, binding));
      M2CostMetrics metrics = new M2CostMetrics();
      try (RequestScope openScope = RequestScope.withTimeout(OPERATION_TIMEOUT)) {
        SystemRecordStore records = new M2CostMetrics.Records(await(HBaseSystemRecordStore.open(
            environment.factory(), environment.identity(), system, openScope)), metrics);
        // The UUID-named system table is fresh and no service has started using it.
        M2CostReadProfile.initializeFreshPins(records, openScope);
        M2CostMetrics.Control control = new M2CostMetrics.Control(await(HBaseCommitControlStore.open(
            environment.factory(), environment.identity(), system, openScope)), metrics);
        InvocationRepository invocations = new HBaseInvocationRepository(records);
        ManifestRepository manifests = new HBaseManifestRepository(records);
        CommitDecisionRepository decisions = new HBaseCommitDecisionRepository(records);
        MaterializationStore materialization = new M2CostMetrics.Materializer(await(
            HBaseMaterializationStore.open(environment.factory(), environment.identity(), primary,
                current, openScope)), metrics);
        CommitState initial = CommitState.initial(system.profileFingerprint(), 1);
        assertTrue(await(control.initializeIfAbsent(initial, openScope)));
        var ownerService = new CommitOwnerService(control);
        long now = System.currentTimeMillis();
        var owner = await(ownerService.claim("j29-minicluster-" + suffix, now,
            now + Duration.ofHours(1).toMillis(), openScope)).orElseThrow();
        owner = await(new io.etcdhbase.core.commit.AuthorizationBarrierUpgrade(control)
            .enable(owner, now, openScope));
        Profile profile = new Profile(environment, layout, primary, system, metrics, records,
            control, invocations, manifests, decisions, materialization, owner);
        return profile;
      }
    }

    private static void createStorage(M1TestEnvironment environment, StorageTableBinding binding) {
      try {
        environment.create(binding, SALT.splitKeys().toArray(byte[][]::new));
      } catch (Exception failure) {
        throw new IllegalStateException("could not create the public benchmark table", failure);
      }
    }

    private static org.apache.hadoop.hbase.client.TableDescriptor systemDescriptor(
        SystemTableBinding binding) {
      return TableDescriptorBuilder.newBuilder(binding.table())
          .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
              .setTimeToLive(HConstants.FOREVER).setMaxVersions(1).build())
          .setDurability(Durability.SYNC_WAL)
          .setValue("etcdhbase.m0.table-group-id", binding.tableGroupId())
          .setValue("etcdhbase.m0.salt-profile-id", "m2j29-profile")
          .setValue("etcdhbase.m0.salt-strategy-id", SALT.profile().strategyId())
          .setValue("etcdhbase.m0.salt-encoding-version",
              Integer.toString(SALT.profile().encodingVersion()))
          .setValue("etcdhbase.m0.salt-bucket-count", Integer.toString(SALT.profile().bucketCount()))
          .setValue("etcdhbase.m0.salt-width-bytes", Integer.toString(SALT.profile().saltWidthBytes()))
          .setValue("etcdhbase.m0.record-categories",
              "CONTROL,INVOCATION,CANDIDATE,MANIFEST,DECISION,SNAPSHOT_PIN")
          .build();
    }

    private CommitFinalizer timedFinalizer(CommitFinalizer delegate) {
      return (invocation, publication, serviceScope) -> {
        long started = System.nanoTime();
        return delegate.finish(invocation, publication, serviceScope)
            .whenComplete((ignored, failure) -> finalizationNanos.add(System.nanoTime() - started));
      };
    }

    private static PublicationHandoff serviceHandoff() {
      return new PublicationHandoff() {
        @Override public <T> CompletableFuture<T> transfer(InvocationBinding invocation,
            RequestScope requestScope,
            java.util.function.Function<RequestScope, CompletableFuture<T>> serviceWork) {
          RequestScope serviceScope = RequestScope.withTimeout(SERVICE_TIMEOUT);
          CompletableFuture<T> work;
          try {
            work = serviceWork.apply(serviceScope);
          } catch (RuntimeException | Error failure) {
            // The transferred service scope must close even when setup fails fatally.
            serviceScope.close();
            if (failure instanceof Error fatal) throw fatal;
            return CompletableFuture.failedFuture(failure);
          }
          if (work == null) {
            serviceScope.close();
            return CompletableFuture.failedFuture(new IllegalStateException("service work returned null"));
          }
          return work.whenComplete((ignored, failure) -> serviceScope.close());
        }
      };
    }

    private void runCommitBaseline(String digest) throws Exception {
      List<CommitSample> samples = List.of(
          executeCommit("SHORT_KV", 1, 64),
          executeCommit("LARGE_KV", 16, 1024),
          executeCommit("SIMULATED_RENEWAL", 0, 0));
      String sampleJson = samples.stream().map(Profile::commitJson)
          .collect(java.util.stream.Collectors.joining(",", "[", "]"));
      System.out.printf(Locale.ROOT,
          "j29-commit-baseline {\"layout\":\"%s\",\"seed\":%d,\"workloadSha256\":\"%s\",\"samples\":%s}%n",
          layout, SEED, digest, sampleJson);
    }

    private CommitSample executeCommit(String kind, int candidateCount, int valueBytes)
        throws Exception {
      runtime.sample();
      long started = System.nanoTime();
      long finalizationBefore = finalizationNanos.sum();
      long bindNanos;
      long prepareNanos;
      long publishNanos;
      try (RequestScope scope = RequestScope.withTimeout(OPERATION_TIMEOUT)) {
        ControlSnapshot base = await(control.read(scope)).orElseThrow();
        CommitState state = base.state();
        TransactionId transactionId = TransactionId.create();
        InvocationId invocationId = InvocationId.create();
        InvocationBinding proposed = InvocationBinding.begin(invocationId,
            digest("request/" + layout + "/" + transactionNumber + "/" + kind),
            AUTHORIZATION_DIGEST, transactionId, owner.epoch(), state.publishedSequence(),
            state.publishedRevision());
        long bindStarted = System.nanoTime();
        InvocationBinding preparing = await(invocationService.bindOrJoin(proposed, scope));
        bindNanos = System.nanoTime() - bindStarted;

        List<io.etcdhbase.core.commit.PreparedCandidate> candidates = candidates(
            transactionId, owner.epoch(), state.publishedSequence(), state.publishedRevision(),
            candidateCount, valueBytes, "txn-" + transactionNumber + "/" + kind);
        long prepareStarted = System.nanoTime();
        ManifestRoot root = await(preparation.prepare(transactionId, owner.epoch(),
            state.publishedSequence(), state.publishedRevision(), candidates.iterator(), scope));
        InvocationBinding sealed = await(invocationService.transition(preparing,
            preparing.seal(root.systemIdentity()), scope));
        prepareNanos = System.nanoTime() - prepareStarted;
        long publishStarted = System.nanoTime();
        CommitOutcomeResolver.Resolution result = await(publisher.publish(sealed,
            owner, root, scope, io.etcdhbase.core.commit.CommitAuthorizationProof.capture(
                AUTHORIZATION_DIGEST, state.authorizationBarrierGeneration(),
                (identity, generation, request) -> CompletableFuture.completedFuture(null))));
        publishNanos = System.nanoTime() - publishStarted;
        assertEquals(CommitOutcomeResolver.State.SELECTED_FINALIZED, result.state());
        CommitState advanced = await(control.read(scope)).orElseThrow().state();
        assertEquals(state.publishedSequence() + 1, advanced.finalizedSequence());
        if (candidateCount == 0) assertEquals(state.publishedRevision(), advanced.publishedRevision());
        else assertEquals(state.publishedRevision() + 1, advanced.publishedRevision());
      }
      transactionNumber++;
      runtime.sample();
      return new CommitSample(kind, candidateCount, bindNanos, prepareNanos, publishNanos,
          finalizationNanos.sum() - finalizationBefore, System.nanoTime() - started);
    }

    private List<io.etcdhbase.core.commit.PreparedCandidate> candidates(TransactionId transaction,
        long ownerEpoch, long baseSequence, long baseRevision, int count, int valueBytes,
        String keyPrefix) {
      List<io.etcdhbase.core.commit.PreparedCandidate> result = new ArrayList<>(count);
      for (int index = 0; index < count; index++) {
        ByteKey key = ByteKey.copyOf((keyPrefix + "/key-" + index).getBytes(StandardCharsets.UTF_8));
        byte[] value = new byte[valueBytes];
        Arrays.fill(value, (byte) (index + transactionNumber));
        long revision = Math.addExact(baseRevision, 1);
        StoredRecord record = StoredRecord.live(key,
            new StorageMetadata(revision, revision, 1, 0, StorageMetadata.State.LIVE, value.length), value);
        result.add(new io.etcdhbase.core.commit.PreparedCandidate(transaction, ownerEpoch,
            baseSequence, baseRevision, record));
      }
      result.sort(Comparator.comparingInt((io.etcdhbase.core.commit.PreparedCandidate candidate) ->
          manifests.candidateBucket(candidate)).thenComparing(candidate -> candidate.record().key()));
      maxCandidatePayloadBytes.accumulateAndGet(result.stream().mapToLong(candidate ->
          (long) candidate.record().key().length()
              + candidate.record().value().map(value -> value.length).orElse(0)).sum(), Math::max);
      return List.copyOf(result);
    }

    private void runPendingReadProfile(String digest) throws Exception {
      M2CostReadProfile.Context context = new M2CostReadProfile.Context(
          environment, layout, primary, systemBinding, BUCKETS, AUTHORIZATION_DIGEST,
          OPERATION_TIMEOUT, records, control, invocations, invocationService, manifests, preparation,
          catalog, outcomes, decisions, materialization, owner, runtime,
          maxCandidatePayloadBytes::get);
      M2CostReadProfile.run(context, SEED, digest);
    }

    private void runMixedQueue(String digest) throws Exception {
      M2CostFairQueue.run(layout.name(), SEED, digest,
          work -> executeCommit(work.kind(), work.candidates(), work.valueBytes()).totalNanos());
    }

    private static String commitJson(CommitSample sample) {
      return String.format(Locale.ROOT,
          "{\"kind\":\"%s\",\"candidates\":%d,\"bindMicros\":%d,"
              + "\"prepareMicros\":%d,\"publishAndFinalizeMicros\":%d,"
              + "\"finalizationMicros\":%d,\"totalMicros\":%d}",
          sample.kind(), sample.candidates(), sample.bindNanos() / 1000,
          sample.prepareNanos() / 1000, sample.publishNanos() / 1000,
          sample.finalizationNanos() / 1000, sample.totalNanos() / 1000);
    }

    private void printTotals(String digest) {
      Map<String, M2CostMetrics.Value> totals = metrics.snapshot();
      String operations = totals.entrySet().stream().map(entry -> {
        M2CostMetrics.Value value = entry.getValue();
        return String.format(Locale.ROOT,
            "\"%s\":{\"calls\":%d,\"succeeded\":%d,\"conflicts\":%d,\"unknown\":%d,"
                + "\"failed\":%d,\"inputBytes\":%d,\"outputBytes\":%d,\"elapsedMicros\":%d}",
            entry.getKey(), value.calls(), value.succeeded(), value.conflicts(), value.unknown(),
            value.failed(), value.inputBytes(), value.outputBytes(), value.elapsedNanos() / 1000);
      }).collect(java.util.stream.Collectors.joining(",", "{", "}"));
      System.out.printf(Locale.ROOT,
          "j29-layout {\"layout\":\"%s\",\"seed\":%d,\"workloadSha256\":\"%s\","
              + "\"hbaseVersion\":\"%s\",\"java\":\"%s\",\"bucketCount\":%d,"
              + "\"publishedSequence\":%d,\"publishedRevision\":%d,\"metrics\":%s,"
              + "\"notObserved\":[\"HBase RPC/retry counts for point operations\","
              + "\"server scan counters\",\"WAL/HFile bytes and flush/compaction IO\","
              + "\"JVM direct-memory high-water mark\"]}%n",
          layout, SEED, digest, VersionInfo.getVersion(), Runtime.version(), BUCKETS,
          lastStateSequence(), lastStateRevision(), operations);
    }

    private long lastStateSequence() {
      try (RequestScope scope = RequestScope.withTimeout(OPERATION_TIMEOUT)) {
        return await(control.read(scope)).orElseThrow().state().publishedSequence();
      } catch (Exception failure) {
        throw new IllegalStateException("could not read final J29 sequence", failure);
      }
    }

    private long lastStateRevision() {
      try (RequestScope scope = RequestScope.withTimeout(OPERATION_TIMEOUT)) {
        return await(control.read(scope)).orElseThrow().state().publishedRevision();
      } catch (Exception failure) {
        throw new IllegalStateException("could not read final J29 revision", failure);
      }
    }
  }
}
