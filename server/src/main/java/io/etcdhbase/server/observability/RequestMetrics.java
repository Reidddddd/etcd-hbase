package io.etcdhbase.server.observability;

import static io.etcdhbase.grpc.RequestCompletionRecorder.MethodClass;
import static io.etcdhbase.grpc.RequestCompletionRecorder.Outcome;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.grpc.RequestCompletionRecorder;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Full bounded-dimension counters, with rate-limited summaries for unexpected failures only. */
public final class RequestMetrics implements RequestCompletionRecorder {
  private static final Logger LOGGER = LoggerFactory.getLogger(RequestMetrics.class);
  private static final Pattern TRACE_ID =
      Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
  static final long WARNING_INTERVAL_NANOS = Duration.ofSeconds(30).toNanos();
  private final Map<Series, Counter> counters;
  private final Map<PutStage, StageCounter> putStages;
  private final Map<TxnStage, StageCounter> txnStages;
  private final Map<LeaseRenewalStage, StageCounter> leaseRenewalStages;
  private final Map<LeaseRevocationStage, StageCounter> leaseRevocationStages;
  private final Map<WatchCreateResourceFailureStage, LongAdder> watchCreateResourceFailures;
  private final Map<WatchReplayOpenResourceFailureStage, LongAdder> watchReplayOpenResourceFailures;
  private final AtomicReference<BudgetManager.AdmissionFailureSnapshot>
      watchReplayLookupAdmissionFailure = new AtomicReference<>();
  private final LongAdder putOutputBytes = new LongAdder();
  private final LongAdder txnOutputBytes = new LongAdder();
  private final LongAdder leaseRenewalRequests = new LongAdder();
  private final LongAdder leaseRenewalResponses = new LongAdder();
  private final LongAdder leaseRenewalSelected = new LongAdder();
  private final LongAdder leaseRenewalUnknownOutcomes = new LongAdder();
  private final LongAdder leaseRenewalRequestBytes = new LongAdder();
  private final LongAdder leaseRenewalResponseBytes = new LongAdder();
  private final LongAdder leaseRenewalAuxiliaryRecords = new LongAdder();
  private final LongAdder leaseRenewalAuxiliaryPayloadBytes = new LongAdder();
  private final LongAdder leaseRenewalHBaseClientOperations = new LongAdder();
  private final LongAdder leaseRenewalLogicalHBaseRequestPayloadBytes = new LongAdder();
  private final LongAdder leaseExpiryScanRows = new LongAdder();
  private final LongAdder leaseExpiryScanBytes = new LongAdder();
  private final LongAdder leaseExpiryCandidates = new LongAdder();
  private final LongAdder leaseExpiryStaleCandidates = new LongAdder();
  private final LongAdder leaseExpiryRevocations = new LongAdder();
  private final LongAdder leaseExpiryDelayMillis = new LongAdder();
  private final LongAdder leaseExpiryDelaySamplesMissing = new LongAdder();
  private final LongAdder gcBacklog = new LongAdder();
  private final LongAdder gcFailures = new LongAdder();
  private final LongAdder gcPages = new LongAdder();
  private final LongAdder gcExamined = new LongAdder();
  private final LongAdder gcRetired = new LongAdder();
  private final LongAdder leaseExpiryRetries = new LongAdder();
  private final LongAdder eventBackfillSteps = new LongAdder();
  private final LongAdder eventBackfillCompletions = new LongAdder();
  private final LongAdder eventBackfillResourceFailures = new LongAdder();
  private final LongAdder eventBackfillOtherFailures = new LongAdder();
  private final LongAdder eventHistoryFailures = new LongAdder();
  private final LongAdder eventHistoryResourceFailures = new LongAdder();
  private final LongAdder eventTailerCycles = new LongAdder();
  private final LongAdder eventTailerProgressCycles = new LongAdder();
  private final LongAdder eventTailerIdleCycles = new LongAdder();
  private final LongAdder eventTailerFailures = new LongAdder();
  private final LongAdder eventTailerResourceFailures = new LongAdder();
  private final LongAdder eventTailerSequences = new LongAdder();
  private final LongAdder eventTailerEvents = new LongAdder();
  private final LongAdder eventTailerEventBytes = new LongAdder();
  private final LongAdder eventTailerHBaseOperations = new LongAdder();
  private final LongAdder eventTailerLogicalRequestBytes = new LongAdder();
  private final AtomicLong eventTailerSequence = new AtomicLong();
  private final AtomicLong eventTailerRevision = new AtomicLong();
  private final AtomicLong eventTailerObservedSequence = new AtomicLong();
  private final AtomicLong eventTailerSequenceLag = new AtomicLong();
  private final AtomicLong eventTailerSubscriptions = new AtomicLong();
  private final AtomicLong leaseExpiryOwnerEpoch = new AtomicLong();
  private final AtomicLong leaseExpiryCheckpointSequence = new AtomicLong();
  private final AtomicLong leaseExpiryWindowStopExclusive = new AtomicLong();
  private final LongSupplier nanoTime;
  private final Consumer<FailureSummary> warnings;
  private final AtomicLong lastWarningNanos;

  public RequestMetrics() {
    this(System::nanoTime, RequestMetrics::logSummary);
  }

  RequestMetrics(LongSupplier nanoTime, Consumer<FailureSummary> warnings) {
    this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    this.warnings = Objects.requireNonNull(warnings, "warnings");
    lastWarningNanos = new AtomicLong(nanoTime.getAsLong() - WARNING_INTERVAL_NANOS);
    Map<Series, Counter> initialized = new HashMap<>();
    for (MethodClass methodClass : MethodClass.values()) {
      for (Outcome outcome : Outcome.values())
        initialized.put(new Series(methodClass, outcome), new Counter());
    }
    counters = Map.copyOf(initialized);
    Map<PutStage, StageCounter> stages = new EnumMap<>(PutStage.class);
    for (PutStage stage : PutStage.values()) stages.put(stage, new StageCounter());
    putStages = Map.copyOf(stages);
    Map<TxnStage, StageCounter> transactions = new EnumMap<>(TxnStage.class);
    for (TxnStage stage : TxnStage.values()) transactions.put(stage, new StageCounter());
    txnStages = Map.copyOf(transactions);
    Map<LeaseRenewalStage, StageCounter> renewals = new EnumMap<>(LeaseRenewalStage.class);
    for (LeaseRenewalStage stage : LeaseRenewalStage.values()) renewals.put(stage, new StageCounter());
    leaseRenewalStages = Map.copyOf(renewals);
    Map<LeaseRevocationStage, StageCounter> revocations = new EnumMap<>(LeaseRevocationStage.class);
    for (LeaseRevocationStage stage : LeaseRevocationStage.values()) {
      revocations.put(stage, new StageCounter());
    }
    leaseRevocationStages = Map.copyOf(revocations);
    Map<WatchCreateResourceFailureStage, LongAdder> watchFailures =
        new EnumMap<>(WatchCreateResourceFailureStage.class);
    for (WatchCreateResourceFailureStage stage : WatchCreateResourceFailureStage.values()) {
      watchFailures.put(stage, new LongAdder());
    }
    watchCreateResourceFailures = Map.copyOf(watchFailures);
    Map<WatchReplayOpenResourceFailureStage, LongAdder> replayFailures =
        new EnumMap<>(WatchReplayOpenResourceFailureStage.class);
    for (WatchReplayOpenResourceFailureStage stage : WatchReplayOpenResourceFailureStage.values()) {
      replayFailures.put(stage, new LongAdder());
    }
    watchReplayOpenResourceFailures = Map.copyOf(replayFailures);
  }

  /** Bounded stages that J30 actually observes; publication includes M2 finalization. */
  public enum PutStage { QUEUE, PREPARE, PUBLISH_AND_FINALIZE, RESULT_RESOLUTION, RESPONSE }

  /** Bounded stages measured for transaction requests. */
  public enum TxnStage { QUEUE, COMPARE_AND_PREPARE, PUBLISH_AND_FINALIZE, RESULT_RESOLUTION, RESPONSE }

  /** Bounded stages and payload counters for Lease KeepAlive; no Lease ID or user label is kept. */
  public enum LeaseRenewalStage {
    QUEUE, PREPARE, PUBLISH_AND_FINALIZE, RESULT_RESOLUTION, DELIVERY_VALIDATION, OUTBOUND_READY_WAIT
  }

  /** Bounded stages measured while preparing and publishing a Lease revocation. */
  public enum LeaseRevocationStage { QUEUE, PREPARE, PUBLISH_AND_FINALIZE }

  /**
   * Coarse, bounded stages for Watch-create resource failures. These counters locate the processing
   * stage that rejected a request; they do not identify the specific budget dimension or limit.
   */
  public enum WatchCreateResourceFailureStage {
    REQUEST_ACCOUNT_ADMISSION,
    STATE_MEMORY_RETENTION,
    TAILER_SUBSCRIPTION,
    BARRIER_READ,
    HISTORY_REPLAY_OPEN
  }

  /** Bounded inner phases for a resource failure while opening historical Watch replay. */
  public enum WatchReplayOpenResourceFailureStage {
    LOOKUP_ACCOUNT_ADMISSION,
    SEQUENCE_LOOKUP,
    PIN_REGISTRATION,
    PIN_RENEWAL,
    CONTROL_REVALIDATION_ACCOUNT_ADMISSION,
    CONTROL_REVALIDATION_READ,
    SEQUENCE_REVALIDATION_ACCOUNT_ADMISSION,
    SEQUENCE_REVALIDATION_READ
  }

  public void recordWatchCreateResourceFailure(WatchCreateResourceFailureStage stage) {
    Objects.requireNonNull(stage, "stage");
    watchCreateResourceFailures.get(stage).increment();
  }

  public Map<WatchCreateResourceFailureStage, Long> watchCreateResourceFailures() {
    Map<WatchCreateResourceFailureStage, Long> snapshot =
        new EnumMap<>(WatchCreateResourceFailureStage.class);
    watchCreateResourceFailures.forEach((stage, count) -> snapshot.put(stage, count.sum()));
    return Map.copyOf(snapshot);
  }

  public void recordWatchReplayOpenResourceFailure(WatchReplayOpenResourceFailureStage stage) {
    Objects.requireNonNull(stage, "stage");
    watchReplayOpenResourceFailures.get(stage).increment();
  }

  public Map<WatchReplayOpenResourceFailureStage, Long> watchReplayOpenResourceFailures() {
    Map<WatchReplayOpenResourceFailureStage, Long> snapshot =
        new EnumMap<>(WatchReplayOpenResourceFailureStage.class);
    watchReplayOpenResourceFailures.forEach((stage, count) -> snapshot.put(stage, count.sum()));
    return Map.copyOf(snapshot);
  }

  /** Retains only the latest bounded lookup-admission rejection; no group or request identity. */
  public void recordWatchReplayLookupAdmissionFailure(
      BudgetManager.AdmissionFailureSnapshot snapshot) {
    watchReplayLookupAdmissionFailure.set(Objects.requireNonNull(snapshot, "snapshot"));
  }

  public Optional<BudgetManager.AdmissionFailureSnapshot>
      watchReplayLookupAdmissionFailure() {
    return Optional.ofNullable(watchReplayLookupAdmissionFailure.get());
  }

  public void recordTxnStage(TxnStage stage, long elapsedNanos) {
    Objects.requireNonNull(stage, "stage");
    if (elapsedNanos < 0) throw new IllegalArgumentException("elapsedNanos must not be negative");
    var counter = txnStages.get(stage);
    counter.count.increment();
    counter.totalNanos.add(elapsedNanos);
  }

  public Map<TxnStage, StageSnapshot> txnStages() {
    Map<TxnStage, StageSnapshot> snapshot = new EnumMap<>(TxnStage.class);
    txnStages.forEach((stage, counter) -> snapshot.put(stage,
        new StageSnapshot(counter.count.sum(), counter.totalNanos.sum())));
    return Map.copyOf(snapshot);
  }

  public void recordTxnOutputBytes(long bytes) {
    if (bytes < 0) throw new IllegalArgumentException("output bytes must not be negative");
    txnOutputBytes.add(bytes);
  }

  public long txnOutputBytes() { return txnOutputBytes.sum(); }

  public void recordLeaseRenewalStage(LeaseRenewalStage stage, long elapsedNanos) {
    Objects.requireNonNull(stage, "stage");
    if (elapsedNanos < 0) throw new IllegalArgumentException("elapsedNanos must not be negative");
    var counter = leaseRenewalStages.get(stage);
    counter.count.increment();
    counter.totalNanos.add(elapsedNanos);
  }

  /** Records one accepted KeepAlive protobuf request, excluding HTTP/2 framing bytes. */
  public void recordLeaseRenewalRequest(int serializedBytes) {
    if (serializedBytes < 0) throw new IllegalArgumentException("request bytes must not be negative");
    leaseRenewalRequests.increment();
    leaseRenewalRequestBytes.add(serializedBytes);
  }

  /** Records one KeepAlive response accepted by the gRPC observer, not a physical socket write. */
  public void recordLeaseRenewalResponse(int serializedBytes) {
    if (serializedBytes < 0) throw new IllegalArgumentException("response bytes must not be negative");
    leaseRenewalResponses.increment();
    leaseRenewalResponseBytes.add(serializedBytes);
  }

  /** Records the auxiliary Lease records in a fully selected and finalized renewal. */
  public void recordLeaseRenewalSelected(int auxiliaryRecords, long payloadBytes) {
    if (auxiliaryRecords < 0 || payloadBytes < 0) {
      throw new IllegalArgumentException("selected Lease storage totals must not be negative");
    }
    leaseRenewalSelected.increment();
    leaseRenewalAuxiliaryRecords.add(auxiliaryRecords);
    leaseRenewalAuxiliaryPayloadBytes.add(payloadBytes);
  }

  /**
   * Records owner-side logical HBase client operations and counted row/cell/range payload bytes.
   * The values exclude retries, RPC envelopes, compression, TLS, and transport framing.
   */
  public void recordLeaseRenewalStorageCost(RequestScope.StorageCostSnapshot cost) {
    Objects.requireNonNull(cost, "cost");
    leaseRenewalHBaseClientOperations.add(cost.logicalOperations());
    leaseRenewalLogicalHBaseRequestPayloadBytes.add(cost.logicalRequestPayloadBytes());
  }

  public void recordLeaseRenewalUnknownOutcome() {
    leaseRenewalUnknownOutcomes.increment();
  }

  public LeaseRenewalSnapshot leaseRenewals() {
    Map<LeaseRenewalStage, StageSnapshot> stages = new EnumMap<>(LeaseRenewalStage.class);
    leaseRenewalStages.forEach((stage, counter) -> stages.put(stage,
        new StageSnapshot(counter.count.sum(), counter.totalNanos.sum())));
    return new LeaseRenewalSnapshot(leaseRenewalRequests.sum(), leaseRenewalResponses.sum(),
        leaseRenewalSelected.sum(), leaseRenewalUnknownOutcomes.sum(),
        leaseRenewalRequestBytes.sum(), leaseRenewalResponseBytes.sum(),
        leaseRenewalAuxiliaryRecords.sum(), leaseRenewalAuxiliaryPayloadBytes.sum(),
        leaseRenewalHBaseClientOperations.sum(),
        leaseRenewalLogicalHBaseRequestPayloadBytes.sum(), stages);
  }

  public void recordLeaseRevocationStage(LeaseRevocationStage stage, long elapsedNanos) {
    Objects.requireNonNull(stage, "stage");
    if (elapsedNanos < 0) throw new IllegalArgumentException("elapsedNanos must not be negative");
    StageCounter counter = leaseRevocationStages.get(stage);
    counter.count.increment();
    counter.totalNanos.add(elapsedNanos);
  }

  public Map<LeaseRevocationStage, StageSnapshot> leaseRevocationStages() {
    Map<LeaseRevocationStage, StageSnapshot> snapshot = new EnumMap<>(LeaseRevocationStage.class);
    leaseRevocationStages.forEach((stage, counter) -> snapshot.put(stage,
        new StageSnapshot(counter.count.sum(), counter.totalNanos.sum())));
    return Map.copyOf(snapshot);
  }

  /** One bounded expiry scan page; counts are logical scan rows/bytes, not HBase RPC wire bytes. */
  public void recordLeaseExpiryScan(long physicalRows, long encodedBytes) {
    if (physicalRows < 0 || encodedBytes < 0) {
      throw new IllegalArgumentException("Lease expiry scan totals must not be negative");
    }
    leaseExpiryScanRows.add(physicalRows);
    leaseExpiryScanBytes.add(encodedBytes);
  }

  public void recordLeaseExpiryCandidate() { leaseExpiryCandidates.increment(); }

  public void recordEventBackfillStep(boolean complete) {
    eventBackfillSteps.increment();
    if (complete) eventBackfillCompletions.increment();
  }

  public void recordEventBackfillFailure(BackendFailure.Category category) {
    if (Objects.requireNonNull(category, "category") == BackendFailure.Category.RESOURCE_LIMIT) {
      eventBackfillResourceFailures.increment();
    } else {
      eventBackfillOtherFailures.increment();
    }
  }

  public EventBackfillSnapshot eventBackfill() {
    return new EventBackfillSnapshot(eventBackfillSteps.sum(), eventBackfillCompletions.sum(),
        eventBackfillResourceFailures.sum(), eventBackfillOtherFailures.sum());
  }

  public record EventBackfillSnapshot(long steps, long completions,
      long resourceFailures, long otherFailures) {}

  public void recordEventHistoryFailure(BackendFailure.Category category) {
    eventHistoryFailures.increment();
    if (Objects.requireNonNull(category, "category") == BackendFailure.Category.RESOURCE_LIMIT) {
      eventHistoryResourceFailures.increment();
    }
  }

  public EventHistoryFailureSnapshot eventHistoryFailures() {
    return new EventHistoryFailureSnapshot(eventHistoryFailures.sum(), eventHistoryResourceFailures.sum());
  }

  public record EventHistoryFailureSnapshot(long failures, long resourceFailures) {}

  /** Bounded per-runtime event read cost and lag; no keys, IDs, or authorization labels are retained. */
  public void recordEventTailerCycle(boolean progressed, long publishedEvents, long eventBytes,
      RequestScope.StorageCostSnapshot storageCost, long sequence, long revision,
      long observedPublishedSequence, int subscriptions) {
    Objects.requireNonNull(storageCost, "storageCost");
    if (publishedEvents < 0 || eventBytes < 0 || sequence < 0 || revision < 0
        || observedPublishedSequence < 0 || subscriptions < 0) {
      throw new IllegalArgumentException("event tailer metrics must not be negative");
    }
    eventTailerCycles.increment();
    if (progressed) {
      eventTailerProgressCycles.increment();
      eventTailerSequences.add(1);
    } else {
      eventTailerIdleCycles.increment();
    }
    eventTailerEvents.add(publishedEvents);
    eventTailerEventBytes.add(eventBytes);
    eventTailerHBaseOperations.add(storageCost.logicalOperations());
    eventTailerLogicalRequestBytes.add(storageCost.logicalRequestPayloadBytes());
    eventTailerSequence.set(sequence);
    eventTailerRevision.set(revision);
    eventTailerObservedSequence.set(observedPublishedSequence);
    eventTailerSequenceLag.set(Math.max(0, observedPublishedSequence - sequence));
    eventTailerSubscriptions.set(subscriptions);
  }

  public void recordEventTailerFailure(BackendFailure.Category category) {
    eventTailerFailures.increment();
    if (Objects.requireNonNull(category, "category") == BackendFailure.Category.RESOURCE_LIMIT) {
      eventTailerResourceFailures.increment();
    }
  }

  public EventTailerSnapshot eventTailer() {
    return new EventTailerSnapshot(eventTailerCycles.sum(), eventTailerProgressCycles.sum(),
        eventTailerIdleCycles.sum(), eventTailerFailures.sum(), eventTailerResourceFailures.sum(),
        eventTailerSequences.sum(), eventTailerEvents.sum(), eventTailerEventBytes.sum(),
        eventTailerHBaseOperations.sum(), eventTailerLogicalRequestBytes.sum(),
        eventTailerSequence.get(), eventTailerRevision.get(), eventTailerObservedSequence.get(),
        eventTailerSequenceLag.get(), eventTailerSubscriptions.get());
  }

  public record EventTailerSnapshot(long cycles, long progressCycles, long idleCycles,
      long failures, long resourceFailures, long sequences, long events, long eventBytes,
      long hbaseClientOperations, long logicalRequestPayloadBytes, long sequence,
      long revision, long observedPublishedSequence, long sequenceLag, long subscriptions) {}

  public void recordLeaseExpiryStaleCandidate() { leaseExpiryStaleCandidates.increment(); }

  /** Records delay from the trusted lower time bound to the revoked candidate deadline. */
  public void recordLeaseExpiryRevocation(long delayMillis) {
    if (delayMillis < 0) throw new IllegalArgumentException("Lease expiry delay must not be negative");
    leaseExpiryRevocations.increment();
    leaseExpiryDelayMillis.add(delayMillis);
  }

  /** Records a committed expiry whose post-finalization trusted-time sample was unavailable. */
  public void recordLeaseExpiryRevocationWithoutDelaySample() {
    leaseExpiryRevocations.increment();
    leaseExpiryDelaySamplesMissing.increment();
  }

  public void recordGcBacklog() { gcBacklog.increment(); }
  public void recordGcFailure() { gcFailures.increment(); }
  public void recordGcPage(int examined, int retired) {
    gcPages.increment();
    gcExamined.add(examined);
    gcRetired.add(retired);
  }
  public GcSnapshot gc() {
    return new GcSnapshot(gcBacklog.sum(), gcFailures.sum(), gcPages.sum(), gcExamined.sum(), gcRetired.sum());
  }
  public record GcSnapshot(long backlog, long failures, long pages, long examined, long retired) {}

  public void recordLeaseExpiryRetry() { leaseExpiryRetries.increment(); }

  /** Replaces only bounded watermarks; it never stores Lease IDs, keys, or credentials. */
  public void recordLeaseExpiryCheckpoint(long ownerEpoch, long sequence, long windowStopExclusive) {
    if (ownerEpoch < 0 || sequence < 0 || windowStopExclusive < 0) {
      throw new IllegalArgumentException("Lease expiry watermarks must not be negative");
    }
    leaseExpiryOwnerEpoch.set(ownerEpoch);
    leaseExpiryCheckpointSequence.set(sequence);
    leaseExpiryWindowStopExclusive.set(windowStopExclusive);
  }

  public LeaseExpirySnapshot leaseExpiry() {
    return new LeaseExpirySnapshot(leaseExpiryScanRows.sum(), leaseExpiryScanBytes.sum(),
        leaseExpiryCandidates.sum(), leaseExpiryStaleCandidates.sum(), leaseExpiryRevocations.sum(),
        leaseExpiryDelayMillis.sum(), leaseExpiryDelaySamplesMissing.sum(), leaseExpiryRetries.sum(),
        leaseExpiryOwnerEpoch.get(), leaseExpiryCheckpointSequence.get(),
        leaseExpiryWindowStopExclusive.get());
  }

  public void recordPutStage(PutStage stage, long elapsedNanos) {
    Objects.requireNonNull(stage, "stage");
    if (elapsedNanos < 0) throw new IllegalArgumentException("elapsedNanos must not be negative");
    var counter = putStages.get(stage);
    counter.count.increment();
    counter.totalNanos.add(elapsedNanos);
  }

  /** Counts protobuf response bytes accepted by onNext, excluding transport framing. */
  public void recordPutOutputBytes(long bytes) {
    if (bytes < 0) throw new IllegalArgumentException("output bytes must not be negative");
    putOutputBytes.add(bytes);
  }

  public long putOutputBytes() { return putOutputBytes.sum(); }

  public Map<PutStage, StageSnapshot> putStages() {
    Map<PutStage, StageSnapshot> snapshot = new EnumMap<>(PutStage.class);
    putStages.forEach((stage, counter) -> snapshot.put(stage,
        new StageSnapshot(counter.count.sum(), counter.totalNanos.sum())));
    return Map.copyOf(snapshot);
  }

  @Override
  public void recordCompletion(
      MethodClass methodClass,
      Outcome outcome,
      String traceId,
      RequestScope scope,
      long elapsedNanos) {
    Objects.requireNonNull(methodClass, "methodClass");
    Objects.requireNonNull(outcome, "outcome");
    Objects.requireNonNull(scope, "scope");
    if (traceId == null || !TRACE_ID.matcher(traceId).matches())
      throw new IllegalArgumentException("traceId must be a generated UUID");
    if (elapsedNanos < 0) throw new IllegalArgumentException("elapsedNanos must not be negative");
    Counter counter = counters.get(new Series(methodClass, outcome));
    counter.completed.increment();
    counter.totalElapsedNanos.add(elapsedNanos);
    counter.cleanupFailures.add(scope.cleanupFailureCount());
    // Routine completions, including rejection/UNIMPLEMENTED, are represented by metrics.
    if (outcome != Outcome.BACKEND_FAILURE && scope.cleanupFailureCount() == 0) return;
    if (!reserveWarning()) return;
    Map<Series, Snapshot> totals = snapshot();
    long unexpected =
        totals.entrySet().stream()
            .filter(entry -> entry.getKey().outcome() == Outcome.BACKEND_FAILURE)
            .mapToLong(entry -> entry.getValue().completed())
            .sum();
    long cleanup = totals.values().stream().mapToLong(Snapshot::cleanupFailures).sum();
    long completed = totals.values().stream().mapToLong(Snapshot::completed).sum();
    warnings.accept(
        new FailureSummary(
            methodClass,
            outcome,
            traceId,
            elapsedNanos,
            scope.hasDeadline() ? scope.remainingDeadlineNanos() : -1,
            unexpected,
            cleanup,
            completed));
  }

  private boolean reserveWarning() {
    long now = nanoTime.getAsLong();
    long previous = lastWarningNanos.get();
    return now - previous >= WARNING_INTERVAL_NANOS
        && lastWarningNanos.compareAndSet(previous, now);
  }

  private static void logSummary(FailureSummary summary) {
    LOGGER.warn(
        "request_failure_summary method_class={} outcome={} elapsed_nanos={}"
            + " deadline_remaining_nanos={} trace_id={} unexpected_failure_total={}"
            + " cleanup_failure_total={} completed_total={}",
        summary.methodClass(),
        summary.outcome(),
        summary.elapsedNanos(),
        summary.remainingNanos(),
        summary.traceId(),
        summary.unexpectedFailures(),
        summary.cleanupFailures(),
        summary.completed());
  }

  public Map<Series, Snapshot> snapshot() {
    Map<Series, Snapshot> snapshot = new HashMap<>();
    counters.forEach(
        (series, counter) ->
            snapshot.put(
                series,
                new Snapshot(
                    counter.completed.sum(),
                    counter.totalElapsedNanos.sum(),
                    counter.cleanupFailures.sum())));
    return Map.copyOf(snapshot);
  }

  public record Series(MethodClass methodClass, Outcome outcome) {
    public Series {
      Objects.requireNonNull(methodClass, "methodClass");
      Objects.requireNonNull(outcome, "outcome");
    }
  }

  public record Snapshot(long completed, long totalElapsedNanos, long cleanupFailures) {}

  public record StageSnapshot(long count, long totalNanos) {}
  public record LeaseExpirySnapshot(long scanRows, long scanBytes, long candidates,
      long staleCandidates, long revocations, long totalDelayMillis, long missingDelaySamples, long retries,
      long ownerEpoch, long checkpointSequence, long windowStopExclusive) {}

  public record LeaseRenewalSnapshot(long requests, long responses, long selected,
      long unknownOutcomes, long requestBytes, long responseBytes, long auxiliaryRecords,
      long auxiliaryPayloadBytes, long hbaseClientOperations,
      long logicalHBaseRequestPayloadBytes,
      Map<LeaseRenewalStage, StageSnapshot> stages) {
    public LeaseRenewalSnapshot {
      stages = Map.copyOf(Objects.requireNonNull(stages, "stages"));
    }
  }

  record FailureSummary(
      MethodClass methodClass,
      Outcome outcome,
      String traceId,
      long elapsedNanos,
      long remainingNanos,
      long unexpectedFailures,
      long cleanupFailures,
      long completed) {}

  private static final class Counter {
    private final LongAdder cleanupFailures = new LongAdder();
    private final LongAdder completed = new LongAdder();
    private final LongAdder totalElapsedNanos = new LongAdder();
  }

  private static final class StageCounter {
    private final LongAdder count = new LongAdder();
    private final LongAdder totalNanos = new LongAdder();
  }
}
