package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventReference;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.ProtectedEventCursor;
import io.etcdhbase.core.commit.SelectedSequenceReader;
import io.etcdhbase.core.commit.SnapshotPinManager;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.server.observability.RequestMetrics;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Bounded, inclusive replay of selected Watch revisions behind one durable history pin. */
final class WatchHistoryReplay implements AutoCloseable {
  private static final Runnable NO_WAKEUP = () -> {};

  record OpenResult(long revision, long sequence, long compactRevision,
      Optional<WatchHistoryReplay> replay,
      Optional<RequestScope.ScopedCleanup> cleanup) {
    OpenResult {
      if (revision < 0 || sequence < 0 || compactRevision < 0) {
        throw new IllegalArgumentException("invalid Watch replay open result");
      }
      Objects.requireNonNull(replay, "replay");
      Objects.requireNonNull(cleanup, "cleanup");
    }

    void closeUntransferredReplay() {
      if (cleanup.isPresent()) {
        try {
          cleanup.orElseThrow().close();
        } catch (Exception failure) {
          throw new CompletionException(failure);
        }
      } else {
        replay.ifPresent(WatchHistoryReplay::close);
      }
    }
  }

  private static final int MAX_PAGES_PER_REVISION = 512;
  private static final long MAX_BYTES_PER_REVISION = 64L << 20;
  private static final int MAX_TOTAL_REVISIONS = 4096;
  private static final int MAX_TOTAL_PAGES = 16_384;
  private static final long MAX_TOTAL_PROOF_BYTES = 512L << 20;
  private static final int MAX_EVENTS_PER_REVISION = 4096;
  private static final long MAX_REVISION_EVENT_BYTES = 16L << 20;
  private static final String RESOURCE_LIMIT_REASON =
      "etcd-hbase: Watch history replay resource limit exceeded";

  private final String tableGroup;
  private final KeyRange range;
  private final long barrierRevision;
  private final long barrierSequence;
  private final ByteKey authorization;
  private final EventHistoryPinManager pins;
  private final SelectedSequenceReader selected;
  private final BudgetManager budgets;
  private final BudgetManager.Account watchAccount;
  private final RequestScope pinScope = RequestScope.withoutDeadline();
  private final ArrayDeque<WatchStream.Delivery> pending = new ArrayDeque<>();
  private final CompletableFuture<Void> settled = new CompletableFuture<>();

  private EventHistoryPinManager.Protection protection;
  private long nextRevision;
  private int readRevisions;
  private int readPages;
  private long readBytes;
  private boolean loading;
  private boolean complete;
  private boolean closed;
  private WatchTermination termination;
  private CompletableFuture<Void> pinRelease;
  private Runnable wakeup = NO_WAKEUP;
  private CompletableFuture<?> inFlight;
  private RequestScope activeReadScope;

  private WatchHistoryReplay(String tableGroup, KeyRange range, long nextRevision,
      long barrierRevision, long barrierSequence,
      ByteKey authorization, EventHistoryPinManager pins, SelectedSequenceReader selected,
      BudgetManager budgets,
      BudgetManager.Account watchAccount) {
    this.tableGroup = tableGroup;
    this.range = range;
    this.barrierRevision = barrierRevision;
    this.barrierSequence = barrierSequence;
    this.authorization = authorization;
    this.pins = pins;
    this.selected = selected;
    this.budgets = budgets;
    this.watchAccount = watchAccount;
    this.nextRevision = nextRevision;
  }

  private CompletableFuture<Void> installPin(EventHistoryPinManager.Protection registered) {
    Objects.requireNonNull(registered, "registered");
    synchronized (this) {
      if (!closed && protection == null && pinRelease == null) {
        protection = registered;
        return CompletableFuture.completedFuture(null);
      }
    }
    try {
      registered.close();
    } catch (RuntimeException | Error failure) {
      return CompletableFuture.failedFuture(failure);
    }
    return registered.releaseCompletion().thenCompose(ignored -> CompletableFuture.<Void>failedFuture(
        new BackendFailure(BackendFailure.Category.CANCELLED))).toCompletableFuture();
  }

  private CompletableFuture<EventHistoryPinManager.Protection> registerPin(
      EventHistoryPinManager.Requirement requirement) {
    var result = new CompletableFuture<EventHistoryPinManager.Protection>();
    boolean started;
    try {
      started = pinScope.startIfOpen(() -> {
        try {
          pins.registerProtection(requirement, pinScope).whenComplete((registered, failure) -> {
            if (failure == null) result.complete(registered);
            else result.completeExceptionally(unwrap(failure));
          });
        } catch (RuntimeException | Error failure) {
          result.completeExceptionally(failure);
        }
      });
    } catch (RuntimeException | Error failure) {
      return CompletableFuture.failedFuture(failure);
    }
    if (!started) {
      result.completeExceptionally(new BackendFailure(BackendFailure.Category.CANCELLED));
    }
    return result;
  }

  static CompletableFuture<OpenResult> open(String tableGroup, KeyRange range,
      long startRevision, ControlSnapshot barrier, long barrierSequence, ByteKey authorization,
      EventHistoryPinManager pins, SelectedSequenceReader selected,
      BudgetManager budgets,
      BudgetManager.Account watchAccount) {
    return open(tableGroup, range, startRevision, barrier, barrierSequence, authorization,
        pins, selected, budgets, watchAccount, null);
  }

  static CompletableFuture<OpenResult> open(String tableGroup, KeyRange range,
      long startRevision, ControlSnapshot barrier, long barrierSequence, ByteKey authorization,
      EventHistoryPinManager pins, SelectedSequenceReader selected,
      BudgetManager budgets,
      BudgetManager.Account watchAccount, RequestScope streamScope) {
    return open(tableGroup, range, startRevision, barrier, barrierSequence, authorization, pins,
        selected, budgets, watchAccount, streamScope, ignored -> {});
  }

  static CompletableFuture<OpenResult> open(String tableGroup, KeyRange range,
      long startRevision, ControlSnapshot barrier, long barrierSequence, ByteKey authorization,
      EventHistoryPinManager pins, SelectedSequenceReader selected,
      BudgetManager budgets,
      BudgetManager.Account watchAccount, RequestScope streamScope,
      Consumer<RequestMetrics.WatchReplayOpenResourceFailureStage> resourceFailureObserver) {
    return open(tableGroup, range, startRevision, barrier, barrierSequence, authorization,
        pins, selected, budgets, watchAccount, streamScope, resourceFailureObserver, ignored -> {});
  }

  static CompletableFuture<OpenResult> open(String tableGroup, KeyRange range,
      long startRevision, ControlSnapshot barrier, long barrierSequence, ByteKey authorization,
      EventHistoryPinManager pins, SelectedSequenceReader selected,
      BudgetManager budgets,
      BudgetManager.Account watchAccount, RequestScope streamScope,
      Consumer<RequestMetrics.WatchReplayOpenResourceFailureStage> resourceFailureObserver,
      Consumer<BudgetManager.ReservedAdmissionFailureSnapshot> admissionFailureObserver) {
    Objects.requireNonNull(barrier, "barrier");
    Objects.requireNonNull(authorization, "authorization");
    Objects.requireNonNull(resourceFailureObserver, "resourceFailureObserver");
    Objects.requireNonNull(admissionFailureObserver, "admissionFailureObserver");
    if (startRevision <= 0 || barrier.state().publishedRevision() < startRevision
        || barrierSequence != barrier.state().publishedSequence()
        || authorization.length() != 32) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid Watch replay barrier"));
    }
    // Revision 1 is the initialized empty store; the first KV-changing revision is 2.
    long firstEventRevision = Math.max(2, startRevision);
    if (firstEventRevision > barrier.state().publishedRevision()) {
      var replay = new WatchHistoryReplay(tableGroup, range, barrier.state().publishedRevision() + 1,
          barrier.state().publishedRevision(), barrierSequence, authorization,
          pins, selected, budgets, watchAccount);
      replay.complete = true;
      replay.settled.complete(null);
      return CompletableFuture.completedFuture(new OpenResult(
          barrier.state().publishedRevision(), barrierSequence, barrier.state().compactRevision(),
          Optional.of(replay), Optional.empty()));
    }

    RequestScope lookupScope = RequestScope.withoutDeadline();
    BudgetManager.Account lookupAccount;
    try {
      lookupAccount = budgets.admitReserved(tableGroup, lookupScope,
          LeaseBudgetProfile.EVENT_TAILER_READ_MEMORY, LeaseBudgetProfile.EVENT_TAILER_READ_LIMITS,
          admissionFailureObserver);
    } catch (BackendFailure failure) {
      lookupScope.close();
      recordResourceFailure(resourceFailureObserver,
          RequestMetrics.WatchReplayOpenResourceFailureStage.LOOKUP_ACCOUNT_ADMISSION, failure);
      return CompletableFuture.failedFuture(failure);
    }
    var lookupBudget = new ManifestCatalog.ProofReadBudget(
        lookupScope, lookupAccount, MAX_PAGES_PER_REVISION, MAX_BYTES_PER_REVISION);
    var replay = new WatchHistoryReplay(tableGroup, range, firstEventRevision,
        barrier.state().publishedRevision(), barrierSequence, authorization,
        pins, selected, budgets, watchAccount);
    var opening = new ReplayOpening(lookupScope, replay);
    Optional<RequestScope.ScopedCleanup> cleanup = streamScope == null
        ? Optional.empty()
        : Optional.of(streamScope.registerScopedCleanup(opening, opening.cleanupCompletion()));

    CompletableFuture<Long> located;
    try {
      located = replay.readSequence(firstEventRevision, barrier.state(), lookupScope, lookupBudget);
    } catch (RuntimeException | Error failure) {
      recordResourceFailure(resourceFailureObserver,
          RequestMetrics.WatchReplayOpenResourceFailureStage.SEQUENCE_LOOKUP, failure);
      lookupAccount.close();
      lookupScope.close();
      opening.markOpeningFinished();
      if (failure instanceof Error fatal) throw fatal;
      return closeFailedOpen(opening, cleanup, failure);
    }
    CompletableFuture<Long> drainedLookup = drainLookup(located, lookupScope, lookupAccount);
    CompletableFuture<OpenResult> opened = drainedLookup.handle((sequence, lookupFailure) -> {
      if (lookupFailure != null) {
        Throwable cause = unwrap(lookupFailure);
        recordResourceFailure(resourceFailureObserver,
            RequestMetrics.WatchReplayOpenResourceFailureStage.SEQUENCE_LOOKUP, cause);
        return replay.openIfCompactedOrFail(startRevision, cause, cleanup,
            resourceFailureObserver, admissionFailureObserver);
      }
      var requirement = new EventHistoryPinManager.Requirement(firstEventRevision, sequence,
          authorization, Optional.empty(), Optional.empty());
      return replay.registerPin(requirement).handle((registered, registrationFailure) -> {
        if (registrationFailure != null) {
          Throwable cause = unwrap(registrationFailure);
          recordResourceFailure(resourceFailureObserver,
              RequestMetrics.WatchReplayOpenResourceFailureStage.PIN_REGISTRATION, cause);
          return replay.openIfCompactedOrFail(startRevision, cause, cleanup,
              resourceFailureObserver, admissionFailureObserver);
        }
        return replay.finishOpen(registered, barrier, startRevision, sequence, barrierSequence,
            cleanup, resourceFailureObserver, admissionFailureObserver);
      }).thenCompose(java.util.function.Function.identity());
    }).thenCompose(java.util.function.Function.identity());
    var result = new CompletableFuture<OpenResult>();
    opened.whenComplete((value, failure) -> {
      opening.markOpeningFinished();
      if (failure == null) {
        result.complete(value);
        return;
      }
      Throwable cause = unwrap(failure);
      closeOpening(opening, cleanup).whenComplete((ignored, releaseFailure) -> {
        if (releaseFailure != null) {
          Throwable cleanupFailure = unwrap(releaseFailure);
          if (cleanupFailure != cause) cause.addSuppressed(cleanupFailure);
        }
        result.completeExceptionally(cause);
      });
    });
    return result;
  }

  private static void recordResourceFailure(
      Consumer<RequestMetrics.WatchReplayOpenResourceFailureStage> observer,
      RequestMetrics.WatchReplayOpenResourceFailureStage stage, Throwable failure) {
    Throwable cause = failure == null ? null : unwrap(failure);
    if (stage != null && cause instanceof BackendFailure backendFailure
        && backendFailure.category() == BackendFailure.Category.RESOURCE_LIMIT) {
      observer.accept(stage);
    }
  }

  private static <T> CompletableFuture<T> observeResourceFailure(
      Supplier<CompletableFuture<T>> operation,
      Consumer<RequestMetrics.WatchReplayOpenResourceFailureStage> observer,
      RequestMetrics.WatchReplayOpenResourceFailureStage stage) {
    try {
      return Objects.requireNonNull(operation.get(), "Watch replay open stage future")
          .whenComplete((ignored, failure) -> recordResourceFailure(observer, stage, failure));
    } catch (RuntimeException | Error failure) {
      recordResourceFailure(observer, stage, failure);
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletableFuture<OpenResult> finishOpen(EventHistoryPinManager.Protection registered,
      ControlSnapshot barrier, long startRevision, long sequence, long barrierSequence,
      Optional<RequestScope.ScopedCleanup> cleanup,
      Consumer<RequestMetrics.WatchReplayOpenResourceFailureStage> resourceFailureObserver,
      Consumer<BudgetManager.ReservedAdmissionFailureSnapshot> admissionFailureObserver) {
    return installPin(registered)
        .thenCompose(ignored -> observeResourceFailure(() -> registered.renew(pinScope),
            resourceFailureObserver,
            RequestMetrics.WatchReplayOpenResourceFailureStage.PIN_RENEWAL))
        .thenCompose(ignored -> withReadScope(
            (scope, budget) -> readControl(scope, budget), resourceFailureObserver,
            RequestMetrics.WatchReplayOpenResourceFailureStage.CONTROL_REVALIDATION_ACCOUNT_ADMISSION,
            RequestMetrics.WatchReplayOpenResourceFailureStage.CONTROL_REVALIDATION_READ,
            admissionFailureObserver))
        .thenCompose(current -> {
          if (current.publishedRevision() < barrier.state().publishedRevision()) {
            return CompletableFuture.failedFuture(
                new BackendFailure(BackendFailure.Category.CANCELLED));
          }
          if (current.compactRevision() > startRevision) {
            return completeCompactedOpen().thenApply(ignored -> new OpenResult(
                current.publishedRevision(), current.publishedSequence(),
                current.compactRevision(), Optional.empty(), cleanup));
          }
          return withReadScope((scope, budget) -> readSequence(
              Math.max(2, startRevision), current, scope, budget), resourceFailureObserver,
              RequestMetrics.WatchReplayOpenResourceFailureStage.SEQUENCE_REVALIDATION_ACCOUNT_ADMISSION,
              RequestMetrics.WatchReplayOpenResourceFailureStage.SEQUENCE_REVALIDATION_READ,
              admissionFailureObserver)
              .thenApply(verified -> {
                if (verified.longValue() != sequence || verified > barrierSequence) {
                  throw new CompletionException(new IllegalStateException(
                      "Watch replay sequence changed while registering its history pin"));
                }
                return new OpenResult(barrier.state().publishedRevision(), barrierSequence,
                    barrier.state().compactRevision(), Optional.of(this), cleanup);
              });
        });
  }

  /**
   * A concurrent Compact may delete an index or advance the pin registration floor between the
   * initial index lookup and pin registration. Re-read the authoritative control row so only a
   * committed compact boundary turns that failure into the protocol's Created + Canceled result.
   */
  private CompletableFuture<OpenResult> openIfCompactedOrFail(long startRevision,
      Throwable openingFailure, Optional<RequestScope.ScopedCleanup> cleanup,
      Consumer<RequestMetrics.WatchReplayOpenResourceFailureStage> resourceFailureObserver,
      Consumer<BudgetManager.ReservedAdmissionFailureSnapshot> admissionFailureObserver) {
    if (!(openingFailure instanceof SelectedSequenceReader.MissingFinalizedRevisionIndex)
        && !(openingFailure instanceof SnapshotPinManager.RegistrationFloorExceeded)) {
      return CompletableFuture.failedFuture(openingFailure);
    }
    synchronized (this) {
      if (closed) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.CANCELLED));
      }
    }
    return withReadScope((scope, budget) -> readControl(scope, budget), resourceFailureObserver,
        RequestMetrics.WatchReplayOpenResourceFailureStage.CONTROL_REVALIDATION_ACCOUNT_ADMISSION,
        RequestMetrics.WatchReplayOpenResourceFailureStage.CONTROL_REVALIDATION_READ,
        admissionFailureObserver)
        .handle((current, stateFailure) -> {
          if (stateFailure != null) {
            Throwable checkFailure = unwrap(stateFailure);
            if (openingFailure != checkFailure) checkFailure.addSuppressed(openingFailure);
            return CompletableFuture.<OpenResult>failedFuture(checkFailure);
          }
          if (current.compactRevision() <= startRevision) {
            return CompletableFuture.<OpenResult>failedFuture(openingFailure);
          }
          return completeCompactedOpen().thenApply(ignored -> new OpenResult(
              current.publishedRevision(), current.publishedSequence(), current.compactRevision(),
              Optional.empty(), cleanup));
        }).thenCompose(java.util.function.Function.identity());
  }

  private static CompletableFuture<Long> drainLookup(CompletableFuture<Long> lookup,
      RequestScope scope, BudgetManager.Account account) {
    var result = new CompletableFuture<Long>();
    lookup.whenComplete((sequence, failure) -> {
      Throwable problem = failure == null ? null : unwrap(failure);
      try {
        if (problem == null) scope.close();
        else scope.cancel();
      } catch (RuntimeException | Error closeFailure) {
        if (problem == null) problem = closeFailure;
        else if (problem != closeFailure) problem.addSuppressed(closeFailure);
      }
      Throwable observed = problem;
      scope.physicalProducersCompletion().whenComplete((ignored, producerFailure) -> {
        Throwable settledFailure = observed;
        if (producerFailure != null) {
          Throwable producer = unwrap(producerFailure);
          if (settledFailure == null) settledFailure = producer;
          else if (settledFailure != producer) settledFailure.addSuppressed(producer);
        }
        try {
          account.close();
        } catch (RuntimeException | Error accountFailure) {
          if (settledFailure == null) settledFailure = accountFailure;
          else if (settledFailure != accountFailure) settledFailure.addSuppressed(accountFailure);
        }
        if (settledFailure == null) result.complete(sequence);
        else result.completeExceptionally(settledFailure);
      });
    });
    return result;
  }

  private static CompletableFuture<OpenResult> closeFailedOpen(ReplayOpening opening,
      Optional<RequestScope.ScopedCleanup> cleanup, Throwable failure) {
    CompletableFuture<Void> closed = closeOpening(opening, cleanup);
    var result = new CompletableFuture<OpenResult>();
    closed.whenComplete((ignored, closeFailure) -> {
      if (closeFailure != null) {
        Throwable cleanupFailure = unwrap(closeFailure);
        if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
      }
      result.completeExceptionally(failure);
    });
    return result;
  }

  private static CompletableFuture<Void> closeOpening(ReplayOpening opening,
      Optional<RequestScope.ScopedCleanup> cleanup) {
    if (cleanup.isPresent()) {
      try {
        cleanup.orElseThrow().close();
      } catch (Exception failure) {
        return CompletableFuture.failedFuture(failure);
      }
      return opening.cleanupCompletion().toCompletableFuture();
    }
    opening.close();
    return opening.cleanupCompletion().toCompletableFuture();
  }

  private static final class ReplayOpening implements AutoCloseable {
    private final RequestScope lookupScope;
    private final WatchHistoryReplay replay;
    private final CompletableFuture<Void> openingFinished = new CompletableFuture<>();
    private final CompletableFuture<Void> cleanupCompletion = new CompletableFuture<>();
    private boolean closeStarted;
    private boolean cleanupStarted;
    private Throwable closeFailure;

    private ReplayOpening(RequestScope lookupScope, WatchHistoryReplay replay) {
      this.lookupScope = Objects.requireNonNull(lookupScope, "lookupScope");
      this.replay = Objects.requireNonNull(replay, "replay");
    }

    @Override
    public void close() {
      synchronized (this) {
        if (closeStarted) return;
        closeStarted = true;
      }
      try {
        lookupScope.cancel();
      } catch (RuntimeException | Error failure) {
        recordCloseFailure(failure);
      }
      try {
        replay.close();
      } catch (RuntimeException | Error failure) {
        recordCloseFailure(failure);
      }
      completeCleanupWhenDrained();
    }

    private void markOpeningFinished() {
      openingFinished.complete(null);
      completeCleanupWhenDrained();
    }

    private CompletionStage<Void> cleanupCompletion() {
      return cleanupCompletion.minimalCompletionStage();
    }

    private void recordCloseFailure(Throwable failure) {
      synchronized (this) {
        if (closeFailure == null) closeFailure = failure;
        else if (closeFailure != failure) closeFailure.addSuppressed(failure);
      }
    }

    private void completeCleanupWhenDrained() {
      synchronized (this) {
        if (!closeStarted || cleanupStarted || !openingFinished.isDone()) return;
        cleanupStarted = true;
      }
      CompletableFuture.allOf(lookupScope.physicalProducersCompletion().toCompletableFuture(),
          replay.settled().toCompletableFuture()).whenComplete((ignored, drainFailure) -> {
            Throwable failure;
            synchronized (ReplayOpening.this) {
              failure = closeFailure;
            }
            if (drainFailure != null) {
              Throwable drain = unwrap(drainFailure);
              if (failure == null) failure = drain;
              else if (failure != drain) failure.addSuppressed(drain);
            }
            if (failure == null) cleanupCompletion.complete(null);
            else cleanupCompletion.completeExceptionally(failure);
          });
    }
  }

  synchronized void setWakeup(Runnable wakeup) {
    this.wakeup = wakeup == null ? NO_WAKEUP : wakeup;
  }

  synchronized Optional<WatchStream.Delivery> poll() {
    if (closed) return Optional.empty();
    WatchStream.Delivery delivery = pending.pollFirst();
    if (delivery != null) return Optional.of(delivery);
    if (!complete && termination == null && !loading) startNextRevision();
    return Optional.empty();
  }

  synchronized Optional<WatchTermination> termination() {
    return Optional.ofNullable(termination);
  }

  synchronized boolean complete() { return complete; }

  private synchronized void startNextRevision() {
    if (closed || complete || termination != null || loading) return;
    if (nextRevision > barrierRevision) {
      complete = true;
      settleAfterPinRelease(null);
      return;
    }
    if (readRevisions >= MAX_TOTAL_REVISIONS || readPages >= MAX_TOTAL_PAGES
        || readBytes >= MAX_TOTAL_PROOF_BYTES) {
      terminate(WatchTermination.resourceLimit(RESOURCE_LIMIT_REASON));
      return;
    }
    loading = true;
    long revision = nextRevision;
    EventHistoryPinManager.Protection replayProtection = protection;
    if (replayProtection == null) {
      terminate(WatchTermination.failure(new IllegalStateException(
          "Watch replay lost its durable history protection")));
      return;
    }
    CompletableFuture<List<WatchStream.Delivery>> read = withReadScope((scope, budget) ->
        replayProtection.renew(pinScope).thenCompose(ignored -> readControl(scope, budget))
            .thenCompose(state -> {
              if (state.compactRevision() > revision) {
                throw new CompletionException(new CompactedHistory(state.compactRevision()));
              }
              if (state.publishedRevision() < barrierRevision) {
                throw new CompletionException(new BackendFailure(BackendFailure.Category.CANCELLED));
              }
              return readSequence(revision, state, scope, budget).thenCompose(sequence ->
                  sequence <= barrierSequence
                      ? readRevision(revision, sequence, scope, budget, replayProtection)
                      : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE)));
            }));
    inFlight = read;
    read.whenComplete((events, failure) -> finishRevision(revision, events, failure, read));
  }

  private void finishRevision(long revision, List<WatchStream.Delivery> events,
      Throwable failure, CompletableFuture<?> read) {
    Runnable signal;
    synchronized (this) {
      if (inFlight == read) inFlight = null;
      loading = false;
      if (closed) {
        Throwable cleanupFailure = collectDeliveryCloseFailures(events, null);
        if (cleanupFailure != null) settleAfterPinRelease(cleanupFailure);
        return;
      }
      if (failure != null) {
        terminate(reason(failure));
        signal = wakeup;
      } else {
        pending.addAll(Objects.requireNonNull(events, "replayed events"));
        nextRevision = Math.addExact(revision, 1);
        readRevisions++;
        signal = wakeup;
      }
    }
    signal.run();
  }

  private CompletableFuture<Long> readSequence(long revision, CommitState state,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget) {
    return selected.sequenceForRevision(revision, state, scope, budget);
  }

  private CompletableFuture<CommitState> readControl(RequestScope scope,
      ManifestCatalog.ProofReadBudget budget) {
    return selected.readPublishedState(scope, budget);
  }

  private CompletableFuture<List<WatchStream.Delivery>> readRevision(long revision, long sequence,
      RequestScope scope, ManifestCatalog.ProofReadBudget budget,
      EventHistoryPinManager.Protection replayProtection) {
    if (revision <= 1) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "event revisions must follow the initialized revision"));
    }
    return selected.openUnderProtection(sequence, revision - 1, authorization,
        replayProtection, scope, budget).thenCompose(found -> {
      var chosen = found.orElseThrow(() -> new CompletionException(
          new BackendFailure(BackendFailure.Category.UNAVAILABLE)));
      if (chosen.decision().sequence() != sequence || chosen.decision().revision() != revision
          || chosen.decision().baseRevision() != revision - 1 || chosen.events().isEmpty()
          || chosen.index().isEmpty()) {
        chosen.events().ifPresent(ProtectedEventCursor::close);
        return CompletableFuture.failedFuture(new CompletionException(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE)));
      }
      EventReference reference = chosen.index().orElseThrow().events();
      if (reference.itemCount() > MAX_EVENTS_PER_REVISION
          || reference.totalBytes() > MAX_REVISION_EVENT_BYTES) {
        chosen.events().orElseThrow().close();
        return CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      var deliveries = new ArrayList<WatchStream.Delivery>();
      long[] retainedBytes = {0};
      long[] observed = {0};
      CompletableFuture<Void> reads = CompletableFuture.completedFuture(null);
      for (long ordinal = 0; ordinal < reference.itemCount(); ordinal++) {
        long expectedOrdinal = ordinal;
        reads = reads.thenCompose(ignored -> chosen.events().orElseThrow().next(budget)
            .thenAccept(event -> {
              PreparedEvent prepared = event.orElseThrow(() -> new CompletionException(
                  new IllegalStateException("sealed Watch revision ended before its event count")));
              if (prepared.ordinal() != expectedOrdinal
                  || prepared.current().metadata().modificationRevision() != revision) {
                throw new CompletionException(new IllegalStateException(
                    "sealed Watch revision is not in contiguous ordinal order"));
              }
              observed[0]++;
              if (!range.contains(prepared.current().key())) return;
              long bytes = prepared.retainedBytes();
              if (deliveries.size() >= MAX_EVENTS_PER_REVISION
                  || bytes > MAX_REVISION_EVENT_BYTES - retainedBytes[0]) {
                throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
              }
              BudgetManager.Lease lease;
              try {
                lease = watchAccount.tryRetain(BudgetManager.Memory.OUTPUT, bytes)
                    .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
              } catch (BackendFailure failure) {
                throw new CompletionException(failure);
              }
              retainedBytes[0] += bytes;
              try {
                deliveries.add(new WatchStream.Delivery(sequence, revision, prepared, lease::close));
              } catch (RuntimeException | Error failure) {
                lease.close();
                throw failure;
              }
            }));
      }
      reads = reads.thenCompose(ignored -> chosen.events().orElseThrow().next(budget)
          .thenAccept(extra -> {
            if (extra.isPresent() || observed[0] != reference.itemCount()) {
              throw new CompletionException(new IllegalStateException(
                  "sealed Watch revision event count differs from its authenticated root"));
            }
          }));
      var result = new CompletableFuture<List<WatchStream.Delivery>>();
      reads.whenComplete((ignored, failure) -> {
        ProtectedEventCursor cursor = chosen.events().orElseThrow();
        cursor.close();
        cursor.releaseCompletion().whenComplete((released, releaseFailure) -> {
          Throwable problem = failure == null ? null : unwrap(failure);
          if (releaseFailure != null) {
            Throwable release = unwrap(releaseFailure);
            if (problem == null) problem = release;
            else if (problem != release) problem.addSuppressed(release);
          }
          if (problem != null) {
            problem = collectDeliveryCloseFailures(deliveries, problem);
            result.completeExceptionally(problem);
          } else {
            result.complete(List.copyOf(deliveries));
          }
        });
      });
      return result;
    });
  }

  private <T> CompletableFuture<T> withReadScope(
      java.util.function.BiFunction<RequestScope, ManifestCatalog.ProofReadBudget,
          CompletableFuture<T>> operation) {
    return withReadScope(operation, null, null, null, null);
  }

  private <T> CompletableFuture<T> withReadScope(
      java.util.function.BiFunction<RequestScope, ManifestCatalog.ProofReadBudget,
          CompletableFuture<T>> operation,
      Consumer<RequestMetrics.WatchReplayOpenResourceFailureStage> resourceFailureObserver,
      RequestMetrics.WatchReplayOpenResourceFailureStage admissionFailureStage,
      RequestMetrics.WatchReplayOpenResourceFailureStage operationFailureStage,
      Consumer<BudgetManager.ReservedAdmissionFailureSnapshot> admissionFailureObserver) {
    RequestScope scope = RequestScope.withoutDeadline();
    BudgetManager.Account account;
    try {
      account = admissionFailureObserver == null
          ? budgets.admitReserved(tableGroup, scope, LeaseBudgetProfile.EVENT_TAILER_READ_MEMORY,
              LeaseBudgetProfile.EVENT_TAILER_READ_LIMITS)
          : budgets.admitReserved(tableGroup, scope, LeaseBudgetProfile.EVENT_TAILER_READ_MEMORY,
              LeaseBudgetProfile.EVENT_TAILER_READ_LIMITS, admissionFailureObserver);
    } catch (BackendFailure failure) {
      scope.close();
      recordResourceFailure(resourceFailureObserver, admissionFailureStage, failure);
      return CompletableFuture.failedFuture(failure);
    }
    var budget = new ManifestCatalog.ProofReadBudget(
        scope, account, MAX_PAGES_PER_REVISION, MAX_BYTES_PER_REVISION);
    CompletableFuture<T> work;
    synchronized (this) {
      if (closed) {
        account.close();
        scope.close();
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.CANCELLED));
      }
      activeReadScope = scope;
    }
    try {
      work = Objects.requireNonNull(operation.apply(scope, budget), "Watch replay read future");
    } catch (RuntimeException | Error failure) {
      recordResourceFailure(resourceFailureObserver, operationFailureStage, failure);
      account.close();
      scope.close();
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    var result = new CompletableFuture<T>();
    work.whenComplete((value, failure) -> {
      readPages = Math.addExact(readPages, budget.pages());
      readBytes = Math.addExact(readBytes, budget.bytes());
      Throwable cause = failure == null ? null : unwrap(failure);
      recordResourceFailure(resourceFailureObserver, operationFailureStage, cause);
      if (failure != null) scope.cancel(); else scope.close();
      scope.physicalProducersCompletion().whenComplete((ignored, drainFailure) -> {
        Throwable settledCleanupFailure = null;
        try { account.close(); } catch (RuntimeException | Error problem) { settledCleanupFailure = problem; }
        if (drainFailure != null) {
          Throwable drain = unwrap(drainFailure);
          if (settledCleanupFailure == null) settledCleanupFailure = drain;
          else if (settledCleanupFailure != drain) settledCleanupFailure.addSuppressed(drain);
        }
        synchronized (WatchHistoryReplay.this) {
          if (activeReadScope == scope) activeReadScope = null;
        }
        Throwable settledFailure = cause;
        if (settledCleanupFailure != null) {
          if (settledFailure == null) settledFailure = settledCleanupFailure;
          else if (settledFailure != settledCleanupFailure) settledFailure.addSuppressed(settledCleanupFailure);
        }
        if (settledFailure == null) result.complete(value);
        else result.completeExceptionally(settledFailure);
      });
    });
    result.whenComplete((ignored, failure) -> { if (result.isCancelled()) scope.cancel(); });
    return result;
  }

  private void terminate(WatchTermination reason) {
    if (termination != null || closed) return;
    termination = reason;
    Throwable cleanupFailure = collectDeliveryCloseFailures(pending, null);
    pending.clear();
    settleAfterPinRelease(cleanupFailure);
  }

  private synchronized CompletionStage<Void> releasePin() {
    if (pinRelease != null) return pinRelease.minimalCompletionStage();
    CompletableFuture<Void> release = new CompletableFuture<>();
    pinRelease = release;
    EventHistoryPinManager.Protection current = protection;
    protection = null;
    if (current == null) {
      pinScope.close();
      release.complete(null);
      return release.minimalCompletionStage();
    }
    try {
      current.close();
      current.releaseCompletion().whenComplete((ignored, failure) -> {
        pinScope.close();
        if (failure == null) release.complete(null);
        else release.completeExceptionally(unwrap(failure));
      });
    } catch (RuntimeException | Error failure) {
      pinScope.close();
      release.completeExceptionally(failure);
      if (failure instanceof Error fatal) throw fatal;
    }
    return release.minimalCompletionStage();
  }

  private void settleAfterPinRelease(Throwable terminalFailure) {
    releasePin().whenComplete((ignored, releaseFailure) -> {
      Throwable resultFailure = terminalFailure;
      if (releaseFailure != null) {
        Throwable cleanup = unwrap(releaseFailure);
        if (resultFailure != null && resultFailure != cleanup) {
          resultFailure.addSuppressed(cleanup);
        } else {
          resultFailure = cleanup;
        }
      }
      if (resultFailure == null) settled.complete(null);
      else settled.completeExceptionally(resultFailure);
    });
  }

  private CompletionStage<Void> completeCompactedOpen() {
    synchronized (this) { complete = true; }
    settleAfterPinRelease(null);
    return settled.minimalCompletionStage();
  }

  private static WatchTermination reason(Throwable failure) {
    Throwable cause = unwrap(failure);
    if (cause instanceof CompactedHistory compacted) {
      return WatchTermination.compacted(compacted.compactRevision());
    }
    if (cause instanceof BackendFailure backendFailure) {
      return switch (backendFailure.category()) {
        case RESOURCE_LIMIT -> WatchTermination.resourceLimit(RESOURCE_LIMIT_REASON);
        case CANCELLED -> WatchTermination.cancelled(backendFailure.safeMessage());
        case UNAVAILABLE -> WatchTermination.unavailable(backendFailure.safeMessage());
        case AUTHENTICATION, INVALID_AUTH_TOKEN, PERMISSION, TIMEOUT, UNKNOWN ->
            WatchTermination.failure(backendFailure);
      };
    }
    return WatchTermination.failure(cause);
  }

  private static final class CompactedHistory extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final long compactRevision;

    private CompactedHistory(long compactRevision) {
      super("Watch replay revision is below the compact boundary");
      if (compactRevision <= 0) throw new IllegalArgumentException("compact revision must be positive");
      this.compactRevision = compactRevision;
    }

    private long compactRevision() { return compactRevision; }
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable cause = failure;
    while ((cause instanceof CompletionException
        || cause instanceof java.util.concurrent.ExecutionException) && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }

  private static Throwable collectDeliveryCloseFailures(
      Iterable<WatchStream.Delivery> deliveries, Throwable failure) {
    for (WatchStream.Delivery delivery : deliveries) {
      failure = WatchCleanup.closeAndCollect(delivery::close, failure);
    }
    return failure;
  }

  @Override
  public void close() {
    CompletableFuture<?> active;
    RequestScope readScope;
    Throwable cleanupFailure;
    synchronized (this) {
      if (closed) return;
      closed = true;
      cleanupFailure = collectDeliveryCloseFailures(pending, null);
      pending.clear();
      active = inFlight;
      readScope = activeReadScope;
      if (active == null) {
        settleAfterPinRelease(cleanupFailure);
      }
    }
    pinScope.cancel();
    if (readScope != null) readScope.cancel();
    if (active != null) {
      active.whenComplete((ignored, failure) -> {
        settleAfterPinRelease(cleanupFailure);
      });
    }
  }

  CompletionStage<Void> settled() { return settled.minimalCompletionStage(); }
}
