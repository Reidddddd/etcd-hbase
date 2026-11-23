package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.grpc.protocol.ResponseHeaderFactory;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class WatchStreamTest {
  private static final long CLUSTER_ID = 11;
  private static final long MEMBER_ID = 12;

  @Test
  void emitsCreatedBeforeInclusiveLiveEventsAndAppliesFilterAndPrevKv() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();
    observer.ready = false;
    var timers = timers();
    byte[] start = {0, (byte) 0xff};
    byte[] end = {1, 0};

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers, () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(41, start, end, 5, true,
          Rpc.WatchCreateRequest.FilterType.NOPUT));

      assertTrue(observer.responses.isEmpty(), "Created waits for transport readiness");
      assertEquals(1, observer.requests, "the stream admits one inbound request at a time");

      subscription.add(event(5, PreparedEvent.Kind.PUT, start, new byte[] {7}, Optional.empty()));
      subscription.add(event(5, PreparedEvent.Kind.DELETE, start, null,
          Optional.of(record(start, 4, 3, new byte[] {6}))));
      observer.ready = true;
      observer.readyHandler.run();

      assertEquals(2, observer.responses.size());
      Rpc.WatchResponse created = observer.responses.get(0);
      assertTrue(created.getCreated());
      assertFalse(created.getCanceled());
      assertEquals(41, created.getWatchId());
      assertEquals(4, created.getHeader().getRevision());

      Rpc.WatchResponse events = observer.responses.get(1);
      assertEquals(41, events.getWatchId());
      assertEquals(5, events.getHeader().getRevision());
      assertEquals(1, events.getEventsCount(), "NOPUT filters only the PUT event");
      assertEquals(mvccpb.Kv.Event.EventType.DELETE, events.getEvents(0).getType());
      assertArrayEquals(start, events.getEvents(0).getKv().getKey().toByteArray());
      assertEquals(0, events.getEvents(0).getKv().getCreateRevision());
      assertEquals(5, events.getEvents(0).getKv().getModRevision());
      assertEquals(0, events.getEvents(0).getKv().getVersion());
      assertEquals(0, events.getEvents(0).getKv().getLease());
      assertTrue(events.getEvents(0).hasPrevKv());
      assertArrayEquals(new byte[] {6}, events.getEvents(0).getPrevKv().getValue().toByteArray());
      assertEquals(2, subscription.releases.get(), "all events in the delivered revision are released");
      assertEquals(2, observer.requests, "the next inbound request follows Created");
      verify(timers, never()).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
      assertEquals(KeyRange.Kind.RANGE, session.range.kind());
      assertEquals(Optional.of(ByteKey.copyOf(start)), session.range.lowerInclusive());
      assertEquals(Optional.of(ByteKey.copyOf(end)), session.range.upperExclusive());

      stream.close();
      assertEquals(1, subscription.closeCount.get());
      assertEquals(2, subscription.releases.get());
    }
  }

  @Test
  void skipsConsecutiveFullyFilteredRevisionsWithoutWaitingForPollTick() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();
    observer.ready = false;
    byte[] key = {3};

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(18, key, new byte[0], 0, false,
          Rpc.WatchCreateRequest.FilterType.NOPUT));
      subscription.add(event(5, PreparedEvent.Kind.PUT, key, new byte[] {5}, Optional.empty()));
      subscription.add(event(6, PreparedEvent.Kind.PUT, key, new byte[] {6}, Optional.empty()));
      subscription.add(event(7, PreparedEvent.Kind.DELETE, key, null,
          Optional.of(record(key, 6, 2, new byte[] {6}))));

      observer.ready = true;
      observer.readyHandler.run();

      assertEquals(2, observer.responses.size(),
          "the visible event follows consecutive fully filtered revisions in the same drain");
      Rpc.WatchResponse deleted = observer.responses.get(1);
      assertEquals(7, deleted.getHeader().getRevision());
      assertEquals(1, deleted.getEventsCount());
      assertEquals(mvccpb.Kv.Event.EventType.DELETE, deleted.getEvents(0).getType());
      assertEquals(3, subscription.releases.get(), "filtered and delivered events are all released");

      stream.close();
      assertEquals(1, subscription.closeCount.get());
    }
  }

  @Test
  void yieldsAfterFilteredRevisionBudgetAndReschedulesWhenPollFiresDuringDrain() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();
    observer.ready = false;
    var worker = new CountingDirectExecutor();
    byte[] key = {3};

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = new WatchStream(observer, scope, worker, immediateZeroDelayTimers(),
          () -> CompletableFuture.completedFuture(session), CLUSTER_ID, MEMBER_ID, 1 << 20);
      stream.onNext(createRequest(19, key, new byte[0], 0, false,
          Rpc.WatchCreateRequest.FilterType.NOPUT));
      assertTrue(observer.responses.isEmpty(), "Created is held until transport readiness");

      for (int index = 0; index < 128; index++) {
        long revision = 5L + index;
        subscription.add(event(revision, PreparedEvent.Kind.PUT, key,
            new byte[] {(byte) index}, Optional.empty()));
      }
      subscription.add(event(133, PreparedEvent.Kind.DELETE, key, null,
          Optional.of(record(key, 132, 128, new byte[] {127}))));

      int executionsBeforeDrain = worker.executions.get();
      observer.ready = true;
      observer.readyHandler.run();

      assertEquals(2, worker.executions.get() - executionsBeforeDrain,
          "the zero-delay poll requests a new worker turn instead of continuing this drain task");
      assertEquals(2, observer.responses.size());
      Rpc.WatchResponse deletion = observer.responses.get(1);
      assertEquals(133, deletion.getHeader().getRevision());
      assertEquals(1, deletion.getEventsCount());
      assertEquals(mvccpb.Kv.Event.EventType.DELETE, deletion.getEvents(0).getType());
      assertEquals(129, subscription.releases.get(),
          "all filtered and delivered revision events are released");

      stream.close();
      assertEquals(1, subscription.closeCount.get());
    }
  }

  @Test
  void nodeleteFilterSuppressesDeleteEventsWithoutSuppressingPuts() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(12, new byte[] {1}, new byte[0], 0, false,
          Rpc.WatchCreateRequest.FilterType.NODELETE));
      subscription.add(event(5, PreparedEvent.Kind.DELETE, new byte[] {1}, null,
          Optional.of(record(new byte[] {1}, 4, 3, new byte[] {4}))));
      subscription.add(event(6, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {6}, Optional.empty()));

      observer.readyHandler.run();
      observer.readyHandler.run();

      assertEquals(2, observer.responses.size());
      Rpc.WatchResponse put = observer.responses.get(1);
      assertEquals(12, put.getWatchId());
      assertEquals(6, put.getHeader().getRevision());
      assertEquals(1, put.getEventsCount());
      assertEquals(mvccpb.Kv.Event.EventType.PUT, put.getEvents(0).getType());
      assertEquals(2, subscription.releases.get());
      stream.close();
    }
  }

  @Test
  void duplicateIdDoesNotReplaceExistingWatchAndCancelClosesOnlyItsSubscription() {
    var first = new RecordingSubscription();
    var second = new RecordingSubscription();
    var session = new RecordingSession(8, first, second);
    var observer = new RecordingObserver();
    var timers = timers();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers, () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(7, new byte[] {1}, new byte[0], 0, false));
      stream.onNext(createRequest(8, new byte[] {2}, new byte[0], 0, false));
      stream.onNext(createRequest(7, new byte[] {3}, new byte[0], 0, false));

      assertEquals(3, observer.responses.size());
      assertEquals(List.of(7L, 8L, -1L),
          observer.responses.stream().map(Rpc.WatchResponse::getWatchId).toList());
      Rpc.WatchResponse duplicate = observer.responses.get(2);
      assertTrue(duplicate.getCreated());
      assertTrue(duplicate.getCanceled());
      assertTrue(duplicate.getCancelReason().contains("duplicate watch ID"));
      assertEquals(2, session.subscriptions.size(), "a rejected duplicate allocates no subscription");
      assertEquals(0, first.closeCount.get());
      assertEquals(0, second.closeCount.get());

      stream.onNext(Rpc.WatchRequest.newBuilder()
          .setCancelRequest(Rpc.WatchCancelRequest.newBuilder().setWatchId(7)).build());
      assertEquals(4, observer.responses.size());
      Rpc.WatchResponse cancelled = observer.responses.get(3);
      assertTrue(cancelled.getCanceled());
      assertEquals(7, cancelled.getWatchId());
      assertEquals(1, first.closeCount.get());
      assertEquals(0, second.closeCount.get());

      stream.onNext(Rpc.WatchRequest.newBuilder()
          .setCancelRequest(Rpc.WatchCancelRequest.newBuilder().setWatchId(99)).build());
      assertEquals(4, observer.responses.size(), "cancelling an unknown ID has no response");
      stream.close();
      assertEquals(1, second.closeCount.get());
    }
  }

  @Test
  void resourceLimitedCreateLeavesExistingWatchActive() {
    var existing = new RecordingSubscription();
    var observer = new RecordingObserver();
    var session = new WatchStream.Session() {
      @Override public long completedRevision() { return 8; }

      @Override public CompletableFuture<Long> currentRevision() {
        return CompletableFuture.completedFuture(8L);
      }

      @Override
      public CompletableFuture<WatchStream.OpenedWatch> subscribe(
          KeyRange range, Rpc.WatchCreateRequest request) {
        if (request.getWatchId() == 2) {
          return CompletableFuture.failedFuture(
              new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
        }
        return CompletableFuture.completedFuture(new WatchStream.OpenedWatch(8, -1, existing));
      }
    };

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(1, new byte[] {1}, new byte[0], 0, false));
      stream.onNext(createRequest(2, new byte[] {2}, new byte[0], 0, false));

      assertEquals(2, observer.responses.size());
      assertTrue(observer.responses.get(0).getCreated());
      assertEquals(1, observer.responses.get(0).getWatchId());
      Rpc.WatchResponse rejected = observer.responses.get(1);
      assertTrue(rejected.getCreated());
      assertTrue(rejected.getCanceled());
      assertEquals(-1, rejected.getWatchId());
      assertTrue(rejected.getCancelReason().contains("resource limit"));
      assertEquals(0, existing.closeCount.get(), "rejecting one create does not close an older watch");

      existing.add(event(9, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {9}, Optional.empty()));
      observer.readyHandler.run();

      assertEquals(3, observer.responses.size());
      Rpc.WatchResponse delivered = observer.responses.get(2);
      assertEquals(1, delivered.getWatchId());
      assertEquals(9, delivered.getHeader().getRevision());
      assertEquals(1, delivered.getEventsCount());
      assertEquals(0, existing.closeCount.get());
      stream.close();
      assertEquals(1, existing.closeCount.get());
    }
  }

  @Test
  void emptyRequestAndUnknownCancelHaveNoResponseAndKeepActiveWatch() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(8, subscription);
    var observer = new RecordingObserver();
    byte[] key = {0, (byte) 0xff};

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(0, key, new byte[0], 0, false));

      assertEquals(1, observer.responses.size());
      assertTrue(observer.responses.getFirst().getCreated());
      assertEquals(0, observer.responses.getFirst().getWatchId(),
          "the first automatic Watch ID is zero");

      stream.onNext(Rpc.WatchRequest.getDefaultInstance());
      assertEquals(1, observer.responses.size(), "an empty request has no response");

      stream.onNext(Rpc.WatchRequest.newBuilder().setCancelRequest(
          Rpc.WatchCancelRequest.newBuilder().setWatchId(99)).build());
      assertEquals(1, observer.responses.size(), "cancelling an unknown ID has no response");

      subscription.add(event(9, PreparedEvent.Kind.PUT, key, new byte[] {9}, Optional.empty()));
      observer.readyHandler.run();

      assertEquals(2, observer.responses.size(), "the active Watch continues after ignored controls");
      Rpc.WatchResponse event = observer.responses.get(1);
      assertEquals(0, event.getWatchId());
      assertEquals(9, event.getHeader().getRevision());
      assertEquals(1, event.getEventsCount());
      assertArrayEquals(key, event.getEvents(0).getKv().getKey().toByteArray());

      stream.close();
      assertEquals(1, subscription.closeCount.get());
      assertEquals(1, subscription.releases.get());
    }
  }

  @Test
  void publishedSubscriptionWakeupDrainsWithoutPollingTimerAndCloseClearsListener() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(8, subscription);
    var observer = new RecordingObserver();
    var timers = timers();
    byte[] key = {1};

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers, () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(31, key, new byte[0], 0, false));
      assertEquals(1, observer.responses.size());
      assertTrue(observer.responses.getFirst().getCreated());

      subscription.add(event(9, PreparedEvent.Kind.PUT, key, new byte[] {9}, Optional.empty()));
      subscription.signalWakeup();

      assertEquals(2, observer.responses.size(), "a published event wakes the stream without periodic polling");
      assertEquals(1, observer.responses.get(1).getEventsCount());
      verify(timers, never()).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));

      stream.close();
      assertTrue(subscription.wakeup == null, "closing the watcher removes its callback reference");
    }
  }

  @Test
  void separateStreamsDrainPublishedEventsWithoutSchedulingPolls() {
    var firstSubscription = new RecordingSubscription();
    var secondSubscription = new RecordingSubscription();
    var firstSession = new RecordingSession(8, firstSubscription);
    var secondSession = new RecordingSession(8, secondSubscription);
    var firstObserver = new RecordingObserver();
    var secondObserver = new RecordingObserver();
    var timers = timers();
    byte[] firstKey = {1};
    byte[] secondKey = {2};

    try (var firstScope = RequestScope.withoutDeadline();
         var secondScope = RequestScope.withoutDeadline();
         var firstStream = stream(firstObserver, firstScope, timers,
             () -> CompletableFuture.completedFuture(firstSession));
         var secondStream = stream(secondObserver, secondScope, timers,
             () -> CompletableFuture.completedFuture(secondSession))) {
      firstStream.onNext(createRequest(41, firstKey, new byte[0], 0, false));
      secondStream.onNext(createRequest(42, secondKey, new byte[0], 0, false));
      assertTrue(firstObserver.responses.getFirst().getCreated());
      assertTrue(secondObserver.responses.getFirst().getCreated());

      firstSubscription.add(event(9, PreparedEvent.Kind.PUT, firstKey, new byte[] {9}, Optional.empty()));
      secondSubscription.add(event(10, PreparedEvent.Kind.PUT, secondKey, new byte[] {10}, Optional.empty()));
      firstSubscription.signalWakeup();
      secondSubscription.signalWakeup();

      assertEquals(2, firstObserver.responses.size(), "the first stream drains its published event");
      assertEquals(2, secondObserver.responses.size(), "the second stream drains its published event");
      assertEquals(41, firstObserver.responses.get(1).getWatchId());
      assertEquals(42, secondObserver.responses.get(1).getWatchId());
      verify(timers, never()).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    }
    assertNull(firstSubscription.wakeup, "closing the first stream releases its callback");
    assertNull(secondSubscription.wakeup, "closing the second stream releases its callback");
  }

  @Test
  void automaticIdsStartAtZeroAndSkipAnActiveExplicitId() {
    var session = new RecordingSession(8, new RecordingSubscription(),
        new RecordingSubscription(), new RecordingSubscription());
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(0, new byte[] {1}, new byte[0], 0, false));
      stream.onNext(createRequest(1, new byte[] {2}, new byte[0], 0, false));
      stream.onNext(createRequest(0, new byte[] {3}, new byte[0], 0, false));

      assertEquals(List.of(0L, 1L, 2L),
          observer.responses.stream().map(Rpc.WatchResponse::getWatchId).toList());
      assertTrue(observer.responses.stream().allMatch(Rpc.WatchResponse::getCreated));
      stream.close();
    }
  }

  @Test
  void automaticIdsSkipCompactedExplicitIdsUntilTheyAreCancelled() {
    var active = new RecordingSubscription();
    var compacted = new RecordingSubscription();
    var next = new RecordingSubscription();
    var session = new RecordingSession(3, 2, active, compacted, next);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(0, new byte[] {1}, new byte[0], 0, false));
      stream.onNext(createRequest(1, new byte[] {2}, new byte[0], -1, false));
      stream.onNext(createRequest(0, new byte[] {3}, new byte[0], 0, false));

      assertEquals(List.of(0L, 1L, 1L, 2L),
          observer.responses.stream().map(Rpc.WatchResponse::getWatchId).toList());
      assertTrue(observer.responses.get(1).getCreated());
      assertTrue(observer.responses.get(2).getCanceled());
      assertEquals(1, compacted.closeCount.get(),
          "the compacted subscription is closed while its ID remains reserved");
      stream.close();
      assertEquals(1, active.closeCount.get());
      assertEquals(1, next.closeCount.get());
    }
  }

  @Test
  void rejectsCompactedCreateWhenProcessTombstoneBudgetIsExhausted() {
    var budget = new CompactedWatchIdBudget(1);
    var firstSubscription = new RecordingSubscription();
    var firstSession = new RecordingSession(3, 2, firstSubscription);
    var firstObserver = new RecordingObserver();
    var secondSubscription = new RecordingSubscription();
    var retrySubscription = new RecordingSubscription();
    var secondSession = new RecordingSession(3, 2, secondSubscription, retrySubscription);
    var secondObserver = new RecordingObserver();

    try (var firstScope = RequestScope.withoutDeadline();
         var secondScope = RequestScope.withoutDeadline()) {
      var firstStream = stream(firstObserver, firstScope, timers(),
          () -> CompletableFuture.completedFuture(firstSession), budget);
      var secondStream = stream(secondObserver, secondScope, timers(),
          () -> CompletableFuture.completedFuture(secondSession), budget);

      firstStream.onNext(createRequest(7, new byte[] {1}, new byte[0], -1, false));
      assertEquals(1, budget.retainedIds());

      secondStream.onNext(createRequest(8, new byte[] {2}, new byte[0], -1, false));
      assertEquals(1, secondObserver.responses.size());
      Rpc.WatchResponse rejected = secondObserver.responses.getFirst();
      assertTrue(rejected.getCreated());
      assertTrue(rejected.getCanceled());
      assertEquals(-1, rejected.getWatchId());
      assertTrue(rejected.getCancelReason().contains("retention limit"));
      assertEquals(1, secondSubscription.closeCount.get(),
          "a rejected compacted create releases its opened subscription");

      firstStream.onNext(Rpc.WatchRequest.newBuilder().setCancelRequest(
          Rpc.WatchCancelRequest.newBuilder().setWatchId(7)).build());
      assertEquals(0, budget.retainedIds(), "explicit cancel releases the process-wide ID slot");

      secondStream.onNext(createRequest(8, new byte[] {2}, new byte[0], -1, false));
      assertEquals(3, secondObserver.responses.size());
      assertTrue(secondObserver.responses.get(1).getCreated());
      assertTrue(secondObserver.responses.get(2).getCanceled());
      assertEquals(8, secondObserver.responses.get(1).getWatchId());
      assertEquals(1, budget.retainedIds());

      firstStream.close();
      secondStream.close();
      assertEquals(0, budget.retainedIds(), "closing a stream releases retained IDs");
      assertEquals(1, retrySubscription.closeCount.get());
    }
  }

  @Test
  void processWideTombstonesFromOneStreamDoNotRejectAnotherStreamsActiveCreate() {
    var budget = CompactedWatchIdBudget.processWide();
    int baseline = budget.retainedIds();
    assertTrue(baseline <= CompactedWatchIdBudget.MAX_PROCESS_IDS - WatchRegistry.MAX_WATCHES_PER_STREAM - 1,
        "the process-wide budget must have room for this isolated multi-stream case");

    var tombstones = new ArrayList<RecordingSubscription>(WatchRegistry.MAX_WATCHES_PER_STREAM);
    for (int index = 0; index < WatchRegistry.MAX_WATCHES_PER_STREAM; index++) {
      tombstones.add(new RecordingSubscription());
    }
    var firstSession = new RecordingSession(3, 2, tombstones.toArray(RecordingSubscription[]::new));
    var firstObserver = new RecordingObserver();
    var activeSubscription = new RecordingSubscription();
    var secondCompactedSubscription = new RecordingSubscription();
    var secondSession = new RecordingSession(3, 2, activeSubscription, secondCompactedSubscription);
    var secondObserver = new RecordingObserver();

    try (var firstScope = RequestScope.withoutDeadline();
         var secondScope = RequestScope.withoutDeadline()) {
      var firstStream = stream(firstObserver, firstScope, timers(),
          () -> CompletableFuture.completedFuture(firstSession));
      var secondStream = stream(secondObserver, secondScope, timers(),
          () -> CompletableFuture.completedFuture(secondSession));

      for (int index = 0; index < WatchRegistry.MAX_WATCHES_PER_STREAM; index++) {
        long watchId = index + 1L;
        firstStream.onNext(createRequest(watchId, new byte[] {1}, new byte[0], -1, false));
      }
      assertEquals(baseline + WatchRegistry.MAX_WATCHES_PER_STREAM, budget.retainedIds(),
          "the first stream retains its bounded compacted IDs in the process-wide budget");

      secondStream.onNext(createRequest(2001, new byte[] {2}, new byte[0], 0, false));
      assertEquals(1, secondObserver.responses.size());
      assertTrue(secondObserver.responses.getFirst().getCreated(),
          "another stream can register a normal active watch while tombstones are retained");

      secondStream.onNext(createRequest(2002, new byte[] {3}, new byte[0], -1, false));
      assertEquals(baseline + WatchRegistry.MAX_WATCHES_PER_STREAM + 1, budget.retainedIds(),
          "another stream can also retain a compacted ID below the process-wide cap");
      assertTrue(secondObserver.responses.get(1).getCreated());
      assertTrue(secondObserver.responses.get(2).getCanceled());

      firstStream.onNext(Rpc.WatchRequest.newBuilder().setCancelRequest(
          Rpc.WatchCancelRequest.newBuilder().setWatchId(1)).build());
      assertEquals(baseline + WatchRegistry.MAX_WATCHES_PER_STREAM, budget.retainedIds(),
          "cancelling one stream's tombstone releases exactly one process-wide slot");

      firstStream.close();
      secondStream.close();
      assertEquals(baseline, budget.retainedIds(), "closing streams releases all of their retained IDs");
      assertEquals(1, activeSubscription.closeCount.get());
    }
  }

  @Test
  void futureStartRevisionDropsOlderLiveEventsAndDeliversTheTargetInclusively() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(9, new byte[] {1}, new byte[0], 7, false));
      subscription.add(event(5, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {5}, Optional.empty()));
      subscription.add(event(6, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {6}, Optional.empty()));
      subscription.add(event(7, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {7}, Optional.empty()));

      observer.readyHandler.run();
      observer.readyHandler.run();
      observer.readyHandler.run();

      assertEquals(2, observer.responses.size());
      assertTrue(observer.responses.getFirst().getCreated());
      Rpc.WatchResponse events = observer.responses.get(1);
      assertEquals(9, events.getWatchId());
      assertEquals(7, events.getHeader().getRevision());
      assertEquals(1, events.getEventsCount());
      assertArrayEquals(new byte[] {7}, events.getEvents(0).getKv().getValue().toByteArray());
      assertEquals(3, subscription.releases.get(), "older and delivered event references are all released");
      stream.close();
    }
  }

  @Test
  void explicitProgressFollowsAllEligibleEventsAndUsesTheAuthoritativeRevision() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();
    byte[] key = {4};

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(31, key, new byte[0], 0, false));
      session.revision = 5;
      subscription.add(event(5, PreparedEvent.Kind.PUT, key, new byte[] {5}, Optional.empty()));

      stream.onNext(progressRequest());

      assertEquals(3, observer.responses.size());
      assertTrue(observer.responses.get(0).getCreated());
      Rpc.WatchResponse event = observer.responses.get(1);
      assertEquals(31, event.getWatchId());
      assertEquals(5, event.getHeader().getRevision());
      assertEquals(1, event.getEventsCount());
      Rpc.WatchResponse progress = observer.responses.get(2);
      assertEquals(-1, progress.getWatchId());
      assertEquals(5, progress.getHeader().getRevision());
      assertEquals(0, progress.getEventsCount());
      assertEquals(1, subscription.releases.get());
      stream.close();
    }
  }

  @Test
  void explicitProgressWaitsForEveryWatchThroughItsBarrier() {
    var future = new RecordingSubscription();
    var pending = new RecordingSubscription();
    var session = new RecordingSession(4, future, pending);
    var observer = new RecordingObserver();
    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(31, new byte[] {1}, new byte[0], 0, false));
      stream.onNext(createRequest(32, new byte[] {2}, new byte[0], 0, false));
      observer.ready = false;
      session.revision = 5;
      future.add(event(6, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {6}, Optional.empty()));
      pending.add(event(5, PreparedEvent.Kind.PUT, new byte[] {2}, new byte[] {5}, Optional.empty()));
      stream.onNext(progressRequest());
      observer.ready = true;
      observer.readyHandler.run();

      int eventIndex = indexOfResponse(observer.responses,
          response -> response.getWatchId() == 32 && response.getEventsCount() > 0);
      int progressIndex = indexOfResponse(observer.responses, response -> response.getWatchId() == -1);
      int futureIndex = indexOfResponse(observer.responses,
          response -> response.getWatchId() == 31 && response.getEventsCount() > 0);
      assertTrue(eventIndex >= 0 && eventIndex < progressIndex);
      assertTrue(progressIndex < futureIndex);
      assertEquals(5, observer.responses.get(progressIndex).getHeader().getRevision());
      stream.close();
    }
  }

  @Test
  void explicitProgressCanAdvanceAcrossARevisionFilteredByTheWatch() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();
    byte[] key = {5};

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(32, key, new byte[0], 0, false,
          Rpc.WatchCreateRequest.FilterType.NOPUT));
      session.revision = 5;
      subscription.add(event(5, PreparedEvent.Kind.PUT, key, new byte[] {6}, Optional.empty()));

      stream.onNext(progressRequest());

      assertEquals(2, observer.responses.size());
      assertTrue(observer.responses.getFirst().getCreated());
      Rpc.WatchResponse progress = observer.responses.get(1);
      assertEquals(-1, progress.getWatchId());
      assertEquals(5, progress.getHeader().getRevision());
      assertEquals(0, progress.getEventsCount());
      assertEquals(1, subscription.releases.get(), "filtered events are consumed before progress");
      stream.close();
    }
  }

  @Test
  void progressRequestIsDroppedWhenAnyWatchHasNotFinishedHistoricalReplay() {
    var ready = new RecordingSubscription();
    var replaying = new RecordingSubscription();
    replaying.progressReady = false;
    var session = new RecordingSession(8, true, ready, replaying);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(33, new byte[] {1}, new byte[0], 0, false));
      stream.onNext(createRequest(34, new byte[] {2}, new byte[0], 3, false));

      stream.onNext(progressRequest());
      assertEquals(2, observer.responses.size(),
          "the all-watch progress request has no response while one replay is incomplete");

      replaying.progressReady = true;
      stream.onNext(progressRequest());
      assertEquals(3, observer.responses.size());
      assertEquals(-1, observer.responses.get(2).getWatchId());
      assertEquals(8, observer.responses.get(2).getHeader().getRevision());
      stream.close();
    }
  }

  @Test
  void progressRequestIsDroppedUntilFutureStartRevisionIsReached() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();
    byte[] key = {6};

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(35, key, new byte[0], 7, false));

      stream.onNext(progressRequest());
      assertEquals(1, observer.responses.size(), "future-start Watch blocks stream progress");

      session.revision = 7;
      subscription.add(event(7, PreparedEvent.Kind.PUT, key, new byte[] {7}, Optional.empty()));
      stream.onNext(progressRequest());

      assertEquals(3, observer.responses.size());
      assertEquals(35, observer.responses.get(1).getWatchId());
      assertEquals(7, observer.responses.get(1).getHeader().getRevision());
      assertEquals(-1, observer.responses.get(2).getWatchId());
      assertEquals(7, observer.responses.get(2).getHeader().getRevision());
      stream.close();
    }
  }

  @Test
  void periodicProgressNotifySupportsRawNegativeIdAndDrainsEventsBeforeItsWatermark() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();
    var periodicTick = new AtomicReference<Runnable>();
    byte[] key = {9};

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = new WatchStream(observer, scope, Runnable::run,
          timers(periodicTick, 1_000), () -> CompletableFuture.completedFuture(session),
          CLUSTER_ID, MEMBER_ID, 1 << 20, 1_000);
      stream.onNext(createRequest(-1, key, new byte[0], 0, false, true, false));
      session.revision = 5;
      subscription.add(event(5, PreparedEvent.Kind.PUT, key, new byte[] {5}, Optional.empty()));

      Runnable tick = periodicTick.getAndSet(null);
      assertTrue(tick != null, "progress_notify schedules a periodic trigger after Created");
      tick.run();

      assertEquals(3, observer.responses.size());
      Rpc.WatchResponse deliveredEvent = observer.responses.get(1);
      assertEquals(-1, deliveredEvent.getWatchId());
      assertEquals(5, deliveredEvent.getHeader().getRevision());
      Rpc.WatchResponse progress = observer.responses.get(2);
      assertEquals(-1, progress.getWatchId(), "periodic progress is scoped to its raw-proto watcher");
      assertEquals(5, progress.getHeader().getRevision());
      assertEquals(0, progress.getEventsCount());
      assertEquals(1, subscription.releases.get());

      stream.close();
      assertEquals(1, subscription.closeCount.get());
    }
  }

  @Test
  void periodicProgressNotifySkipsFutureAndUnsyncedWatchersIndependently() {
    var ready = new RecordingSubscription();
    var future = new RecordingSubscription();
    future.progressReady = false;
    var session = new RecordingSession(5, ready, future);
    var observer = new RecordingObserver();
    var periodicTick = new AtomicReference<Runnable>();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = new WatchStream(observer, scope, Runnable::run,
          timers(periodicTick, 1_000), () -> CompletableFuture.completedFuture(session),
          CLUSTER_ID, MEMBER_ID, 1 << 20, 1_000);
      stream.onNext(createRequest(40, new byte[] {1}, new byte[0], 0, false, true, false));
      stream.onNext(createRequest(41, new byte[] {2}, new byte[0], 9, false, true, false));

      Runnable tick = periodicTick.getAndSet(null);
      assertTrue(tick != null, "the stream has one timer for all opted-in watchers");
      tick.run();

      assertEquals(3, observer.responses.size());
      Rpc.WatchResponse progress = observer.responses.get(2);
      assertEquals(40, progress.getWatchId(), "one unsynced watcher does not block another");
      assertEquals(5, progress.getHeader().getRevision());

      stream.close();
      assertEquals(1, ready.closeCount.get());
      assertEquals(1, future.closeCount.get());
    }
  }

  @Test
  void fragmentsKeepTheRevisionContiguousAheadOfProgressAndOtherWatches() {
    var fragmented = new RecordingSubscription();
    var other = new RecordingSubscription();
    var session = new RecordingSession(4, fragmented, other);
    var observer = new RecordingObserver();
    var values = List.of(new byte[80], new byte[80], new byte[80]);
    byte[] fragmentedKey = {1};
    byte[] otherKey = {2};
    var fragmentedEvents = values.stream()
        .map(value -> event(5, PreparedEvent.Kind.PUT, fragmentedKey, value, Optional.empty()))
        .toList();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = new WatchStream(observer, scope, Runnable::run, timers(),
          () -> CompletableFuture.completedFuture(session), CLUSTER_ID, MEMBER_ID, 220);
      stream.onNext(createRequest(31, fragmentedKey, new byte[0], 0, false,
          false, true));
      stream.onNext(createRequest(32, otherKey, new byte[0], 0, false));

      session.revision = 5;
      fragmentedEvents.forEach(fragmented::add);
      other.add(event(6, PreparedEvent.Kind.PUT, otherKey, new byte[] {6}, Optional.empty()));
      AtomicInteger blockedAfterFirstFragment = new AtomicInteger();
      observer.onNextHook = () -> {
        Rpc.WatchResponse sent = observer.responses.getLast();
        if (sent.getWatchId() == 31 && sent.getFragment()
            && blockedAfterFirstFragment.compareAndSet(0, 1)) {
          observer.ready = false;
          stream.onNext(progressRequest());
        }
      };

      stream.onNext(Rpc.WatchRequest.getDefaultInstance());
      assertEquals(1, blockedAfterFirstFragment.get());
      assertEquals(3, observer.responses.size(), "only Created responses and the first fragment were sent");
      assertEquals(0, fragmented.releases.get(), "event leases remain held during an incomplete revision");

      observer.ready = true;
      observer.readyHandler.run();

      List<Rpc.WatchResponse> fragments = observer.responses.stream()
          .filter(response -> response.getWatchId() == 31 && response.getEventsCount() > 0)
          .toList();
      assertTrue(fragments.size() > 1);
      assertTrue(fragments.getFirst().getFragment());
      assertFalse(fragments.getLast().getFragment());
      assertTrue(fragments.stream().allMatch(response -> response.getSerializedSize() <= 220));
      List<ByteString> deliveredValues = fragments.stream()
          .flatMap(response -> response.getEventsList().stream())
          .map(event -> event.getKv().getValue()).toList();
      assertEquals(values.stream().map(ByteString::copyFrom).toList(), deliveredValues);

      int firstFragmentIndex = observer.responses.indexOf(fragments.getFirst());
      int finalFragmentIndex = observer.responses.indexOf(fragments.getLast());
      assertEquals(fragments.size(), finalFragmentIndex - firstFragmentIndex + 1,
          "no other watch or control response may split a fragment sequence");
      int progressIndex = indexOfResponse(observer.responses, response -> response.getWatchId() == -1);
      int otherWatchEventIndex = indexOfResponse(observer.responses,
          response -> response.getWatchId() == 32 && response.getEventsCount() > 0);
      assertTrue(progressIndex > finalFragmentIndex);
      assertTrue(otherWatchEventIndex > progressIndex);
      assertEquals(3, fragmented.releases.get(), "the final fragment releases the complete revision");

      stream.close();
      assertEquals(1, fragmented.closeCount.get());
      assertEquals(1, other.closeCount.get());
    }
  }

  @Test
  void rejectsARevisionBeyondTheAggregationBoundWithoutSendingAPartialRevision() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();
    byte[] key = {4};
    for (int index = 0; index <= WatchEventBatchEncoder.MAX_PENDING_EVENTS_PER_WATCH; index++) {
      subscription.add(event(5, PreparedEvent.Kind.PUT, key, new byte[] {(byte) index}, Optional.empty()));
    }

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(33, key, new byte[0], 0, false, false, true));

      assertEquals(2, observer.responses.size());
      assertTrue(observer.responses.getFirst().getCreated());
      Rpc.WatchResponse cancelled = observer.responses.getLast();
      assertTrue(cancelled.getCanceled());
      assertEquals(33, cancelled.getWatchId());
      assertTrue(cancelled.getCancelReason().contains("aggregation budget"));
      assertEquals(WatchEventBatchEncoder.MAX_PENDING_EVENTS_PER_WATCH + 1, subscription.releases.get());

      stream.close();
      assertEquals(1, subscription.closeCount.get());
    }
  }

  @Test
  void closingDuringAFragmentSequenceReleasesTheUnfinishedRevision() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();
    byte[] key = {5};
    for (int value = 0; value < 3; value++) {
      subscription.add(event(5, PreparedEvent.Kind.PUT, key, new byte[80], Optional.empty()));
    }

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = new WatchStream(observer, scope, Runnable::run, timers(),
          () -> CompletableFuture.completedFuture(session), CLUSTER_ID, MEMBER_ID, 220);
      observer.onNextHook = () -> {
        Rpc.WatchResponse sent = observer.responses.getLast();
        if (sent.getWatchId() == 34 && sent.getFragment()) observer.ready = false;
      };
      stream.onNext(createRequest(34, key, new byte[0], 0, false, false, true));

      assertEquals(2, observer.responses.size());
      assertTrue(observer.responses.getLast().getFragment());
      assertEquals(0, subscription.releases.get());

      stream.close();
      assertEquals(3, subscription.releases.get());
      assertEquals(1, subscription.closeCount.get());
      observer.ready = true;
      observer.readyHandler.run();
      assertEquals(2, observer.responses.size(), "closing must discard the unfinished fragment tail");
    }
  }

  @Test
  void zeroStartWaitsForCreatedBarrierAndExcludesEarlierEvents() {
    long startRevision = 0;
    var subscription = new RecordingSubscription();
    var barrier = new CompletableFuture<WatchStream.OpenedWatch>();
    var session = new WatchStream.Session() {
      @Override public long completedRevision() { return 3; }
      @Override public CompletableFuture<Long> currentRevision() {
        return CompletableFuture.completedFuture(3L);
      }

      @Override
      public CompletableFuture<WatchStream.OpenedWatch> subscribe(KeyRange range,
          Rpc.WatchCreateRequest request) {
        return barrier;
      }
    };
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(15, new byte[] {1}, new byte[0], startRevision, false));
      subscription.add(event(4, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {4}, Optional.empty()));
      subscription.add(event(5, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {5}, Optional.empty()));
      assertTrue(observer.responses.isEmpty(), "Created waits for the authoritative barrier");

      barrier.complete(new WatchStream.OpenedWatch(4, -1, subscription));

      observer.readyHandler.run();
      observer.readyHandler.run();

      assertEquals(2, observer.responses.size());
      assertTrue(observer.responses.getFirst().getCreated());
      assertEquals(4, observer.responses.getFirst().getHeader().getRevision());
      Rpc.WatchResponse events = observer.responses.get(1);
      assertEquals(5, events.getHeader().getRevision());
      assertEquals(1, events.getEventsCount());
      assertEquals(2, subscription.releases.get(),
          "the event at the barrier is filtered while the next revision is delivered");
      stream.close();
      assertEquals(2, subscription.releases.get());
    }
  }

  @Test
  void rejectsHistoricalStartWithoutPublishingAWatchThatWouldMissEvents() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(8, subscription);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(7, new byte[] {1}, new byte[0], 8, false));

      assertEquals(1, observer.responses.size());
      Rpc.WatchResponse rejected = observer.responses.getFirst();
      assertEquals(-1, rejected.getWatchId());
      assertTrue(rejected.getCreated());
      assertTrue(rejected.getCanceled());
      assertTrue(rejected.getCancelReason().contains("historical Watch replay is not implemented"));
      assertEquals(1, subscription.closeCount.get());
      assertEquals(1, session.subscriptions.size());
      stream.close();
    }
  }

  @Test
  void initialNegativeStartWithNoHistoryUsesTheLiveBarrier() {
    var subscription = new RecordingSubscription();
    byte[] key = {1};
    subscription.add(event(2, PreparedEvent.Kind.PUT, key, new byte[] {9}, Optional.empty()));
    var session = new RecordingSession(1, -1, subscription);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(7, key, new byte[0], -1, false));

      assertEquals(List.of(-1L), session.startRevisions,
          "the negative start revision is forwarded unchanged to the tailer");
      assertEquals(2, observer.responses.size());
      assertTrue(observer.responses.getFirst().getCreated());
      assertFalse(observer.responses.getFirst().getCanceled());
      assertEquals(7, observer.responses.getFirst().getWatchId());
      assertEquals(1, observer.responses.getFirst().getHeader().getRevision());
      Rpc.WatchResponse live = observer.responses.get(1);
      assertEquals(7, live.getWatchId());
      assertEquals(2, live.getHeader().getRevision());
      assertEquals(2, live.getEvents(0).getKv().getModRevision());
      assertEquals(1, session.subscriptions.size());
      assertEquals(0, subscription.closeCount.get());
      stream.close();
      assertEquals(1, subscription.closeCount.get());
    }
  }

  @Test
  void negativeStartWithUncompactedHistorySkipsOldEventsAndDeliversNewEvents() {
    var subscription = new RecordingSubscription();
    byte[] key = {1};
    subscription.add(event(2, PreparedEvent.Kind.PUT, key, new byte[] {9}, Optional.empty()));
    subscription.add(event(4, PreparedEvent.Kind.PUT, key, new byte[] {10}, Optional.empty()));
    var session = new RecordingSession(3, -1, subscription);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(7, key, new byte[0], -1, false));

      assertEquals(2, observer.responses.size());
      Rpc.WatchResponse created = observer.responses.getFirst();
      assertTrue(created.getCreated());
      assertFalse(created.getCanceled());
      assertEquals(7, created.getWatchId());
      assertEquals(3, created.getHeader().getRevision());
      Rpc.WatchResponse live = observer.responses.get(1);
      assertEquals(7, live.getWatchId());
      assertEquals(4, live.getHeader().getRevision());
      assertEquals(1, live.getEventsCount());
      assertArrayEquals(new byte[] {10}, live.getEvents(0).getKv().getValue().toByteArray());
      assertEquals(2, subscription.releases.get(),
          "the pre-watch historical event is suppressed and every retained event is released");
      stream.close();
      assertEquals(1, subscription.closeCount.get());
    }
  }

  @Test
  void startBelowInitialCompactBoundaryCreatesThenCancelsAndKeepsIdUntilCancel() {
    var compacted = new RecordingSubscription();
    var replacement = new RecordingSubscription();
    var session = new RecordingSession(1, -1, compacted, replacement);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(7, new byte[] {1}, new byte[0], -2, false));

      assertEquals(2, observer.responses.size());
      Rpc.WatchResponse created = observer.responses.get(0);
      Rpc.WatchResponse cancelled = observer.responses.get(1);
      assertTrue(created.getCreated());
      assertFalse(created.getCanceled());
      assertEquals(7, created.getWatchId());
      assertTrue(cancelled.getCanceled());
      assertFalse(cancelled.getCreated());
      assertEquals(7, cancelled.getWatchId());
      assertEquals(0, cancelled.getHeader().getRevision());
      assertEquals(-1, cancelled.getCompactRevision());
      assertEquals("", cancelled.getCancelReason());
      assertEquals(1, compacted.closeCount.get(),
          "compaction releases subscription resources while retaining the RPC watch ID");

      stream.onNext(createRequest(7, new byte[] {2}, new byte[0], 0, false));
      assertEquals(3, observer.responses.size());
      Rpc.WatchResponse duplicate = observer.responses.get(2);
      assertTrue(duplicate.getCreated());
      assertTrue(duplicate.getCanceled());
      assertEquals(-1, duplicate.getWatchId());
      assertTrue(duplicate.getCancelReason().contains("duplicate watch ID"));
      assertEquals(1, session.subscriptions.size(), "duplicate creation cannot install another range");

      stream.onNext(Rpc.WatchRequest.newBuilder().setCancelRequest(
          Rpc.WatchCancelRequest.newBuilder().setWatchId(7)).build());
      Rpc.WatchResponse explicitCancel = observer.responses.get(3);
      assertTrue(explicitCancel.getCanceled());
      assertEquals(7, explicitCancel.getWatchId());
      assertEquals(0, explicitCancel.getCompactRevision());
      assertEquals(1, compacted.closeCount.get());

      stream.onNext(createRequest(7, new byte[] {2}, new byte[0], 0, false));
      assertEquals(5, observer.responses.size());
      assertTrue(observer.responses.get(4).getCreated());
      assertEquals(7, observer.responses.get(4).getWatchId());
      assertEquals(2, session.subscriptions.size(), "explicit cancel frees the watch ID");
      stream.close();
      assertEquals(1, replacement.closeCount.get());
    }
  }

  @Test
  void compactionDuringHistoricalReplaySendsCompactRevisionAfterCreated() {
    var subscription = new RecordingSubscription();
    subscription.termination = Optional.of(WatchTermination.compacted(4));
    var session = new RecordingSession(6, true, subscription);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(17, new byte[] {1}, new byte[0], 3, false));

      assertEquals(2, observer.responses.size());
      assertTrue(observer.responses.get(0).getCreated());
      Rpc.WatchResponse cancelled = observer.responses.get(1);
      assertTrue(cancelled.getCanceled());
      assertEquals(17, cancelled.getWatchId());
      assertEquals(6, cancelled.getHeader().getRevision());
      assertEquals(4, cancelled.getCompactRevision());
      assertEquals("", cancelled.getCancelReason());
      assertEquals(1, subscription.closeCount.get());
      stream.close();
    }
  }

  @Test
  void negativeStartsBelowAnAdvancedCompactBoundaryReportTheNativeBoundary() {
    for (long startRevision : new long[] {-2, -1}) {
      var subscription = new RecordingSubscription();
      var session = new RecordingSession(3, 2, subscription);
      var observer = new RecordingObserver();

      try (var scope = RequestScope.withoutDeadline()) {
        var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
        stream.onNext(createRequest(9, new byte[] {1}, new byte[0], startRevision, false));

        assertEquals(2, observer.responses.size(), "start revision " + startRevision);
        assertTrue(observer.responses.get(0).getCreated());
        assertTrue(observer.responses.get(1).getCanceled());
        assertEquals(9, observer.responses.get(0).getWatchId());
        assertEquals(9, observer.responses.get(1).getWatchId());
        assertEquals(2, observer.responses.get(1).getCompactRevision());
        assertEquals(0, observer.responses.get(1).getHeader().getRevision());
        assertEquals(1, subscription.closeCount.get(),
            "compaction releases per-watch resources before waiting for client cancellation");
        stream.close();
        assertEquals(1, subscription.closeCount.get());
      }
    }
  }

  @Test
  void zeroStartUsesTheLiveBarrierEvenWhenTheCompactBoundaryIsPositive() {
    var subscription = new RecordingSubscription();
    byte[] key = {1};
    subscription.add(event(4, PreparedEvent.Kind.PUT, key, new byte[] {1}, Optional.empty()));
    var session = new RecordingSession(3, 2, subscription);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(10, key, new byte[0], 0, false));

      assertEquals(2, observer.responses.size());
      assertTrue(observer.responses.get(0).getCreated());
      assertFalse(observer.responses.get(1).getCanceled());
      assertEquals(10, observer.responses.get(1).getWatchId());
      assertEquals(4, observer.responses.get(1).getHeader().getRevision());
      assertEquals(List.of(0L), session.startRevisions);
      stream.close();
    }
  }

  @Test
  void cancellingOneWatchKeepsOtherByteRangeSubscriptionActive() {
    var cancelled = new RecordingSubscription();
    var active = new RecordingSubscription();
    var session = new RecordingSession(8, cancelled, active);
    var observer = new RecordingObserver();
    byte[] pointKey = {0, (byte) 0xff};
    byte[] rangeStart = {1, 0};
    byte[] rangeEnd = {2, 0};

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(21, pointKey, new byte[0], 0, false));
      stream.onNext(createRequest(22, rangeStart, rangeEnd, 0, false));
      cancelled.add(event(9, PreparedEvent.Kind.PUT, pointKey, new byte[] {9}, Optional.empty()));
      active.add(event(10, PreparedEvent.Kind.PUT, rangeStart, new byte[] {10}, Optional.empty()));
      stream.onNext(Rpc.WatchRequest.newBuilder()
          .setCancelRequest(Rpc.WatchCancelRequest.newBuilder().setWatchId(21)).build());

      List<Rpc.WatchResponse> eventResponses = observer.responses.stream()
          .filter(response -> response.getEventsCount() > 0).toList();
      assertEquals(1, eventResponses.size());
      assertEquals(22, eventResponses.getFirst().getWatchId());
      assertArrayEquals(rangeStart, eventResponses.getFirst().getEvents(0).getKv().getKey().toByteArray());
      assertTrue(observer.responses.stream().anyMatch(response -> response.getCanceled()
          && response.getWatchId() == 21));
      assertEquals(1, cancelled.closeCount.get());
      assertEquals(0, active.closeCount.get());
      assertEquals(1, cancelled.releases.get(), "cancel releases only the cancelled watch's queued event");
      assertEquals(1, active.releases.get(), "the other watch transfers and releases its own event");
      assertEquals(KeyRange.Kind.POINT, session.ranges.get(0).kind());
      assertEquals(Optional.of(ByteKey.copyOf(rangeStart)), session.ranges.get(1).lowerInclusive());
      assertEquals(Optional.of(ByteKey.copyOf(rangeEnd)), session.ranges.get(1).upperExclusive());
      stream.close();
      assertEquals(1, active.closeCount.get());
    }
  }

  @Test
  void resourceLimitTerminationCancelsOnlyItsWatchAndReleasesQueuedDeliveries() {
    assertTerminationCancelsOnlyItsWatchAndReleasesQueuedDeliveries(
        "etcd-hbase: Watch subscription resource limit exceeded");
  }

  @Test
  void closedTailerTerminationCancelsOnlyItsWatchAndReleasesQueuedDeliveries() {
    assertTerminationCancelsOnlyItsWatchAndReleasesQueuedDeliveries(
        "etcd-hbase: Watch event source is unavailable");
  }

  @Test
  void oversizedCompleteRevisionCancelsWatchWithoutSendingPartialEvents() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();
    byte[] firstKey = {1};
    byte[] firstValue = {5, 6};
    byte[] secondKey = {2};
    byte[] secondValue = {7, 8};
    subscription.add(event(5, PreparedEvent.Kind.PUT, firstKey, firstValue, Optional.empty()));
    subscription.add(event(5, PreparedEvent.Kind.PUT, secondKey, secondValue, Optional.empty()));
    int maximumResponseBytes = 1;

    Rpc.WatchResponse completeRevision = Rpc.WatchResponse.newBuilder()
        .setHeader(ResponseHeaderFactory.create(CLUSTER_ID, MEMBER_ID, 5, 0))
        .setWatchId(63)
        .addEvents(mvccpb.Kv.Event.newBuilder().setType(mvccpb.Kv.Event.EventType.PUT)
            .setKv(mvccpb.Kv.KeyValue.newBuilder().setKey(ByteString.copyFrom(firstKey))
                .setValue(ByteString.copyFrom(firstValue)).setCreateRevision(1)
                .setModRevision(5).setVersion(5)))
        .addEvents(mvccpb.Kv.Event.newBuilder().setType(mvccpb.Kv.Event.EventType.PUT)
            .setKv(mvccpb.Kv.KeyValue.newBuilder().setKey(ByteString.copyFrom(secondKey))
                .setValue(ByteString.copyFrom(secondValue)).setCreateRevision(1)
                .setModRevision(5).setVersion(5)))
        .build();
    assertTrue(completeRevision.getSerializedSize() > maximumResponseBytes);

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = new WatchStream(observer, scope, Runnable::run, timers(),
          () -> CompletableFuture.completedFuture(session), CLUSTER_ID, MEMBER_ID,
          maximumResponseBytes);
      stream.onNext(createRequest(63, firstKey, new byte[0], 0, false));

      assertEquals(2, observer.responses.size(),
          "an oversized revision is rejected as a whole after Created");
      assertTrue(observer.responses.get(0).getCreated());
      Rpc.WatchResponse cancelled = observer.responses.get(1);
      assertFalse(cancelled.getCreated());
      assertTrue(cancelled.getCanceled());
      assertEquals(63, cancelled.getWatchId());
      assertEquals("etcd-hbase: Watch response exceeds the configured message budget",
          cancelled.getCancelReason());
      assertEquals(0, observer.responses.stream()
          .mapToInt(Rpc.WatchResponse::getEventsCount).sum(),
          "no subset of the revision may be sent");
      assertEquals(2, subscription.releases.get(),
          "every delivery in the rejected revision releases its retained budget");
      assertEquals(1, subscription.closeCount.get());

      stream.close();
      assertEquals(1, subscription.closeCount.get());
    }
  }

  @Test
  void slowWatchResourceLimitDoesNotBlockReadyWatcherAfterBackpressure() {
    var slow = new RecordingSubscription();
    var fast = new RecordingSubscription();
    var session = new RecordingSession(8, slow, fast);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(31, new byte[] {1}, new byte[0], 0, false));
      stream.onNext(createRequest(32, new byte[] {2}, new byte[0], 0, false));
      int slowPollsBeforeBackpressure = slow.pollCalls;
      int fastPollsBeforeBackpressure = fast.pollCalls;

      observer.ready = false;
      observer.readyHandler.run();
      assertEquals(slowPollsBeforeBackpressure, slow.pollCalls,
          "an unready transport stops requesting pages from the slow watch");
      assertEquals(fastPollsBeforeBackpressure, fast.pollCalls,
          "an unready transport stops requesting pages from the fast watch");

      slow.termination = Optional.of(WatchTermination.resourceLimit(
          "etcd-hbase: Watch subscription resource limit exceeded"));
      fast.add(event(9, PreparedEvent.Kind.PUT, new byte[] {2}, new byte[] {9}, Optional.empty()));
      observer.ready = true;
      observer.readyHandler.run();

      assertTrue(observer.responses.stream().anyMatch(response ->
          response.getWatchId() == 31 && response.getCanceled()
              && response.getCancelReason().contains("resource limit")));
      assertTrue(observer.responses.stream().anyMatch(response ->
          response.getWatchId() == 32 && response.getEventsCount() == 1
              && response.getHeader().getRevision() == 9));
      assertEquals(1, slow.closeCount.get());
      assertEquals(0, fast.closeCount.get(), "one exhausted subscription leaves the fast watch active");
      stream.close();
      assertEquals(1, fast.closeCount.get());
    }
  }

  @Test
  void cancelAcknowledgementWaitsForAndUsesTheCurrentSessionRevision() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var currentRevision = new CompletableFuture<Long>();
    session.currentRevision = currentRevision;
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(21, new byte[] {1}, new byte[0], 0, false));
      stream.onNext(Rpc.WatchRequest.newBuilder().setCancelRequest(
          Rpc.WatchCancelRequest.newBuilder().setWatchId(21)).build());

      assertEquals(1, observer.responses.size(), "cancel waits for the current revision read");
      assertEquals(1, subscription.closeCount.get(), "the subscription closes before the read completes");
      assertEquals(2, observer.requests, "the next inbound request waits behind the cancel ack");

      currentRevision.complete(9L);

      assertEquals(2, observer.responses.size());
      Rpc.WatchResponse cancelled = observer.responses.get(1);
      assertTrue(cancelled.getCanceled());
      assertEquals(21, cancelled.getWatchId());
      assertEquals(9, cancelled.getHeader().getRevision());
      stream.close();
    }
  }

  @Test
  void createsAndCancelsRawWatchIdMinusOne() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      // A raw proto watch ID is not constrained by the Go client's InvalidWatchID sentinel.
      stream.onNext(createRequest(-1, new byte[] {1}, new byte[0], 0, false));

      assertEquals(1, observer.responses.size());
      Rpc.WatchResponse created = observer.responses.getFirst();
      assertEquals(-1, created.getWatchId());
      assertTrue(created.getCreated());
      assertFalse(created.getCanceled());

      stream.onNext(Rpc.WatchRequest.newBuilder().setCancelRequest(
          Rpc.WatchCancelRequest.newBuilder().setWatchId(-1)).build());

      assertEquals(2, observer.responses.size());
      Rpc.WatchResponse cancelled = observer.responses.get(1);
      assertEquals(-1, cancelled.getWatchId());
      assertFalse(cancelled.getCreated());
      assertTrue(cancelled.getCanceled());
      assertEquals(1, subscription.closeCount.get(), "Cancel(-1) closes the raw-proto subscription");
      stream.close();
      assertEquals(1, subscription.closeCount.get());
    }
  }

  @Test
  void createRejectionUsesTheCurrentSessionRevision() {
    var session = new RecordingSession(4, new RecordingSubscription());
    var currentRevision = new CompletableFuture<Long>();
    session.currentRevision = currentRevision;
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(22, new byte[] {2}, new byte[] {1}, 0, false));

      assertTrue(observer.responses.isEmpty(), "the rejected create waits for the current revision read");

      currentRevision.complete(9L);

      assertEquals(1, observer.responses.size());
      Rpc.WatchResponse rejected = observer.responses.getFirst();
      assertTrue(rejected.getCreated());
      assertTrue(rejected.getCanceled());
      assertEquals(-1, rejected.getWatchId());
      assertEquals(9, rejected.getHeader().getRevision());
      stream.close();
    }
  }

  @Test
  void cancellationDuringPollClosesTheEventBeforeItCanEscapeTheWatcher() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();
    var streamHolder = new AtomicReference<WatchStream>();
    subscription.afterPoll = () -> streamHolder.get().close();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      streamHolder.set(stream);
      subscription.add(event(5, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {5}, Optional.empty()));
      stream.onNext(createRequest(16, new byte[] {1}, new byte[0], 0, false));

      assertEquals(1, observer.responses.size(), "only the Created response precedes cancellation");
      assertTrue(observer.responses.getFirst().getCreated());
      assertEquals(1, subscription.closeCount.get());
      assertEquals(1, subscription.releases.get(), "the event removed by poll still releases its budget");
    }
  }

  @Test
  void terminalFailureWaitsForTheCurrentResponseWriter() throws Exception {
    var observer = new RecordingObserver();
    var session = new RecordingSession(4, new RecordingSubscription());
    CountDownLatch responseStarted = new CountDownLatch(1);
    CountDownLatch releaseResponse = new CountDownLatch(1);
    CountDownLatch failureQueued = new CountDownLatch(1);
    AtomicReference<Throwable> invocationFailure = new AtomicReference<>();
    observer.onNextHook = () -> {
      responseStarted.countDown();
      try {
        assertTrue(releaseResponse.await(5, TimeUnit.SECONDS));
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new AssertionError(interrupted);
      }
    };

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      Thread failureThread = new Thread(() -> {
        try {
          assertTrue(responseStarted.await(5, TimeUnit.SECONDS));
          var fail = WatchStream.class.getDeclaredMethod("fail", Throwable.class);
          fail.setAccessible(true);
          fail.invoke(stream, new IllegalStateException("asynchronous watch failure"));
          failureQueued.countDown();
        } catch (Throwable failure) {
          invocationFailure.set(failure);
        } finally {
          releaseResponse.countDown();
        }
      });
      failureThread.start();
      stream.onNext(createRequest(17, new byte[] {1}, new byte[0], 0, false));
      failureThread.join(5_000);

      assertFalse(failureThread.isAlive());
      assertEquals(null, invocationFailure.get());
      assertTrue(failureQueued.await(5, TimeUnit.SECONDS));
      assertTrue(observer.failure != null);
      assertFalse(observer.concurrentTerminalWrite,
          "onError must be delivered by the same serialized writer as onNext");
      stream.close();
    }
  }

  @Test
  void clientHalfCloseKeepsActiveWatchOpenUntilTheRpcContextIsCancelled() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(18, new byte[] {1}, new byte[0], 0, false));
      stream.onCompleted();

      assertFalse(observer.completed, "a client half-close must not terminate the response side");
      assertEquals(0, subscription.closeCount.get(), "the active watch remains subscribed");
      subscription.add(event(5, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {5}, Optional.empty()));
      observer.readyHandler.run();

      assertEquals(2, observer.responses.size());
      assertTrue(observer.responses.get(0).getCreated());
      assertEquals(1, observer.responses.get(1).getEventsCount());
      assertEquals(5, observer.responses.get(1).getHeader().getRevision());
      assertFalse(observer.completed);
      assertEquals(0, subscription.closeCount.get());

      stream.close();
      assertEquals(1, subscription.closeCount.get());
      assertFalse(observer.completed,
          "server cancellation cleans resources without claiming normal stream completion");
    }
  }

  @Test
  void emptyClientHalfCloseLeavesWatchRpcOpenLikeEtcd() {
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> {
        throw new AssertionError("an empty half-close must not open a backend session");
      });
      stream.onCompleted();

      assertTrue(observer.responses.isEmpty(), "an empty Watch stream has no response to send");
      assertEquals(null, observer.failure);
      assertFalse(observer.completed,
          "etcd v3.5.23 keeps the Watch RPC open after request half-close, even without active watches");

      stream.close();
      assertFalse(observer.completed,
          "peer cancellation cleans the stream without reporting normal completion");
    }
  }

  @Test
  void halfClosedRpcRemainsOpenAfterItsLastWatchIsCompacted() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(23, new byte[] {1}, new byte[0], 0, false));
      stream.onCompleted();

      subscription.termination = Optional.of(WatchTermination.compacted(4));
      observer.readyHandler.run();

      assertEquals(2, observer.responses.size());
      assertTrue(observer.responses.get(0).getCreated());
      Rpc.WatchResponse compacted = observer.responses.get(1);
      assertTrue(compacted.getCanceled());
      assertEquals(23, compacted.getWatchId());
      assertEquals(4, compacted.getCompactRevision());
      assertEquals(1, subscription.closeCount.get(), "compaction releases the final watch resources");
      assertFalse(observer.completed,
          "etcd v3.5.23 keeps the bidirectional RPC open after its final watch is canceled");

      stream.close();
      assertEquals(1, subscription.closeCount.get());
      assertFalse(observer.completed);
    }
  }

  @Test
  void clientHalfCloseAllowsAnAcceptedCreateToFinishAsynchronously() {
    var subscription = new RecordingSubscription();
    var observer = new RecordingObserver();
    var opening = new CompletableFuture<WatchStream.Session>();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> opening);
      stream.onNext(createRequest(19, new byte[] {1}, new byte[0], 0, false));
      assertTrue(observer.responses.isEmpty(), "the session gate keeps the accepted create pending");

      stream.onCompleted();
      opening.complete(new RecordingSession(4, subscription));

      assertEquals(1, observer.responses.size());
      assertTrue(observer.responses.getFirst().getCreated(),
          "a half-close does not discard an already accepted Create request");
      assertFalse(observer.completed);
      assertEquals(0, subscription.closeCount.get());

      subscription.add(event(5, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {5}, Optional.empty()));
      observer.readyHandler.run();

      assertEquals(2, observer.responses.size());
      assertEquals(5, observer.responses.getLast().getHeader().getRevision());
      assertEquals(1, observer.responses.getLast().getEventsCount());
      assertFalse(observer.completed);

      stream.close();
      assertEquals(1, subscription.closeCount.get());
    }
  }

  @Test
  void clientHalfCloseAllowsAnAcceptedSubscriptionToFinishAsynchronously() {
    var subscription = new RecordingSubscription();
    var observer = new RecordingObserver();
    var opening = new CompletableFuture<WatchStream.OpenedWatch>();
    WatchStream.Session session = new WatchStream.Session() {
      @Override public long completedRevision() { return 4; }
      @Override public CompletableFuture<Long> currentRevision() {
        return CompletableFuture.completedFuture(4L);
      }
      @Override public CompletableFuture<WatchStream.OpenedWatch> subscribe(
          KeyRange range, Rpc.WatchCreateRequest request) {
        return opening;
      }
    };

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(),
          () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(22, new byte[] {1}, new byte[0], 0, false));
      assertTrue(observer.responses.isEmpty(), "the subscription gate keeps Create pending");

      stream.onCompleted();
      opening.complete(new WatchStream.OpenedWatch(4, 0, -1, subscription, false));

      assertEquals(1, observer.responses.size());
      assertTrue(observer.responses.getFirst().getCreated(),
          "a half-close does not discard an accepted asynchronous subscription");
      assertEquals(0, subscription.closeCount.get());
      subscription.add(event(5, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {5}, Optional.empty()));
      observer.readyHandler.run();

      assertEquals(2, observer.responses.size());
      assertEquals(5, observer.responses.getLast().getHeader().getRevision());
      assertEquals(1, observer.responses.getLast().getEventsCount());
      assertFalse(observer.completed);

      stream.close();
      assertEquals(1, subscription.closeCount.get());
    }
  }

  @Test
  void requestStreamErrorDuringPollClosesTheDetachedDelivery() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();
    var streamHolder = new AtomicReference<WatchStream>();
    subscription.afterPoll = () -> streamHolder.get().onError(
        new StatusRuntimeException(io.grpc.Status.CANCELLED));

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      streamHolder.set(stream);
      subscription.add(event(5, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {5}, Optional.empty()));
      stream.onNext(createRequest(20, new byte[] {1}, new byte[0], 0, false));

      assertEquals(1, observer.responses.size(), "peer termination emits no further response");
      assertTrue(observer.responses.getFirst().getCreated());
      assertEquals(1, subscription.closeCount.get());
      assertEquals(1, subscription.releases.get(), "a polled delivery is released after onError");
      assertEquals(null, observer.failure, "the already failed request transport is not written again");
    }
  }

  @Test
  void replayAuthenticationAndPermissionFailuresUseTheirRpcStatus() {
    for (BackendFailure.Category category : List.of(
        BackendFailure.Category.AUTHENTICATION,
        BackendFailure.Category.INVALID_AUTH_TOKEN,
        BackendFailure.Category.PERMISSION)) {
      var subscription = new RecordingSubscription();
      subscription.termination = Optional.of(WatchTermination.failure(new BackendFailure(category)));
      var session = new RecordingSession(4, subscription);
      var observer = new RecordingObserver();

      try (var scope = RequestScope.withoutDeadline()) {
        var stream = stream(observer, scope, timers(),
            () -> CompletableFuture.completedFuture(session));
        stream.onNext(createRequest(21, new byte[] {1}, new byte[0], 0, false));

        assertEquals(1, observer.responses.size());
        assertTrue(observer.responses.getFirst().getCreated());
        Status.Code expectedStatus = switch (category) {
          case AUTHENTICATION -> Status.Code.INVALID_ARGUMENT;
          case INVALID_AUTH_TOKEN -> Status.Code.UNAUTHENTICATED;
          case PERMISSION -> Status.Code.PERMISSION_DENIED;
          default -> throw new AssertionError("unexpected test category: " + category);
        };
        assertEquals(expectedStatus,
            Status.fromThrowable(observer.failure).getCode());
        assertEquals(1, subscription.closeCount.get());
        stream.close();
      }
    }
  }

  @Test
  void normalizesEmptyKeyAndFromKeyRangeSentinelAsEtcdDoes() {
    var session = new RecordingSession(4, new RecordingSubscription(), new RecordingSubscription());
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(31, new byte[0], new byte[0], 0, false));
      stream.onNext(createRequest(32, new byte[0], new byte[] {0}, 0, false));

      ByteKey normalizedKey = ByteKey.copyOf(new byte[] {0});
      assertEquals(KeyRange.Kind.POINT, session.ranges.get(0).kind());
      assertEquals(Optional.of(normalizedKey), session.ranges.get(0).lowerInclusive());
      assertEquals(Optional.empty(), session.ranges.get(0).upperExclusive());
      assertEquals(KeyRange.Kind.RANGE, session.ranges.get(1).kind());
      assertEquals(Optional.of(normalizedKey), session.ranges.get(1).lowerInclusive());
      assertEquals(Optional.empty(), session.ranges.get(1).upperExclusive());
      stream.close();
    }
  }

  @Test
  void sendsAdjacentRevisionsWithoutWaitingForAnotherPoll() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(9, new byte[] {1}, new byte[0], 0, false));
      subscription.add(event(5, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {5}, Optional.empty()));
      subscription.add(event(6, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {6}, Optional.empty()));
      observer.readyHandler.run();

      assertEquals(3, observer.responses.size());
      assertTrue(observer.responses.get(0).getCreated());
      assertEquals(5, observer.responses.get(1).getHeader().getRevision());
      assertEquals(6, observer.responses.get(2).getHeader().getRevision());
      assertEquals(2, subscription.releases.get());
      stream.close();
    }
  }

  @Test
  void matchingCancelStopsTheRemainingFragmentsAndReleasesTheirDeliveries() {
    var subscription = new RecordingSubscription();
    var session = new RecordingSession(4, subscription);
    var observer = new RecordingObserver();
    var cancelSent = new AtomicInteger();
    byte[] key = {1};

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(),
          () -> CompletableFuture.completedFuture(session), 512);
      stream.onNext(createRequest(7, key, new byte[0], 0, false, false, true));
      for (int index = 0; index < 5; index++) {
        subscription.add(event(5, PreparedEvent.Kind.PUT, key, new byte[160], Optional.empty()));
      }
      observer.onNextHook = () -> {
        if (observer.responses.size() == 2 && cancelSent.compareAndSet(0, 1)) {
          stream.onNext(Rpc.WatchRequest.newBuilder().setCancelRequest(
              Rpc.WatchCancelRequest.newBuilder().setWatchId(7)).build());
        }
      };
      observer.readyHandler.run();

      assertEquals(3, observer.responses.size(),
          "cancellation discards every unsent fragment of the active revision");
      Rpc.WatchResponse sentFragment = observer.responses.get(1);
      assertEquals(7, sentFragment.getWatchId());
      assertTrue(sentFragment.getFragment());
      assertTrue(observer.responses.get(2).getCanceled());
      assertEquals(7, observer.responses.get(2).getWatchId());
      assertEquals(5, subscription.releases.get(),
          "all retained event deliveries are released exactly once");
      assertEquals(1, subscription.closeCount.get());
      stream.close();
      assertEquals(5, subscription.releases.get());
      assertEquals(1, subscription.closeCount.get());
    }
  }

  @Test
  void peerCancellationClosesEveryActiveSubscription() {
    var first = new RecordingSubscription();
    var second = new RecordingSubscription();
    var session = new RecordingSession(8, first, second);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(7, new byte[] {1}, new byte[0], 0, false));
      stream.onNext(createRequest(8, new byte[] {2}, new byte[0], 0, false));

      observer.cancelled = true;
      observer.cancelHandler.run();

      assertEquals(1, first.closeCount.get());
      assertEquals(1, second.closeCount.get());
      assertEquals(2, observer.responses.size(), "peer cancellation emits no more stream messages");
      stream.close();
      assertEquals(1, first.closeCount.get());
      assertEquals(1, second.closeCount.get());
    }
  }

  @Test
  void cancelCleanupFailureReportsAnErrorAndStillClosesOtherWatchers() {
    var cancelled = new RecordingSubscription();
    cancelled.closeFailure = new IllegalStateException("subscription cleanup failed");
    var active = new RecordingSubscription();
    var session = new RecordingSession(8, cancelled, active);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(21, new byte[] {1}, new byte[0], 0, false));
      stream.onNext(createRequest(22, new byte[] {2}, new byte[0], 0, false));
      stream.onNext(Rpc.WatchRequest.newBuilder()
          .setCancelRequest(Rpc.WatchCancelRequest.newBuilder().setWatchId(21)).build());

      assertTrue(observer.failure != null, "a failed cancellation cleanup must fail the stream");
      assertEquals(1, cancelled.closeCount.get());
      assertEquals(1, active.closeCount.get(), "failure cleanup also closes remaining subscriptions");
      assertEquals(2, observer.responses.size(), "failed cancellation does not claim a successful Canceled response");
      stream.close();
      assertEquals(1, cancelled.closeCount.get());
      assertEquals(1, active.closeCount.get());
    }
  }

  @Test
  void subscriptionPollFailureFailsTheWatchStreamInsteadOfReportingWatcherCancellation() {
    var subscription = new RecordingSubscription();
    subscription.pollFailure = new IllegalStateException("replay cleanup failed");
    var session = new RecordingSession(8, subscription);
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(),
          () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(23, new byte[] {1}, new byte[0], 0, false));

      assertNotNull(observer.failure, "a subscription poll failure must fail the RPC stream");
      assertEquals(1, subscription.closeCount.get());
      assertTrue(observer.responses.stream().noneMatch(Rpc.WatchResponse::getCanceled),
          "a cleanup failure must not be presented as watcher-local resource cancellation");
      stream.close();
      assertEquals(1, subscription.closeCount.get());
    }
  }

  @Test
  void watcherCloseStillClosesSubscriptionWhenPendingDeliveryCleanupThrowsError() {
    var subscription = new RecordingSubscription();
    var subscriptionFailure = new IllegalStateException("subscription cleanup failed");
    subscription.closeFailure = subscriptionFailure;
    var watcher = new WatchRegistry(new CompactedWatchIdBudget(1))
        .registerActive(1, 1, 1, false, false, false, false, false, subscription).watcher();
    var deliveryFailure = new AssertionError("pending delivery cleanup failed");
    var pendingDelivery = new WatchStream.Delivery(2,
        event(2, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {2}, Optional.empty()),
        () -> { throw deliveryFailure; });
    watcher.defer(pendingDelivery);

    assertSame(deliveryFailure, assertThrows(AssertionError.class, watcher::close));

    assertEquals(1, subscription.closeCount.get(),
        "subscription cleanup runs even when pending delivery cleanup throws Error");
    assertEquals(1, deliveryFailure.getSuppressed().length);
    assertSame(subscriptionFailure, deliveryFailure.getSuppressed()[0]);
  }

  @Test
  void outputReleaseAttemptsEveryDeliveryAndPrioritizesFatalCleanupFailure() {
    var firstFailure = new IllegalStateException("first delivery cleanup failed");
    var fatalFailure = new AssertionError("second delivery cleanup failed");
    var lastFailure = new IllegalArgumentException("last delivery cleanup failed");
    var firstAttempts = new AtomicInteger();
    var secondAttempts = new AtomicInteger();
    var lastAttempts = new AtomicInteger();
    var first = deliveryWithCleanupFailure(firstAttempts, firstFailure);
    var second = deliveryWithCleanupFailure(secondAttempts, fatalFailure);
    var last = deliveryWithCleanupFailure(lastAttempts, lastFailure);
    var output = new WatchEventBatchEncoder.Output(Rpc.WatchResponse.getDefaultInstance(),
        List.of(first, second, last));

    assertSame(fatalFailure, assertThrows(AssertionError.class, output::release));

    assertEquals(1, firstAttempts.get());
    assertEquals(1, secondAttempts.get());
    assertEquals(1, lastAttempts.get(), "a cleanup failure does not skip later deliveries");
    assertEquals(2, fatalFailure.getSuppressed().length);
    assertSame(firstFailure, fatalFailure.getSuppressed()[0]);
    assertSame(lastFailure, fatalFailure.getSuppressed()[1]);
  }

  private static WatchStream.Delivery deliveryWithCleanupFailure(
      AtomicInteger attempts, Throwable failure) {
    return new WatchStream.Delivery(2,
        event(2, PreparedEvent.Kind.PUT, new byte[] {1}, new byte[] {2}, Optional.empty()),
        () -> {
          attempts.incrementAndGet();
          if (failure instanceof Error fatal) throw fatal;
          throw (RuntimeException) failure;
        });
  }

  private static void assertTerminationCancelsOnlyItsWatchAndReleasesQueuedDeliveries(
      String reason) {
    var terminated = new RecordingSubscription();
    var active = new RecordingSubscription();
    var replacement = new RecordingSubscription();
    var session = new RecordingSession(8, terminated, active, replacement);
    session.currentRevision = CompletableFuture.completedFuture(9L);
    var observer = new RecordingObserver();
    byte[] firstKey = {1};
    byte[] secondKey = {2};

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(),
          () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(51, firstKey, new byte[0], 0, false));
      stream.onNext(createRequest(52, secondKey, new byte[0], 0, false));
      assertEquals(2, observer.responses.size());

      terminated.add(event(9, PreparedEvent.Kind.PUT, firstKey, new byte[] {9}, Optional.empty()));
      terminated.terminate(reason);
      observer.ready = true;
      observer.readyHandler.run();

      assertEquals(3, observer.responses.size());
      Rpc.WatchResponse cancelled = observer.responses.get(2);
      assertFalse(cancelled.getCreated());
      assertTrue(cancelled.getCanceled());
      assertEquals(51, cancelled.getWatchId());
      assertEquals(reason, cancelled.getCancelReason());
      assertEquals(9, cancelled.getHeader().getRevision(),
          "termination reports the current session revision when it exceeds the tailer checkpoint");
      assertEquals(1, terminated.closeCount.get());
      assertEquals(1, terminated.releases.get(),
          "closing a terminated watcher releases queued delivery");
      assertEquals(0, active.closeCount.get(), "a sibling watcher remains registered");

      stream.onNext(createRequest(51, firstKey, new byte[0], 0, false));
      assertEquals(4, observer.responses.size(), "a terminated Watch ID is no longer reserved");
      assertTrue(observer.responses.get(3).getCreated());
      assertEquals(51, observer.responses.get(3).getWatchId());

      active.add(event(10, PreparedEvent.Kind.PUT, secondKey, new byte[] {10}, Optional.empty()));
      observer.readyHandler.run();
      Rpc.WatchResponse siblingEvent = observer.responses.get(4);
      assertEquals(52, siblingEvent.getWatchId());
      assertEquals(1, siblingEvent.getEventsCount());
      assertEquals(0, active.closeCount.get());

      stream.close();
      assertEquals(1, terminated.closeCount.get());
      assertEquals(1, active.closeCount.get());
      assertEquals(1, replacement.closeCount.get());
    }
  }

  @Test
  void asynchronousTerminationRevisionFailureFailsStreamWithoutCanceledResponse() {
    var terminated = new RecordingSubscription();
    var session = new RecordingSession(8, terminated);
    var revision = new CompletableFuture<Long>();
    session.currentRevision = revision;
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(61, new byte[] {1}, new byte[0], 0, false));
      terminated.terminate("watch source closed");
      assertEquals(1, observer.responses.size(), "termination waits for the current revision lookup");

      revision.completeExceptionally(new IllegalStateException("revision read failed"));

      assertEquals(1, observer.responses.size(), "a failed revision lookup does not emit a stale cancel header");
      assertTrue(observer.failure != null, "a revision lookup failure terminates the Watch stream");
      assertEquals(1, terminated.closeCount.get());
      stream.close();
    }
  }

  @Test
  void lateTerminationRevisionDoesNotWriteAfterStreamClose() {
    var terminated = new RecordingSubscription();
    var session = new RecordingSession(8, terminated);
    var revision = new CompletableFuture<Long>();
    session.currentRevision = revision;
    var observer = new RecordingObserver();

    try (var scope = RequestScope.withoutDeadline()) {
      var stream = stream(observer, scope, timers(), () -> CompletableFuture.completedFuture(session));
      stream.onNext(createRequest(62, new byte[] {1}, new byte[0], 0, false));
      terminated.terminate("watch source closed");
      assertEquals(1, observer.responses.size());

      stream.close();
      revision.complete(9L);

      assertEquals(1, observer.responses.size(), "late completion after close is discarded");
      assertEquals(1, terminated.closeCount.get());
      assertTrue(observer.failure == null, "a late success does not write an error after close");
    }
  }

  private static WatchStream stream(RecordingObserver observer, RequestScope scope,
      ScheduledExecutorService timers, WatchStream.SessionProvider sessions) {
    return stream(observer, scope, timers, sessions, CompactedWatchIdBudget.processWide());
  }

  private static WatchStream stream(RecordingObserver observer, RequestScope scope,
      ScheduledExecutorService timers, WatchStream.SessionProvider sessions,
      CompactedWatchIdBudget compactedWatchIdBudget) {
    return new WatchStream(observer, scope, Runnable::run, timers, sessions,
        CLUSTER_ID, MEMBER_ID, 1 << 20, compactedWatchIdBudget);
  }

  private static WatchStream stream(RecordingObserver observer, RequestScope scope,
      ScheduledExecutorService timers, WatchStream.SessionProvider sessions,
      int maximumResponseBytes) {
    return new WatchStream(observer, scope, Runnable::run, timers, sessions,
        CLUSTER_ID, MEMBER_ID, maximumResponseBytes, CompactedWatchIdBudget.processWide());
  }

  private static Rpc.WatchRequest createRequest(long id, byte[] key, byte[] rangeEnd,
      long startRevision, boolean prevKv, Rpc.WatchCreateRequest.FilterType... filters) {
    return createRequest(id, key, rangeEnd, startRevision, prevKv, false, false, filters);
  }

  private static Rpc.WatchRequest createRequest(long id, byte[] key, byte[] rangeEnd,
      long startRevision, boolean prevKv, boolean progressNotify, boolean fragment,
      Rpc.WatchCreateRequest.FilterType... filters) {
    var create = Rpc.WatchCreateRequest.newBuilder().setWatchId(id)
        .setKey(ByteString.copyFrom(key)).setRangeEnd(ByteString.copyFrom(rangeEnd))
        .setStartRevision(startRevision).setPrevKv(prevKv).setProgressNotify(progressNotify)
        .setFragment(fragment).addAllFilters(List.of(filters)).build();
    return Rpc.WatchRequest.newBuilder().setCreateRequest(create).build();
  }

  private static Rpc.WatchRequest progressRequest() {
    return Rpc.WatchRequest.newBuilder()
        .setProgressRequest(Rpc.WatchProgressRequest.getDefaultInstance()).build();
  }

  private static PreparedEvent event(long revision, PreparedEvent.Kind kind, byte[] key,
      byte[] value, Optional<StoredRecord> previous) {
    ByteKey eventKey = ByteKey.copyOf(key);
    StorageMetadata metadata = new StorageMetadata(1, revision, revision, 0,
        kind == PreparedEvent.Kind.PUT ? StorageMetadata.State.LIVE
            : StorageMetadata.State.TOMBSTONE,
        value == null ? 0 : value.length);
    StoredRecord current = kind == PreparedEvent.Kind.PUT
        ? StoredRecord.live(eventKey, metadata, value)
        : StoredRecord.metadataOnly(eventKey, metadata);
    return new PreparedEvent(new TransactionId(new UUID(0, revision)), 1, revision - 1,
        revision - 1, 0, kind, current, previous);
  }

  private static StoredRecord record(byte[] key, long modificationRevision, long version,
      byte[] value) {
    return StoredRecord.live(ByteKey.copyOf(key),
        new StorageMetadata(1, modificationRevision, version, 0,
            StorageMetadata.State.LIVE, value.length), value);
  }

  private static ScheduledExecutorService timers() {
    ScheduledExecutorService timers = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> future = mock(ScheduledFuture.class);
    doAnswer(invocation -> future).when(timers)
        .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    return timers;
  }

  private static ScheduledExecutorService timers(
      AtomicReference<Runnable> periodicTick, long periodicDelayMillis) {
    ScheduledExecutorService timers = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> future = mock(ScheduledFuture.class);
    doAnswer(invocation -> {
      Runnable task = invocation.getArgument(0);
      long delay = invocation.getArgument(1);
      if (delay == periodicDelayMillis) periodicTick.set(task);
      return future;
    }).when(timers).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    return timers;
  }

  private static ScheduledExecutorService immediateZeroDelayTimers() {
    ScheduledExecutorService timers = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> future = mock(ScheduledFuture.class);
    doAnswer(invocation -> {
      Runnable task = invocation.getArgument(0);
      long delay = invocation.getArgument(1);
      if (delay == 0) task.run();
      return future;
    }).when(timers).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    return timers;
  }

  private static int indexOfResponse(List<Rpc.WatchResponse> responses,
      java.util.function.Predicate<Rpc.WatchResponse> predicate) {
    for (int index = 0; index < responses.size(); index++) {
      if (predicate.test(responses.get(index))) return index;
    }
    return -1;
  }

  private static final class CountingDirectExecutor implements Executor {
    private final AtomicInteger executions = new AtomicInteger();

    @Override public void execute(Runnable command) {
      executions.incrementAndGet();
      command.run();
    }
  }

  private static final class RecordingSession implements WatchStream.Session {
    private long revision;
    private final long compactRevision;
    private final boolean historicalReplaySupported;
    private final ArrayDeque<RecordingSubscription> available = new ArrayDeque<>();
    private final List<RecordingSubscription> subscriptions = new ArrayList<>();
    private final List<KeyRange> ranges = new ArrayList<>();
    private final List<Long> startRevisions = new ArrayList<>();
    private CompletableFuture<Long> currentRevision;
    private KeyRange range;

    private RecordingSession(long revision, RecordingSubscription... subscriptions) {
      this(revision, -1, false, subscriptions);
    }

    private RecordingSession(long revision, long compactRevision, RecordingSubscription... subscriptions) {
      this(revision, compactRevision, false, subscriptions);
    }

    private RecordingSession(long revision, boolean historicalReplaySupported,
        RecordingSubscription... subscriptions) {
      this(revision, -1, historicalReplaySupported, subscriptions);
    }

    private RecordingSession(long revision, long compactRevision, boolean historicalReplaySupported,
        RecordingSubscription... subscriptions) {
      this.revision = revision;
      this.compactRevision = compactRevision;
      this.historicalReplaySupported = historicalReplaySupported;
      available.addAll(List.of(subscriptions));
    }

    @Override public long completedRevision() { return revision; }

    @Override public CompletableFuture<Long> currentRevision() {
      return currentRevision == null ? CompletableFuture.completedFuture(revision) : currentRevision;
    }

    @Override
    public CompletableFuture<WatchStream.OpenedWatch> subscribe(KeyRange requestedRange,
        Rpc.WatchCreateRequest request) {
      range = requestedRange;
      ranges.add(requestedRange);
      startRevisions.add(request.getStartRevision());
      RecordingSubscription subscription = available.removeFirst();
      subscriptions.add(subscription);
      return CompletableFuture.completedFuture(
          new WatchStream.OpenedWatch(revision, 0, compactRevision, subscription,
              historicalReplaySupported));
    }
  }

  private static final class RecordingSubscription implements WatchStream.Subscription {
    private final ArrayDeque<WatchStream.Delivery> deliveries = new ArrayDeque<>();
    private final AtomicInteger releases = new AtomicInteger();
    private final AtomicInteger closeCount = new AtomicInteger();
    private RuntimeException closeFailure;
    private RuntimeException pollFailure;
    private Optional<WatchTermination> termination = Optional.empty();
    private Runnable afterPoll;
    private Runnable wakeup;
    private int pollCalls;
    private boolean liveDeliveryClosed;
    private boolean progressReady = true;
    private boolean closed;

    private void add(PreparedEvent event) {
      deliveries.addLast(new WatchStream.Delivery(event.current().metadata().modificationRevision(),
          event, releases::incrementAndGet));
    }

    private void signalWakeup() {
      if (wakeup != null) wakeup.run();
    }

    private void terminate(String reason) {
      String normalized = reason.toLowerCase(java.util.Locale.ROOT);
      termination = normalized.contains("resource limit") || normalized.contains("budget")
          ? Optional.of(WatchTermination.resourceLimit(reason))
          : normalized.contains("unavailable") || normalized.contains("closed")
              ? Optional.of(WatchTermination.unavailable(reason))
              : Optional.of(WatchTermination.cancelled(reason));
      if (wakeup != null) wakeup.run();
    }

    @Override public Optional<WatchStream.Delivery> poll() {
      pollCalls++;
      if (closed || liveDeliveryClosed || termination.isPresent()) return Optional.empty();
      if (pollFailure != null) throw pollFailure;
      WatchStream.Delivery delivery = deliveries.pollFirst();
      Runnable hook = afterPoll;
      afterPoll = null;
      if (delivery != null && hook != null) hook.run();
      return Optional.ofNullable(delivery);
    }

    @Override public Optional<WatchTermination> termination() { return termination; }

    @Override public boolean progressReady(long revision) { return progressReady; }

    @Override
    public void setWakeup(Runnable wakeup) {
      this.wakeup = wakeup;
      if (wakeup != null && (termination.isPresent() || !deliveries.isEmpty())) wakeup.run();
    }

    @Override public void closeLiveDelivery() {
      if (liveDeliveryClosed) return;
      liveDeliveryClosed = true;
      wakeup = null;
      while (!deliveries.isEmpty()) deliveries.removeFirst().close();
    }

    @Override public void close() {
      if (closed) return;
      closed = true;
      closeLiveDelivery();
      closeCount.incrementAndGet();
      while (!deliveries.isEmpty()) deliveries.removeFirst().close();
      if (closeFailure != null) throw closeFailure;
    }
  }

  private static final class RecordingObserver extends ServerCallStreamObserver<Rpc.WatchResponse> {
    private final List<Rpc.WatchResponse> responses = new ArrayList<>();
    private Runnable readyHandler;
    private Runnable cancelHandler;
    private boolean ready = true;
    private boolean cancelled;
    private boolean completed;
    private int requests;
    private Throwable failure;
    private Runnable onNextHook;
    private boolean writingResponse;
    private boolean concurrentTerminalWrite;

    @Override public boolean isCancelled() { return cancelled; }
    @Override public void setOnCancelHandler(Runnable handler) { cancelHandler = handler; }
    @Override public void setCompression(String compression) {}
    @Override public void setOnReadyHandler(Runnable handler) { readyHandler = handler; }
    @Override public boolean isReady() { return ready; }
    @Override public void request(int count) { requests += count; }
    @Override public void disableAutoInboundFlowControl() {}
    @Override public void setMessageCompression(boolean enabled) {}
    @Override public void onNext(Rpc.WatchResponse response) {
      writingResponse = true;
      try {
        responses.add(response);
        if (onNextHook != null) onNextHook.run();
      } finally {
        writingResponse = false;
      }
    }
    @Override public void onError(Throwable cause) {
      concurrentTerminalWrite |= writingResponse;
      failure = cause;
    }
    @Override public void onCompleted() { completed = true; }
  }
}
