package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.AuxiliaryFormatUpgrade;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitDecisionRepository;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationRepository;
import io.etcdhbase.core.commit.RecoveryManager;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class M3OwnerLeaseTest {
  private static final ByteKey PROFILE = ByteKey.copyOf(new byte[32]);

  @Test
  void recoversBeforeReadyRenewsAndFencesTheLaneOnHeartbeatFailure() throws Exception {
    var store = new MemoryControl();
    var owners = new CommitOwnerService(store);
    var lost = new CountDownLatch(1);
    try (var startup = RequestScope.withTimeout(Duration.ofSeconds(5));
         var competing = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var lease = M3OwnerLease.acquire(recovery(store, owners), owners, "owner-a",
          Duration.ofMillis(1200), Duration.ofMillis(200), Duration.ofMillis(400),
          startup, lost::countDown).join();
      try {
        assertEquals(1, lease.token().epoch());
        assertTrue(lease.isReady());
        var rejected = assertThrows(CompletionException.class, () ->
            M3OwnerLease.acquire(recovery(store, owners), owners, "owner-b",
                Duration.ofMillis(1200), Duration.ofMillis(200), Duration.ofMillis(400),
                competing, () -> {}).join());
        assertEquals(BackendFailure.Category.UNAVAILABLE,
            assertInstanceOf(BackendFailure.class, rejected.getCause()).category());
        assertTrue(store.heartbeat.await(5, TimeUnit.SECONDS));
        store.failHeartbeat = true;
        assertTrue(lost.await(5, TimeUnit.SECONDS));
        assertFalse(lease.isReady());
      } finally {
        lease.close();
        lease.drained().get(5, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  void startupFormatUpgradeRotatesTheFenceWithoutLosingOwnerRenewal() throws Exception {
    var store = new MemoryControl();
    var owners = new CommitOwnerService(store);
    var clock = new MutableClock(System.currentTimeMillis());
    try (var startup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var lease = M3OwnerLease.acquire(recovery(store, owners), owners, "owner-a",
          Duration.ofMillis(1200), Duration.ofMillis(200), Duration.ofMillis(400), startup, () -> {}, clock).join();
      try {
        var previous = lease.token();
        long previousDeadline = store.currentState().ownerLeaseDeadlineEpochMillis();
        clock.advance(400);
        var upgraded = lease.transitionFence(owners,
            current -> new AuxiliaryFormatUpgrade(store)
                .upgrade(current, System.currentTimeMillis(), startup), startup)
            .get(5, TimeUnit.SECONDS);

        assertEquals(previous.instance(), upgraded.instance());
        assertTrue(upgraded.epoch() > previous.epoch());
        assertEquals(upgraded, lease.token());
        assertTrue(store.currentState().supportsAuxiliaryRecords());
        assertTrue(store.currentState().ownerLeaseDeadlineEpochMillis() > previousDeadline,
            "the fence transition extends the persisted owner deadline");
        assertTrue(lease.isReady());
      } finally {
        lease.close();
        lease.drained().get(5, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  void closingWaitsForTheActualInFlightHeartbeatProducer() throws Exception {
    var store = new MemoryControl();
    store.blockHeartbeat = true;
    var owners = new CommitOwnerService(store);
    var lost = new AtomicInteger();
    try (var startup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var lease = M3OwnerLease.acquire(recovery(store, owners), owners, "owner-a",
          Duration.ofSeconds(3), Duration.ofMillis(200), Duration.ofSeconds(1),
          startup, lost::incrementAndGet).join();
      try {
        assertTrue(store.heartbeat.await(5, TimeUnit.SECONDS));
        lease.close();
        assertFalse(lease.drained().isDone(), "a cancelled waiter does not stop the backend producer");
        store.physicalHeartbeat.complete(null);
        lease.drained().get(5, TimeUnit.SECONDS);
        assertEquals(0, lost.get(), "an intentional shutdown is not a lost-owner notification");
      } finally {
        store.physicalHeartbeat.complete(null);
        lease.close();
      }
    }
  }

  @Test
  void cannotBecomeReadyWhenOwnerVerificationFailsAfterRecovery() {
    var store = new MemoryControl();
    store.failVerificationRead = true;
    var owners = new CommitOwnerService(store);
    try (var startup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var failure = assertThrows(CompletionException.class, () ->
          M3OwnerLease.acquire(recovery(store, owners), owners, "owner-a", Duration.ofSeconds(3),
              Duration.ofMillis(200), Duration.ofSeconds(1), startup, () -> {}).join());
      assertEquals(BackendFailure.Category.UNAVAILABLE,
          assertInstanceOf(BackendFailure.class, failure.getCause()).category());
    }
  }

  @Test
  void heartbeatTimeoutFencesWritesButKeepsTheProducerOwnedUntilItStops() throws Exception {
    var store = new MemoryControl();
    store.blockHeartbeat = true;
    var owners = new CommitOwnerService(store);
    var lost = new CountDownLatch(1);
    try (var startup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var lease = M3OwnerLease.acquire(recovery(store, owners), owners, "owner-a",
          Duration.ofMillis(1200), Duration.ofMillis(200), Duration.ofMillis(400),
          startup, lost::countDown).join();
      try {
        assertTrue(store.heartbeat.await(5, TimeUnit.SECONDS));
        assertTrue(lost.await(5, TimeUnit.SECONDS));
        assertFalse(lease.isReady());
        assertFalse(lease.drained().isDone());
        store.physicalHeartbeat.complete(null);
        lease.drained().get(5, TimeUnit.SECONDS);
      } finally {
        store.physicalHeartbeat.complete(null);
        lease.close();
      }
    }
  }

  @Test
  void cannotBecomeReadyWhenVerificationReturnsAfterItsProposedLeaseExpired() {
    var clock = new MutableClock(1_000);
    var store = new MemoryControl();
    store.delayVerificationRead = true;
    var owners = new CommitOwnerService(store);
    try (var startup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var future = M3OwnerLease.acquire(recovery(store, owners), owners, "owner-a", Duration.ofSeconds(3),
          Duration.ofMillis(200), Duration.ofSeconds(1), startup, () -> {}, clock);
      assertFalse(future.isDone());
      clock.advance(3_001);
      store.completeVerificationRead();
      var failure = assertThrows(CompletionException.class, future::join);
      assertEquals(BackendFailure.Category.UNAVAILABLE,
          assertInstanceOf(BackendFailure.class, failure.getCause()).category());
    }
  }

  @Test
  void readinessClosesAsSoonAsTheLastVerifiedLeaseExpires() throws Exception {
    var clock = new MutableClock(1_000);
    var store = new MemoryControl();
    var owners = new CommitOwnerService(store);
    try (var startup = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var lease = M3OwnerLease.acquire(recovery(store, owners), owners, "owner-a", Duration.ofSeconds(3),
          Duration.ofMillis(200), Duration.ofSeconds(1), startup, () -> {}, clock).join();
      try {
        assertTrue(lease.isReady());
        clock.advance(3_001);
        assertFalse(lease.isReady());
      } finally {
        lease.close();
        lease.drained().get(5, TimeUnit.SECONDS);
      }
    }
  }

  private static RecoveryManager recovery(MemoryControl store, CommitOwnerService owners) {
    InvocationRepository invocations = new InvocationRepository() {
      @Override public CompletableFuture<Optional<InvocationBinding>> read(InvocationId id, RequestScope scope) {
        throw new AssertionError("no publication should require an invocation lookup");
      }
      @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
          InvocationBinding binding, RequestScope scope) { throw new AssertionError("unexpected invocation create"); }
      @Override public CompletableFuture<Boolean> compareAndSet(
          InvocationBinding before, InvocationBinding after, RequestScope scope) {
        throw new AssertionError("unexpected invocation transition");
      }
    };
    CommitDecisionRepository decisions = new CommitDecisionRepository() {
      @Override public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope) {
        throw new AssertionError("unexpected decision lookup");
      }
      @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
          CommitDecision decision, RequestScope scope) { throw new AssertionError("unexpected decision create"); }
    };
    var outcomes = new CommitOutcomeResolver(store, decisions);
    return new RecoveryManager(PROFILE, store, owners, invocations, outcomes,
        (invocation, publication, scope) -> { throw new AssertionError("unexpected finalization"); });
  }

  private static final class MemoryControl implements CommitControlStore {
    private CommitState state = CommitState.initial(PROFILE, 1);
    private final CountDownLatch heartbeat = new CountDownLatch(1);
    private volatile boolean failHeartbeat;
    private volatile boolean blockHeartbeat;
    private boolean failVerificationRead;
    private boolean delayVerificationRead;
    private final CompletableFuture<Optional<ControlSnapshot>> delayedVerification = new CompletableFuture<>();
    private ControlSnapshot verificationSnapshot;
    private int reads;
    private final CompletableFuture<Void> physicalHeartbeat = new CompletableFuture<>();

    @Override public synchronized CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      reads++;
      if (reads == 4 && delayVerificationRead) {
        verificationSnapshot = snapshot(state);
        return delayedVerification;
      }
      if (reads >= 4 && failVerificationRead) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      return CompletableFuture.completedFuture(Optional.of(snapshot(state)));
    }
    @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState initial, RequestScope scope) {
      throw new AssertionError("control is already initialized");
    }
    @Override public synchronized CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      if (observed.state().stateVersion() != state.stateVersion()) return CompletableFuture.completedFuture(false);
      if (replacement.ownerEpoch() == state.ownerEpoch()) {
        if (blockHeartbeat && reads > 4) {
          var reply = new CompletableFuture<Boolean>();
          scope.trackPhysicalProducer(physicalHeartbeat);
          scope.registerScopedCleanup(() -> reply.completeExceptionally(new CancellationException()),
              physicalHeartbeat);
          heartbeat.countDown();
          return reply;
        }
        if (!blockHeartbeat) heartbeat.countDown();
        if (failHeartbeat) {
          return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
        }
      }
      state = replacement;
      return CompletableFuture.completedFuture(true);
    }
    private synchronized CommitState currentState() { return state; }
    private static ControlSnapshot snapshot(CommitState state) {
      return new ControlSnapshot(state, ByteBuffer.allocate(Long.BYTES).putLong(state.stateVersion()).array());
    }
    private synchronized void completeVerificationRead() {
      delayedVerification.complete(Optional.of(verificationSnapshot));
    }
  }

  private static final class MutableClock extends Clock {
    private final AtomicLong millis;
    private MutableClock(long initial) { millis = new AtomicLong(initial); }
    private void advance(long amount) { millis.addAndGet(amount); }
    @Override public ZoneId getZone() { return ZoneId.of("UTC"); }
    @Override public Clock withZone(ZoneId zone) { return this; }
    @Override public Instant instant() { return Instant.ofEpochMilli(millis.get()); }
    @Override public long millis() { return millis.get(); }
  }
}
