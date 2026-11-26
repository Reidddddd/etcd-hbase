package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationRepository;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.commit.PublicationSlot;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class MutationOwnerLaneTest {
  private static final String GROUP = "group-a";
  private static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);
  private static final ByteKey CONTEXT = ByteKey.copyOf("context".getBytes(StandardCharsets.UTF_8));
  private static final long LONG_TIMEOUT = TimeUnit.SECONDS.toNanos(30);

  @Test
  void nextRequestBindsOnlyAfterThePreviousPublicationFinalizes() throws Exception {
    var firstResult = new CompletableFuture<OwnerForwarding.Reply>();
    var firstStarted = new CountDownLatch(1);
    var firstBinding = new AtomicReference<InvocationBinding>();
    var secondBinding = new AtomicReference<InvocationBinding>();
    var calls = new AtomicInteger();
    try (Fixture fixture = new Fixture((request, binding, identity, scope) -> {
      if (calls.getAndIncrement() == 0) {
        firstBinding.set(binding);
        firstStarted.countDown();
        return firstResult;
      }
      secondBinding.set(binding);
      return CompletableFuture.completedFuture(new OwnerForwarding.Completed(request.invocationId().bytes()));
    })) {
      var first = request(LONG_TIMEOUT);
      var second = request(LONG_TIMEOUT);
      var firstScope = RequestScope.withoutDeadline();
      var secondScope = RequestScope.withoutDeadline();
      try {
        var firstReply = fixture.lane.receive("trusted-peer", first, firstScope);
        assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
        var secondReply = fixture.lane.receive("trusted-peer", second, secondScope);
        assertEquals(1, fixture.queue.snapshot().waiting());
        assertFalse(secondReply.isDone());
        assertTrue(fixture.repository.read(second.invocationId(), secondScope).join().isEmpty());

        InvocationBinding old = firstBinding.get();
        var slot = new PublicationSlot(old.transactionId(), old.invocationId(), 1, 0, 1,
            1, 2, true, ByteKey.copyOf(new byte[] {1}), DIGEST);
        fixture.state.updateAndGet(state -> state.publish(slot).finalizePublication(old.transactionId()));
        firstResult.complete(new OwnerForwarding.Completed(first.invocationId().bytes()));
        assertInstanceOf(OwnerForwarding.Completed.class, firstReply.get(5, TimeUnit.SECONDS));
        assertInstanceOf(OwnerForwarding.Completed.class, secondReply.get(5, TimeUnit.SECONDS));
        assertEquals(1, secondBinding.get().baseSequence());
        assertEquals(2, secondBinding.get().baseRevision());
      } finally {
        firstResult.complete(new OwnerForwarding.Unknown(BackendFailure.Category.UNKNOWN));
        firstScope.close();
        secondScope.close();
      }
    }
  }

  @Test
  void queueWaitConsumesForwardedDeadlineBeforeBinding() throws Exception {
    var clock = new AtomicLong();
    var firstResult = new CompletableFuture<OwnerForwarding.Reply>();
    var started = new CountDownLatch(1);
    var calls = new AtomicInteger();
    try (Fixture fixture = new Fixture((request, binding, identity, scope) -> {
      calls.incrementAndGet();
      started.countDown();
      return firstResult;
    }, clock)) {
      var first = request(LONG_TIMEOUT);
      var second = request(1_000);
      var firstScope = RequestScope.withoutDeadline();
      var secondScope = RequestScope.withoutDeadline();
      try {
        var firstReply = fixture.lane.receive("trusted-peer", first, firstScope);
        assertTrue(started.await(5, TimeUnit.SECONDS));
        var waiting = fixture.lane.receive("trusted-peer", second, secondScope);
        clock.set(1_000);
        firstResult.complete(new OwnerForwarding.Completed(first.invocationId().bytes()));
        assertInstanceOf(OwnerForwarding.Completed.class, firstReply.get(5, TimeUnit.SECONDS));
        var unknown = assertInstanceOf(OwnerForwarding.Unknown.class, waiting.get(5, TimeUnit.SECONDS));
        assertEquals(BackendFailure.Category.TIMEOUT, unknown.category());
        assertEquals(1, calls.get());
        assertTrue(fixture.repository.read(second.invocationId(), secondScope).join().isEmpty());
      } finally {
        firstResult.complete(new OwnerForwarding.Unknown(BackendFailure.Category.UNKNOWN));
        firstScope.close();
        secondScope.close();
      }
    }
  }

  @Test
  void queueWaitReducesTheBudgetPassedToTheReceiver() throws Exception {
    var clock = new AtomicLong();
    var firstResult = new CompletableFuture<OwnerForwarding.Reply>();
    var started = new CountDownLatch(1);
    var observedBudget = new AtomicLong(-1);
    var calls = new AtomicInteger();
    try (Fixture fixture = new Fixture((request, binding, identity, scope) -> {
      if (calls.getAndIncrement() == 0) {
        started.countDown();
        return firstResult;
      }
      observedBudget.set(request.remainingTimeoutNanos());
      return CompletableFuture.completedFuture(new OwnerForwarding.Completed(request.invocationId().bytes()));
    }, clock)) {
      var first = request(LONG_TIMEOUT);
      var second = request(TimeUnit.SECONDS.toNanos(1));
      var firstScope = RequestScope.withoutDeadline();
      var secondScope = RequestScope.withoutDeadline();
      try {
        var firstReply = fixture.lane.receive("trusted-peer", first, firstScope);
        assertTrue(started.await(5, TimeUnit.SECONDS));
        var waiting = fixture.lane.receive("trusted-peer", second, secondScope);
        clock.set(TimeUnit.MILLISECONDS.toNanos(400));
        firstResult.complete(new OwnerForwarding.Completed(first.invocationId().bytes()));
        assertInstanceOf(OwnerForwarding.Completed.class, firstReply.get(5, TimeUnit.SECONDS));
        assertInstanceOf(OwnerForwarding.Completed.class, waiting.get(5, TimeUnit.SECONDS));
        assertEquals(TimeUnit.MILLISECONDS.toNanos(600), observedBudget.get());
      } finally {
        firstResult.complete(new OwnerForwarding.Unknown(BackendFailure.Category.UNKNOWN));
        firstScope.close();
        secondScope.close();
      }
    }
  }

  @Test
  void malformedCompletedReceiptDoesNotBecomeSuccess() throws Exception {
    try (Fixture fixture = new Fixture((request, binding, identity, scope) ->
        CompletableFuture.completedFuture(new OwnerForwarding.Completed(new byte[] {1})))) {
      try (RequestScope scope = RequestScope.withoutDeadline()) {
        var unknown = assertInstanceOf(OwnerForwarding.Unknown.class,
            fixture.lane.receive("trusted-peer", request(LONG_TIMEOUT), scope).get(5, TimeUnit.SECONDS));
        assertEquals(BackendFailure.Category.UNKNOWN, unknown.category());
      }
    }
  }

  private static MutationForwardRequest request(long timeout) {
    ByteKey operation = ByteKey.copyOf(new byte[] {1, 2, 3});
    return new MutationForwardRequest(GROUP, InvocationId.create(),
        MutationForwardRequest.digest(GROUP, operation), DIGEST, CONTEXT, 1, timeout,
        MutationForwardRequest.MAX_HOPS, operation);
  }

  private static final class Fixture implements AutoCloseable {
    private final AtomicReference<CommitState> state = new AtomicReference<>(
        CommitState.initial(DIGEST, 1).claimOwner("owner", 0, Long.MAX_VALUE));
    private final MemoryInvocations repository = new MemoryInvocations();
    private final SerialMutationQueue queue = new SerialMutationQueue(2, 20_000, Duration.ofSeconds(30));
    private final MutationOwnerLane lane;

    private Fixture(MutationForwardReceiver.MutationProcessor processor) {
      this(processor, null);
    }

    private Fixture(MutationForwardReceiver.MutationProcessor processor, AtomicLong clock) {
      CommitControlStore control = new CommitControlStore() {
        @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
          return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state.get(), new byte[] {1})));
        }
        @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState value, RequestScope scope) {
          throw new UnsupportedOperationException();
        }
        @Override public CompletableFuture<Boolean> compareAndSet(
            ControlSnapshot expected, CommitState value, RequestScope scope) {
          throw new UnsupportedOperationException();
        }
      };
      var identity = new AuthorizedBackendIdentity("provider", "fixture", "context",
          new AuthenticationScope(GROUP, "profile"), Set.of(GROUP), 0);
      var receiver = new MutationForwardReceiver(control, new InvocationService(repository),
          (peer, request, barrierGeneration, scope) -> CompletableFuture.completedFuture(
              CommitAuthorizationProofTestSupport.verified(identity, DIGEST, barrierGeneration)), processor);
      lane = clock == null ? new MutationOwnerLane(GROUP, queue, receiver)
          : new MutationOwnerLane(GROUP, queue, receiver, clock::get);
    }

    @Override public void close() throws Exception {
      queue.close();
      queue.drained().get(5, TimeUnit.SECONDS);
      assertTrue(queue.awaitTermination(Duration.ofSeconds(5)));
    }
  }

  private static final class MemoryInvocations implements InvocationRepository {
    private final Map<InvocationId, InvocationBinding> bindings = new ConcurrentHashMap<>();

    @Override public CompletableFuture<Optional<InvocationBinding>> read(InvocationId id, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(bindings.get(id)));
    }

    @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        InvocationBinding value, RequestScope scope) {
      var previous = bindings.putIfAbsent(value.invocationId(), value);
      return CompletableFuture.completedFuture(previous == null
          ? SystemRecordStore.CreateResult.CREATED : SystemRecordStore.CreateResult.ALREADY_PRESENT);
    }

    @Override public CompletableFuture<Boolean> compareAndSet(
        InvocationBinding expected, InvocationBinding replacement, RequestScope scope) {
      return CompletableFuture.completedFuture(bindings.replace(expected.invocationId(), expected, replacement));
    }
  }
}
