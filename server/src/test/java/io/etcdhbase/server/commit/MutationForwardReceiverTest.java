package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

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
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class MutationForwardReceiverTest {
  private static final String GROUP = "group-a";
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);
  private static final ByteKey AUTH_DIGEST = ByteKey.copyOf(new byte[32]);
  private static final ByteKey MANIFEST_ROOT = ByteKey.copyOf(new byte[] {9});
  private static final InvocationId INVOCATION = new InvocationId(
      UUID.fromString("00000000-0000-0000-0000-000000000001"));
  private static final TransactionId TRANSACTION = new TransactionId(
      UUID.fromString("00000000-0000-0000-0000-000000000002"));

  @Test
  void preparingInvocationIsNotDispatchedWhenBarrierAdvancesBeforeSecondControlRead() {
    CommitState initial = authorizationBarrierState();
    CommitState advanced = advanceAuthorizationBarrier(initial);
    var control = new MutableControlStore(initial);
    var repository = new InMemoryInvocationRepository();
    var processors = new AtomicInteger();
    var fixture = receiver(control, repository,
        (peer, request, barrierGeneration, scope) -> {
          assertEquals(1, control.readCalls.get(), "the initial control read precedes authorization");
          var verified = verifiedAuthorization(barrierGeneration);
          control.advanceTo(advanced);
          return CompletableFuture.completedFuture(verified);
        }, (request, binding, authorization, scope) -> {
          processors.incrementAndGet();
          return CompletableFuture.completedFuture(
              new OwnerForwarding.Rejected(BackendFailure.Category.RESOURCE_LIMIT));
        });

    OwnerForwarding.Reply reply = fixture.receiver().receive(
        "authenticated-peer", requestForOwnerEpoch(initial.ownerEpoch()),
        RequestScope.withoutDeadline()).join();

    var unknown = assertInstanceOf(OwnerForwarding.Unknown.class, reply);
    assertEquals(BackendFailure.Category.PERMISSION, unknown.category());
    assertEquals(2, control.readCalls.get(), "authorization is followed by the owner recheck");
    assertEquals(1, repository.createCalls, "the invocation binding remains durable");
    assertEquals(InvocationBinding.State.PREPARING, repository.binding.state());
    assertEquals(0, processors.get(), "stale authorization must not dispatch new work");
  }

  @Test
  void sealedInvocationsStillResolveDurableOutcomesAfterBarrierAdvances() {
    assertSealedInvocationResolvesAfterBarrierAdvance(
        authorizationBarrierState(),
        new OwnerForwarding.Unknown(BackendFailure.Category.UNAVAILABLE));
    assertSealedInvocationResolvesAfterBarrierAdvance(
        selectedAuthorizationBarrierState(),
        new OwnerForwarding.Completed(request().invocationId().bytes()));
  }

  @Test
  void stalePreparingBindingIsNotDispatchedAfterOwnerTakeover() {
    assertStaleBindingRejected(InvocationBinding.State.PREPARING);
  }

  @Test
  void staleSealedBindingWithoutDurableSelectionIsResolvedWithoutMutation() {
    var repository = sealedBindingRepository();
    var processors = new AtomicInteger();
    var fixture = receiver(currentState(), repository,
        (peer, request, barrierGeneration, scope) -> CompletableFuture.completedFuture(
            verifiedAuthorization(barrierGeneration)),
        (request, binding, identity, scope) -> {
          processors.incrementAndGet();
          return CompletableFuture.completedFuture(
              new OwnerForwarding.Unknown(BackendFailure.Category.UNAVAILABLE));
        });

    OwnerForwarding.Reply reply = fixture.receiver().receive(
        "authenticated-peer", request(), RequestScope.withoutDeadline()).join();

    var unknown = assertInstanceOf(OwnerForwarding.Unknown.class, reply);
    assertEquals(BackendFailure.Category.UNAVAILABLE, unknown.category());
    assertEquals(1, processors.get(), "sealed attempts must reach durable outcome resolution");
  }

  @Test
  void staleSealedBindingAfterSelectionCanReturnItsDurableOutcome() {
    var repository = sealedBindingRepository();
    var processors = new AtomicInteger();
    CommitState current = CommitState.initial(FINGERPRINT, 1)
        .claimOwner("owner-old", 1, 10)
        .publish(publication(1))
        .claimOwner("owner-current", 10, 2_000)
        .finalizePublication(TRANSACTION);
    assertEquals(1, current.publishedSequence());
    assertEquals(1, current.finalizedSequence());
    var fixture = receiver(current, repository,
        (peer, request, barrierGeneration, scope) -> CompletableFuture.completedFuture(
            verifiedAuthorization(barrierGeneration)),
        (request, binding, identity, scope) -> {
          processors.incrementAndGet();
          return CompletableFuture.completedFuture(
              new OwnerForwarding.Completed(request.invocationId().bytes()));
        });

    OwnerForwarding.Reply reply = fixture.receiver().receive(
        "authenticated-peer", request(), RequestScope.withoutDeadline()).join();

    var completed = assertInstanceOf(OwnerForwarding.Completed.class, reply);
    assertArrayEquals(request().invocationId().bytes(), completed.operationResult());
    assertEquals(1, processors.get(), "the selected attempt must be resolved, not re-executed");
  }

  @Test
  void duplicateRequestDuringPublicationDoesNotClaimKnownRejection() {
    var repository = new InMemoryInvocationRepository();
    repository.binding = InvocationBinding.begin(INVOCATION, request().requestDigest(), AUTH_DIGEST,
        TRANSACTION, 2, 0, 1).seal(MANIFEST_ROOT);
    var processors = new AtomicInteger();
    CommitState publishing = CommitState.initial(FINGERPRINT, 1)
        .claimOwner("owner-old", 0, 10)
        .claimOwner("owner-current", 10, 2_000)
        .publish(publication(2));
    assertEquals(2, publishing.ownerEpoch());
    assertFalse(publishing.ownerLeaseExpired(1_000));
    assertTrue(publishing.publication().isPresent());
    var fixture = receiver(publishing, repository, processors);

    OwnerForwarding.Reply reply = fixture.receiver().receive(
        "authenticated-peer", request(), RequestScope.withoutDeadline()).join();

    var unknown = assertInstanceOf(OwnerForwarding.Unknown.class, reply);
    assertEquals(BackendFailure.Category.UNAVAILABLE, unknown.category());
    assertEquals(0, processors.get());
  }

  @Test
  void currentBindingCanReachProcessorAndRejectedMeansNoMutationWasDispatched() {
    var repository = new InMemoryInvocationRepository();
    var processors = new AtomicInteger();
    var fixture = receiver(repository, processors);

    OwnerForwarding.Reply reply = fixture.receiver().receive(
        "authenticated-peer", request(), RequestScope.withoutDeadline()).join();

    var rejected = assertInstanceOf(OwnerForwarding.Rejected.class, reply);
    assertEquals(BackendFailure.Category.RESOURCE_LIMIT, rejected.category());
    assertEquals(1, processors.get(), "a processor may run and prove that it dispatched no mutation");
  }

  @Test
  void authorizationScopeMismatchIsRejectedBeforeProcessorDispatch() {
    var repository = new InMemoryInvocationRepository();
    var processors = new AtomicInteger();
    byte[] differentDigest = new byte[32];
    differentDigest[0] = 1;
    var fixture = receiver(repository, processors, ByteKey.copyOf(differentDigest));

    OwnerForwarding.Reply reply = fixture.receiver().receive(
        "authenticated-peer", request(), RequestScope.withoutDeadline()).join();

    var unknown = assertInstanceOf(OwnerForwarding.Unknown.class, reply);
    assertEquals(BackendFailure.Category.PERMISSION, unknown.category());
    assertEquals(0, processors.get());
  }

  @Test
  void receiverUsesOneScopeCappedByEnvelopeAndOuterDeadline() {
    assertEffectiveBudget(
        RequestScope.withoutDeadline(() -> 0), 1_000_000_000L, 1_000_000_000L);
    assertEffectiveBudget(
        RequestScope.withTimeoutNanos(5_000_000_000L, () -> 0), 1_000_000_000L, 1_000_000_000L);
    assertEffectiveBudget(
        RequestScope.withTimeoutNanos(500_000_000L, () -> 0), 1_000_000_000L, 500_000_000L);
  }

  @Test
  void authorizationWaitConsumesBudgetAndPreventsBindingAndDispatch() {
    AtomicLong now = new AtomicLong();
    var authorizationBudget = new AtomicLong(-1);
    var pendingAuthorization = new CompletableFuture<MutationForwardReceiver.VerifiedAuthorization>();
    var repository = new InMemoryInvocationRepository();
    var processors = new AtomicInteger();
    var fixture = receiver(currentState(), repository,
        (peer, request, barrierGeneration, scope) -> {
          authorizationBudget.set(scope.remainingDeadlineNanos());
          return pendingAuthorization;
        }, (request, binding, identity, scope) -> {
          processors.incrementAndGet();
          return CompletableFuture.completedFuture(
              new OwnerForwarding.Rejected(BackendFailure.Category.RESOURCE_LIMIT));
        });

    CompletableFuture<OwnerForwarding.Reply> reply = fixture.receiver().receive(
        "authenticated-peer", request(1_000), RequestScope.withoutDeadline(now::get));
    now.set(1_000);
    pendingAuthorization.complete(verifiedAuthorization(0));

    var unknown = assertInstanceOf(OwnerForwarding.Unknown.class, reply.join());
    assertEquals(BackendFailure.Category.TIMEOUT, unknown.category());
    assertEquals(0, repository.createCalls);
    assertEquals(0, processors.get());
    assertEquals(1_000, authorizationBudget.get());
  }

  @Test
  void authorizationWaitDoesNotReissueTheEnvelopeBudget() {
    AtomicLong now = new AtomicLong();
    var capturedAuthorizationScope = new AtomicReference<RequestScope>();
    var processorScope = new AtomicReference<RequestScope>();
    var pendingAuthorization = new CompletableFuture<MutationForwardReceiver.VerifiedAuthorization>();
    var repository = new InMemoryInvocationRepository();
    var processors = new AtomicInteger();
    var fixture = receiver(currentState(), repository,
        (peer, request, barrierGeneration, scope) -> {
          capturedAuthorizationScope.set(scope);
          return pendingAuthorization;
        }, (request, binding, identity, scope) -> {
          processorScope.set(scope);
          processors.incrementAndGet();
          return CompletableFuture.completedFuture(
              new OwnerForwarding.Rejected(BackendFailure.Category.RESOURCE_LIMIT));
        });

    CompletableFuture<OwnerForwarding.Reply> reply = fixture.receiver().receive(
        "authenticated-peer", request(1_000), RequestScope.withoutDeadline(now::get));
    now.set(400);
    pendingAuthorization.complete(verifiedAuthorization(0));

    assertInstanceOf(OwnerForwarding.Rejected.class, reply.join());
    assertSame(capturedAuthorizationScope.get(), processorScope.get());
    assertSame(capturedAuthorizationScope.get(), fixture.controlScope().get());
    assertSame(capturedAuthorizationScope.get(), repository.lastScope);
    assertEquals(600, processorScope.get().remainingDeadlineNanos());
    assertEquals(1, repository.createCalls);
    assertEquals(1, processors.get());
  }

  @Test
  void parentCleanupWaitsForForwardedPhysicalProducerAfterReply() {
    var repository = new InMemoryInvocationRepository();
    var physicalWrite = new CompletableFuture<Void>();
    var outerScope = RequestScope.withoutDeadline();
    try {
      var fixture = receiver(currentState(), repository,
          (peer, request, barrierGeneration, scope) -> CompletableFuture.completedFuture(
              verifiedAuthorization(barrierGeneration)),
          (request, binding, identity, scope) -> {
            scope.trackPhysicalProducer(physicalWrite);
            return CompletableFuture.completedFuture(new OwnerForwarding.Completed(new byte[] {1}));
          });
      assertInstanceOf(OwnerForwarding.Completed.class,
          fixture.receiver().receive("authenticated-peer", request(), outerScope).join());
      outerScope.close();
      assertFalse(outerScope.cleanupCompletion().toCompletableFuture().isDone(),
          "parent cleanup must not finish while forwarded physical work is still running");
      physicalWrite.complete(null);
      outerScope.cleanupCompletion().toCompletableFuture().join();
    } finally {
      physicalWrite.complete(null);
      outerScope.close();
    }
  }

  private static void assertStaleBindingRejected(InvocationBinding.State state) {
    var repository = new InMemoryInvocationRepository();
    var stale = InvocationBinding.begin(INVOCATION, request().requestDigest(), AUTH_DIGEST,
        TRANSACTION, 1, 0, 1);
    if (state == InvocationBinding.State.SEALED) {
      stale = stale.seal(ByteKey.copyOf(new byte[] {9}));
    }
    repository.binding = stale;
    var processors = new AtomicInteger();
    var fixture = receiver(repository, processors);

    OwnerForwarding.Reply reply = fixture.receiver().receive(
        "authenticated-peer", request(), RequestScope.withoutDeadline()).join();

    var unknown = assertInstanceOf(OwnerForwarding.Unknown.class, reply);
    assertEquals(BackendFailure.Category.UNAVAILABLE, unknown.category());
    assertEquals(0, processors.get());
  }

  private static InMemoryInvocationRepository sealedBindingRepository() {
    var repository = new InMemoryInvocationRepository();
    repository.binding = InvocationBinding.begin(INVOCATION, request().requestDigest(), AUTH_DIGEST,
        TRANSACTION, 1, 0, 1).seal(MANIFEST_ROOT);
    return repository;
  }

  private static void assertSealedInvocationResolvesAfterBarrierAdvance(
      CommitState initial, OwnerForwarding.Reply durableReply) {
    CommitState advanced = advanceAuthorizationBarrier(initial);
    var control = new MutableControlStore(initial);
    var repository = sealedBindingRepository();
    var processors = new AtomicInteger();
    var fixture = receiver(control, repository,
        (peer, request, barrierGeneration, scope) -> {
          assertEquals(1, control.readCalls.get(), "authorization follows the initial control read");
          var verified = verifiedAuthorization(barrierGeneration);
          control.advanceTo(advanced);
          return CompletableFuture.completedFuture(verified);
        }, (request, binding, authorization, scope) -> {
          processors.incrementAndGet();
          return CompletableFuture.completedFuture(durableReply);
        });

    OwnerForwarding.Reply reply = fixture.receiver().receive(
        "authenticated-peer", requestForOwnerEpoch(initial.ownerEpoch()),
        RequestScope.withoutDeadline()).join();

    assertSame(durableReply, reply,
        "sealed invocations must keep resolving their durable outcome after revocation");
    assertEquals(2, control.readCalls.get());
    assertEquals(1, processors.get());
  }

  private static CommitState authorizationBarrierState() {
    return CommitState.initial(FINGERPRINT, 1)
        .claimOwner("owner-current", 0, 2_000)
        .enableAuthorizationBarrier("owner-current", 1, 1_000);
  }

  private static CommitState selectedAuthorizationBarrierState() {
    return CommitState.initial(FINGERPRINT, 1)
        .claimOwner("owner-old", 0, 10)
        .publish(publication(1))
        .finalizePublication(TRANSACTION)
        .claimOwner("owner-current", 10, 2_000)
        .enableAuthorizationBarrier("owner-current", 2, 1_000);
  }

  private static CommitState advanceAuthorizationBarrier(CommitState state) {
    return state.advanceAuthorizationBarrier(state.ownerInstance().orElseThrow(),
        state.ownerEpoch(), state.authorizationBarrierGeneration());
  }

  private static Fixture receiver(InMemoryInvocationRepository repository, AtomicInteger processors) {
    return receiver(repository, processors, AUTH_DIGEST);
  }

  private static Fixture receiver(InMemoryInvocationRepository repository,
      AtomicInteger processors, ByteKey resolvedAuthorizationDigest) {
    return receiver(currentState(), repository, processors, resolvedAuthorizationDigest);
  }

  private static Fixture receiver(CommitState current, InMemoryInvocationRepository repository,
      AtomicInteger processors) {
    return receiver(current, repository, processors, AUTH_DIGEST);
  }

  private static Fixture receiver(CommitState current, InMemoryInvocationRepository repository,
      AtomicInteger processors, ByteKey resolvedAuthorizationDigest) {
    return receiver(current, repository,
        (peer, request, barrierGeneration, scope) -> CompletableFuture.completedFuture(
            CommitAuthorizationProofTestSupport.verified(
                identity(), resolvedAuthorizationDigest, barrierGeneration)),
        (request, binding, verified, scope) -> {
          processors.incrementAndGet();
          return CompletableFuture.completedFuture(
              new OwnerForwarding.Rejected(BackendFailure.Category.RESOURCE_LIMIT));
        });
  }

  private static Fixture receiver(CommitState current, InMemoryInvocationRepository repository,
      MutationForwardReceiver.AuthorizationResolver authorizations,
      MutationForwardReceiver.MutationProcessor processor) {
    var controlScope = new AtomicReference<RequestScope>();
    CommitControlStore control = new FixedControlStore(current, controlScope);
    var invocations = new InvocationService(repository);
    var receiver = new MutationForwardReceiver(control, invocations, authorizations, processor,
        Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
    return new Fixture(receiver, controlScope);
  }

  private static Fixture receiver(CommitControlStore control,
      InMemoryInvocationRepository repository,
      MutationForwardReceiver.AuthorizationResolver authorizations,
      MutationForwardReceiver.MutationProcessor processor) {
    var receiver = new MutationForwardReceiver(control, new InvocationService(repository),
        authorizations, processor, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
    return new Fixture(receiver, new AtomicReference<>());
  }

  private static void assertEffectiveBudget(
      RequestScope outerScope, long requestNanos, long expectedNanos) {
    AtomicReference<RequestScope> authorizationScope = new AtomicReference<>();
    AtomicReference<RequestScope> processorScope = new AtomicReference<>();
    var repository = new InMemoryInvocationRepository();
    var processors = new AtomicInteger();
    var fixture = receiver(currentState(), repository,
        (peer, request, barrierGeneration, scope) -> {
          authorizationScope.set(scope);
          return CompletableFuture.completedFuture(verifiedAuthorization(barrierGeneration));
        }, (request, binding, identity, scope) -> {
          processorScope.set(scope);
          processors.incrementAndGet();
          return CompletableFuture.completedFuture(
              new OwnerForwarding.Rejected(BackendFailure.Category.RESOURCE_LIMIT));
        });

    OwnerForwarding.Reply reply = fixture.receiver().receive(
        "authenticated-peer", request(requestNanos), outerScope).join();

    assertInstanceOf(OwnerForwarding.Rejected.class, reply);
    assertSame(authorizationScope.get(), processorScope.get());
    assertSame(authorizationScope.get(), fixture.controlScope().get());
    assertSame(authorizationScope.get(), repository.lastScope);
    assertEquals(expectedNanos, processorScope.get().remainingDeadlineNanos());
    assertEquals(1, processors.get());
  }

  private static MutationForwardReceiver.VerifiedAuthorization verifiedAuthorization(
      long barrierGeneration) {
    return CommitAuthorizationProofTestSupport.verified(identity(), AUTH_DIGEST, barrierGeneration);
  }

  private static AuthorizedBackendIdentity identity() {
    return new AuthorizedBackendIdentity("test-provider", "test-subject", "ctx-1",
        new AuthenticationScope(GROUP, "profile-a"), Set.of(GROUP), 1);
  }

  private static CommitState currentState() {
    return CommitState.initial(FINGERPRINT, 1)
        .claimOwner("owner-old", 1, 10)
        .claimOwner("owner-current", 10, 2_000);
  }

  private static PublicationSlot publication(long ownerEpoch) {
    return new PublicationSlot(TRANSACTION, INVOCATION, ownerEpoch, 0, 1,
        1, 2, true, MANIFEST_ROOT, FINGERPRINT);
  }

  private static MutationForwardRequest request() {
    return request(MutationForwardRequest.MAX_TIMEOUT_NANOS);
  }

  private static MutationForwardRequest request(long remainingTimeoutNanos) {
    return request(remainingTimeoutNanos, 2);
  }

  private static MutationForwardRequest requestForOwnerEpoch(long ownerEpoch) {
    return request(MutationForwardRequest.MAX_TIMEOUT_NANOS, ownerEpoch);
  }

  private static MutationForwardRequest request(long remainingTimeoutNanos, long ownerEpoch) {
    ByteKey operation = ByteKey.copyOf(new byte[] {1, 2, 3});
    return new MutationForwardRequest(GROUP, INVOCATION,
        MutationForwardRequest.digest(GROUP, operation), AUTH_DIGEST,
        ByteKey.copyOf("ctx-1".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
        ownerEpoch, remainingTimeoutNanos, MutationForwardRequest.MAX_HOPS, operation);
  }

  private record Fixture(
      MutationForwardReceiver receiver, AtomicReference<RequestScope> controlScope) {}

  private static final class FixedControlStore implements CommitControlStore {
    private final ControlSnapshot snapshot;
    private final AtomicReference<RequestScope> lastScope;

    private FixedControlStore(CommitState state, AtomicReference<RequestScope> lastScope) {
      snapshot = new ControlSnapshot(state, new byte[] {1});
      this.lastScope = lastScope;
    }

    @Override
    public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      lastScope.set(scope);
      return CompletableFuture.completedFuture(Optional.of(snapshot));
    }

    @Override
    public CompletableFuture<Boolean> initializeIfAbsent(CommitState state, RequestScope scope) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class MutableControlStore implements CommitControlStore {
    private final AtomicReference<CommitState> current;
    private final AtomicInteger readCalls = new AtomicInteger();

    private MutableControlStore(CommitState initial) {
      current = new AtomicReference<>(initial);
    }

    private void advanceTo(CommitState state) {
      current.set(state);
    }

    @Override
    public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      readCalls.incrementAndGet();
      return CompletableFuture.completedFuture(
          Optional.of(new ControlSnapshot(current.get(), new byte[] {1})));
    }

    @Override
    public CompletableFuture<Boolean> initializeIfAbsent(CommitState state, RequestScope scope) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class InMemoryInvocationRepository implements InvocationRepository {
    private InvocationBinding binding;
    private int createCalls;
    private RequestScope lastScope;

    @Override
    public CompletableFuture<Optional<InvocationBinding>> read(
        InvocationId invocationId, RequestScope scope) {
      lastScope = scope;
      return CompletableFuture.completedFuture(Optional.ofNullable(binding));
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        InvocationBinding proposed, RequestScope scope) {
      lastScope = scope;
      createCalls++;
      if (binding == null) {
        binding = proposed;
        return CompletableFuture.completedFuture(SystemRecordStore.CreateResult.CREATED);
      }
      return CompletableFuture.completedFuture(SystemRecordStore.CreateResult.ALREADY_PRESENT);
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(
        InvocationBinding expected, InvocationBinding replacement, RequestScope scope) {
      lastScope = scope;
      if (binding != expected) return CompletableFuture.completedFuture(false);
      binding = replacement;
      return CompletableFuture.completedFuture(true);
    }
  }
}
