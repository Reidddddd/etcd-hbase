package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.*;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class CompactOwnerLaneTest {
  private static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);
  private static final ByteKey REFERENCE = ByteKey.copyOf(new byte[] {1});
  private static final CommitOwnerService.OwnerToken OWNER = new CommitOwnerService.OwnerToken("owner", 7);

  @Test
  void compactUsesItsOwnOperationReceiptAndNeverCreatesAnInvocationBinding() throws Exception {
    try (var fixture = new Fixture(); var scope = RequestScope.withoutDeadline()) {
      var request = request("group", 7, 1, REFERENCE);
      var result = assertInstanceOf(OwnerForwarding.Completed.class,
          fixture.lane.receive("trusted", request, scope).get(5, TimeUnit.SECONDS));
      var operation = CompactionOperation.decode(ByteKey.copyOf(result.operationResult()));
      assertEquals(request.invocationId(), operation.id());
      assertEquals(request.requestDigest(), operation.requestDigest());
      verify(fixture.identities).verifyAndResolve(eq("trusted"), same(request), eq(11L), any());
      verify(fixture.compactions).compact(eq(5L), argThat(binding ->
          binding.invocationId().equals(request.invocationId())), eq(OWNER), same(fixture.proof), any());
    }
  }

  @Test
  void peerContextGroupHopAndEpochAreCheckedBeforeSelection() throws Exception {
    for (int invalid = 0; invalid < 5; invalid++) {
      try (var fixture = new Fixture(); var scope = RequestScope.withoutDeadline()) {
        var request = request(invalid == 0 ? "other" : "group", invalid == 1 ? 6 : 7,
            invalid == 2 ? 0 : 1, invalid == 3 ? ByteKey.copyOf(new byte[] {2}) : REFERENCE);
        assertInstanceOf(OwnerForwarding.Unknown.class, fixture.lane.receive(
            invalid == 4 ? "intruder" : "trusted", request, scope).get(5, TimeUnit.SECONDS));
        verifyNoInteractions(fixture.compactions);
      }
    }
  }

  @Test
  void queuedCompactRechecksOwnerAndCancellationCannotDispatchIt() throws Exception {
    for (boolean cancel : new boolean[] {false, true}) {
      try (var fixture = new Fixture(); var scope = RequestScope.withoutDeadline();
          var blockerScope = RequestScope.withoutDeadline()) {
        var entered = new CompletableFuture<Void>();
        var gate = new CompletableFuture<Void>();
        fixture.queue.submit(1, blockerScope, serviceScope -> { entered.complete(null); return gate; });
        entered.get(5, TimeUnit.SECONDS);
        var queued = fixture.lane.receive("trusted", request("group", 7, 1, REFERENCE), scope);
        if (cancel) scope.cancel();
        else fixture.owner.set(Optional.of(new CommitOwnerService.OwnerToken("successor", 8)));
        gate.complete(null);
        assertInstanceOf(OwnerForwarding.Unknown.class, queued.get(5, TimeUnit.SECONDS));
        verifyNoInteractions(fixture.compactions, fixture.identities);
      }
    }
  }

  private static MutationForwardRequest request(String group, long epoch, int hops, ByteKey reference) {
    var bytes = MutationOperationEnvelope.compact(ByteBuffer.allocate(9).putLong(5).put((byte) 1).array());
    return new MutationForwardRequest(group, InvocationId.create(), MutationForwardRequest.digest(group, bytes),
        DIGEST, reference, epoch, TimeUnit.SECONDS.toNanos(5), hops, bytes);
  }

  private static final class Fixture implements AutoCloseable {
    final SerialMutationQueue queue = new SerialMutationQueue(8, 64L << 10, Duration.ofSeconds(5));
    final CommitControlStore control = mock(CommitControlStore.class);
    final CompactionService compactions = mock(CompactionService.class);
    final MutationForwardReceiver.AuthorizationResolver identities = mock(MutationForwardReceiver.AuthorizationResolver.class);
    final CompactRequestAuthorization management = mock(CompactRequestAuthorization.class);
    final AtomicReference<Optional<CommitOwnerService.OwnerToken>> owner = new AtomicReference<>(Optional.of(OWNER));
    final CommitAuthorizationProof proof = CommitAuthorizationProof.capture(DIGEST, 11,
        (identity, generation, scope) -> CompletableFuture.completedFuture(null));
    final CompactOwnerLane lane = new CompactOwnerLane("group", queue, control, owner::get,
        compactions, identities, management);

    Fixture() {
      var state = mock(CommitState.class);
      when(state.ownerInstance()).thenReturn(Optional.of("owner"));
      when(state.ownerEpoch()).thenReturn(7L);
      when(state.publishedRevision()).thenReturn(9L);
      when(state.publishedSequence()).thenReturn(8L);
      when(state.authorizationBarrierGeneration()).thenReturn(11L);
      when(state.publication()).thenReturn(Optional.empty());
      when(control.read(any())).thenReturn(CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1}))));
      var identity = new AuthorizedBackendIdentity("provider", "manager", "context",
          new AuthenticationScope("group", "profile"), Set.of("group"), 1);
      var verified = new MutationForwardReceiver.VerifiedAuthorization(identity, DIGEST, proof);
      when(identities.verifyAndResolve(any(), any(), anyLong(), any())).thenAnswer(call -> {
        MutationForwardRequest request = call.getArgument(1);
        return "trusted".equals(call.getArgument(0)) && request.authorizationContextReference().equals(REFERENCE)
            ? CompletableFuture.completedFuture(verified)
            : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
      });
      when(management.captureProof(verified)).thenReturn(proof);
      when(compactions.compact(anyLong(), any(), any(), any(), any())).thenAnswer(call -> {
        InvocationBinding binding = call.getArgument(1);
        return CompletableFuture.completedFuture(new CompactionOperation(binding.invocationId(),
            binding.requestDigest(), DIGEST, DIGEST, 5, 9, 7, 8, 10, DIGEST, CompactionOperation.Stage.LOGICAL));
      });
    }

    @Override public void close() throws Exception {
      queue.close();
      queue.drained().get(5, TimeUnit.SECONDS);
      assertTrue(queue.awaitTermination(Duration.ofSeconds(5)));
    }
  }
}
