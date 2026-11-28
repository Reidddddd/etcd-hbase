package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import etcdserverpb.Rpc;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.*;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

class CompactForwarderTest {
  private static final ByteKey PROFILE = ByteKey.copyOf(new byte[32]);
  private static final ByteKey AUTH = ByteKey.copyOf(new byte[32]);

  @Test
  void boundedCommandPreservesNegativeZeroAndPhysicalWithoutProtoUnknownFields() {
    for (long revision : new long[] {Long.MIN_VALUE, -1, 0, 5, Long.MAX_VALUE}) {
      for (byte physical : new byte[] {0, 1}) {
        var payload = ByteBuffer.allocate(9).putLong(revision).put(physical).array();
        var encoded = MutationOperationEnvelope.compact(payload);
        assertEquals(13, encoded.length());
        assertEquals(MutationOperationEnvelope.Kind.COMPACT, MutationOperationEnvelope.kind(encoded));
        assertArrayEquals(payload, MutationOperationEnvelope.compactBytes(encoded));
      }
    }
    for (var bytes : new byte[][] {new byte[0], new byte[8], new byte[10],
        ByteBuffer.allocate(9).putLong(5).put((byte) 2).array()}) {
      assertThrows(IllegalArgumentException.class, () -> MutationOperationEnvelope.compact(bytes));
    }
    assertThrows(IllegalArgumentException.class, () -> MutationOperationEnvelope.kind(
        ByteKey.copyOf(new byte[] {0, 'C', 'P', 1, 0})));
  }

  @Test
  void returnedOperationMustMatchInvocationRequestAuthorizationProfileRevisionAndTerminalStage() throws Exception {
    for (int changed = 0; changed < 7; changed++) {
      var fixture = new Fixture();
      int field = changed;
      fixture.reply = original -> new CompactionOperation(field == 0 ? InvocationId.create() : original.id(),
          field == 1 ? other() : original.requestDigest(), field == 2 ? other() : AUTH,
          field == 3 ? other() : PROFILE, field == 4 ? 4 : 5, 9, 7, 8, 10, PROFILE,
          field == 5 ? CompactionOperation.Stage.PREPARED : CompactionOperation.Stage.LOGICAL);
      try (var scope = RequestScope.withoutDeadline()) {
        var result = fixture.forwarder().forward(request(), Optional.empty(), scope);
        if (field == 6) assertEquals(5, result.join().revision());
        else assertInstanceOf(BackendFailure.class,
            assertThrows(CompletionException.class, result::join).getCause());
        verify(fixture.relay).forward(any(), argThat(child -> child != scope));
      }
    }
  }

  @Test
  void unknownSendOutcomeNeverAllocatesAnotherAttempt() {
    var fixture = new Fixture();
    doReturn(CompletableFuture.completedFuture(
        new MutationRelay.ResultUnknown(BackendFailure.Category.UNAVAILABLE)))
        .when(fixture.relay).forward(any(), any());
    try (var scope = RequestScope.withoutDeadline()) {
      var failure = assertThrows(CompletionException.class,
          () -> fixture.forwarder().forward(request(), Optional.empty(), scope).join());
      assertEquals(BackendFailure.Category.UNAVAILABLE, assertInstanceOf(BackendFailure.class, failure.getCause()).category());
      verify(fixture.relay).forward(any(), argThat(child -> child != scope));
    }
  }

  @Test
  void logicalReceiptWaitsForPhysicalDrainAndCannotHideCleanupFailure() {
    for (boolean failCleanup : new boolean[] {false, true}) {
      var fixture = new Fixture();
      var producer = new CompletableFuture<Void>();
      var cleanup = new java.util.concurrent.atomic.AtomicBoolean();
      var failure = new IllegalStateException("forwarded identity cleanup failed");
      doAnswer(call -> {
        RequestScope child = call.getArgument(1);
        child.trackPhysicalProducer(producer);
        child.registerCleanup(() -> {
          cleanup.set(true);
          if (failCleanup) throw failure;
        });
        return CompletableFuture.completedFuture(fixture.resolvedIdentity);
      }).when(fixture.authorizations).resolve(any(), any());
      try (var scope = RequestScope.withoutDeadline()) {
        var result = fixture.forwarder().forward(request(), Optional.empty(), scope);
        assertTrue(cleanup.get());
        assertFalse(result.isDone());
        producer.complete(null);
        if (failCleanup) assertSame(failure, assertThrows(CompletionException.class, result::join).getCause());
        else assertEquals(CompactionOperation.Stage.LOGICAL, result.join().stage());
      }
    }
  }

  private static Rpc.CompactionRequest request() {
    return Rpc.CompactionRequest.newBuilder().setRevision(5).setPhysical(true).build();
  }

  private static ByteKey other() {
    var value = new byte[32];
    value[0] = 1;
    return ByteKey.copyOf(value);
  }

  private static final class Fixture {
    final CompactRequestAuthorization authorizations = mock(CompactRequestAuthorization.class);
    final CommitControlStore control = mock(CommitControlStore.class);
    final MutationRelay relay = mock(MutationRelay.class);
    final AuthenticatedRequestIdentityResolver.ResolvedRequestIdentity resolvedIdentity;
    UnaryOperator<CompactionOperation> reply = UnaryOperator.identity();

    Fixture() {
      var identity = new AuthorizedBackendIdentity("provider", "manager", "context",
          new AuthenticationScope("group", "profile"), Set.of("group"), 1);
      resolvedIdentity = new AuthenticatedRequestIdentityResolver.ResolvedRequestIdentity(
          identity, AUTH, ByteKey.copyOf(new byte[] {1}), null);
      when(authorizations.resolve(any(), any())).thenReturn(CompletableFuture.completedFuture(resolvedIdentity));
      var state = mock(CommitState.class);
      when(state.ownerInstance()).thenReturn(Optional.of("owner"));
      when(state.ownerEpoch()).thenReturn(7L);
      when(control.read(any())).thenReturn(CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, new byte[] {1}))));
      when(relay.forward(any(), any())).thenAnswer(call -> {
        MutationForwardRequest request = call.getArgument(0);
        assertEquals(0, request.hopCount());
        assertEquals(13, request.operationBytes().length());
        var operation = new CompactionOperation(request.invocationId(), request.requestDigest(), AUTH,
            PROFILE, 5, 9, 7, 8, 10, PROFILE, CompactionOperation.Stage.LOGICAL);
        return CompletableFuture.completedFuture(new MutationRelay.KnownResult(reply.apply(operation).encode().bytes()));
      });
    }

    CompactForwarder forwarder() {
      var limits = new BudgetManager.Limits(8, 8, 8, 8, 8, 8L << 20, 64L << 20);
      return new CompactForwarder("group", PROFILE, authorizations, control, relay,
          new BudgetManager(limits, Map.of("group", limits), limits));
    }
  }
}
