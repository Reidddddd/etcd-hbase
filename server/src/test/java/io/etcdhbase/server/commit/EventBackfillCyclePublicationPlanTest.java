package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class EventBackfillCyclePublicationPlanTest {
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);
  private static final ByteKey AUTHORIZATION = ByteKey.copyOf(new byte[32]);
  private static final ByteKey NAMESPACE = ByteKey.copyOf(new byte[16]);
  private static final ByteKey REGISTRY = ByteKey.copyOf(new byte[] {1, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0});

  @Test
  void bindsVerifiedProgressToTheFreshOwnerLaneBase() {
    var owner = new CommitOwnerService.OwnerToken("owner", 7);
    var verifiedProgress = new EventIndexBackfillCheckpoint(7, 5, 2, 4, 3, 4, 5, 6, 1, REGISTRY);
    var latest = state(7, 10, 10, 7, 7, 1);

    var plan = EventBackfillCycle.publicationPlan(owner, verifiedProgress, latest, AUTHORIZATION);

    assertEquals(7, plan.checkpoint().ownerEpoch());
    assertEquals(11, plan.checkpoint().sequence());
    assertEquals(2, plan.checkpoint().firstSequence());
    assertEquals(4, plan.checkpoint().upperSequence());
    assertEquals(3, plan.checkpoint().nextSequence());
    assertEquals(4, plan.checkpoint().firstBaseRevision());
    assertEquals(5, plan.checkpoint().currentRevision());
    assertEquals(6, plan.checkpoint().upperRevision());
    assertEquals(1, plan.checkpoint().compactRevision());
    assertEquals(REGISTRY, plan.checkpoint().registryIdentity());
    assertEquals(10, plan.invocation().baseSequence());
    assertEquals(7, plan.invocation().baseRevision());
    assertEquals(7, plan.invocation().preparedOwnerEpoch());
  }

  @Test
  void rereadsExpandedFinalizedStateBeforeRebindingTheProofProposal() {
    try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(5))) {
      var previous = new EventIndexBackfillCheckpoint(7, 4, 1, 2, 2, 1, 2, 2, 1, REGISTRY);
      var verifiedProgress = previous.advance(7, 5, decision(2, 2));
      var latest = state(7, 10, 10, 2, 2, 1);
      var control = mock(HBaseCommitControlStore.class);
      when(control.read(scope)).thenReturn(CompletableFuture.completedFuture(
          Optional.of(new ControlSnapshot(latest, new byte[] {1}))));
      var stores = mock(TableGroupStores.class);
      when(stores.control()).thenReturn(control);
      var pins = mock(EventHistoryPinManager.class);
      when(pins.namespace()).thenReturn(Optional.of(NAMESPACE));
      when(pins.registryIdentity()).thenReturn(REGISTRY);
      var limits = new BudgetManager.Limits(4, 4, 4, 4, 4, 1_000_000, 1024L << 20);
      var budgets = new BudgetManager(limits, Map.of("group", limits), limits);
      var cycle = new EventBackfillCycle("group", mock(SerialMutationQueue.class), stores, pins,
          mock(InvocationService.class), mock(EventBackfillPreparation.class),
          mock(CommitPublisher.class), budgets, identity("group"), AUTHORIZATION,
          Clock.fixed(Instant.ofEpochMilli(20), ZoneOffset.UTC));
      var owner = new CommitOwnerService.OwnerToken("owner", 7);

      CommitState base = cycle.readPublicationBase(owner, verifiedProgress, previous, false, scope).join();
      var plan = EventBackfillCycle.publicationPlan(owner, verifiedProgress, base, AUTHORIZATION);

      assertEquals(10, base.publishedSequence());
      assertEquals(11, plan.checkpoint().sequence());
      assertEquals(10, plan.invocation().baseSequence());
      assertEquals(2, plan.invocation().baseRevision());
      assertEquals(2, plan.checkpoint().upperSequence());
      assertEquals(3, plan.checkpoint().nextSequence());

      var changedOwner = state(8, 10, 10, 2, 2, 1);
      when(control.read(scope)).thenReturn(CompletableFuture.completedFuture(
          Optional.of(new ControlSnapshot(changedOwner, new byte[] {1}))));
      assertThrows(CompletionException.class,
          () -> cycle.readPublicationBase(owner, verifiedProgress, previous, false, scope).join());
    }
  }

  private static CommitDecision decision(long sequence, long baseRevision) {
    return new CommitDecision(TransactionId.create(), InvocationId.create(), sequence,
        baseRevision, baseRevision,
        ByteKey.copyOf(new byte[] {1}), FINGERPRINT, false);
  }

  private static CommitState state(long ownerEpoch, long publishedSequence, long finalizedSequence,
      long publishedRevision, long finalizedRevision, long compactRevision) {
    return CommitState.restore(CommitState.EVENT_REGISTRY_FORMAT_VERSION, FINGERPRINT,
        Optional.of("owner"), ownerEpoch, 10_000, 1, publishedSequence, publishedRevision,
        finalizedSequence, finalizedRevision, Optional.empty(), compactRevision,
        Optional.of(NAMESPACE), Optional.empty(), 1, true, 0);
  }

  private static AuthorizedBackendIdentity identity(String tableGroup) {
    return new AuthorizedBackendIdentity("dev-no-auth", "fixture", "fixture-context",
        new AuthenticationScope(tableGroup, "primary"), Set.of(tableGroup), 0);
  }
}
