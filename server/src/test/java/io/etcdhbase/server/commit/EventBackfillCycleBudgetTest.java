package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.EventHistoryPinManager;
import io.etcdhbase.core.commit.EventIndexBackfillCheckpoint;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ReadView;
import io.etcdhbase.core.commit.ReadViewPinRegistry;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.commit.SystemRecordEntry;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.hbase.storage.HBaseCommitDecisionRepository;
import io.etcdhbase.hbase.storage.HBaseManifestRepository;
import io.etcdhbase.hbase.storage.HBaseSystemRecordStore;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class EventBackfillCycleBudgetTest {
  private static final CommitOwnerService.OwnerToken OWNER =
      new CommitOwnerService.OwnerToken("owner", 7);
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);
  private static final ByteKey AUTHORIZATION = ByteKey.copyOf(new byte[32]);
  private static final ByteKey NAMESPACE = ByteKey.copyOf(new byte[16]);
  private static final ByteKey REGISTRY = ByteKey.copyOf(new byte[] {1, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0});

  @Test
  void cycleFailsClosedWhenItsReservedAccountCannotBeAdmitted() {
    var limits = new BudgetManager.Limits(16, 16, 32, 16, 32, 1L << 20, 256L << 20);
    var reserve = new BudgetManager.ReservedCapacity(1, 0, 1, 0, 1, 0, 2);
    var budgets = new BudgetManager(limits, Map.of("group", limits), limits,
        reserve, Map.of("group", reserve));
    var stores = mock(TableGroupStores.class);
    when(stores.control()).thenReturn(mock(HBaseCommitControlStore.class));
    var pins = mock(EventHistoryPinManager.class);
    when(pins.namespace()).thenReturn(Optional.of(ByteKey.copyOf(new byte[16])));
    var cycle = new EventBackfillCycle("group", mock(SerialMutationQueue.class), stores, pins,
        mock(InvocationService.class),
        mock(EventBackfillPreparation.class), mock(CommitPublisher.class), budgets,
        identity("group"), ByteKey.copyOf(new byte[32]), Clock.systemUTC());
    try (var scope = RequestScope.withTimeout(Duration.ofSeconds(10))) {
      var failure = assertThrows(CompletionException.class,
          () -> cycle.run(new CommitOwnerService.OwnerToken("owner", 1), scope).join());
      assertEquals(BackendFailure.Category.RESOURCE_LIMIT,
          assertInstanceOf(BackendFailure.class, failure.getCause()).category());
    }
  }

  @Test
  void successfulCheckpointReadReportsPinReleaseFailure() throws Exception {
    var fixture = new AsyncReadFixture();
    var releaseFailure = new BackendFailure(BackendFailure.Category.UNAVAILABLE);
    try (var scope = RequestScope.withoutDeadline()) {
      var result = fixture.cycle.run(OWNER, scope);
      boolean started = fixture.started.await(5, TimeUnit.SECONDS);
      if (!started && result.isCompletedExceptionally()) result.join();
      assertTrue(started);
      fixture.request.complete(Optional.empty());
      fixture.physical.complete(null);
      assertTrue(fixture.pinClosed.await(5, TimeUnit.SECONDS));
      fixture.released.completeExceptionally(releaseFailure);

      var observed = assertThrows(CompletionException.class, result::join);
      assertSame(releaseFailure, CompletionFailures.unwrapCompletion(observed));
    } finally {
      fixture.physical.complete(null);
      fixture.released.complete(null);
    }
  }

  @Test
  void checkpointReadFailureRemainsPrimaryWhenPinReleaseAlsoFails() throws Exception {
    var fixture = new AsyncReadFixture();
    var readFailure = new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
    var releaseFailure = new BackendFailure(BackendFailure.Category.UNAVAILABLE);
    try (var scope = RequestScope.withoutDeadline()) {
      var result = fixture.cycle.run(OWNER, scope);
      boolean started = fixture.started.await(5, TimeUnit.SECONDS);
      if (!started && result.isCompletedExceptionally()) result.join();
      assertTrue(started);
      fixture.request.completeExceptionally(readFailure);
      fixture.physical.complete(null);
      assertTrue(fixture.pinClosed.await(5, TimeUnit.SECONDS));
      fixture.released.completeExceptionally(releaseFailure);

      var observed = assertThrows(CompletionException.class, result::join);
      Throwable primary = CompletionFailures.unwrapCompletionPreservingSuppressed(observed);
      assertSame(readFailure, primary);
      assertEquals(1, primary.getSuppressed().length);
      assertSame(releaseFailure,
          CompletionFailures.unwrapCompletionPreservingSuppressed(primary.getSuppressed()[0]));
    } finally {
      fixture.physical.complete(null);
      fixture.released.complete(null);
    }
  }

  private static final class AsyncReadFixture {
    private final CompletableFuture<Optional<SystemRecordEntry>> request = new CompletableFuture<>();
    private final CompletableFuture<Void> physical = new CompletableFuture<>();
    private final CompletableFuture<Void> released = new CompletableFuture<>();
    private final CountDownLatch started = new CountDownLatch(1);
    private final CountDownLatch pinClosed = new CountDownLatch(1);
    private final EventBackfillCycle cycle;

    private AsyncReadFixture() {
      var control = mock(HBaseCommitControlStore.class);
      when(control.read(any(RequestScope.class))).thenReturn(
          CompletableFuture.completedFuture(Optional.of(snapshot(state()))));
      var pin = mock(ReadViewPinRegistry.Pin.class);
      when(pin.generation()).thenReturn(1L);
      when(pin.renew(any(RequestScope.class))).thenReturn(CompletableFuture.completedFuture(null));
      when(pin.releaseCompletion()).thenReturn(released);
      doAnswer(invocation -> {
        pinClosed.countDown();
        return null;
      }).when(pin).close();
      var viewPins = mock(ReadViewPinRegistry.class);
      when(viewPins.registerAuxiliary(any(ReadView.class), any(RequestScope.class)))
          .thenReturn(CompletableFuture.completedFuture(pin));
      var manifests = mock(ManifestCatalog.class);
      var records = mock(HBaseSystemRecordStore.class);
      when(records.latestAtOrBefore(eq(SystemRecordType.EVENT_INDEX_BACKFILL_CHECKPOINT),
          eq(EventIndexBackfillCheckpoint.routePrefix()), eq(40L), any(RequestScope.class)))
          .thenAnswer(invocation -> {
            RequestScope readScope = invocation.getArgument(3);
            readScope.trackPhysicalProducer(physical);
            started.countDown();
            return request;
          });
      var stores = mock(TableGroupStores.class);
      when(stores.control()).thenReturn(control);
      when(stores.views()).thenReturn(new ReadViewService(control, manifests, viewPins, FINGERPRINT));
      when(stores.records()).thenReturn(records);
      when(stores.manifests()).thenReturn(manifests);
      when(stores.decisions()).thenReturn(mock(HBaseCommitDecisionRepository.class));
      when(stores.manifestRepository()).thenReturn(mock(HBaseManifestRepository.class));
      var eventPins = mock(EventHistoryPinManager.class);
      when(eventPins.namespace()).thenReturn(Optional.of(NAMESPACE));
      when(eventPins.registryIdentity()).thenReturn(REGISTRY);
      var limits = new BudgetManager.Limits(8, 8, 8, 8, 8, 64L << 20, 1L << 30);
      var reserved = new BudgetManager.ReservedCapacity(4, 4, 4, 4, 4, 1L << 20, 512L << 20);
      var budgets = new BudgetManager(limits, Map.of("group", limits), limits,
          reserved, Map.of("group", reserved));
      cycle = new EventBackfillCycle("group", mock(SerialMutationQueue.class), stores, eventPins,
          mock(InvocationService.class), mock(EventBackfillPreparation.class), mock(CommitPublisher.class),
          budgets, identity("group"), AUTHORIZATION, Clock.systemUTC());
    }

    private static ControlSnapshot snapshot(CommitState state) {
      return new ControlSnapshot(state, new byte[] {1});
    }

    private static CommitState state() {
      return CommitState.restore(CommitState.EVENT_REGISTRY_FORMAT_VERSION, FINGERPRINT,
          Optional.of("owner"), OWNER.epoch(), 10_000, 1, 40, 30, 40, 30,
          Optional.empty(), 1, Optional.of(NAMESPACE), Optional.empty(), 1, true, 0);
    }
  }

  private static AuthorizedBackendIdentity identity(String tableGroup) {
    return new AuthorizedBackendIdentity("dev-no-auth", "fixture", "fixture-context",
        new AuthenticationScope(tableGroup, "primary"), Set.of(tableGroup), 0);
  }
}
