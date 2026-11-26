package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.auth.AuthConfigurationUpdateExecutor;
import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.core.auth.AuthDeploymentRegistryAdmin;
import io.etcdhbase.core.auth.AuthDeploymentRegistryStore;
import io.etcdhbase.core.auth.TargetConfigurationBarriers;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;

class CommitPublisherTest {
  private static final String OWNER = "publisher-owner";
  private static final ByteKey FINGERPRINT = ByteKey.copyOf(new byte[32]);
  private static final ByteKey DIGEST = ByteKey.copyOf(new byte[32]);

  @Test
  void configurationUpdateFencesAnAlreadyAuthorizedSelection() {
    Fixture fixture = new Fixture(true, ScriptedControlStore::write);
    var oldGroup = new AuthDeploymentRegistry.Group("a", "provider", "realm", "profile",
        DIGEST, FINGERPRINT, true);
    var newGroup = new AuthDeploymentRegistry.Group("b", "provider", "realm", "profile",
        DIGEST, FINGERPRINT, true);
    var registry = new AtomicReference<>(AuthDeploymentRegistry.initial("trust", List.of(oldGroup)));
    AuthDeploymentRegistryStore store = new AuthDeploymentRegistryStore() {
      @Override public CompletionStage<Optional<AuthDeploymentRegistry>> readRegistry(RequestScope scope) {
        return CompletableFuture.completedFuture(Optional.of(registry.get()));
      }
      @Override public CompletionStage<Boolean> createRegistryIfAbsent(AuthDeploymentRegistry initial,
          RequestScope scope) { throw new UnsupportedOperationException(); }
      @Override public CompletionStage<Boolean> compareAndSetRegistry(AuthDeploymentRegistry observed,
          AuthDeploymentRegistry next, RequestScope scope) {
        if (!observed.permitsReplacement(next)) throw new IllegalArgumentException();
        return CompletableFuture.completedFuture(registry.compareAndSet(observed, next));
      }
    };
    var gate = new CompletableFuture<Void>();
    var proof = CommitAuthorizationProof.capture(DIGEST, 0, (identity, barrier, scope) -> {
      var state = registry.get();
      if (state.configurationUpdate() != null || state.configurationGeneration() != 1) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
      }
      return gate;
    });
    try (var scope = RequestScope.withoutDeadline()) {
      var publication = fixture.publisher.publish(fixture.invocation, fixture.owner, fixture.manifest, scope, proof);
      assertTrue(new AuthDeploymentRegistryAdmin(store).reserveUpdate(1, List.of(newGroup), scope)
          .toCompletableFuture().join());
      var installer = new AuthorizationBarrierInstaller(fixture.control, fixedClock());
      var barriers = new TargetConfigurationBarriers(Map.of("a",
          new TargetConfigurationBarriers.Group(DIGEST, installer)));
      var result = new AuthConfigurationUpdateExecutor(store, barriers).resume(2, Set.of("a"), scope)
          .toCompletableFuture().join();
      assertEquals(2, result.configurationGeneration());
      gate.complete(null);
      assertThrows(CompletionException.class, publication::join);
      assertEquals(1, fixture.control.current.get().authorizationBarrierGeneration());
      assertTrue(fixture.control.current.get().publication().isEmpty());
    }
  }

  @Test
  void failedPreflightDoesNotTransferWorkOrAttemptCas() {
    Fixture fixture = new Fixture(false, ScriptedControlStore::write);

    assertThrows(CompletionException.class, () -> fixture.publish().join());

    assertEquals(0, fixture.handoff.calls.get());
    assertEquals(0, fixture.control.casCalls.get());
  }

  @Test
  void missingAuthorizationProofIsRejectedBeforeHandoff() {
    Fixture fixture = new Fixture(true, ScriptedControlStore::write);

    assertThrows(NullPointerException.class, () -> fixture.publisher.publish(fixture.invocation,
        fixture.owner, fixture.manifest, RequestScope.withoutDeadline(),
        (CommitAuthorizationProof) null));

    assertEquals(0, fixture.handoff.calls.get());
    assertEquals(0, fixture.control.casCalls.get());
  }

  @Test
  void staleSealedObjectCannotPublishAfterNoChangeWinsDurably() {
    Fixture fixture = new Fixture(true, ScriptedControlStore::write);
    var sealed = fixture.invocation;
    var preparing = InvocationBinding.begin(sealed.invocationId(), sealed.requestDigest(),
        sealed.authorizationScopeDigest(), sealed.transactionId(), sealed.preparedOwnerEpoch(),
        sealed.baseSequence(), sealed.baseRevision());
    fixture.durable.set(preparing.completeWithoutChanges(ByteKey.copyOf(new byte[] {7})));

    assertThrows(CompletionException.class, () -> fixture.publish().join());
    assertEquals(0, fixture.handoff.calls.get());
    assertEquals(0, fixture.control.casCalls.get());
  }

  @Test
  void unpersistedOrDifferentSealedObjectCannotAcquireSendRight() {
    Fixture fixture = new Fixture(true, ScriptedControlStore::write);
    var sealed = fixture.invocation;
    var preparing = InvocationBinding.begin(sealed.invocationId(), sealed.requestDigest(),
        sealed.authorizationScopeDigest(), sealed.transactionId(), sealed.preparedOwnerEpoch(),
        sealed.baseSequence(), sealed.baseRevision());
    fixture.durable.set(preparing);
    assertThrows(CompletionException.class, () -> fixture.publish().join());
    fixture.durable.set(preparing.seal(ByteKey.copyOf(new byte[] {8})));
    assertThrows(CompletionException.class, () -> fixture.publish().join());
    assertEquals(0, fixture.handoff.calls.get());
    assertEquals(0, fixture.control.casCalls.get());
  }

  @Test
  void layoutPreflightFailureDoesNotPublish() {
    Fixture fixture = new Fixture(true, ScriptedControlStore::write, true);

    assertThrows(CompletionException.class, () -> fixture.publish().join());

    assertEquals(0, fixture.handoff.calls.get());
    assertEquals(0, fixture.control.casCalls.get());
  }

  @Test
  void mismatchedOwnerTokenDoesNotTransferWorkOrAttemptCas() {
    Fixture fixture = new Fixture(true, ScriptedControlStore::write);

    assertThrows(CompletionException.class, () -> fixture.publisher.publish(fixture.invocation,
        new CommitOwnerService.OwnerToken(OWNER, 2), fixture.manifest,
        RequestScope.withoutDeadline(), fixture.authorization()).join());

    assertEquals(0, fixture.handoff.calls.get());
    assertEquals(0, fixture.control.casCalls.get());
  }

  @Test
  void successfulCasReturnsSelectedPendingObservation() {
    Fixture fixture = new Fixture(true, ScriptedControlStore::write);

    var result = fixture.publish().join();

    assertEquals(CommitOutcomeResolver.State.SELECTED_PENDING, result.state());
    assertEquals(1, fixture.handoff.calls.get());
    assertEquals(1, fixture.control.casCalls.get());
    assertEquals(1, fixture.authorizationChecks.get());
  }

  @Test
  void repeatedCasNotUpdatedReturnsIndeterminateWithoutClaimingSelection() {
    Fixture fixture = new Fixture(true,
        (store, replacement) -> CompletableFuture.completedFuture(false));

    var result = fixture.publish().join();

    assertEquals(CommitOutcomeResolver.State.INDETERMINATE, result.state());
    assertEquals(32, fixture.control.casCalls.get());
    assertEquals(32, fixture.authorizationChecks.get());
  }

  @Test
  void barrierAdvanceAfterCasMissRevalidatesTheCapturedProof() {
    var barriers = new ArrayList<Long>();
    var attempts = new AtomicInteger();
    Fixture fixture = new Fixture(true, (store, replacement) -> {
      if (attempts.getAndIncrement() == 0) {
        CommitState observed = store.current.get();
        store.current.set(observed.advanceAuthorizationBarrier(
            OWNER, observed.ownerEpoch(), observed.authorizationBarrierGeneration()));
        return CompletableFuture.completedFuture(false);
      }
      return ScriptedControlStore.write(store, replacement);
    });
    var proof = CommitAuthorizationProof.capture(DIGEST, 0,
        (identity, barrier, scope) -> {
          barriers.add(barrier);
          return CompletableFuture.completedFuture(null);
        });

    var result = fixture.publisher.publish(fixture.invocation, fixture.owner, fixture.manifest,
        RequestScope.withoutDeadline(), proof).join();

    assertEquals(CommitOutcomeResolver.State.SELECTED_PENDING, result.state());
    assertEquals(List.of(0L, 1L), barriers);
    assertEquals(2, fixture.control.casCalls.get());
  }

  @Test
  void deniedReauthorizationAfterBarrierAdvanceDoesNotAttemptAnotherCas() {
    var attempts = new AtomicInteger();
    Fixture fixture = new Fixture(true, (store, replacement) -> {
      if (attempts.getAndIncrement() == 0) {
        CommitState observed = store.current.get();
        store.current.set(observed.advanceAuthorizationBarrier(
            OWNER, observed.ownerEpoch(), observed.authorizationBarrierGeneration()));
        return CompletableFuture.completedFuture(false);
      }
      return ScriptedControlStore.write(store, replacement);
    });
    var proof = CommitAuthorizationProof.capture(DIGEST, 0,
        (identity, barrier, scope) -> barrier == 0
            ? CompletableFuture.completedFuture(null)
            : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION)));

    assertThrows(CompletionException.class, () -> fixture.publisher.publish(fixture.invocation,
        fixture.owner, fixture.manifest, RequestScope.withoutDeadline(), proof).join());

    assertEquals(1, fixture.control.casCalls.get());
    assertTrue(fixture.control.current.get().publication().isEmpty());
  }

  @Test
  void installedRevocationBarrierRejectsASelectionUsingAnEarlierControlSnapshot() {
    Fixture fixture = new Fixture(true, ScriptedControlStore::write);
    var firstAuthorization = new CompletableFuture<Void>();
    var revoked = new AtomicBoolean();
    var checkedBarriers = new ArrayList<Long>();
    var proof = CommitAuthorizationProof.capture(DIGEST, 0, (identity, barrier, scope) -> {
      checkedBarriers.add(barrier);
      if (barrier == 0) return firstAuthorization;
      return revoked.get()
          ? CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION))
          : CompletableFuture.completedFuture(null);
    });

    try (var scope = RequestScope.withoutDeadline()) {
      var publication = fixture.publisher.publish(
          fixture.invocation, fixture.owner, fixture.manifest, scope, proof);
      assertEquals(List.of(0L), checkedBarriers);
      revoked.set(true);
      var installer = new AuthorizationBarrierInstaller(fixture.control, fixedClock());
      long target = installer.nextTargetGeneration(scope).join();
      assertEquals(1, installer.installAtLeast(target, scope).join());
      firstAuthorization.complete(null);

      CompletionException failure = assertThrows(CompletionException.class, publication::join);
      assertEquals(BackendFailure.Category.PERMISSION, ((BackendFailure) failure.getCause()).category());
      assertEquals(List.of(0L, 1L), checkedBarriers);
      assertEquals(1, fixture.control.current.get().authorizationBarrierGeneration());
      assertTrue(fixture.control.current.get().publication().isEmpty());
      assertEquals(2, fixture.control.casCalls.get());
    }
  }

  @Test
  void selectedPublicationRemainsSelectedWhenRevocationInstallsBeforeCasReply() {
    assertSelectionSurvivesRevocation(false);
  }

  @Test
  void timedOutSelectionIsResolvedAfterRevocationWithoutReauthorization() {
    assertSelectionSurvivesRevocation(true);
  }

  @Test
  void timedOutUnselectedAttemptRemainsIndeterminateAfterRevocation() {
    var selectionReply = new CompletableFuture<Boolean>();
    Fixture fixture = new Fixture(true, (store, replacement) -> {
      if (replacement.publication().isPresent()) return selectionReply;
      return ScriptedControlStore.write(store, replacement);
    });
    var resolutionRead = new CompletableFuture<Void>();

    try (var scope = RequestScope.withoutDeadline()) {
      var publication = fixture.publisher.publish(
          fixture.invocation, fixture.owner, fixture.manifest, scope, fixture.authorization());
      fixture.control.nextReadGate = resolutionRead;
      selectionReply.completeExceptionally(new BackendFailure(BackendFailure.Category.TIMEOUT));
      assertFalse(publication.isDone());
      var installer = new AuthorizationBarrierInstaller(fixture.control, fixedClock());
      long target = installer.nextTargetGeneration(scope).join();
      assertEquals(1, installer.installAtLeast(target, scope).join());
      resolutionRead.complete(null);

      assertEquals(CommitOutcomeResolver.State.INDETERMINATE, publication.join().state());
      assertEquals(1, fixture.control.current.get().authorizationBarrierGeneration());
      assertTrue(fixture.control.current.get().publication().isEmpty());
      assertEquals(1, fixture.authorizationChecks.get());
      assertEquals(2, fixture.control.casCalls.get());
    }
  }

  private static void assertSelectionSurvivesRevocation(boolean timeout) {
    var selectionReply = new CompletableFuture<Boolean>();
    var resolutionRead = new CompletableFuture<Void>();
    Fixture fixture = new Fixture(true, (store, replacement) -> {
      store.current.set(replacement);
      if (replacement.publication().isPresent()
          && replacement.authorizationBarrierGeneration() == 0) return selectionReply;
      return CompletableFuture.completedFuture(true);
    });
    var revoked = new AtomicBoolean();
    var checks = new AtomicInteger();
    var proof = CommitAuthorizationProof.capture(DIGEST, 0, (identity, barrier, scope) -> {
      checks.incrementAndGet();
      return revoked.get()
          ? CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION))
          : CompletableFuture.completedFuture(null);
    });

    try (var scope = RequestScope.withoutDeadline()) {
      var publication = fixture.publisher.publish(
          fixture.invocation, fixture.owner, fixture.manifest, scope, proof);
      assertTrue(fixture.control.current.get().publication().isPresent());
      if (timeout) {
        fixture.control.nextReadGate = resolutionRead;
        selectionReply.completeExceptionally(new BackendFailure(BackendFailure.Category.TIMEOUT));
        assertFalse(publication.isDone());
      }
      revoked.set(true);
      var installer = new AuthorizationBarrierInstaller(fixture.control, fixedClock());
      long target = installer.nextTargetGeneration(scope).join();
      assertEquals(1, installer.installAtLeast(target, scope).join());
      if (timeout) resolutionRead.complete(null);
      else selectionReply.complete(true);

      var result = publication.join();
      assertEquals(CommitOutcomeResolver.State.SELECTED_PENDING, result.state());
      assertEquals(fixture.invocation.invocationId(), result.decision().orElseThrow().invocationId());
      assertEquals(1, fixture.control.current.get().authorizationBarrierGeneration());
      assertEquals(fixture.invocation.invocationId(),
          fixture.control.current.get().publication().orElseThrow().invocationId());
      assertEquals(1, checks.get());
      assertEquals(2, fixture.control.casCalls.get());
    }
  }

  private static Clock fixedClock() {
    return Clock.fixed(Instant.ofEpochMilli(20), ZoneOffset.UTC);
  }

  @Test
  void formatWithoutAuthorizationBarrierFailsClosedBeforeProofRevalidation() {
    Fixture fixture = new Fixture(true, ScriptedControlStore::write);
    fixture.control.current.set(controlState(CommitState.EVENT_REGISTRY_FORMAT_VERSION, false, 0));

    assertThrows(CompletionException.class, () -> fixture.publish().join());

    assertEquals(0, fixture.authorizationChecks.get());
    assertEquals(0, fixture.control.casCalls.get());
  }

  @Test
  void competingSelectedSlotProvesThisInvocationWasNotSelected() {
    Fixture fixture = new Fixture(true,
        (store, replacement) -> {
          CommitState observed = store.current.get();
          PublicationSlot competitor = new PublicationSlot(TransactionId.create(),
              new InvocationId(UUID.randomUUID()), 1, 0, 1, 1, 2, true,
              ByteKey.copyOf(new byte[] {9}), DIGEST);
          store.current.set(observed.publish(competitor));
          return CompletableFuture.completedFuture(false);
        });

    var result = fixture.publish().join();

    assertEquals(CommitOutcomeResolver.State.NOT_SELECTED, result.state());
    assertTrue(result.notSelectedProof().isPresent());
  }

  @Test
  void unknownCasThatWasPersistedResolvesSelectedFromTheControlRow() {
    Fixture fixture = new Fixture(true,
        (store, replacement) -> {
          store.current.set(replacement);
          return CompletableFuture.failedFuture(
              new BackendFailure(BackendFailure.Category.UNKNOWN));
        });

    var result = fixture.publish().join();

    assertEquals(CommitOutcomeResolver.State.SELECTED_PENDING, result.state());
  }

  @Test
  void unknownCasWithoutDurableEvidenceRemainsIndeterminate() {
    Fixture fixture = new Fixture(true,
        (store, replacement) -> CompletableFuture.failedFuture(
            new BackendFailure(BackendFailure.Category.UNKNOWN)));

    var result = fixture.publish().join();

    assertEquals(CommitOutcomeResolver.State.INDETERMINATE, result.state());
  }

  @Test
  void selectionConditionIsRecheckedAfterHandoffAndBeforeEachRetry() {
    var allowed = new java.util.concurrent.atomic.AtomicBoolean(true);
    Fixture delayed = new Fixture(true, ScriptedControlStore::write);
    delayed.handoff.gate = new CompletableFuture<>();
    var pending = delayed.publish(() -> {
      if (!allowed.get()) throw new IllegalStateException("clock unhealthy");
    });
    allowed.set(false);
    delayed.handoff.gate.complete(null);
    assertThrows(CompletionException.class, pending::join);
    assertEquals(0, delayed.control.casCalls.get());

    allowed.set(true);
    Fixture retry = new Fixture(true, (store, replacement) -> {
      allowed.set(false);
      return CompletableFuture.completedFuture(false);
    });
    assertThrows(CompletionException.class, () -> retry.publish(() -> {
      if (!allowed.get()) throw new IllegalStateException("clock unhealthy");
    }).join());
    assertEquals(1, retry.control.casCalls.get());
  }

  @Test
  void selectedOrUnknownCasIsResolvedWithoutRepeatingSelectionCondition() {
    var checks = new AtomicInteger();
    Fixture fixture = new Fixture(true, (store, replacement) -> {
      store.current.set(replacement);
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
    });
    var result = fixture.publish(() -> {
      if (checks.incrementAndGet() > 1) throw new IllegalStateException("condition changed");
    }).join();
    assertEquals(CommitOutcomeResolver.State.SELECTED_PENDING, result.state());
    assertEquals(1, checks.get());
    assertEquals(1, fixture.authorizationChecks.get());
    var alreadySelected = fixture.publish(() -> { throw new IllegalStateException("condition changed"); }).join();
    assertEquals(CommitOutcomeResolver.State.SELECTED_PENDING, alreadySelected.state());
    assertEquals(1, fixture.control.casCalls.get());
    assertEquals(1, fixture.authorizationChecks.get());
  }

  private static ManifestRoot root(TransactionId transactionId, long ownerEpoch) {
    return ManifestRoot.create(transactionId, ownerEpoch, 0, 1, 0, 0, Optional.empty());
  }

  private static ControlSnapshot snapshot(CommitState state) {
    return new ControlSnapshot(state, new byte[] {1});
  }

  private static CommitState controlState(
      int formatVersion, boolean authorizationBarrierEnabled, long barrierGeneration) {
    boolean hasEventRegistry = formatVersion >= CommitState.EVENT_REGISTRY_FORMAT_VERSION;
    return CommitState.restore(formatVersion, FINGERPRINT, Optional.of(OWNER), 1, 10_000,
        1, 0, 1, 0, 1, Optional.empty(), 0,
        hasEventRegistry ? Optional.of(ByteKey.copyOf(new byte[16])) : Optional.empty(),
        Optional.empty(), hasEventRegistry ? 1 : 0,
        authorizationBarrierEnabled, barrierGeneration);
  }

  private static final class Fixture {
    private final TransactionId transactionId = TransactionId.create();
    private final InvocationBinding invocation;
    private final CommitOwnerService.OwnerToken owner =
        new CommitOwnerService.OwnerToken(OWNER, 1);
    private final ManifestRoot manifest;
    private final PreparedCandidate candidate;
    private final ManifestPage page;
    private final ScriptedControlStore control;
    private final AtomicInteger authorizationChecks = new AtomicInteger();
    private final AtomicReference<InvocationBinding> durable = new AtomicReference<>();
    private final DirectHandoff handoff = new DirectHandoff();
    private final CommitPublisher publisher;

    private Fixture(boolean storedRootPresent,
        BiFunction<ScriptedControlStore, CommitState, CompletableFuture<Boolean>> compareAndSet) {
      this(storedRootPresent, compareAndSet, false);
    }

    private Fixture(boolean storedRootPresent,
        BiFunction<ScriptedControlStore, CommitState, CompletableFuture<Boolean>> compareAndSet,
        boolean failLayoutPreflight) {
      if (failLayoutPreflight) {
        byte[] value = {7};
        candidate = new PreparedCandidate(transactionId, 1, 0, 1,
            StoredRecord.live(ByteKey.copyOf(new byte[] {'k'}),
                new StorageMetadata(1, 2, 2, 0, StorageMetadata.State.LIVE, value.length), value));
        page = ManifestPage.create(transactionId, 0,
            java.util.List.of(ManifestCandidateReference.from(candidate, 0)));
        manifest = ManifestRoot.create(transactionId, 1, 0, 1, 1, value.length,
            Optional.of(page.reference()));
      } else {
        candidate = null;
        page = null;
        manifest = root(transactionId, 1);
      }
      invocation = InvocationBinding.begin(new InvocationId(UUID.randomUUID()), DIGEST, DIGEST,
          transactionId, 1, 0, 1).seal(manifest.systemIdentity());
      durable.set(invocation);
      control = new ScriptedControlStore(controlState(CommitState.FORMAT_VERSION, true, 0), compareAndSet);
      var outcomes = new CommitOutcomeResolver(control, new EmptyDecisionRepository());
      MaterializationStore materialization = new MaterializationStore() {
        @Override
        public CompletableFuture<ApplyResult> applySelected(
            CommitDecision decision, PreparedCandidate candidate, RequestScope scope) {
          return CompletableFuture.completedFuture(ApplyResult.APPLIED);
        }

        @Override
        public CompletableFuture<Void> preflight(PreparedCandidate candidate, RequestScope scope) {
          return failLayoutPreflight
              ? CompletableFuture.failedFuture(new IllegalStateException("layout preflight failed"))
              : CompletableFuture.completedFuture(null);
        }
      };
      var verifier = new CommitPlanVerifier(new ManifestCatalog(new StubManifestRepository(
          storedRootPresent ? Optional.of(manifest) : Optional.empty(), candidate, page)), materialization);
      CommitFinalizer finalizer = (invocation, publication, scope) ->
          CompletableFuture.completedFuture(publication);
      InvocationRepository repository = new InvocationRepository() {
        @Override public CompletableFuture<Optional<InvocationBinding>> read(InvocationId id, RequestScope scope) {
          return CompletableFuture.completedFuture(Optional.ofNullable(durable.get()));
        }
        @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
            InvocationBinding binding, RequestScope scope) { throw new UnsupportedOperationException(); }
        @Override public CompletableFuture<Boolean> compareAndSet(
            InvocationBinding expected, InvocationBinding replacement, RequestScope scope) {
          throw new UnsupportedOperationException();
        }
      };
      publisher = new CommitPublisher(control, outcomes, verifier, repository, handoff, finalizer);
    }

    private CompletableFuture<CommitOutcomeResolver.Resolution> publish() {
      return publisher.publish(
          invocation, owner, manifest, RequestScope.withoutDeadline(), authorization());
    }

    private CompletableFuture<CommitOutcomeResolver.Resolution> publish(Runnable beforeSelection) {
      return publisher.publish(invocation, owner, manifest, RequestScope.withoutDeadline(),
          authorization(), beforeSelection);
    }

    private CommitAuthorizationProof authorization() {
      return CommitAuthorizationProof.capture(DIGEST, 0, (identity, barrier, scope) -> {
        authorizationChecks.incrementAndGet();
        return CompletableFuture.completedFuture(null);
      });
    }
  }

  private static final class DirectHandoff implements PublicationHandoff {
    private final AtomicInteger calls = new AtomicInteger();
    private CompletableFuture<Void> gate = CompletableFuture.completedFuture(null);

    @Override
    public <T> CompletableFuture<T> transfer(InvocationBinding invocation,
        RequestScope requestScope, java.util.function.Function<RequestScope, CompletableFuture<T>> work) {
      calls.incrementAndGet();
      return gate.thenCompose(ignored -> work.apply(requestScope));
    }
  }

  private static final class ScriptedControlStore implements CommitControlStore {
    private final AtomicReference<CommitState> current;
    private final AtomicInteger casCalls = new AtomicInteger();
    private final BiFunction<ScriptedControlStore, CommitState, CompletableFuture<Boolean>> compareAndSet;
    private CompletableFuture<Void> nextReadGate;

    private ScriptedControlStore(CommitState initial,
        BiFunction<ScriptedControlStore, CommitState, CompletableFuture<Boolean>> compareAndSet) {
      current = new AtomicReference<>(initial);
      this.compareAndSet = compareAndSet;
    }

    private static CompletableFuture<Boolean> write(
        ScriptedControlStore store, CommitState replacement) {
      store.current.set(replacement);
      return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      if (nextReadGate != null) {
        CompletableFuture<Void> gate = nextReadGate;
        nextReadGate = null;
        return gate.thenApply(ignored -> Optional.of(snapshot(current.get())));
      }
      return CompletableFuture.completedFuture(Optional.of(snapshot(current.get())));
    }

    @Override
    public CompletableFuture<Boolean> initializeIfAbsent(CommitState initialState, RequestScope scope) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      casCalls.incrementAndGet();
      if (current.get() != observed.state()) return CompletableFuture.completedFuture(false);
      return compareAndSet.apply(this, replacement);
    }
  }

  private static final class EmptyDecisionRepository implements CommitDecisionRepository {
    @Override
    public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        CommitDecision decision, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class StubManifestRepository implements ManifestRepository {
    private final Optional<ManifestRoot> root;
    private final PreparedCandidate candidate;
    private final ManifestPage page;

    private StubManifestRepository(
        Optional<ManifestRoot> root, PreparedCandidate candidate, ManifestPage page) {
      this.root = root;
      this.candidate = candidate;
      this.page = page;
    }

    @Override public int candidateBucket(PreparedCandidate candidate) {
      return candidate.equals(this.candidate) ? 0 : -1;
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(
        PreparedCandidate candidate, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(
        ManifestPage page, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(
        ManifestDirectoryPage page, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEvent(
        PreparedEvent event, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(
        EventPage page, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(
        EventDirectoryPage page, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> seal(
        ManifestRoot value, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<Optional<ManifestRoot>> readRoot(
        ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(root.filter(value -> value.systemIdentity().equals(identity)));
    }
    @Override public CompletableFuture<Optional<PreparedCandidate>> readCandidate(
        ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(candidate != null && candidate.systemIdentity().equals(identity)
          ? Optional.of(candidate) : Optional.empty());
    }
    @Override public CompletableFuture<Optional<ManifestPage>> readPage(
        ByteKey identity, RequestScope scope) {
      return CompletableFuture.completedFuture(page != null && page.systemIdentity().equals(identity)
          ? Optional.of(page) : Optional.empty());
    }
    @Override public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(
        ByteKey identity, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<Optional<PreparedEvent>> readEvent(
        ByteKey identity, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<Optional<EventPage>> readEventPage(
        ByteKey identity, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(
        ByteKey identity, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
  }
}
