package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class CompactionServiceTest {
  private static final ByteKey PROFILE = digest(1);
  private static final ByteKey AUTHORIZATION = digest(2);
  private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(50), ZoneOffset.UTC);
  private static final CommitOwnerService.OwnerToken OWNER = new CommitOwnerService.OwnerToken("owner", 1);

  @Test
  void matchesNativeZeroRepeatedFutureAndNegativeBoundariesWithoutKvCommit() {
    var fixture = new Fixture(1, 0);
    try (var scope = RequestScope.withoutDeadline()) {
      assertEquals(CompactionOperation.Stage.COMPACTED, fixture.compact(-2, scope).stage());
      assertEquals(CompactionOperation.Stage.COMPACTED, fixture.compact(-1, scope).stage());
      var zero = fixture.compact(0, scope);
      assertEquals(CompactionOperation.Stage.LOGICAL, zero.stage());
      assertEquals(1, zero.responseRevision());
      assertEquals(CompactionOperation.Stage.COMPACTED, fixture.compact(0, scope).stage());
      assertEquals(CompactionOperation.Stage.FUTURE, fixture.compact(2, scope).stage());
      assertEquals(1, fixture.control.state.publishedRevision());
      assertEquals(0, fixture.control.state.publishedSequence());
      assertTrue(fixture.control.state.publication().isEmpty());
    }
    var populated = new Fixture(8, 7);
    try (var scope = RequestScope.withoutDeadline()) {
      var operation = populated.compact(5, scope);
      assertEquals(CompactionOperation.Stage.LOGICAL, operation.stage());
      assertEquals(8, operation.responseRevision());
      assertEquals(CompactionOperation.Stage.COMPACTED, populated.compact(4, scope).stage());
      assertEquals(CompactionOperation.Stage.COMPACTED, populated.compact(5, scope).stage());
      assertEquals(CompactionOperation.Stage.LOGICAL, populated.compact(6, scope).stage());
      assertEquals(7, populated.control.state.publishedSequence());
      assertEquals(7, populated.control.state.finalizedSequence());
      assertEquals(8, populated.control.state.publishedRevision());
    }
  }

  @Test
  void recoversUnknownCreateControlAndOperationAcknowledgements() {
    var fixture = new Fixture(8, 7);
    fixture.records.loseCreate = true;
    fixture.records.loseCas = true;
    fixture.control.loseCas = true;
    try (var scope = RequestScope.withoutDeadline()) {
      InvocationBinding binding = fixture.binding();
      var operation = fixture.service.compact(5, binding, OWNER, fixture.proof(), scope).join();
      assertEquals(CompactionOperation.Stage.LOGICAL, operation.stage());
      assertEquals(operation, new CompactionService(fixture.control, fixture.records, CLOCK)
          .compact(5, binding, OWNER, fixture.proof(), scope).join());
      assertEquals(1, fixture.control.selections);
    }
  }

  @Test
  void persistsPreviousSelectionBeforeReplacingItsControlIdentity() {
    var fixture = new Fixture(8, 7);
    try (var scope = RequestScope.withoutDeadline()) {
      InvocationBinding first = fixture.binding();
      fixture.records.failLogical = true;
      assertThrows(CompletionException.class,
          () -> fixture.service.compact(4, first, OWNER, fixture.proof(), scope).join());
      assertEquals(4, fixture.control.state.compactRevision());
      fixture.records.failLogical = false;
      var second = fixture.compact(5, scope);
      assertEquals(CompactionOperation.Stage.LOGICAL, second.stage());
      assertEquals(CompactionOperation.Stage.LOGICAL,
          fixture.service.read(first.invocationId(), scope).join().orElseThrow().stage());
      var newOwner = fixture.control.state.claimOwner("new-owner", 101, 200);
      fixture.control.state = newOwner;
      assertEquals(CompactionOperation.Stage.LOGICAL, fixture.service.compact(4, first,
          new CommitOwnerService.OwnerToken("new-owner", 2), fixture.proof(), scope).join().stage());
      assertEquals(2, fixture.control.selections);
    }
  }

  @Test
  void unknownCasBeforeWriteDoesNotBecomeSuccessFromAHigherUnrelatedCompact() {
    var fixture = new Fixture(8, 7);
    try (var scope = RequestScope.withoutDeadline()) {
      fixture.control.beforeCas = replacement -> {
        fixture.control.state = fixture.control.state.advanceCompactRevision(6);
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      };
      assertEquals(CompactionOperation.Stage.COMPACTED, fixture.compact(5, scope).stage());
      assertEquals(0, fixture.control.selections);
    }
  }

  @Test
  void authorizationBarrierRaceRevalidatesAndUsesExactFullState() {
    var fixture = new Fixture(8, 7);
    int[] validations = {0};
    var proof = CommitAuthorizationProof.capture(AUTHORIZATION, 0, (identity, generation, scope) -> {
      validations[0]++;
      if (generation == 0) {
        fixture.control.state = fixture.control.state.advanceAuthorizationBarrier("owner", 1, 0);
        return CompletableFuture.completedFuture(null);
      }
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
    });
    try (var scope = RequestScope.withoutDeadline()) {
      Throwable failure = failure(() -> fixture.service.compact(5, fixture.binding(), OWNER, proof, scope).join());
      assertEquals(BackendFailure.Category.PERMISSION, assertInstanceOf(BackendFailure.class, failure).category());
      assertEquals(2, validations[0]);
      assertEquals(0, fixture.control.state.compactRevision());
      assertEquals(1, fixture.control.state.authorizationBarrierGeneration());
    }
  }

  @Test
  void refusesLegacyControlBeforeCreatingAnOperationWithoutAnAuthorizationBarrier() {
    var fixture = new Fixture(8, 7);
    fixture.control.state = CommitState.restore(PROFILE, Optional.of("owner"), 1, 100, 18,
        7, 8, 7, 8, Optional.empty(), 0);
    try (var scope = RequestScope.withoutDeadline()) {
      assertEquals(BackendFailure.Category.UNAVAILABLE, assertInstanceOf(BackendFailure.class,
          failure(() -> fixture.compact(5, scope))).category());
      assertTrue(fixture.records.values.isEmpty());
      assertEquals(0, fixture.control.state.compactRevision());
    }
  }

  @Test
  void refreshesResponseBarrierAfterConcurrentControlChange() {
    var fixture = new Fixture(8, 7);
    fixture.control.beforeCas = replacement -> {
      fixture.control.state = state(9, 8);
      return CompletableFuture.completedFuture(false);
    };
    try (var scope = RequestScope.withoutDeadline()) {
      var operation = fixture.compact(5, scope);
      assertEquals(9, operation.responseRevision());
      assertEquals(8, operation.baseSequence());
      assertEquals(5, fixture.control.state.compactRevision());
    }
  }

  @Test
  void permissionFailureIsNotHiddenByReadbackAndCancellationStopsNewSelection() {
    var fixture = new Fixture(8, 7);
    try (var scope = RequestScope.withoutDeadline()) {
      fixture.records.failCreate = true;
      var failure = failure(() -> fixture.compact(5, scope));
      assertEquals(BackendFailure.Category.PERMISSION, assertInstanceOf(BackendFailure.class, failure).category());
      assertEquals(0, fixture.control.state.compactRevision());
      fixture.records.failCreate = false;
      var pending = new CompletableFuture<Void>();
      var proof = CommitAuthorizationProof.capture(AUTHORIZATION, 0, (identity, generation, checked) -> pending);
      var outcome = fixture.service.compact(5, fixture.binding(), OWNER, proof, scope);
      assertFalse(outcome.isDone());
      scope.cancel();
      pending.complete(null);
      assertEquals(BackendFailure.Category.CANCELLED,
          assertInstanceOf(BackendFailure.class, failure(outcome::join)).category());
      assertEquals(0, fixture.control.state.compactRevision());
    }
  }

  @Test
  void ownerTakeoverAndInvocationConflictCannotSelectAPreparedOperation() {
    var fixture = new Fixture(8, 7);
    try (var scope = RequestScope.withoutDeadline()) {
      var pending = new CompletableFuture<Void>();
      var proof = CommitAuthorizationProof.capture(AUTHORIZATION, 0, (identity, generation, checked) -> pending);
      var binding = fixture.binding();
      var outcome = fixture.service.compact(5, binding, OWNER, proof, scope);
      fixture.control.state = fixture.control.state.claimOwner("new-owner", 101, 200);
      pending.complete(null);
      assertEquals(BackendFailure.Category.UNAVAILABLE,
          assertInstanceOf(BackendFailure.class, failure(outcome::join)).category());
      assertEquals(0, fixture.control.state.compactRevision());
      assertEquals(BackendFailure.Category.PERMISSION, assertInstanceOf(BackendFailure.class,
          failure(() -> fixture.service.compact(6, binding, OWNER, fixture.proof(), scope).join())).category());
    }
  }

  @Test
  void strictRecordCodecRejectsMalformedAndUnknownStages() {
    var fixture = new Fixture(8, 7);
    try (var scope = RequestScope.withoutDeadline()) {
      var operation = fixture.compact(5, scope);
      assertEquals(operation, CompactionOperation.decode(operation.encode()));
      byte[] encoded = operation.encode().bytes();
      ByteBuffer.wrap(encoded).putInt(2);
      assertThrows(IllegalArgumentException.class, () -> CompactionOperation.decode(ByteKey.copyOf(encoded)));
      assertThrows(IllegalArgumentException.class, () -> CompactionOperation.decode(ByteKey.copyOf(new byte[0])));
      byte[] stage = operation.encode().bytes();
      ByteBuffer.wrap(stage).putInt(stage.length - 4, 99);
      assertThrows(IllegalArgumentException.class, () -> CompactionOperation.decode(ByteKey.copyOf(stage)));
    }
  }

  private static Throwable failure(Runnable operation) {
    return CompletionFailures.unwrapCompletion(assertThrows(CompletionException.class, operation::run));
  }

  private static ByteKey digest(int value) {
    byte[] bytes = new byte[32];
    bytes[0] = (byte) value;
    return ByteKey.copyOf(bytes);
  }

  private static CommitState state(long revision, long sequence) {
    return CommitState.restore(CommitState.FORMAT_VERSION, PROFILE, Optional.of("owner"), 1,
        100, revision + 10, sequence, revision, sequence, revision, Optional.empty(), 0,
        Optional.empty(), Optional.empty(), 0, true, 0);
  }

  static final class Fixture {
    final MemoryControl control;
    final MemoryRecords records = new MemoryRecords();
    final CompactionService service;
    Fixture(long revision, long sequence) {
      control = new MemoryControl(state(revision, sequence));
      service = new CompactionService(control, records, CLOCK);
    }
    InvocationBinding binding() {
      return InvocationBinding.begin(InvocationId.create(), digest(3), AUTHORIZATION,
          TransactionId.create(), 1, control.state.publishedSequence(), control.state.publishedRevision());
    }
    CommitAuthorizationProof proof() {
      return CommitAuthorizationProof.capture(AUTHORIZATION, 0, (identity, generation, scope) ->
          CompletableFuture.completedFuture(null));
    }
    CompactionOperation compact(long revision, RequestScope scope) {
      return service.compact(revision, binding(), OWNER, proof(), scope).join();
    }
  }

  static final class MemoryControl implements CommitControlStore {
    CommitState state;
    boolean loseCas;
    int selections;
    java.util.function.Function<CommitState, CompletableFuture<Boolean>> beforeCas;
    MemoryControl(CommitState state) { this.state = state; }
    @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state,
          ByteBuffer.allocate(16).putLong(state.stateVersion()).putLong(state.ownerEpoch()).array())));
    }
    @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState state, RequestScope scope) {
      throw new UnsupportedOperationException();
    }
    @Override public CompletableFuture<Boolean> compareAndSet(ControlSnapshot observed,
        CommitState replacement, RequestScope scope) {
      if (beforeCas != null) {
        var callback = beforeCas;
        beforeCas = null;
        return callback.apply(replacement);
      }
      if (observed.state() != state) return CompletableFuture.completedFuture(false);
      state = replacement;
      selections++;
      if (loseCas) {
        loseCas = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(true);
    }
  }

  static final class MemoryRecords implements SystemRecordStore {
    final Map<ByteKey, ByteKey> values = new HashMap<>();
    boolean loseCreate, loseCas, failCreate, failLogical;
    @Override public CompletableFuture<Optional<ByteKey>> read(SystemRecordType type, ByteKey id, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(values.get(id)));
    }
    @Override public int bucket(SystemRecordType type, ByteKey id) { return 0; }
    @Override public CompletableFuture<CreateResult> createIfAbsent(SystemRecordType type, ByteKey id,
        ByteKey value, RequestScope scope) {
      if (failCreate) return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
      ByteKey previous = values.putIfAbsent(id, value);
      if (loseCreate) {
        loseCreate = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(previous == null ? CreateResult.CREATED
          : previous.equals(value) ? CreateResult.ALREADY_PRESENT : CreateResult.CONFLICT);
    }
    @Override public CompletableFuture<Boolean> compareAndSet(SystemRecordType type, ByteKey id,
        Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) {
      if (failLogical && CompactionOperation.decode(replacement).stage() == CompactionOperation.Stage.LOGICAL) {
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
      }
      if (!Optional.ofNullable(values.get(id)).equals(expected)) return CompletableFuture.completedFuture(false);
      values.put(id, replacement);
      if (loseCas) {
        loseCas = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(true);
    }
  }
}
