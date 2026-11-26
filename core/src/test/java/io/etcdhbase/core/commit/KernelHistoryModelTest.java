package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.BatchWriteResult;
import io.etcdhbase.core.storage.ReadProjection;
import io.etcdhbase.core.storage.RecordStore;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/** Bounded histories through the M2 publication, read-view, and recovery components. */
class KernelHistoryModelTest {
  @Test
  void selectedUnknownTransactionIsVisibleAcrossCancellationAndRecovery() {
    KernelDriver driver = new KernelDriver(Map.of("alpha", "old-alpha", "beta", "old-beta"));
    Map<String, Mutation> changes = new LinkedHashMap<>();
    changes.put("alpha", Mutation.put("new-alpha"));
    changes.put("beta", Mutation.put("new-beta"));
    try (PendingTransaction transaction = driver.beginCompareAndWrite(
        "alpha", Optional.of("old-alpha"), changes, PublicationPause.AFTER_FIRST_CANDIDATE)) {
      assertEquals(1, driver.materialization.appliedCount());
      assertEquals(Optional.of("new-alpha"), driver.physicalValue("alpha"));
      assertEquals(Optional.of("old-beta"), driver.physicalValue("beta"));

      HistoryOperation alphaRead = driver.read("alpha");
      assertEquals(Optional.of("new-alpha"), alphaRead.observedValue());
      HistoryOperation cancelled = transaction.cancelCallerAfterSelection();
      HistoryOperation betaRead = driver.read("beta");
      assertEquals(Optional.of("new-beta"), betaRead.observedValue());

      // A missing selected candidate must fail the actual view read rather than return the stale row.
      driver.manifests.missingKey = key("beta");
      CompletionException missing = assertThrows(CompletionException.class, () -> driver.read("beta"));
      assertEquals("private candidate referenced by a sealed manifest is missing",
          assertInstanceOf(IllegalStateException.class, missing.getCause()).getMessage());
      driver.manifests.missingKey = null;

      transaction.stopServiceForRecovery();
      assertEquals(RecoveryManager.State.FINALIZED, driver.recoverSelectedPublication());
      assertEquals(RecoveryManager.State.NO_PENDING_PUBLICATION, driver.recoverSelectedPublication());
      assertEquals(2, driver.materialization.appliedCount());
      HistoryOperation staleCompare = driver.compareAndWrite("alpha", Optional.of("old-alpha"),
          Map.of("alpha", Mutation.put("stale")));
      HistoryOperation put = driver.put("gamma", "created");
      HistoryOperation putRead = driver.read("gamma");
      HistoryOperation delete = driver.delete("beta");
      HistoryOperation deletedRead = driver.read("beta");

      List<HistoryOperation> history = List.of(
          cancelled, alphaRead, betaRead, staleCompare, put, putRead, delete, deletedRead);
      SerialReferenceChecker checker = new SerialReferenceChecker(Map.of(
          "alpha", "old-alpha", "beta", "old-beta"));

      assertTrue(checker.hasLinearization(history));
      assertFalse(checker.hasLinearization(withObservedValue(history, alphaRead.id(), Optional.of("old-alpha"))));
      assertFalse(checker.hasLinearization(withObservedValue(history, betaRead.id(), Optional.of("old-beta"))));
      assertTrue(driver.control.state.publication().isEmpty());
      assertEquals(driver.control.state.finalizedSequence(), driver.control.state.publishedSequence());
      assertEquals(3, driver.control.state.publishedSequence(),
          "the selected unknown transaction, put, and delete each advance the commit sequence once");
      assertEquals(4, driver.control.state.publishedRevision(),
          "replaying recovery must not allocate a second revision");
      assertTrue(driver.openedPins > 0);
      assertEquals(driver.openedPins, driver.closedPins);
    }
  }

  @Test
  void unresolvedUnknownMayHaveTakenEffectOrMayNotHaveTakenEffect() {
    HistoryOperation unknown = HistoryOperation.compareAndWrite(1, 4, OptionalInt.empty(),
        "alpha", Optional.of("old"), Map.of("alpha", Mutation.put("new")),
        Outcome.UNKNOWN, PublicationObservation.UNKNOWN);
    HistoryOperation oldRead = HistoryOperation.read(5, 6, "alpha", Optional.of("old"));
    HistoryOperation newRead = HistoryOperation.read(5, 6, "alpha", Optional.of("new"));

    assertTrue(new SerialReferenceChecker(Map.of("alpha", "old"))
        .hasLinearization(List.of(unknown, oldRead)));
    assertTrue(new SerialReferenceChecker(Map.of("alpha", "old"))
        .hasLinearization(List.of(unknown, newRead)));
  }

  @Test
  void unknownCompareMismatchMayHaveNoEffect() {
    HistoryOperation unknown = HistoryOperation.compareAndWrite(0, 1, OptionalInt.empty(),
        "alpha", Optional.of("old"), Map.of("alpha", Mutation.put("new")),
        Outcome.UNKNOWN, PublicationObservation.UNKNOWN);
    SerialReferenceChecker checker = new SerialReferenceChecker(Map.of("alpha", "other"));

    assertTrue(checker.hasLinearization(List.of(unknown,
        HistoryOperation.read(2, 3, "alpha", Optional.of("other")))));
    assertFalse(checker.hasLinearization(List.of(unknown,
        HistoryOperation.read(2, 3, "alpha", Optional.of("new")))));
  }

  @Test
  void unknownNotSelectedCannotWrite() {
    HistoryOperation unknown = HistoryOperation.compareAndWrite(0, 1, OptionalInt.empty(),
        "alpha", Optional.of("old"), Map.of("alpha", Mutation.put("new")),
        Outcome.UNKNOWN, PublicationObservation.NOT_SELECTED);
    SerialReferenceChecker checker = new SerialReferenceChecker(Map.of("alpha", "old"));

    assertTrue(checker.hasLinearization(List.of(unknown,
        HistoryOperation.read(2, 3, "alpha", Optional.of("old")))));
    assertFalse(checker.hasLinearization(List.of(unknown,
        HistoryOperation.read(2, 3, "alpha", Optional.of("new")))));
    HistoryOperation unknownPut = new HistoryOperation(0, Kind.PUT, 0, 1, OptionalInt.empty(),
        "alpha", Optional.empty(), Map.of("alpha", Mutation.put("new")), Optional.empty(),
        Outcome.UNKNOWN, PublicationObservation.NOT_SELECTED);
    assertTrue(checker.hasLinearization(List.of(unknownPut,
        HistoryOperation.read(2, 3, "alpha", Optional.of("old")))));
    assertFalse(checker.hasLinearization(List.of(unknownPut,
        HistoryOperation.read(2, 3, "alpha", Optional.of("new")))));
    assertTrue(new SerialReferenceChecker(Map.of("alpha", "other")).hasLinearization(List.of(
        unknown, HistoryOperation.read(2, 3, "alpha", Optional.of("other")))));
  }

  @Test
  void selectedUnknownMayDecideAfterCallerReturns() {
    HistoryOperation unknown = HistoryOperation.compareAndWrite(0, 1, OptionalInt.of(4),
        "alpha", Optional.of("old"), Map.of("alpha", Mutation.put("new")),
        Outcome.UNKNOWN, PublicationObservation.SELECTED);
    HistoryOperation oldRead = HistoryOperation.read(2, 3, "alpha", Optional.of("old"));
    HistoryOperation newRead = HistoryOperation.read(5, 6, "alpha", Optional.of("new"));
    SerialReferenceChecker checker = new SerialReferenceChecker(Map.of("alpha", "old"));

    assertTrue(checker.hasLinearization(List.of(unknown, oldRead, newRead)));
    assertFalse(checker.hasLinearization(List.of(unknown,
        HistoryOperation.read(2, 3, "alpha", Optional.of("new")))));
    assertFalse(checker.hasLinearization(List.of(unknown,
        HistoryOperation.read(5, 6, "alpha", Optional.of("old")))));
  }

  @Test
  void unresolvedUnknownMayDecideAfterCallerReturns() {
    HistoryOperation unknown = HistoryOperation.compareAndWrite(0, 1, OptionalInt.empty(),
        "alpha", Optional.of("old"), Map.of("alpha", Mutation.put("new")),
        Outcome.UNKNOWN, PublicationObservation.UNKNOWN);
    SerialReferenceChecker checker = new SerialReferenceChecker(Map.of("alpha", "old"));

    assertTrue(checker.hasLinearization(List.of(unknown,
        HistoryOperation.read(2, 3, "alpha", Optional.of("old")),
        HistoryOperation.read(4, 5, "alpha", Optional.of("new")))));
  }

  @Test
  void completedAndSelectedOutcomesKeepTheirTimeAndCompareConstraints() {
    SerialReferenceChecker checker = new SerialReferenceChecker(Map.of("alpha", "old"));
    HistoryOperation completed = HistoryOperation.compareAndWrite(0, 1, OptionalInt.empty(),
        "alpha", Optional.of("old"), Map.of("alpha", Mutation.put("new")),
        Outcome.COMPLETE, PublicationObservation.SELECTED);
    assertFalse(checker.hasLinearization(List.of(completed,
        HistoryOperation.read(2, 3, "alpha", Optional.of("old")))));
    assertThrows(IllegalArgumentException.class, () -> HistoryOperation.compareAndWrite(
        0, 1, OptionalInt.of(4), "alpha", Optional.of("old"),
        Map.of("alpha", Mutation.put("new")), Outcome.COMPLETE, PublicationObservation.SELECTED));

    HistoryOperation selectedBeforeReturn = HistoryOperation.compareAndWrite(0, 2, OptionalInt.of(1),
        "alpha", Optional.of("old"), Map.of("alpha", Mutation.put("new")),
        Outcome.UNKNOWN, PublicationObservation.SELECTED);
    assertFalse(checker.hasLinearization(List.of(selectedBeforeReturn,
        HistoryOperation.read(3, 4, "alpha", Optional.of("old")))));
    assertTrue(checker.hasLinearization(List.of(selectedBeforeReturn,
        HistoryOperation.read(3, 4, "alpha", Optional.of("new")))));

    HistoryOperation selectedMismatch = HistoryOperation.compareAndWrite(0, 1, OptionalInt.of(1),
        "alpha", Optional.of("other"), Map.of("alpha", Mutation.put("new")),
        Outcome.UNKNOWN, PublicationObservation.SELECTED);
    assertFalse(checker.hasLinearization(List.of(selectedMismatch,
        HistoryOperation.read(2, 3, "alpha", Optional.of("old")))));
  }

  @Test
  void successfulOutcomeConstrainsAnUnknownPublicationObservation() {
    HistoryOperation complete = HistoryOperation.compareAndWrite(0, 1, OptionalInt.empty(),
        "alpha", Optional.of("old"), Map.of("alpha", Mutation.put("new")),
        Outcome.COMPLETE, PublicationObservation.UNKNOWN);
    SerialReferenceChecker matching = new SerialReferenceChecker(Map.of("alpha", "old"));
    assertTrue(matching.hasLinearization(List.of(complete,
        HistoryOperation.read(2, 3, "alpha", Optional.of("new")))));
    assertFalse(matching.hasLinearization(List.of(complete,
        HistoryOperation.read(2, 3, "alpha", Optional.of("old")))));
    HistoryOperation contradicted = HistoryOperation.compareAndWrite(0, 1, OptionalInt.empty(),
        "alpha", Optional.of("old"), Map.of("alpha", Mutation.put("new")),
        Outcome.COMPLETE, PublicationObservation.NOT_SELECTED);
    assertFalse(matching.hasLinearization(List.of(contradicted,
        HistoryOperation.read(2, 3, "alpha", Optional.of("new")))));
  }

  @Test
  void failedCompareConstrainsAnUnknownPublicationObservation() {
    HistoryOperation compareFailed = HistoryOperation.compareAndWrite(0, 1, OptionalInt.empty(),
        "alpha", Optional.of("old"), Map.of("alpha", Mutation.put("new")),
        Outcome.COMPARE_FAILED, PublicationObservation.UNKNOWN);
    SerialReferenceChecker mismatch = new SerialReferenceChecker(Map.of("alpha", "other"));
    assertTrue(mismatch.hasLinearization(List.of(compareFailed,
        HistoryOperation.read(2, 3, "alpha", Optional.of("other")))));
    assertFalse(mismatch.hasLinearization(List.of(compareFailed,
        HistoryOperation.read(2, 3, "alpha", Optional.of("new")))));
    HistoryOperation contradicted = HistoryOperation.compareAndWrite(0, 1, OptionalInt.of(1),
        "alpha", Optional.of("old"), Map.of("alpha", Mutation.put("new")),
        Outcome.COMPARE_FAILED, PublicationObservation.SELECTED);
    assertFalse(mismatch.hasLinearization(List.of(contradicted,
        HistoryOperation.read(2, 3, "alpha", Optional.of("other")))));
  }

  @Test
  void cancelledCallerMayObserveAServiceDecisionLater() {
    KernelDriver driver = new KernelDriver(Map.of("alpha", "old"));
    try (PendingTransaction transaction = driver.beginCompareAndWrite(
        "alpha", Optional.of("old"), Map.of("alpha", Mutation.put("new")),
        PublicationPause.BEFORE_SELECTION)) {
      int returnedAt = transaction.cancelCallerBeforeSelection();
      HistoryOperation before = driver.read("alpha");
      assertEquals(Optional.of("old"), before.observedValue());

      driver.control.releasePublication();
      HistoryOperation after = driver.read("alpha");
      assertEquals(Optional.of("new"), after.observedValue());
      HistoryOperation laterWrite = driver.put("beta", "later");
      HistoryOperation unknown = transaction.selectedAfterReturn(returnedAt);

      assertTrue(new SerialReferenceChecker(Map.of("alpha", "old"))
          .hasLinearization(List.of(unknown, before, after, laterWrite)));
      assertEquals(2, driver.control.state.finalizedSequence());
      assertTrue(transaction.handoff.serviceScope.isClosed());
    }
    assertEquals(driver.openedPins, driver.closedPins);
  }

  private static List<HistoryOperation> withObservedValue(
      List<HistoryOperation> history, int operationId, Optional<String> value) {
    return history.stream().map(operation -> operation.id() == operationId
        ? operation.withObservedValue(value) : operation).toList();
  }

  private static Map<String, Mutation> immutableMutations(Map<String, Mutation> mutations) {
    return Collections.unmodifiableMap(new LinkedHashMap<>(mutations));
  }

  private enum Kind { COMPARE_AND_WRITE, PUT, DELETE, READ }
  private enum Outcome { COMPLETE, COMPARE_FAILED, UNKNOWN }
  private enum PublicationObservation { SELECTED, NOT_SELECTED, UNKNOWN }
  private enum PublicationPause { BEFORE_SELECTION, AFTER_FIRST_CANDIDATE }
  private enum MutationKind { PUT, DELETE }

  private record Mutation(MutationKind kind, Optional<String> value) {
    private Mutation {
      Objects.requireNonNull(kind, "kind");
      Objects.requireNonNull(value, "value");
      if ((kind == MutationKind.PUT) != value.isPresent()) {
        throw new IllegalArgumentException("mutation value does not match its kind");
      }
    }

    private static Mutation put(String value) {
      return new Mutation(MutationKind.PUT, Optional.of(Objects.requireNonNull(value, "value")));
    }

    private static Mutation delete() {
      return new Mutation(MutationKind.DELETE, Optional.empty());
    }
  }

  /** Invocation and return times are distinct from the durable publication decision time. */
  private record HistoryOperation(int id, Kind kind, int invokedAt, int returnedAt,
      OptionalInt decisionAt, String key, Optional<String> expectedValue,
      Map<String, Mutation> mutations, Optional<String> observedValue,
      Outcome outcome, PublicationObservation publication) {
    private HistoryOperation {
      Objects.requireNonNull(kind, "kind");
      Objects.requireNonNull(decisionAt, "decisionAt");
      Objects.requireNonNull(key, "key");
      Objects.requireNonNull(expectedValue, "expectedValue");
      mutations = immutableMutations(mutations);
      Objects.requireNonNull(observedValue, "observedValue");
      Objects.requireNonNull(outcome, "outcome");
      Objects.requireNonNull(publication, "publication");
      if (invokedAt < 0 || returnedAt <= invokedAt
          || decisionAt.isPresent() && (decisionAt.getAsInt() < invokedAt
              || outcome != Outcome.UNKNOWN && decisionAt.getAsInt() > returnedAt)) {
        throw new IllegalArgumentException("invalid operation interval");
      }
      if (kind == Kind.READ && (outcome != Outcome.COMPLETE || !mutations.isEmpty())) {
        throw new IllegalArgumentException("read history entry has a write outcome");
      }
      if (kind != Kind.READ && mutations.isEmpty()) {
        throw new IllegalArgumentException("write history entry must contain a mutation");
      }
    }

    private static HistoryOperation compareAndWrite(int invokedAt, int returnedAt,
        OptionalInt decisionAt, String key, Optional<String> expectedValue,
        Map<String, Mutation> mutations, Outcome outcome, PublicationObservation publication) {
      return new HistoryOperation(0, Kind.COMPARE_AND_WRITE, invokedAt, returnedAt, decisionAt,
          key, expectedValue, mutations, Optional.empty(), outcome, publication);
    }

    private static HistoryOperation mutation(int id, Kind kind, int invokedAt, int returnedAt,
        String key, Mutation mutation, OptionalInt decisionAt) {
      return new HistoryOperation(id, kind, invokedAt, returnedAt, decisionAt, key,
          Optional.empty(), Map.of(key, mutation), Optional.empty(), Outcome.COMPLETE,
          PublicationObservation.SELECTED);
    }

    private static HistoryOperation read(
        int invokedAt, int returnedAt, String key, Optional<String> observedValue) {
      return new HistoryOperation(0, Kind.READ, invokedAt, returnedAt, OptionalInt.empty(), key,
          Optional.empty(), Map.of(), observedValue, Outcome.COMPLETE,
          PublicationObservation.NOT_SELECTED);
    }

    private HistoryOperation withId(int newId) {
      return new HistoryOperation(newId, kind, invokedAt, returnedAt, decisionAt, key,
          expectedValue, mutations, observedValue, outcome, publication);
    }

    private HistoryOperation withObservedValue(Optional<String> value) {
      return new HistoryOperation(id, kind, invokedAt, returnedAt, decisionAt, key,
          expectedValue, mutations, value, outcome, publication);
    }
  }

  /** Backtracks over real-time-compatible orders and branches only on genuinely unknown outcomes. */
  private static final class SerialReferenceChecker {
    private final Map<String, String> initialValues;

    private SerialReferenceChecker(Map<String, String> initialValues) {
      this.initialValues = Map.copyOf(initialValues);
    }

    private boolean hasLinearization(List<HistoryOperation> history) {
      if (history.size() > 20) throw new IllegalArgumentException("history exceeds the bounded search size");
      List<HistoryOperation> operations = new ArrayList<>(history.size());
      for (int index = 0; index < history.size(); index++) {
        operations.add(history.get(index).withId(index));
      }
      return search(operations, 0L, initialValues);
    }

    private boolean search(List<HistoryOperation> operations, long completed,
        Map<String, String> values) {
      if (completed == (1L << operations.size()) - 1) return true;
      for (HistoryOperation operation : operations) {
        long bit = 1L << operation.id();
        if ((completed & bit) != 0
            || hasUnorderedPredecessor(operations, operation, completed)) continue;
        for (Map<String, String> next : apply(operation, values)) {
          if (search(operations, completed | bit, next)) return true;
        }
      }
      return false;
    }

    private boolean hasUnorderedPredecessor(List<HistoryOperation> operations,
        HistoryOperation candidate, long completed) {
      for (HistoryOperation predecessor : operations) {
        if (predecessor.id() == candidate.id() || (completed & (1L << predecessor.id())) != 0) continue;
        if (mustPrecede(predecessor, candidate)) return true;
      }
      return false;
    }

    private static boolean mustPrecede(HistoryOperation first, HistoryOperation second) {
      // An unknown caller result does not stop accepted service work. Only a known decision
      // bounds that operation; otherwise it can still take effect after the caller returns.
      if (first.outcome() == Outcome.UNKNOWN && first.decisionAt().isEmpty()) return false;
      int firstLatest = first.decisionAt().orElse(first.returnedAt());
      int secondEarliest = second.decisionAt().orElse(second.invokedAt());
      return first.id() != second.id() && firstLatest <= secondEarliest;
    }

    private static List<Map<String, String>> apply(
        HistoryOperation operation, Map<String, String> values) {
      return switch (operation.kind()) {
        case READ -> Objects.equals(Optional.ofNullable(values.get(operation.key())),
            operation.observedValue()) ? List.of(values) : List.of();
        case PUT, DELETE -> applyWrite(operation, values);
        case COMPARE_AND_WRITE -> applyCompareWrite(operation, values);
      };
    }

    private static List<Map<String, String>> applyCompareWrite(
        HistoryOperation operation, Map<String, String> values) {
      boolean matches = Objects.equals(Optional.ofNullable(values.get(operation.key())),
          operation.expectedValue());
      if (!matches) {
        return operation.publication() != PublicationObservation.SELECTED
            && (operation.outcome() == Outcome.UNKNOWN || operation.outcome() == Outcome.COMPARE_FAILED)
            ? List.of(values) : List.of();
      }
      if (operation.outcome() == Outcome.COMPARE_FAILED) return List.of();
      return applyWrite(operation, values);
    }

    private static List<Map<String, String>> applyWrite(
        HistoryOperation operation, Map<String, String> values) {
      if (operation.outcome() == Outcome.COMPARE_FAILED) return List.of();
      if (operation.publication() == PublicationObservation.NOT_SELECTED) {
        return operation.outcome() == Outcome.UNKNOWN ? List.of(values) : List.of();
      }
      if (operation.outcome() == Outcome.COMPLETE
          || operation.publication() == PublicationObservation.SELECTED) {
        return List.of(applyMutations(values, operation.mutations()));
      }
      if (operation.publication() == PublicationObservation.UNKNOWN
          && operation.outcome() == Outcome.UNKNOWN) {
        Map<String, String> unchanged = Map.copyOf(values);
        Map<String, String> changed = applyMutations(values, operation.mutations());
        return changed.equals(unchanged) ? List.of(unchanged) : List.of(unchanged, changed);
      }
      return List.of();
    }

    private static Map<String, String> applyMutations(
        Map<String, String> values, Map<String, Mutation> mutations) {
      Map<String, String> changed = new HashMap<>(values);
      mutations.forEach((key, mutation) -> {
        if (mutation.kind() == MutationKind.PUT) changed.put(key, mutation.value().orElseThrow());
        else changed.remove(key);
      });
      return Map.copyOf(changed);
    }
  }

  /** The stores are deterministic fakes; publication, view capture, and recovery are real. */
  private static final class KernelDriver {
    private static final ByteKey PROFILE = ByteKey.copyOf(new byte[32]);

    private final EventClock clock = new EventClock();
    private final MemoryControl control = new MemoryControl(
        CommitState.initial(PROFILE, 1).claimOwner("history-owner", 1, 100)
            .enableAuthorizationBarrier("history-owner", 1, 2), clock);
    private final MemoryInvocations invocations = new MemoryInvocations();
    private final MemoryDecisions decisions = new MemoryDecisions();
    private final MemoryManifests manifests = new MemoryManifests();
    private final MemoryRecords physical = new MemoryRecords();
    private final MemoryMaterialization materialization = new MemoryMaterialization(physical);
    private final BudgetManager.Limits limits =
        new BudgetManager.Limits(4, 32, 2, 2, 2, 1_000_000, 16_000_000);
    private final BudgetManager budgets = new BudgetManager(limits, Map.of("public", limits), limits);
    private final ManifestCatalog catalog = new ManifestCatalog(manifests);
    private final CommitOutcomeResolver outcomes = new CommitOutcomeResolver(control, decisions);
    private final CommitFinalizationService finalizer = new CommitFinalizationService(
        control, outcomes, decisions, catalog, materialization);
    private final RecoveryManager recovery = new RecoveryManager(
        PROFILE, control, new CommitOwnerService(control), invocations, outcomes, finalizer);
    private int openedPins;
    private int closedPins;
    private final ReadViewService views = new ReadViewService(control, catalog,
        (view, scope) -> {
          openedPins++;
          return CompletableFuture.completedFuture(new ReadViewPinRegistry.Pin() {
            private boolean closed;
            @Override public long generation() { return 1; }
            @Override public void close() {
              if (!closed) {
                closed = true;
                closedPins++;
              }
            }
          });
        }, PROFILE);
    private int nextOperationId = 1;
    private int nextIdentity = 1;

    private KernelDriver(Map<String, String> initialValues) {
      initialValues.forEach((key, value) ->
          physical.add(record(key, Optional.of(value), 1, Optional.empty())));
    }

    private PendingTransaction beginCompareAndWrite(String key, Optional<String> expectedValue,
        Map<String, Mutation> mutations, PublicationPause pause) {
      int invokedAt = clock.next();
      if (!readValue(key).equals(expectedValue)) {
        throw new IllegalStateException("fixture compare must pass before publication");
      }
      if (pause == PublicationPause.BEFORE_SELECTION) control.deferPublication();
      else materialization.blockAfterOneNewCandidate();
      int id = nextOperationId++;
      RequestScope requestScope = RequestScope.withoutDeadline();
      TestHandoff handoff = new TestHandoff();
      PublicationAttempt attempt = publish(mutations, requestScope, handoff);
      CompletableFuture<CommitOutcomeResolver.Resolution> result = attempt.result();
      boolean atWrongBoundary = pause == PublicationPause.BEFORE_SELECTION
          ? control.state.publication().isPresent() : control.state.publication().isEmpty();
      if (result.isDone() || atWrongBoundary) {
        throw new IllegalStateException("the publication must stop at the requested boundary");
      }
      return new PendingTransaction(this, id, invokedAt, key, expectedValue,
          immutableMutations(mutations), attempt.transactionId(), requestScope, handoff, result);
    }

    private HistoryOperation compareAndWrite(String key, Optional<String> expectedValue,
        Map<String, Mutation> mutations) {
      int invokedAt = clock.next();
      boolean matches = readValue(key).equals(expectedValue);
      int returnedAt = clock.next();
      if (matches) {
        throw new IllegalStateException("this bounded driver expects the stale comparison to fail");
      }
      return new HistoryOperation(nextOperationId++, Kind.COMPARE_AND_WRITE, invokedAt, returnedAt,
          OptionalInt.empty(), key, expectedValue, mutations, Optional.empty(),
          Outcome.COMPARE_FAILED, PublicationObservation.NOT_SELECTED);
    }

    private HistoryOperation delete(String key) {
      return publishAndFinalize(Kind.DELETE, key, Mutation.delete());
    }

    private HistoryOperation put(String key, String value) {
      return publishAndFinalize(Kind.PUT, key, Mutation.put(value));
    }

    private HistoryOperation publishAndFinalize(Kind kind, String key, Mutation mutation) {
      int invokedAt = clock.next();
      try (RequestScope requestScope = RequestScope.withoutDeadline()) {
        PublicationAttempt attempt = publish(Map.of(key, mutation), requestScope, new TestHandoff());
        CommitOutcomeResolver.Resolution result = attempt.result().join();
        int returnedAt = clock.next();
        if (result.state() != CommitOutcomeResolver.State.SELECTED_FINALIZED) {
          throw new IllegalStateException("the fixture write did not finish finalization");
        }
        return HistoryOperation.mutation(nextOperationId++, kind, invokedAt, returnedAt,
            key, mutation, OptionalInt.of(control.decisionAt(attempt.transactionId())));
      }
    }

    private PublicationAttempt publish(
        Map<String, Mutation> mutations, RequestScope requestScope, TestHandoff handoff) {
      CommitState base = control.state;
      if (base.publication().isPresent()) {
        throw new IllegalStateException("publication must be finalized first");
      }
      TransactionId transaction = transactionId(nextIdentity);
      InvocationId invocationId = invocationId(nextIdentity++);
      List<PreparedCandidate> candidates = mutations.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .map(entry -> new PreparedCandidate(transaction, base.ownerEpoch(),
              base.publishedSequence(), base.publishedRevision(),
              record(entry.getKey(), entry.getValue().value(),
                  base.publishedRevision() + 1, physical.latest(key(entry.getKey())))))
          .toList();
      List<ManifestCandidateReference> references = candidates.stream()
          .map(candidate -> ManifestCandidateReference.from(candidate, 0)).toList();
      ManifestPage page = ManifestPage.create(transaction, 0, references);
      long totalValueBytes = candidates.stream()
          .mapToLong(candidate -> candidate.record().metadata().valueLength()).sum();
      ManifestRoot root = ManifestRoot.create(transaction, base.ownerEpoch(),
          base.publishedSequence(), base.publishedRevision(), candidates.size(), totalValueBytes,
          Optional.of(page.reference()));
      manifests.add(root, page, candidates);
      InvocationBinding binding = InvocationBinding.begin(invocationId, PROFILE, PROFILE,
          transaction, base.ownerEpoch(), base.publishedSequence(), base.publishedRevision())
          .seal(root.systemIdentity());
      if (invocations.createIfAbsent(binding, requestScope).join()
          != SystemRecordStore.CreateResult.CREATED) {
        throw new IllegalStateException("fixture invocation identity is not unique");
      }
      CommitPublisher publisher = new CommitPublisher(control, outcomes,
          new CommitPlanVerifier(catalog, materialization), invocations, handoff, finalizer);
      CommitOwnerService.OwnerToken owner = new CommitOwnerService.OwnerToken(
          base.ownerInstance().orElseThrow(), base.ownerEpoch());
      CommitAuthorizationProof authorization = CommitAuthorizationProof.capture(PROFILE,
          base.authorizationBarrierGeneration(),
          (proofIdentity, barrierGeneration, scope) -> CompletableFuture.completedFuture(null));
      return new PublicationAttempt(transaction,
          publisher.publish(binding, owner, root, requestScope, authorization));
    }

    private HistoryOperation read(String key) {
      int invokedAt = clock.next();
      Optional<String> value = readValue(key);
      int returnedAt = clock.next();
      return new HistoryOperation(nextOperationId++, Kind.READ, invokedAt, returnedAt,
          OptionalInt.empty(), key, Optional.empty(), Map.of(), value,
          Outcome.COMPLETE, PublicationObservation.NOT_SELECTED);
    }

    private Optional<String> readValue(String key) {
      try (RequestScope scope = RequestScope.withoutDeadline()) {
        PinnedReadView pinned = views.open(OptionalLong.empty(), PROFILE, scope).join();
        var reservation = new BudgetManager.Reservation(8192, 8192, 8192, 8192, 8192);
        try (BudgetManager.Account account = budgets.admit("public", scope, reservation);
            ReadViewRecordStore view = new ReadViewRecordStore(
                pinned, physical, catalog, ignored -> 0, 1, scope)) {
          return logicalValue(view.get(key(key), pinned.view().targetRevision(),
              ReadProjection.FULL, account).join());
        }
      } catch (BackendFailure failure) {
        throw new CompletionException(failure);
      }
    }

    private Optional<String> physicalValue(String key) {
      return logicalValue(physical.latest(key(key)));
    }

    private RecoveryManager.State recoverSelectedPublication() {
      try (RequestScope scope = RequestScope.withoutDeadline()) {
        return recovery.recover("recovery-owner", 200, 500, scope).join().state();
      }
    }
  }

  private record PublicationAttempt(
      TransactionId transactionId, CompletableFuture<CommitOutcomeResolver.Resolution> result) {}

  private static final class PendingTransaction implements AutoCloseable {
    private final KernelDriver driver;
    private final int id;
    private final int invokedAt;
    private final String key;
    private final Optional<String> expectedValue;
    private final Map<String, Mutation> mutations;
    private final TransactionId transactionId;
    private final RequestScope requestScope;
    private final TestHandoff handoff;
    private final CompletableFuture<CommitOutcomeResolver.Resolution> result;

    private PendingTransaction(KernelDriver driver, int id, int invokedAt, String key,
        Optional<String> expectedValue, Map<String, Mutation> mutations, TransactionId transactionId,
        RequestScope requestScope, TestHandoff handoff,
        CompletableFuture<CommitOutcomeResolver.Resolution> result) {
      this.driver = driver;
      this.id = id;
      this.invokedAt = invokedAt;
      this.key = key;
      this.expectedValue = expectedValue;
      this.mutations = mutations;
      this.transactionId = transactionId;
      this.requestScope = requestScope;
      this.handoff = handoff;
      this.result = result;
    }

    private HistoryOperation cancelCallerAfterSelection() {
      if (driver.materialization.appliedCount() != 1 || driver.control.state.publication().isEmpty()) {
        throw new IllegalStateException("cancellation requires a partially materialized selection");
      }
      int returnedAt = cancelCaller();
      return selectedHistory(returnedAt);
    }

    private int cancelCallerBeforeSelection() {
      if (driver.control.state.publication().isPresent() || driver.control.hasDecision(transactionId)) {
        throw new IllegalStateException("cancellation must precede the publication decision");
      }
      return cancelCaller();
    }

    private int cancelCaller() {
      requestScope.cancel();
      Throwable failure = assertThrows(CompletionException.class, result::join).getCause();
      assertTrue(failure instanceof CancellationException);
      assertTrue(requestScope.isCancelled());
      assertFalse(handoff.serviceScope.isClosed());
      return driver.clock.next();
    }

    private HistoryOperation selectedAfterReturn(int returnedAt) {
      if (driver.control.decisionAt(transactionId) <= returnedAt) {
        throw new IllegalStateException("publication must be decided after the caller returned");
      }
      return selectedHistory(returnedAt);
    }

    private HistoryOperation selectedHistory(int returnedAt) {
      // Cancellation is the observed caller result; the control CAS separately proves selection.
      return new HistoryOperation(id, Kind.COMPARE_AND_WRITE, invokedAt, returnedAt,
          OptionalInt.of(driver.control.decisionAt(transactionId)), key, expectedValue, mutations, Optional.empty(),
          Outcome.UNKNOWN, PublicationObservation.SELECTED);
    }

    private void stopServiceForRecovery() {
      handoff.stopService();
    }

    @Override public void close() {
      requestScope.close();
      handoff.closeService();
    }
  }

  /** Test adapter: the request result is cancellable, while accepted work is service-owned. */
  private static final class TestHandoff implements PublicationHandoff {
    private RequestScope serviceScope;

    @Override
    public <T> CompletableFuture<T> transfer(InvocationBinding invocation, RequestScope requestScope,
        Function<RequestScope, CompletableFuture<T>> work) {
      serviceScope = RequestScope.withoutDeadline();
      CompletableFuture<T> result = requestScope.track(new CompletableFuture<>());
      work.apply(serviceScope).whenComplete((value, failure) -> {
        if (failure == null) result.complete(value);
        else result.completeExceptionally(failure);
        serviceScope.close();
      });
      return result;
    }

    private void stopService() {
      if (serviceScope == null || !serviceScope.cancel()) {
        throw new IllegalStateException("the in-flight service scope was not stopped");
      }
    }

    private void closeService() {
      if (serviceScope != null) serviceScope.close();
    }
  }

  private static final class EventClock {
    private int next;
    private int next() { return next++; }
  }

  private static final class MemoryControl implements CommitControlStore {
    private CommitState state;
    private final EventClock clock;
    private int version;
    private final Map<TransactionId, Integer> decisionTimes = new HashMap<>();
    private CompletableFuture<Void> publicationGate = CompletableFuture.completedFuture(null);

    private MemoryControl(CommitState state, EventClock clock) {
      this.state = state;
      this.clock = clock;
    }

    @Override public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(new ControlSnapshot(state, token())));
    }

    @Override public CompletableFuture<Boolean> initializeIfAbsent(
        CommitState initialState, RequestScope scope) {
      return CompletableFuture.completedFuture(false);
    }

    @Override public CompletableFuture<Boolean> compareAndSet(
        ControlSnapshot observed, CommitState replacement, RequestScope scope) {
      if (state.publication().isEmpty() && replacement.publication().isPresent()
          && !publicationGate.isDone()) {
        CompletableFuture<Void> gate = publicationGate;
        scope.registerCleanup(() -> gate.completeExceptionally(
            new BackendFailure(BackendFailure.Category.CANCELLED)));
        return gate.thenCompose(ignored -> compareAndSet(observed, replacement, scope));
      }
      if (!Arrays.equals(observed.comparisonToken(), token())) {
        return CompletableFuture.completedFuture(false);
      }
      Optional<TransactionId> selected = state.publication().isEmpty()
          ? replacement.publication().map(PublicationSlot::transactionId) : Optional.empty();
      if (selected.filter(decisionTimes::containsKey).isPresent()) {
        throw new IllegalStateException("transaction already has a publication decision");
      }
      state = replacement;
      version++;
      selected.ifPresent(transaction -> decisionTimes.put(transaction, clock.next()));
      return CompletableFuture.completedFuture(true);
    }

    private boolean hasDecision(TransactionId transaction) {
      return decisionTimes.containsKey(transaction);
    }

    private int decisionAt(TransactionId transaction) {
      Integer decisionAt = decisionTimes.get(transaction);
      if (decisionAt == null) throw new IllegalStateException("publication decision is missing");
      return decisionAt;
    }

    private void deferPublication() {
      if (!publicationGate.isDone()) throw new IllegalStateException("publication is already deferred");
      publicationGate = new CompletableFuture<>();
    }

    private void releasePublication() {
      if (!publicationGate.complete(null)) throw new IllegalStateException("publication was not deferred");
    }

    private byte[] token() { return ByteBuffer.allocate(Integer.BYTES).putInt(version).array(); }
  }

  private static final class MemoryInvocations implements InvocationRepository {
    private final Map<InvocationId, InvocationBinding> bindings = new HashMap<>();

    @Override public CompletableFuture<Optional<InvocationBinding>> read(
        InvocationId id, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(bindings.get(id)));
    }

    @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        InvocationBinding value, RequestScope scope) {
      InvocationBinding previous = bindings.putIfAbsent(value.invocationId(), value);
      return CompletableFuture.completedFuture(previous == null
          ? SystemRecordStore.CreateResult.CREATED : SystemRecordStore.CreateResult.CONFLICT);
    }

    @Override public CompletableFuture<Boolean> compareAndSet(
        InvocationBinding expected, InvocationBinding replacement, RequestScope scope) {
      return CompletableFuture.completedFuture(bindings.replace(
          expected.invocationId(), expected, replacement));
    }
  }

  private static final class MemoryDecisions implements CommitDecisionRepository {
    private final Map<Long, CommitDecision> values = new HashMap<>();

    @Override public CompletableFuture<Optional<CommitDecision>> read(long sequence, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(values.get(sequence)));
    }

    @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
        CommitDecision decision, RequestScope scope) {
      CommitDecision previous = values.putIfAbsent(decision.sequence(), decision);
      return CompletableFuture.completedFuture(previous == null
          ? SystemRecordStore.CreateResult.CREATED
          : previous.equals(decision) ? SystemRecordStore.CreateResult.ALREADY_PRESENT
              : SystemRecordStore.CreateResult.CONFLICT);
    }
  }

  private static final class MemoryManifests implements ManifestRepository {
    private final Map<ByteKey, ManifestRoot> roots = new HashMap<>();
    private final Map<ByteKey, ManifestPage> pages = new HashMap<>();
    private final Map<ByteKey, PreparedCandidate> candidates = new HashMap<>();
    private ByteKey missingKey;

    private void add(ManifestRoot root, ManifestPage page, List<PreparedCandidate> values) {
      roots.put(root.systemIdentity(), root);
      pages.put(page.systemIdentity(), page);
      values.forEach(candidate -> candidates.put(candidate.systemIdentity(), candidate));
    }

    @Override public int candidateBucket(PreparedCandidate candidate) { return 0; }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createCandidate(
        PreparedCandidate candidate, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(
        ManifestPage page, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(
        ManifestDirectoryPage page, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEvent(
        PreparedEvent event, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventPage(
        EventPage page, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(
        EventDirectoryPage page, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> seal(
        ManifestRoot root, RequestScope scope) { return unsupported(); }
    @Override public CompletableFuture<Optional<ManifestRoot>> readRoot(ByteKey id, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(roots.get(id)));
    }
    @Override public CompletableFuture<Optional<PreparedCandidate>> readCandidate(
        ByteKey id, RequestScope scope) {
      PreparedCandidate candidate = candidates.get(id);
      return CompletableFuture.completedFuture(candidate != null
          && !candidate.record().key().equals(missingKey) ? Optional.of(candidate) : Optional.empty());
    }
    @Override public CompletableFuture<Optional<ManifestPage>> readPage(ByteKey id, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(pages.get(id)));
    }
    @Override public CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(
        ByteKey id, RequestScope scope) { return empty(); }
    @Override public CompletableFuture<Optional<PreparedEvent>> readEvent(
        ByteKey id, RequestScope scope) { return empty(); }
    @Override public CompletableFuture<Optional<EventPage>> readEventPage(
        ByteKey id, RequestScope scope) { return empty(); }
    @Override public CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(
        ByteKey id, RequestScope scope) { return empty(); }

    private static <T> CompletableFuture<T> unsupported() {
      return CompletableFuture.failedFuture(new UnsupportedOperationException("unexpected manifest write"));
    }
    private static <T> CompletableFuture<Optional<T>> empty() {
      return CompletableFuture.completedFuture(Optional.empty());
    }
  }

  private static final class MemoryRecords implements RecordStore {
    private final Map<ByteKey, List<StoredRecord>> versions = new HashMap<>();

    private void add(StoredRecord record) {
      versions.computeIfAbsent(record.key(), ignored -> new ArrayList<>()).add(record);
    }

    private Optional<StoredRecord> latest(ByteKey key) {
      return latestAt(key, Long.MAX_VALUE);
    }

    private Optional<StoredRecord> latestAt(ByteKey key, long upper) {
      return versions.getOrDefault(key, List.of()).stream()
          .filter(record -> record.metadata().modificationRevision() <= upper)
          .max(Comparator.comparingLong(record -> record.metadata().modificationRevision()));
    }

    @Override public CompletableFuture<Optional<StoredRecord>> get(
        ByteKey key, long upper, ReadProjection projection) {
      return CompletableFuture.completedFuture(latestAt(key, upper).map(record ->
          projection == ReadProjection.METADATA && record.hasValue()
              ? StoredRecord.metadataOnly(record.key(), record.metadata()) : record));
    }
    @Override public ScanOperation startScan(ScanRequest request) {
      throw new UnsupportedOperationException("the bounded history performs point reads only");
    }
    @Override public CompletableFuture<Void> put(StoredRecord record) { return unsupported(); }
    @Override public CompletableFuture<Boolean> compareAndSet(
        ByteKey key, Optional<StorageMetadata> expected, StoredRecord replacement) {
      return unsupported();
    }
    @Override public CompletableFuture<List<BatchWriteResult>> putBatch(List<StoredRecord> records) {
      return unsupported();
    }
    private static <T> CompletableFuture<T> unsupported() {
      return CompletableFuture.failedFuture(new UnsupportedOperationException("unexpected record write"));
    }
  }

  private static final class MemoryMaterialization implements MaterializationStore {
    private final MemoryRecords physical;
    private final Map<ByteKey, Long> appliedSequence = new HashMap<>();
    private boolean blockAfterOne;
    private int newSinceBlock;
    private int appliedCount;

    private MemoryMaterialization(MemoryRecords physical) { this.physical = physical; }
    private int appliedCount() { return appliedCount; }

    private void blockAfterOneNewCandidate() {
      blockAfterOne = true;
      newSinceBlock = 0;
    }

    @Override public CompletableFuture<Void> preflight(PreparedCandidate candidate, RequestScope scope) {
      return CompletableFuture.completedFuture(null);
    }

    @Override public CompletableFuture<ApplyResult> applySelected(
        CommitDecision decision, PreparedCandidate candidate, RequestScope scope) {
      ByteKey key = candidate.record().key();
      Long previous = appliedSequence.get(key);
      if (previous != null && previous == decision.sequence()) {
        return CompletableFuture.completedFuture(ApplyResult.ALREADY_APPLIED);
      }
      if (previous != null && previous > decision.sequence()) {
        return CompletableFuture.completedFuture(ApplyResult.CURRENT_SUPERSEDED);
      }
      if (blockAfterOne && newSinceBlock == 1) {
        blockAfterOne = false;
        CompletableFuture<ApplyResult> pending = new CompletableFuture<>();
        scope.registerCleanup(() -> pending.completeExceptionally(
            new BackendFailure(BackendFailure.Category.CANCELLED)));
        return pending;
      }
      physical.add(candidate.record());
      appliedSequence.put(key, decision.sequence());
      appliedCount++;
      if (blockAfterOne) newSinceBlock++;
      return CompletableFuture.completedFuture(ApplyResult.APPLIED);
    }
  }

  private static StoredRecord record(String key, Optional<String> value, long revision,
      Optional<StoredRecord> prior) {
    boolean wasLive = prior.filter(record ->
        record.metadata().state() == StorageMetadata.State.LIVE).isPresent();
    long createRevision = wasLive ? prior.orElseThrow().metadata().createRevision() : revision;
    long version = wasLive ? prior.orElseThrow().metadata().version() + 1 : 1;
    byte[] bytes = value.map(text -> text.getBytes(StandardCharsets.UTF_8)).orElseGet(() -> new byte[0]);
    StorageMetadata metadata = new StorageMetadata(createRevision, revision, version, 0,
        value.isPresent() ? StorageMetadata.State.LIVE : StorageMetadata.State.TOMBSTONE, bytes.length);
    return value.isPresent() ? StoredRecord.live(key(key), metadata, bytes)
        : StoredRecord.metadataOnly(key(key), metadata);
  }

  private static Optional<String> logicalValue(Optional<StoredRecord> record) {
    return record.filter(value -> value.metadata().state() == StorageMetadata.State.LIVE)
        .map(value -> new String(value.value().orElseThrow(), StandardCharsets.UTF_8));
  }

  private static ByteKey key(String value) {
    return ByteKey.copyOf(value.getBytes(StandardCharsets.UTF_8));
  }

  private static TransactionId transactionId(int value) {
    return new TransactionId(new UUID(0, value));
  }

  private static InvocationId invocationId(int value) {
    return new InvocationId(new UUID(1, value));
  }
}
