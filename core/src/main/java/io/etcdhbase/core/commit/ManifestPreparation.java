package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.concurrent.ContinuationExecutor;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/** Sequential, bounded writer for private candidates, ordered events, and Merkle directories. */
public final class ManifestPreparation {
  private static final ContinuationExecutor CONTINUATION_EXECUTOR = new ContinuationExecutor();

  public record Limits(long maxCandidates, long maxValueBytes, long maxEvents, long maxEventBytes) {
    public Limits(long maxCandidates, long maxValueBytes) {
      this(maxCandidates, maxValueBytes, ManifestRoot.MAX_EVENTS, ManifestRoot.MAX_EVENT_BYTES);
    }

    public Limits {
      if (maxCandidates < 0 || maxCandidates > ManifestRoot.MAX_CANDIDATES
          || maxValueBytes < 0 || maxValueBytes > ManifestRoot.MAX_VALUE_BYTES
          || maxEvents < 0 || maxEvents > ManifestRoot.MAX_EVENTS
          || maxEventBytes < 0 || maxEventBytes > ManifestRoot.MAX_EVENT_BYTES) {
        throw new IllegalArgumentException("manifest limits exceed the storage contract");
      }
    }
  }

  private final ManifestRepository repository;
  private final Limits limits;

  public ManifestPreparation(ManifestRepository repository, Limits limits) {
    this.repository = Objects.requireNonNull(repository, "repository");
    this.limits = Objects.requireNonNull(limits, "limits");
  }

  public CompletableFuture<ManifestRoot> prepare(
      TransactionId transactionId,
      long ownerEpoch,
      long baseSequence,
      long baseRevision,
      Iterator<PreparedCandidate> candidates,
      RequestScope scope) {
    return prepare(transactionId, ownerEpoch, baseSequence, baseRevision, candidates,
        java.util.Collections.emptyIterator(), scope);
  }

  public CompletableFuture<ManifestRoot> prepare(
      TransactionId transactionId,
      long ownerEpoch,
      long baseSequence,
      long baseRevision,
      Iterator<PreparedCandidate> candidates,
      Iterator<PreparedEvent> events,
      RequestScope scope) {
    Objects.requireNonNull(candidates, "candidates");
    Objects.requireNonNull(events, "events");
    if (ownerEpoch <= 0 || baseSequence < 0 || baseRevision <= 0) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid manifest base"));
    }
    return new Writer(transactionId, ownerEpoch, baseSequence, baseRevision,
        AsyncCursor.fromIterator(candidates), AsyncCursor.fromIterator(events),
        Optional.empty(), null, Optional.empty(),
        () -> CompletableFuture.completedFuture(null), scope).start().copy();
  }

  /** Prepares a business manifest only after its result belongs to this invocation. */
  public CompletableFuture<ManifestRoot> prepareWithResult(InvocationBinding invocation,
      Iterator<PreparedCandidate> candidates, Iterator<PreparedEvent> events,
      ResultReference result, ResultCatalog results,
      Supplier<CompletableFuture<Void>> beforeSeal, RequestScope scope) {
    return prepareWithResult(invocation, AsyncCursor.fromIterator(candidates), AsyncCursor.fromIterator(events),
        result, results, beforeSeal, scope);
  }

  /** Streams bounded private records into a manifest without retaining the entire transaction. */
  public CompletableFuture<ManifestRoot> prepareWithResult(InvocationBinding invocation,
      AsyncCursor<PreparedCandidate> candidates, AsyncCursor<PreparedEvent> events,
      ResultReference result, ResultCatalog results,
      Supplier<CompletableFuture<Void>> beforeSeal, RequestScope scope) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(candidates, "candidates");
    Objects.requireNonNull(events, "events");
    Objects.requireNonNull(result, "result");
    Objects.requireNonNull(results, "results");
    Objects.requireNonNull(beforeSeal, "beforeSeal");
    if (invocation.state() != InvocationBinding.State.PREPARING
        || !result.identity().matches(invocation)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "result does not belong to the preparing invocation"));
    }
    return new Writer(invocation.transactionId(), invocation.preparedOwnerEpoch(),
        invocation.baseSequence(), invocation.baseRevision(), candidates, events,
        Optional.of(result), results, Optional.empty(), beforeSeal, scope).start().copy();
  }

  /** Prepares an auxiliary-only M2 transaction such as Grant or KeepAlive. */
  public CompletableFuture<ManifestRoot> prepareWithAuxiliary(InvocationBinding invocation,
      Iterator<PreparedAuxiliaryRecord> records,
      Supplier<CompletableFuture<Void>> beforeSeal, RequestScope scope) {
    Objects.requireNonNull(records, "records");
    return prepareWithAuxiliary(invocation, AsyncCursor.fromIterator(records), beforeSeal, scope);
  }

  /** Streams auxiliary updates without retaining the full transaction in memory. */
  public CompletableFuture<ManifestRoot> prepareWithAuxiliary(InvocationBinding invocation,
      AsyncCursor<PreparedAuxiliaryRecord> records,
      Supplier<CompletableFuture<Void>> beforeSeal, RequestScope scope) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(records, "records");
    Objects.requireNonNull(beforeSeal, "beforeSeal");
    if (invocation.state() != InvocationBinding.State.PREPARING) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "only a preparing invocation can seal auxiliary records"));
    }
    return new Writer(invocation.transactionId(), invocation.preparedOwnerEpoch(),
        invocation.baseSequence(), invocation.baseRevision(),
        AsyncCursor.fromIterator(java.util.Collections.<PreparedCandidate>emptyIterator()),
        AsyncCursor.fromIterator(java.util.Collections.<PreparedEvent>emptyIterator()),
        Optional.empty(), null, Optional.of(records), beforeSeal, scope).start().copy();
  }

  /** Seals a non-KV RPC result and its auxiliary state under one M2 publication decision. */
  public CompletableFuture<ManifestRoot> prepareWithResultAndAuxiliary(
      InvocationBinding invocation, Iterator<PreparedCandidate> candidates,
      Iterator<PreparedEvent> events, ResultReference result, ResultCatalog results,
      Iterator<PreparedAuxiliaryRecord> auxiliaryRecords,
      Supplier<CompletableFuture<Void>> beforeSeal, RequestScope scope) {
    return prepareWithResultAndAuxiliary(invocation, AsyncCursor.fromIterator(candidates),
        AsyncCursor.fromIterator(events), result, results,
        AsyncCursor.fromIterator(auxiliaryRecords), beforeSeal, scope);
  }

  /** Streams KV, event, result, and auxiliary records into one immutable publication plan. */
  public CompletableFuture<ManifestRoot> prepareWithResultAndAuxiliary(
      InvocationBinding invocation, AsyncCursor<PreparedCandidate> candidates,
      AsyncCursor<PreparedEvent> events, ResultReference result, ResultCatalog results,
      AsyncCursor<PreparedAuxiliaryRecord> auxiliaryRecords,
      Supplier<CompletableFuture<Void>> beforeSeal, RequestScope scope) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(candidates, "candidates");
    Objects.requireNonNull(events, "events");
    Objects.requireNonNull(result, "result");
    Objects.requireNonNull(results, "results");
    Objects.requireNonNull(auxiliaryRecords, "auxiliaryRecords");
    Objects.requireNonNull(beforeSeal, "beforeSeal");
    if (invocation.state() != InvocationBinding.State.PREPARING
        || !result.identity().matches(invocation)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "result does not belong to the preparing invocation"));
    }
    return new Writer(invocation.transactionId(), invocation.preparedOwnerEpoch(),
        invocation.baseSequence(), invocation.baseRevision(), candidates, events,
        Optional.of(result), results, Optional.of(auxiliaryRecords), beforeSeal, scope)
        .start().copy();
  }

  private final class Writer {
    private final TransactionId transactionId;
    private final long ownerEpoch;
    private final long baseSequence;
    private final long baseRevision;
    private final AsyncCursor<PreparedCandidate> input;
    private final AsyncCursor<PreparedEvent> eventInput;
    private final RequestScope scope;
    private final Optional<ResultReference> result;
    private final ResultCatalog results;
    private final Optional<AsyncCursor<PreparedAuxiliaryRecord>> auxiliaryInput;
    private final Supplier<CompletableFuture<Void>> beforeSeal;
    private final CompletableFuture<ManifestRoot> completion = new CompletableFuture<>();
    private final List<ManifestCandidateReference> leaf = new ArrayList<>(ManifestPage.MAX_ENTRIES);
    private final List<List<ManifestChildReference>> levels = new ArrayList<>(ManifestDirectoryPage.MAX_DEPTH + 1);
    private long count;
    private long valueBytes;
    private int leafPageIndex;
    private int directoryPageIndex;
    private int leafBytes = 64;
    private int leafBucket = -1;
    private int previousBucket = -1;
    private ByteKey previousKey;

    private Writer(TransactionId transactionId, long ownerEpoch, long baseSequence, long baseRevision,
        AsyncCursor<PreparedCandidate> input, AsyncCursor<PreparedEvent> eventInput,
        Optional<ResultReference> result, ResultCatalog results,
        Optional<AsyncCursor<PreparedAuxiliaryRecord>> auxiliaryInput,
        Supplier<CompletableFuture<Void>> beforeSeal, RequestScope scope) {
      this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
      this.ownerEpoch = ownerEpoch;
      this.baseSequence = baseSequence;
      this.baseRevision = baseRevision;
      this.input = input;
      this.eventInput = eventInput;
      this.result = result;
      this.results = results;
      this.auxiliaryInput = Objects.requireNonNull(auxiliaryInput, "auxiliaryInput");
      this.beforeSeal = beforeSeal;
      this.scope = Objects.requireNonNull(scope, "scope");
      for (int level = 0; level <= ManifestDirectoryPage.MAX_DEPTH; level++) {
        levels.add(new ArrayList<>(ManifestDirectoryPage.MAX_CHILDREN));
      }
    }

    private CompletableFuture<ManifestRoot> start() {
      next();
      return completion;
    }

    private void next() {
      try {
        if (scope.isDeadlineExceeded()) throw new CompletionException(new BackendFailure(BackendFailure.Category.TIMEOUT));
        if (scope.isClosed()) throw new CompletionException(new BackendFailure(BackendFailure.Category.CANCELLED));
        Objects.requireNonNull(input.next(), "candidate cursor future")
            .whenCompleteAsync(this::acceptInput, CONTINUATION_EXECUTOR);
      } catch (RuntimeException failure) {
        completion.completeExceptionally(failure);
      } catch (Error fatal) {
        completion.completeExceptionally(fatal);
        throw fatal;
      }
    }

    private void acceptInput(Optional<PreparedCandidate> item, Throwable failure) {
      if (failure != null) {
        completion.completeExceptionally(failure);
        return;
      }
      try {
        if (scope.isDeadlineExceeded()) throw new CompletionException(new BackendFailure(BackendFailure.Category.TIMEOUT));
        if (scope.isClosed()) throw new CompletionException(new BackendFailure(BackendFailure.Category.CANCELLED));
        Optional<PreparedCandidate> found = Objects.requireNonNull(item, "candidate cursor result");
        if (found.isEmpty()) {
          flushLeaf().thenCompose(ignored -> finishDirectories()).whenComplete((root, problem) -> {
            if (problem == null) completion.complete(root);
            else completion.completeExceptionally(problem);
          });
          return;
        }
        PreparedCandidate candidate = found.orElseThrow();
        int bucket = repository.candidateBucket(candidate);
        validateCandidate(candidate, bucket);
        int bytes = referenceBytes(candidate);
        CompletableFuture<Void> ready = !leaf.isEmpty() && (leaf.size() == ManifestPage.MAX_ENTRIES
            || leafBucket != bucket || leafBytes + bytes > ManifestPage.MAX_PAGE_BYTES)
            ? flushLeaf() : CompletableFuture.completedFuture(null);
        ready.thenCompose(ignored -> accept(candidate, bucket)).whenCompleteAsync((ignored, problem) -> {
          if (problem == null) next();
          else completion.completeExceptionally(problem);
        }, CONTINUATION_EXECUTOR);
      } catch (RuntimeException problem) {
        completion.completeExceptionally(problem);
      } catch (Error fatal) {
        completion.completeExceptionally(fatal);
        throw fatal;
      }
    }

    private CompletableFuture<Void> accept(PreparedCandidate candidate, int bucket) {
      checkCandidateBudget(candidate);
      return repository.createCandidate(candidate, scope)
          .thenAccept(result -> {
        requireCreatedOrIdentical(result, "candidate");
        ManifestCandidateReference reference = ManifestCandidateReference.from(candidate, bucket);
        leaf.add(reference);
        leafBytes = Math.addExact(leafBytes, referenceBytes(candidate));
        leafBucket = bucket;
        count = Math.addExact(count, 1);
        valueBytes = Math.addExact(valueBytes, reference.valueLength());
        previousBucket = bucket;
        previousKey = candidate.record().key();
      });
    }

    private CompletableFuture<Void> flushLeaf() {
      if (leaf.isEmpty()) return CompletableFuture.completedFuture(null);
      ManifestPage page = ManifestPage.create(transactionId, leafPageIndex++, List.copyOf(leaf));
      return repository.createPage(page, scope).thenCompose(result -> {
        requireCreatedOrIdentical(result, "manifest page");
        leaf.clear();
        leafBytes = 64;
        leafBucket = -1;
        return addReference(page.reference(), 0);
      });
    }

    private CompletableFuture<Void> addReference(ManifestChildReference reference, int level) {
      if (level > ManifestDirectoryPage.MAX_DEPTH) {
        return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      List<ManifestChildReference> buffer = levels.get(level);
      buffer.add(reference);
      if (buffer.size() < ManifestDirectoryPage.MAX_CHILDREN) {
        return CompletableFuture.completedFuture(null);
      }
      return writeDirectory(level, buffer).thenCompose(parent -> addReference(parent.reference(), level + 1));
    }

    private CompletableFuture<ManifestDirectoryPage> writeDirectory(
        int childLevel, List<ManifestChildReference> references) {
      if (childLevel >= ManifestDirectoryPage.MAX_DEPTH) {
        return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      ManifestDirectoryPage page = ManifestDirectoryPage.create(transactionId,
          directoryPageIndex++, childLevel + 1, List.copyOf(references));
      return repository.createDirectoryPage(page, scope).thenApply(result -> {
        requireCreatedOrIdentical(result, "manifest directory page");
        references.clear();
        return page;
      });
    }

    private CompletableFuture<ManifestRoot> finishDirectories() {
      return finishLevel(0).thenCompose(candidateRoot ->
          new EventWriter(transactionId, ownerEpoch, baseSequence, baseRevision, eventInput, scope)
              .write().thenCompose(events -> prepareAuxiliary().thenCompose(auxiliary -> {
        ManifestRoot root = createRoot(candidateRoot, events, auxiliary);
        CompletableFuture<?> verified = result.isPresent()
            ? results.verify(result.orElseThrow(), scope) : CompletableFuture.completedFuture(null);
        return verified.thenCompose(ignored -> validateBeforeSeal())
            .thenCompose(ignored -> repository.seal(root, scope)).thenApply(outcome -> {
              requireCreatedOrIdentical(outcome, "sealed manifest root");
              return root;
            });
      })));
    }

    private CompletableFuture<Optional<AuxiliaryReference>> prepareAuxiliary() {
      if (auxiliaryInput.isEmpty()) return CompletableFuture.completedFuture(Optional.empty());
      return new AuxiliaryManifestPreparation(repository).prepare(transactionId, ownerEpoch,
          baseSequence, auxiliaryInput.orElseThrow(), scope).thenApply(Optional::of);
    }

    private ManifestRoot createRoot(ManifestChildReference candidateRoot, EventSummary events,
        Optional<AuxiliaryReference> auxiliary) {
      Optional<ManifestChildReference> candidates = Optional.ofNullable(candidateRoot);
      Optional<EventReference> eventDirectory = Optional.ofNullable(events.root());
      if (auxiliary.isPresent()) {
        if (result.isPresent()) {
          return ManifestRoot.createWithResultAndAuxiliary(transactionId, ownerEpoch,
              baseSequence, baseRevision, count, valueBytes, candidates, events.count(),
              events.bytes(), eventDirectory, result.orElseThrow(), auxiliary.orElseThrow());
        }
        return ManifestRoot.createWithAuxiliary(transactionId, ownerEpoch, baseSequence,
            baseRevision, count, valueBytes, candidates, events.count(), events.bytes(),
            eventDirectory, auxiliary);
      }
      if (result.isPresent()) {
        return ManifestRoot.createWithResult(transactionId, ownerEpoch, baseSequence,
            baseRevision, count, valueBytes, candidates, events.count(), events.bytes(),
            eventDirectory, result.orElseThrow());
      }
      return ManifestRoot.create(transactionId, ownerEpoch, baseSequence, baseRevision,
          count, valueBytes, candidates, events.count(), events.bytes(), eventDirectory);
    }

    private CompletableFuture<Void> validateBeforeSeal() {
      var started = new java.util.concurrent.atomic.AtomicReference<CompletableFuture<Void>>();
      if (!scope.startIfOpen(() -> started.set(
          Objects.requireNonNull(beforeSeal.get(), "pre-seal validation future")))) {
        return failed(new BackendFailure(scope.isDeadlineExceeded()
            ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
      }
      return started.get().thenCompose(ignored -> {
        if (scope.isClosed() || scope.isDeadlineExceeded()) {
          return failed(new BackendFailure(scope.isDeadlineExceeded()
              ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
        }
        return CompletableFuture.completedFuture(null);
      });
    }

    private CompletableFuture<ManifestChildReference> finishLevel(int level) {
      if (level > ManifestDirectoryPage.MAX_DEPTH) {
        boolean anyRemaining = levels.stream().anyMatch(buffer -> !buffer.isEmpty());
        return anyRemaining
            ? failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT))
            : CompletableFuture.completedFuture(null);
      }
      List<ManifestChildReference> current = levels.get(level);
      if (current.isEmpty()) return finishLevel(level + 1);
      boolean higherHasEntries = false;
      for (int index = level + 1; index < levels.size(); index++) {
        higherHasEntries |= !levels.get(index).isEmpty();
      }
      if (current.size() == 1 && !higherHasEntries) {
        return CompletableFuture.completedFuture(current.getFirst());
      }
      return writeDirectory(level, current).thenCompose(page -> {
        if (level + 1 == levels.size()) {
          return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
        }
        levels.get(level + 1).add(page.reference());
        return finishLevel(level + 1);
      });
    }

    private void validateCandidate(PreparedCandidate candidate, int bucket) {
      if (!candidate.transactionId().equals(transactionId) || candidate.ownerEpoch() != ownerEpoch
          || candidate.baseSequence() != baseSequence || candidate.baseRevision() != baseRevision
          || bucket < 0) {
        throw new IllegalArgumentException("candidate belongs to a different commit attempt");
      }
      if (previousBucket > bucket || previousBucket == bucket && previousKey != null
          && previousKey.compareTo(candidate.record().key()) >= 0) {
        throw new IllegalArgumentException("candidates must be sorted by bucket and logical key");
      }
    }

    private void checkCandidateBudget(PreparedCandidate candidate) {
      long nextCount;
      long nextValueBytes;
      try {
        nextCount = Math.addExact(count, 1);
        nextValueBytes = Math.addExact(valueBytes, candidate.record().metadata().valueLength());
      } catch (ArithmeticException overflow) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      if (nextCount > limits.maxCandidates() || nextValueBytes > limits.maxValueBytes()) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
    }

    private int referenceBytes(PreparedCandidate candidate) {
      long bytes = 64L + 5L * Integer.BYTES + candidate.record().key().length()
          + candidate.systemIdentity().length() + 64L + 1;
      if (bytes > ManifestPage.MAX_PAGE_BYTES) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      return (int) bytes;
    }
  }

  private final class EventWriter {
    private final TransactionId transactionId;
    private final long ownerEpoch;
    private final long baseSequence;
    private final long baseRevision;
    private final AsyncCursor<PreparedEvent> input;
    private final RequestScope scope;
    private final CompletableFuture<EventSummary> completion = new CompletableFuture<>();
    private final List<PreparedEventReference> leaf = new ArrayList<>(EventPage.MAX_EVENTS);
    private final List<List<EventReference>> levels = new ArrayList<>(EventDirectoryPage.MAX_DEPTH + 1);
    private long count;
    private long totalBytes;
    private long nextOrdinal;
    private int pageBytes = 64;
    private int eventPageIndex;
    private int directoryPageIndex;

    private EventWriter(TransactionId transactionId, long ownerEpoch, long baseSequence,
        long baseRevision, AsyncCursor<PreparedEvent> input, RequestScope scope) {
      this.transactionId = transactionId;
      this.ownerEpoch = ownerEpoch;
      this.baseSequence = baseSequence;
      this.baseRevision = baseRevision;
      this.input = input;
      this.scope = scope;
      for (int level = 0; level <= EventDirectoryPage.MAX_DEPTH; level++) {
        levels.add(new ArrayList<>(EventDirectoryPage.MAX_CHILDREN));
      }
    }

    private CompletableFuture<EventSummary> write() {
      next();
      return completion;
    }

    private void next() {
      try {
        if (scope.isDeadlineExceeded()) throw new CompletionException(new BackendFailure(BackendFailure.Category.TIMEOUT));
        if (scope.isClosed()) throw new CompletionException(new BackendFailure(BackendFailure.Category.CANCELLED));
        Objects.requireNonNull(input.next(), "event cursor future")
            .whenCompleteAsync(this::acceptInput, CONTINUATION_EXECUTOR);
      } catch (RuntimeException failure) {
        completion.completeExceptionally(failure);
      } catch (Error fatal) {
        completion.completeExceptionally(fatal);
        throw fatal;
      }
    }

    private void acceptInput(Optional<PreparedEvent> item, Throwable failure) {
      if (failure != null) {
        completion.completeExceptionally(failure);
        return;
      }
      try {
        if (scope.isDeadlineExceeded()) throw new CompletionException(new BackendFailure(BackendFailure.Category.TIMEOUT));
        if (scope.isClosed()) throw new CompletionException(new BackendFailure(BackendFailure.Category.CANCELLED));
        Optional<PreparedEvent> found = Objects.requireNonNull(item, "event cursor result");
        if (found.isEmpty()) {
          flushLeaf().thenCompose(ignored -> finishDirectories()).whenComplete((summary, problem) -> {
            if (problem == null) completion.complete(summary);
            else completion.completeExceptionally(problem);
          });
          return;
        }
        PreparedEvent event = found.orElseThrow();
        validate(event);
        int referenceBytes = 64 + event.systemIdentity().length() + 32 + Integer.BYTES;
        CompletableFuture<Void> ready = !leaf.isEmpty() && (leaf.size() == EventPage.MAX_EVENTS
            || pageBytes + referenceBytes > EventPage.MAX_PAGE_BYTES)
            ? flushLeaf() : CompletableFuture.completedFuture(null);
        ready.thenCompose(ignored -> accept(event)).whenCompleteAsync((ignored, problem) -> {
          if (problem == null) next();
          else completion.completeExceptionally(problem);
        }, CONTINUATION_EXECUTOR);
      } catch (RuntimeException problem) {
        completion.completeExceptionally(problem);
      } catch (Error fatal) {
        completion.completeExceptionally(fatal);
        throw fatal;
      }
    }

    private CompletableFuture<Void> accept(PreparedEvent event) {
      return repository.createEvent(event, scope).thenAccept(result -> {
        requireCreatedOrIdentical(result, "event");
        PreparedEventReference reference = PreparedEventReference.from(event);
        leaf.add(reference);
        pageBytes = Math.addExact(pageBytes,
            64 + reference.identity().length() + reference.digest().length() + Integer.BYTES);
        count = Math.addExact(count, 1);
        totalBytes = Math.addExact(totalBytes, reference.encodedBytes());
        nextOrdinal++;
      });
    }

    private CompletableFuture<Void> flushLeaf() {
      if (leaf.isEmpty()) return CompletableFuture.completedFuture(null);
      EventPage page = EventPage.create(transactionId, eventPageIndex++, List.copyOf(leaf));
      return repository.createEventPage(page, scope).thenCompose(result -> {
        requireCreatedOrIdentical(result, "event page");
        leaf.clear();
        pageBytes = 64;
        return addReference(page.reference(), 0);
      });
    }

    private CompletableFuture<Void> addReference(EventReference reference, int level) {
      if (level > EventDirectoryPage.MAX_DEPTH) {
        return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      List<EventReference> buffer = levels.get(level);
      buffer.add(reference);
      if (buffer.size() < EventDirectoryPage.MAX_CHILDREN) {
        return CompletableFuture.completedFuture(null);
      }
      return writeDirectory(level, buffer)
          .thenCompose(parent -> addReference(parent.reference(), level + 1));
    }

    private CompletableFuture<EventDirectoryPage> writeDirectory(
        int childLevel, List<EventReference> references) {
      if (childLevel >= EventDirectoryPage.MAX_DEPTH) {
        return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      EventDirectoryPage page = EventDirectoryPage.create(transactionId,
          directoryPageIndex++, childLevel + 1, List.copyOf(references));
      return repository.createEventDirectoryPage(page, scope).thenApply(result -> {
        requireCreatedOrIdentical(result, "event directory page");
        references.clear();
        return page;
      });
    }

    private CompletableFuture<EventSummary> finishDirectories() {
      return finishLevel(0).thenApply(root -> new EventSummary(count, totalBytes, root));
    }

    private CompletableFuture<EventReference> finishLevel(int level) {
      if (level > EventDirectoryPage.MAX_DEPTH) {
        boolean remaining = levels.stream().anyMatch(buffer -> !buffer.isEmpty());
        return remaining ? failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT))
            : CompletableFuture.completedFuture(null);
      }
      List<EventReference> current = levels.get(level);
      if (current.isEmpty()) return finishLevel(level + 1);
      boolean higherHasEntries = false;
      for (int index = level + 1; index < levels.size(); index++) {
        higherHasEntries |= !levels.get(index).isEmpty();
      }
      if (current.size() == 1 && !higherHasEntries) {
        return CompletableFuture.completedFuture(current.getFirst());
      }
      return writeDirectory(level, current).thenCompose(page -> {
        if (level + 1 == levels.size()) return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
        levels.get(level + 1).add(page.reference());
        return finishLevel(level + 1);
      });
    }

    private void validate(PreparedEvent event) {
      if (!event.transactionId().equals(transactionId) || event.ownerEpoch() != ownerEpoch
          || event.baseSequence() != baseSequence || event.baseRevision() != baseRevision
          || event.ordinal() != nextOrdinal) {
        throw new IllegalArgumentException("events must be contiguous and belong to this commit attempt");
      }
      long nextCount;
      long nextBytes;
      try {
        nextCount = Math.addExact(count, 1);
        nextBytes = Math.addExact(totalBytes, event.retainedBytes());
      } catch (ArithmeticException overflow) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      if (nextCount > limits.maxEvents() || nextBytes > limits.maxEventBytes()) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
    }
  }

  private record EventSummary(long count, long bytes, EventReference root) {}

  private static void requireCreatedOrIdentical(SystemRecordStore.CreateResult result, String kind) {
    if (result == SystemRecordStore.CreateResult.CONFLICT) {
      throw new IllegalStateException(kind + " identity already exists with different bytes");
    }
  }

  private static <T> CompletableFuture<T> failed(Throwable failure) {
    return CompletableFuture.failedFuture(failure);
  }
}
