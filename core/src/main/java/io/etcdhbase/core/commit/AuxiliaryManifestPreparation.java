package io.etcdhbase.core.commit;

import io.etcdhbase.core.concurrent.ContinuationExecutor;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Streams bounded auxiliary records into the private part of one M2 manifest. */
final class AuxiliaryManifestPreparation {
  private static final ContinuationExecutor CONTINUATIONS = new ContinuationExecutor();
  private static final int DIRECTORY_ENVELOPE_BYTES = 64;

  private final ManifestRepository repository;

  AuxiliaryManifestPreparation(ManifestRepository repository) {
    this.repository = Objects.requireNonNull(repository, "repository");
  }

  CompletableFuture<AuxiliaryReference> prepare(TransactionId transactionId, long ownerEpoch,
      long baseSequence, AsyncCursor<PreparedAuxiliaryRecord> records, RequestScope scope) {
    Objects.requireNonNull(transactionId, "transactionId");
    Objects.requireNonNull(records, "records");
    Objects.requireNonNull(scope, "scope");
    if (ownerEpoch <= 0 || baseSequence < 0) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "invalid auxiliary preparation base"));
    }
    return new Writer(transactionId, ownerEpoch, baseSequence, records, scope).start().copy();
  }

  private final class Writer {
    private final TransactionId transactionId;
    private final long ownerEpoch;
    private final long baseSequence;
    private final AsyncCursor<PreparedAuxiliaryRecord> input;
    private final RequestScope scope;
    private final CompletableFuture<AuxiliaryReference> completion = new CompletableFuture<>();
    private final List<AuxiliaryRecordReference> pageRecords =
        new ArrayList<>(AuxiliaryPage.MAX_RECORDS);
    private final List<AuxiliaryPageReference> pages =
        new ArrayList<>(AuxiliaryReference.MAX_PAGES);
    private final java.util.Set<Target> targets = new java.util.HashSet<>();
    private long nextOrdinal;
    private long totalBytes;
    private int pageIndex;
    private int pageBytes = DIRECTORY_ENVELOPE_BYTES;

    private Writer(TransactionId transactionId, long ownerEpoch, long baseSequence,
        AsyncCursor<PreparedAuxiliaryRecord> input, RequestScope scope) {
      this.transactionId = transactionId;
      this.ownerEpoch = ownerEpoch;
      this.baseSequence = baseSequence;
      this.input = input;
      this.scope = scope;
    }

    private CompletableFuture<AuxiliaryReference> start() {
      next();
      return completion;
    }

    private void next() {
      try {
        checkActive();
        Objects.requireNonNull(input.next(), "auxiliary cursor future")
            .whenCompleteAsync(this::acceptInput, CONTINUATIONS);
      } catch (RuntimeException failure) {
        completion.completeExceptionally(failure);
      } catch (Error fatal) {
        completion.completeExceptionally(fatal);
        throw fatal;
      }
    }

    private void acceptInput(Optional<PreparedAuxiliaryRecord> item, Throwable failure) {
      if (failure != null) {
        completion.completeExceptionally(failure);
        return;
      }
      try {
        checkActive();
        Optional<PreparedAuxiliaryRecord> found = Objects.requireNonNull(item, "auxiliary cursor result");
        if (found.isEmpty()) {
          finish();
          return;
        }
        PreparedAuxiliaryRecord record = found.orElseThrow();
        validate(record);
        int referenceBytes = referenceBytes(record);
        CompletableFuture<Void> ready = !pageRecords.isEmpty()
            && (pageRecords.size() == AuxiliaryPage.MAX_RECORDS
                || pageBytes > AuxiliaryPage.MAX_PAGE_BYTES - referenceBytes)
                    ? flushPage() : CompletableFuture.completedFuture(null);
        ready.thenCompose(ignored -> persist(record, referenceBytes))
            .whenCompleteAsync((ignored, problem) -> {
              if (problem == null) next();
              else completion.completeExceptionally(problem);
            }, CONTINUATIONS);
      } catch (RuntimeException problem) {
        completion.completeExceptionally(problem);
      } catch (Error fatal) {
        completion.completeExceptionally(fatal);
        throw fatal;
      }
    }

    private CompletableFuture<Void> persist(PreparedAuxiliaryRecord record, int referenceBytes) {
      long nextCount;
      long nextTotalBytes;
      try {
        nextCount = Math.addExact(nextOrdinal, 1);
        nextTotalBytes = Math.addExact(totalBytes, record.encodedBytes());
      } catch (ArithmeticException overflow) {
        return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT, overflow));
      }
      if (nextCount > AuxiliaryReference.MAX_RECORDS
          || nextTotalBytes > AuxiliaryReference.MAX_TOTAL_BYTES) {
        return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      return repository.createAuxiliaryRecord(record, scope).thenAccept(result -> {
        requireCreatedOrIdentical(result, "auxiliary record");
        AuxiliaryRecordReference reference = new AuxiliaryRecordReference(record.ordinal(),
            record.systemIdentity(), record.digest(), record.encodedBytes());
        pageRecords.add(reference);
        pageBytes = Math.addExact(pageBytes, referenceBytes);
        nextOrdinal = nextCount;
        totalBytes = nextTotalBytes;
      });
    }

    private CompletableFuture<Void> flushPage() {
      if (pageRecords.isEmpty()) return CompletableFuture.completedFuture(null);
      if (pages.size() >= AuxiliaryReference.MAX_PAGES) {
        return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      }
      AuxiliaryPage page = AuxiliaryPage.create(transactionId, pageIndex++,
          List.copyOf(pageRecords));
      return repository.createAuxiliaryPage(page, scope).thenCompose(result -> {
        requireCreatedOrIdentical(result, "auxiliary page");
        pages.add(page.reference());
        pageRecords.clear();
        pageBytes = DIRECTORY_ENVELOPE_BYTES;
        return CompletableFuture.completedFuture(null);
      });
    }

    private void finish() {
      flushPage().thenCompose(ignored -> writeDirectory())
          .whenComplete((root, failure) -> {
            if (failure == null) completion.complete(root);
            else completion.completeExceptionally(failure);
          });
    }

    private CompletableFuture<AuxiliaryReference> writeDirectory() {
      if (pages.isEmpty()) return failed(new IllegalArgumentException(
          "an auxiliary manifest must contain at least one record"));
      AuxiliaryDirectory directory = AuxiliaryDirectory.create(transactionId, List.copyOf(pages));
      return repository.createAuxiliaryDirectory(directory, scope).thenApply(result -> {
        requireCreatedOrIdentical(result, "auxiliary directory");
        return directory.reference();
      });
    }

    private void validate(PreparedAuxiliaryRecord record) {
      if (!record.transactionId().equals(transactionId)
          || record.ownerEpoch() != ownerEpoch
          || record.baseSequence() != baseSequence
          || record.ordinal() != nextOrdinal) {
        throw new IllegalArgumentException("auxiliary record belongs to another attempt or ordinal");
      }
      if (!targets.add(new Target(record.targetType(), record.targetIdentity()))) {
        throw new IllegalArgumentException("duplicate auxiliary materialization target");
      }
    }

    private void checkActive() {
      if (scope.isDeadlineExceeded()) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.TIMEOUT));
      }
      if (scope.isClosed()) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.CANCELLED));
      }
    }
  }

  private record Target(SystemRecordType type, io.etcdhbase.core.bytes.ByteKey identity) {}

  private static int referenceBytes(PreparedAuxiliaryRecord record) {
    long bytes = 128L + record.systemIdentity().length();
    if (bytes > AuxiliaryPage.MAX_PAGE_BYTES) {
      throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    }
    return (int) bytes;
  }

  private static void requireCreatedOrIdentical(SystemRecordStore.CreateResult result, String kind) {
    if (result == SystemRecordStore.CreateResult.CONFLICT) {
      throw new IllegalStateException(kind + " identity already exists with different bytes");
    }
  }

  private static <T> CompletableFuture<T> failed(Throwable failure) {
    return CompletableFuture.failedFuture(failure);
  }
}
