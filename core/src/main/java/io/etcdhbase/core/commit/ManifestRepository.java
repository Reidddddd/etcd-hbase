package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.ToLongFunction;

/** Append-only persistence for private candidates and the sealed manifest tree. */
public interface ManifestRepository {
  /** A decoded immutable record together with its stored row and payload byte count. */
  record ReadValue<T>(Optional<T> value, long encodedBytes) {
    public ReadValue {
      Objects.requireNonNull(value, "value");
      if (encodedBytes < 0 || value.isEmpty() && encodedBytes != 0) {
        throw new IllegalArgumentException("invalid manifest read byte count");
      }
    }

    static <T> ReadValue<T> estimated(Optional<T> value, ToLongFunction<T> size) {
      Objects.requireNonNull(value, "value");
      Objects.requireNonNull(size, "size");
      return new ReadValue<>(value, value.map(item -> size.applyAsLong(item)).orElse(0L));
    }
  }

  /** Validates a candidate row identity and returns its fixed salt bucket. */
  int candidateBucket(PreparedCandidate candidate);

  CompletableFuture<SystemRecordStore.CreateResult> createCandidate(
      PreparedCandidate candidate, RequestScope scope);

  CompletableFuture<SystemRecordStore.CreateResult> createPage(ManifestPage page, RequestScope scope);

  CompletableFuture<SystemRecordStore.CreateResult> createDirectoryPage(
      ManifestDirectoryPage page, RequestScope scope);

  CompletableFuture<SystemRecordStore.CreateResult> createEvent(PreparedEvent event, RequestScope scope);

  CompletableFuture<SystemRecordStore.CreateResult> createEventPage(EventPage page, RequestScope scope);

  CompletableFuture<SystemRecordStore.CreateResult> createEventDirectoryPage(
      EventDirectoryPage page, RequestScope scope);

  default CompletableFuture<SystemRecordStore.CreateResult> createAuxiliaryRecord(
      PreparedAuxiliaryRecord record, RequestScope scope) {
    return CompletableFuture.failedFuture(new UnsupportedOperationException(
        "auxiliary manifest records are not available"));
  }

  default CompletableFuture<SystemRecordStore.CreateResult> createAuxiliaryPage(
      AuxiliaryPage page, RequestScope scope) {
    return CompletableFuture.failedFuture(new UnsupportedOperationException(
        "auxiliary manifest pages are not available"));
  }

  default CompletableFuture<SystemRecordStore.CreateResult> createAuxiliaryDirectory(
      AuxiliaryDirectory directory, RequestScope scope) {
    return CompletableFuture.failedFuture(new UnsupportedOperationException(
        "auxiliary manifest directories are not available"));
  }

  CompletableFuture<SystemRecordStore.CreateResult> seal(ManifestRoot root, RequestScope scope);

  CompletableFuture<Optional<ManifestRoot>> readRoot(ByteKey identity, RequestScope scope);

  CompletableFuture<Optional<PreparedCandidate>> readCandidate(ByteKey identity, RequestScope scope);

  CompletableFuture<Optional<ManifestPage>> readPage(ByteKey identity, RequestScope scope);

  CompletableFuture<Optional<ManifestDirectoryPage>> readDirectoryPage(ByteKey identity, RequestScope scope);

  CompletableFuture<Optional<PreparedEvent>> readEvent(ByteKey identity, RequestScope scope);

  CompletableFuture<Optional<EventPage>> readEventPage(ByteKey identity, RequestScope scope);

  CompletableFuture<Optional<EventDirectoryPage>> readEventDirectoryPage(
      ByteKey identity, RequestScope scope);

  default CompletableFuture<Optional<PreparedAuxiliaryRecord>> readAuxiliaryRecord(
      ByteKey identity, RequestScope scope) {
    return CompletableFuture.failedFuture(new UnsupportedOperationException(
        "auxiliary manifest records are not available"));
  }

  default CompletableFuture<Optional<AuxiliaryPage>> readAuxiliaryPage(
      ByteKey identity, RequestScope scope) {
    return CompletableFuture.failedFuture(new UnsupportedOperationException(
        "auxiliary manifest pages are not available"));
  }

  default CompletableFuture<Optional<AuxiliaryDirectory>> readAuxiliaryDirectory(
      ByteKey identity, RequestScope scope) {
    return CompletableFuture.failedFuture(new UnsupportedOperationException(
        "auxiliary manifest directories are not available"));
  }

  /** Reads an auxiliary record with its stored-byte cost; adapters should return the exact size. */
  default CompletableFuture<ReadValue<PreparedAuxiliaryRecord>> readAuxiliaryRecordWithSize(
      ByteKey identity, RequestScope scope) {
    return readAuxiliaryRecord(identity, scope).thenApply(value -> ReadValue.estimated(value,
        ManifestRepository::estimatedAuxiliaryRecordBytes));
  }

  /** Reads an auxiliary page with its stored-byte cost; the default is a conservative model size. */
  default CompletableFuture<ReadValue<AuxiliaryPage>> readAuxiliaryPageWithSize(
      ByteKey identity, RequestScope scope) {
    return readAuxiliaryPage(identity, scope).thenApply(value -> ReadValue.estimated(value,
        ManifestRepository::estimatedAuxiliaryPageBytes));
  }

  /** Reads an auxiliary directory with its stored-byte cost; the default is a conservative model size. */
  default CompletableFuture<ReadValue<AuxiliaryDirectory>> readAuxiliaryDirectoryWithSize(
      ByteKey identity, RequestScope scope) {
    return readAuxiliaryDirectory(identity, scope).thenApply(value -> ReadValue.estimated(value,
        ManifestRepository::estimatedAuxiliaryDirectoryBytes));
  }

  private static long estimatedAuxiliaryRecordBytes(PreparedAuxiliaryRecord record) {
    return Math.addExact(record.systemIdentity().length(), Math.addExact(192L,
        Math.addExact(record.targetIdentity().length(), record.payload().length())));
  }

  private static long estimatedAuxiliaryPageBytes(AuxiliaryPage page) {
    long references = page.records().stream().mapToLong(record -> Math.addExact(128L,
        Math.addExact(record.identity().length(), record.digest().length()))).reduce(0L, Math::addExact);
    return Math.addExact(page.systemIdentity().length(), Math.addExact(128L, references));
  }

  private static long estimatedAuxiliaryDirectoryBytes(AuxiliaryDirectory directory) {
    long references = directory.pages().stream().mapToLong(page -> Math.addExact(128L,
        Math.addExact(page.identity().length(), page.digest().length()))).reduce(0L, Math::addExact);
    return Math.addExact(directory.systemIdentity().length(), Math.addExact(128L, references));
  }
}
