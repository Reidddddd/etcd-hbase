package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

/** Sealed bounded root for the complete candidate directory of one transaction attempt. */
public final class ManifestRoot {
  public static final long MAX_CANDIDATES = 1_000_000;
  public static final long MAX_VALUE_BYTES = 64L * 1024 * 1024 * 1024;
  public static final long MAX_EVENTS = 1_000_000;
  public static final long MAX_EVENT_BYTES = 64L * 1024 * 1024 * 1024;
  private final TransactionId transactionId;
  private final long ownerEpoch;
  private final long baseSequence;
  private final long baseRevision;
  private final long candidateCount;
  private final long totalValueBytes;
  private final Optional<ManifestChildReference> candidateDirectory;
  private final long eventCount;
  private final long totalEventBytes;
  private final Optional<EventReference> eventDirectory;
  private final Optional<ResultReference> resultReference;
  private final Optional<AuxiliaryReference> auxiliaryReference;
  private final ByteKey digest;

  private ManifestRoot(TransactionId transactionId, long ownerEpoch, long baseSequence,
      long baseRevision, long candidateCount, long totalValueBytes,
      Optional<ManifestChildReference> candidateDirectory, long eventCount, long totalEventBytes,
      Optional<EventReference> eventDirectory, Optional<ResultReference> resultReference,
      Optional<AuxiliaryReference> auxiliaryReference, ByteKey expectedDigest) {
    this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
    this.candidateDirectory = Objects.requireNonNull(candidateDirectory, "candidateDirectory");
    this.eventDirectory = Objects.requireNonNull(eventDirectory, "eventDirectory");
    this.resultReference = Objects.requireNonNull(resultReference, "resultReference");
    this.auxiliaryReference = Objects.requireNonNull(auxiliaryReference, "auxiliaryReference");
    if (ownerEpoch <= 0 || baseSequence < 0 || baseRevision <= 0 || candidateCount < 0
        || candidateCount > MAX_CANDIDATES || totalValueBytes < 0
        || totalValueBytes > MAX_VALUE_BYTES || eventCount < 0 || eventCount > MAX_EVENTS
        || totalEventBytes < 0 || totalEventBytes > MAX_EVENT_BYTES
        || expectedDigest != null && expectedDigest.length() != 32
        || (candidateCount == 0) != candidateDirectory.isEmpty()
        || candidateCount == 0 && totalValueBytes != 0
        || (eventCount == 0) != eventDirectory.isEmpty()
        || eventCount == 0 && totalEventBytes != 0) {
      throw new IllegalArgumentException("invalid manifest root bounds");
    }
    if (candidateDirectory.isPresent()
        && (candidateDirectory.orElseThrow().itemCount() != candidateCount
            || candidateDirectory.orElseThrow().valueBytes() != totalValueBytes)) {
      throw new IllegalArgumentException("manifest root aggregates differ from its directory");
    }
    if (eventDirectory.isPresent()) {
      EventReference events = eventDirectory.orElseThrow();
      if (events.itemCount() != eventCount || events.totalBytes() != totalEventBytes
          || events.firstOrdinal() != 0 || events.lastOrdinal() != eventCount - 1) {
        throw new IllegalArgumentException("manifest event aggregates differ from its directory");
      }
    }
    if (resultReference.isPresent()) {
      ResultIdentity identity = resultReference.orElseThrow().identity();
      if (!identity.transactionId().equals(transactionId) || identity.ownerEpoch() != ownerEpoch
          || identity.baseSequence() != baseSequence || identity.baseRevision() != baseRevision) {
        throw new IllegalArgumentException("result belongs to a different commit attempt");
      }
    }
    if (auxiliaryReference.isPresent()
        && !auxiliaryReference.orElseThrow().identity().equals(
            AuxiliaryDirectory.identityFor(transactionId))) {
      throw new IllegalArgumentException("auxiliary root belongs to a different transaction");
    }
    this.ownerEpoch = ownerEpoch;
    this.baseSequence = baseSequence;
    this.baseRevision = baseRevision;
    this.candidateCount = candidateCount;
    this.totalValueBytes = totalValueBytes;
    this.eventCount = eventCount;
    this.totalEventBytes = totalEventBytes;
    this.digest = calculateDigest(transactionId, ownerEpoch, baseSequence, baseRevision,
        candidateCount, totalValueBytes, candidateDirectory, eventCount, totalEventBytes,
        eventDirectory, resultReference, auxiliaryReference);
    if (expectedDigest != null && !this.digest.equals(expectedDigest)) {
      throw new IllegalArgumentException("manifest root digest mismatch");
    }
  }

  public static ManifestRoot create(TransactionId transactionId, long ownerEpoch,
      long baseSequence, long baseRevision, long candidateCount, long totalValueBytes,
      Optional<ManifestChildReference> candidateDirectory) {
    return new ManifestRoot(transactionId, ownerEpoch, baseSequence, baseRevision, candidateCount,
        totalValueBytes, candidateDirectory, 0, 0, Optional.empty(), Optional.empty(),
        Optional.empty(), null);
  }

  public static ManifestRoot create(TransactionId transactionId, long ownerEpoch,
      long baseSequence, long baseRevision, long candidateCount, long totalValueBytes,
      Optional<ManifestChildReference> candidateDirectory, long eventCount, long totalEventBytes,
      Optional<EventReference> eventDirectory) {
    return new ManifestRoot(transactionId, ownerEpoch, baseSequence, baseRevision, candidateCount,
        totalValueBytes, candidateDirectory, eventCount, totalEventBytes, eventDirectory,
        Optional.empty(), Optional.empty(), null);
  }

  public static ManifestRoot createWithResult(TransactionId transactionId, long ownerEpoch,
      long baseSequence, long baseRevision, long candidateCount, long totalValueBytes,
      Optional<ManifestChildReference> candidateDirectory, long eventCount, long totalEventBytes,
      Optional<EventReference> eventDirectory, ResultReference resultReference) {
    return new ManifestRoot(transactionId, ownerEpoch, baseSequence, baseRevision, candidateCount,
        totalValueBytes, candidateDirectory, eventCount, totalEventBytes, eventDirectory,
        Optional.of(Objects.requireNonNull(resultReference, "resultReference")), Optional.empty(), null);
  }

  public static ManifestRoot createWithAuxiliary(TransactionId transactionId, long ownerEpoch,
      long baseSequence, long baseRevision, Optional<AuxiliaryReference> auxiliaryReference) {
    return new ManifestRoot(transactionId, ownerEpoch, baseSequence, baseRevision, 0, 0,
        Optional.empty(), 0, 0, Optional.empty(), Optional.empty(),
        requireAuxiliary(auxiliaryReference), null);
  }

  public static ManifestRoot createWithAuxiliary(TransactionId transactionId, long ownerEpoch,
      long baseSequence, long baseRevision, long candidateCount, long totalValueBytes,
      Optional<ManifestChildReference> candidateDirectory, long eventCount, long totalEventBytes,
      Optional<EventReference> eventDirectory, Optional<AuxiliaryReference> auxiliaryReference) {
    return new ManifestRoot(transactionId, ownerEpoch, baseSequence, baseRevision, candidateCount,
        totalValueBytes, candidateDirectory, eventCount, totalEventBytes, eventDirectory,
        Optional.empty(), requireAuxiliary(auxiliaryReference), null);
  }

  public static ManifestRoot createWithResultAndAuxiliary(TransactionId transactionId,
      long ownerEpoch, long baseSequence, long baseRevision, long candidateCount,
      long totalValueBytes, Optional<ManifestChildReference> candidateDirectory,
      long eventCount, long totalEventBytes, Optional<EventReference> eventDirectory,
      ResultReference resultReference, AuxiliaryReference auxiliaryReference) {
    return new ManifestRoot(transactionId, ownerEpoch, baseSequence, baseRevision, candidateCount,
        totalValueBytes, candidateDirectory, eventCount, totalEventBytes, eventDirectory,
        Optional.of(Objects.requireNonNull(resultReference, "resultReference")),
        Optional.of(Objects.requireNonNull(auxiliaryReference, "auxiliaryReference")), null);
  }

  public static ManifestRoot restore(TransactionId transactionId, long ownerEpoch,
      long baseSequence, long baseRevision, long candidateCount, long totalValueBytes,
      Optional<ManifestChildReference> candidateDirectory, ByteKey digest) {
    return new ManifestRoot(transactionId, ownerEpoch, baseSequence, baseRevision, candidateCount,
        totalValueBytes, candidateDirectory, 0, 0, Optional.empty(), Optional.empty(),
        Optional.empty(), Objects.requireNonNull(digest, "digest"));
  }

  public static ManifestRoot restore(TransactionId transactionId, long ownerEpoch,
      long baseSequence, long baseRevision, long candidateCount, long totalValueBytes,
      Optional<ManifestChildReference> candidateDirectory, long eventCount, long totalEventBytes,
      Optional<EventReference> eventDirectory, ByteKey digest) {
    return new ManifestRoot(transactionId, ownerEpoch, baseSequence, baseRevision, candidateCount,
        totalValueBytes, candidateDirectory, eventCount, totalEventBytes, eventDirectory,
        Optional.empty(), Optional.empty(), Objects.requireNonNull(digest, "digest"));
  }

  public static ManifestRoot restoreWithResult(TransactionId transactionId, long ownerEpoch,
      long baseSequence, long baseRevision, long candidateCount, long totalValueBytes,
      Optional<ManifestChildReference> candidateDirectory, long eventCount, long totalEventBytes,
      Optional<EventReference> eventDirectory, ResultReference resultReference, ByteKey digest) {
    return new ManifestRoot(transactionId, ownerEpoch, baseSequence, baseRevision, candidateCount,
        totalValueBytes, candidateDirectory, eventCount, totalEventBytes, eventDirectory,
        Optional.of(Objects.requireNonNull(resultReference, "resultReference")),
        Optional.empty(), Objects.requireNonNull(digest, "digest"));
  }

  public static ManifestRoot restoreWithAuxiliary(TransactionId transactionId, long ownerEpoch,
      long baseSequence, long baseRevision, long candidateCount, long totalValueBytes,
      Optional<ManifestChildReference> candidateDirectory, long eventCount, long totalEventBytes,
      Optional<EventReference> eventDirectory, Optional<ResultReference> resultReference,
      AuxiliaryReference auxiliaryReference, ByteKey digest) {
    return new ManifestRoot(transactionId, ownerEpoch, baseSequence, baseRevision, candidateCount,
        totalValueBytes, candidateDirectory, eventCount, totalEventBytes, eventDirectory,
        Objects.requireNonNull(resultReference, "resultReference"),
        Optional.of(Objects.requireNonNull(auxiliaryReference, "auxiliaryReference")),
        Objects.requireNonNull(digest, "digest"));
  }

  public TransactionId transactionId() { return transactionId; }
  public long ownerEpoch() { return ownerEpoch; }
  public long baseSequence() { return baseSequence; }
  public long baseRevision() { return baseRevision; }
  public long candidateCount() { return candidateCount; }
  public long totalValueBytes() { return totalValueBytes; }
  public Optional<ManifestChildReference> candidateDirectory() { return candidateDirectory; }
  public long eventCount() { return eventCount; }
  public long totalEventBytes() { return totalEventBytes; }
  public Optional<EventReference> eventDirectory() { return eventDirectory; }
  public Optional<ResultReference> resultReference() { return resultReference; }
  public Optional<AuxiliaryReference> auxiliaryReference() { return auxiliaryReference; }
  public ByteKey digest() { return digest; }
  public boolean containsKeyChanges() { return candidateCount > 0; }

  public ByteKey systemIdentity() {
    return ByteKey.copyOf(ByteBuffer.allocate(16 + 1).put(transactionId.bytes())
        .put(ManifestRecordFormat.MANIFEST_ROOT).array());
  }

  private static ByteKey calculateDigest(TransactionId txn, long epoch, long sequence, long revision,
      long count, long valueBytes, Optional<ManifestChildReference> directory,
      long eventCount, long eventBytes, Optional<EventReference> eventDirectory,
      Optional<ResultReference> resultReference, Optional<AuxiliaryReference> auxiliaryReference) {
    return ManifestDigest.sha256(output -> {
      output.writeInt(auxiliaryReference.isPresent() ? 4 : resultReference.isPresent() ? 3 : 2);
      output.write(txn.bytes());
      output.writeLong(epoch);
      output.writeLong(sequence);
      output.writeLong(revision);
      output.writeLong(count);
      output.writeLong(valueBytes);
      output.writeBoolean(directory.isPresent());
      if (directory.isPresent()) {
        ManifestChildReference child = directory.orElseThrow();
        ManifestDigest.writeBytes(output, child.identity().bytes());
        ManifestDigest.writeBytes(output, child.digest().bytes());
        output.writeInt(child.firstBucket());
        output.writeInt(child.lastBucket());
        ManifestDigest.writeBytes(output, child.firstKey().bytes());
        ManifestDigest.writeBytes(output, child.lastKey().bytes());
        output.writeLong(child.itemCount());
        output.writeLong(child.valueBytes());
        output.writeInt(child.leafPageCount());
        output.writeInt(child.level());
      }
      output.writeLong(eventCount);
      output.writeLong(eventBytes);
      output.writeBoolean(eventDirectory.isPresent());
      if (eventDirectory.isPresent()) {
        EventReference child = eventDirectory.orElseThrow();
        ManifestDigest.writeBytes(output, child.identity().bytes());
        ManifestDigest.writeBytes(output, child.digest().bytes());
        output.writeLong(child.firstOrdinal());
        output.writeLong(child.lastOrdinal());
        output.writeLong(child.itemCount());
        output.writeLong(child.totalBytes());
        output.writeInt(child.leafPageCount());
        output.writeInt(child.level());
      }
      if (resultReference.isPresent()) {
        ResultReference result = resultReference.orElseThrow();
        result.identity().writeTo(output);
        output.write(result.digest().bytes());
        output.writeInt(result.pageCount());
        output.writeInt(result.itemCount());
        output.writeLong(result.totalBytes());
      }
      if (auxiliaryReference.isPresent()) {
        AuxiliaryReference auxiliary = auxiliaryReference.orElseThrow();
        ManifestDigest.writeBytes(output, auxiliary.identity().bytes());
        output.write(auxiliary.digest().bytes());
        output.writeLong(auxiliary.itemCount());
        output.writeLong(auxiliary.totalBytes());
        output.writeInt(auxiliary.pageCount());
      }
    });
  }

  private static Optional<AuxiliaryReference> requireAuxiliary(
      Optional<AuxiliaryReference> reference) {
    Objects.requireNonNull(reference, "auxiliaryReference");
    if (reference.isEmpty()) throw new IllegalArgumentException("auxiliary root is required");
    return reference;
  }
}
