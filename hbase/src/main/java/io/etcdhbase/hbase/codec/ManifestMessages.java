package io.etcdhbase.hbase.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.AuxiliaryDirectory;
import io.etcdhbase.core.commit.AuxiliaryPage;
import io.etcdhbase.core.commit.AuxiliaryPageReference;
import io.etcdhbase.core.commit.AuxiliaryRecordReference;
import io.etcdhbase.core.commit.AuxiliaryReference;
import io.etcdhbase.core.commit.ManifestCandidateReference;
import io.etcdhbase.core.commit.ManifestChildReference;
import io.etcdhbase.core.commit.ManifestDirectoryPage;
import io.etcdhbase.core.commit.ManifestPage;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.EventDirectoryPage;
import io.etcdhbase.core.commit.EventPage;
import io.etcdhbase.core.commit.EventReference;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.PreparedEventReference;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedAuxiliaryRecord;
import io.etcdhbase.core.commit.ResultReference;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.storage.proto.CommitProtos;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.HConstants;

/** Strict bounded codecs for private candidate and sealed manifest records. */
public final class ManifestMessages {
  public static final int FORMAT_VERSION = 2;
  public static final int ROOT_RESULT_FORMAT_VERSION = 3;
  public static final int ROOT_AUXILIARY_FORMAT_VERSION = 4;
  public static final int MAX_ROOT_BYTES = 16 * 1024;
  public static final int MAX_RESULT_ROOT_BYTES = 128 * 1024;
  public static final int MAX_AUXILIARY_ROOT_BYTES = 128 * 1024;
  public static final int MAX_DIRECTORY_BYTES = ManifestDirectoryPage.MAX_PAGE_BYTES;
  public static final int MAX_EVENT_DIRECTORY_BYTES = 64 * 1024;

  private ManifestMessages() {}

  public static byte[] encode(PreparedCandidate candidate) {
    var record = candidate.record();
    var metadata = record.metadata();
    var builder = CommitProtos.PreparedCandidate.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setTransactionId(ByteString.copyFrom(candidate.transactionId().bytes()))
        .setOwnerEpoch(candidate.ownerEpoch())
        .setBaseSequence(candidate.baseSequence())
        .setBaseRevision(candidate.baseRevision())
        .setLogicalKey(ByteString.copyFrom(record.key().bytes()))
        .setCreateRevision(metadata.createRevision())
        .setVersion(metadata.version())
        .setLeaseId(metadata.leaseId())
        .setTombstone(metadata.state() == StorageMetadata.State.TOMBSTONE)
        .setContentDigest(ByteString.copyFrom(candidate.contentDigest().bytes()));
    record.value().ifPresent(value -> builder.setValue(ByteString.copyFrom(value)));
    byte[] encoded = builder.build().toByteArray();
    requireLength(encoded.length, HBaseSystemRecordLimits.MAX_CANDIDATE_BYTES);
    return encoded;
  }

  public static PreparedCandidate decodeCandidate(byte[] encoded) {
    requireLength(encoded.length, HBaseSystemRecordLimits.MAX_CANDIDATE_BYTES);
    try {
      var message = CommitProtos.PreparedCandidate.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != FORMAT_VERSION
          || !message.hasTransactionId() || !message.hasOwnerEpoch()
          || !message.hasBaseSequence() || !message.hasBaseRevision()
          || !message.hasLogicalKey() || !message.hasCreateRevision() || !message.hasVersion()
          || !message.hasLeaseId() || !message.hasTombstone() || !message.hasContentDigest()) {
        throw invalid();
      }
      byte[] txn = message.getTransactionId().toByteArray();
      byte[] key = message.getLogicalKey().toByteArray();
      byte[] digest = message.getContentDigest().toByteArray();
      if (txn.length != 16 || key.length == 0 || digest.length != 32) throw invalid();
      boolean tombstone = message.getTombstone();
      StorageMetadata.State state = tombstone ? StorageMetadata.State.TOMBSTONE : StorageMetadata.State.LIVE;
      if (tombstone == message.hasValue()) throw invalid();
      byte[] value = message.hasValue() ? message.getValue().toByteArray() : new byte[0];
      if (value.length > PreparedCandidate.MAX_VALUE_BYTES) throw invalid();
      long modificationRevision = Math.addExact(message.getBaseRevision(), 1);
      var metadata = new StorageMetadata(message.getCreateRevision(), modificationRevision,
          message.getVersion(), message.getLeaseId(), state, value.length);
      StoredRecord record = tombstone
          ? StoredRecord.metadataOnly(ByteKey.copyOf(key), metadata)
          : StoredRecord.live(ByteKey.copyOf(key), metadata, value);
      PreparedCandidate candidate = new PreparedCandidate(TransactionId.fromBytes(txn),
          message.getOwnerEpoch(), message.getBaseSequence(), message.getBaseRevision(), record);
      if (!candidate.contentDigest().equals(ByteKey.copyOf(digest))) throw invalid();
      return candidate;
    } catch (InvalidProtocolBufferException | IllegalArgumentException | ArithmeticException failure) {
      throw new IllegalArgumentException("invalid persisted private candidate", failure);
    }
  }

  public static byte[] encode(ManifestPage page) {
    var builder = CommitProtos.ManifestPage.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setTransactionId(ByteString.copyFrom(page.transactionId().bytes()))
        .setPageIndex(page.pageIndex())
        .setPageDigest(ByteString.copyFrom(page.digest().bytes()));
    page.candidates().forEach(candidate -> builder.addCandidates(encode(candidate)));
    byte[] encoded = builder.build().toByteArray();
    requireLength(encoded.length, ManifestPage.MAX_PAGE_BYTES);
    return encoded;
  }

  public static ManifestPage decodePage(byte[] encoded) {
    requireLength(encoded.length, ManifestPage.MAX_PAGE_BYTES);
    try {
      var message = CommitProtos.ManifestPage.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != FORMAT_VERSION
          || !message.hasTransactionId() || !message.hasPageIndex() || !message.hasPageDigest()
          || message.getCandidatesCount() > ManifestPage.MAX_ENTRIES) throw invalid();
      byte[] txn = message.getTransactionId().toByteArray();
      if (txn.length != 16) throw invalid();
      List<ManifestCandidateReference> candidates = new ArrayList<>(message.getCandidatesCount());
      for (var candidate : message.getCandidatesList()) candidates.add(decode(candidate));
      return ManifestPage.restore(TransactionId.fromBytes(txn), message.getPageIndex(), candidates,
          ByteKey.copyOf(message.getPageDigest().toByteArray()));
    } catch (InvalidProtocolBufferException | IllegalArgumentException | ArithmeticException failure) {
      throw new IllegalArgumentException("invalid persisted manifest page", failure);
    }
  }

  public static byte[] encode(ManifestDirectoryPage page) {
    var builder = CommitProtos.ManifestDirectoryPage.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setTransactionId(ByteString.copyFrom(page.transactionId().bytes()))
        .setLevel(page.level())
        .setPageDigest(ByteString.copyFrom(page.digest().bytes()))
        .setPageIndex(page.pageIndex());
    page.children().forEach(child -> builder.addChildren(encode(child)));
    byte[] encoded = builder.build().toByteArray();
    requireLength(encoded.length, MAX_DIRECTORY_BYTES);
    return encoded;
  }

  public static ManifestDirectoryPage decodeDirectory(byte[] encoded) {
    requireLength(encoded.length, MAX_DIRECTORY_BYTES);
    try {
      var message = CommitProtos.ManifestDirectoryPage.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != FORMAT_VERSION
          || !message.hasTransactionId() || !message.hasLevel() || !message.hasPageDigest()
          || !message.hasPageIndex() || message.getChildrenCount() > ManifestDirectoryPage.MAX_CHILDREN) {
        throw invalid();
      }
      byte[] txn = message.getTransactionId().toByteArray();
      if (txn.length != 16) throw invalid();
      List<ManifestChildReference> children = new ArrayList<>(message.getChildrenCount());
      for (var child : message.getChildrenList()) children.add(decode(child));
      return ManifestDirectoryPage.restore(TransactionId.fromBytes(txn), message.getPageIndex(),
          message.getLevel(), children, ByteKey.copyOf(message.getPageDigest().toByteArray()));
    } catch (InvalidProtocolBufferException | IllegalArgumentException | ArithmeticException failure) {
      throw new IllegalArgumentException("invalid persisted manifest directory", failure);
    }
  }

  public static byte[] encode(ManifestRoot root) {
    root.candidateDirectory().ifPresent(child -> {
      requireRootKeyLength(child.firstKey().length());
      requireRootKeyLength(child.lastKey().length());
      requireRootKeyLength(child.identity().length());
    });
    root.eventDirectory().ifPresent(child -> requireRootKeyLength(child.identity().length()));
    int formatVersion = root.auxiliaryReference().isPresent() ? ROOT_AUXILIARY_FORMAT_VERSION
        : root.resultReference().isPresent() ? ROOT_RESULT_FORMAT_VERSION : FORMAT_VERSION;
    var builder = CommitProtos.ManifestRoot.newBuilder()
        .setFormatVersion(formatVersion)
        .setTransactionId(ByteString.copyFrom(root.transactionId().bytes()))
        .setOwnerEpoch(root.ownerEpoch())
        .setBaseSequence(root.baseSequence())
        .setBaseRevision(root.baseRevision())
        .setCandidateCount(root.candidateCount())
        .setTotalValueBytes(root.totalValueBytes())
        .setEventCount(root.eventCount())
        .setTotalEventBytes(root.totalEventBytes())
        .setRootDigest(ByteString.copyFrom(root.digest().bytes()));
    root.candidateDirectory().ifPresent(child -> builder
        .setDirectoryIdentity(ByteString.copyFrom(child.identity().bytes()))
        .setDirectoryDigest(ByteString.copyFrom(child.digest().bytes()))
        .setDirectoryFirstBucket(child.firstBucket())
        .setDirectoryLastBucket(child.lastBucket())
        .setDirectoryFirstKey(ByteString.copyFrom(child.firstKey().bytes()))
        .setDirectoryLastKey(ByteString.copyFrom(child.lastKey().bytes()))
        .setDirectoryItemCount(child.itemCount())
        .setDirectoryPageCount(child.leafPageCount())
        .setDirectoryLevel(child.level())
        .setDirectoryValueBytes(child.valueBytes()));
    root.eventDirectory().ifPresent(child -> builder
        .setEventDirectoryIdentity(ByteString.copyFrom(child.identity().bytes()))
        .setEventDirectoryDigest(ByteString.copyFrom(child.digest().bytes()))
        .setEventDirectoryFirstOrdinal(child.firstOrdinal())
        .setEventDirectoryLastOrdinal(child.lastOrdinal())
        .setEventDirectoryItemCount(child.itemCount())
        .setEventDirectoryPageCount(child.leafPageCount())
        .setEventDirectoryLevel(child.level())
        .setEventDirectoryTotalBytes(child.totalBytes()));
    root.resultReference().ifPresent(reference -> builder.setResult(ResultMessages.encode(reference)));
    root.auxiliaryReference().ifPresent(reference -> builder.setAuxiliary(encode(reference)));
    byte[] encoded = builder.build().toByteArray();
    requireLength(encoded.length, rootLimit(formatVersion));
    return encoded;
  }

  public static ManifestRoot decodeRoot(byte[] encoded) {
    requireLength(encoded.length, MAX_RESULT_ROOT_BYTES);
    try {
      var message = CommitProtos.ManifestRoot.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion()
          || message.getFormatVersion() != FORMAT_VERSION
              && message.getFormatVersion() != ROOT_RESULT_FORMAT_VERSION
              && message.getFormatVersion() != ROOT_AUXILIARY_FORMAT_VERSION
          || !message.hasTransactionId() || !message.hasOwnerEpoch() || !message.hasBaseSequence()
          || !message.hasBaseRevision() || !message.hasCandidateCount()
          || !message.hasTotalValueBytes() || !message.hasRootDigest()
          || !message.hasEventCount() || !message.hasTotalEventBytes()) throw invalid();
      if (message.getFormatVersion() == FORMAT_VERSION
          && (message.hasResult() || message.hasAuxiliary())
          || message.getFormatVersion() == ROOT_RESULT_FORMAT_VERSION
              && (!message.hasResult() || message.hasAuxiliary())
          || message.getFormatVersion() == ROOT_AUXILIARY_FORMAT_VERSION
              && !message.hasAuxiliary()) throw invalid();
      requireLength(encoded.length, rootLimit(message.getFormatVersion()));
      byte[] txn = message.getTransactionId().toByteArray();
      if (txn.length != 16) throw invalid();
      boolean hasDirectory = message.hasDirectoryIdentity();
      boolean anyDirectoryField = hasDirectory || message.hasDirectoryDigest()
          || message.hasDirectoryFirstKey() || message.hasDirectoryLastKey()
          || message.hasDirectoryItemCount() || message.hasDirectoryPageCount()
          || message.hasDirectoryLevel() || message.hasDirectoryValueBytes()
          || message.hasDirectoryFirstBucket() || message.hasDirectoryLastBucket();
      boolean allDirectoryFields = hasDirectory && message.hasDirectoryDigest()
          && message.hasDirectoryFirstKey()
          && message.hasDirectoryLastKey() && message.hasDirectoryItemCount()
          && message.hasDirectoryPageCount() && message.hasDirectoryLevel()
          && message.hasDirectoryValueBytes() && message.hasDirectoryFirstBucket()
          && message.hasDirectoryLastBucket();
      if (anyDirectoryField != allDirectoryFields) throw invalid();
      Optional<ManifestChildReference> directory = Optional.empty();
      if (hasDirectory) {
        requireRootKeyLength(message.getDirectoryIdentity().size());
        requireRootKeyLength(message.getDirectoryFirstKey().size());
        requireRootKeyLength(message.getDirectoryLastKey().size());
        directory = Optional.of(new ManifestChildReference(
            ByteKey.copyOf(message.getDirectoryIdentity().toByteArray()),
            ByteKey.copyOf(message.getDirectoryDigest().toByteArray()),
            message.getDirectoryFirstBucket(), message.getDirectoryLastBucket(),
            ByteKey.copyOf(message.getDirectoryFirstKey().toByteArray()),
            ByteKey.copyOf(message.getDirectoryLastKey().toByteArray()),
            message.getDirectoryItemCount(), message.getDirectoryValueBytes(),
            message.getDirectoryPageCount(), message.getDirectoryLevel()));
      }
      boolean hasEventDirectory = message.hasEventDirectoryIdentity();
      boolean anyEventField = hasEventDirectory || message.hasEventDirectoryDigest()
          || message.hasEventDirectoryFirstOrdinal() || message.hasEventDirectoryLastOrdinal()
          || message.hasEventDirectoryItemCount() || message.hasEventDirectoryPageCount()
          || message.hasEventDirectoryLevel() || message.hasEventDirectoryTotalBytes();
      boolean allEventFields = hasEventDirectory && message.hasEventDirectoryDigest()
          && message.hasEventDirectoryFirstOrdinal() && message.hasEventDirectoryLastOrdinal()
          && message.hasEventDirectoryItemCount() && message.hasEventDirectoryPageCount()
          && message.hasEventDirectoryLevel() && message.hasEventDirectoryTotalBytes();
      if (anyEventField != allEventFields) throw invalid();
      Optional<EventReference> eventDirectory = Optional.empty();
      if (hasEventDirectory) {
        requireRootKeyLength(message.getEventDirectoryIdentity().size());
        eventDirectory = Optional.of(new EventReference(
            ByteKey.copyOf(message.getEventDirectoryIdentity().toByteArray()),
            ByteKey.copyOf(message.getEventDirectoryDigest().toByteArray()),
            message.getEventDirectoryFirstOrdinal(), message.getEventDirectoryLastOrdinal(),
            message.getEventDirectoryItemCount(), message.getEventDirectoryTotalBytes(),
            message.getEventDirectoryPageCount(), message.getEventDirectoryLevel()));
      }
      if (message.hasAuxiliary()) {
        Optional<ResultReference> result = message.hasResult()
            ? Optional.of(ResultMessages.decode(message.getResult())) : Optional.empty();
        return ManifestRoot.restoreWithAuxiliary(TransactionId.fromBytes(txn), message.getOwnerEpoch(),
            message.getBaseSequence(), message.getBaseRevision(), message.getCandidateCount(),
            message.getTotalValueBytes(), directory, message.getEventCount(),
            message.getTotalEventBytes(), eventDirectory, result,
            decode(message.getAuxiliary()), ByteKey.copyOf(message.getRootDigest().toByteArray()));
      }
      if (message.hasResult()) {
        return ManifestRoot.restoreWithResult(TransactionId.fromBytes(txn), message.getOwnerEpoch(),
            message.getBaseSequence(), message.getBaseRevision(), message.getCandidateCount(),
            message.getTotalValueBytes(), directory, message.getEventCount(),
            message.getTotalEventBytes(), eventDirectory, ResultMessages.decode(message.getResult()),
            ByteKey.copyOf(message.getRootDigest().toByteArray()));
      }
      return ManifestRoot.restore(TransactionId.fromBytes(txn), message.getOwnerEpoch(),
          message.getBaseSequence(), message.getBaseRevision(), message.getCandidateCount(),
          message.getTotalValueBytes(), directory, message.getEventCount(),
          message.getTotalEventBytes(), eventDirectory,
          ByteKey.copyOf(message.getRootDigest().toByteArray()));
    } catch (InvalidProtocolBufferException | IllegalArgumentException | ArithmeticException failure) {
      throw new IllegalArgumentException("invalid persisted manifest root", failure);
    }
  }

  public static byte[] encode(PreparedAuxiliaryRecord record) {
    var builder = CommitProtos.PreparedAuxiliaryRecord.newBuilder()
        .setFormatVersion(1)
        .setTransactionId(ByteString.copyFrom(record.transactionId().bytes()))
        .setOwnerEpoch(record.ownerEpoch())
        .setBaseSequence(record.baseSequence())
        .setOrdinal(record.ordinal())
        .setTargetRecordType(record.targetType().id())
        .setTargetIdentity(ByteString.copyFrom(record.targetIdentity().bytes()))
        .setPayload(ByteString.copyFrom(record.payload().bytes()))
        .setDigest(ByteString.copyFrom(record.digest().bytes()));
    byte[] encoded = builder.build().toByteArray();
    requireLength(encoded.length, maxAuxiliaryRecordBytes());
    return encoded;
  }

  public static PreparedAuxiliaryRecord decodeAuxiliaryRecord(byte[] encoded) {
    requireLength(encoded.length, maxAuxiliaryRecordBytes());
    try {
      var message = CommitProtos.PreparedAuxiliaryRecord.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != 1
          || !message.hasTransactionId() || !message.hasOwnerEpoch()
          || !message.hasBaseSequence() || !message.hasOrdinal()
          || !message.hasTargetRecordType() || !message.hasTargetIdentity()
          || !message.hasPayload() || !message.hasDigest()) throw invalid();
      byte[] txn = message.getTransactionId().toByteArray();
      byte[] targetIdentity = message.getTargetIdentity().toByteArray();
      byte[] payload = message.getPayload().toByteArray();
      byte[] digest = message.getDigest().toByteArray();
      if (txn.length != 16 || targetIdentity.length == 0
          || targetIdentity.length > PreparedAuxiliaryRecord.MAX_TARGET_IDENTITY_BYTES
          || payload.length == 0 || payload.length > PreparedAuxiliaryRecord.MAX_PAYLOAD_BYTES
          || digest.length != 32 || message.getOrdinal() < 0) throw invalid();
      return PreparedAuxiliaryRecord.restore(TransactionId.fromBytes(txn), message.getOwnerEpoch(),
          message.getBaseSequence(), message.getOrdinal(),
          SystemRecordType.fromId(message.getTargetRecordType()), ByteKey.copyOf(targetIdentity),
          ByteKey.copyOf(payload), ByteKey.copyOf(digest));
    } catch (InvalidProtocolBufferException | IllegalArgumentException | ArithmeticException failure) {
      throw new IllegalArgumentException("invalid persisted auxiliary record", failure);
    }
  }

  public static byte[] encode(AuxiliaryPage page) {
    var builder = CommitProtos.AuxiliaryPage.newBuilder()
        .setFormatVersion(1)
        .setTransactionId(ByteString.copyFrom(page.transactionId().bytes()))
        .setPageIndex(page.pageIndex())
        .setPageDigest(ByteString.copyFrom(page.digest().bytes()));
    for (AuxiliaryRecordReference record : page.records()) {
      builder.addRecords(CommitProtos.AuxiliaryRecordReference.newBuilder()
          .setOrdinal(record.ordinal())
          .setIdentity(ByteString.copyFrom(record.identity().bytes()))
          .setDigest(ByteString.copyFrom(record.digest().bytes()))
          .setEncodedBytes(record.encodedBytes()));
    }
    byte[] encoded = builder.build().toByteArray();
    requireLength(encoded.length, AuxiliaryPage.MAX_PAGE_BYTES);
    return encoded;
  }

  public static AuxiliaryPage decodeAuxiliaryPage(byte[] encoded) {
    requireLength(encoded.length, AuxiliaryPage.MAX_PAGE_BYTES);
    try {
      var message = CommitProtos.AuxiliaryPage.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != 1
          || !message.hasTransactionId() || !message.hasPageIndex()
          || !message.hasPageDigest() || message.getRecordsCount() > AuxiliaryPage.MAX_RECORDS) {
        throw invalid();
      }
      byte[] txn = message.getTransactionId().toByteArray();
      if (txn.length != 16) throw invalid();
      var references = new ArrayList<AuxiliaryRecordReference>(message.getRecordsCount());
      for (var record : message.getRecordsList()) {
        if (!record.getUnknownFields().asMap().isEmpty() || !record.hasOrdinal()
            || !record.hasIdentity() || !record.hasDigest() || !record.hasEncodedBytes()
            || record.getOrdinal() < 0) throw invalid();
        references.add(new AuxiliaryRecordReference(record.getOrdinal(),
            ByteKey.copyOf(record.getIdentity().toByteArray()),
            ByteKey.copyOf(record.getDigest().toByteArray()), record.getEncodedBytes()));
      }
      return AuxiliaryPage.restore(TransactionId.fromBytes(txn), message.getPageIndex(), references,
          ByteKey.copyOf(message.getPageDigest().toByteArray()));
    } catch (InvalidProtocolBufferException | IllegalArgumentException | ArithmeticException failure) {
      throw new IllegalArgumentException("invalid persisted auxiliary page", failure);
    }
  }

  public static byte[] encode(AuxiliaryDirectory directory) {
    var builder = CommitProtos.AuxiliaryDirectory.newBuilder()
        .setFormatVersion(1)
        .setTransactionId(ByteString.copyFrom(directory.transactionId().bytes()))
        .setDirectoryDigest(ByteString.copyFrom(directory.digest().bytes()))
        .setItemCount(directory.itemCount())
        .setTotalBytes(directory.totalBytes());
    for (AuxiliaryPageReference page : directory.pages()) {
      builder.addPages(CommitProtos.AuxiliaryPageReference.newBuilder()
          .setPageIndex(page.pageIndex())
          .setIdentity(ByteString.copyFrom(page.identity().bytes()))
          .setDigest(ByteString.copyFrom(page.digest().bytes()))
          .setFirstOrdinal(page.firstOrdinal())
          .setLastOrdinal(page.lastOrdinal())
          .setItemCount(page.itemCount())
          .setPayloadBytes(page.payloadBytes()));
    }
    byte[] encoded = builder.build().toByteArray();
    requireLength(encoded.length, 128 * 1024);
    return encoded;
  }

  public static AuxiliaryDirectory decodeAuxiliaryDirectory(byte[] encoded) {
    requireLength(encoded.length, 128 * 1024);
    try {
      var message = CommitProtos.AuxiliaryDirectory.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != 1
          || !message.hasTransactionId() || !message.hasDirectoryDigest()
          || !message.hasItemCount() || !message.hasTotalBytes()
          || message.getPagesCount() > AuxiliaryReference.MAX_PAGES) throw invalid();
      byte[] txn = message.getTransactionId().toByteArray();
      if (txn.length != 16) throw invalid();
      var pages = new ArrayList<AuxiliaryPageReference>(message.getPagesCount());
      for (var page : message.getPagesList()) {
        if (!page.getUnknownFields().asMap().isEmpty() || !page.hasPageIndex()
            || !page.hasIdentity() || !page.hasDigest() || !page.hasFirstOrdinal()
            || !page.hasLastOrdinal() || !page.hasItemCount() || !page.hasPayloadBytes()) {
          throw invalid();
        }
        pages.add(new AuxiliaryPageReference(page.getPageIndex(),
            ByteKey.copyOf(page.getIdentity().toByteArray()),
            ByteKey.copyOf(page.getDigest().toByteArray()), page.getFirstOrdinal(),
            page.getLastOrdinal(), page.getItemCount(), page.getPayloadBytes()));
      }
      AuxiliaryDirectory directory = AuxiliaryDirectory.restore(TransactionId.fromBytes(txn), pages,
          ByteKey.copyOf(message.getDirectoryDigest().toByteArray()));
      if (directory.itemCount() != message.getItemCount()
          || directory.totalBytes() != message.getTotalBytes()) throw invalid();
      return directory;
    } catch (InvalidProtocolBufferException | IllegalArgumentException | ArithmeticException failure) {
      throw new IllegalArgumentException("invalid persisted auxiliary directory", failure);
    }
  }

  private static CommitProtos.AuxiliaryReference encode(AuxiliaryReference reference) {
    return CommitProtos.AuxiliaryReference.newBuilder()
        .setIdentity(ByteString.copyFrom(reference.identity().bytes()))
        .setDigest(ByteString.copyFrom(reference.digest().bytes()))
        .setItemCount(reference.itemCount())
        .setTotalBytes(reference.totalBytes())
        .setPageCount(reference.pageCount())
        .build();
  }

  private static AuxiliaryReference decode(CommitProtos.AuxiliaryReference message) {
    if (!message.getUnknownFields().asMap().isEmpty() || !message.hasIdentity()
        || !message.hasDigest() || !message.hasItemCount() || !message.hasTotalBytes()
        || !message.hasPageCount()) throw invalid();
    return new AuxiliaryReference(ByteKey.copyOf(message.getIdentity().toByteArray()),
        ByteKey.copyOf(message.getDigest().toByteArray()), message.getItemCount(),
        message.getTotalBytes(), message.getPageCount());
  }

  private static int rootLimit(int formatVersion) {
    return formatVersion == FORMAT_VERSION ? MAX_ROOT_BYTES
        : formatVersion == ROOT_RESULT_FORMAT_VERSION ? MAX_RESULT_ROOT_BYTES
            : MAX_AUXILIARY_ROOT_BYTES;
  }

  private static int maxAuxiliaryRecordBytes() {
    return Math.addExact(PreparedAuxiliaryRecord.MAX_PAYLOAD_BYTES,
        PreparedAuxiliaryRecord.MAX_TARGET_IDENTITY_BYTES + 1024);
  }

  public static byte[] encode(PreparedEvent event) {
    byte[] encoded = encodeEventMessage(event).toByteArray();
    requireLength(encoded.length, PreparedEvent.MAX_EVENT_BYTES);
    return encoded;
  }

  private static CommitProtos.PreparedEvent encodeEventMessage(PreparedEvent event) {
    var builder = CommitProtos.PreparedEvent.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setTransactionId(ByteString.copyFrom(event.transactionId().bytes()))
        .setOwnerEpoch(event.ownerEpoch())
        .setBaseSequence(event.baseSequence())
        .setBaseRevision(event.baseRevision())
        .setOrdinal(event.ordinal())
        .setKind(event.kind() == PreparedEvent.Kind.PUT
            ? CommitProtos.PreparedEventKind.PREPARED_EVENT_PUT
            : CommitProtos.PreparedEventKind.PREPARED_EVENT_DELETE)
        .setCurrent(encodeRecord(event.current()))
        .setEventDigest(ByteString.copyFrom(event.digest().bytes()));
    event.previous().ifPresent(previous -> builder.setPrevious(encodeRecord(previous)));
    return builder.build();
  }

  public static PreparedEvent decodeEvent(byte[] encoded) {
    requireLength(encoded.length, PreparedEvent.MAX_EVENT_BYTES);
    try {
      var message = CommitProtos.PreparedEvent.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != FORMAT_VERSION
          || !message.hasTransactionId() || !message.hasOwnerEpoch()
          || !message.hasBaseSequence() || !message.hasBaseRevision() || !message.hasOrdinal()
          || !message.hasKind() || !message.hasCurrent() || !message.hasEventDigest()) throw invalid();
      byte[] txn = message.getTransactionId().toByteArray();
      if (txn.length != 16) throw invalid();
      PreparedEvent.Kind kind = switch (message.getKind()) {
        case PREPARED_EVENT_PUT -> PreparedEvent.Kind.PUT;
        case PREPARED_EVENT_DELETE -> PreparedEvent.Kind.DELETE;
        case PREPARED_EVENT_UNSPECIFIED, UNRECOGNIZED -> throw invalid();
      };
      Optional<StoredRecord> previous = message.hasPrevious()
          ? Optional.of(decodeRecord(message.getPrevious())) : Optional.empty();
      return PreparedEvent.restore(TransactionId.fromBytes(txn), message.getOwnerEpoch(),
          message.getBaseSequence(), message.getBaseRevision(), message.getOrdinal(), kind,
          decodeRecord(message.getCurrent()), previous,
          ByteKey.copyOf(message.getEventDigest().toByteArray()));
    } catch (InvalidProtocolBufferException | IllegalArgumentException | ArithmeticException failure) {
      throw new IllegalArgumentException("invalid persisted M2 event", failure);
    }
  }

  public static byte[] encode(EventPage page) {
    var builder = CommitProtos.EventPage.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setTransactionId(ByteString.copyFrom(page.transactionId().bytes()))
        .setPageIndex(page.pageIndex())
        .setPageDigest(ByteString.copyFrom(page.digest().bytes()));
    page.events().forEach(event -> builder.addEvents(encode(event)));
    byte[] encoded = builder.build().toByteArray();
    requireLength(encoded.length, EventPage.MAX_PAGE_BYTES);
    return encoded;
  }

  public static EventPage decodeEventPage(byte[] encoded) {
    requireLength(encoded.length, EventPage.MAX_PAGE_BYTES);
    try {
      var message = CommitProtos.EventPage.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != FORMAT_VERSION
          || !message.hasTransactionId() || !message.hasPageIndex() || !message.hasPageDigest()
          || message.getEventsCount() > EventPage.MAX_EVENTS) throw invalid();
      byte[] txn = message.getTransactionId().toByteArray();
      if (txn.length != 16) throw invalid();
      List<PreparedEventReference> events = new ArrayList<>(message.getEventsCount());
      for (var event : message.getEventsList()) events.add(decodeEventReference(event));
      return EventPage.restore(TransactionId.fromBytes(txn), message.getPageIndex(), events,
          ByteKey.copyOf(message.getPageDigest().toByteArray()));
    } catch (InvalidProtocolBufferException | IllegalArgumentException | ArithmeticException failure) {
      throw new IllegalArgumentException("invalid persisted M2 event page", failure);
    }
  }

  public static byte[] encode(EventDirectoryPage page) {
    var builder = CommitProtos.EventDirectoryPage.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setTransactionId(ByteString.copyFrom(page.transactionId().bytes()))
        .setPageIndex(page.pageIndex()).setLevel(page.level())
        .setPageDigest(ByteString.copyFrom(page.digest().bytes()));
    page.children().forEach(child -> builder.addChildren(encode(child)));
    byte[] encoded = builder.build().toByteArray();
    requireLength(encoded.length, MAX_EVENT_DIRECTORY_BYTES);
    return encoded;
  }

  public static EventDirectoryPage decodeEventDirectory(byte[] encoded) {
    requireLength(encoded.length, MAX_EVENT_DIRECTORY_BYTES);
    try {
      var message = CommitProtos.EventDirectoryPage.parseFrom(encoded);
      if (!message.getUnknownFields().asMap().isEmpty()
          || !message.hasFormatVersion() || message.getFormatVersion() != FORMAT_VERSION
          || !message.hasTransactionId() || !message.hasPageIndex() || !message.hasLevel()
          || !message.hasPageDigest() || message.getChildrenCount() > EventDirectoryPage.MAX_CHILDREN) {
        throw invalid();
      }
      byte[] txn = message.getTransactionId().toByteArray();
      if (txn.length != 16) throw invalid();
      List<EventReference> children = new ArrayList<>(message.getChildrenCount());
      for (var child : message.getChildrenList()) children.add(decodeEventReference(child));
      return EventDirectoryPage.restore(TransactionId.fromBytes(txn), message.getPageIndex(),
          message.getLevel(), children, ByteKey.copyOf(message.getPageDigest().toByteArray()));
    } catch (InvalidProtocolBufferException | IllegalArgumentException | ArithmeticException failure) {
      throw new IllegalArgumentException("invalid persisted M2 event directory", failure);
    }
  }

  private static CommitProtos.CandidateReference encode(ManifestCandidateReference candidate) {
    return CommitProtos.CandidateReference.newBuilder()
        .setBucket(candidate.bucket())
        .setLogicalKey(ByteString.copyFrom(candidate.logicalKey().bytes()))
        .setCandidateIdentity(ByteString.copyFrom(candidate.candidateIdentity().bytes()))
        .setCandidateDigest(ByteString.copyFrom(candidate.candidateDigest().bytes()))
        .setValueDigest(ByteString.copyFrom(candidate.valueDigest().bytes()))
        .setValueLength(candidate.valueLength())
        .setTombstone(candidate.tombstone()).build();
  }

  private static ManifestCandidateReference decode(CommitProtos.CandidateReference message) {
    if (!message.getUnknownFields().asMap().isEmpty()
        || !message.hasBucket() || !message.hasLogicalKey() || !message.hasCandidateIdentity()
        || !message.hasCandidateDigest() || !message.hasValueDigest()
        || !message.hasValueLength() || !message.hasTombstone()) throw invalid();
    return new ManifestCandidateReference(message.getBucket(),
        ByteKey.copyOf(message.getLogicalKey().toByteArray()),
        ByteKey.copyOf(message.getCandidateIdentity().toByteArray()),
        ByteKey.copyOf(message.getCandidateDigest().toByteArray()),
        ByteKey.copyOf(message.getValueDigest().toByteArray()), message.getValueLength(),
        message.getTombstone());
  }

  private static CommitProtos.ManifestPageReference encode(ManifestChildReference child) {
    return CommitProtos.ManifestPageReference.newBuilder()
        .setPageIdentity(ByteString.copyFrom(child.identity().bytes()))
        .setPageDigest(ByteString.copyFrom(child.digest().bytes()))
        .setFirstBucket(child.firstBucket())
        .setLastBucket(child.lastBucket())
        .setFirstKey(ByteString.copyFrom(child.firstKey().bytes()))
        .setLastKey(ByteString.copyFrom(child.lastKey().bytes()))
        .setItemCount(child.itemCount())
        .setPageCount(child.leafPageCount())
        .setValueBytes(child.valueBytes())
        .setLevel(child.level()).build();
  }

  private static CommitProtos.EventPageReference encode(EventReference child) {
    return CommitProtos.EventPageReference.newBuilder()
        .setIdentity(ByteString.copyFrom(child.identity().bytes()))
        .setDigest(ByteString.copyFrom(child.digest().bytes()))
        .setFirstOrdinal(child.firstOrdinal()).setLastOrdinal(child.lastOrdinal())
        .setItemCount(child.itemCount()).setTotalBytes(child.totalBytes())
        .setLeafPageCount(child.leafPageCount()).setLevel(child.level()).build();
  }

  private static CommitProtos.PreparedEventReference encode(PreparedEventReference event) {
    return CommitProtos.PreparedEventReference.newBuilder()
        .setOrdinal(event.ordinal()).setIdentity(ByteString.copyFrom(event.identity().bytes()))
        .setDigest(ByteString.copyFrom(event.digest().bytes())).setEncodedBytes(event.encodedBytes())
        .build();
  }

  private static PreparedEventReference decodeEventReference(
      CommitProtos.PreparedEventReference message) {
    if (!message.getUnknownFields().asMap().isEmpty() || !message.hasOrdinal()
        || !message.hasIdentity() || !message.hasDigest() || !message.hasEncodedBytes()) {
      throw invalid();
    }
    return new PreparedEventReference(message.getOrdinal(),
        ByteKey.copyOf(message.getIdentity().toByteArray()),
        ByteKey.copyOf(message.getDigest().toByteArray()), message.getEncodedBytes());
  }

  private static EventReference decodeEventReference(CommitProtos.EventPageReference message) {
    if (!message.getUnknownFields().asMap().isEmpty() || !message.hasIdentity()
        || !message.hasDigest() || !message.hasFirstOrdinal() || !message.hasLastOrdinal()
        || !message.hasItemCount() || !message.hasTotalBytes() || !message.hasLeafPageCount()
        || !message.hasLevel()) throw invalid();
    return new EventReference(ByteKey.copyOf(message.getIdentity().toByteArray()),
        ByteKey.copyOf(message.getDigest().toByteArray()), message.getFirstOrdinal(),
        message.getLastOrdinal(), message.getItemCount(), message.getTotalBytes(),
        message.getLeafPageCount(), message.getLevel());
  }

  private static CommitProtos.EventRecord encodeRecord(StoredRecord record) {
    var metadata = record.metadata();
    var builder = CommitProtos.EventRecord.newBuilder()
        .setLogicalKey(ByteString.copyFrom(record.key().bytes()))
        .setCreateRevision(metadata.createRevision())
        .setModificationRevision(metadata.modificationRevision())
        .setVersion(metadata.version()).setLeaseId(metadata.leaseId())
        .setTombstone(metadata.state() == StorageMetadata.State.TOMBSTONE)
        .setValueLength(metadata.valueLength());
    record.value().ifPresent(value -> builder.setValue(ByteString.copyFrom(value)));
    return builder.build();
  }

  private static StoredRecord decodeRecord(CommitProtos.EventRecord message) {
    if (!message.getUnknownFields().asMap().isEmpty() || !message.hasLogicalKey()
        || !message.hasCreateRevision() || !message.hasModificationRevision()
        || !message.hasVersion() || !message.hasLeaseId() || !message.hasTombstone()
        || !message.hasValueLength()) throw invalid();
    byte[] key = message.getLogicalKey().toByteArray();
    if (key.length == 0) throw invalid();
    boolean tombstone = message.getTombstone();
    if (tombstone == message.hasValue()) throw invalid();
    byte[] value = message.hasValue() ? message.getValue().toByteArray() : new byte[0];
    if (value.length != message.getValueLength() || value.length > PreparedCandidate.MAX_VALUE_BYTES) {
      throw invalid();
    }
    StorageMetadata metadata = new StorageMetadata(message.getCreateRevision(),
        message.getModificationRevision(), message.getVersion(), message.getLeaseId(),
        tombstone ? StorageMetadata.State.TOMBSTONE : StorageMetadata.State.LIVE, value.length);
    return tombstone ? StoredRecord.metadataOnly(ByteKey.copyOf(key), metadata)
        : StoredRecord.live(ByteKey.copyOf(key), metadata, value);
  }

  private static ManifestChildReference decode(CommitProtos.ManifestPageReference message) {
    if (!message.getUnknownFields().asMap().isEmpty()
        || !message.hasPageIdentity() || !message.hasPageDigest()
        || !message.hasFirstBucket() || !message.hasLastBucket()
        || !message.hasFirstKey() || !message.hasLastKey() || !message.hasItemCount()
        || !message.hasPageCount() || !message.hasValueBytes() || !message.hasLevel()) throw invalid();
    return new ManifestChildReference(ByteKey.copyOf(message.getPageIdentity().toByteArray()),
        ByteKey.copyOf(message.getPageDigest().toByteArray()),
        message.getFirstBucket(), message.getLastBucket(),
        ByteKey.copyOf(message.getFirstKey().toByteArray()),
        ByteKey.copyOf(message.getLastKey().toByteArray()), message.getItemCount(),
        message.getValueBytes(), message.getPageCount(), message.getLevel());
  }

  private static void requireLength(int length, int max) {
    if (length <= 0 || length > max) throw invalid();
  }

  private static void requireRootKeyLength(int length) {
    if (length <= 0 || length > HConstants.MAX_ROW_LENGTH) throw invalid();
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("M2 manifest message violates its storage contract");
  }

  private static final class HBaseSystemRecordLimits {
    private static final int MAX_CANDIDATE_BYTES = 5 * 1024 * 1024;
  }
}
