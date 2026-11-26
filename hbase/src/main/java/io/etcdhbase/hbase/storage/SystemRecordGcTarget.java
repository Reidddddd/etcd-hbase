package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.ManifestRecordFormat;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.SystemRecordEntry;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.hbase.codec.CommitDecisionMessages;
import io.etcdhbase.hbase.codec.EventRevisionIndexMessages;
import io.etcdhbase.hbase.codec.ManifestMessages;
import io.etcdhbase.hbase.codec.ResultMessages;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** Canonical transaction and independent lower fences authenticated by a retirement target. */
record SystemRecordGcTarget(TransactionId transaction, long revision, long sequence) {
  static SystemRecordGcTarget decode(SystemRecordType type, SystemRecordEntry entry) {
    byte[] value = entry.value().bytes();
    return switch (type) {
      case CANDIDATE -> {
        var candidate = ManifestMessages.decodeCandidate(value);
        SystemRecordReferences.requireIdentity(entry.identity(), candidate.systemIdentity());
        yield prepared(candidate.transactionId(), candidate.baseRevision(), candidate.baseSequence());
      }
      case EVENT -> {
        var event = ManifestMessages.decodeEvent(value);
        SystemRecordReferences.requireIdentity(entry.identity(), event.systemIdentity());
        yield prepared(event.transactionId(), event.baseRevision(), event.baseSequence());
      }
      case RESULT -> result(entry);
      case MANIFEST -> manifest(entry);
      case DECISION -> {
        var decision = CommitDecisionMessages.decode(value);
        requireSequenceIdentity(entry.identity(), decision.sequence());
        yield new SystemRecordGcTarget(decision.transactionId(), decision.revision(), decision.sequence());
      }
      case EVENT_REVISION_INDEX -> {
        var decision = EventRevisionIndexMessages.decode(value).decision();
        requireSequenceIdentity(entry.identity(), decision.revision());
        yield new SystemRecordGcTarget(decision.transactionId(), decision.revision(), decision.sequence());
      }
      default -> throw new IllegalArgumentException("record domain is not collectable");
    };
  }

  private static SystemRecordGcTarget manifest(SystemRecordEntry entry) {
    // All variants are validated by the same decoder used for their outgoing graph edges.
    SystemRecordReferences.manifestReferences(entry, SystemRecordType.MANIFEST, entry.identity());
    byte[] identity = entry.identity().bytes();
    if (identity[16] == ManifestRecordFormat.MANIFEST_ROOT) {
      var root = ManifestMessages.decodeRoot(entry.value().bytes());
      return prepared(root.transactionId(), root.baseRevision(), root.baseSequence());
    }
    // Pages carry no revision/sequence. Durable invocations and decisions constrain their transaction.
    return new SystemRecordGcTarget(TransactionId.fromBytes(Arrays.copyOf(identity, 16)), 0, 0);
  }

  private static SystemRecordGcTarget result(SystemRecordEntry entry) {
    SystemRecordReferences.resultReferences(entry, entry.identity());
    byte kind = entry.identity().bytes()[16];
    ResultIdentity identity = kind == ManifestRecordFormat.RESULT_DIRECTORY
        ? ResultMessages.decodeDirectory(entry.value().bytes()).identity()
        : ResultMessages.decodePage(entry.value().bytes()).identity();
    return prepared(identity.transactionId(), identity.baseRevision(), identity.baseSequence());
  }

  private static SystemRecordGcTarget prepared(TransactionId transaction, long revision, long sequence) {
    return new SystemRecordGcTarget(transaction, Math.addExact(revision, 1), Math.addExact(sequence, 1));
  }

  static void requireSequenceIdentity(ByteKey identity, long sequence) {
    if (identity.length() != Long.BYTES || ByteBuffer.wrap(identity.bytes()).getLong() != sequence) {
      throw new IllegalArgumentException("system record sequence identity mismatch");
    }
  }
}
