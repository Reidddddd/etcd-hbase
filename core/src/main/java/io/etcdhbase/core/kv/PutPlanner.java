package io.etcdhbase.core.kv;

import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Objects;
import java.util.Optional;

/** Plans one standalone Put against a complete record from the invocation's fixed base view. */
public final class PutPlanner {
  public record Plan(PreparedCandidate candidate, PreparedEvent event, PutResult result) {
    public Plan {
      Objects.requireNonNull(candidate, "candidate");
      Objects.requireNonNull(event, "event");
      Objects.requireNonNull(result, "result");
    }
  }

  public Plan plan(PutCommand command, InvocationBinding invocation, Optional<StoredRecord> baseRecord) {
    return plan(command, invocation, baseRecord, false, false);
  }

  public Plan planWithLease(PutCommand command, InvocationBinding invocation,
      Optional<StoredRecord> baseRecord) {
    return plan(command, invocation, baseRecord, false, true);
  }

  /** The caller must obtain an in-transaction record from its verified completed workset. */
  Plan planWorking(PutCommand command, InvocationBinding invocation, Optional<StoredRecord> current) {
    return plan(command, invocation, current, true, false);
  }

  Plan planWorkingWithLease(PutCommand command, InvocationBinding invocation,
      Optional<StoredRecord> current) {
    return plan(command, invocation, current, true, true);
  }

  private Plan plan(PutCommand command, InvocationBinding invocation, Optional<StoredRecord> baseRecord,
      boolean workingView, boolean leaseEnabled) {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(baseRecord, "baseRecord");
    if (invocation.state() != InvocationBinding.State.PREPARING) {
      throw new IllegalArgumentException("Put requires a preparing invocation");
    }
    if (baseRecord.isPresent()) {
      StoredRecord record = baseRecord.orElseThrow();
      if (!record.key().equals(command.key())
          || record.metadata().modificationRevision() > invocation.baseRevision()
              && (!workingView || record.metadata().modificationRevision() - invocation.baseRevision() != 1)
          || record.metadata().state() == StorageMetadata.State.LIVE && !record.hasValue()) {
        throw new IllegalArgumentException("Put base record does not match its fixed read view");
      }
    }
    Optional<StoredRecord> previous = baseRecord.filter(
        record -> record.metadata().state() == StorageMetadata.State.LIVE);
    if (previous.isEmpty() && (command.ignoreValue()
        || command.leaseIntent() == PutCommand.LeaseIntent.PRESERVE)) {
      throw new KvFailure(KvFailure.Reason.KEY_NOT_FOUND);
    }
    if (!leaseEnabled && (command.leaseIntent() == PutCommand.LeaseIntent.EXPLICIT
        || previous.filter(record -> record.metadata().leaseId() != 0).isPresent())) {
      throw new KvFailure(KvFailure.Reason.LEASE_UNAVAILABLE);
    }
    long revision = Math.addExact(invocation.baseRevision(), 1);
    long createRevision = previous.map(record -> record.metadata().createRevision()).orElse(revision);
    long version = previous.isPresent() ? Math.addExact(previous.orElseThrow().metadata().version(), 1) : 1;
    byte[] value = command.ignoreValue() ? previous.orElseThrow().value().orElseThrow() : command.value();
    long leaseId = switch (command.leaseIntent()) {
      case NONE -> 0;
      case EXPLICIT -> command.leaseId();
      case PRESERVE -> previous.orElseThrow().metadata().leaseId();
    };
    var metadata = new StorageMetadata(createRevision, revision, version, leaseId,
        StorageMetadata.State.LIVE, value.length);
    StoredRecord current = StoredRecord.live(command.key(), metadata, value);
    PreparedCandidate candidate = new PreparedCandidate(invocation.transactionId(),
        invocation.preparedOwnerEpoch(), invocation.baseSequence(), invocation.baseRevision(), current);
    PreparedEvent event = new PreparedEvent(invocation.transactionId(), invocation.preparedOwnerEpoch(),
        invocation.baseSequence(), invocation.baseRevision(), 0, PreparedEvent.Kind.PUT, current, previous);
    PutResult result = new PutResult(command.key(), revision,
        command.previousRequested() ? previous : Optional.empty());
    return new Plan(candidate, event, result);
  }
}
