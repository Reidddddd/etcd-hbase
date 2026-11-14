package io.etcdhbase.core.kv;

import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.util.Objects;
import java.util.Optional;

/** Plans one observed record; its caller supplies the protected transaction working view. */
public final class DeletePlanner {
  public record Plan(PreparedCandidate candidate, PreparedEvent event) {
    public Plan {
      Objects.requireNonNull(candidate, "candidate");
      Objects.requireNonNull(event, "event");
    }
  }

  /** An absent record or an earlier tombstone has no delete event and does not advance revision. */
  public Optional<Plan> plan(InvocationBinding invocation, Optional<StoredRecord> observed, long ordinal) {
    return plan(invocation, observed, ordinal, false, false);
  }

  Optional<Plan> planWithLease(InvocationBinding invocation,
      Optional<StoredRecord> observed, long ordinal) {
    return plan(invocation, observed, ordinal, false, true);
  }

  /** Prepares a delete for the shared Lease revoke path while preserving the ordinary event rules. */
  public Optional<Plan> planForLeaseRevocation(InvocationBinding invocation,
      Optional<StoredRecord> observed, long ordinal) {
    return plan(invocation, observed, ordinal, false, true);
  }

  /** The caller must obtain an in-transaction record from its verified completed workset. */
  Optional<Plan> planWorking(InvocationBinding invocation, Optional<StoredRecord> observed, long ordinal) {
    return plan(invocation, observed, ordinal, true, false);
  }

  Optional<Plan> planWorkingWithLease(InvocationBinding invocation,
      Optional<StoredRecord> observed, long ordinal) {
    return plan(invocation, observed, ordinal, true, true);
  }

  private Optional<Plan> plan(InvocationBinding invocation, Optional<StoredRecord> observed,
      long ordinal, boolean workingView, boolean leaseEnabled) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(observed, "observed");
    if (invocation.state() != InvocationBinding.State.PREPARING || ordinal < 0) {
      throw new IllegalArgumentException("delete requires a preparing invocation and a valid event ordinal");
    }
    if (observed.isEmpty()) return Optional.empty();
    StoredRecord previous = observed.orElseThrow();
    StorageMetadata metadata = previous.metadata();
    if (metadata.modificationRevision() > invocation.baseRevision()
        && (!workingView || ordinal == 0 || metadata.modificationRevision() - invocation.baseRevision() != 1)) {
      throw new IllegalArgumentException("delete record is newer than its working view");
    }
    if (metadata.state() == StorageMetadata.State.TOMBSTONE) return Optional.empty();
    if (!previous.hasValue()) throw new IllegalArgumentException("delete requires the complete previous value");
    if (!leaseEnabled && metadata.leaseId() != 0) {
      throw new KvFailure(KvFailure.Reason.LEASE_UNAVAILABLE);
    }
    long revision = Math.addExact(invocation.baseRevision(), 1);
    StoredRecord tombstone = StoredRecord.metadataOnly(previous.key(),
        new StorageMetadata(metadata.createRevision(), revision, metadata.version(), 0,
            StorageMetadata.State.TOMBSTONE, 0));
    var candidate = new PreparedCandidate(invocation.transactionId(), invocation.preparedOwnerEpoch(),
        invocation.baseSequence(), invocation.baseRevision(), tombstone);
    var event = new PreparedEvent(invocation.transactionId(), invocation.preparedOwnerEpoch(),
        invocation.baseSequence(), invocation.baseRevision(), ordinal, PreparedEvent.Kind.DELETE,
        tombstone, Optional.of(previous));
    return Optional.of(new Plan(candidate, event));
  }
}
