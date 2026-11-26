package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;

/** Authority only for redundant materialized KV versions; it never permits system-record deletion. */
public final class HistoryGcPermit {
  private final ByteKey profile;
  private final ByteKey registryIdentity;
  private final InvocationId operationId;
  private final long generation;
  private final long safeRevision;
  private final long safeSequence;
  private final RequestScope scope;

  HistoryGcPermit(ByteKey profile, ByteKey registryIdentity, InvocationId operationId,
      long generation, long safeRevision, long safeSequence, RequestScope scope) {
    this.profile = Objects.requireNonNull(profile, "profile");
    this.registryIdentity = Objects.requireNonNull(registryIdentity, "registryIdentity");
    this.operationId = Objects.requireNonNull(operationId, "operationId");
    this.scope = Objects.requireNonNull(scope, "scope");
    if (generation <= 0 || safeRevision < 0 || safeSequence < 0) {
      throw new IllegalArgumentException("invalid history GC permit");
    }
    this.generation = generation;
    this.safeRevision = safeRevision;
    this.safeSequence = safeSequence;
  }

  public ByteKey profile() { return profile; }
  public ByteKey registryIdentity() { return registryIdentity; }
  public InvocationId operationId() { return operationId; }
  public long generation() { return generation; }
  public long safeRevision() { return safeRevision; }
  public long safeSequence() { return safeSequence; }

  /** A permit cannot outlive the collection scope or be reused for a different table group. */
  public void requireUsable(ByteKey expectedProfile, RequestScope executionScope) {
    if (!profile.equals(expectedProfile) || scope != executionScope || scope.isClosed()
        || scope.isDeadlineExceeded()) throw new IllegalStateException("history GC permit is no longer usable");
  }
}
