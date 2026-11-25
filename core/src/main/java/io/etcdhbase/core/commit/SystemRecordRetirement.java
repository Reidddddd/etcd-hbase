package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * The one conservative authority for retiring system records whose references have all cleared.
 *
 * <p>A retire never deletes a value on its own. The record first moves to a durable RETIRED state
 * that carries the maintenance generation which retired it, and only then is its payload removed.
 * The retained tombstone is what stops a late {@code createIfAbsent} or an old absent-CAS from
 * resurrecting a record that GC already decided was unreachable.
 *
 * <p>See {@code docs/specs/2026-11-20-m5-auth-gc-lifecycle-design.md} §6.2 (per-domain retention
 * rules) and §6.3 (retirement state and generation for directories and system records).
 */
public interface SystemRecordRetirement {
  /** Why a record was considered unreachable; recorded for diagnosis, never used as permission. */
  enum Reason {
    COMPACTED_HISTORY("compacted-history"),
    UNREFERENCED_CLOSURE("unreferenced-closure"),
    EXPIRED_INVOCATION("expired-invocation");

    private final String label;

    Reason(String label) { this.label = label; }

    public String label() { return label; }
  }

  /**
   * A permit for one bounded retirement page. It binds the table group profile, the pin registry
   * that confirmed the boundary, the Compact operation, and the caller's scope, so a permit can
   * never be replayed against another table group or after its scope closed.
   */
  final class Permit {
    private final ByteKey profile;
    private final ByteKey registryIdentity;
    private final InvocationId operationId;
    private final long generation;
    private final long safeRevision;
    private final long safeSequence;
    private final RequestScope scope;

    /** Only the GC authority may mint a permit; reachability and durability stay separated. */
    public static Permit issue(ByteKey profile, ByteKey registryIdentity, InvocationId operationId,
        long generation, long safeRevision, long safeSequence, RequestScope scope) {
      return new Permit(profile, registryIdentity, operationId, generation, safeRevision,
          safeSequence, scope);
    }

    private Permit(ByteKey profile, ByteKey registryIdentity, InvocationId operationId, long generation,
        long safeRevision, long safeSequence, RequestScope scope) {
      this.profile = Objects.requireNonNull(profile, "profile");
      this.registryIdentity = Objects.requireNonNull(registryIdentity, "registryIdentity");
      this.operationId = Objects.requireNonNull(operationId, "operationId");
      this.scope = Objects.requireNonNull(scope, "scope");
      if (generation <= 0 || safeRevision < 0 || safeSequence < 0) {
        throw new IllegalArgumentException("invalid system-record retirement permit");
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

    /** Fails closed once the permit's scope closed, expired, or targets another profile. */
    public void requireUsable(ByteKey expectedProfile, RequestScope executionScope) {
      if (!profile.equals(expectedProfile) || scope != executionScope || scope.isClosed()
          || scope.isDeadlineExceeded()) {
        throw new IllegalStateException("system-record retirement permit is no longer usable");
      }
    }
  }

  /** The durable state of one record identity after a retirement attempt. */
  record Outcome(boolean retiredNow, boolean alreadyRetired, long visiblePayloadBytes) {
    public Outcome {
      if (retiredNow && alreadyRetired) {
        throw new IllegalArgumentException("a retirement is either new or already applied");
      }
      if (visiblePayloadBytes < 0) {
        throw new IllegalArgumentException("negative payload bytes");
      }
    }
  }

  /**
   * Reads the retirement tombstone for one identity, if any. A present tombstone means the record
   * is retired even when its payload has already been removed.
   */
  CompletableFuture<Optional<RetirementTombstone>> readTombstone(
      SystemRecordType type, ByteKey identity, RequestScope scope);

  /**
   * Retires one identity only if its exact current value still matches {@code expected}. The
   * caller decides reachability; this method decides durability. A stale expectation returns a
   * non-retired outcome instead of deleting a value the caller did not observe.
   */
  CompletableFuture<Outcome> retire(SystemRecordType type, ByteKey identity, ByteKey expected,
      Permit permit, Reason reason, RequestScope scope);
}
