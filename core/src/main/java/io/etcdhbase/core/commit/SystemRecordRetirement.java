package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.List;

/**
 * Immutable permission and outcome models for system-record retirement.
 *
 * <p>A retire never deletes a value on its own. The record first moves to a durable RETIRED state
 * that carries the maintenance generation which retired it, and only then is its payload removed.
 * The retained tombstone is what stops a late {@code createIfAbsent} or an old absent-CAS from
 * resurrecting a record that GC already decided was unreachable.
 *
 * <p>See {@code docs/specs/2026-11-20-m5-auth-gc-lifecycle-design.md} §6.2 (per-domain retention
 * rules) and §6.3 (retirement state and generation for directories and system records).
 */
public final class SystemRecordRetirement {
  private SystemRecordRetirement() {}
  /** Why a record was considered unreachable; recorded for diagnosis, never used as permission. */
  public enum Reason {
    COMPACTED_HISTORY("compacted-history"),
    UNREFERENCED_CLOSURE("unreferenced-closure"),
    CLOSED_INVOCATION_WINDOW("closed-invocation-window");

    private final String label;

    Reason(String label) { this.label = label; }

    public String label() { return label; }
  }

  /**
   * A permit for one collection scope. It binds the table group profile, both sealed pin registries,
   * the owner and authorization fences, and the selected Compact operation, so a permit can
   * never be replayed against another table group or after its scope closed.
   */
  public static final class Permit {
    private final ByteKey profile;
    private final ByteKey registryIdentity;
    private final InvocationId operationId;
    private final long generation;
    private final long safeRevision;
    private final long safeSequence;
    private final RequestScope scope;

    private final Authority authority;
    private final List<SnapshotGcBoundary.Protection> protections;

    Permit(ByteKey profile, ByteKey registryIdentity, InvocationId operationId, long generation,
        long safeRevision, long safeSequence, Authority authority,
        List<SnapshotGcBoundary.Protection> protections, RequestScope scope) {
      this.profile = Objects.requireNonNull(profile, "profile");
      this.registryIdentity = Objects.requireNonNull(registryIdentity, "registryIdentity");
      this.operationId = Objects.requireNonNull(operationId, "operationId");
      this.scope = Objects.requireNonNull(scope, "scope");
      this.authority = Objects.requireNonNull(authority, "authority");
      this.protections = List.copyOf(protections);
      if (profile.length() != 32 || registryIdentity.length() != 16
          || generation <= 0 || safeRevision < 0 || safeSequence < 0) {
        throw new IllegalArgumentException("invalid system-record retirement permit");
      }
      this.generation = generation;
      this.safeRevision = safeRevision;
      this.safeSequence = safeSequence;
    }

    public Authority authority() { return authority; }
    public List<SnapshotGcBoundary.Protection> protections() { return protections; }

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
  public record Outcome(boolean retiredNow, boolean alreadyRetired, long visiblePayloadBytes) {
    public Outcome {
      if (retiredNow && alreadyRetired) {
        throw new IllegalArgumentException("a retirement is either new or already applied");
      }
      if (visiblePayloadBytes < 0) {
        throw new IllegalArgumentException("negative payload bytes");
      }
    }
  }

  /** Immutable identities needed for independent validation by the target backend. */
  public record Authority(CommitOwnerService.OwnerToken owner, long authorizationGeneration,
      SnapshotPinConfiguration kvConfiguration, SnapshotPinConfiguration eventConfiguration,
      ByteKey eventRegistryIdentity, ByteKey eventNamespace) {
    public Authority {
      Objects.requireNonNull(owner, "owner");
      Objects.requireNonNull(kvConfiguration, "kvConfiguration");
      Objects.requireNonNull(eventConfiguration, "eventConfiguration");
      Objects.requireNonNull(eventRegistryIdentity, "eventRegistryIdentity");
      Objects.requireNonNull(eventNamespace, "eventNamespace");
      if (authorizationGeneration < 0 || eventRegistryIdentity.length() != 16 || eventNamespace.length() != 16) {
        throw new IllegalArgumentException("invalid retirement authority identity");
      }
    }
  }
}
