package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;
import java.util.Optional;

/** Immutable logical control state; HBase serialization stays in the adapter module. */
public final class CommitState {
  public static final int FORMAT_VERSION = 1;
  public static final int AUXILIARY_FORMAT_VERSION = 2;
  public static final int LEASE_BINDING_FORMAT_VERSION = 3;
  public static final int LEASE_EXPIRY_FORMAT_VERSION = 4;
  public static final int EVENT_INDEX_FORMAT_VERSION = 5;
  public static final int EVENT_REGISTRY_FORMAT_VERSION = 6;
  public static final int MAX_EVENT_REGISTRY_ATTEMPTS = 32;
  public static final int MAX_OWNER_INSTANCE_BYTES = 128;

  private final ByteKey profileFingerprint;
  private final int formatVersion;
  private final Optional<String> ownerInstance;
  private final long ownerEpoch;
  private final long ownerLeaseDeadlineEpochMillis;
  private final long stateVersion;
  private final long publishedSequence;
  private final long publishedRevision;
  private final long finalizedSequence;
  private final long finalizedRevision;
  private final Optional<PublicationSlot> publication;
  private final long compactRevision;
  private final Optional<ByteKey> eventRegistryNamespace;
  private final Optional<ByteKey> pendingEventRegistryNamespace;
  private final int eventRegistryAttempts;
  private final boolean authorizationBarrierEnabled;
  private final long authorizationBarrierGeneration;

  private CommitState(
      int formatVersion,
      ByteKey profileFingerprint,
      Optional<String> ownerInstance,
      long ownerEpoch,
      long ownerLeaseDeadlineEpochMillis,
      long stateVersion,
      long publishedSequence,
      long publishedRevision,
      long finalizedSequence,
      long finalizedRevision,
      Optional<PublicationSlot> publication,
      long compactRevision) {
    this(formatVersion, profileFingerprint, ownerInstance, ownerEpoch, ownerLeaseDeadlineEpochMillis,
        stateVersion, publishedSequence, publishedRevision, finalizedSequence, finalizedRevision,
        publication, compactRevision, Optional.empty(), Optional.empty(), 0, false, 0);
  }

  private CommitState(int formatVersion, ByteKey profileFingerprint, Optional<String> ownerInstance,
      long ownerEpoch, long ownerLeaseDeadlineEpochMillis, long stateVersion, long publishedSequence,
      long publishedRevision, long finalizedSequence, long finalizedRevision,
      Optional<PublicationSlot> publication, long compactRevision, Optional<ByteKey> eventRegistryNamespace,
      Optional<ByteKey> pendingEventRegistryNamespace, int eventRegistryAttempts) {
    this(formatVersion, profileFingerprint, ownerInstance, ownerEpoch, ownerLeaseDeadlineEpochMillis,
        stateVersion, publishedSequence, publishedRevision, finalizedSequence, finalizedRevision,
        publication, compactRevision, eventRegistryNamespace, pendingEventRegistryNamespace,
        eventRegistryAttempts, false, 0);
  }

  private CommitState(int formatVersion, ByteKey profileFingerprint, Optional<String> ownerInstance,
      long ownerEpoch, long ownerLeaseDeadlineEpochMillis, long stateVersion, long publishedSequence,
      long publishedRevision, long finalizedSequence, long finalizedRevision,
      Optional<PublicationSlot> publication, long compactRevision, Optional<ByteKey> eventRegistryNamespace,
      Optional<ByteKey> pendingEventRegistryNamespace, int eventRegistryAttempts,
      boolean authorizationBarrierEnabled, long authorizationBarrierGeneration) {
    this.profileFingerprint = Objects.requireNonNull(profileFingerprint, "profileFingerprint");
    this.ownerInstance = Objects.requireNonNull(ownerInstance, "ownerInstance");
    this.publication = Objects.requireNonNull(publication, "publication");
    this.eventRegistryNamespace = Objects.requireNonNull(eventRegistryNamespace, "eventRegistryNamespace");
    this.pendingEventRegistryNamespace = Objects.requireNonNull(pendingEventRegistryNamespace,
        "pendingEventRegistryNamespace");
    if (formatVersion != FORMAT_VERSION && formatVersion != AUXILIARY_FORMAT_VERSION
        && formatVersion != LEASE_BINDING_FORMAT_VERSION
        && formatVersion != LEASE_EXPIRY_FORMAT_VERSION
        && formatVersion != EVENT_INDEX_FORMAT_VERSION && formatVersion != EVENT_REGISTRY_FORMAT_VERSION) {
      throw new IllegalArgumentException("unknown commit control format");
    }
    if ((formatVersion >= EVENT_REGISTRY_FORMAT_VERSION) != eventRegistryNamespace.isPresent()
        || eventRegistryNamespace.filter(value -> value.length() != 16).isPresent()
        || pendingEventRegistryNamespace.filter(value -> value.length() != 16).isPresent()
        || eventRegistryAttempts < 0 || eventRegistryAttempts > MAX_EVENT_REGISTRY_ATTEMPTS
        || formatVersion < EVENT_INDEX_FORMAT_VERSION
            && (eventRegistryAttempts != 0 || pendingEventRegistryNamespace.isPresent())
        || formatVersion == EVENT_INDEX_FORMAT_VERSION
            && (eventRegistryAttempts > 0) != pendingEventRegistryNamespace.isPresent()
        || formatVersion >= EVENT_REGISTRY_FORMAT_VERSION
            && (eventRegistryAttempts == 0 || pendingEventRegistryNamespace.isPresent())
        || authorizationBarrierGeneration < 0
        || !authorizationBarrierEnabled && authorizationBarrierGeneration != 0) {
      throw new IllegalArgumentException("invalid event registry or authorization barrier state");
    }
    this.eventRegistryAttempts = eventRegistryAttempts;
    this.authorizationBarrierEnabled = authorizationBarrierEnabled;
    this.authorizationBarrierGeneration = authorizationBarrierGeneration;
    this.formatVersion = formatVersion;
    validateProfileFingerprint(profileFingerprint);
    validateCountersAndWatermarks(ownerEpoch, stateVersion, publishedSequence, publishedRevision,
        finalizedSequence, finalizedRevision, compactRevision);
    validateOwnerLease(ownerInstance, ownerEpoch, ownerLeaseDeadlineEpochMillis);
    validatePublication(publishedSequence, publishedRevision, finalizedSequence,
        finalizedRevision, ownerEpoch, publication);
    this.ownerEpoch = ownerEpoch;
    this.ownerLeaseDeadlineEpochMillis = ownerLeaseDeadlineEpochMillis;
    this.stateVersion = stateVersion;
    this.publishedSequence = publishedSequence;
    this.publishedRevision = publishedRevision;
    this.finalizedSequence = finalizedSequence;
    this.finalizedRevision = finalizedRevision;
    this.compactRevision = compactRevision;
  }

  public static CommitState initial(ByteKey profileFingerprint, long initialRevision) {
    if (initialRevision <= 0) throw new IllegalArgumentException("initial revision must be positive");
    return new CommitState(FORMAT_VERSION, profileFingerprint, Optional.empty(), 0, 0, 1,
        0, initialRevision, 0, initialRevision, Optional.empty(), 0);
  }

  /** Restores a state decoded from its single authoritative durable control row. */
  public static CommitState restore(
      ByteKey profileFingerprint,
      Optional<String> ownerInstance,
      long ownerEpoch,
      long ownerLeaseDeadlineEpochMillis,
      long stateVersion,
      long publishedSequence,
      long publishedRevision,
      long finalizedSequence,
      long finalizedRevision,
      Optional<PublicationSlot> publication,
      long compactRevision) {
    return restore(FORMAT_VERSION, profileFingerprint, ownerInstance, ownerEpoch,
        ownerLeaseDeadlineEpochMillis, stateVersion, publishedSequence, publishedRevision,
        finalizedSequence, finalizedRevision, publication, compactRevision);
  }

  public static CommitState restore(int formatVersion, ByteKey profileFingerprint,
      Optional<String> ownerInstance, long ownerEpoch, long ownerLeaseDeadlineEpochMillis,
      long stateVersion, long publishedSequence, long publishedRevision, long finalizedSequence,
      long finalizedRevision, Optional<PublicationSlot> publication, long compactRevision) {
    return new CommitState(formatVersion, profileFingerprint, ownerInstance, ownerEpoch,
        ownerLeaseDeadlineEpochMillis, stateVersion, publishedSequence, publishedRevision,
        finalizedSequence, finalizedRevision, publication, compactRevision);
  }

  public static CommitState restore(int formatVersion, ByteKey profileFingerprint,
      Optional<String> ownerInstance, long ownerEpoch, long ownerLeaseDeadlineEpochMillis,
      long stateVersion, long publishedSequence, long publishedRevision, long finalizedSequence,
      long finalizedRevision, Optional<PublicationSlot> publication, long compactRevision,
      Optional<ByteKey> eventRegistryNamespace, Optional<ByteKey> pendingEventRegistryNamespace,
      int eventRegistryAttempts) {
    return new CommitState(formatVersion, profileFingerprint, ownerInstance, ownerEpoch,
        ownerLeaseDeadlineEpochMillis, stateVersion, publishedSequence, publishedRevision,
        finalizedSequence, finalizedRevision, publication, compactRevision, eventRegistryNamespace,
        pendingEventRegistryNamespace, eventRegistryAttempts);
  }

  public static CommitState restore(int formatVersion, ByteKey profileFingerprint,
      Optional<String> ownerInstance, long ownerEpoch, long ownerLeaseDeadlineEpochMillis,
      long stateVersion, long publishedSequence, long publishedRevision, long finalizedSequence,
      long finalizedRevision, Optional<PublicationSlot> publication, long compactRevision,
      Optional<ByteKey> eventRegistryNamespace, Optional<ByteKey> pendingEventRegistryNamespace,
      int eventRegistryAttempts, long authorizationBarrierGeneration) {
    return new CommitState(formatVersion, profileFingerprint, ownerInstance, ownerEpoch,
        ownerLeaseDeadlineEpochMillis, stateVersion, publishedSequence, publishedRevision,
        finalizedSequence, finalizedRevision, publication, compactRevision, eventRegistryNamespace,
        pendingEventRegistryNamespace, eventRegistryAttempts, true, authorizationBarrierGeneration);
  }

  public static CommitState restore(int formatVersion, ByteKey profileFingerprint,
      Optional<String> ownerInstance, long ownerEpoch, long ownerLeaseDeadlineEpochMillis,
      long stateVersion, long publishedSequence, long publishedRevision, long finalizedSequence,
      long finalizedRevision, Optional<PublicationSlot> publication, long compactRevision,
      Optional<ByteKey> eventRegistryNamespace, Optional<ByteKey> pendingEventRegistryNamespace,
      int eventRegistryAttempts, boolean authorizationBarrierEnabled,
      long authorizationBarrierGeneration) {
    return new CommitState(formatVersion, profileFingerprint, ownerInstance, ownerEpoch,
        ownerLeaseDeadlineEpochMillis, stateVersion, publishedSequence, publishedRevision,
        finalizedSequence, finalizedRevision, publication, compactRevision, eventRegistryNamespace,
        pendingEventRegistryNamespace, eventRegistryAttempts, authorizationBarrierEnabled,
        authorizationBarrierGeneration);
  }

  public int formatVersion() { return formatVersion; }
  public boolean supportsAuxiliaryRecords() { return formatVersion >= AUXILIARY_FORMAT_VERSION; }
  public boolean supportsLeaseBindings() { return formatVersion >= LEASE_BINDING_FORMAT_VERSION; }
  public boolean supportsLeaseExpiryCheckpoints() { return formatVersion >= LEASE_EXPIRY_FORMAT_VERSION; }
  public boolean supportsEventRevisionIndex() { return formatVersion >= EVENT_INDEX_FORMAT_VERSION; }
  public boolean supportsAuthorizationBarrier() { return authorizationBarrierEnabled; }
  public ByteKey profileFingerprint() { return profileFingerprint; }
  public Optional<String> ownerInstance() { return ownerInstance; }
  public long ownerEpoch() { return ownerEpoch; }
  public long ownerLeaseDeadlineEpochMillis() { return ownerLeaseDeadlineEpochMillis; }
  public long stateVersion() { return stateVersion; }
  public long publishedSequence() { return publishedSequence; }
  public long publishedRevision() { return publishedRevision; }
  public long finalizedSequence() { return finalizedSequence; }
  public long finalizedRevision() { return finalizedRevision; }
  public Optional<PublicationSlot> publication() { return publication; }
  public long compactRevision() { return compactRevision; }
  public Optional<ByteKey> eventRegistryNamespace() { return eventRegistryNamespace; }
  public Optional<ByteKey> pendingEventRegistryNamespace() { return pendingEventRegistryNamespace; }
  public int eventRegistryAttempts() { return eventRegistryAttempts; }
  public long authorizationBarrierGeneration() { return authorizationBarrierGeneration; }

  /** Fences the old owner in the same CAS that enables the auxiliary control format. */
  public CommitState upgradeAuxiliaryFormat(String expectedOwner, long expectedEpoch, long nowMillis) {
    requireOwner(expectedOwner, expectedEpoch);
    if (formatVersion != FORMAT_VERSION || publication.isPresent() || ownerLeaseExpired(nowMillis)) {
      throw new IllegalStateException("control state cannot be upgraded while active work is pending");
    }
    return new CommitState(AUXILIARY_FORMAT_VERSION, profileFingerprint, ownerInstance,
        Math.addExact(ownerEpoch, 1), ownerLeaseDeadlineEpochMillis, nextStateVersion(),
        publishedSequence, publishedRevision, finalizedSequence, finalizedRevision,
        publication, compactRevision, eventRegistryNamespace, pendingEventRegistryNamespace,
        eventRegistryAttempts, authorizationBarrierEnabled, authorizationBarrierGeneration);
  }

  /** Fences J40 writers before J41 persists Lease binding histories and accounting. */
  public CommitState upgradeLeaseBindingFormat(
      String expectedOwner, long expectedEpoch, long nowMillis) {
    requireOwner(expectedOwner, expectedEpoch);
    if (formatVersion != AUXILIARY_FORMAT_VERSION || publication.isPresent()
        || ownerLeaseExpired(nowMillis)) {
      throw new IllegalStateException("control state cannot enable Lease bindings while work is pending");
    }
    return new CommitState(LEASE_BINDING_FORMAT_VERSION, profileFingerprint, ownerInstance,
        Math.addExact(ownerEpoch, 1), ownerLeaseDeadlineEpochMillis, nextStateVersion(),
        publishedSequence, publishedRevision, finalizedSequence, finalizedRevision,
        publication, compactRevision, eventRegistryNamespace, pendingEventRegistryNamespace,
        eventRegistryAttempts, authorizationBarrierEnabled, authorizationBarrierGeneration);
  }

  /** Fences J41 owners before J43 persists expiry checkpoints and runs automatic revocation. */
  public CommitState upgradeLeaseExpiryFormat(
      String expectedOwner, long expectedEpoch, long nowMillis) {
    requireOwner(expectedOwner, expectedEpoch);
    if (formatVersion != LEASE_BINDING_FORMAT_VERSION || publication.isPresent()
        || ownerLeaseExpired(nowMillis)) {
      throw new IllegalStateException("control state cannot enable Lease expiry while work is pending");
    }
    return new CommitState(LEASE_EXPIRY_FORMAT_VERSION, profileFingerprint, ownerInstance,
        Math.addExact(ownerEpoch, 1), ownerLeaseDeadlineEpochMillis, nextStateVersion(),
        publishedSequence, publishedRevision, finalizedSequence, finalizedRevision,
        publication, compactRevision, eventRegistryNamespace, pendingEventRegistryNamespace,
        eventRegistryAttempts, authorizationBarrierEnabled, authorizationBarrierGeneration);
  }

  /** Fences older owners before event indexes become mandatory for new KV commits. */
  public CommitState upgradeEventIndexFormat(String expectedOwner, long expectedEpoch, long nowMillis) {
    requireOwner(expectedOwner, expectedEpoch);
    if (formatVersion != LEASE_EXPIRY_FORMAT_VERSION || publication.isPresent()
        || ownerLeaseExpired(nowMillis)) {
      throw new IllegalStateException("control state cannot enable event indexes while work is pending");
    }
    return new CommitState(EVENT_INDEX_FORMAT_VERSION, profileFingerprint, ownerInstance,
        Math.addExact(ownerEpoch, 1), ownerLeaseDeadlineEpochMillis, nextStateVersion(),
        publishedSequence, publishedRevision, finalizedSequence, finalizedRevision,
        publication, compactRevision, eventRegistryNamespace, pendingEventRegistryNamespace,
        eventRegistryAttempts, authorizationBarrierEnabled, authorizationBarrierGeneration);
  }

  /** Reserves one bounded staging attempt and fences earlier attempts before any staging writes. */
  public CommitState reserveEventRegistry(String expectedOwner, long expectedEpoch, long nowMillis,
      ByteKey namespace) {
    requireOwner(expectedOwner, expectedEpoch);
    Objects.requireNonNull(namespace, "namespace");
    if (formatVersion != EVENT_INDEX_FORMAT_VERSION || publication.isPresent() || ownerLeaseExpired(nowMillis)
        || namespace.length() != 16 || eventRegistryAttempts == MAX_EVENT_REGISTRY_ATTEMPTS) {
      throw new IllegalStateException("control state cannot reserve an event registry attempt");
    }
    return new CommitState(formatVersion, profileFingerprint, ownerInstance,
        Math.addExact(ownerEpoch, 1), ownerLeaseDeadlineEpochMillis, nextStateVersion(),
        publishedSequence, publishedRevision, finalizedSequence, finalizedRevision, publication,
        compactRevision, Optional.empty(), Optional.of(namespace), eventRegistryAttempts + 1,
        authorizationBarrierEnabled, authorizationBarrierGeneration);
  }

  /** Selects one fully prepared registry namespace and fences the previous owner in the same CAS. */
  public CommitState upgradeEventRegistryFormat(String expectedOwner, long expectedEpoch, long nowMillis,
      ByteKey namespace) {
    requireOwner(expectedOwner, expectedEpoch);
    Objects.requireNonNull(namespace, "namespace");
    if (formatVersion != EVENT_INDEX_FORMAT_VERSION || publication.isPresent() || ownerLeaseExpired(nowMillis)
        || !pendingEventRegistryNamespace.equals(Optional.of(namespace))) {
      throw new IllegalStateException("control state cannot select an event registry");
    }
    return new CommitState(EVENT_REGISTRY_FORMAT_VERSION, profileFingerprint, ownerInstance,
        Math.addExact(ownerEpoch, 1), ownerLeaseDeadlineEpochMillis, nextStateVersion(),
        publishedSequence, publishedRevision, finalizedSequence, finalizedRevision,
        publication, compactRevision, Optional.of(namespace), Optional.empty(), eventRegistryAttempts,
        authorizationBarrierEnabled, authorizationBarrierGeneration);
  }

  /** Fences older control-row writers before enabling authorization publication barriers. */
  public CommitState enableAuthorizationBarrier(
      String expectedOwner, long expectedEpoch, long nowMillis) {
    requireOwner(expectedOwner, expectedEpoch);
    if (authorizationBarrierEnabled || publication.isPresent() || ownerLeaseExpired(nowMillis)) {
      throw new IllegalStateException("control state cannot enable authorization fencing while work is pending");
    }
    return new CommitState(formatVersion, profileFingerprint, ownerInstance,
        Math.addExact(ownerEpoch, 1), ownerLeaseDeadlineEpochMillis, nextStateVersion(),
        publishedSequence, publishedRevision, finalizedSequence, finalizedRevision,
        publication, compactRevision, eventRegistryNamespace, pendingEventRegistryNamespace,
        eventRegistryAttempts, true, 0);
  }

  /** Advances the per-table-group authorization fence without changing an existing selection. */
  public CommitState advanceAuthorizationBarrier(
      String expectedOwner, long expectedEpoch, long expectedBarrierGeneration) {
    requireOwner(expectedOwner, expectedEpoch);
    if (!supportsAuthorizationBarrier()
        || authorizationBarrierGeneration != expectedBarrierGeneration) {
      throw new IllegalStateException("authorization barrier generation is stale");
    }
    return copy(ownerInstance, ownerEpoch, ownerLeaseDeadlineEpochMillis, nextStateVersion(),
        publishedSequence, publishedRevision, finalizedSequence, finalizedRevision,
        publication, compactRevision, Math.addExact(authorizationBarrierGeneration, 1));
  }

  public boolean ownerLeaseExpired(long nowEpochMillis) {
    if (nowEpochMillis < 0) throw new IllegalArgumentException("current time must be non-negative");
    return ownerEpoch > 0 && nowEpochMillis >= ownerLeaseDeadlineEpochMillis;
  }

  /** Claims an unowned or expired slot. The epoch CAS, not the clock, fences the old process. */
  public CommitState claimOwner(String newOwnerInstance, long nowEpochMillis, long leaseDeadlineEpochMillis) {
    requireOwnerInstance(newOwnerInstance);
    if (ownerEpoch > 0 && !ownerLeaseExpired(nowEpochMillis)) {
      throw new IllegalStateException("current commit owner lease is still active");
    }
    if (leaseDeadlineEpochMillis <= nowEpochMillis) {
      throw new IllegalArgumentException("new owner lease must end in the future");
    }
    return copy(Optional.of(newOwnerInstance), Math.addExact(ownerEpoch, 1), leaseDeadlineEpochMillis,
        nextStateVersion(), publishedSequence, publishedRevision, finalizedSequence,
        finalizedRevision, publication, compactRevision);
  }

  /** Refreshes liveness while preserving the logical sequence and any selected publication. */
  public CommitState heartbeat(String expectedOwner, long expectedEpoch, long leaseDeadlineEpochMillis) {
    requireOwner(expectedOwner, expectedEpoch);
    if (leaseDeadlineEpochMillis <= ownerLeaseDeadlineEpochMillis) {
      throw new IllegalArgumentException("owner heartbeat must extend the existing lease");
    }
    return copy(ownerInstance, ownerEpoch, leaseDeadlineEpochMillis, nextStateVersion(),
        publishedSequence, publishedRevision, finalizedSequence, finalizedRevision,
        publication, compactRevision);
  }

  public CommitState publish(PublicationSlot slot) {
    Objects.requireNonNull(slot, "slot");
    if (ownerEpoch == 0 || slot.ownerEpoch() != ownerEpoch
        || slot.baseSequence() != publishedSequence || slot.baseRevision() != publishedRevision
        || publication.isPresent()) {
      throw new IllegalStateException("publication does not match the current owner and base state");
    }
    return copy(ownerInstance, ownerEpoch, ownerLeaseDeadlineEpochMillis, nextStateVersion(),
        slot.sequence(), slot.revision(), finalizedSequence, finalizedRevision,
        Optional.of(slot), compactRevision);
  }

  /** Completes a selected transaction only after every required materialization is confirmed. */
  public CommitState finalizePublication(TransactionId transactionId) {
    Objects.requireNonNull(transactionId, "transactionId");
    PublicationSlot selected = publication.orElseThrow(
        () -> new IllegalStateException("no publication is pending"));
    if (!selected.transactionId().equals(transactionId)) {
      throw new IllegalStateException("transaction does not own the selected publication");
    }
    return copy(ownerInstance, ownerEpoch, ownerLeaseDeadlineEpochMillis, nextStateVersion(),
        publishedSequence, publishedRevision, publishedSequence, publishedRevision,
        Optional.empty(), compactRevision);
  }

  public CommitState advanceCompactRevision(long newCompactRevision) {
    if (publication.isPresent() || newCompactRevision < compactRevision
        || newCompactRevision > finalizedRevision) {
      throw new IllegalStateException("compact revision is outside the finalized safe range");
    }
    return copy(ownerInstance, ownerEpoch, ownerLeaseDeadlineEpochMillis, nextStateVersion(),
        publishedSequence, publishedRevision, finalizedSequence, finalizedRevision,
        publication, newCompactRevision);
  }

  private void requireOwner(String expectedOwner, long expectedEpoch) {
    if (ownerInstance.isEmpty() || !ownerInstance.orElseThrow().equals(expectedOwner)
        || ownerEpoch != expectedEpoch) {
      throw new IllegalStateException("commit owner fencing token is stale");
    }
  }

  private CommitState copy(Optional<String> nextOwner, long nextEpoch, long nextLeaseDeadline,
      long nextVersion, long nextPublishedSequence, long nextPublishedRevision,
      long nextFinalizedSequence, long nextFinalizedRevision,
      Optional<PublicationSlot> nextPublication, long nextCompactRevision) {
    return copy(nextOwner, nextEpoch, nextLeaseDeadline, nextVersion, nextPublishedSequence,
        nextPublishedRevision, nextFinalizedSequence, nextFinalizedRevision, nextPublication,
        nextCompactRevision, authorizationBarrierGeneration);
  }

  private CommitState copy(Optional<String> nextOwner, long nextEpoch, long nextLeaseDeadline,
      long nextVersion, long nextPublishedSequence, long nextPublishedRevision,
      long nextFinalizedSequence, long nextFinalizedRevision,
      Optional<PublicationSlot> nextPublication, long nextCompactRevision,
      long nextAuthorizationBarrierGeneration) {
    return new CommitState(formatVersion, profileFingerprint, nextOwner, nextEpoch, nextLeaseDeadline, nextVersion,
        nextPublishedSequence, nextPublishedRevision, nextFinalizedSequence,
        nextFinalizedRevision, nextPublication, nextCompactRevision, eventRegistryNamespace,
        pendingEventRegistryNamespace, eventRegistryAttempts, authorizationBarrierEnabled,
        nextAuthorizationBarrierGeneration);
  }

  private long nextStateVersion() {
    return Math.addExact(stateVersion, 1);
  }

  private static void validateProfileFingerprint(ByteKey profileFingerprint) {
    if (profileFingerprint.length() != 32) {
      throw new IllegalArgumentException("invalid commit control state");
    }
  }

  private static void validateCountersAndWatermarks(long ownerEpoch, long stateVersion,
      long publishedSequence, long publishedRevision, long finalizedSequence,
      long finalizedRevision, long compactRevision) {
    if (ownerEpoch < 0 || stateVersion <= 0 || publishedSequence < 0 || publishedRevision <= 0
        || finalizedSequence < 0 || finalizedRevision <= 0 || compactRevision < 0
        || compactRevision > finalizedRevision) {
      throw new IllegalArgumentException("invalid commit control state");
    }
  }

  private static void validateOwnerLease(Optional<String> ownerInstance, long ownerEpoch,
      long ownerLeaseDeadlineEpochMillis) {
    if ((ownerEpoch == 0) != ownerInstance.isEmpty()
        || (ownerEpoch == 0) != (ownerLeaseDeadlineEpochMillis == 0)
        || ownerLeaseDeadlineEpochMillis < 0
        || ownerInstance.isPresent() && !validOwnerInstance(ownerInstance.orElseThrow())) {
      throw new IllegalArgumentException("invalid commit control state");
    }
  }

  private static void validatePublication(long publishedSequence, long publishedRevision,
      long finalizedSequence, long finalizedRevision, long ownerEpoch,
      Optional<PublicationSlot> publication) {
    if (publishedSequence < finalizedSequence || publishedSequence - finalizedSequence > 1) {
      throw new IllegalArgumentException("published and finalized sequences violate the one-slot bound");
    }
    if (publication.isEmpty()) {
      if (publishedSequence != finalizedSequence || publishedRevision != finalizedRevision) {
        throw new IllegalArgumentException("control state has an unaccounted publication gap");
      }
      return;
    }
    PublicationSlot slot = publication.orElseThrow();
    if (finalizedSequence == Long.MAX_VALUE) {
      throw new IllegalArgumentException("publication sequence is exhausted");
    }
    long expectedSequence = finalizedSequence + 1;
    if (publishedSequence != expectedSequence
        || slot.sequence() != publishedSequence || slot.revision() != publishedRevision
        || slot.baseSequence() != finalizedSequence || slot.baseRevision() != finalizedRevision
        || slot.ownerEpoch() > ownerEpoch) {
      throw new IllegalArgumentException("publication slot does not match control state");
    }
  }

  private static boolean validOwnerInstance(String ownerInstance) {
    return !ownerInstance.isBlank() && ownerInstance.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
        <= MAX_OWNER_INSTANCE_BYTES
        && ownerInstance.codePoints().noneMatch(Character::isISOControl);
  }

  private static void requireOwnerInstance(String ownerInstance) {
    if (!validOwnerInstance(Objects.requireNonNull(ownerInstance, "ownerInstance"))) {
      throw new IllegalArgumentException("invalid commit owner instance");
    }
  }
}
