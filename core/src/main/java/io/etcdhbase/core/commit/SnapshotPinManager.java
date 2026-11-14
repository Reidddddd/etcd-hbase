package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/** Durable, bounded, cross-replica read protection with independent GC revision and sequence floors. */
public final class SnapshotPinManager implements ReadViewPinRegistry {
  private static final int MAX_CAS_ATTEMPTS = 32;
  private static final int MAX_INITIALIZATION_ATTEMPTS = 32;
  private static final int INITIALIZATION_BATCH = 8;
  private static final int BOUNDARY_BATCH = 16;
  private static final Duration RELEASE_TIMEOUT = Duration.ofSeconds(5);
  private static final ByteKey CONFIGURATION_ID = ByteKey.copyOf(
      new byte[] {0x53, 0x50, 0x49, 0x4e, 0x01, 0x00});
  private static final ByteKey INITIALIZATION_ID = ByteKey.copyOf(
      new byte[] {0x53, 0x50, 0x49, 0x4e, 0x01, 0x01});

  private final SystemRecordStore records;
  private final SnapshotPinConfiguration configuration;
  private final Clock clock;
  private final ByteKey registryId;

  private SnapshotPinManager(SystemRecordStore records, SnapshotPinConfiguration configuration,
      Clock clock, ByteKey registryId) {
    this.records = records;
    this.configuration = configuration;
    this.clock = clock;
    this.registryId = registryId;
  }

  /** Opens an established registry or resumes an initialization with a durable witness. */
  public static CompletableFuture<SnapshotPinManager> open(SystemRecordStore records,
      SnapshotPinConfiguration configuration, RequestScope scope) {
    return open(records, configuration, Clock.systemUTC(), scope);
  }

  /** Clock injection keeps lease expiry and fencing deterministic in core tests. */
  public static CompletableFuture<SnapshotPinManager> open(SystemRecordStore records,
      SnapshotPinConfiguration configuration, Clock clock, RequestScope scope) {
    return start(records, configuration, clock, scope, false);
  }

  /**
   * Bootstraps a verified new table group. The caller must confirm that the table is fresh, exclude
   * existing services and other bootstrap attempts, and wait for earlier bootstrap writes to stop.
   * Absent metadata alone cannot prove that an old registry never existed.
   * Abandoned staging generations are bounded and the prior format needs explicit migration.
   */
  public static CompletableFuture<SnapshotPinManager> initializeFresh(SystemRecordStore records,
      SnapshotPinConfiguration configuration, RequestScope scope) {
    return initializeFresh(records, configuration, Clock.systemUTC(), scope);
  }

  /** Clock injection keeps initial creation deterministic in core tests. */
  public static CompletableFuture<SnapshotPinManager> initializeFresh(SystemRecordStore records,
      SnapshotPinConfiguration configuration, Clock clock, RequestScope scope) {
    return start(records, configuration, clock, scope, true);
  }

  private static CompletableFuture<SnapshotPinManager> start(SystemRecordStore records,
      SnapshotPinConfiguration configuration, Clock clock, RequestScope scope, boolean bootstrap) {
    Objects.requireNonNull(records, "records");
    Objects.requireNonNull(configuration, "configuration");
    Objects.requireNonNull(clock, "clock");
    Objects.requireNonNull(scope, "scope");
    var manager = new SnapshotPinManager(records, configuration, clock,
        ByteKey.copyOf(TransactionId.create().bytes()));
    return manager.openRegistry(scope, bootstrap);
  }

  @Override
  public CompletableFuture<Pin> register(ReadView view, RequestScope scope) {
    return register(view, scope, false);
  }

  @Override
  public CompletableFuture<Pin> registerAuxiliary(ReadView view, RequestScope scope) {
    return register(view, scope, true);
  }

  private CompletableFuture<Pin> register(ReadView view, RequestScope scope, boolean auxiliary) {
    Objects.requireNonNull(view, "view");
    Objects.requireNonNull(scope, "scope");
    if (!active(scope)) return failed(inactive(scope));
    final PinRequirement requirement;
    final ByteKey pinId;
    try {
      requirement = PinRequirement.from(view, auxiliary);
      pinId = ByteKey.copyOf(TransactionId.create().bytes());
    } catch (RuntimeException failure) {
      return failed(failure);
    }
    int shard = shardFor(pinId);
    return registerAttempt(shard, pinId, requirement, scope, 0);
  }

  /**
   * Advances the minimum revision/sequence accepted by new pins on every shard. The returned
   * boundary is a candidate lower fence for each history dimension, not deletion permission.
   * Reclaimers must also honor Compact, each key's required baseline version, and publication,
   * recovery, root, decision, and result-window references.
   */
  public CompletableFuture<SnapshotGcBoundary> advanceRegistrationFloors(long revision,
      long sequence, RequestScope scope) {
    if (revision < 0 || sequence < 0) {
      return failed(new IllegalArgumentException("snapshot registration floors must be non-negative"));
    }
    Objects.requireNonNull(scope, "scope");
    if (!active(scope)) return failed(inactive(scope));
    return advanceShards(0, revision, sequence, scope).thenCompose(ignored -> readBoundary(scope));
  }

  /**
   * Persists REVOKED for expired generations before allowing them to stop protecting history.
   * Merely observing an expired timestamp never removes a slot from a safe boundary.
   */
  public CompletableFuture<SnapshotGcBoundary> revokeExpiredPins(RequestScope scope) {
    Objects.requireNonNull(scope, "scope");
    if (!active(scope)) return failed(inactive(scope));
    return revokeExpiredShards(0, scope).thenCompose(ignored -> readBoundary(scope));
  }

  /** Reads every configured shard strongly; a missing or malformed shard fails closed. */
  public CompletableFuture<SnapshotGcBoundary> readBoundary(RequestScope scope) {
    Objects.requireNonNull(scope, "scope");
    if (!active(scope)) return failed(inactive(scope));
    return readBoundaryBatch(0, new ArrayList<>(), scope).thenApply(this::boundary);
  }

  private CompletableFuture<SnapshotPinManager> openRegistry(RequestScope scope, boolean bootstrap) {
    if (!active(scope)) return failed(inactive(scope));
    return readConfiguration(scope).thenCompose(stored ->
        readInitialization(scope).thenCompose(initialization ->
            openObserved(stored, initialization, scope, bootstrap)));
  }

  private CompletableFuture<SnapshotPinManager> openObserved(
      Optional<StoredConfiguration> stored, Optional<StoredInitialization> initialization,
      RequestScope scope, boolean bootstrap) {
    if (stored.isPresent()) {
      if (initialization.isEmpty()) {
        return failed(new IllegalStateException("snapshot pin initialization witness is missing"));
      }
      return finishOpen(stored.orElseThrow(), scope);
    }
    if (initialization.isEmpty()) {
      return bootstrap ? beginFirstInitialization(scope)
          : failed(new IllegalStateException("snapshot pin registry requires explicit bootstrap"));
    }
    if (initialization.orElseThrow().value().sealedRegistryId().isPresent()) {
      // The configuration read preceded the witness read; allow a concurrent publication.
      return readConfiguration(scope).thenCompose(latest -> latest.isPresent()
          ? finishOpen(latest.orElseThrow(), scope)
          : failed(new IllegalStateException("configured snapshot pin registry is missing")));
    }
    return reserveInitialization(initialization.orElseThrow(), scope);
  }

  private CompletableFuture<SnapshotPinManager> beginFirstInitialization(RequestScope scope) {
    return checkLegacyShards(0, scope)
        .thenCompose(ignored -> createInitializationWitness(scope))
        .thenCompose(ignored -> openRegistry(scope, false));
  }

  private CompletableFuture<Void> checkLegacyShards(int start, RequestScope scope) {
    if (!active(scope)) return failed(inactive(scope));
    if (start == SnapshotPinConfiguration.MAX_SHARDS) return CompletableFuture.completedFuture(null);
    int end = Math.min(SnapshotPinConfiguration.MAX_SHARDS, start + BOUNDARY_BATCH);
    var batch = new ArrayList<CompletableFuture<Void>>(end - start);
    for (int shard = start; shard < end; shard++) {
      batch.add(readMarker(legacyShardIdentity(shard), scope).thenApply(value -> {
        if (value.isPresent()) {
          throw new IllegalStateException("legacy snapshot pin state requires explicit migration");
        }
        return null;
      }));
    }
    return CompletableFuture.allOf(batch.toArray(CompletableFuture[]::new))
        .thenCompose(ignored -> checkLegacyShards(end, scope));
  }

  private CompletableFuture<Void> createInitializationWitness(RequestScope scope) {
    if (!active(scope)) return failed(inactive(scope));
    ByteKey initial = SnapshotPinRecordCodec.encodeInitialization(
        new SnapshotPinRecordCodec.Initialization(0, Optional.empty()));
    CompletableFuture<SystemRecordStore.CreateResult> create;
    try {
      create = records.createIfAbsent(SystemRecordType.SNAPSHOT_PIN, INITIALIZATION_ID, initial, scope);
    } catch (RuntimeException failure) {
      return failed(failure);
    }
    if (create == null) return failed(new IllegalStateException("pin store returned no create future"));
    return create.thenApply(result -> {
      if (result == null) throw new IllegalStateException("pin store returned no create result");
      return null;
    });
  }

  private CompletableFuture<SnapshotPinManager> reserveInitialization(
      StoredInitialization initialization, RequestScope scope) {
    // Staging rows from abandoned attempts remain isolated; the durable count bounds them.
    int attempts = initialization.value().attempts();
    if (attempts >= MAX_INITIALIZATION_ATTEMPTS) {
      return readConfiguration(scope).thenCompose(stored -> stored.isPresent()
          ? finishOpen(stored.orElseThrow(), scope)
          : failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT)));
    }
    ByteKey next = SnapshotPinRecordCodec.encodeInitialization(
        new SnapshotPinRecordCodec.Initialization(attempts + 1, Optional.empty()));
    return compareInitialization(initialization.encoded(), next, scope).thenCompose(updated -> {
      if (!updated) return openRegistry(scope, false);
      return stageOrAdopt(scope);
    });
  }

  private CompletableFuture<SnapshotPinManager> stageOrAdopt(RequestScope scope) {
    return readConfiguration(scope).thenCompose(stored -> {
      if (stored.isPresent()) return finishOpen(stored.orElseThrow(), scope);
      return readInitialization(scope).thenCompose(initialization -> {
        if (initialization.isEmpty()
            || initialization.orElseThrow().value().sealedRegistryId().isPresent()) {
          return openRegistry(scope, false);
        }
        return initializeShards(0, scope)
            .thenCompose(ignored -> ensureConfiguration(scope))
            .thenCompose(selected -> finishOpen(selected, scope));
      });
    });
  }

  private CompletableFuture<SnapshotPinManager> finishOpen(
      StoredConfiguration selected, RequestScope scope) {
    // A manager is returned only after the witness seals the selected shard namespace.
    var manager = new SnapshotPinManager(records, configuration, clock, selected.value().registryId());
    return manager.readBoundary(scope)
        .thenCompose(ignored -> manager.sealInitialization(scope, 0))
        .thenCompose(ignored -> manager.confirmConfiguration(selected.encoded(), scope))
        .thenCompose(ignored -> manager.readBoundary(scope))
        .thenApply(ignored -> manager);
  }

  private CompletableFuture<Void> sealInitialization(RequestScope scope, int attempt) {
    if (attempt >= MAX_CAS_ATTEMPTS) return failed(new BackendFailure(BackendFailure.Category.UNKNOWN));
    return readInitialization(scope).thenCompose(initialization -> {
      StoredInitialization stored = initialization.orElseThrow(
          () -> new IllegalStateException("snapshot pin initialization witness is missing"));
      Optional<ByteKey> sealed = stored.value().sealedRegistryId();
      if (sealed.isPresent()) {
        if (!sealed.orElseThrow().equals(registryId)) {
          return failed(new IllegalStateException("snapshot pin initialization witness differs"));
        }
        return CompletableFuture.completedFuture(null);
      }
      if (stored.value().attempts() == 0) {
        return failed(new IllegalStateException("snapshot pin configuration has no initialization attempt"));
      }
      ByteKey sealedValue = SnapshotPinRecordCodec.encodeInitialization(
          new SnapshotPinRecordCodec.Initialization(stored.value().attempts(), Optional.of(registryId)));
      return compareInitialization(stored.encoded(), sealedValue, scope)
          .handle((updated, failure) -> new StoreResult<>(updated, failure))
          .thenCompose(result -> resolveSeal(result, scope, attempt));
    });
  }

  private CompletableFuture<Void> resolveSeal(StoreResult<Boolean> result, RequestScope scope,
      int attempt) {
    if (result.failure() != null) {
      Throwable cause = CompletionFailures.unwrapCompletion(result.failure());
      if (cause instanceof Error fatal) return failed(fatal);
      if (!outcomeUnknown(cause)) return failed(cause);
      return readInitialization(scope).handle((stored, readFailure) -> {
        if (readFailure != null) throw unknownReadFailure(result.failure(), readFailure);
        if (stored.isPresent()
            && stored.orElseThrow().value().sealedRegistryId().filter(registryId::equals).isPresent()) {
          return null;
        }
        throw new CompletionException(new BackendFailure(BackendFailure.Category.UNKNOWN, cause));
      });
    }
    if (result.value() == null) return failed(new IllegalStateException("pin store returned no CAS result"));
    return result.value() ? CompletableFuture.completedFuture(null) : sealInitialization(scope, attempt + 1);
  }

  private CompletableFuture<Boolean> compareInitialization(ByteKey expected, ByteKey replacement,
      RequestScope scope) {
    if (!active(scope)) return failed(inactive(scope));
    CompletableFuture<Boolean> compare;
    try {
      compare = records.compareAndSet(SystemRecordType.SNAPSHOT_PIN, INITIALIZATION_ID,
          Optional.of(expected), replacement, scope);
    } catch (RuntimeException failure) {
      return failed(failure);
    }
    if (compare == null) return failed(new IllegalStateException("pin store returned no CAS future"));
    return compare.thenApply(updated -> {
      if (updated == null) throw new IllegalStateException("pin store returned no CAS result");
      return updated;
    });
  }

  private CompletableFuture<Void> confirmConfiguration(ByteKey expected, RequestScope scope) {
    return readConfiguration(scope).thenCompose(stored -> {
      if (stored.isEmpty() || !stored.orElseThrow().encoded().equals(expected)) {
        return failed(new IllegalStateException("snapshot pin configuration changed during open"));
      }
      return readInitialization(scope).thenApply(initialization -> {
        if (initialization.isEmpty()
            || initialization.orElseThrow().value().sealedRegistryId().filter(registryId::equals).isEmpty()) {
          throw new IllegalStateException("snapshot pin initialization witness differs");
        }
        return null;
      });
    });
  }

  private CompletableFuture<StoredConfiguration> ensureConfiguration(RequestScope scope) {
    if (!active(scope)) return failed(inactive(scope));
    ByteKey expected = SnapshotPinRecordCodec.encodeConfiguration(configuration, registryId);
    CompletableFuture<SystemRecordStore.CreateResult> create;
    try {
      create = records.createIfAbsent(SystemRecordType.SNAPSHOT_PIN, CONFIGURATION_ID, expected, scope);
    } catch (RuntimeException failure) {
      return failed(failure);
    }
    if (create == null) return failed(new IllegalStateException("pin store returned no create future"));
    return create.handle((result, failure) -> new StoreResult<>(result, failure))
        .thenCompose(result -> resolveConfigurationCreate(result, expected, scope));
  }

  private CompletableFuture<Optional<StoredConfiguration>> readConfiguration(RequestScope scope) {
    return readMarker(CONFIGURATION_ID, scope)
        .thenApply(stored -> stored.map(this::decodeStoredConfiguration));
  }

  private StoredConfiguration decodeStoredConfiguration(ByteKey encoded) {
    SnapshotPinRecordCodec.RegistryConfiguration decoded = SnapshotPinRecordCodec.decodeConfiguration(encoded);
    if (!decoded.configuration().equals(configuration)) {
      throw new IllegalStateException("snapshot pin geometry or lease differs from its durable format");
    }
    return new StoredConfiguration(decoded, encoded);
  }

  private CompletableFuture<Optional<StoredInitialization>> readInitialization(RequestScope scope) {
    return readMarker(INITIALIZATION_ID, scope).thenApply(stored -> stored.map(encoded -> {
      SnapshotPinRecordCodec.Initialization decoded = SnapshotPinRecordCodec.decodeInitialization(encoded);
      if (decoded.attempts() > MAX_INITIALIZATION_ATTEMPTS
          || decoded.attempts() == 0 && decoded.sealedRegistryId().isPresent()) {
        throw new IllegalStateException("snapshot pin initialization count exceeds its bound");
      }
      return new StoredInitialization(decoded, encoded);
    }));
  }

  private CompletableFuture<Optional<ByteKey>> readMarker(ByteKey identity, RequestScope scope) {
    if (!active(scope)) return failed(inactive(scope));
    CompletableFuture<Optional<ByteKey>> read;
    try {
      read = records.read(SystemRecordType.SNAPSHOT_PIN, identity, scope);
    } catch (RuntimeException failure) {
      return failed(failure);
    }
    if (read == null) return failed(new IllegalStateException("pin store returned no marker read future"));
    return read.thenApply(stored -> {
      if (stored == null) throw new IllegalStateException("pin store returned no marker result");
      return stored;
    });
  }

  private CompletableFuture<StoredConfiguration> resolveConfigurationCreate(
      StoreResult<SystemRecordStore.CreateResult> result, ByteKey expected, RequestScope scope) {
    if (result.failure() == null && result.value() == null) {
      return failed(new IllegalStateException("pin store returned no create result"));
    }
    if (result.failure() == null
        && result.value() != SystemRecordStore.CreateResult.CONFLICT) {
      return CompletableFuture.completedFuture(new StoredConfiguration(
          SnapshotPinRecordCodec.decodeConfiguration(expected), expected));
    }
    if (result.failure() != null
        && CompletionFailures.unwrapCompletion(result.failure()) instanceof Error fatal) {
      return failed(fatal);
    }
    return readMarker(CONFIGURATION_ID, scope).handle((stored, readFailure) -> {
      if (readFailure != null) throw unknownReadFailure(result.failure(), readFailure);
      ByteKey value = stored.orElseThrow(() -> new CompletionException(
          new BackendFailure(BackendFailure.Category.UNKNOWN)));
      return decodeStoredConfiguration(value);
    });
  }

  private CompletableFuture<Void> initializeShards(int start, RequestScope scope) {
    if (!active(scope)) return failed(inactive(scope));
    if (start == configuration.shardCount()) return CompletableFuture.completedFuture(null);
    int end = Math.min(configuration.shardCount(), start + INITIALIZATION_BATCH);
    var batch = new ArrayList<CompletableFuture<Void>>(end - start);
    for (int shard = start; shard < end; shard++) batch.add(ensureShard(shard, scope));
    return CompletableFuture.allOf(batch.toArray(CompletableFuture[]::new))
        .thenCompose(ignored -> initializeShards(end, scope));
  }

  private CompletableFuture<Void> ensureShard(int shard, RequestScope scope) {
    if (!active(scope)) return failed(inactive(scope));
    SnapshotPinShard empty = new SnapshotPinShard(shard, 0, 0, 0,
        java.util.Collections.nCopies(configuration.slotsPerShard(), SnapshotPinShard.Slot.empty()));
    ByteKey identity = shardIdentity(shard);
    ByteKey encoded = SnapshotPinRecordCodec.encodeShard(empty);
    CompletableFuture<SystemRecordStore.CreateResult> create;
    try {
      create = records.createIfAbsent(SystemRecordType.SNAPSHOT_PIN, identity, encoded, scope);
    } catch (RuntimeException failure) {
      return failed(failure);
    }
    if (create == null) return failed(new IllegalStateException("pin store returned no create future"));
    return create.thenCompose(created -> {
      if (created == null) return failed(new IllegalStateException("pin store returned no create result"));
      if (created != SystemRecordStore.CreateResult.CONFLICT) return CompletableFuture.completedFuture(null);
      return failed(new IllegalStateException("snapshot pin initialization shard conflicts"));
    });
  }

  private CompletableFuture<Pin> registerAttempt(int shard, ByteKey pinId,
      PinRequirement requirement, RequestScope scope, int attempt) {
    if (attempt >= MAX_CAS_ATTEMPTS) {
      return failed(new BackendFailure(BackendFailure.Category.UNKNOWN));
    }
    if (!active(scope)) return failed(inactive(scope));
    return readShard(shard, scope).thenCompose(stored ->
        registerFromShard(stored, pinId, requirement, scope, attempt));
  }

  private CompletableFuture<Pin> registerFromShard(StoredShard stored, ByteKey pinId,
      PinRequirement requirement, RequestScope scope, int attempt) {
    SnapshotPinShard current = stored.shard();
    if (belowRegistrationFloor(current, requirement)) {
      return failed(new IllegalStateException(
          "read view is below the durable snapshot registration floor"));
    }
    long now = clock.millis();
    int existing = activePinSlot(current, pinId);
    if (existing >= 0) {
      SnapshotPinShard.Slot slot = current.slots().get(existing);
      if (!matches(slot, requirement)) {
        return failed(new IllegalStateException("pin identity is bound to another read view"));
      }
      if (slot.expiresAtEpochMillis() > now) {
        return CompletableFuture.completedFuture(
            new PinLease(current.shard(), existing, slot.generation(), pinId));
      }
      return expireAndRetry(stored, existing, pinId, requirement, scope, attempt);
    }
    int expired = expiredPinSlot(current, now);
    if (expired >= 0) {
      return expireAndRetry(stored, expired, pinId, requirement, scope, attempt);
    }
    return registerInReusableSlot(stored, pinId, requirement, scope, attempt, now);
  }

  private CompletableFuture<Pin> expireAndRetry(StoredShard stored, int slot,
      ByteKey pinId, PinRequirement requirement, RequestScope scope, int attempt) {
    return transitionExpiredSlot(stored, slot, scope, 0)
        .thenCompose(ignored -> registerAttempt(stored.shard().shard(), pinId,
            requirement, scope, attempt + 1));
  }

  private CompletableFuture<Pin> registerInReusableSlot(StoredShard stored, ByteKey pinId,
      PinRequirement requirement, RequestScope scope, int attempt, long now) {
    SnapshotPinShard current = stored.shard();
    int slotIndex = reusableSlot(current);
    if (slotIndex < 0) return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
    SnapshotPinShard.Slot previous = current.slots().get(slotIndex);
    SnapshotPinShard.Slot replacement = SnapshotPinShard.Slot.active(
        Math.addExact(previous.generation(), 1), pinId, requirement.requiredRevision(),
        requirement.requiredSequence(), requirement.viewDigest(), expiryFrom(now),
        requirement.transactionId(), requirement.rootIdentity(), requirement.rootDigest());
    SnapshotPinShard next = replaceSlot(current, slotIndex, replacement);
    return compareShard(stored, next, scope)
        .handle((updated, failure) -> new StoreResult<>(updated, failure))
        .thenCompose(result -> {
          if (result.failure() == null && Boolean.TRUE.equals(result.value())) {
            return CompletableFuture.completedFuture(
                new PinLease(current.shard(), slotIndex, replacement.generation(), pinId));
          }
          return resolveRegistration(current.shard(), pinId, requirement, scope,
              attempt + 1, result.failure());
        });
  }

  private static boolean belowRegistrationFloor(SnapshotPinShard shard,
      PinRequirement requirement) {
    return requirement.requiredRevision() < shard.revisionFloor()
        || requirement.requiredSequence() > 0
            && requirement.requiredSequence() < shard.sequenceFloor();
  }

  private static int activePinSlot(SnapshotPinShard shard, ByteKey pinId) {
    for (int index = 0; index < shard.slots().size(); index++) {
      SnapshotPinShard.Slot slot = shard.slots().get(index);
      if (slot.status() == SnapshotPinShard.Status.ACTIVE && slot.pinId().equals(pinId)) {
        return index;
      }
    }
    return -1;
  }

  private static int expiredPinSlot(SnapshotPinShard shard, long now) {
    for (int index = 0; index < shard.slots().size(); index++) {
      SnapshotPinShard.Slot slot = shard.slots().get(index);
      if (slot.status() == SnapshotPinShard.Status.ACTIVE && slot.expiresAtEpochMillis() <= now) {
        return index;
      }
    }
    return -1;
  }

  private CompletableFuture<Pin> resolveRegistration(int shard, ByteKey pinId,
      PinRequirement requirement, RequestScope scope, int attempt, Throwable priorFailure) {
    if (!active(scope)) return failed(inactive(scope));
    if (priorFailure != null && !outcomeUnknown(priorFailure)) {
      return failed(CompletionFailures.unwrapCompletion(priorFailure));
    }
    return readShard(shard, scope)
        .handle((stored, readFailure) -> resolveObservedRegistration(stored, readFailure,
            shard, pinId, requirement, scope, priorFailure))
        .thenCompose(resolved -> resolved != null
        ? CompletableFuture.completedFuture(resolved)
        : registerAttempt(shard, pinId, requirement, scope, attempt));
  }

  private Pin resolveObservedRegistration(StoredShard stored, Throwable readFailure, int shard,
      ByteKey pinId, PinRequirement requirement, RequestScope scope, Throwable priorFailure) {
    if (readFailure != null) throw unknownReadFailure(priorFailure, readFailure);
    PinLease existing = findActivePin(stored.shard(), shard, pinId, requirement);
    if (existing != null) return existing;
    if (priorFailure != null && !active(scope)) throw new CompletionException(inactive(scope));
    return null;
  }

  private PinLease findActivePin(SnapshotPinShard current, int shard, ByteKey pinId,
      PinRequirement requirement) {
    long now = clock.millis();
    for (int index = 0; index < current.slots().size(); index++) {
      SnapshotPinShard.Slot slot = current.slots().get(index);
      if (slot.status() != SnapshotPinShard.Status.ACTIVE || !slot.pinId().equals(pinId)) continue;
      if (!matches(slot, requirement)) {
        throw new CompletionException(new IllegalStateException("pin identity is bound to another read view"));
      }
      if (slot.expiresAtEpochMillis() > now) {
        return new PinLease(shard, index, slot.generation(), pinId);
      }
    }
    return null;
  }

  private CompletableFuture<Void> transitionExpiredSlot(StoredShard stored, int slotIndex,
      RequestScope scope, int attempt) {
    if (attempt >= MAX_CAS_ATTEMPTS) return failed(new BackendFailure(BackendFailure.Category.UNKNOWN));
    SnapshotPinShard current = stored.shard();
    SnapshotPinShard.Slot slot = current.slots().get(slotIndex);
    if (slot.status() != SnapshotPinShard.Status.ACTIVE || slot.expiresAtEpochMillis() > clock.millis()) {
      return CompletableFuture.completedFuture(null);
    }
    SnapshotPinShard next = replaceSlot(current, slotIndex, slot.terminal(SnapshotPinShard.Status.REVOKED));
    return compareShard(stored, next, scope).handle((updated, failure) -> new StoreResult<>(updated, failure))
        .thenCompose(result -> {
          if (result.failure() == null && Boolean.TRUE.equals(result.value())) {
            return CompletableFuture.completedFuture(null);
          }
          if (result.failure() != null && !outcomeUnknown(result.failure())) {
            return failed(CompletionFailures.unwrapCompletion(result.failure()));
          }
          return readShard(current.shard(), scope).thenCompose(latest -> {
            SnapshotPinShard.Slot observed = latest.shard().slots().get(slotIndex);
            if (!observed.matches(slot.pinId(), slot.generation())
                || observed.status() != SnapshotPinShard.Status.ACTIVE
                || observed.expiresAtEpochMillis() > clock.millis()) {
              return CompletableFuture.completedFuture(null);
            }
            return transitionExpiredSlot(latest, slotIndex, scope, attempt + 1);
          });
        });
  }

  private CompletableFuture<Void> advanceShards(int shard, long revision, long sequence,
      RequestScope scope) {
    if (shard == configuration.shardCount()) return CompletableFuture.completedFuture(null);
    int end = Math.min(configuration.shardCount(), shard + INITIALIZATION_BATCH);
    var batch = new ArrayList<CompletableFuture<Void>>(end - shard);
    for (int index = shard; index < end; index++) {
      batch.add(advanceShard(index, revision, sequence, scope, 0));
    }
    return CompletableFuture.allOf(batch.toArray(CompletableFuture[]::new))
        .thenCompose(ignored -> advanceShards(end, revision, sequence, scope));
  }

  private CompletableFuture<Void> revokeExpiredShards(int shard, RequestScope scope) {
    if (shard == configuration.shardCount()) return CompletableFuture.completedFuture(null);
    int end = Math.min(configuration.shardCount(), shard + INITIALIZATION_BATCH);
    var batch = new ArrayList<CompletableFuture<Void>>(end - shard);
    for (int index = shard; index < end; index++) {
      batch.add(advanceShard(index, null, null, scope, 0));
    }
    return CompletableFuture.allOf(batch.toArray(CompletableFuture[]::new))
        .thenCompose(ignored -> revokeExpiredShards(end, scope));
  }

  private CompletableFuture<Void> advanceShard(int shard, Long revision, Long sequence,
      RequestScope scope, int attempt) {
    if (attempt >= MAX_CAS_ATTEMPTS) return failed(new BackendFailure(BackendFailure.Category.UNKNOWN));
    return readShard(shard, scope).thenCompose(stored -> {
      SnapshotPinShard current = stored.shard();
      long nextRevisionFloor = revision == null
          ? current.revisionFloor() : Math.max(current.revisionFloor(), revision);
      long nextSequenceFloor = sequence == null
          ? current.sequenceFloor() : Math.max(current.sequenceFloor(), sequence);
      var slots = new ArrayList<>(current.slots());
      boolean changed = nextRevisionFloor != current.revisionFloor()
          || nextSequenceFloor != current.sequenceFloor();
      long now = clock.millis();
      for (int index = 0; index < slots.size(); index++) {
        SnapshotPinShard.Slot slot = slots.get(index);
        if (slot.status() == SnapshotPinShard.Status.ACTIVE && slot.expiresAtEpochMillis() <= now) {
          slots.set(index, slot.terminal(SnapshotPinShard.Status.REVOKED));
          changed = true;
        }
      }
      if (!changed) return CompletableFuture.completedFuture(null);
      SnapshotPinShard next = current.update(nextRevisionFloor, nextSequenceFloor, slots);
      return compareShard(stored, next, scope).handle((updated, failure) -> new StoreResult<>(updated, failure))
          .thenCompose(result -> {
            if (result.failure() == null && Boolean.TRUE.equals(result.value())) {
              return CompletableFuture.completedFuture(null);
            }
            if (result.failure() != null && !outcomeUnknown(result.failure())) {
              return failed(CompletionFailures.unwrapCompletion(result.failure()));
            }
            return advanceShard(shard, revision, sequence, scope, attempt + 1);
          });
    });
  }

  private CompletableFuture<List<SnapshotPinShard>> readBoundaryBatch(int start,
      List<SnapshotPinShard> observed, RequestScope scope) {
    if (start == configuration.shardCount()) return CompletableFuture.completedFuture(List.copyOf(observed));
    int end = Math.min(configuration.shardCount(), start + BOUNDARY_BATCH);
    var batch = new ArrayList<CompletableFuture<StoredShard>>(end - start);
    for (int shard = start; shard < end; shard++) batch.add(readShard(shard, scope));
    return CompletableFuture.allOf(batch.toArray(CompletableFuture[]::new)).thenCompose(ignored -> {
      for (CompletableFuture<StoredShard> item : batch) observed.add(item.join().shard());
      return readBoundaryBatch(end, observed, scope);
    });
  }

  private SnapshotGcBoundary boundary(List<SnapshotPinShard> shards) {
    if (shards.size() != configuration.shardCount()) {
      throw new IllegalStateException("snapshot GC boundary is missing a configured shard");
    }
    long safeRevision = Long.MAX_VALUE;
    long safeSequence = Long.MAX_VALUE;
    var protections = new ArrayList<SnapshotGcBoundary.Protection>();
    for (SnapshotPinShard shard : shards) {
      long shardRevision = shard.revisionFloor();
      long shardSequence = shard.sequenceFloor();
      for (int index = 0; index < shard.slots().size(); index++) {
        SnapshotPinShard.Slot slot = shard.slots().get(index);
        if (slot.status() != SnapshotPinShard.Status.ACTIVE) continue;
        shardRevision = Math.min(shardRevision, slot.requiredRevision());
        if (slot.requiredSequence() > 0) shardSequence = Math.min(shardSequence, slot.requiredSequence());
        protections.add(slot.protection(shard.shard(), index));
      }
      safeRevision = Math.min(safeRevision, shardRevision);
      safeSequence = Math.min(safeSequence, shardSequence);
    }
    protections.sort(java.util.Comparator.comparingInt(SnapshotGcBoundary.Protection::shard)
        .thenComparingInt(SnapshotGcBoundary.Protection::slot));
    return new SnapshotGcBoundary(safeRevision, safeSequence, protections);
  }

  private CompletableFuture<StoredShard> readShard(int shard, RequestScope scope) {
    return records.read(SystemRecordType.SNAPSHOT_PIN, shardIdentity(shard), scope)
        .thenApply(value -> {
          ByteKey encoded = value.orElseThrow(
              () -> new IllegalStateException("configured snapshot pin shard is missing"));
          SnapshotPinShard record = SnapshotPinRecordCodec.decodeShard(encoded);
          validateShard(record, shard);
          return new StoredShard(record, encoded);
        });
  }

  private CompletableFuture<Boolean> compareShard(StoredShard expected, SnapshotPinShard replacement,
      RequestScope scope) {
    return records.compareAndSet(SystemRecordType.SNAPSHOT_PIN, shardIdentity(expected.shard().shard()),
        Optional.of(expected.encoded()), SnapshotPinRecordCodec.encodeShard(replacement), scope);
  }

  private void validateShard(SnapshotPinShard shard, int expectedId) {
    if (shard.shard() != expectedId || shard.slots().size() != configuration.slotsPerShard()) {
      throw new IllegalStateException("snapshot pin shard differs from its durable configuration");
    }
  }

  private static int reusableSlot(SnapshotPinShard shard) {
    for (int index = 0; index < shard.slots().size(); index++) {
      if (shard.slots().get(index).status() != SnapshotPinShard.Status.ACTIVE) return index;
    }
    return -1;
  }

  private static SnapshotPinShard replaceSlot(SnapshotPinShard shard, int index,
      SnapshotPinShard.Slot replacement) {
    var slots = new ArrayList<>(shard.slots());
    slots.set(index, replacement);
    return shard.update(shard.revisionFloor(), shard.sequenceFloor(), slots);
  }

  private static boolean matches(SnapshotPinShard.Slot slot, PinRequirement requirement) {
    return slot.requiredRevision() == requirement.requiredRevision()
        && slot.requiredSequence() == requirement.requiredSequence()
        && slot.viewDigest().equals(requirement.viewDigest())
        && slot.transactionId().equals(requirement.transactionId())
        && slot.rootIdentity().equals(requirement.rootIdentity())
        && slot.rootDigest().equals(requirement.rootDigest());
  }

  private long expiryFrom(long now) {
    try {
      return Math.addExact(now, configuration.leaseMillis());
    } catch (ArithmeticException overflow) {
      throw new IllegalStateException("snapshot pin expiry exceeds the supported clock range", overflow);
    }
  }

  private ByteKey shardIdentity(int shard) {
    return ByteKey.copyOf(ByteBuffer.allocate(Integer.BYTES * 2 + 16)
        .putInt(2).put(registryId.bytes()).putInt(shard).array());
  }

  private static ByteKey legacyShardIdentity(int shard) {
    return ByteKey.copyOf(ByteBuffer.allocate(Integer.BYTES * 2).putInt(1).putInt(shard).array());
  }

  private int shardFor(ByteKey pinId) {
    byte[] digest = ManifestDigest.sha256(pinId.bytes()).bytes();
    return Math.floorMod(ByteBuffer.wrap(digest).getInt(), configuration.shardCount());
  }

  private static boolean active(RequestScope scope) {
    return !scope.isClosed() && !scope.isDeadlineExceeded();
  }

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }

  private static ByteKey digestView(ReadView view) {
    return ManifestDigest.sha256(output -> {
      output.writeInt(1);
      ManifestDigest.writeBytes(output, view.profileFingerprint().bytes());
      ManifestDigest.writeBytes(output, view.authorizationScopeDigest().bytes());
      output.writeLong(view.barrierSequence());
      output.writeLong(view.finalizedSequence());
      output.writeLong(view.targetRevision());
      output.writeLong(view.stableRevision());
      output.writeLong(view.compactRevision());
      output.writeLong(view.selectedDecision().map(CommitDecision::sequence).orElse(0L));
      output.writeBoolean(view.selectedRoot().isPresent());
      if (view.selectedRoot().isPresent()) {
        ManifestRoot root = view.selectedRoot().orElseThrow();
        ManifestDigest.writeBytes(output, root.systemIdentity().bytes());
        ManifestDigest.writeBytes(output, root.transactionId().bytes());
        ManifestDigest.writeBytes(output, root.digest().bytes());
      }
    });
  }

  private static boolean outcomeUnknown(Throwable failure) {
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    if (cause instanceof BackendFailure backend) {
      return backend.category() == BackendFailure.Category.UNKNOWN
          || backend.category() == BackendFailure.Category.TIMEOUT
          || backend.category() == BackendFailure.Category.UNAVAILABLE;
    }
    return cause instanceof java.util.concurrent.TimeoutException
        || cause instanceof IOException;
  }

  private static CompletionException unknownReadFailure(Throwable priorFailure,
      Throwable readFailure) {
    Throwable readCause = CompletionFailures.unwrapCompletion(readFailure);
    if (readCause instanceof Error fatal) throw fatal;
    Throwable cause = priorFailure == null
        ? readCause : CompletionFailures.unwrapCompletion(priorFailure);
    if (cause instanceof Error fatal) throw fatal;
    if (priorFailure != null) cause.addSuppressed(readCause);
    return new CompletionException(new BackendFailure(BackendFailure.Category.UNKNOWN, cause));
  }

  private static <T> CompletableFuture<T> failed(Throwable failure) {
    return CompletableFuture.failedFuture(failure);
  }

  private record StoreResult<T>(T value, Throwable failure) {}
  private record StoredConfiguration(SnapshotPinRecordCodec.RegistryConfiguration value, ByteKey encoded) {}
  private record StoredInitialization(SnapshotPinRecordCodec.Initialization value, ByteKey encoded) {}
  private record StoredShard(SnapshotPinShard shard, ByteKey encoded) {}
  private record PinRequirement(long requiredRevision, long requiredSequence, ByteKey viewDigest,
      Optional<TransactionId> transactionId, Optional<ByteKey> rootIdentity,
      Optional<ByteKey> rootDigest) {
    private static PinRequirement from(ReadView view, boolean auxiliary) {
      Optional<CommitDecision> decision = view.selectedDecision();
      Optional<ManifestRoot> root = view.selectedRoot();
      long sequence = auxiliary && view.finalizedSequence() > 0
          ? view.finalizedSequence() : decision.map(CommitDecision::sequence).orElse(0L);
      return new PinRequirement(view.stableRevision(), sequence, digestView(view),
          decision.map(CommitDecision::transactionId), root.map(ManifestRoot::systemIdentity),
          root.map(ManifestRoot::digest));
    }
  }

  private final class PinLease implements Pin {
    private final int shard;
    private final int slot;
    private final long generation;
    private final ByteKey pinId;
    private final AtomicBoolean closeStarted = new AtomicBoolean();
    private final CompletableFuture<Void> releaseCompletion = new CompletableFuture<>();

    private PinLease(int shard, int slot, long generation, ByteKey pinId) {
      this.shard = shard;
      this.slot = slot;
      this.generation = generation;
      this.pinId = pinId;
    }

    @Override public long generation() { return generation; }

    @Override
    public CompletableFuture<Void> renew(RequestScope scope) {
      if (closeStarted.get()) return failed(new BackendFailure(BackendFailure.Category.CANCELLED));
      return mutateLease(this, LeaseMutation.RENEW, scope, 0);
    }

    @Override
    public CompletableFuture<Void> complete(RequestScope scope) {
      if (closeStarted.get()) return failed(new BackendFailure(BackendFailure.Category.CANCELLED));
      return mutateLease(this, LeaseMutation.COMPLETE, scope, 0)
          .whenComplete((ignored, failure) -> {
            if (failure == null) {
              closeStarted.set(true);
              releaseCompletion.complete(null);
            }
          });
    }

    @Override
    public void close() {
      if (!closeStarted.compareAndSet(false, true)) return;
      RequestScope cleanupScope = RequestScope.withTimeout(RELEASE_TIMEOUT);
      CompletableFuture<Void> release;
      try {
        release = Objects.requireNonNull(
            mutateLease(this, LeaseMutation.RELEASE, cleanupScope, 0),
            "pin release future");
      } catch (RuntimeException | Error failure) {
        // Settle release observation and close its private scope before propagating a fatal Error.
        cleanupScope.close();
        releaseCompletion.completeExceptionally(failure);
        if (failure instanceof Error fatal) throw fatal;
        return;
      }
      release.whenComplete((ignored, failure) -> {
        cleanupScope.close();
        if (failure == null) releaseCompletion.complete(null);
        else releaseCompletion.completeExceptionally(CompletionFailures.unwrapCompletion(failure));
      });
    }

    @Override
    public CompletionStage<Void> releaseCompletion() {
      return releaseCompletion.minimalCompletionStage();
    }
  }

  private enum LeaseMutation { RENEW, COMPLETE, RELEASE }

  private CompletableFuture<Void> mutateLease(PinLease pin, LeaseMutation mutation,
      RequestScope scope, int attempt) {
    if (attempt >= MAX_CAS_ATTEMPTS) return failed(new BackendFailure(BackendFailure.Category.UNKNOWN));
    if (!active(scope)) return failed(inactive(scope));
    return readShard(pin.shard, scope).thenCompose(stored ->
        mutateObservedLease(pin, mutation, scope, attempt, stored));
  }

  private CompletableFuture<Void> mutateObservedLease(PinLease pin, LeaseMutation mutation,
      RequestScope scope, int attempt, StoredShard stored) {
    SnapshotPinShard current = stored.shard();
    SnapshotPinShard.Slot slot = current.slots().get(pin.slot);
    if (!slot.matches(pin.pinId, pin.generation)) return failed(cancelled());
    if (slot.status() == SnapshotPinShard.Status.COMPLETED) {
      return mutation == LeaseMutation.RENEW ? failed(cancelled()) : CompletableFuture.completedFuture(null);
    }
    if (slot.status() == SnapshotPinShard.Status.REVOKED) {
      return mutation == LeaseMutation.RELEASE
          ? CompletableFuture.completedFuture(null) : failed(cancelled());
    }
    if (slot.status() != SnapshotPinShard.Status.ACTIVE) return failed(cancelled());
    long now = clock.millis();
    if (slot.expiresAtEpochMillis() <= now) {
      return revokeExpiredLease(pin, mutation, scope, attempt, stored, slot);
    }
    return writeLeaseMutation(pin, mutation, scope, attempt, stored, slot, now);
  }

  private CompletableFuture<Void> revokeExpiredLease(PinLease pin, LeaseMutation mutation,
      RequestScope scope, int attempt, StoredShard stored, SnapshotPinShard.Slot slot) {
    SnapshotPinShard revoked = replaceSlot(stored.shard(), pin.slot,
        slot.terminal(SnapshotPinShard.Status.REVOKED));
    return compareShard(stored, revoked, scope)
        .handle((updated, failure) -> new StoreResult<>(updated, failure))
        .thenCompose(result -> {
          if (result.failure() == null && Boolean.TRUE.equals(result.value())) {
            return mutation == LeaseMutation.RELEASE ? CompletableFuture.completedFuture(null)
                : failed(cancelled());
          }
          if (result.failure() != null && !outcomeUnknown(result.failure())) {
            return failed(CompletionFailures.unwrapCompletion(result.failure()));
          }
          return mutateLease(pin, mutation, scope, attempt + 1);
        });
  }

  private CompletableFuture<Void> writeLeaseMutation(PinLease pin, LeaseMutation mutation,
      RequestScope scope, int attempt, StoredShard stored, SnapshotPinShard.Slot slot, long now) {
    SnapshotPinShard.Slot replacementSlot = mutation == LeaseMutation.RENEW
        ? slot.renewed(expiryFrom(now))
        : slot.terminal(mutation == LeaseMutation.COMPLETE
            ? SnapshotPinShard.Status.COMPLETED : SnapshotPinShard.Status.REVOKED);
    SnapshotPinShard replacement = replaceSlot(stored.shard(), pin.slot, replacementSlot);
    return compareShard(stored, replacement, scope)
        .handle((updated, failure) -> new StoreResult<>(updated, failure))
        .thenCompose(result -> {
          if (result.failure() == null && Boolean.TRUE.equals(result.value())) {
            return CompletableFuture.completedFuture(null);
          }
          return resolveLeaseMutation(pin, mutation, scope, attempt + 1, result.failure());
        });
  }

  private CompletableFuture<Void> resolveLeaseMutation(PinLease pin, LeaseMutation mutation,
      RequestScope scope, int attempt, Throwable priorFailure) {
    return readShard(pin.shard, scope).handle((stored, readFailure) -> {
      if (readFailure != null) throw unknownReadFailure(priorFailure, readFailure);
      SnapshotPinShard.Slot current = stored.shard().slots().get(pin.slot);
      if (!current.matches(pin.pinId, pin.generation)) throw new CompletionException(cancelled());
      if (current.status() == SnapshotPinShard.Status.COMPLETED) {
        if (mutation == LeaseMutation.RENEW) throw new CompletionException(cancelled());
        return true;
      }
      if (current.status() == SnapshotPinShard.Status.REVOKED) {
        if (mutation == LeaseMutation.RELEASE) return true;
        throw new CompletionException(cancelled());
      }
      if (current.status() != SnapshotPinShard.Status.ACTIVE) throw new CompletionException(cancelled());
      if (priorFailure != null && !outcomeUnknown(priorFailure)) {
        throw new CompletionException(CompletionFailures.unwrapCompletion(priorFailure));
      }
      if (priorFailure != null && !active(scope)) throw new CompletionException(inactive(scope));
      return false;
    }).thenCompose(done -> done ? CompletableFuture.completedFuture(null)
        : mutateLease(pin, mutation, scope, attempt));
  }

  private static BackendFailure cancelled() {
    return new BackendFailure(BackendFailure.Category.CANCELLED);
  }
}
