package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Separate durable protection domain for event, index and decision history. */
public final class EventHistoryPinManager {
  /** Durable event-history protection with a monotonic selected-root promotion operation. */
  public interface Pin extends ReadViewPinRegistry.Pin {
    CompletableFuture<Void> promoteRoot(Requirement previous, Requirement promoted, RequestScope scope);
  }

  /** Couples a durable registration to the boundary it was created to protect. */
  public static final class Protection implements AutoCloseable {
    private final Pin pin;
    private final Requirement requirement;
    private final ByteKey registryIdentity;

    private Protection(Pin pin, Requirement requirement, ByteKey registryIdentity) {
      this.pin = Objects.requireNonNull(pin, "pin");
      this.requirement = Objects.requireNonNull(requirement, "requirement");
      this.registryIdentity = Objects.requireNonNull(registryIdentity, "registryIdentity");
    }

    Pin pin() { return pin; }
    Requirement requirement() { return requirement; }
    ByteKey registryIdentity() { return registryIdentity; }

    public long generation() { return pin.generation(); }

    public CompletableFuture<Void> renew(RequestScope scope) { return pin.renew(scope); }

    public CompletionStage<Void> releaseCompletion() { return pin.releaseCompletion(); }

    @Override public void close() { pin.close(); }
  }

  private final SnapshotPinManager registry;
  private final Optional<ByteKey> namespace;

  private EventHistoryPinManager(SnapshotPinManager registry, Optional<ByteKey> namespace) {
    this.registry = registry;
    this.namespace = namespace;
  }

  /** Identifies the immutable registry generation whose protections must be recovered. */
  public ByteKey registryIdentity() { return registry.registryIdentity(); }

  SnapshotPinConfiguration configuration() { return registry.configuration(); }

  /** Storage namespace, distinct from the registry's generated identity; staging is not selected yet. */
  public Optional<ByteKey> namespace() { return namespace; }

  /** Initializes only after exclusive verification that the event registry is new. */
  public static CompletableFuture<EventHistoryPinManager> initializeFresh(SystemRecordStore records,
      SnapshotPinConfiguration configuration, Clock clock, RequestScope scope) {
    return SnapshotPinManager.initializeFresh(new EventPinRecords(records), configuration, clock, scope)
        .thenApply(registry -> new EventHistoryPinManager(registry, Optional.empty()));
  }

  /** Opens an existing registry; missing initialization and damaged shards fail closed. */
  public static CompletableFuture<EventHistoryPinManager> open(SystemRecordStore records,
      SnapshotPinConfiguration configuration, Clock clock, RequestScope scope) {
    return SnapshotPinManager.open(new EventPinRecords(records), configuration, clock, scope)
        .thenApply(registry -> new EventHistoryPinManager(registry, Optional.empty()));
  }

  /** A staging namespace belongs to one attempt and cannot be shared with another initializer. */
  static CompletableFuture<EventHistoryPinManager> stage(SystemRecordStore records, ByteKey namespace,
      SnapshotPinConfiguration configuration, Clock clock, RequestScope scope) {
    return SnapshotPinManager.initializeFresh(new EventPinRecords(records, namespace), configuration, clock, scope)
        .thenApply(registry -> new EventHistoryPinManager(registry, Optional.of(namespace)));
  }

  /** Opens only the namespace selected by the authoritative control row; never recreates it. */
  public static CompletableFuture<EventHistoryPinManager> openSelected(SystemRecordStore records,
      CommitControlStore control, SnapshotPinConfiguration configuration, Clock clock, RequestScope scope) {
    Objects.requireNonNull(control, "control");
    return control.read(scope).thenCompose(found -> {
      CommitState state = found.orElseThrow(() -> new IllegalStateException("event control row is missing")).state();
      ByteKey namespace = state.eventRegistryNamespace().orElseThrow(
          () -> new IllegalStateException("event registry namespace is not selected"));
      return SnapshotPinManager.openSealed(new EventPinRecords(records, namespace), configuration, clock, scope)
          .thenCompose(registry -> control.read(scope).thenApply(latest -> {
            CommitState current = latest.orElseThrow(
                () -> new IllegalStateException("event control row is missing")).state();
            if (!current.eventRegistryNamespace().equals(Optional.of(namespace))) {
              throw new IllegalStateException("event registry namespace changed during open");
            }
            return new EventHistoryPinManager(registry, Optional.of(namespace));
          }));
    });
  }

  /**
   * Registers before event or index reads. The caller must re-read the logical compact/control
   * boundary after registration and stop if the requested history is no longer available.
   */
  public CompletableFuture<Pin> register(Requirement requirement, RequestScope scope) {
    Objects.requireNonNull(requirement, "requirement");
    return registry.registerProtected(requirement.revision(), requirement.sequence(),
        requirement.digest(), requirement.transaction(), requirement.eventRoot().map(EventReference::identity),
        requirement.eventRoot().map(EventReference::digest), scope).thenApply(EventPin::new);
  }

  /** Registers a history floor and retains the proof that identifies what the returned pin covers. */
  public CompletableFuture<Protection> registerProtection(
      Requirement requirement, RequestScope scope) {
    Objects.requireNonNull(requirement, "requirement");
    ByteKey registeredIdentity = registryIdentity();
    return register(requirement, scope).thenApply(pin ->
        new Protection(pin, requirement, registeredIdentity));
  }

  private static SnapshotPinManager.PinRequirement pinRequirement(Requirement requirement) {
    return new SnapshotPinManager.PinRequirement(requirement.revision(), requirement.sequence(),
        requirement.digest(), requirement.transaction(), requirement.eventRoot().map(EventReference::identity),
        requirement.eventRoot().map(EventReference::digest));
  }

  private record EventPin(SnapshotPinManager.PinLease delegate) implements Pin {
    @Override public long generation() { return delegate.generation(); }
    @Override public CompletableFuture<Void> renew(RequestScope scope) { return delegate.renew(scope); }
    @Override public CompletableFuture<Void> complete(RequestScope scope) { return delegate.complete(scope); }
    @Override public CompletionStage<Void> releaseCompletion() { return delegate.releaseCompletion(); }
    @Override public void close() { delegate.close(); }

    @Override public CompletableFuture<Void> promoteRoot(Requirement previous,
        Requirement promoted, RequestScope scope) {
      Objects.requireNonNull(previous, "previous");
      Objects.requireNonNull(promoted, "promoted");
      Objects.requireNonNull(scope, "scope");
      try {
        promoted.requirePromotionFrom(previous);
      } catch (IllegalArgumentException failure) {
        return CompletableFuture.failedFuture(failure);
      }
      return delegate.promoteRoot(pinRequirement(previous), pinRequirement(promoted), scope);
    }
  }

  /** Candidate pin boundary only; use EventHistoryRetention before physical event collection. */
  public CompletableFuture<SnapshotGcBoundary> advanceRegistrationFloors(
      long revision, long sequence, RequestScope scope) {
    return registry.advanceRegistrationFloors(revision, sequence, scope);
  }

  public CompletableFuture<SnapshotGcBoundary> readBoundary(RequestScope scope) {
    return registry.readBoundary(scope);
  }

  public CompletableFuture<SnapshotGcBoundary> revokeExpiredPins(RequestScope scope) {
    return registry.revokeExpiredPins(scope);
  }

  /** Optional root is the authenticated event directory, not the latest KV value or manifest root. */
  public record Requirement(long revision, long sequence, ByteKey authorizationDigest,
      Optional<TransactionId> transaction, Optional<EventReference> eventRoot) {
    public Requirement {
      Objects.requireNonNull(authorizationDigest, "authorizationDigest");
      Objects.requireNonNull(transaction, "transaction");
      Objects.requireNonNull(eventRoot, "eventRoot");
      if (revision <= 0 || sequence <= 0 || authorizationDigest.length() != 32
          || transaction.isPresent() != eventRoot.isPresent()
          || eventRoot.filter(root -> root.identity().length() > 1024).isPresent()) {
        throw new IllegalArgumentException("invalid event history protection requirement");
      }
    }

    ByteKey digest() {
      return ManifestDigest.sha256(output -> {
        output.writeInt(1);
        output.writeLong(revision);
        output.writeLong(sequence);
        ManifestDigest.writeBytes(output, authorizationDigest.bytes());
        output.writeBoolean(eventRoot.isPresent());
        if (eventRoot.isPresent()) {
          ManifestDigest.writeBytes(output, transaction.orElseThrow().bytes());
          ManifestDigest.writeBytes(output, eventRoot.orElseThrow().identity().bytes());
          ManifestDigest.writeBytes(output, eventRoot.orElseThrow().digest().bytes());
        }
      });
    }

    void requirePromotionFrom(Requirement previous) {
      Objects.requireNonNull(previous, "previous");
      if (previous.revision != revision || previous.sequence != sequence
          || !previous.authorizationDigest.equals(authorizationDigest)
          || previous.transaction.isPresent() || previous.eventRoot.isPresent()
          || transaction.isEmpty() || eventRoot.isEmpty()) {
        throw new IllegalArgumentException("event root promotion changes its pinned boundary or identity");
      }
    }
  }

  /** Keeps event pins in a distinct durable namespace while reusing the CAS lease registry. */
  private static final class EventPinRecords implements SystemRecordStore {
    private static final byte[] PREFIX = {0x45, 0x56, 0x50, 0x49, 0x4e, 0x01, 0x00};
    private static final byte[] STAGING_PREFIX = {0x45, 0x56, 0x50, 0x49, 0x4e, 0x01, 0x01};
    private final SystemRecordStore delegate;
    private final byte[] prefix;

    private EventPinRecords(SystemRecordStore delegate) {
      this.delegate = Objects.requireNonNull(delegate, "records");
      this.prefix = PREFIX;
    }

    private EventPinRecords(SystemRecordStore delegate, ByteKey namespace) {
      this.delegate = Objects.requireNonNull(delegate, "records");
      Objects.requireNonNull(namespace, "namespace");
      if (namespace.length() != 16) throw new IllegalArgumentException("invalid event registry namespace");
      this.prefix = ByteBuffer.allocate(STAGING_PREFIX.length + 16)
          .put(STAGING_PREFIX).put(namespace.bytes()).array();
    }

    @Override public int bucket(SystemRecordType type, ByteKey identity) {
      return delegate.bucket(type, namespaced(type, identity));
    }

    @Override public CompletableFuture<Optional<ByteKey>> read(
        SystemRecordType type, ByteKey identity, RequestScope scope) {
      return delegate.read(type, namespaced(type, identity), scope);
    }

    @Override public CompletableFuture<CreateResult> createIfAbsent(
        SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope) {
      return delegate.createIfAbsent(type, namespaced(type, identity), value, scope);
    }

    @Override public CompletableFuture<Boolean> compareAndSet(SystemRecordType type, ByteKey identity,
        Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) {
      return delegate.compareAndSet(type, namespaced(type, identity), expected, replacement, scope);
    }

    private ByteKey namespaced(SystemRecordType type, ByteKey identity) {
      if (type != SystemRecordType.SNAPSHOT_PIN) {
        throw new IllegalArgumentException("event pin registry can only access pin records");
      }
      byte[] value = Objects.requireNonNull(identity, "identity").bytes();
      return ByteKey.copyOf(ByteBuffer.allocate(Math.addExact(prefix.length, value.length))
          .put(prefix).put(value).array());
    }
  }
}
