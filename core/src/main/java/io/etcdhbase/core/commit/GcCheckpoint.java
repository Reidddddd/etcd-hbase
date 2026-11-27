package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Durable progress of one domain/bucket sweep under an actual Compact permission. */
public record GcCheckpoint(String tableGroupId, SystemRecordClosure.Domain domain,
    long maintenanceEpoch, int bucket, int bucketCount, Plan plan, Position position,
    Position fencePosition, Phase phase, long completedPages, long sweep, boolean exhausted,
    long pendingRecords, Completion completion) {
  private static final byte[] IDENTITY_PREFIX = {'G', 'C', 'C', 1};
  private static final byte[] VALUE_MAGIC = {'G', 'C', 'V', 3};
  // HBase physical rows are at most 32767 bytes, including the framed system identity.
  public static final int MAX_CURSOR_BYTES = 32767;
  private static final int MAX_GROUP_BYTES = 255;
  private static final int FIXED_VALUE_BYTES =
      4 + Integer.BYTES * 6 + Long.BYTES * 10 + 16 + 2 + RegistryBinding.BYTES;
  public static final int MAX_ENCODED_BYTES = FIXED_VALUE_BYTES + MAX_GROUP_BYTES + 2 * MAX_CURSOR_BYTES;

  /** A metadata holder cannot retire payloads until its complete shard fence pass is durable. */
  public enum Phase {
    FENCING(1), SCANNING(2);
    private final int id;
    Phase(int id) { this.id = id; }
    private static Phase decode(int id) {
      for (var phase : values()) if (phase.id == id) return phase;
      throw invalid();
    }
  }

  /** Safe bounds and selected operation are part of the sweep identity, never local defaults. */
  public record Plan(InvocationId operationId, long generation, long safeSequence, long safeRevision,
      RegistryBinding registries) {
    public Plan {
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(registries, "registries");
      if (generation <= 0 || safeSequence < 0 || safeRevision < 0) {
        throw new IllegalArgumentException("invalid GC plan");
      }
    }

    public static Plan from(SystemRecordRetirement.Permit permit) {
      return new Plan(permit.operationId(), permit.generation(), permit.safeSequence(), permit.safeRevision(),
          new RegistryBinding(permit.profile(), permit.registryIdentity(),
              permit.authority().eventRegistryIdentity(), permit.authority().eventNamespace()));
    }
  }

  /** Completion never transfers to another physical profile or replacement protection registry. */
  public record RegistryBinding(ByteKey profile, ByteKey kvIdentity, ByteKey eventIdentity, ByteKey eventNamespace) {
    private static final int BYTES = 32 + 16 * 3;
    public RegistryBinding {
      if (profile.length() != 32 || kvIdentity.length() != 16 || eventIdentity.length() != 16
          || eventNamespace.length() != 16) throw new IllegalArgumentException("invalid GC registry binding");
    }
  }

  /**
   * Proves all eligible obligations strictly below both bounds. A positive generation distinguishes
   * a proven empty zero-bound sweep from an initial checkpoint.
   */
  public record Completion(long revision, long sequence, long generation) {
    public Completion {
      if (revision < 0 || sequence < 0 || generation < 0
          || generation == 0 && (revision != 0 || sequence != 0)) {
        throw new IllegalArgumentException("invalid system-record completion");
      }
    }

    public static Completion none() { return new Completion(0, 0, 0); }

    public boolean covers(long revision, long sequence) {
      return generation > 0 && this.revision >= revision && this.sequence >= sequence;
    }
  }

  /** Exclusive storage-order cursor within this bucket. */
  public record Position(Optional<ByteKey> afterIdentity) {
    public Position {
      Objects.requireNonNull(afterIdentity, "afterIdentity");
      afterIdentity.ifPresent(identity -> {
        if (identity.length() == 0 || identity.length() > MAX_CURSOR_BYTES) {
          throw new IllegalArgumentException("invalid GC cursor size");
        }
      });
    }

    public static Position start() { return new Position(Optional.empty()); }
  }

  public GcCheckpoint {
    groupBytes(tableGroupId);
    Objects.requireNonNull(domain, "domain");
    Objects.requireNonNull(plan, "plan");
    Objects.requireNonNull(position, "position");
    Objects.requireNonNull(fencePosition, "fencePosition");
    Objects.requireNonNull(phase, "phase");
    Objects.requireNonNull(completion, "completion");
    if (maintenanceEpoch <= 0 || bucketCount < 1 || bucketCount > 65_536
        || bucket < 0 || bucket >= bucketCount || completedPages < 0 || sweep < 0 || pendingRecords < 0
        || completion.generation() > plan.generation() || completion.revision() > plan.safeRevision()
        || completion.sequence() > plan.safeSequence()
        || phase == Phase.FENCING && exhausted
        || phase == Phase.SCANNING && fencePosition.afterIdentity().isPresent()) {
      throw new IllegalArgumentException("invalid GC checkpoint");
    }
  }

  public static GcCheckpoint start(String group, SystemRecordClosure.Domain domain, long epoch,
      int bucket, int bucketCount, SystemRecordRetirement.Permit permit) {
    return new GcCheckpoint(group, domain, epoch, bucket, bucketCount, Plan.from(permit),
        Position.start(), Position.start(), Phase.FENCING, 0, 0, false, 0, Completion.none());
  }

  static ByteKey identity(String group, SystemRecordClosure.Domain domain, int bucket) {
    byte[] encodedGroup = groupBytes(group);
    if (bucket < 0 || bucket >= 65_536) throw new IllegalArgumentException("invalid GC bucket");
    return ByteKey.copyOf(ByteBuffer.allocate(IDENTITY_PREFIX.length + 1 + encodedGroup.length + 8)
        .put(IDENTITY_PREFIX).put((byte) encodedGroup.length).put(encodedGroup)
        .putInt(domain.recordType().id()).putInt(bucket).array());
  }

  /** Topology cannot change under an existing physical table's checkpoint. */
  void requireSameShard(GcCheckpoint other) {
    if (!tableGroupId.equals(other.tableGroupId) || domain != other.domain || bucket != other.bucket
        || bucketCount != other.bucketCount || !plan.registries.equals(other.plan.registries)) {
      throw new IllegalArgumentException("GC checkpoint shard or topology mismatch");
    }
  }

  /** A new operation or finished sweep revisits retained prefixes with a distinct sweep CAS. */
  GcCheckpoint resumeFrom(GcCheckpoint previous) {
    requireSameShard(previous);
    if (maintenanceEpoch < previous.maintenanceEpoch) {
      throw new IllegalArgumentException("GC checkpoint owner epoch regressed");
    }
    boolean sameOperation = plan.operationId.equals(previous.plan.operationId)
        && plan.generation == previous.plan.generation;
    if (plan.generation == previous.plan.generation && !sameOperation) {
      throw new IllegalArgumentException("GC generation has another Compact operation");
    }
    boolean restart = previous.exhausted || !sameOperation;
    boolean refence = maintenanceEpoch > previous.maintenanceEpoch || !sameOperation;
    // Finalized sequence can grow on every foreground write. Widen only at the next sweep,
    // otherwise a retained first page would continually reset an unfinished cursor.
    Plan sweepPlan = restart ? plan : previous.plan;
    return new GcCheckpoint(tableGroupId, domain, maintenanceEpoch, bucket, bucketCount, sweepPlan,
        restart ? Position.start() : previous.position,
        refence ? Position.start() : previous.fencePosition,
        refence ? Phase.FENCING : previous.phase, previous.completedPages,
        restart ? Math.addExact(previous.sweep, 1) : previous.sweep, false,
        restart ? 0 : previous.pendingRecords, previous.completion);
  }

  GcCheckpoint after(SystemRecordClosure.Page page) {
    if (exhausted || page.domain() != domain
        || phase == Phase.FENCING && (page.retired() != 0 || page.pending() != 0)) {
      throw new IllegalArgumentException("invalid GC checkpoint page");
    }
    if (phase == Phase.FENCING) {
      return new GcCheckpoint(tableGroupId, domain, maintenanceEpoch, bucket, bucketCount, plan, position,
          page.exhausted() ? Position.start() : new Position(page.lastIdentity()),
          page.exhausted() ? Phase.SCANNING : Phase.FENCING,
          Math.addExact(completedPages, 1), sweep, false, pendingRecords, completion);
    }
    long pending = Math.addExact(pendingRecords, page.pending());
    var confirmed = page.exhausted() && pending == 0
        ? new Completion(plan.safeRevision, plan.safeSequence, plan.generation) : completion;
    return new GcCheckpoint(tableGroupId, domain, maintenanceEpoch, bucket, bucketCount, plan,
        new Position(page.lastIdentity()), Position.start(), Phase.SCANNING,
        Math.addExact(completedPages, 1), sweep, page.exhausted(), pending, confirmed);
  }

  public ByteKey encode() {
    byte[] group = groupBytes(tableGroupId);
    byte[] cursor = cursor(position);
    byte[] fenceCursor = cursor(fencePosition);
    return ByteKey.copyOf(ByteBuffer.allocate(FIXED_VALUE_BYTES + group.length + cursor.length + fenceCursor.length)
        .put(VALUE_MAGIC).putInt(group.length).put(group).putInt(domain.recordType().id())
        .putInt(bucket).putInt(bucketCount).putLong(maintenanceEpoch).put(plan.registries.profile.bytes())
        .put(plan.registries.kvIdentity.bytes()).put(plan.registries.eventIdentity.bytes())
        .put(plan.registries.eventNamespace.bytes()).put(plan.operationId.bytes())
        .putLong(plan.generation).putLong(plan.safeSequence).putLong(plan.safeRevision)
        .putLong(completedPages).putLong(sweep).putLong(pendingRecords)
        .putLong(completion.revision).putLong(completion.sequence).putLong(completion.generation)
        .put((byte) (exhausted ? 1 : 0)).put((byte) phase.id)
        .putInt(cursor.length).put(cursor).putInt(fenceCursor.length).put(fenceCursor).array());
  }

  private static byte[] cursor(Position position) {
    return position.afterIdentity.map(ByteKey::bytes).orElseGet(() -> new byte[0]);
  }

  public static GcCheckpoint decode(ByteKey encoded) {
    if (encoded.length() > MAX_ENCODED_BYTES) throw invalid();
    try {
      var input = ByteBuffer.wrap(encoded.bytes());
      byte[] magic = new byte[4];
      input.get(magic);
      if (!Arrays.equals(magic, VALUE_MAGIC)) throw invalid();
      int groupLength = input.getInt();
      if (groupLength < 1 || groupLength > MAX_GROUP_BYTES) throw invalid();
      byte[] group = new byte[groupLength];
      input.get(group);
      String groupId = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(group)).toString();
      var domain = domain(input.getInt());
      int bucket = input.getInt();
      int buckets = input.getInt();
      long epoch = input.getLong();
      var plan = readPlan(input);
      long pages = input.getLong();
      long sweep = input.getLong();
      long pending = input.getLong();
      var completion = new Completion(input.getLong(), input.getLong(), input.getLong());
      boolean exhausted = readBoolean(input);
      Phase phase = Phase.decode(Byte.toUnsignedInt(input.get()));
      Position position = readPosition(input);
      Position fencePosition = readPosition(input);
      if (input.hasRemaining()) throw invalid();
      return new GcCheckpoint(groupId, domain, epoch, bucket, buckets, plan,
          position, fencePosition, phase, pages, sweep, exhausted, pending, completion);
    } catch (java.nio.BufferUnderflowException | CharacterCodingException malformed) {
      throw new IllegalArgumentException("invalid GC checkpoint encoding", malformed);
    }
  }

  private static Plan readPlan(ByteBuffer input) {
    var registries = new RegistryBinding(readBytes(input, 32), readBytes(input, 16),
        readBytes(input, 16), readBytes(input, 16));
    byte[] operation = new byte[16];
    input.get(operation);
    return new Plan(InvocationId.fromBytes(operation), input.getLong(), input.getLong(), input.getLong(), registries);
  }

  private static Position readPosition(ByteBuffer input) {
    int length = input.getInt();
    if (length < 0 || length > MAX_CURSOR_BYTES || length > input.remaining()) throw invalid();
    if (length == 0) return Position.start();
    byte[] cursor = new byte[length];
    input.get(cursor);
    return new Position(Optional.of(ByteKey.copyOf(cursor)));
  }

  private static ByteKey readBytes(ByteBuffer input, int length) {
    byte[] bytes = new byte[length];
    input.get(bytes);
    return ByteKey.copyOf(bytes);
  }

  private static boolean readBoolean(ByteBuffer input) {
    byte value = input.get();
    if (value != 0 && value != 1) throw invalid();
    return value == 1;
  }

  private static SystemRecordClosure.Domain domain(int id) {
    for (var domain : SystemRecordClosure.Domain.values()) {
      if (domain.recordType().id() == id) return domain;
    }
    throw invalid();
  }

  private static byte[] groupBytes(String group) {
    Objects.requireNonNull(group, "tableGroupId");
    byte[] bytes = group.getBytes(StandardCharsets.UTF_8);
    if (group.isBlank() || bytes.length > MAX_GROUP_BYTES
        || !group.equals(new String(bytes, StandardCharsets.UTF_8))) throw new IllegalArgumentException("invalid GC group");
    return bytes;
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("invalid GC checkpoint encoding");
  }
}
