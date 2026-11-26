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
    long completedPages, long sweep, boolean exhausted) {
  private static final byte[] IDENTITY_PREFIX = {'G', 'C', 'C', 1};
  private static final byte[] VALUE_MAGIC = {'G', 'C', 'V', 1};
  // HBase physical rows are at most 32767 bytes, including the framed system identity.
  public static final int MAX_CURSOR_BYTES = 32767;
  private static final int MAX_GROUP_BYTES = 255;
  public static final int MAX_ENCODED_BYTES = 4 + 4 + MAX_GROUP_BYTES + 4 * 4
      + 8 * 6 + 16 + 2 + MAX_CURSOR_BYTES;

  /** Safe bounds and selected operation are part of the sweep identity, never local defaults. */
  public record Plan(InvocationId operationId, long generation, long safeSequence, long safeRevision) {
    public Plan {
      Objects.requireNonNull(operationId, "operationId");
      if (generation <= 0 || safeSequence < 0 || safeRevision < 0) {
        throw new IllegalArgumentException("invalid GC plan");
      }
    }

    public static Plan from(SystemRecordRetirement.Permit permit) {
      return new Plan(permit.operationId(), permit.generation(), permit.safeSequence(), permit.safeRevision());
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
    if (maintenanceEpoch <= 0 || bucketCount < 1 || bucketCount > 65_536
        || bucket < 0 || bucket >= bucketCount || completedPages < 0 || sweep < 0) {
      throw new IllegalArgumentException("invalid GC checkpoint");
    }
  }

  public static GcCheckpoint start(String group, SystemRecordClosure.Domain domain, long epoch,
      int bucket, int bucketCount, SystemRecordRetirement.Permit permit) {
    return new GcCheckpoint(group, domain, epoch, bucket, bucketCount, Plan.from(permit),
        Position.start(), 0, 0, false);
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
        || bucketCount != other.bucketCount) {
      throw new IllegalArgumentException("GC checkpoint shard or topology mismatch");
    }
  }

  /** A changed permission or finished sweep revisits retained prefixes with a distinct sweep CAS. */
  GcCheckpoint resumeFrom(GcCheckpoint previous) {
    requireSameShard(previous);
    if (maintenanceEpoch < previous.maintenanceEpoch) {
      throw new IllegalArgumentException("GC checkpoint owner epoch regressed");
    }
    boolean restart = previous.exhausted || !plan.equals(previous.plan);
    return new GcCheckpoint(tableGroupId, domain, maintenanceEpoch, bucket, bucketCount, plan,
        restart ? Position.start() : previous.position, previous.completedPages,
        restart ? Math.addExact(previous.sweep, 1) : previous.sweep, false);
  }

  GcCheckpoint after(SystemRecordClosure.Page page) {
    if (exhausted || page.domain() != domain) throw new IllegalArgumentException("invalid GC page domain");
    return new GcCheckpoint(tableGroupId, domain, maintenanceEpoch, bucket, bucketCount, plan,
        new Position(page.lastIdentity()), Math.addExact(completedPages, 1), sweep, page.exhausted());
  }

  public ByteKey encode() {
    byte[] group = groupBytes(tableGroupId);
    byte[] cursor = position.afterIdentity.map(ByteKey::bytes).orElseGet(() -> new byte[0]);
    return ByteKey.copyOf(ByteBuffer.allocate(4 + 4 + group.length + 4 * 4 + 8 * 6 + 16 + 2 + cursor.length)
        .put(VALUE_MAGIC).putInt(group.length).put(group).putInt(domain.recordType().id())
        .putInt(bucket).putInt(bucketCount).putLong(maintenanceEpoch).put(plan.operationId.bytes())
        .putLong(plan.generation).putLong(plan.safeSequence).putLong(plan.safeRevision)
        .putLong(completedPages).putLong(sweep).put((byte) (exhausted ? 1 : 0))
        .put((byte) (position.afterIdentity.isPresent() ? 1 : 0)).putInt(cursor.length).put(cursor).array());
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
      byte[] operation = new byte[16];
      input.get(operation);
      var plan = new Plan(InvocationId.fromBytes(operation), input.getLong(), input.getLong(), input.getLong());
      long pages = input.getLong();
      long sweep = input.getLong();
      boolean exhausted = readBoolean(input);
      boolean hasCursor = readBoolean(input);
      int length = input.getInt();
      if (length < 0 || length > MAX_CURSOR_BYTES || input.remaining() != length
          || hasCursor != (length > 0)) throw invalid();
      byte[] cursor = new byte[length];
      input.get(cursor);
      return new GcCheckpoint(groupId, domain, epoch, bucket, buckets, plan,
          new Position(hasCursor ? Optional.of(ByteKey.copyOf(cursor)) : Optional.empty()), pages, sweep, exhausted);
    } catch (java.nio.BufferUnderflowException | CharacterCodingException malformed) {
      throw new IllegalArgumentException("invalid GC checkpoint encoding", malformed);
    }
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
