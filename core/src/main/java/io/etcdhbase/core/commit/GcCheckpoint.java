package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Durable resume point for one {@code tableGroup + recordDomain + bucket} GC shard.
 *
 * <p>§6.3 of the M5 design requires background work to resume from a persisted checkpoint instead of
 * rescanning every round, and requires a takeover to stay bound to the maintenance epoch and
 * incarnation that issued it. An older worker therefore cannot advance a newer checkpoint: its
 * compare-and-set carries the exact bytes it read, including the epoch it claimed under.
 *
 * <p>{@code protectedSequence} records the boundary the shard may not pass. It is deliberately part
 * of the checkpoint rather than recomputed locally, so a restart cannot silently widen the window a
 * previous round had already narrowed.
 */
public record GcCheckpoint(
    String tableGroupId,
    SystemRecordClosure.Domain domain,
    long maintenanceEpoch,
    int bucket,
    int bucketCount,
    long protectedSequence,
    long protectedRevision,
    Position position,
    long completedPages) {
  private static final byte[] IDENTITY_PREFIX = {'G', 'C', 'C', 1};
  private static final byte[] VALUE_MAGIC = {'G', 'C', 'V', 1};
  private static final int MAX_BUCKETS = 65_536;
  public static final int ROUTE_BYTES = IDENTITY_PREFIX.length;
  public static final int IDENTITY_BYTES = ROUTE_BYTES + Long.BYTES;
  public static final int MAX_ENCODED_BYTES = 4 + Long.BYTES * 6 + Integer.BYTES * 4 + 8192;

  /** The exclusive resume point inside one bucket; an empty cursor means "start of the bucket". */
  public record Position(Optional<ByteKey> afterIdentity) {
    public Position {
      Objects.requireNonNull(afterIdentity, "afterIdentity");
      afterIdentity.ifPresent(identity -> {
        if (identity.length() == 0) {
          throw new IllegalArgumentException("a GC resume identity must not be empty");
        }
      });
    }

    public static Position start() {
      return new Position(Optional.empty());
    }
  }

  public GcCheckpoint {
    Objects.requireNonNull(tableGroupId, "tableGroupId");
    Objects.requireNonNull(domain, "domain");
    Objects.requireNonNull(position, "position");
    if (tableGroupId.isBlank() || maintenanceEpoch <= 0 || bucket < 0
        || bucketCount < 1 || bucketCount > MAX_BUCKETS || bucket >= bucketCount
        || protectedSequence < 0 || protectedRevision < 0 || completedPages < 0) {
      throw new IllegalArgumentException("invalid GC checkpoint");
    }
  }

  /** Row identity of one shard. The epoch is in the value, so a takeover replaces the same row. */
  public static ByteKey identity(String tableGroupId, SystemRecordClosure.Domain domain,
      int bucket) {
    Objects.requireNonNull(tableGroupId, "tableGroupId");
    Objects.requireNonNull(domain, "domain");
    if (tableGroupId.isBlank() || bucket < 0) {
      throw new IllegalArgumentException("invalid GC checkpoint identity");
    }
    byte[] group = tableGroupId.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    if (group.length == 0 || group.length > 255) {
      throw new IllegalArgumentException("table group id exceeds its bound");
    }
    var buffer = ByteBuffer.allocate(IDENTITY_PREFIX.length + 1 + group.length + 4 + Integer.BYTES);
    buffer.put(IDENTITY_PREFIX).put((byte) group.length).put(group).putInt(domain.ordinal())
        .putInt(bucket);
    return ByteKey.copyOf(buffer.array());
  }

  public ByteKey encode() {
    byte[] cursor = position.afterIdentity().map(ByteKey::bytes).orElseGet(() -> new byte[0]);
    byte[] group = tableGroupId.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    // Layout: magic, group length + group, then 4 ints (domain, bucket, bucketCount, cursor length),
    // 4 longs, 1 presence byte, cursor.
    return ByteKey.copyOf(ByteBuffer.allocate(VALUE_MAGIC.length + Integer.BYTES + group.length
            + Integer.BYTES * 4 + Long.BYTES * 4 + 1 + cursor.length)
        .put(VALUE_MAGIC)
        .putInt(group.length)
        .put(group)
        .putInt(domain.ordinal())
        .putInt(bucket)
        .putInt(bucketCount)
        .putLong(maintenanceEpoch)
        .putLong(protectedSequence)
        .putLong(protectedRevision)
        .putLong(completedPages)
        .put((byte) (position.afterIdentity().isPresent() ? 1 : 0))
        .putInt(cursor.length)
        .put(cursor)
        .array());
  }

  public static GcCheckpoint decode(ByteKey encoded) {
    Objects.requireNonNull(encoded, "encoded");
    if (encoded.length() < VALUE_MAGIC.length || encoded.length() > MAX_ENCODED_BYTES) {
      throw invalid();
    }
    try {
      ByteBuffer input = ByteBuffer.wrap(encoded.bytes());
      byte[] magic = new byte[VALUE_MAGIC.length];
      input.get(magic);
      if (!Arrays.equals(magic, VALUE_MAGIC)) throw invalid();
      int groupLength = input.getInt();
      if (groupLength <= 0 || groupLength > 255 || input.remaining() < groupLength) throw invalid();
      byte[] group = new byte[groupLength];
      input.get(group);
      int domainOrdinal = input.getInt();
      SystemRecordClosure.Domain[] domains = SystemRecordClosure.Domain.values();
      if (domainOrdinal < 0 || domainOrdinal >= domains.length) throw invalid();
      int bucket = input.getInt();
      int bucketCount = input.getInt();
      long epoch = input.getLong();
      long protectedSequence = input.getLong();
      long protectedRevision = input.getLong();
      long completedPages = input.getLong();
      boolean hasCursor = input.get() != 0;
      int cursorLength = input.getInt();
      if (cursorLength < 0 || input.remaining() != cursorLength || hasCursor && cursorLength == 0) {
        throw invalid();
      }
      byte[] cursor = new byte[cursorLength];
      input.get(cursor);
      Optional<ByteKey> after = hasCursor ? Optional.of(ByteKey.copyOf(cursor)) : Optional.empty();
      return new GcCheckpoint(new String(group, java.nio.charset.StandardCharsets.UTF_8),
          domains[domainOrdinal], epoch, bucket, bucketCount, protectedSequence, protectedRevision,
          new Position(after), completedPages);
    } catch (java.nio.BufferUnderflowException | IllegalArgumentException malformed) {
      throw new IllegalArgumentException("invalid GC checkpoint encoding", malformed);
    }
  }

  /**
   * Builds the takeover state: the new epoch owns the shard but inherits the previous resume
   * position and the tighter of the two protection boundaries. Progress never moves backwards, and
   * a takeover never widens a window a previous round already narrowed.
   */
  public GcCheckpoint resumeFrom(GcCheckpoint previous) {
    Objects.requireNonNull(previous, "previous");
    if (!tableGroupId.equals(previous.tableGroupId) || domain != previous.domain
        || bucket != previous.bucket || previous.maintenanceEpoch >= maintenanceEpoch) {
      throw new IllegalArgumentException("checkpoint takeover must be a newer epoch of one shard");
    }
    return new GcCheckpoint(tableGroupId, domain, maintenanceEpoch, bucket, bucketCount,
        Math.min(protectedSequence, previous.protectedSequence),
        Math.min(protectedRevision, previous.protectedRevision),
        previous.position(), previous.completedPages());
  }

  /** True when this checkpoint belongs to the same shard but an older maintenance epoch. */
  public boolean isSupersededBy(GcCheckpoint other) {
    Objects.requireNonNull(other, "other");
    return tableGroupId.equals(other.tableGroupId) && domain == other.domain
        && bucket == other.bucket && other.maintenanceEpoch > maintenanceEpoch;
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("invalid GC checkpoint encoding");
  }
}
