package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Durable key/version progress and completed safe bounds for one KV history bucket. */
public record HistoryGcCheckpoint(String tableGroupId, ByteKey profile, ByteKey registryIdentity,
    long ownerEpoch, int bucket, int bucketCount, InvocationId operationId, long generation,
    long safeRevision, long safeSequence, Optional<ByteKey> afterKey, Optional<ByteKey> activeKey,
    long afterRevision, boolean exhausted, long completedRevision, long completedSequence, long completedGeneration) {
  private static final byte[] MAGIC = {'H', 'G', 'V', 1};
  private static final byte[] IDENTITY_PREFIX = {'H', 'G', 'C', 1};
  private static final int MAX_GROUP_BYTES = 255;
  private static final int MAX_KEY_BYTES = 32767;
  private static final int FIXED_BYTES = 4 + 3 * Integer.BYTES + 32 + 16 + 16
      + 8 * Long.BYTES + 1 + 2 * Integer.BYTES;
  public static final int MAX_ENCODED_BYTES = FIXED_BYTES + MAX_GROUP_BYTES + 2 * MAX_KEY_BYTES;

  public HistoryGcCheckpoint {
    groupBytes(tableGroupId);
    Objects.requireNonNull(profile, "profile");
    Objects.requireNonNull(registryIdentity, "registryIdentity");
    Objects.requireNonNull(operationId, "operationId");
    checkKey(afterKey);
    checkKey(activeKey);
    if (profile.length() != 32 || registryIdentity.length() != 16 || ownerEpoch <= 0
        || bucketCount < 1 || bucketCount > 65_536 || bucket < 0 || bucket >= bucketCount
        || generation <= 0 || safeRevision < 0 || safeSequence < 0 || afterRevision < 0
        || completedRevision < 0 || completedSequence < 0 || completedGeneration < 0
        || completedGeneration > generation
        || completedGeneration == 0 && (completedRevision != 0 || completedSequence != 0)
        || completedRevision > safeRevision || completedSequence > safeSequence
        || activeKey.isEmpty() && afterRevision != 0 || exhausted && activeKey.isPresent()
        || activeKey.isPresent() && afterKey.isPresent()
            && activeKey.orElseThrow().compareTo(afterKey.orElseThrow()) <= 0) {
      throw new IllegalArgumentException("invalid history GC checkpoint");
    }
  }

  public static HistoryGcCheckpoint start(String group, long epoch, int bucket, int buckets,
      HistoryGcPermit permit) {
    return new HistoryGcCheckpoint(group, permit.profile(), permit.registryIdentity(), epoch,
        bucket, buckets, permit.operationId(), permit.generation(), permit.safeRevision(),
        permit.safeSequence(), Optional.empty(), Optional.empty(), 0, false, 0, 0, 0);
  }

  /** A sweep freezes both safe bounds until all its keys and versions have been confirmed. */
  HistoryGcCheckpoint resumeFrom(HistoryGcCheckpoint previous) {
    requireSameShard(previous);
    if (ownerEpoch < previous.ownerEpoch || generation < previous.generation
        || safeRevision < previous.safeRevision || safeSequence < previous.safeSequence) {
      throw new IllegalArgumentException("history GC authority regressed");
    }
    boolean sameOperation = generation == previous.generation && operationId.equals(previous.operationId);
    if (generation == previous.generation && !sameOperation) {
      throw new IllegalArgumentException("history GC generation has another operation");
    }
    boolean restart = !sameOperation || previous.exhausted
        && (safeRevision > previous.safeRevision || safeSequence > previous.safeSequence);
    var plan = restart ? this : previous;
    return new HistoryGcCheckpoint(tableGroupId, profile, registryIdentity, ownerEpoch, bucket,
        bucketCount, plan.operationId, plan.generation, plan.safeRevision, plan.safeSequence,
        restart ? Optional.empty() : previous.afterKey,
        restart ? Optional.empty() : previous.activeKey,
        restart ? 0 : previous.afterRevision, !restart && previous.exhausted,
        previous.completedRevision, previous.completedSequence, previous.completedGeneration);
  }

  /** Persist the active key before dispatching its first delete page. */
  public HistoryGcCheckpoint beginKey(ByteKey key) {
    if (exhausted || activeKey.isPresent()) throw new IllegalStateException("history GC key already active");
    return position(afterKey, Optional.of(key), 0, false, completedRevision, completedSequence);
  }

  /** The caller invokes this only after the collector's exact deletion readback has completed. */
  public HistoryGcCheckpoint afterPage(long lastRevision, boolean keyExhausted, int deleted) {
    if (exhausted || activeKey.isEmpty() || deleted < 0 || lastRevision < afterRevision
        || lastRevision > safeRevision
        || !keyExhausted && lastRevision == afterRevision && deleted == 0) {
      throw new IllegalArgumentException("history GC version page made no valid progress");
    }
    return keyExhausted
        ? position(activeKey, Optional.empty(), 0, false, completedRevision, completedSequence)
        : position(afterKey, activeKey, lastRevision, false, completedRevision, completedSequence);
  }

  /** Only a backend EOF after the last fully drained key can publish a completed safe bound. */
  public HistoryGcCheckpoint finishBucket() {
    if (activeKey.isPresent()) throw new IllegalStateException("history GC key is incomplete");
    return new HistoryGcCheckpoint(tableGroupId, profile, registryIdentity, ownerEpoch, bucket,
        bucketCount, operationId, generation, safeRevision, safeSequence, afterKey, Optional.empty(),
        0, true, safeRevision, safeSequence, generation);
  }

  private HistoryGcCheckpoint position(Optional<ByteKey> after, Optional<ByteKey> active,
      long revision, boolean finished, long completedRevision, long completedSequence) {
    return new HistoryGcCheckpoint(tableGroupId, profile, registryIdentity, ownerEpoch, bucket,
        bucketCount, operationId, generation, safeRevision, safeSequence, after, active, revision,
        finished, completedRevision, completedSequence, completedGeneration);
  }

  void requireSameShard(HistoryGcCheckpoint other) {
    if (!tableGroupId.equals(other.tableGroupId) || !profile.equals(other.profile)
        || !registryIdentity.equals(other.registryIdentity) || bucket != other.bucket
        || bucketCount != other.bucketCount) throw new IllegalArgumentException("history GC shard differs");
  }

  static ByteKey identity(String group, int bucket) {
    byte[] bytes = groupBytes(group);
    if (bucket < 0 || bucket >= 65_536) throw new IllegalArgumentException("invalid history GC bucket");
    return ByteKey.copyOf(ByteBuffer.allocate(4 + 4 + bytes.length + 4)
        .put(IDENTITY_PREFIX).putInt(bytes.length).put(bytes).putInt(bucket).array());
  }

  public ByteKey encode() {
    byte[] group = groupBytes(tableGroupId);
    int keys = afterKey.map(ByteKey::length).orElse(0) + activeKey.map(ByteKey::length).orElse(0);
    var output = ByteBuffer.allocate(FIXED_BYTES + group.length + keys)
        .put(MAGIC).putInt(group.length).put(group).put(profile.bytes()).put(registryIdentity.bytes())
        .putLong(ownerEpoch).putInt(bucket).putInt(bucketCount).put(operationId.bytes())
        .putLong(generation).putLong(safeRevision).putLong(safeSequence).putLong(afterRevision)
        .put((byte) (exhausted ? 1 : 0)).putLong(completedRevision).putLong(completedSequence)
        .putLong(completedGeneration);
    putKey(output, afterKey);
    putKey(output, activeKey);
    return ByteKey.copyOf(output.array());
  }

  public static HistoryGcCheckpoint decode(ByteKey encoded) {
    if (encoded.length() > MAX_ENCODED_BYTES) throw invalid();
    try {
      var input = ByteBuffer.wrap(encoded.bytes());
      byte[] magic = new byte[4];
      input.get(magic);
      if (!Arrays.equals(magic, MAGIC)) throw invalid();
      int length = input.getInt();
      if (length < 1 || length > MAX_GROUP_BYTES) throw invalid();
      byte[] group = new byte[length];
      input.get(group);
      String groupId = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(group)).toString();
      ByteKey profile = readBytes(input, 32);
      ByteKey registry = readBytes(input, 16);
      long epoch = input.getLong();
      int bucket = input.getInt();
      int buckets = input.getInt();
      var operation = InvocationId.fromBytes(readBytes(input, 16).bytes());
      long generation = input.getLong();
      long revision = input.getLong();
      long sequence = input.getLong();
      long afterRevision = input.getLong();
      byte exhausted = input.get();
      if (exhausted != 0 && exhausted != 1) throw invalid();
      long completedRevision = input.getLong();
      long completedSequence = input.getLong();
      long completedGeneration = input.getLong();
      var afterKey = readKey(input);
      var activeKey = readKey(input);
      if (input.hasRemaining()) throw invalid();
      return new HistoryGcCheckpoint(groupId, profile, registry, epoch, bucket, buckets, operation,
          generation, revision, sequence, afterKey, activeKey, afterRevision, exhausted == 1,
          completedRevision, completedSequence, completedGeneration);
    } catch (java.nio.BufferUnderflowException | CharacterCodingException malformed) {
      throw new IllegalArgumentException("invalid history GC checkpoint encoding", malformed);
    }
  }

  private static void putKey(ByteBuffer output, Optional<ByteKey> key) {
    output.putInt(key.map(ByteKey::length).orElse(-1));
    key.ifPresent(value -> output.put(value.bytes()));
  }

  private static Optional<ByteKey> readKey(ByteBuffer input) {
    int length = input.getInt();
    if (length == -1) return Optional.empty();
    if (length < 0 || length > MAX_KEY_BYTES || length > input.remaining()) throw invalid();
    return Optional.of(readBytes(input, length));
  }

  private static ByteKey readBytes(ByteBuffer input, int length) {
    byte[] bytes = new byte[length];
    input.get(bytes);
    return ByteKey.copyOf(bytes);
  }

  private static void checkKey(Optional<ByteKey> key) {
    Objects.requireNonNull(key, "key");
    if (key.filter(value -> value.length() > MAX_KEY_BYTES).isPresent()) throw invalid();
  }

  private static byte[] groupBytes(String group) {
    Objects.requireNonNull(group, "tableGroupId");
    byte[] bytes = group.getBytes(StandardCharsets.UTF_8);
    if (group.isBlank() || bytes.length > MAX_GROUP_BYTES
        || !group.equals(new String(bytes, StandardCharsets.UTF_8))) throw invalid();
    return bytes;
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("invalid history GC checkpoint encoding");
  }
}
