package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

/** Strict bounded encoding for the immutable registry configuration and mutable shard rows. */
final class SnapshotPinRecordCodec {
  private static final int CONFIG_MAGIC = 0x53504331;
  private static final int INITIALIZATION_MAGIC = 0x53504931;
  private static final int SHARD_MAGIC = 0x53505331;
  private static final int CONFIG_FORMAT_VERSION = 2;
  private static final int FORMAT_VERSION = 1;
  private static final int AUXILIARY_SHARD_FORMAT_VERSION = 2;
  private static final int REGISTRY_ID_BYTES = 16;
  private static final int CHECKSUM_BYTES = 32;
  private static final int MAX_ROOT_ID_BYTES = 1024;
  private static final int MAX_RECORD_BYTES = 2 * 1024 * 1024;

  private SnapshotPinRecordCodec() {}

  record RegistryConfiguration(SnapshotPinConfiguration configuration, ByteKey registryId) {}

  record Initialization(int attempts, Optional<ByteKey> sealedRegistryId) {}

  static ByteKey encodeConfiguration(SnapshotPinConfiguration configuration, ByteKey registryId) {
    requireRegistryId(registryId);
    return checksum(out -> {
      out.writeInt(CONFIG_MAGIC);
      out.writeInt(CONFIG_FORMAT_VERSION);
      out.writeInt(configuration.shardCount());
      out.writeInt(configuration.slotsPerShard());
      out.writeLong(configuration.leaseMillis());
      writeFixed(out, registryId, REGISTRY_ID_BYTES);
    });
  }

  static RegistryConfiguration decodeConfiguration(ByteKey encoded) {
    return decode(encoded, input -> {
      if (input.readInt() != CONFIG_MAGIC || input.readInt() != CONFIG_FORMAT_VERSION) {
        throw new IllegalArgumentException("unsupported snapshot pin configuration format");
      }
      int shards = input.readInt();
      int slots = input.readInt();
      long leaseMillis = input.readLong();
      if (leaseMillis <= 0 || leaseMillis > Long.MAX_VALUE / 1_000_000L) {
        throw new IllegalArgumentException("invalid snapshot pin lease duration");
      }
      ByteKey registryId = readFixed(input, REGISTRY_ID_BYTES);
      requireRegistryId(registryId);
      return new RegistryConfiguration(
          new SnapshotPinConfiguration(shards, slots, Duration.ofMillis(leaseMillis)), registryId);
    });
  }

  static ByteKey encodeInitialization(Initialization initialization) {
    if (initialization.attempts() < 0) throw new IllegalArgumentException("negative initialization count");
    initialization.sealedRegistryId().ifPresent(SnapshotPinRecordCodec::requireRegistryId);
    return checksum(out -> {
      out.writeInt(INITIALIZATION_MAGIC);
      out.writeInt(FORMAT_VERSION);
      out.writeInt(initialization.attempts());
      out.writeBoolean(initialization.sealedRegistryId().isPresent());
      writeFixed(out, initialization.sealedRegistryId().orElse(ByteKey.empty()), REGISTRY_ID_BYTES);
    });
  }

  static Initialization decodeInitialization(ByteKey encoded) {
    return decode(encoded, input -> {
      if (input.readInt() != INITIALIZATION_MAGIC || input.readInt() != FORMAT_VERSION) {
        throw new IllegalArgumentException("unsupported snapshot pin initialization format");
      }
      int attempts = input.readInt();
      boolean sealed = input.readBoolean();
      ByteKey registryId = readFixed(input, REGISTRY_ID_BYTES);
      if (attempts < 0 || (!sealed && !allZero(registryId))) {
        throw new IllegalArgumentException("invalid snapshot pin initialization state");
      }
      if (sealed) requireRegistryId(registryId);
      return new Initialization(attempts, sealed ? Optional.of(registryId) : Optional.empty());
    });
  }

  static ByteKey encodeShard(SnapshotPinShard shard) {
    boolean needsAuxiliaryFormat = shard.slots().stream().anyMatch(slot ->
        slot.status() == SnapshotPinShard.Status.ACTIVE
            && slot.requiredSequence() > 0 && slot.transactionId().isEmpty());
    return checksum(out -> {
      out.writeInt(SHARD_MAGIC);
      out.writeInt(needsAuxiliaryFormat ? AUXILIARY_SHARD_FORMAT_VERSION : FORMAT_VERSION);
      out.writeInt(shard.shard());
      out.writeLong(shard.generation());
      out.writeLong(shard.revisionFloor());
      out.writeLong(shard.sequenceFloor());
      out.writeInt(shard.slots().size());
      for (SnapshotPinShard.Slot slot : shard.slots()) writeSlot(out, slot);
    });
  }

  static SnapshotPinShard decodeShard(ByteKey encoded) {
    return decode(encoded, input -> {
      int magic = input.readInt();
      int version = input.readInt();
      if (magic != SHARD_MAGIC || version != FORMAT_VERSION
          && version != AUXILIARY_SHARD_FORMAT_VERSION) {
        throw new IllegalArgumentException("unsupported snapshot pin shard format");
      }
      int shard = input.readInt();
      long generation = input.readLong();
      long revisionFloor = input.readLong();
      long sequenceFloor = input.readLong();
      int count = input.readInt();
      if (count <= 0 || count > SnapshotPinConfiguration.MAX_SLOTS_PER_SHARD) {
        throw new IllegalArgumentException("snapshot pin shard has an invalid slot count");
      }
      var slots = new ArrayList<SnapshotPinShard.Slot>(count);
      for (int index = 0; index < count; index++) {
        var slot = readSlot(input);
        if (version == FORMAT_VERSION && slot.status() == SnapshotPinShard.Status.ACTIVE
            && slot.requiredSequence() > 0 && slot.transactionId().isEmpty()) {
          throw new IllegalArgumentException("legacy pin shard contains an unbound sequence");
        }
        slots.add(slot);
      }
      return new SnapshotPinShard(shard, generation, revisionFloor, sequenceFloor, slots);
    });
  }

  private static void writeSlot(DataOutputStream out, SnapshotPinShard.Slot slot) throws IOException {
    out.writeByte(slot.status().ordinal());
    out.writeLong(slot.generation());
    writeFixed(out, slot.pinId(), 16);
    out.writeLong(slot.requiredRevision());
    out.writeLong(slot.requiredSequence());
    writeFixed(out, slot.viewDigest(), 32);
    out.writeLong(slot.expiresAtEpochMillis());
    out.writeBoolean(slot.transactionId().isPresent());
    writeFixed(out, slot.transactionId().map(TransactionId::bytes)
        .map(ByteKey::copyOf).orElse(ByteKey.empty()), 16);
    writeBytes(out, slot.rootIdentity().orElse(ByteKey.empty()));
    writeFixed(out, slot.rootDigest().orElse(ByteKey.empty()), 32);
  }

  private static SnapshotPinShard.Slot readSlot(DataInputStream input) throws IOException {
    int ordinal = input.readUnsignedByte();
    SnapshotPinShard.Status[] states = SnapshotPinShard.Status.values();
    if (ordinal >= states.length) throw new IllegalArgumentException("unknown snapshot pin slot state");
    SnapshotPinShard.Status status = states[ordinal];
    long generation = input.readLong();
    ByteKey pinId = readFixed(input, 16);
    long revision = input.readLong();
    long sequence = input.readLong();
    ByteKey viewDigest = readFixed(input, 32);
    long expiry = input.readLong();
    boolean hasTransaction = input.readBoolean();
    ByteKey transactionBytes = readFixed(input, 16);
    ByteKey rootIdentity = readBytes(input, MAX_ROOT_ID_BYTES);
    ByteKey rootDigest = readFixed(input, 32);

    if (status == SnapshotPinShard.Status.EMPTY) {
      requireZero(generation, pinId, revision, sequence, viewDigest, expiry,
          hasTransaction, transactionBytes, rootIdentity, rootDigest);
      return SnapshotPinShard.Slot.empty();
    }
    if (status != SnapshotPinShard.Status.ACTIVE) {
      if (revision != 0 || sequence != 0 || !allZero(viewDigest) || expiry != 0
          || hasTransaction || rootIdentity.length() != 0) {
        throw new IllegalArgumentException("terminal snapshot pin slot carries active state");
      }
      requireZeros(transactionBytes, rootDigest);
      return new SnapshotPinShard.Slot(status, generation, pinId, 0, 0,
          ByteKey.empty(), 0, Optional.empty(), Optional.empty(), Optional.empty());
    }
    Optional<TransactionId> transaction = hasTransaction
        ? Optional.of(TransactionId.fromBytes(transactionBytes.bytes())) : Optional.empty();
    Optional<ByteKey> root = rootIdentity.length() == 0 ? Optional.empty() : Optional.of(rootIdentity);
    Optional<ByteKey> digest = root.isPresent() ? Optional.of(rootDigest) : Optional.empty();
    if (!hasTransaction && !allZero(transactionBytes)
        || root.isEmpty() && !allZero(rootDigest)) {
      throw new IllegalArgumentException("active snapshot pin contains an unbound reference");
    }
    return SnapshotPinShard.Slot.active(generation, pinId, revision, sequence, viewDigest,
        expiry, transaction, root, digest);
  }

  private static void requireZero(long generation, ByteKey pinId, long revision, long sequence,
      ByteKey viewDigest, long expiry, boolean transaction, ByteKey transactionBytes,
      ByteKey rootIdentity, ByteKey rootDigest) {
    if (generation != 0 || !allZero(pinId) || revision != 0 || sequence != 0
        || !allZero(viewDigest) || expiry != 0 || transaction
        || !allZero(transactionBytes) || rootIdentity.length() != 0 || !allZero(rootDigest)) {
      throw new IllegalArgumentException("empty snapshot pin slot carries encoded state");
    }
  }

  private static void requireZeros(ByteKey first, ByteKey second) {
    if (!allZero(first) || !allZero(second)) {
      throw new IllegalArgumentException("terminal snapshot pin slot carries encoded references");
    }
  }

  private static boolean allZero(ByteKey value) {
    for (byte part : value.bytes()) if (part != 0) return false;
    return true;
  }

  private static void requireRegistryId(ByteKey registryId) {
    if (registryId.length() != REGISTRY_ID_BYTES || allZero(registryId)) {
      throw new IllegalArgumentException("invalid snapshot pin registry identity");
    }
  }

  private static void writeFixed(DataOutputStream out, ByteKey value, int length) throws IOException {
    byte[] bytes = value.bytes();
    if (bytes.length != 0 && bytes.length != length) {
      throw new IllegalArgumentException("snapshot pin field has an invalid fixed width");
    }
    out.write(bytes.length == 0 ? new byte[length] : bytes);
  }

  private static ByteKey readFixed(DataInputStream input, int length) throws IOException {
    byte[] bytes = input.readNBytes(length);
    if (bytes.length != length) throw new IllegalArgumentException("truncated snapshot pin field");
    return ByteKey.copyOf(bytes);
  }

  private static void writeBytes(DataOutputStream out, ByteKey value) throws IOException {
    byte[] bytes = value.bytes();
    if (bytes.length > MAX_ROOT_ID_BYTES) {
      throw new IllegalArgumentException("snapshot pin reference exceeds its bound");
    }
    ManifestDigest.writeBytes(out, bytes);
  }

  private static ByteKey readBytes(DataInputStream input, int maximum) throws IOException {
    int length = input.readInt();
    if (length < 0 || length > maximum) {
      throw new IllegalArgumentException("snapshot pin reference exceeds its bound");
    }
    byte[] bytes = input.readNBytes(length);
    if (bytes.length != length) throw new IllegalArgumentException("truncated snapshot pin reference");
    return ByteKey.copyOf(bytes);
  }

  private static ByteKey checksum(ManifestDigest.Encoder writer) {
    byte[] content = ManifestDigest.encode(writer::write);
    if (content.length + CHECKSUM_BYTES > MAX_RECORD_BYTES) {
      throw new IllegalArgumentException("snapshot pin record exceeds its byte budget");
    }
    byte[] result = Arrays.copyOf(content, content.length + CHECKSUM_BYTES);
    System.arraycopy(ManifestDigest.sha256(content).bytes(), 0, result, content.length,
        CHECKSUM_BYTES);
    return ByteKey.copyOf(result);
  }

  private static <T> T decode(ByteKey encoded, Decoder<T> decoder) {
    byte[] bytes = encoded.bytes();
    if (bytes.length <= CHECKSUM_BYTES || bytes.length > MAX_RECORD_BYTES) {
      throw new IllegalArgumentException("snapshot pin record has an invalid size");
    }
    int bodyLength = bytes.length - CHECKSUM_BYTES;
    byte[] body = Arrays.copyOf(bytes, bodyLength);
    byte[] expected = Arrays.copyOfRange(bytes, bodyLength, bytes.length);
    if (!MessageDigest.isEqual(ManifestDigest.sha256(body).bytes(), expected)) {
      throw new IllegalArgumentException("snapshot pin record checksum mismatch");
    }
    try (var input = new DataInputStream(new ByteArrayInputStream(body))) {
      T value = decoder.read(input);
      if (input.available() != 0) throw new IllegalArgumentException("trailing snapshot pin record bytes");
      return value;
    } catch (IOException truncated) {
      throw new IllegalArgumentException("truncated snapshot pin record", truncated);
    }
  }

  @FunctionalInterface
  private interface Decoder<T> { T read(DataInputStream input) throws IOException; }
}
