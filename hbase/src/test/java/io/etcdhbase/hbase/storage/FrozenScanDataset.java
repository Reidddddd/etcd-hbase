package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.KeyRange;
import io.etcdhbase.core.storage.StorageMetadata;
import io.etcdhbase.core.storage.StoredRecord;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/** Synthetic mutation log and linear oracle; no row codec, storage adapter or merge dependency. */
final class FrozenScanDataset {
  static final long SEED = 20261030L;
  private final List<StoredRecord> mutations;
  private final List<ByteKey> keys;
  private final ByteKey hotKey;

  private FrozenScanDataset(List<StoredRecord> mutations, List<ByteKey> keys, ByteKey hotKey) {
    this.mutations = List.copyOf(mutations);
    this.keys = List.copyOf(keys);
    this.hotKey = hotKey;
  }

  static FrozenScanDataset create() {
    var builder = new Builder();
    builder.group("recovery/a/b/c/d/", 48, 32);
    builder.group("small/", 2, 8);
    List<ByteKey> deleted = builder.group("deleted/", 16, 16);
    builder.group("large/", 2, 131072);
    for (int index = 0; index < 8; index++) builder.create(new byte[] {0, (byte) index, (byte) 0xff}, 7);
    byte[] longKey = new byte[2048];
    Arrays.fill(longKey, (byte) 'm'); longKey[0] = 'l'; longKey[1] = '/'; longKey[1000] = 0; longKey[2047] = (byte) 0xff;
    builder.create(longKey, 64);
    ByteKey hot = builder.create("hot/fixed".getBytes(StandardCharsets.UTF_8), 64);
    for (ByteKey key : deleted) builder.delete(key);
    for (int index = 0; index < deleted.size(); index += 4) builder.recreate(deleted.get(index), 16);
    for (int index = 0; index < 20; index++) builder.update(hot, 64);
    return new FrozenScanDataset(builder.mutations, new ArrayList<>(builder.current.keySet()), hot);
  }

  List<StoredRecord> mutations() { return mutations; }
  List<ByteKey> keys() { return keys; }
  ByteKey hotKey() { return hotKey; }

  List<StoredRecord> liveView(KeyRange range, long upper) {
    Map<ByteKey, StoredRecord> view = new LinkedHashMap<>();
    for (StoredRecord mutation : mutations) {
      if (mutation.metadata().modificationRevision() <= upper) view.put(mutation.key(), mutation);
    }
    return view.values().stream().filter(record -> record.metadata().state() == StorageMetadata.State.LIVE)
        .filter(record -> range.contains(record.key()))
        .sorted((left, right) -> Arrays.compareUnsigned(left.key().bytes(), right.key().bytes())).toList();
  }

  String digest() {
    try {
      MessageDigest hash = MessageDigest.getInstance("SHA-256");
      for (StoredRecord record : mutations) {
        byte[] key = record.key().bytes();
        hash.update(ByteBuffer.allocate(4).putInt(key.length).array()); hash.update(key);
        var metadata = record.metadata();
        hash.update(ByteBuffer.allocate(40).putLong(metadata.createRevision()).putLong(metadata.modificationRevision())
            .putLong(metadata.version()).putLong(metadata.leaseId()).putInt(metadata.state().ordinal()).putInt(metadata.valueLength()).array());
        hash.update((byte) (record.hasValue() ? 1 : 0));
        record.value().ifPresent(hash::update);
      }
      return HexFormat.of().formatHex(hash.digest());
    } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
  }

  private static final class Builder {
    final Random random = new Random(SEED);
    final List<StoredRecord> mutations = new ArrayList<>();
    final Map<ByteKey, StoredRecord> current = new LinkedHashMap<>();
    long revision;

    List<ByteKey> group(String prefix, int count, int bytes) {
      List<ByteKey> keys = new ArrayList<>();
      for (int index = 0; index < count; index++) keys.add(create(String.format(Locale.ROOT, "%s%03d", prefix, index).getBytes(StandardCharsets.UTF_8), bytes));
      return keys;
    }
    ByteKey create(byte[] raw, int bytes) { ByteKey key = ByteKey.copyOf(raw); recreate(key, bytes); return key; }
    void recreate(ByteKey key, int bytes) {
      long next = ++revision;
      append(StoredRecord.live(key, new StorageMetadata(next, next, 1, 0, StorageMetadata.State.LIVE, bytes), value(bytes)));
    }
    void update(ByteKey key, int bytes) {
      var previous = current.get(key).metadata();
      append(StoredRecord.live(key, new StorageMetadata(previous.createRevision(), ++revision, previous.version() + 1,
          0, StorageMetadata.State.LIVE, bytes), value(bytes)));
    }
    void delete(ByteKey key) {
      var previous = current.get(key).metadata();
      append(StoredRecord.metadataOnly(key, new StorageMetadata(previous.createRevision(), ++revision, previous.version(),
          0, StorageMetadata.State.TOMBSTONE, 0)));
    }
    byte[] value(int bytes) { byte[] value = new byte[bytes]; random.nextBytes(value); return value; }
    void append(StoredRecord record) { mutations.add(record); current.put(record.key(), record); }
  }
}
