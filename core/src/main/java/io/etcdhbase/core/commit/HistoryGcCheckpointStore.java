package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.ToIntFunction;

/** Exact-value CAS ownership of per-bucket KV history progress and completion receipts. */
public final class HistoryGcCheckpointStore {
  private static final int MAX_ATTEMPTS = 8;
  private final SystemRecordStore records;
  private final String group;
  private final ByteKey profile;
  private final ByteKey registryIdentity;
  private final int buckets;
  private final ToIntFunction<ByteKey> bucketOf;

  public HistoryGcCheckpointStore(SystemRecordStore records, String group, ByteKey profile,
      ByteKey registryIdentity, int buckets, ToIntFunction<ByteKey> bucketOf) {
    this.records = Objects.requireNonNull(records, "records");
    this.group = Objects.requireNonNull(group, "group");
    this.profile = Objects.requireNonNull(profile, "profile");
    this.registryIdentity = Objects.requireNonNull(registryIdentity, "registryIdentity");
    this.bucketOf = Objects.requireNonNull(bucketOf, "bucketOf");
    if (profile.length() != 32 || registryIdentity.length() != 16 || buckets < 1 || buckets > 65_536) {
      throw new IllegalArgumentException("invalid history GC checkpoint binding");
    }
    HistoryGcCheckpoint.identity(group, 0);
    this.buckets = buckets;
  }

  /** A receipt binds every progress update to the exact persisted bytes it acquired. */
  public record Held(HistoryGcCheckpoint checkpoint, ByteKey exactBytes) {
    public Held {
      Objects.requireNonNull(checkpoint, "checkpoint");
      Objects.requireNonNull(exactBytes, "exactBytes");
      if (!checkpoint.encode().equals(exactBytes)) throw new IllegalArgumentException("history GC receipt differs");
    }
  }

  /** Empty means a newer owner or stronger plan already owns this bucket. */
  public CompletableFuture<Optional<Held>> claim(long ownerEpoch, int bucket,
      HistoryGcPermit permit, RequestScope scope) {
    permit.requireUsable(profile, scope);
    if (!registryIdentity.equals(permit.registryIdentity())) {
      throw new IllegalArgumentException("history GC pin registry differs");
    }
    var proposed = HistoryGcCheckpoint.start(group, ownerEpoch, bucket, buckets, permit);
    return claim(proposed, scope, 0);
  }

  private CompletableFuture<Optional<Held>> claim(HistoryGcCheckpoint proposed,
      RequestScope scope, int attempt) {
    if (attempt >= MAX_ATTEMPTS) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
    }
    var identity = HistoryGcCheckpoint.identity(group, proposed.bucket());
    return read(proposed.bucket(), scope).thenCompose(found -> {
      if (found.isEmpty()) {
        return records.createIfAbsent(SystemRecordType.RECOVERY_CHECKPOINT, identity,
                proposed.encode(), scope)
            .thenCompose(created -> created == SystemRecordStore.CreateResult.CREATED
                ? CompletableFuture.completedFuture(Optional.of(new Held(proposed, proposed.encode())))
                : claim(proposed, scope, attempt + 1));
      }
      var previous = found.orElseThrow();
      if (previous.ownerEpoch() > proposed.ownerEpoch() || previous.generation() > proposed.generation()
          || previous.safeRevision() > proposed.safeRevision() || previous.safeSequence() > proposed.safeSequence()) {
        return CompletableFuture.completedFuture(Optional.empty());
      }
      var next = proposed.resumeFrom(previous);
      if (next.equals(previous)) {
        return CompletableFuture.completedFuture(Optional.of(new Held(previous, previous.encode())));
      }
      return records.compareAndSet(SystemRecordType.RECOVERY_CHECKPOINT, identity,
              Optional.of(previous.encode()), next.encode(), scope)
          .thenCompose(updated -> updated
              ? CompletableFuture.completedFuture(Optional.of(new Held(next, next.encode())))
              : claim(proposed, scope, attempt + 1));
    });
  }

  public CompletableFuture<Optional<HistoryGcCheckpoint>> read(int bucket, RequestScope scope) {
    if (bucket < 0 || bucket >= buckets) throw new IllegalArgumentException("invalid history GC bucket");
    return records.read(SystemRecordType.RECOVERY_CHECKPOINT,
            HistoryGcCheckpoint.identity(group, bucket), scope)
        .thenApply(found -> found.map(encoded -> {
          var checkpoint = HistoryGcCheckpoint.decode(encoded);
          requireBinding(checkpoint);
          if (checkpoint.bucket() != bucket) throw new IllegalArgumentException("history GC row bucket differs");
          return checkpoint;
        }));
  }

  /** Key intent precedes deletion, so a cancelled page can be resumed after process restart. */
  public CompletableFuture<Boolean> beginKey(Held acquired, ByteKey key, RequestScope scope) {
    return update(acquired, acquired.checkpoint().beginKey(key), scope);
  }

  /** The caller must await exact-version deletion confirmation before publishing this cursor. */
  public CompletableFuture<Boolean> afterPage(Held acquired, long lastRevision,
      boolean keyExhausted, int deleted, RequestScope scope) {
    return update(acquired, acquired.checkpoint().afterPage(lastRevision, keyExhausted, deleted), scope);
  }

  public CompletableFuture<Boolean> finishBucket(Held acquired, RequestScope scope) {
    return update(acquired, acquired.checkpoint().finishBucket(), scope);
  }

  private CompletableFuture<Boolean> update(Held acquired, HistoryGcCheckpoint next, RequestScope scope) {
    requireBinding(acquired.checkpoint());
    requireBinding(next);
    return records.compareAndSet(SystemRecordType.RECOVERY_CHECKPOINT,
        HistoryGcCheckpoint.identity(group, next.bucket()), Optional.of(acquired.exactBytes()), next.encode(), scope);
  }

  private void requireBinding(HistoryGcCheckpoint checkpoint) {
    if (!checkpoint.tableGroupId().equals(group) || !checkpoint.profile().equals(profile)
        || !checkpoint.registryIdentity().equals(registryIdentity) || checkpoint.bucketCount() != buckets) {
      throw new IllegalArgumentException("history GC checkpoint binding differs");
    }
    for (var key : java.util.List.of(checkpoint.afterKey(), checkpoint.activeKey())) {
      if (key.isPresent() && bucketOf.applyAsInt(key.orElseThrow()) != checkpoint.bucket()) {
        throw new IllegalArgumentException("history GC key belongs to another bucket");
      }
    }
  }
}
