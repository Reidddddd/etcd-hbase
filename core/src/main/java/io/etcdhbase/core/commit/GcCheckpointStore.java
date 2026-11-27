package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Claims, resumes and hands over one GC shard from its durable checkpoint.
 *
 * <p>§6.3 requires fencing before takeover and requires an older worker's checkpoint update to stay
 * bound to the generation it claimed under. Every update is an exact-value compare-and-set;
 * a new owner or Compact operation must finish FENCING before payload collection can start.
 */
public final class GcCheckpointStore {
  private static final int MAX_ATTEMPTS = 8;

  private final SystemRecordStore records;

  public GcCheckpointStore(SystemRecordStore records) {
    this.records = Objects.requireNonNull(records, "records");
  }

  /** The shard state a worker may act on, or the fact that a newer epoch already owns it. */
  public sealed interface Claim permits Held, Superseded {}

  /** Metadata lease at this epoch; payload retirement still requires the SCANNING phase. */
  public record Held(GcCheckpoint checkpoint, ByteKey exactBytes) implements Claim {
    public Held {
      Objects.requireNonNull(checkpoint, "checkpoint");
      Objects.requireNonNull(exactBytes, "exactBytes");
    }
  }

  /** A newer owner epoch or collection plan already owns this shard; the caller stands down. */
  public record Superseded(GcCheckpoint observed) implements Claim {
    public Superseded {
      Objects.requireNonNull(observed, "observed");
    }
  }

  /**
   * Resumes the same plan, takes over a newer owner epoch, or starts a distinct authorized sweep.
   * Concurrent page completions compete through an exact-value CAS.
   */
  public CompletableFuture<Claim> claim(GcCheckpoint proposed, RequestScope scope) {
    Objects.requireNonNull(proposed, "proposed");
    Objects.requireNonNull(scope, "scope");
    if (proposed.phase() != GcCheckpoint.Phase.FENCING || proposed.exhausted()
        || proposed.position().afterIdentity().isPresent() || proposed.fencePosition().afterIdentity().isPresent()
        || proposed.completedPages() != 0 || proposed.sweep() != 0) {
      throw new IllegalArgumentException("a GC lease proposal must start with an empty fence pass");
    }
    return claim(proposed, scope, 0);
  }

  private CompletableFuture<Claim> claim(GcCheckpoint proposed, RequestScope scope, int attempt) {
    if (attempt >= MAX_ATTEMPTS) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
    }
    ByteKey identity = GcCheckpoint.identity(proposed.tableGroupId(), proposed.domain(),
        proposed.bucket());
    return records.read(SystemRecordType.RECOVERY_CHECKPOINT, identity, scope)
        .thenCompose(found -> {
          if (found.isEmpty()) {
            return records.createIfAbsent(SystemRecordType.RECOVERY_CHECKPOINT, identity,
                proposed.encode(), scope).thenCompose(created -> switch (created) {
                  case CREATED -> CompletableFuture.completedFuture(
                      new Held(proposed, proposed.encode()));
                  case ALREADY_PRESENT, CONFLICT -> claim(proposed, scope, attempt + 1);
                });
          }
          GcCheckpoint observed = decode(found.orElseThrow());
          proposed.requireSameShard(observed);
          if (observed.maintenanceEpoch() > proposed.maintenanceEpoch()
              || observed.plan().generation() > proposed.plan().generation()
              || observed.plan().safeSequence() > proposed.plan().safeSequence()
              || observed.plan().safeRevision() > proposed.plan().safeRevision()) {
            return CompletableFuture.completedFuture(new Superseded(observed));
          }
          GcCheckpoint takeover = proposed.resumeFrom(observed);
          if (takeover.equals(observed)) {
            return CompletableFuture.completedFuture(new Held(observed, found.orElseThrow()));
          }
          return records.compareAndSet(SystemRecordType.RECOVERY_CHECKPOINT, identity,
                  Optional.of(found.orElseThrow()), takeover.encode(), scope)
              .thenCompose(updated -> updated
                  ? CompletableFuture.completedFuture(new Held(takeover, takeover.encode()))
                  : claim(proposed, scope, attempt + 1));
        });
  }

  /**
   * Advances one shard only if the exact bytes this worker read are still current. A {@code false}
   * result means another worker moved the shard first; the caller must re-claim rather than retry
   * with stale text.
   */
  public CompletableFuture<Boolean> advance(Held acquired, SystemRecordClosure.Page page,
      RequestScope scope) {
    var previous = acquired.checkpoint();
    if (!previous.encode().equals(acquired.exactBytes())) {
      throw new IllegalArgumentException("checkpoint receipt bytes mismatch");
    }
    var next = previous.after(page);
    requireCursorShard(next);
    return records.compareAndSet(SystemRecordType.RECOVERY_CHECKPOINT,
        GcCheckpoint.identity(next.tableGroupId(), next.domain(), next.bucket()),
        Optional.of(acquired.exactBytes()), next.encode(), scope);
  }

  public CompletableFuture<Optional<GcCheckpoint>> read(String tableGroupId,
      SystemRecordClosure.Domain domain, int bucket, RequestScope scope) {
    return records.read(SystemRecordType.RECOVERY_CHECKPOINT,
            GcCheckpoint.identity(tableGroupId, domain, bucket), scope)
        .thenApply(found -> found.map(encoded -> {
          var checkpoint = decode(encoded);
          if (!checkpoint.tableGroupId().equals(tableGroupId) || checkpoint.domain() != domain
              || checkpoint.bucket() != bucket) {
            throw new IllegalArgumentException("GC checkpoint row identity mismatch");
          }
          return checkpoint;
        }));
  }
  private GcCheckpoint decode(ByteKey encoded) {
    var checkpoint = GcCheckpoint.decode(encoded);
    requireCursorShard(checkpoint);
    return checkpoint;
  }

  private void requireCursorShard(GcCheckpoint checkpoint) {
    for (var position : java.util.List.of(checkpoint.position(), checkpoint.fencePosition())) {
      position.afterIdentity().ifPresent(identity -> {
        if (records.bucket(checkpoint.domain().recordType(), identity) != checkpoint.bucket()) {
          throw new IllegalArgumentException("GC checkpoint cursor belongs to another bucket");
        }
      });
    }
  }

}
