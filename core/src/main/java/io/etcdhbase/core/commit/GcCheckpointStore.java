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
 * bound to the generation it claimed under. Both properties come from the same mechanism: every
 * update is an exact-value compare-and-set on the bytes that were read, so a stale worker loses and
 * a newer epoch always wins.
 */
public final class GcCheckpointStore {
  private static final int MAX_ATTEMPTS = 8;

  private final SystemRecordStore records;

  public GcCheckpointStore(SystemRecordStore records) {
    this.records = Objects.requireNonNull(records, "records");
  }

  /** The shard state a worker may act on, or the fact that a newer epoch already owns it. */
  public sealed interface Claim permits Acquired, Superseded, Absent {}

  /** This worker owns the shard at {@code checkpoint}'s epoch. */
  public record Acquired(GcCheckpoint checkpoint, ByteKey exactBytes) implements Claim {
    public Acquired {
      Objects.requireNonNull(checkpoint, "checkpoint");
      Objects.requireNonNull(exactBytes, "exactBytes");
    }
  }

  /** A different, newer maintenance epoch already owns this shard; the caller must stand down. */
  public record Superseded(GcCheckpoint observed) implements Claim {
    public Superseded {
      Objects.requireNonNull(observed, "observed");
    }
  }

  /** Nothing was ever written for this shard; the caller starts from position zero. */
  public record Absent() implements Claim {}

  /**
   * Claims one shard for {@code maintenanceEpoch}. A takeover replaces the row only when the caller's
   * epoch is strictly newer, so two workers in the same epoch can never both advance it.
   */
  public CompletableFuture<Claim> claim(GcCheckpoint proposed, RequestScope scope) {
    Objects.requireNonNull(proposed, "proposed");
    Objects.requireNonNull(scope, "scope");
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
                      new Acquired(proposed, proposed.encode()));
                  case ALREADY_PRESENT, CONFLICT -> claim(proposed, scope, attempt + 1);
                });
          }
          GcCheckpoint observed = GcCheckpoint.decode(found.orElseThrow());
          if (observed.maintenanceEpoch() > proposed.maintenanceEpoch()) {
            return CompletableFuture.completedFuture(new Superseded(observed));
          }
          if (observed.maintenanceEpoch() == proposed.maintenanceEpoch()) {
            // Same epoch: this is a resume, and it must not silently reset the shard's progress.
            return CompletableFuture.completedFuture(
                new Acquired(observed, found.orElseThrow()));
          }
          GcCheckpoint takeover = proposed.resumeFrom(observed);
          return records.compareAndSet(SystemRecordType.RECOVERY_CHECKPOINT, identity,
                  Optional.of(found.orElseThrow()), takeover.encode(), scope)
              .thenCompose(updated -> updated
                  ? CompletableFuture.completedFuture(new Acquired(takeover, takeover.encode()))
                  : claim(proposed, scope, attempt + 1));
        });
  }

  /**
   * Advances one shard only if the exact bytes this worker read are still current. A {@code false}
   * result means another worker moved the shard first; the caller must re-claim rather than retry
   * with stale text.
   */
  public CompletableFuture<Boolean> advance(GcCheckpoint next, ByteKey expected,
      RequestScope scope) {
    Objects.requireNonNull(next, "next");
    Objects.requireNonNull(expected, "expected");
    Objects.requireNonNull(scope, "scope");
    return records.compareAndSet(SystemRecordType.RECOVERY_CHECKPOINT,
        GcCheckpoint.identity(next.tableGroupId(), next.domain(), next.bucket()),
        Optional.of(expected), next.encode(), scope);
  }

  public CompletableFuture<Optional<GcCheckpoint>> read(String tableGroupId,
      SystemRecordClosure.Domain domain, int bucket, RequestScope scope) {
    return records.read(SystemRecordType.RECOVERY_CHECKPOINT,
            GcCheckpoint.identity(tableGroupId, domain, bucket), scope)
        .thenApply(found -> found.map(GcCheckpoint::decode));
  }
}
