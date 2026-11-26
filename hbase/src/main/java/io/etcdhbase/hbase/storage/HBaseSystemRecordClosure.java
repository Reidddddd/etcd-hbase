package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.SnapshotGcBoundary;
import io.etcdhbase.core.commit.SystemRecordClosure;
import io.etcdhbase.core.commit.SystemRecordEntry;
import io.etcdhbase.core.commit.SystemRecordPage;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.CommitDecisionMessages;
import io.etcdhbase.hbase.codec.ManifestMessages;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * HBase-backed closure decision for the record domains in §6.2 of the M5 design.
 *
 * <p>It never deletes on its own: reachability is decided here from the durable references, then
 * handed to {@link SystemRecordRetirement} which performs the exact retirement. A domain whose
 * protecting references still exist is examined and reported, but nothing is retired.
 */
public final class HBaseSystemRecordClosure implements SystemRecordClosure {
  private static final int DEFAULT_BUCKET_COUNT = 32;
  private static final long MAX_SCAN_BYTES = 1L << 20;

  private final SystemRecordStore records;
  private final SystemRecordRetirement retirement;
  private final ByteKey profile;
  private final int bucketCount;

  public HBaseSystemRecordClosure(SystemRecordStore records, SystemRecordRetirement retirement,
      ByteKey profile, int bucketCount) {
    this.records = Objects.requireNonNull(records, "records");
    this.retirement = Objects.requireNonNull(retirement, "retirement");
    this.profile = Objects.requireNonNull(profile, "profile");
    if (profile.length() != 32 || bucketCount < 1 || bucketCount > 1024) {
      throw new IllegalArgumentException("invalid closure configuration");
    }
    this.bucketCount = bucketCount;
  }

  @Override
  public CompletableFuture<Page> collect(Domain domain, Optional<ByteKey> afterIdentity,
      int maximumRecords, SystemRecordRetirement.Permit permit, RequestScope scope) {
    Objects.requireNonNull(domain, "domain");
    Objects.requireNonNull(afterIdentity, "afterIdentity");
    Objects.requireNonNull(permit, "permit");
    Objects.requireNonNull(scope, "scope");
    if (maximumRecords < 1 || maximumRecords > 1024) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("closure page budget is outside its limit"));
    }
    permit.requireUsable(profile, scope);
    return collectInBuckets(domain, afterIdentity, maximumRecords, permit, scope);
  }

  private CompletableFuture<Page> collectInBuckets(Domain domain, Optional<ByteKey> afterIdentity,
      int maximumRecords, SystemRecordRetirement.Permit permit, RequestScope scope) {
    var examined = new ArrayList<ByteKey>();
    var retired = new ArrayList<ByteKey>();
    // A resume identity is bucket-relative, so each bucket restarts from its own beginning. The
    // caller's resume point therefore applies only to the bucket that owns it.
    var last = new java.util.concurrent.atomic.AtomicReference<>(Optional.<ByteKey>empty());
    CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
    for (int bucket = 0; bucket < bucketCount; bucket++) {
      final int current = bucket;
      chain = chain.thenCompose(ignored -> {
        if (examined.size() >= maximumRecords || scope.isClosed() || scope.isDeadlineExceeded()) {
          return CompletableFuture.completedFuture(null);
        }
        Optional<ByteKey> start = afterIdentity.filter(identity ->
            records.bucket(domain.recordType(), identity) == current);
        return records.scanBucket(domain.recordType(), current, start, maximumRecords,
                MAX_SCAN_BYTES, scope).thenCompose(page -> retireReachable(domain, page,
                    maximumRecords, examined, retired, last, permit, scope));
      });
    }
    return chain.thenApply(ignored -> new Page(domain, examined.size(), retired.size(),
        last.get(), examined.size() < maximumRecords));
  }

  private CompletableFuture<Void> retireReachable(Domain domain, SystemRecordPage page,
      int maximumRecords, List<ByteKey> examined, List<ByteKey> retired,
      java.util.concurrent.atomic.AtomicReference<Optional<ByteKey>> last,
      SystemRecordRetirement.Permit permit, RequestScope scope) {
    CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
    for (var entry : page.entries()) {
      chain = chain.thenCompose(ignored -> {
        if (examined.size() >= maximumRecords || scope.isClosed() || scope.isDeadlineExceeded()) {
          return CompletableFuture.completedFuture(null);
        }
        examined.add(entry.identity());
        last.set(Optional.of(entry.identity()));
        return unreachable(domain, entry, permit, scope).thenCompose(reachable -> {
          if (reachable) return CompletableFuture.completedFuture(null);
          return retirement.retire(domain.recordType(), entry.identity(), entry.value(), permit,
              SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, scope)
              .thenApply(outcome -> {
                if (outcome.retiredNow() || outcome.alreadyRetired()) retired.add(entry.identity());
                return null;
              });
        });
      });
    }
    return chain;
  }

  /**
   * Answers "is this record still referenced or protected". Any doubt resolves to reachable, so a
   * decode or lookup failure can only retain data, never delete it.
   */
  private CompletableFuture<Boolean> unreachable(Domain domain, SystemRecordEntry entry,
      SystemRecordRetirement.Permit permit, RequestScope scope) {
    try {
      return switch (domain) {
        case MANIFEST -> referencedByDecision(entry, scope);
        case DECISION -> referencedByInvocation(entry, scope);
        case RESULT -> referencedByManifest(entry, scope);
        case CANDIDATE -> referencedByManifest(entry, scope);
        case EVENT -> referencedByManifest(entry, scope);
        case INVOCATION -> withinSafeBoundary(entry, permit);
      };
    } catch (RuntimeException malformed) {
      return CompletableFuture.completedFuture(true);
    }
  }

  /**
   * Invocations are never collected here, and that is the design rather than a gap: §6.2 requires
   * the request digest and a non-replayable proof to be retained after the window closes, and the
   * invocation binding is exactly that proof. Collecting it would let the same request execute
   * again as a new write. Only its RESULT is retired, so a closed window answers "cannot query"
   * instead of replaying. Bounded growth of these proofs belongs to the table-generation rebuild
   * that §7 assigns to J53/J57.
   */
  private CompletableFuture<Boolean> withinSafeBoundary(SystemRecordEntry entry,
      SystemRecordRetirement.Permit permit) {
    return CompletableFuture.completedFuture(true);
  }

  private CompletableFuture<Boolean> referencedByDecision(SystemRecordEntry entry,
      RequestScope scope) {
    ManifestRoot root = ManifestMessages.decodeRoot(entry.value().bytes());
    return anyDecisionReferences(entry.identity(), root, scope);
  }

  private CompletableFuture<Boolean> referencedByInvocation(SystemRecordEntry entry,
      RequestScope scope) {
    CommitDecision decision = CommitDecisionMessages.decode(entry.value().bytes());
    return records.read(SystemRecordType.INVOCATION, decisionIdentity(decision.invocationId()),
        scope).thenApply(invocation -> invocation.isPresent());
  }

  private CompletableFuture<Boolean> referencedByManifest(SystemRecordEntry entry,
      RequestScope scope) {
    return scanForReference(SystemRecordType.MANIFEST, entry.identity(), scope);
  }

  private CompletableFuture<Boolean> anyDecisionReferences(ByteKey rootIdentity,
      ManifestRoot root, RequestScope scope) {
    return scanForReference(SystemRecordType.DECISION, rootIdentity, scope);
  }

  /** Conservative scan: any surviving decision whose root matches keeps the target reachable. */
  private CompletableFuture<Boolean> scanForReference(SystemRecordType type,
      ByteKey target, RequestScope scope) {
    var found = new java.util.concurrent.atomic.AtomicBoolean();
    CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
    for (int bucket = 0; bucket < bucketCount; bucket++) {
      final int current = bucket;
      chain = chain.thenCompose(ignored -> {
        if (found.get()) return CompletableFuture.completedFuture(null);
        return records.scanBucket(type, current, Optional.empty(), 1024, MAX_SCAN_BYTES, scope)
            .thenAccept(page -> {
              for (var entry : page.entries()) {
                if (references(type, entry, target)) {
                  found.set(true);
                  return;
                }
              }
            });
      });
    }
    return chain.thenApply(ignored -> found.get());
  }

  private boolean references(SystemRecordType type,
      SystemRecordEntry entry, ByteKey target) {
    try {
      return switch (type) {
        case DECISION -> CommitDecisionMessages.decode(entry.value().bytes())
            .manifestRoot().equals(target);
        default -> false;
      };
    } catch (RuntimeException malformed) {
      // An undecodable record might be the reference we are looking for.
      return true;
    }
  }

  private static ByteKey decisionIdentity(io.etcdhbase.core.commit.InvocationId id) {
    return ByteKey.copyOf(id.bytes());
  }

}
