package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.SystemRecordClosure.Domain;
import io.etcdhbase.core.commit.SystemRecordClosure.Page;
import io.etcdhbase.core.commit.SystemRecordEntry;
import io.etcdhbase.core.commit.SystemRecordPage;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.CommitDecisionMessages;
import io.etcdhbase.hbase.codec.EventRevisionIndexMessages;
import io.etcdhbase.hbase.codec.InvocationMessages;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/** Enumerates bounded retirement pages and proves the complete surviving direct reference set. */
public final class HBaseSystemRecordClosure implements io.etcdhbase.core.commit.SystemRecordClosure {
  private static final long MAX_SCAN_BYTES = 8L << 20;
  private final SystemRecordStore records;
  private final HBaseSystemRecordStore retirement;
  private final SystemRecordGcAuthority authority;
  private final ByteKey profile;
  private final int bucketCount;

  public HBaseSystemRecordClosure(HBaseSystemRecordStore store) {
    this(store, store, store.profileFingerprint(), store.bucketCount(), new SystemRecordGcAuthority(store));
  }

  HBaseSystemRecordClosure(SystemRecordStore records, HBaseSystemRecordStore retirement,
      ByteKey profile, int bucketCount, SystemRecordGcAuthority authority) {
    this.authority = Objects.requireNonNull(authority, "authority");
    this.records = Objects.requireNonNull(records, "records");
    this.retirement = Objects.requireNonNull(retirement, "retirement");
    this.profile = Objects.requireNonNull(profile, "profile");
    if (profile.length() != 32 || bucketCount < 1 || bucketCount > 65_536) {
      throw new IllegalArgumentException("invalid closure configuration");
    }
    this.bucketCount = bucketCount;
  }

  /** Collects a bounded domain page, retaining records while any reference or protection holds. */
  public CompletableFuture<Page> collect(Domain domain, int bucket, Optional<ByteKey> afterIdentity,
      int maximumRecords, SystemRecordRetirement.Permit permit, RequestScope scope) {
    Objects.requireNonNull(domain, "domain");
    Objects.requireNonNull(afterIdentity, "afterIdentity");
    Objects.requireNonNull(permit, "permit");
    Objects.requireNonNull(scope, "scope");
    if (maximumRecords < 1 || maximumRecords > 1024 || bucket < 0 || bucket >= bucketCount
        || afterIdentity.filter(identity -> records.bucket(domain.recordType(), identity) != bucket).isPresent()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid closure page budget"));
    }
    permit.requireUsable(profile, scope);
    var progress = new Progress(maximumRecords, afterIdentity);
    return authority.validate(permit, scope).thenCompose(ignored ->
        collectBucket(domain, bucket, afterIdentity, progress, permit, scope))
        .thenApply(exhausted -> new Page(domain, progress.examined, progress.retired,
            progress.lastIdentity, exhausted));
  }

  private CompletableFuture<Boolean> collectBucket(Domain domain, int bucket, Optional<ByteKey> after,
      Progress progress, SystemRecordRetirement.Permit permit, RequestScope scope) {
    requireActive(scope);
    if (progress.remainingRows == 0) return CompletableFuture.completedFuture(false);
    return records.scanBucket(domain.recordType(), bucket, after, progress.remainingRows, MAX_SCAN_BYTES, scope)
        .thenComposeAsync(page -> retireUnreferenced(domain, page, progress, permit, scope).thenCompose(ignored -> {
          progress.remainingRows = Math.max(0, progress.remainingRows - Math.toIntExact(page.physicalRows()));
          page.lastIdentity().ifPresent(identity -> progress.lastIdentity = Optional.of(identity));
          return page.exhausted() ? CompletableFuture.completedFuture(true)
              : collectBucket(domain, bucket, page.lastIdentity(), progress, permit, scope);
        }));
  }

  enum ResultCollection { ABSENT, REFERENCED, RETIRED, CHANGED }

  CompletableFuture<ResultCollection> collectResult(ByteKey identity, SystemRecordRetirement.Permit permit,
      RequestScope scope) {
    return authority.validate(permit, scope).thenCompose(ignored ->
        records.read(SystemRecordType.RESULT, identity, scope)).thenCompose(found -> {
      if (found.isEmpty()) return CompletableFuture.completedFuture(ResultCollection.ABSENT);
      var entry = new SystemRecordEntry(identity, found.orElseThrow());
      return isReferenced(Domain.RESULT, entry, permit, new ProofBudget(), scope).thenCompose(referenced -> {
        if (referenced) return CompletableFuture.completedFuture(ResultCollection.REFERENCED);
        return retirement.retire(SystemRecordType.RESULT, identity, entry.value(), permit,
            SystemRecordRetirement.Reason.CLOSED_INVOCATION_WINDOW, scope)
            .thenApply(outcome -> outcome.retiredNow() || outcome.alreadyRetired()
                ? ResultCollection.RETIRED : ResultCollection.CHANGED);
      });
    });
  }

  private CompletableFuture<Void> retireUnreferenced(Domain domain, SystemRecordPage page,
      Progress progress, SystemRecordRetirement.Permit permit, RequestScope scope) {
    CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
    for (var entry : page.entries()) {
      chain = chain.thenCompose(ignored -> {
        requireActive(scope);
        progress.examined++;
        return isReferenced(domain, entry, permit, progress.proof, scope).thenCompose(referenced -> {
          if (referenced) return CompletableFuture.completedFuture(null);
          return retirement.retire(domain.recordType(), entry.identity(), entry.value(), permit,
              SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, scope).thenAccept(outcome -> {
                if (outcome.retiredNow() || outcome.alreadyRetired()) progress.retired++;
              });
        });
      });
    }
    return chain;
  }

  private CompletableFuture<Boolean> isReferenced(Domain domain, SystemRecordEntry entry,
      SystemRecordRetirement.Permit permit, ProofBudget budget, RequestScope scope) {
    // The retained invocation binding is the non-replay proof, even after its query window closes.
    if (domain == Domain.INVOCATION) return CompletableFuture.completedFuture(true);
    var target = SystemRecordGcTarget.decode(domain.recordType(), entry);
    if (target.revision() >= permit.safeRevision() || target.sequence() >= permit.safeSequence()) {
      return CompletableFuture.completedFuture(true);
    }
    return authority.validate(permit, scope).thenCompose(protections -> {
      if (protections.stream().anyMatch(protection -> protection.transactionId()
          .filter(target.transaction()::equals).isPresent()
              || protection.rootIdentity().filter(entry.identity()::equals).isPresent())) {
        return CompletableFuture.completedFuture(true);
      }
      return scanForReference(SystemRecordType.INVOCATION, reference -> {
        var binding = InvocationMessages.decode(reference.value().bytes());
        SystemRecordReferences.requireIdentity(reference.identity(), ByteKey.copyOf(binding.invocationId().bytes()));
        return binding.transactionId().equals(target.transaction())
            && binding.state() != InvocationBinding.State.WINDOW_CLOSED
            && binding.state() != InvocationBinding.State.NOT_SELECTED;
      }, budget, scope).thenCompose(found -> found ? CompletableFuture.completedFuture(true)
          : scanRecordEdges(domain, entry, target, permit, budget, scope));
    });
  }

  private CompletableFuture<Boolean> scanRecordEdges(Domain domain, SystemRecordEntry entry,
      SystemRecordGcTarget target, SystemRecordRetirement.Permit permit, ProofBudget budget,
      RequestScope scope) {
    return scanForReference(SystemRecordType.DECISION, reference -> {
      var decision = CommitDecisionMessages.decode(reference.value().bytes());
      SystemRecordGcTarget.requireSequenceIdentity(reference.identity(), decision.sequence());
      return domain == Domain.MANIFEST && decision.manifestRoot().equals(entry.identity())
          || decision.transactionId().equals(target.transaction())
              && (decision.revision() >= permit.safeRevision() || decision.sequence() >= permit.safeSequence());
    }, budget, scope).thenCompose(found -> found ? CompletableFuture.completedFuture(true)
        : scanIndexEdges(domain, entry, budget, scope));
  }

  private CompletableFuture<Boolean> scanIndexEdges(Domain domain, SystemRecordEntry entry,
      ProofBudget budget, RequestScope scope) {
    return scanForReference(SystemRecordType.EVENT_REVISION_INDEX, reference -> {
      var index = EventRevisionIndexMessages.decode(reference.value().bytes());
      SystemRecordGcTarget.requireSequenceIdentity(reference.identity(), index.decision().revision());
      return switch (domain) {
        case DECISION -> index.decision().sequence()
            == java.nio.ByteBuffer.wrap(entry.identity().bytes()).getLong();
        case MANIFEST -> index.decision().manifestRoot().equals(entry.identity())
            || index.events().identity().equals(entry.identity());
        default -> false;
      };
    }, budget, scope).thenCompose(found -> found ? CompletableFuture.completedFuture(true)
        : scanManifestEdges(domain, entry, budget, scope));
  }

  private CompletableFuture<Boolean> scanManifestEdges(Domain domain, SystemRecordEntry entry,
      ProofBudget budget, RequestScope scope) {
    return scanForReference(SystemRecordType.MANIFEST,
        reference -> SystemRecordReferences.manifestReferences(reference, domain.recordType(), entry.identity()),
        budget, scope).thenCompose(found -> {
          if (found || domain != Domain.RESULT) return CompletableFuture.completedFuture(found);
          return scanForReference(SystemRecordType.RESULT,
              reference -> SystemRecordReferences.resultReferences(reference, entry.identity()), budget, scope);
        });
  }

  private CompletableFuture<Boolean> scanForReference(SystemRecordType type,
      Predicate<SystemRecordEntry> references, ProofBudget budget, RequestScope scope) {
    return scanReferenceBucket(type, 0, Optional.empty(), references, budget, scope);
  }

  private CompletableFuture<Boolean> scanReferenceBucket(SystemRecordType type, int bucket,
      Optional<ByteKey> after, Predicate<SystemRecordEntry> references, ProofBudget budget,
      RequestScope scope) {
    requireActive(scope);
    if (bucket == bucketCount) return CompletableFuture.completedFuture(false);
    budget.reserveScan();
    return records.scanBucket(type, bucket, after, 1024, MAX_SCAN_BYTES, scope).thenComposeAsync(page -> {
      budget.charge(page);
      for (var entry : page.entries()) {
        // Corrupt references abort the page. They must never become evidence of absence.
        if (references.test(entry)) return CompletableFuture.completedFuture(true);
      }
      return page.exhausted()
          ? scanReferenceBucket(type, bucket + 1, Optional.empty(), references, budget, scope)
          : scanReferenceBucket(type, bucket, page.lastIdentity(), references, budget, scope);
    });
  }

  private static void requireActive(RequestScope scope) {
    if (scope.isClosed() || scope.isDeadlineExceeded()) {
      throw new IllegalStateException("system-record closure scope is no longer active");
    }
  }

  private static final class Progress {
    private final ProofBudget proof = new ProofBudget();
    private int remainingRows;
    private Optional<ByteKey> lastIdentity;
    private int examined;
    private int retired;

    private Progress(int maximumRows, Optional<ByteKey> after) {
      remainingRows = maximumRows;
      lastIdentity = after;
    }

  }

  /** A failed proof has no resumable retirement checkpoint; retry with an appropriate smaller page. */
  private static final class ProofBudget {
    private int scans;
    private long rows;
    private long bytes;

    private void reserveScan() {
      if (++scans > 8192) throw new IllegalStateException("closure proof scan budget exceeded");
    }

    private void charge(SystemRecordPage page) {
      rows = Math.addExact(rows, page.physicalRows());
      bytes = Math.addExact(bytes, page.encodedBytes());
      if (rows > 65536 || bytes > (64L << 20)) {
        throw new IllegalStateException("closure proof record budget exceeded");
      }
    }
  }
}
