package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.ManifestRecordFormat;
import io.etcdhbase.core.commit.ResultDirectory;
import io.etcdhbase.core.commit.SystemRecordClosure.Domain;
import io.etcdhbase.core.commit.SystemRecordClosure.Page;
import io.etcdhbase.core.commit.SystemRecordEntry;
import io.etcdhbase.core.commit.SystemRecordPage;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.CommitDecisionMessages;
import io.etcdhbase.hbase.codec.EventRevisionIndexMessages;
import io.etcdhbase.hbase.codec.InvocationMessages;
import io.etcdhbase.hbase.codec.ResultMessages;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Predicate;

/** Enumerates bounded retirement pages and proves the complete surviving direct reference set. */
public final class HBaseSystemRecordClosure implements io.etcdhbase.core.commit.SystemRecordClosure {
  private static final long MAX_SCAN_BYTES = 8L << 20;
  private final SystemRecordStore records;
  private final HBaseSystemRecordStore retirement;
  private final SystemRecordGcAuthority authority;
  private final InvocationWindowCloser windows;
  private final ByteKey profile;
  private final int bucketCount;

  public HBaseSystemRecordClosure(HBaseSystemRecordStore store) {
    this(store, ResultWindowRetention.unconfigured());
  }

  public HBaseSystemRecordClosure(HBaseSystemRecordStore store, ResultWindowRetention resultRetention) {
    this(store, store, store.profileFingerprint(), store.bucketCount(), new SystemRecordGcAuthority(store),
        resultRetention);
    store.requireGcFencing();
  }

  HBaseSystemRecordClosure(SystemRecordStore records, HBaseSystemRecordStore retirement,
      ByteKey profile, int bucketCount, SystemRecordGcAuthority authority, ResultWindowRetention resultRetention) {
    this.authority = Objects.requireNonNull(authority, "authority");
    this.records = Objects.requireNonNull(records, "records");
    this.retirement = Objects.requireNonNull(retirement, "retirement");
    windows = new InvocationWindowCloser(records, retirement, authority, resultRetention);
    this.profile = Objects.requireNonNull(profile, "profile");
    if (profile.length() != 32 || bucketCount < 1 || bucketCount > 65_536) {
      throw new IllegalArgumentException("invalid closure configuration");
    }
    this.bucketCount = bucketCount;
  }

  @Override
  public CompletableFuture<Page> fence(Domain domain, int bucket, Optional<ByteKey> afterIdentity,
      int maximumRecords, SystemRecordRetirement.Permit permit, RequestScope scope) {
    validatePage(domain, bucket, afterIdentity, maximumRecords, permit, scope);
    return authority.validate(permit, scope).thenCompose(ignored -> records.scanBucket(domain.recordType(), bucket,
        afterIdentity, maximumRecords, MAX_SCAN_BYTES, scope)).thenCompose(page -> {
          CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
          for (var entry : page.entries()) {
            chain = chain.thenCompose(ignored -> retirement.fenceForGc(domain.recordType(), entry.identity(), permit, scope));
          }
          return chain.thenApply(ignored -> new Page(domain, page.entries().size(), 0,
              page.lastIdentity(), page.exhausted(), 0));
        });
  }

  /** Collects a bounded domain page, retaining records while any reference or protection holds. */
  public CompletableFuture<Page> collect(Domain domain, int bucket, Optional<ByteKey> afterIdentity,
      int maximumRecords, SystemRecordRetirement.Permit permit, RequestScope scope) {
    validatePage(domain, bucket, afterIdentity, maximumRecords, permit, scope);
    var progress = new Progress(maximumRecords, afterIdentity);
    return authority.validate(permit, scope).thenCompose(ignored ->
        collectBucket(domain, bucket, afterIdentity, progress, permit, scope))
        .handle((exhausted, failure) -> {
          boolean complete = failure == null && exhausted;
          if (failure != null) {
            Throwable cause = CompletionFailures.unwrapCompletion(failure);
            if (!(cause instanceof ProofLimitExceeded) || progress.examined == 0) {
              throw new CompletionException(cause);
            }
            // Only completed entries form a resume prefix. The unproved entry is retried.
            complete = false;
          }
          return new Page(domain, progress.examined, progress.retired,
              progress.lastIdentity, complete, progress.pending);
        });
  }

  private void validatePage(Domain domain, int bucket, Optional<ByteKey> afterIdentity,
      int maximumRecords, SystemRecordRetirement.Permit permit, RequestScope scope) {
    Objects.requireNonNull(domain, "domain");
    Objects.requireNonNull(afterIdentity, "afterIdentity");
    Objects.requireNonNull(permit, "permit");
    Objects.requireNonNull(scope, "scope");
    if (maximumRecords < 1 || maximumRecords > 1024 || bucket < 0 || bucket >= bucketCount
        || afterIdentity.filter(identity -> records.bucket(domain.recordType(), identity) != bucket).isPresent()) {
      throw new IllegalArgumentException("invalid closure page or shard");
    }
    permit.requireUsable(profile, scope);
  }

  private CompletableFuture<Boolean> collectBucket(Domain domain, int bucket, Optional<ByteKey> after,
      Progress progress, SystemRecordRetirement.Permit permit, RequestScope scope) {
    requireActive(scope);
    if (progress.remainingRows == 0) return CompletableFuture.completedFuture(false);
    return records.scanBucket(domain.recordType(), bucket, after, progress.remainingRows, MAX_SCAN_BYTES, scope)
        .thenComposeAsync(page -> collectEntries(domain, page, progress, permit, scope).thenCompose(ignored -> {
          progress.remainingRows = Math.max(0, progress.remainingRows - Math.toIntExact(page.physicalRows()));
          page.lastIdentity().ifPresent(identity -> progress.lastIdentity = Optional.of(identity));
          return page.exhausted() ? CompletableFuture.completedFuture(true)
              : collectBucket(domain, bucket, page.lastIdentity(), progress, permit, scope);
        }));
  }

  private CompletableFuture<Void> collectEntries(Domain domain, SystemRecordPage page,
      Progress progress, SystemRecordRetirement.Permit permit, RequestScope scope) {
    CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
    for (var entry : page.entries()) {
      chain = chain.thenCompose(ignored -> {
        requireActive(scope);
        var collected = domain == Domain.INVOCATION
            ? closeWindow(entry, progress, permit, scope)
            : collectRecord(domain, entry, progress, permit, scope);
        return collected.thenRun(() -> {
          progress.examined++;
          progress.lastIdentity = Optional.of(entry.identity());
        });
      });
    }
    return chain;
  }

  private CompletableFuture<Void> closeWindow(SystemRecordEntry entry, Progress progress,
      SystemRecordRetirement.Permit permit, RequestScope scope) {
    var binding = InvocationMessages.decode(entry.value().bytes());
    SystemRecordReferences.requireIdentity(entry.identity(), ByteKey.copyOf(binding.invocationId().bytes()));
    if (binding.state() == InvocationBinding.State.WINDOW_CLOSED
        || binding.baseRevision() >= permit.safeRevision() - 1
        || binding.baseSequence() >= permit.safeSequence() - 1) {
      return CompletableFuture.completedFuture(null);
    }
    return windows.close(entry.identity(), permit, scope)
        .thenCompose(ignored -> records.read(SystemRecordType.INVOCATION, entry.identity(), scope))
        .thenAccept(found -> {
          var current = InvocationMessages.decode(found.orElseThrow(
              () -> new IllegalStateException("invocation non-replay binding is missing")).bytes());
          if (!current.sameAttempt(binding)) throw new IllegalStateException("invocation attempt changed during GC");
          if (current.state() != InvocationBinding.State.WINDOW_CLOSED) progress.pending++;
        });
  }

  private CompletableFuture<Void> collectRecord(Domain domain, SystemRecordEntry entry, Progress progress,
      SystemRecordRetirement.Permit permit, RequestScope scope) {
    return targetForCollection(domain, entry, progress.proof, scope).thenCompose(target -> {
      if (target.revision() >= permit.safeRevision() || target.sequence() >= permit.safeSequence()) {
        return CompletableFuture.completedFuture(null);
      }
      return isReferenced(domain, entry, target, permit, progress.proof, scope).thenCompose(referenced -> {
        if (referenced) {
          progress.pending++;
          return CompletableFuture.completedFuture(null);
        }
        return retirement.retire(domain.recordType(), entry.identity(), entry.value(), permit,
            SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, scope).thenAccept(outcome -> {
              if (outcome.retiredNow() || outcome.alreadyRetired()) progress.retired++;
              else progress.pending++;
            });
      });
    });
  }

  private CompletableFuture<SystemRecordGcTarget> targetForCollection(Domain domain, SystemRecordEntry entry,
      ProofBudget budget, RequestScope scope) {
    var target = SystemRecordGcTarget.decode(domain.recordType(), entry);
    if (target.revision() != 0 || target.sequence() != 0) return CompletableFuture.completedFuture(target);
    // Child pages have no counters. Their current immutable root or permanent invocation supplies
    // the window; an unrelated, still-readable transaction must not block old-target completion.
    var root = ByteKey.copyOf(ByteBuffer.allocate(17).put(target.transaction().bytes())
        .put(ManifestRecordFormat.MANIFEST_ROOT).array());
    return records.read(SystemRecordType.MANIFEST, root, scope).thenCompose(found -> found.isPresent()
        ? CompletableFuture.completedFuture(SystemRecordGcTarget.decode(SystemRecordType.MANIFEST,
            new SystemRecordEntry(root, found.orElseThrow())))
        : invocationTarget(target.transaction(), 0, Optional.empty(), budget, scope)
            .thenApply(bounds -> bounds.orElse(target)));
  }

  private CompletableFuture<Optional<SystemRecordGcTarget>> invocationTarget(TransactionId transaction,
      int bucket, Optional<ByteKey> after, ProofBudget budget, RequestScope scope) {
    requireActive(scope);
    if (bucket == bucketCount) return CompletableFuture.completedFuture(Optional.empty());
    budget.reserveScan();
    return records.scanBucket(SystemRecordType.INVOCATION, bucket, after, 1024, MAX_SCAN_BYTES, scope)
        .thenComposeAsync(page -> {
          budget.charge(page);
          for (var entry : page.entries()) {
            var binding = InvocationMessages.decode(entry.value().bytes());
            SystemRecordReferences.requireIdentity(entry.identity(), ByteKey.copyOf(binding.invocationId().bytes()));
            if (binding.transactionId().equals(transaction)) {
              return CompletableFuture.completedFuture(Optional.of(SystemRecordGcTarget.prepared(
                  transaction, binding.baseRevision(), binding.baseSequence())));
            }
          }
          return page.exhausted() ? invocationTarget(transaction, bucket + 1, Optional.empty(), budget, scope)
              : invocationTarget(transaction, bucket, page.lastIdentity(), budget, scope);
        });
  }

  private CompletableFuture<Boolean> isReferenced(Domain domain, SystemRecordEntry entry,
      SystemRecordGcTarget target, SystemRecordRetirement.Permit permit, ProofBudget budget, RequestScope scope) {
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
    if (domain != Domain.DECISION && domain != Domain.MANIFEST) {
      return scanManifestEdges(domain, entry, budget, scope);
    }
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
    if (domain == Domain.DECISION || domain == Domain.EVENT_INDEX) {
      return CompletableFuture.completedFuture(false);
    }
    return scanForReference(SystemRecordType.MANIFEST,
        reference -> SystemRecordReferences.manifestReferences(reference, domain.recordType(), entry.identity()),
        budget, scope).thenCompose(found -> {
          boolean resultPage = domain == Domain.RESULT
              && entry.identity().bytes()[16] == ManifestRecordFormat.RESULT_PAGE;
          if (found || !resultPage) return CompletableFuture.completedFuture(found);
          return resultDirectoryReferences(entry, scope);
        });
  }

  private CompletableFuture<Boolean> resultDirectoryReferences(SystemRecordEntry entry, RequestScope scope) {
    var page = ResultMessages.decodePage(entry.value().bytes());
    var identity = ResultDirectory.systemIdentity(page.identity());
    // Result directories can only name ordered pages of their own exact preparation identity.
    return records.read(SystemRecordType.RESULT, identity, scope).thenApply(found -> {
      if (found.isEmpty()) return false;
      var directory = ResultMessages.decodeDirectory(found.orElseThrow().bytes());
      SystemRecordReferences.requireIdentity(identity, directory.systemIdentity());
      if (!directory.identity().equals(page.identity())) {
        throw new IllegalArgumentException("result directory differs from its page preparation");
      }
      return page.index() < directory.pages().size();
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
    private int pending;

    private Progress(int maximumRows, Optional<ByteKey> after) {
      remainingRows = maximumRows;
      lastIdentity = after;
    }

  }

  /** A page may retain its completed prefix; a single record still needs a complete bounded proof. */
  private static final class ProofBudget {
    private int scans;
    private long rows;
    private long bytes;

    private void reserveScan() {
      if (++scans > 8192) throw new ProofLimitExceeded("closure proof scan budget exceeded");
    }

    private void charge(SystemRecordPage page) {
      rows = Math.addExact(rows, page.physicalRows());
      bytes = Math.addExact(bytes, page.encodedBytes());
      if (rows > 65536 || bytes > (64L << 20)) {
        throw new ProofLimitExceeded("closure proof record budget exceeded");
      }
    }
  }

  private static final class ProofLimitExceeded extends IllegalStateException {
    private static final long serialVersionUID = 1L;
    private ProofLimitExceeded(String message) { super(message); }
  }
}
