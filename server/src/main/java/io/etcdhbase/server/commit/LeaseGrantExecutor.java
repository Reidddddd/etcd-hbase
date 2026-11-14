package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.LeaseReadView;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.PinnedReadView;
import io.etcdhbase.core.commit.ReadViewService;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.lease.LeaseFailure;
import io.etcdhbase.core.lease.LeaseGrantPlanner;
import io.etcdhbase.core.lease.LeaseIdAllocator;
import io.etcdhbase.core.lease.LeaseTimeSource;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Runs a new Grant on the owner's serialized mutation lane and returns its durable receipt. */
public final class LeaseGrantExecutor {
  private static final BudgetManager.Work FINALIZATION_CAPACITY =
      new BudgetManager.Work(0, 1, 0, 1, 0);

  private final String tableGroup;
  private final ByteKey profileFingerprint;
  private final CommitControlStore control;
  private final ReadViewService views;
  private final LeaseReadView leases;
  private final LeaseIdAllocator allocator;
  private final LeaseTimeSource time;
  private final long minimumTtlSeconds;
  private final long maximumTtlSeconds;
  private final Clock ownerClock;
  private final LeaseGrantPreparation preparation;
  private final CommitPublisher publisher;

  public LeaseGrantExecutor(String tableGroup, ByteKey profileFingerprint,
      CommitControlStore control, ReadViewService views, LeaseReadView leases,
      LeaseIdAllocator allocator, LeaseTimeSource time, long minimumTtlSeconds,
      long maximumTtlSeconds, Clock ownerClock, LeaseGrantPreparation preparation,
      CommitPublisher publisher) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.profileFingerprint = Objects.requireNonNull(profileFingerprint, "profileFingerprint");
    this.control = Objects.requireNonNull(control, "control");
    this.views = Objects.requireNonNull(views, "views");
    this.leases = Objects.requireNonNull(leases, "leases");
    this.allocator = Objects.requireNonNull(allocator, "allocator");
    this.time = Objects.requireNonNull(time, "time");
    this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
    this.preparation = Objects.requireNonNull(preparation, "preparation");
    this.publisher = Objects.requireNonNull(publisher, "publisher");
    if (tableGroup.isBlank() || profileFingerprint.length() != 32
        || minimumTtlSeconds < 1 || maximumTtlSeconds < minimumTtlSeconds) {
      throw new IllegalArgumentException("invalid Grant execution configuration");
    }
    this.minimumTtlSeconds = minimumTtlSeconds;
    this.maximumTtlSeconds = maximumTtlSeconds;
  }

  /** Caller owns the serial lane and resolves previously sealed or selected invocations. */
  public CompletableFuture<InvocationId> executeNew(long requestedId, long requestedTtlSeconds,
      InvocationBinding binding, CommitOwnerService.OwnerToken owner,
      AuthorizedBackendIdentity identity, RequestScope scope, BudgetManager.Account account) {
    Objects.requireNonNull(binding, "binding");
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    if (binding.state() != InvocationBinding.State.PREPARING
        || binding.preparedOwnerEpoch() != owner.epoch() || !account.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid new Grant attempt"));
    }
    if (!identity.scope().tableGroupId().equals(tableGroup)
        || !identity.allowedTableGroupIds().contains(tableGroup)) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
    }
    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) {
      return CompletableFuture.failedFuture(new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
    }
    CompletableFuture<InvocationId> operation;
    try {
      PutCommitPreparation.retainUntilDrain(scope, account.tryAcquire(FINALIZATION_CAPACITY)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT)));
      operation = currentBase(binding, owner, scope)
          .thenCompose(ignored -> views.openAuxiliary(binding.authorizationScopeDigest(), scope))
          .thenCompose(pinned -> prepare(requestedId, requestedTtlSeconds, binding, pinned, scope, account))
          .thenCompose(sealed -> currentBase(binding, owner, scope)
              .thenCompose(ignored -> publisher.publish(sealed.invocation(), owner, sealed.manifest(), scope)))
          .thenCompose(outcome -> outcome.state() == CommitOutcomeResolver.State.SELECTED_FINALIZED
              ? CompletableFuture.completedFuture(binding.invocationId())
              : CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN)));
    } catch (BackendFailure | RuntimeException failure) {
      drained.complete(null);
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      drained.complete(null);
      throw fatal;
    }
    operation.whenComplete((ignored, failure) -> drained.complete(null));
    return operation.copy();
  }

  private CompletableFuture<LeaseGrantPreparation.Sealed> prepare(long requestedId, long ttl,
      InvocationBinding binding, PinnedReadView pinned, RequestScope scope, BudgetManager.Account account) {
    if (pinned.view().barrierSequence() != binding.baseSequence()
        || pinned.view().targetRevision() != binding.baseRevision()
        || pinned.view().selectedDecision().isPresent()) {
      return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    var metadataBudget = new ManifestCatalog.ProofReadBudget(scope, account, 1024, 64L << 30);
    return allocator.select(requestedId, id -> leases.find(pinned, id, scope, metadataBudget))
        .thenApply(selection -> LeaseGrantPlanner.grant(selection.id(), ttl, selection.previous(),
            Math.addExact(binding.baseSequence(), 1), time.observe(),
            minimumTtlSeconds, maximumTtlSeconds))
        .exceptionallyCompose(failure -> {
          Throwable cause = CompletionFailures.unwrapCompletion(failure);
          return CompletableFuture.failedFuture(cause instanceof LeaseFailure semantic
              ? new RejectedBeforePreparation(semantic) : cause);
        })
        .thenCompose(state -> preparation.prepare(binding, state, scope, account,
            () -> leases.finish(pinned, scope)));
  }

  /** Marks a domain rejection proven to have occurred before any private Grant preparation. */
  static final class RejectedBeforePreparation extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final LeaseFailure.Reason reason;

    private RejectedBeforePreparation(LeaseFailure failure) {
      super(failure);
      reason = failure.reason();
    }

    LeaseFailure.Reason reason() { return reason; }
  }

  private CompletableFuture<Void> currentBase(InvocationBinding binding,
      CommitOwnerService.OwnerToken owner, RequestScope scope) {
    return control.read(scope).thenAccept(observed -> {
      CommitState state = observed.orElseThrow(() ->
          new IllegalStateException("authoritative commit control state is missing")).state();
      if (!state.profileFingerprint().equals(profileFingerprint)) {
        throw new IllegalStateException("Grant control profile differs from its table group");
      }
      if (state.formatVersion() != CommitState.AUXILIARY_FORMAT_VERSION
          || state.ownerEpoch() != owner.epoch()
          || state.ownerInstance().filter(owner.instance()::equals).isEmpty()
          || state.ownerLeaseExpired(ownerClock.millis()) || state.publication().isPresent()
          || state.publishedSequence() != binding.baseSequence()
          || state.publishedRevision() != binding.baseRevision()) {
        throw new CompletionException(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
    });
  }
}
