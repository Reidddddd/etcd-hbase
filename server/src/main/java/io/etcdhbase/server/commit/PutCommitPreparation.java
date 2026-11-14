package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.PreparedAuxiliaryRecord;
import io.etcdhbase.core.commit.ResultCatalog;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.ResultPage;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.PutCommand;
import io.etcdhbase.core.kv.PutPlanner;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.core.storage.StoredRecord;
import io.etcdhbase.hbase.codec.ResultMessages;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Prepares one Put and seals its durable response before it can enter the publication path. */
public final class PutCommitPreparation {
  private static final long FIXED_CANDIDATE_BYTES = 1L << 20;
  private static final long COPY_ALLOWANCE = 16;
  private static final long RESULT_IO_BYTES = 8L * ResultPage.MAX_PAYLOAD_BYTES;

  public record Sealed(InvocationBinding invocation, ManifestRoot manifest) {
    public Sealed {
      Objects.requireNonNull(invocation, "invocation");
      Objects.requireNonNull(manifest, "manifest");
      if (invocation.state() != InvocationBinding.State.SEALED
          || invocation.manifestRoot().filter(manifest.systemIdentity()::equals).isEmpty()) {
        throw new IllegalArgumentException("sealed invocation and manifest differ");
      }
    }
  }

  private final PutPlanner planner = new PutPlanner();
  private final ResultPreparation results;
  private final ResultCatalog resultCatalog;
  private final ManifestPreparation manifests;
  private final InvocationService invocations;

  public PutCommitPreparation(ResultPreparation results, ResultCatalog resultCatalog,
      ManifestPreparation manifests, InvocationService invocations) {
    this.results = Objects.requireNonNull(results, "results");
    this.resultCatalog = Objects.requireNonNull(resultCatalog, "resultCatalog");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.invocations = Objects.requireNonNull(invocations, "invocations");
  }

  /**
   * The caller supplies a full record from the invocation's pinned base and retains its point-read
   * lease until this future settles. The owner must close the service scope after the operation;
   * preparation leases remain charged until that scope's physical producers actually drain.
   */
  public CompletableFuture<Sealed> prepare(PutCommand command, InvocationBinding invocation,
      Optional<StoredRecord> base, RequestScope scope, BudgetManager.Account account,
      Supplier<CompletableFuture<Void>> finishPinnedRead) {
    final PutPlanner.Plan plan;
    try {
      plan = planner.plan(command, invocation, base);
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    return prepare(plan, command, invocation, base, List.of(), scope, account, finishPinnedRead);
  }

  /** Seals a planned Put and any Lease binding updates under one manifest and one M2 decision. */
  public CompletableFuture<Sealed> prepare(PutPlanner.Plan plan, PutCommand command,
      InvocationBinding invocation, Optional<StoredRecord> base,
      List<PreparedAuxiliaryRecord> auxiliaryRecords, RequestScope scope,
      BudgetManager.Account account, Supplier<CompletableFuture<Void>> finishPinnedRead) {
    Objects.requireNonNull(plan, "plan");
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(base, "base");
    Objects.requireNonNull(auxiliaryRecords, "auxiliaryRecords");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(finishPinnedRead, "finishPinnedRead");
    if (invocation.state() != InvocationBinding.State.PREPARING || !account.belongsTo(scope)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "Put preparation requires its preparing invocation and service budget"));
    }
    CompletableFuture<Void> preparationDrained = new CompletableFuture<>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(preparationDrained))) {
      return CompletableFuture.failedFuture(inactive(scope));
    }
    CompletableFuture<Sealed> operation;
    try {
      retainUntilDrain(scope, requireMemory(account, BudgetManager.Memory.CANDIDATE,
          candidateBudget(command, base)));
      retainUntilDrain(scope, requireMemory(account, BudgetManager.Memory.IO, RESULT_IO_BYTES));
      validatePlan(plan, invocation, command);
      byte[] encoded = ResultMessages.encode(plan.result());
      ResultIdentity identity = ResultIdentity.from(invocation);
      List<ResultPage> pages = ResultPreparation.split(identity, ByteBuffer.wrap(encoded));
      operation = results.prepare(identity, pages, scope).thenCompose(reference ->
          auxiliaryRecords.isEmpty()
              ? manifests.prepareWithResult(invocation, List.of(plan.candidate()).iterator(),
                  List.of(plan.event()).iterator(), reference, resultCatalog, finishPinnedRead, scope)
              : manifests.prepareWithResultAndAuxiliary(invocation,
                  List.of(plan.candidate()).iterator(), List.of(plan.event()).iterator(),
                  reference, resultCatalog, auxiliaryRecords.iterator(), finishPinnedRead, scope))
          .thenCompose(manifest -> invocations.transition(invocation,
              invocation.seal(manifest.systemIdentity()), scope)
              .thenApply(sealed -> new Sealed(sealed, manifest)));
    } catch (BackendFailure | RuntimeException failure) {
      preparationDrained.complete(null);
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      preparationDrained.complete(null);
      throw fatal;
    }
    // This dependent is never exposed to callers, so cancelling a returned copy cannot suppress it.
    operation.whenComplete((ignored, failure) -> preparationDrained.complete(null));
    return operation.copy();
  }

  private static void validatePlan(PutPlanner.Plan plan, InvocationBinding invocation,
      PutCommand command) {
    var candidate = plan.candidate();
    var event = plan.event();
    if (!candidate.transactionId().equals(invocation.transactionId())
        || candidate.ownerEpoch() != invocation.preparedOwnerEpoch()
        || candidate.baseSequence() != invocation.baseSequence()
        || candidate.baseRevision() != invocation.baseRevision()
        || !event.transactionId().equals(invocation.transactionId())
        || event.ownerEpoch() != invocation.preparedOwnerEpoch()
        || event.baseSequence() != invocation.baseSequence()
        || event.baseRevision() != invocation.baseRevision()
        || !candidate.record().key().equals(command.key())
        || !event.current().key().equals(command.key()) || event.ordinal() != 0) {
      throw new IllegalArgumentException("planned Put differs from its invocation or request");
    }
  }

  private static long candidateBudget(PutCommand command, Optional<StoredRecord> base) {
    long bytes = Math.addExact(command.key().length(), command.valueLength());
    if (base.isPresent()) bytes = Math.addExact(bytes, base.orElseThrow().retainedBytes());
    return Math.addExact(FIXED_CANDIDATE_BYTES, Math.multiplyExact(COPY_ALLOWANCE, bytes));
  }

  private static BudgetManager.Lease requireMemory(BudgetManager.Account account,
      BudgetManager.Memory lane, long bytes) throws BackendFailure {
    return account.tryRetain(lane, bytes)
        .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
  }

  static void retainUntilDrain(RequestScope scope, BudgetManager.Lease lease) {
    CompletableFuture<Void> released = new CompletableFuture<>();
    try {
      scope.registerScopedCleanup(() -> scope.physicalProducersCompletion()
          .whenComplete((ignored, failure) -> {
            try {
              lease.close();
              released.complete(null);
            } catch (RuntimeException | Error cleanup) {
              released.completeExceptionally(cleanup);
            }
          }), released);
    } catch (RuntimeException | Error failure) {
      try {
        lease.close();
      } catch (RuntimeException | Error cleanup) {
        if (cleanup != failure) failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }
}
