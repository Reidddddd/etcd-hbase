package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.PreparedAuxiliaryRecord;
import io.etcdhbase.core.commit.PreparedCandidate;
import io.etcdhbase.core.commit.PreparedEvent;
import io.etcdhbase.core.commit.ResultCatalog;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.ResultPage;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Seals one durable RPC result together with its auxiliary-only M2 publication plan. */
public final class LeaseAuxiliaryMutationPreparation {
  private static final long CANDIDATE_BYTES = 1L << 20;
  private static final long RESULT_IO_BYTES = 8L * ResultPage.MAX_PAYLOAD_BYTES;

  public record Sealed(InvocationBinding invocation, ManifestRoot manifest) {
    public Sealed {
      Objects.requireNonNull(invocation, "invocation");
      Objects.requireNonNull(manifest, "manifest");
      if (invocation.state() != InvocationBinding.State.SEALED
          || invocation.manifestRoot().filter(manifest.systemIdentity()::equals).isEmpty()) {
        throw new IllegalArgumentException("sealed Lease invocation and manifest differ");
      }
    }
  }

  private final ResultPreparation results;
  private final ResultCatalog resultCatalog;
  private final ManifestPreparation manifests;
  private final InvocationService invocations;

  public LeaseAuxiliaryMutationPreparation(ResultPreparation results, ResultCatalog resultCatalog,
      ManifestPreparation manifests, InvocationService invocations) {
    this.results = Objects.requireNonNull(results, "results");
    this.resultCatalog = Objects.requireNonNull(resultCatalog, "resultCatalog");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.invocations = Objects.requireNonNull(invocations, "invocations");
  }

  /**
   * Keeps the result, auxiliary pages, read protection, and invocation seal under the caller's
   * request budget until every physical writer has drained.
   */
  public CompletableFuture<Sealed> prepare(InvocationBinding invocation, ByteKey resultPayload,
      Iterator<PreparedAuxiliaryRecord> auxiliaryRecords, Supplier<CompletableFuture<Void>> finishRead,
      RequestScope scope, BudgetManager.Account account) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(resultPayload, "resultPayload");
    Objects.requireNonNull(auxiliaryRecords, "auxiliaryRecords");
    Objects.requireNonNull(finishRead, "finishRead");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    if (invocation.state() != InvocationBinding.State.PREPARING || !account.belongsTo(scope)
        || resultPayload.length() == 0 || resultPayload.length() > ResultPage.MAX_PAYLOAD_BYTES
        || invocation.baseSequence() == Long.MAX_VALUE) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("invalid Lease auxiliary mutation preparation"));
    }

    CompletableFuture<Void> drained = new CompletableFuture<>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) {
      return CompletableFuture.failedFuture(new BackendFailure(scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
    }
    CompletableFuture<Sealed> operation;
    try {
      retain(scope, account, BudgetManager.Memory.CANDIDATE, CANDIDATE_BYTES);
      retain(scope, account, BudgetManager.Memory.IO, RESULT_IO_BYTES);
      ResultIdentity identity = ResultIdentity.from(invocation);
      List<ResultPage> pages = ResultPreparation.split(identity, ByteBuffer.wrap(resultPayload.bytes()));
      operation = results.prepare(identity, pages, scope)
          .thenCompose(reference -> manifests.prepareWithResultAndAuxiliary(invocation,
              List.<PreparedCandidate>of().iterator(), List.<PreparedEvent>of().iterator(),
              reference, resultCatalog, auxiliaryRecords, finishRead, scope))
          .thenCompose(manifest -> invocations.transition(invocation,
              invocation.seal(manifest.systemIdentity()), scope)
              .thenApply(sealed -> new Sealed(sealed, manifest)));
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

  private static void retain(RequestScope scope, BudgetManager.Account account,
      BudgetManager.Memory lane, long bytes) throws BackendFailure {
    PutCommitPreparation.retainUntilDrain(scope, account.tryRetain(lane, bytes)
        .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT)));
  }
}
