package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.AsyncCursor;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.PreparedAuxiliaryRecord;
import io.etcdhbase.core.commit.ResultCatalog;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.ResultPage;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.ResultNode;
import io.etcdhbase.core.kv.ResponseBudget;
import io.etcdhbase.core.kv.TxnEvaluator;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.codec.ResultMessages;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Persists the original transaction result before sealing it or completing it unchanged. */
public final class TxnCommitPreparation {
  public sealed interface Prepared permits Sealed, NoChanges {}

  public record Sealed(InvocationBinding invocation, ManifestRoot manifest) implements Prepared {
    public Sealed {
      Objects.requireNonNull(invocation, "invocation");
      Objects.requireNonNull(manifest, "manifest");
      if (invocation.state() != InvocationBinding.State.SEALED
          || invocation.manifestRoot().filter(manifest.systemIdentity()::equals).isEmpty()) {
        throw new IllegalArgumentException("sealed transaction and manifest differ");
      }
    }
  }

  public record NoChanges(InvocationBinding invocation) implements Prepared {
    public NoChanges {
      Objects.requireNonNull(invocation, "invocation");
      if (invocation.state() != InvocationBinding.State.NO_CHANGES) {
        throw new IllegalArgumentException("invocation has no durable no-change result");
      }
    }
  }

  private final ResultPreparation results;
  private final ResultCatalog catalog;
  private final ManifestPreparation manifests;
  private final InvocationService invocations;
  private final long maximumResponseBytes;

  public TxnCommitPreparation(ResultPreparation results, ResultCatalog catalog,
      ManifestPreparation manifests, InvocationService invocations, long maximumResponseBytes) {
    this.results = Objects.requireNonNull(results, "results");
    this.catalog = Objects.requireNonNull(catalog, "catalog");
    this.manifests = Objects.requireNonNull(manifests, "manifests");
    this.invocations = Objects.requireNonNull(invocations, "invocations");
    if (maximumResponseBytes < ResponseBudget.FIXED_BYTES) {
      throw new IllegalArgumentException("transaction response budget is too small");
    }
    this.maximumResponseBytes = maximumResponseBytes;
  }

  /** The caller owns the scope until this preparation and any publication producer have drained. */
  public CompletableFuture<Prepared> prepare(TxnEvaluator.Evaluation evaluation,
      InvocationBinding invocation, RequestScope scope, BudgetManager.Account account) {
    return prepare(evaluation, invocation, scope, account, List.of());
  }

  /** Includes Lease state and binding deltas in the same prepared M2 publication as KV results. */
  public CompletableFuture<Prepared> prepare(TxnEvaluator.Evaluation evaluation,
      InvocationBinding invocation, RequestScope scope, BudgetManager.Account account,
      List<PreparedAuxiliaryRecord> auxiliaryRecords) {
    Objects.requireNonNull(evaluation, "evaluation");
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(account, "account");
    Objects.requireNonNull(auxiliaryRecords, "auxiliaryRecords");
    boolean changed = evaluation.candidateCount() > 0;
    if (invocation.state() != InvocationBinding.State.PREPARING || !account.belongsTo(scope)
        || !evaluation.belongsTo(invocation, scope)
        || evaluation.eventCount() < evaluation.candidateCount()
        || !changed && (evaluation.eventCount() != 0 || !auxiliaryRecords.isEmpty())
        || !(evaluation.result().root() instanceof ResultNode.Txn txn)
        || !txn.header().present() || txn.header().revision() != invocation.baseRevision() + (changed ? 1 : 0)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("transaction result and invocation differ"));
    }
    var drained = new CompletableFuture<Void>();
    if (!scope.startIfOpen(() -> scope.trackPhysicalProducer(drained))) {
      return CompletableFuture.failedFuture(inactive(scope));
    }
    CompletableFuture<Prepared> operation;
    try {
      long ioBytes = Math.addExact(8L * ResultPage.MAX_PAYLOAD_BYTES,
          Math.multiplyExact(8, evaluation.result().retainedBytes()));
      var lease = account.tryRetain(BudgetManager.Memory.IO, ioBytes)
          .orElseThrow(() -> new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      PutCommitPreparation.retainUntilDrain(scope, lease);
      byte[] encoded = ResultMessages.encode(evaluation.result());
      if (ResponseBudget.forEncodedResult(encoded.length) > maximumResponseBytes) {
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
      ResultIdentity identity = ResultIdentity.from(invocation);
      List<ResultPage> pages = ResultPreparation.split(identity, ByteBuffer.wrap(encoded));
      operation = results.prepare(identity, pages, scope).thenCompose(reference -> {
        if (!changed) {
          return invocations.completeWithoutChanges(invocation, reference, catalog,
              evaluation::finishRead, scope).thenApply(NoChanges::new);
        }
        return (auxiliaryRecords.isEmpty()
            ? manifests.prepareWithResult(invocation, evaluation.candidates(),
                evaluation.events(), reference, catalog, evaluation::finishRead, scope)
            : manifests.prepareWithResultAndAuxiliary(invocation, evaluation.candidates(),
                evaluation.events(), reference, catalog,
                AsyncCursor.fromIterator(auxiliaryRecords.iterator()),
                evaluation::finishRead, scope))
            .thenCompose(manifest -> invocations.transition(invocation,
                invocation.seal(manifest.systemIdentity()), scope)
                .thenApply(sealed -> new Sealed(sealed, manifest)));
      });
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

  private static BackendFailure inactive(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }
}
