package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.MutationOperationEnvelope;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.server.observability.RequestMetrics;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/** Serializes forwarded mutations before the receiver captures and binds their logical base. */
public final class MutationOwnerLane {
  private static final long ENVELOPE_ALLOWANCE = 1024;

  private final String tableGroup;
  private final SerialMutationQueue queue;
  private final MutationForwardReceiver receiver;
  private final LongSupplier nanoTime;
  private final RequestMetrics metrics;

  public MutationOwnerLane(String tableGroup, SerialMutationQueue queue,
      MutationForwardReceiver receiver) {
    this(tableGroup, queue, receiver, System::nanoTime, new RequestMetrics());
  }

  public MutationOwnerLane(String tableGroup, SerialMutationQueue queue,
      MutationForwardReceiver receiver, RequestMetrics metrics) {
    this(tableGroup, queue, receiver, System::nanoTime, metrics);
  }

  MutationOwnerLane(String tableGroup, SerialMutationQueue queue,
      MutationForwardReceiver receiver, LongSupplier nanoTime) {
    this(tableGroup, queue, receiver, nanoTime, new RequestMetrics());
  }

  private MutationOwnerLane(String tableGroup, SerialMutationQueue queue,
      MutationForwardReceiver receiver, LongSupplier nanoTime, RequestMetrics metrics) {
    this.tableGroup = Objects.requireNonNull(tableGroup, "tableGroup");
    this.queue = Objects.requireNonNull(queue, "queue");
    this.receiver = Objects.requireNonNull(receiver, "receiver");
    this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    if (tableGroup.isBlank()) throw new IllegalArgumentException("table group is required");
  }

  /**
   * A completed reply contains only the original invocation ID; the caller resolves its durable
   * result separately under its own response budget. Queue lifecycle remains with the runtime.
   */
  public CompletableFuture<OwnerForwarding.Reply> receive(String peerIdentity,
      MutationForwardRequest request, RequestScope scope) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(scope, "scope");
    if (!tableGroup.equals(request.tableGroupId())) {
      return CompletableFuture.completedFuture(new OwnerForwarding.Unknown(BackendFailure.Category.PERMISSION));
    }
    final MutationOperationEnvelope.Kind operationKind;
    try { operationKind = MutationOperationEnvelope.kind(request.operationBytes()); }
    catch (IllegalArgumentException malformed) {
      return CompletableFuture.completedFuture(new OwnerForwarding.Unknown(BackendFailure.Category.UNKNOWN));
    }
    long entered = nanoTime.getAsLong();
    long bytes = ENVELOPE_ALLOWANCE + 8L * request.operationBytes().length()
        + 2L * request.authorizationContextReference().length() + 2L * tableGroup.length();
    try {
      return queue.submit(bytes, scope, serviceScope -> {
        long elapsed = nanoTime.getAsLong() - entered;
        if (operationKind == MutationOperationEnvelope.Kind.PUT) {
          metrics.recordPutStage(RequestMetrics.PutStage.QUEUE, Math.max(0, elapsed));
        } else if (operationKind != MutationOperationEnvelope.Kind.LEASE_GRANT
            && operationKind != MutationOperationEnvelope.Kind.LEASE_EXPIRE_EMPTY) {
          metrics.recordTxnStage(RequestMetrics.TxnStage.QUEUE, Math.max(0, elapsed));
        }
        long remaining = elapsed < 0 || elapsed >= request.remainingTimeoutNanos()
            ? 0 : Math.min(request.remainingTimeoutNanos() - elapsed, serviceScope.remainingDeadlineNanos());
        if (remaining <= 0) {
          return CompletableFuture.completedFuture(new OwnerForwarding.Unknown(BackendFailure.Category.TIMEOUT));
        }
        MutationForwardRequest delivered = new MutationForwardRequest(request.tableGroupId(),
            request.invocationId(), request.requestDigest(), request.authorizationScopeDigest(),
            request.authorizationContextReference(), request.expectedOwnerEpoch(), remaining,
            request.hopCount(), request.operationBytes());
        return receiver.receive(peerIdentity, delivered, serviceScope)
            .thenApply(reply -> validateReceipt(request, reply));
      }).handle((reply, failure) -> failure == null ? reply : new OwnerForwarding.Unknown(category(failure)));
    } catch (RuntimeException failure) {
      return CompletableFuture.completedFuture(new OwnerForwarding.Unknown(category(failure)));
    }
  }

  private static OwnerForwarding.Reply validateReceipt(
      MutationForwardRequest request, OwnerForwarding.Reply reply) {
    if (reply instanceof OwnerForwarding.Completed completed
        && (completed.operationResultSize() != 16
            || !Arrays.equals(completed.operationResult(), request.invocationId().bytes()))) {
      return new OwnerForwarding.Unknown(BackendFailure.Category.UNKNOWN);
    }
    return Objects.requireNonNull(reply, "owner returned no reply");
  }

  private static BackendFailure.Category category(Throwable failure) {
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    if (cause instanceof BackendFailure backend) return backend.category();
    if (cause instanceof CancellationException) return BackendFailure.Category.CANCELLED;
    return BackendFailure.Category.UNKNOWN;
  }
}
