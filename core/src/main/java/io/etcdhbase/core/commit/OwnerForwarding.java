package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.lease.LeaseFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Transport-neutral boundary; the concrete channel must enforce trusted discovery and peer auth. */
public interface OwnerForwarding {
  int MAX_OPERATION_RESULT_BYTES = 4 * 1024 * 1024;

  sealed interface Reply permits Completed, Rejected, SemanticRejected, LeaseRejected, Unknown {}

  record Route(String ownerInstance, long ownerEpoch, String endpointId, String peerIdentity) {
    public Route {
      Objects.requireNonNull(ownerInstance, "ownerInstance");
      Objects.requireNonNull(endpointId, "endpointId");
      Objects.requireNonNull(peerIdentity, "peerIdentity");
      if (ownerInstance.isBlank() || endpointId.isBlank() || peerIdentity.isBlank() || ownerEpoch <= 0) {
        throw new IllegalArgumentException("invalid trusted owner route");
      }
    }
  }

  record Completed(byte[] operationResult) implements Reply {
    public Completed {
      Objects.requireNonNull(operationResult, "operationResult");
      if (operationResult.length > MAX_OPERATION_RESULT_BYTES) {
        throw new IllegalArgumentException("owner forwarding result exceeds its byte budget");
      }
      operationResult = operationResult.clone();
    }

    @Override public byte[] operationResult() { return operationResult.clone(); }
    public int operationResultSize() { return operationResult.length; }
  }

  /** A known rejection requires proof that no mutation was dispatched for the invocation. */
  record Rejected(BackendFailure.Category category) implements Reply {
    public Rejected {
      Objects.requireNonNull(category, "category");
    }
  }

  /** A semantic rejection is valid only when the operation is known not to have published. */
  record SemanticRejected(KvFailure.Reason reason) implements Reply {
    public SemanticRejected {
      Objects.requireNonNull(reason, "reason");
    }
  }

  /** A Lease rejection is returned only before any publication was attempted. */
  record LeaseRejected(LeaseFailure.Reason reason) implements Reply {
    public LeaseRejected {
      Objects.requireNonNull(reason, "reason");
    }
  }

  /** The receiver could not resolve the original invocation; callers must not retry under a new ID. */
  record Unknown(BackendFailure.Category category) implements Reply {
    public Unknown {
      Objects.requireNonNull(category, "category");
    }
  }

  interface Directory {
    /** Resolves only an owner identity obtained from the authoritative control state. */
    CompletableFuture<Route> resolve(String tableGroupId, String ownerInstance, long ownerEpoch);
  }

  interface AuthenticatedChannel {
    /**
     * Sends one attempt without automatic retries. Implementations authenticate the route peer
     * and table-group audience before dispatch; an indeterminate transport failure is surfaced
     * so the caller can resolve the durable invocation instead of submitting it under a new ID.
     * The send initiation must return promptly; its invocation is the request-cancellation
     * ownership handoff.
     */
    CompletableFuture<Reply> sendOnce(Route route, MutationForwardRequest request, RequestScope scope);
  }
}
