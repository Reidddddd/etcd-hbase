package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.concurrent.CompletableFuture;

/** Preflights and idempotently applies records selected by an M2 publication decision. */
public interface AuxiliaryMaterializationStore {
  enum ApplyResult { APPLIED, ALREADY_APPLIED, SUPERSEDED }

  /** Detects immutable-record conflicts and stale pointer changes before the publication CAS. */
  CompletableFuture<Void> preflight(PreparedAuxiliaryRecord record, RequestScope scope);

  /** Applies one record after selection; implementations must preserve monotonic version pointers. */
  CompletableFuture<ApplyResult> applySelected(
      CommitDecision decision, PreparedAuxiliaryRecord record, RequestScope scope);
}
