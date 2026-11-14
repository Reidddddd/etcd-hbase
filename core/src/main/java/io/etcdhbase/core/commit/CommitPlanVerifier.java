package io.etcdhbase.core.commit;

import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Verifies every sealed candidate and event reference before the publication CAS. */
public final class CommitPlanVerifier {
  private final ManifestCatalog catalog;
  private final MaterializationStore materialization;
  private final ResultCatalog results;
  private final AuxiliaryMaterializationStore auxiliaryMaterialization;
  private final AuxiliaryLeasePlanVerifier auxiliaryLeasePlanVerifier;

  public CommitPlanVerifier(ManifestCatalog catalog, MaterializationStore materialization) {
    this.catalog = Objects.requireNonNull(catalog, "catalog");
    this.materialization = Objects.requireNonNull(materialization, "materialization");
    this.results = null;
    this.auxiliaryMaterialization = null;
    this.auxiliaryLeasePlanVerifier = null;
  }

  /** Creates a verifier for business requests, which must have a complete durable result. */
  public CommitPlanVerifier(ManifestCatalog catalog, MaterializationStore materialization,
      ResultCatalog results) {
    this(catalog, materialization, results, null);
  }

  /** Creates a verifier equipped for manifests that contain M4 auxiliary state. */
  public CommitPlanVerifier(ManifestCatalog catalog, MaterializationStore materialization,
      ResultCatalog results, AuxiliaryMaterializationStore auxiliaryMaterialization) {
    this(catalog, materialization, results, auxiliaryMaterialization, null);
  }

  /** Creates a verifier that checks both auxiliary bytes and cross-record Lease invariants. */
  public CommitPlanVerifier(ManifestCatalog catalog, MaterializationStore materialization,
      ResultCatalog results, AuxiliaryMaterializationStore auxiliaryMaterialization,
      AuxiliaryLeasePlanVerifier auxiliaryLeasePlanVerifier) {
    this.catalog = Objects.requireNonNull(catalog, "catalog");
    this.materialization = Objects.requireNonNull(materialization, "materialization");
    this.results = Objects.requireNonNull(results, "results");
    this.auxiliaryMaterialization = auxiliaryMaterialization;
    this.auxiliaryLeasePlanVerifier = auxiliaryLeasePlanVerifier;
  }

  public CompletableFuture<ManifestRoot> verify(
      InvocationBinding invocation, ManifestRoot proposed, RequestScope scope) {
    Objects.requireNonNull(invocation, "invocation");
    Objects.requireNonNull(proposed, "proposed");
    if (invocation.state() != InvocationBinding.State.SEALED
        || invocation.manifestRoot().filter(proposed.systemIdentity()::equals).isEmpty()
        || !invocation.transactionId().equals(proposed.transactionId())
        || invocation.preparedOwnerEpoch() != proposed.ownerEpoch()
        || invocation.baseSequence() != proposed.baseSequence()
        || invocation.baseRevision() != proposed.baseRevision()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "commit preflight binding differs from the sealed invocation"));
    }
    return catalog.readRoot(proposed.systemIdentity(), scope).thenCompose(stored -> {
      ManifestRoot root = stored.orElseThrow(() ->
          new IllegalStateException("sealed manifest root is missing"));
      if (!root.digest().equals(proposed.digest())) {
        return CompletableFuture.failedFuture(new IllegalStateException(
            "durable manifest root differs from the proposed root"));
      }
      if (root.resultReference().isPresent() != (results != null)) {
        return CompletableFuture.failedFuture(new IllegalStateException(
            "manifest result capability does not match the publisher"));
      }
      CompletableFuture<?> verified;
      if (root.resultReference().isPresent()) {
        ResultReference result = root.resultReference().orElseThrow();
        if (!result.identity().matches(invocation)) {
          return CompletableFuture.failedFuture(new IllegalStateException(
              "manifest result belongs to another invocation"));
        }
        verified = results.verify(result, scope);
      } else {
        verified = CompletableFuture.completedFuture(null);
      }
      if (root.auxiliaryReference().isPresent() && auxiliaryMaterialization == null) {
        return CompletableFuture.failedFuture(new IllegalStateException(
            "publisher is not equipped to verify auxiliary materialization"));
      }
      if (root.auxiliaryReference().isPresent() && auxiliaryLeasePlanVerifier == null) {
        return CompletableFuture.failedFuture(new IllegalStateException(
            "publisher is not equipped to verify auxiliary Lease relationships"));
      }
      AuxiliaryLeasePlanVerifier.Transaction leasePlan = root.auxiliaryReference().isPresent()
          ? auxiliaryLeasePlanVerifier.begin(root.baseSequence()) : null;
      return verified.thenCompose(ignored -> catalog.forEachCandidate(root, scope,
              candidate -> materialization.preflight(candidate, scope)))
          .thenCompose(ignored -> catalog.forEachEvent(root, scope,
              event -> CompletableFuture.completedFuture(null)))
          .thenCompose(ignored -> root.auxiliaryReference().isEmpty()
              ? CompletableFuture.completedFuture(null)
              : catalog.forEachAuxiliary(root, scope,
                  record -> {
                    leasePlan.accept(record);
                    return auxiliaryMaterialization.preflight(record, scope);
                  }))
          .thenApply(ignored -> {
            if (leasePlan != null) leasePlan.finish();
            return root;
          });
    });
  }
}
