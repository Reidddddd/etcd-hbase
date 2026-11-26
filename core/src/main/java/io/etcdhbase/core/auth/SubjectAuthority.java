package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.util.List;
import java.util.Objects;

/** Current per-subject authorization fence shared by all persisted sessions. */
public record SubjectAuthority(
    ByteKey subjectDigest,
    String trustDomain,
    String providerId,
    AuthenticationScope scope,
    long credentialGeneration,
    long authorizationGeneration,
    State state,
    List<RevocationTarget> revocationTargets,
    ByteKey revocationOperationId,
    long revocationOperationSequence,
    long revocationConfigurationGeneration) {
  public SubjectAuthority(
      ByteKey subjectDigest, String trustDomain, String providerId, AuthenticationScope scope,
      long credentialGeneration, long authorizationGeneration, State state) {
    this(subjectDigest, trustDomain, providerId, scope, credentialGeneration,
        authorizationGeneration, state, List.of(), null, 0, 0);
  }

  public SubjectAuthority(ByteKey subjectDigest, String trustDomain, String providerId,
      AuthenticationScope scope, long credentialGeneration, long authorizationGeneration,
      State state, List<RevocationTarget> revocationTargets) {
    this(subjectDigest, trustDomain, providerId, scope, credentialGeneration,
        authorizationGeneration, state, revocationTargets, null, 0, 0);
  }

  public SubjectAuthority {
    Objects.requireNonNull(subjectDigest, "subjectDigest");
    if (subjectDigest.length() != 32) {
      throw new IllegalArgumentException("subjectDigest must contain 32 bytes");
    }
    trustDomain = requireId(trustDomain, "trustDomain");
    providerId = requireId(providerId, "providerId");
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(state, "state");
    if (revocationOperationId != null && revocationOperationId.length() != 16) {
      throw new IllegalArgumentException("invalid revocation operation ID");
    }
    if (revocationOperationId == null && (revocationOperationSequence != 0
        || revocationConfigurationGeneration != 0)
        || revocationOperationId != null && (revocationOperationSequence < 1
            || revocationConfigurationGeneration < 1)) {
      throw new IllegalArgumentException("invalid deployment revocation binding");
    }
    revocationTargets = List.copyOf(Objects.requireNonNull(revocationTargets, "revocationTargets"));
    if (credentialGeneration < 0 || authorizationGeneration < 1) {
      throw new IllegalArgumentException("subject authority generation is outside its valid range");
    }
    if (state == State.REVOKING) {
      if (revocationTargets.size() != 1
          || !revocationTargets.getFirst().tableGroupId().equals(scope.tableGroupId())) {
        throw new IllegalArgumentException(
            "revocation target must match the subject authority table group");
      }
    } else if (!revocationTargets.isEmpty()) {
      throw new IllegalArgumentException("completed subject authority cannot retain revocation targets");
    }
  }

  public enum State {
    ACTIVE,
    REVOKED,
    REVOKING
  }

  /** A durable per-table-group barrier target and installation acknowledgement. */
  public record RevocationTarget(
      String tableGroupId, long barrierGeneration, boolean installed) {
    public RevocationTarget {
      tableGroupId = requireId(tableGroupId, "tableGroupId");
      if (barrierGeneration < 0 || barrierGeneration == 0 && installed) {
        throw new IllegalArgumentException("authorization barrier target is outside its valid range");
      }
    }
  }

  /** Starts a monotone, resumable authorization-generation transition. */
  public SubjectAuthority beginRevocation(
      long newCredentialGeneration, long newAuthorizationGeneration,
      List<RevocationTarget> targets) {
    if (state == State.REVOKING || newCredentialGeneration <= credentialGeneration
        || !advancesOnce(authorizationGeneration, newAuthorizationGeneration)) {
      throw new IllegalArgumentException("subject revocation must advance its authority generation");
    }
    if (targets.size() != 1 || targets.getFirst().barrierGeneration() != 0
        || targets.getFirst().installed()) {
      throw new IllegalArgumentException("subject revocation must start without an allocated barrier target");
    }
    return new SubjectAuthority(subjectDigest, trustDomain, providerId, scope,
        newCredentialGeneration, newAuthorizationGeneration, State.REVOKING, targets);
  }

  /** Starts a credential rotation with a durable but not-yet-allocated group barrier target. */
  public SubjectAuthority beginRevocation(
      long newCredentialGeneration, long newAuthorizationGeneration) {
    return beginRevocation(newCredentialGeneration, newAuthorizationGeneration,
        List.of(new RevocationTarget(scope.tableGroupId(), 0, false)));
  }

  /** Re-fences an already-current credential when a deployment-wide operation includes this group. */
  public SubjectAuthority beginAuthorizationRefresh(long newAuthorizationGeneration) {
    if (state != State.ACTIVE || !advancesOnce(authorizationGeneration, newAuthorizationGeneration)) {
      throw new IllegalArgumentException("authorization refresh must advance an active authority");
    }
    return new SubjectAuthority(subjectDigest, trustDomain, providerId, scope,
        credentialGeneration, newAuthorizationGeneration, State.REVOKING,
        List.of(new RevocationTarget(scope.tableGroupId(), 0, false)));
  }

  /** Binds a deployment operation to this subject until and after its barrier is released. */
  public SubjectAuthority beginDeploymentRevocation(ByteKey operationId, long operationSequence,
      long configurationGeneration, long newCredentialGeneration) {
    if (Objects.requireNonNull(operationId, "operationId").length() != 16
        || state != State.ACTIVE || newCredentialGeneration < credentialGeneration
        || operationId.equals(revocationOperationId) || operationSequence <= revocationOperationSequence
        || configurationGeneration < Math.max(1, revocationConfigurationGeneration)
        || authorizationGeneration == Long.MAX_VALUE) {
      throw new IllegalArgumentException("invalid deployment subject revocation");
    }
    return new SubjectAuthority(subjectDigest, trustDomain, providerId, scope, newCredentialGeneration,
        authorizationGeneration + 1, State.REVOKING,
        List.of(new RevocationTarget(scope.tableGroupId(), 0, false)), operationId,
        operationSequence, configurationGeneration);
  }

  /** Updates pending credentials without reopening authorization admission between rotations. */
  public SubjectAuthority advanceRevocationCredentialGeneration(
      long newCredentialGeneration, long newAuthorizationGeneration) {
    if (state != State.REVOKING || revocationOperationId != null
        || newCredentialGeneration <= credentialGeneration
        || !advancesOnce(authorizationGeneration, newAuthorizationGeneration)) {
      throw new IllegalArgumentException("pending revocation generations must advance");
    }
    return new SubjectAuthority(subjectDigest, trustDomain, providerId, scope,
        newCredentialGeneration, newAuthorizationGeneration, State.REVOKING, revocationTargets, null, 0, 0);
  }

  /** Persists the next group barrier target only after this authority is already REVOKING. */
  public SubjectAuthority assignRevocationBarrierTarget(long barrierGeneration) {
    if (state != State.REVOKING || barrierGeneration < 1) {
      throw new IllegalStateException("subject authority has no allocatable revocation target");
    }
    RevocationTarget current = revocationTargets.getFirst();
    if (current.barrierGeneration() == barrierGeneration) return this;
    if (current.barrierGeneration() != 0 || current.installed()) {
      throw new IllegalStateException("subject authority already has a revocation barrier target");
    }
    return new SubjectAuthority(subjectDigest, trustDomain, providerId, scope,
        credentialGeneration, authorizationGeneration, State.REVOKING,
        List.of(new RevocationTarget(current.tableGroupId(), barrierGeneration, false)), revocationOperationId,
        revocationOperationSequence, revocationConfigurationGeneration);
  }

  public SubjectAuthority markRevocationInstalled(String tableGroupId, long barrierGeneration) {
    if (state != State.REVOKING) {
      throw new IllegalStateException("subject authority has no pending revocation");
    }
    if (barrierGeneration < 1) {
      throw new IllegalArgumentException("installed authorization barrier must be positive");
    }
    boolean matched = false;
    var updated = new java.util.ArrayList<RevocationTarget>(revocationTargets.size());
    for (RevocationTarget target : revocationTargets) {
      if (target.tableGroupId().equals(tableGroupId)
          && target.barrierGeneration() == barrierGeneration) {
        matched = true;
        updated.add(target.installed() ? target
            : new RevocationTarget(tableGroupId, barrierGeneration, true));
      } else {
        updated.add(target);
      }
    }
    if (!matched) throw new IllegalArgumentException("revocation target does not match its task");
    return new SubjectAuthority(subjectDigest, trustDomain, providerId, scope,
        credentialGeneration, authorizationGeneration, State.REVOKING, updated, revocationOperationId,
        revocationOperationSequence, revocationConfigurationGeneration);
  }

  public SubjectAuthority finishRevocation(State completedState) {
    if (state != State.REVOKING || (completedState != State.ACTIVE && completedState != State.REVOKED)
        || revocationTargets.size() != 1
        || revocationTargets.getFirst().barrierGeneration() < 1
        || revocationTargets.stream().anyMatch(target -> !target.installed())) {
      throw new IllegalStateException("subject revocation is not fully installed");
    }
    return new SubjectAuthority(subjectDigest, trustDomain, providerId, scope,
        credentialGeneration, authorizationGeneration, completedState, List.of(), revocationOperationId,
        revocationOperationSequence, revocationConfigurationGeneration);
  }

  /** Restricts auth-control CAS updates to monotone task progress and completed installation. */
  public boolean allowsTransitionTo(SubjectAuthority replacement) {
    if (replacement == null || !sameIdentity(replacement)) return false;
    if (state != State.REVOKING) {
      return replacement.state == State.REVOKING
          && (replacement.revocationOperationId == null && revocationOperationId == null
              || replacement.revocationOperationId != null
                  && !replacement.revocationOperationId.equals(revocationOperationId)
                  && replacement.revocationOperationSequence > revocationOperationSequence
                  && replacement.revocationConfigurationGeneration >= revocationConfigurationGeneration)
          && advancesOnce(authorizationGeneration, replacement.authorizationGeneration)
          && (replacement.credentialGeneration > credentialGeneration
              || state == State.ACTIVE && replacement.credentialGeneration == credentialGeneration)
          && replacement.revocationTargets.size() == 1
          && replacement.revocationTargets.getFirst().barrierGeneration() == 0
          && replacement.revocationTargets.stream().noneMatch(RevocationTarget::installed);
    }
    if (!Objects.equals(revocationOperationId, replacement.revocationOperationId)
        || revocationOperationSequence != replacement.revocationOperationSequence
        || revocationConfigurationGeneration != replacement.revocationConfigurationGeneration
        || revocationOperationId != null && replacement.credentialGeneration != credentialGeneration) return false;
    if (replacement.credentialGeneration < credentialGeneration
        || replacement.credentialGeneration > credentialGeneration
            && !advancesOnce(authorizationGeneration, replacement.authorizationGeneration)
        || replacement.credentialGeneration == credentialGeneration
            && replacement.authorizationGeneration != authorizationGeneration) {
      return false;
    }
    if (replacement.state == State.REVOKING) {
      if (replacement.revocationTargets.size() != revocationTargets.size()) return false;
      for (int index = 0; index < revocationTargets.size(); index++) {
        RevocationTarget current = revocationTargets.get(index);
        RevocationTarget next = replacement.revocationTargets.get(index);
        if (!current.tableGroupId().equals(next.tableGroupId())
            || next.barrierGeneration() < current.barrierGeneration()
            || current.barrierGeneration() == 0 && next.installed()
            || current.barrierGeneration() > 0
                && next.barrierGeneration() != current.barrierGeneration()
            || current.installed() && !next.installed()) {
          return false;
        }
      }
      return true;
    }
    return (replacement.state == State.ACTIVE || replacement.state == State.REVOKED)
        && replacement.credentialGeneration == credentialGeneration
        && replacement.authorizationGeneration == authorizationGeneration
        && replacement.revocationTargets.isEmpty()
        && revocationTargets.stream().allMatch(RevocationTarget::installed);
  }

  private boolean sameIdentity(SubjectAuthority other) {
    return subjectDigest.equals(other.subjectDigest) && trustDomain.equals(other.trustDomain)
        && providerId.equals(other.providerId) && scope.equals(other.scope);
  }

  private static boolean advancesOnce(long current, long replacement) {
    return current < Long.MAX_VALUE && replacement == current + 1;
  }

  private static String requireId(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!value.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException(name + " must be a bounded configured identifier");
    }
    return value;
  }
}
