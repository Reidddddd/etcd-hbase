package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Immutable deployment snapshot and monotone progress of one authorization revocation. */
public record AuthorizationRevocation(ByteKey operationId, ByteKey subjectFamilyDigest,
    String providerId, String backendIdentityDomain, long credentialGeneration, long operationSequence,
    long configurationGeneration, long revision, State state, List<Target> targets) {
  // The public runtime configuration limits a deployment to 32 table groups.
  public static final int MAX_TARGETS = 32;

  public enum State { INSTALLING, COMPLETED }

  /** Dataset and authority digests bind the target to an incarnation and scoped subject. */
  public record TargetIdentity(String tableGroupId, ByteKey datasetDigest, ByteKey authorityDigest) {
    public TargetIdentity {
      if (tableGroupId == null || !tableGroupId.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
        throw new IllegalArgumentException("invalid revocation table group");
      }
      requireDigest(datasetDigest, 32, "datasetDigest");
      requireDigest(authorityDigest, 32, "authorityDigest");
    }
  }

  public record Target(TargetIdentity identity, long barrierGeneration, boolean installed,
      boolean released) {
    public Target {
      Objects.requireNonNull(identity, "identity");
      if (barrierGeneration < 0 || barrierGeneration == 0 && installed || released && !installed) {
        throw new IllegalArgumentException("invalid revocation barrier progress");
      }
    }
  }

  public AuthorizationRevocation {
    requireDigest(operationId, 16, "operationId");
    requireDigest(subjectFamilyDigest, 32, "subjectFamilyDigest");
    requireId(providerId);
    requireId(backendIdentityDomain);
    Objects.requireNonNull(state, "state");
    targets = List.copyOf(Objects.requireNonNull(targets, "targets"));
    if (credentialGeneration < 1 || operationSequence < 1 || configurationGeneration < 1
        || revision < 1 || targets.isEmpty()
        || targets.size() > MAX_TARGETS) {
      throw new IllegalArgumentException("invalid authorization revocation record");
    }
    String previous = null;
    for (Target target : targets) {
      String group = target.identity().tableGroupId();
      if (previous != null && previous.compareTo(group) >= 0) {
        throw new IllegalArgumentException("revocation targets must be distinct and ordered");
      }
      previous = group;
    }
    if (state == State.COMPLETED && targets.stream().anyMatch(target -> !target.released())) {
      throw new IllegalArgumentException("completed revocation has unfinished targets");
    }
  }

  public static AuthorizationRevocation start(ByteKey operationId, ByteKey subjectFamilyDigest,
      String providerId, String backendIdentityDomain, long credentialGeneration, long operationSequence,
      long configurationGeneration, List<TargetIdentity> targets) {
    Objects.requireNonNull(targets, "targets");
    var ordered = targets.stream().sorted(Comparator.comparing(TargetIdentity::tableGroupId))
        .map(target -> new Target(target, 0, false, false)).toList();
    return new AuthorizationRevocation(operationId, subjectFamilyDigest, providerId,
        backendIdentityDomain, credentialGeneration, operationSequence,
        configurationGeneration, 1, State.INSTALLING, ordered);
  }

  public boolean isInitial() {
    return revision == 1 && state == State.INSTALLING
        && targets.stream().allMatch(target -> target.barrierGeneration() == 0
            && !target.installed() && !target.released());
  }

  public AuthorizationRevocation assignBarrier(String group, long barrierGeneration) {
    requireInstalling();
    if (barrierGeneration < 1) throw new IllegalArgumentException("barrier must be positive");
    int index = targetIndex(group);
    Target current = targets.get(index);
    if (current.barrierGeneration() == barrierGeneration) return this;
    if (current.barrierGeneration() != 0) {
      throw new IllegalStateException("revocation barrier is already assigned");
    }
    return replaceTarget(index, new Target(current.identity(), barrierGeneration, false, false));
  }

  public AuthorizationRevocation acknowledge(String group, long barrierGeneration) {
    requireInstalling();
    int index = targetIndex(group);
    Target current = targets.get(index);
    if (barrierGeneration < 1 || current.barrierGeneration() != barrierGeneration) {
      throw new IllegalArgumentException("acknowledgement does not match its barrier");
    }
    if (current.installed()) return this;
    return replaceTarget(index, new Target(current.identity(), barrierGeneration, true, false));
  }

  /** Release an authority only after every affected group's barrier is durable. */
  public AuthorizationRevocation release(String group) {
    requireInstalling();
    if (targets.stream().anyMatch(target -> !target.installed())) {
      throw new IllegalStateException("not all authorization barriers are installed");
    }
    int index = targetIndex(group);
    Target current = targets.get(index);
    if (current.released()) return this;
    return replaceTarget(index, new Target(current.identity(), current.barrierGeneration(), true, true));
  }

  public AuthorizationRevocation finish() {
    requireInstalling();
    if (targets.stream().anyMatch(target -> !target.released())) {
      throw new IllegalStateException("revocation authorities are not fully released");
    }
    return new AuthorizationRevocation(operationId, subjectFamilyDigest, providerId,
        backendIdentityDomain, credentialGeneration, operationSequence, configurationGeneration,
        nextRevision(), State.COMPLETED, targets);
  }

  /** Exact-CAS replacement permits one durable progress step and never changes the snapshot. */
  public boolean allowsTransitionTo(AuthorizationRevocation replacement) {
    if (replacement == null || state != State.INSTALLING || revision == Long.MAX_VALUE
        || replacement.revision != revision + 1 || !operationId.equals(replacement.operationId)
        || !subjectFamilyDigest.equals(replacement.subjectFamilyDigest)
        || !providerId.equals(replacement.providerId)
        || !backendIdentityDomain.equals(replacement.backendIdentityDomain)
        || credentialGeneration != replacement.credentialGeneration
        || operationSequence != replacement.operationSequence
        || configurationGeneration != replacement.configurationGeneration
        || targets.size() != replacement.targets.size()) return false;
    int changes = 0;
    for (int index = 0; index < targets.size(); index++) {
      Target current = targets.get(index);
      Target next = replacement.targets.get(index);
      if (!current.identity().equals(next.identity())) return false;
      if (current.equals(next)) continue;
      boolean assigned = current.barrierGeneration() == 0 && !current.installed()
          && !current.released() && next.barrierGeneration() > 0 && !next.installed() && !next.released();
      boolean acknowledged = current.barrierGeneration() > 0 && !current.installed()
          && !current.released() && next.barrierGeneration() == current.barrierGeneration()
          && next.installed() && !next.released();
      boolean released = current.installed() && !current.released()
          && next.barrierGeneration() == current.barrierGeneration() && next.installed() && next.released()
          && targets.stream().allMatch(Target::installed);
      if (!assigned && !acknowledged && !released) return false;
      changes++;
    }
    return replacement.state == State.INSTALLING && changes == 1
        || replacement.state == State.COMPLETED && changes == 0
            && targets.stream().allMatch(Target::released);
  }

  private AuthorizationRevocation replaceTarget(int index, Target replacement) {
    var updated = new ArrayList<>(targets);
    updated.set(index, replacement);
    return new AuthorizationRevocation(operationId, subjectFamilyDigest, providerId,
        backendIdentityDomain, credentialGeneration, operationSequence, configurationGeneration,
        nextRevision(), state, updated);
  }

  private int targetIndex(String group) {
    for (int index = 0; index < targets.size(); index++) {
      if (targets.get(index).identity().tableGroupId().equals(group)) return index;
    }
    throw new IllegalArgumentException("table group is not in the revocation snapshot");
  }

  private void requireInstalling() {
    if (state != State.INSTALLING) throw new IllegalStateException("revocation is completed");
  }

  private long nextRevision() {
    if (revision == Long.MAX_VALUE) throw new IllegalStateException("revocation revision exhausted");
    return revision + 1;
  }

  private static void requireDigest(ByteKey value, int length, String name) {
    if (Objects.requireNonNull(value, name).length() != length) {
      throw new IllegalArgumentException(name + " has an invalid length");
    }
  }

  private static void requireId(String value) {
    if (value == null || !value.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException("invalid revocation identity domain");
    }
  }
}
