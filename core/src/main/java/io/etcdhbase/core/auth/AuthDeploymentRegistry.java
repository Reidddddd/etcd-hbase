package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** One atomic deployment inventory and its in-progress authorization operation. */
public record AuthDeploymentRegistry(String trustDomain, long configurationGeneration,
    long revision, List<Group> groups, AuthorizationRevocation revocation,
    AuthConfigurationUpdate configurationUpdate) {

  public AuthDeploymentRegistry(String trustDomain, long configurationGeneration, long revision,
      List<Group> groups, AuthorizationRevocation revocation) {
    this(trustDomain, configurationGeneration, revision, groups, revocation, null);
  }

  public record Group(String tableGroupId, String providerId, String backendIdentityDomain,
      String connectionProfileId,
      ByteKey datasetDigest, ByteKey configurationDigest, boolean enabled) {
    public Group {
      requireId(tableGroupId);
      requireId(providerId);
      requireId(backendIdentityDomain);
      requireId(connectionProfileId);
      requireDigest(datasetDigest);
      requireDigest(configurationDigest);
    }
  }

  public AuthDeploymentRegistry {
    requireId(trustDomain);
    groups = List.copyOf(Objects.requireNonNull(groups, "groups"));
    if (configurationGeneration < 1 || revision < 1 || groups.isEmpty()
        || groups.size() > AuthorizationRevocation.MAX_TARGETS) {
      throw new IllegalArgumentException("invalid deployment registry");
    }
    String previous = null;
    for (Group group : groups) {
      if (previous != null && previous.compareTo(group.tableGroupId()) >= 0) {
        throw new IllegalArgumentException("table groups must be distinct and ordered");
      }
      previous = group.tableGroupId();
    }
    if (revocation != null && !matchesInventory(groups, configurationGeneration, revocation)) {
      throw new IllegalArgumentException("revocation does not match deployment inventory");
    }
    if (configurationUpdate != null && (!matchesUpdateInventory(groups, configurationUpdate)
        || configurationUpdate.operationSequence() > revision
        || configurationUpdate.replacement().equals(groups)
        || revocation != null && revocation.state() == AuthorizationRevocation.State.INSTALLING)) {
      throw new IllegalArgumentException("configuration update does not match deployment inventory");
    }
  }

  public static AuthDeploymentRegistry initial(String trustDomain, List<Group> groups) {
    return new AuthDeploymentRegistry(trustDomain, 1, 1, ordered(groups), null);
  }

  public AuthDeploymentRegistry beginRevocation(AuthorizationRevocation operation) {
    Objects.requireNonNull(operation, "operation");
    if (installing()) throw new IllegalStateException("deployment revocation is already installing");
    if (!operation.isInitial() || operation.operationSequence() != nextRevision()
        || !matchesInventory(groups, configurationGeneration, operation)
        || revocation != null && revocation.operationId().equals(operation.operationId())) {
      throw new IllegalArgumentException("invalid deployment revocation snapshot");
    }
    return new AuthDeploymentRegistry(trustDomain, configurationGeneration, nextRevision(),
        groups, operation);
  }

  public AuthDeploymentRegistry beginConfigurationUpdate(List<Group> replacement) {
    if (installing()) throw new IllegalStateException("deployment operation is installing");
    var update = AuthConfigurationUpdate.start(nextRevision(), groups, replacement);
    return new AuthDeploymentRegistry(trustDomain, configurationGeneration, nextRevision(),
        groups, revocation, update);
  }

  public AuthDeploymentRegistry advanceConfigurationUpdate(AuthConfigurationUpdate next) {
    if (configurationUpdate == null || !configurationUpdate.allowsTransitionTo(next)) {
      throw new IllegalArgumentException("invalid configuration update progress");
    }
    return new AuthDeploymentRegistry(trustDomain, configurationGeneration, nextRevision(),
        groups, revocation, next);
  }

  public AuthDeploymentRegistry finishConfigurationUpdate() {
    if (configurationUpdate == null || !configurationUpdate.isInstalled()) {
      throw new IllegalStateException("configuration barriers are not installed");
    }
    return new AuthDeploymentRegistry(trustDomain, Math.addExact(configurationGeneration, 1),
        nextRevision(), configurationUpdate.replacement(), null, null);
  }

  public AuthDeploymentRegistry advanceRevocation(AuthorizationRevocation next) {
    if (revocation == null || revocation.state() != AuthorizationRevocation.State.INSTALLING) {
      throw new IllegalStateException("deployment has no installing revocation");
    }
    if (!revocation.allowsTransitionTo(next)) {
      throw new IllegalArgumentException("invalid revocation progress");
    }
    return new AuthDeploymentRegistry(trustDomain, configurationGeneration, nextRevision(), groups, next);
  }

  public AuthDeploymentRegistry updateConfiguration(List<Group> replacement) {
    if (installing()) throw new IllegalStateException("deployment revocation is installing");
    List<Group> next = ordered(replacement);
    if (groups.equals(next)) throw new IllegalArgumentException("deployment configuration is unchanged");
    return new AuthDeploymentRegistry(trustDomain, Math.addExact(configurationGeneration, 1),
        nextRevision(), next, null);
  }

  /** Exact CAS must advance one registry revision and one legal operation or configuration step. */
  public boolean permitsReplacement(AuthDeploymentRegistry next) {
    if (next == null || revision == Long.MAX_VALUE || next.revision != revision + 1
        || !trustDomain.equals(next.trustDomain)) return false;
    if (configurationGeneration == next.configurationGeneration && groups.equals(next.groups)) {
      if (configurationUpdate != null) {
        return Objects.equals(revocation, next.revocation)
            && configurationUpdate.allowsTransitionTo(next.configurationUpdate);
      }
      if (next.configurationUpdate != null) {
        return !installing() && Objects.equals(revocation, next.revocation)
            && next.configurationUpdate.operationSequence() == next.revision
            && next.configurationUpdate.isInitial();
      }
      if (installing()) return revocation.allowsTransitionTo(next.revocation);
      return next.revocation != null && next.revocation.isInitial()
          && next.revocation.operationSequence() == next.revision
          && (revocation == null || !revocation.operationId().equals(next.revocation.operationId()));
    }
    return configurationUpdate != null && configurationUpdate.isInstalled()
        && configurationGeneration < Long.MAX_VALUE
        && next.configurationGeneration == configurationGeneration + 1
        && next.groups.equals(configurationUpdate.replacement())
        && next.revocation == null && next.configurationUpdate == null;
  }

  private boolean installing() {
    return configurationUpdate != null
        || revocation != null && revocation.state() == AuthorizationRevocation.State.INSTALLING;
  }

  private static boolean matchesUpdateInventory(List<Group> groups, AuthConfigurationUpdate update) {
    if (groups.size() != update.targets().size()) return false;
    for (int i = 0; i < groups.size(); i++) {
      if (!groups.get(i).tableGroupId().equals(update.targets().get(i).tableGroupId())
          || !groups.get(i).datasetDigest().equals(update.targets().get(i).datasetDigest())) return false;
    }
    return true;
  }

  private static boolean matchesInventory(List<Group> groups, long generation,
      AuthorizationRevocation operation) {
    List<Group> familyGroups = groups.stream().filter(group -> group.enabled()
        && group.providerId().equals(operation.providerId())
        && group.backendIdentityDomain().equals(operation.backendIdentityDomain())).toList();
    if (operation.configurationGeneration() != generation
        || operation.targets().size() != familyGroups.size()) return false;
    for (int i = 0; i < familyGroups.size(); i++) {
      var group = familyGroups.get(i);
      var target = operation.targets().get(i).identity();
      if (!group.tableGroupId().equals(target.tableGroupId())
          || !group.datasetDigest().equals(target.datasetDigest())) return false;
    }
    return true;
  }

  private long nextRevision() {
    return Math.addExact(revision, 1);
  }

  private static List<Group> ordered(List<Group> groups) {
    return Objects.requireNonNull(groups, "groups").stream()
        .sorted(Comparator.comparing(Group::tableGroupId)).toList();
  }

  private static void requireId(String value) {
    if (value == null || !value.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException("invalid deployment registry identifier");
    }
  }

  private static void requireDigest(ByteKey value) {
    if (value == null || value.length() != 32) {
      throw new IllegalArgumentException("invalid deployment registry digest");
    }
  }
}
