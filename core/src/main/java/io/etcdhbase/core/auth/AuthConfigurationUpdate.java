package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Durable progress for fencing the old inventory before publishing its replacement. */
public record AuthConfigurationUpdate(long operationSequence,
    List<AuthDeploymentRegistry.Group> replacement, List<Target> targets) {
  public record Target(String tableGroupId, ByteKey datasetDigest, long barrierGeneration,
      boolean installed) {
    public Target {
      if (tableGroupId == null || !tableGroupId.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")
          || datasetDigest == null || datasetDigest.length() != 32 || barrierGeneration < 0
          || installed && barrierGeneration == 0) {
        throw new IllegalArgumentException("invalid configuration barrier target");
      }
    }
  }

  public AuthConfigurationUpdate {
    replacement = List.copyOf(Objects.requireNonNull(replacement, "replacement"));
    targets = List.copyOf(Objects.requireNonNull(targets, "targets"));
    if (operationSequence < 2 || replacement.isEmpty() || targets.isEmpty()
        || replacement.size() > AuthorizationRevocation.MAX_TARGETS
        || targets.size() > AuthorizationRevocation.MAX_TARGETS) {
      throw new IllegalArgumentException("invalid configuration update");
    }
    requireOrdered(replacement.stream().map(AuthDeploymentRegistry.Group::tableGroupId).toList());
    requireOrdered(targets.stream().map(Target::tableGroupId).toList());
  }

  public static AuthConfigurationUpdate start(long sequence, List<AuthDeploymentRegistry.Group> oldGroups,
      List<AuthDeploymentRegistry.Group> replacement) {
    var next = Objects.requireNonNull(replacement, "replacement").stream()
        .sorted(Comparator.comparing(AuthDeploymentRegistry.Group::tableGroupId)).toList();
    var targets = Objects.requireNonNull(oldGroups, "oldGroups").stream()
        .map(group -> new Target(group.tableGroupId(), group.datasetDigest(), 0, false)).toList();
    return new AuthConfigurationUpdate(sequence, next, targets);
  }

  public AuthConfigurationUpdate assign(String group, long generation) {
    if (generation < 1) throw new IllegalArgumentException("invalid barrier generation");
    return change(group, target -> {
      if (target.barrierGeneration() != 0 || target.installed()) throw new IllegalStateException();
      return new Target(group, target.datasetDigest(), generation, false);
    });
  }

  public AuthConfigurationUpdate acknowledge(String group, long generation) {
    return change(group, target -> {
      if (target.installed() || generation < 1 || target.barrierGeneration() != generation) {
        throw new IllegalStateException();
      }
      return new Target(group, target.datasetDigest(), generation, true);
    });
  }

  public boolean isInitial() {
    return targets.stream().allMatch(target -> target.barrierGeneration() == 0 && !target.installed());
  }

  public boolean isInstalled() { return targets.stream().allMatch(Target::installed); }

  public boolean allowsTransitionTo(AuthConfigurationUpdate next) {
    if (next == null || operationSequence != next.operationSequence
        || !replacement.equals(next.replacement) || targets.size() != next.targets.size()) return false;
    int changes = 0;
    for (int i = 0; i < targets.size(); i++) {
      Target old = targets.get(i);
      Target current = next.targets.get(i);
      if (old.equals(current)) continue;
      if (!old.tableGroupId().equals(current.tableGroupId())
          || !old.datasetDigest().equals(current.datasetDigest())) return false;
      boolean assign = old.barrierGeneration() == 0 && !old.installed()
          && current.barrierGeneration() > 0 && !current.installed();
      boolean acknowledge = old.barrierGeneration() > 0 && !old.installed()
          && current.barrierGeneration() == old.barrierGeneration() && current.installed();
      if (!assign && !acknowledge || ++changes > 1) return false;
    }
    return changes == 1;
  }

  private AuthConfigurationUpdate change(String group, java.util.function.UnaryOperator<Target> action) {
    var changed = new ArrayList<>(targets);
    for (int i = 0; i < changed.size(); i++) {
      if (changed.get(i).tableGroupId().equals(group)) {
        changed.set(i, action.apply(changed.get(i)));
        return new AuthConfigurationUpdate(operationSequence, replacement, changed);
      }
    }
    throw new IllegalArgumentException("unknown configuration barrier target");
  }

  private static void requireOrdered(List<String> values) {
    for (int i = 1; i < values.size(); i++) {
      if (values.get(i - 1).compareTo(values.get(i)) >= 0) {
        throw new IllegalArgumentException("configuration targets must be distinct and ordered");
      }
    }
  }
}
