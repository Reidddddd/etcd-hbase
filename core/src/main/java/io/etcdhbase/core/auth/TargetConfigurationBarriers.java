package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.AuthorizationBarrierInstaller;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Binds a configuration update to the old group's exact dataset and commit control. */
public final class TargetConfigurationBarriers implements AuthConfigurationUpdateExecutor.Barriers {
  public record Group(ByteKey datasetDigest, AuthorizationBarrierInstaller installer) {
    public Group {
      Objects.requireNonNull(datasetDigest, "datasetDigest");
      Objects.requireNonNull(installer, "installer");
      if (datasetDigest.length() != 32) throw new IllegalArgumentException("invalid dataset digest");
    }
  }

  private final Map<String, Group> groups;

  public TargetConfigurationBarriers(Map<String, Group> groups) {
    this.groups = Map.copyOf(groups);
  }

  @Override public CompletionStage<Long> allocate(AuthConfigurationUpdate.Target target, RequestScope scope) {
    Group group = group(target);
    return group == null ? unavailable() : group.installer().nextTargetGeneration(scope);
  }

  @Override public CompletionStage<Void> install(AuthConfigurationUpdate.Target target, RequestScope scope) {
    Group group = group(target);
    return group == null ? unavailable()
        : group.installer().installAtLeast(target.barrierGeneration(), scope).thenApply(ignored -> null);
  }

  private Group group(AuthConfigurationUpdate.Target target) {
    Group group = groups.get(target.tableGroupId());
    return group != null && group.datasetDigest().equals(target.datasetDigest()) ? group : null;
  }

  private static <T> CompletionStage<T> unavailable() {
    return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
  }
}
