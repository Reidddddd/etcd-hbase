package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.AuthorizationBarrierInstaller;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Binds subject authority transitions to the matching incarnation's durable commit barrier. */
public final class TargetAuthorizationBarriers implements AuthorizationRevocationExecutor.Barriers {
  private static final int MAX_ATTEMPTS = 32;

  public record Group(String providerId, AuthenticationScope scope, ByteKey datasetDigest,
      AuthorizationBarrierInstaller installer) {
    public Group {
      Objects.requireNonNull(providerId, "providerId");
      Objects.requireNonNull(scope, "scope");
      Objects.requireNonNull(datasetDigest, "datasetDigest");
      Objects.requireNonNull(installer, "installer");
      if (datasetDigest.length() != 32) throw new IllegalArgumentException("invalid dataset digest");
    }
  }

  private final AuthSessionStore store;
  private final String trustDomain;
  private final Map<String, Group> groups;

  public TargetAuthorizationBarriers(AuthSessionStore store, String trustDomain, Map<String, Group> groups) {
    this.store = Objects.requireNonNull(store, "store");
    this.trustDomain = Objects.requireNonNull(trustDomain, "trustDomain");
    this.groups = Map.copyOf(groups);
  }

  @Override
  public CompletionStage<Long> allocate(AuthorizationRevocation.TargetIdentity target, ByteKey operationId,
      long operationSequence, long configurationGeneration, long credentialGeneration, RequestScope scope) {
    return allocate(target, operationId, operationSequence, configurationGeneration, credentialGeneration, scope, 0);
  }

  private CompletionStage<Long> allocate(AuthorizationRevocation.TargetIdentity target, ByteKey operationId,
      long operationSequence, long configurationGeneration, long credentialGeneration, RequestScope scope, int attempts) {
    Group group = group(target);
    if (group == null || attempts >= MAX_ATTEMPTS) return unavailable();
    return store.readAuthority(target.authorityDigest(), scope).thenCompose(found -> {
      if (found.isEmpty()) {
        var initial = new SubjectAuthority(target.authorityDigest(), trustDomain, group.providerId(),
            group.scope(), 0, 1, SubjectAuthority.State.ACTIVE);
        return store.createAuthorityIfAbsent(initial, scope).thenCompose(ignored ->
            allocate(target, operationId, operationSequence, configurationGeneration,
                credentialGeneration, scope, attempts + 1));
      }
      var authority = found.orElseThrow();
      if (!matches(authority, target, group) || authority.credentialGeneration() > credentialGeneration) {
        return unavailable();
      }
      if (authority.state() == SubjectAuthority.State.ACTIVE) {
        if (operationId.equals(authority.revocationOperationId())) return unavailable();
        SubjectAuthority pending;
        try { pending = authority.beginDeploymentRevocation(operationId, operationSequence,
            configurationGeneration, credentialGeneration); }
        catch (IllegalArgumentException invalid) { return unavailable(); }
        return store.compareAndSetAuthority(authority, pending, scope).thenCompose(ignored ->
            allocate(target, operationId, operationSequence, configurationGeneration,
                credentialGeneration, scope, attempts + 1));
      }
      if (authority.state() != SubjectAuthority.State.REVOKING
          || !operationId.equals(authority.revocationOperationId())
          || authority.revocationOperationSequence() != operationSequence
          || authority.revocationConfigurationGeneration() != configurationGeneration
          || authority.credentialGeneration() != credentialGeneration) return unavailable();
      long existing = authority.revocationTargets().getFirst().barrierGeneration();
      if (existing > 0) return CompletableFuture.completedFuture(existing);
      return group.installer().nextTargetGeneration(scope).thenCompose(generation ->
          store.compareAndSetAuthority(authority, authority.assignRevocationBarrierTarget(generation), scope)
              .thenCompose(ignored -> allocate(target, operationId, operationSequence,
                  configurationGeneration, credentialGeneration, scope, attempts + 1)));
    });
  }

  @Override
  public CompletionStage<Void> install(AuthorizationRevocation.TargetIdentity target, ByteKey operationId,
      long generation, RequestScope scope) {
    return install(target, operationId, generation, scope, 0);
  }

  private CompletionStage<Void> install(AuthorizationRevocation.TargetIdentity target, ByteKey operationId,
      long generation, RequestScope scope, int attempts) {
    Group group = group(target);
    if (group == null || attempts >= MAX_ATTEMPTS) return unavailable();
    return store.readAuthority(target.authorityDigest(), scope).thenCompose(found -> {
      if (found.isEmpty()) return unavailable();
      var authority = found.orElseThrow();
      if (!matches(authority, target, group) || !operationId.equals(authority.revocationOperationId())) {
        return unavailable();
      }
      if (authority.state() == SubjectAuthority.State.ACTIVE) return CompletableFuture.completedFuture(null);
      if (authority.state() != SubjectAuthority.State.REVOKING) return unavailable();
      var progress = authority.revocationTargets().getFirst();
      if (generation < 1 || progress.barrierGeneration() != generation) return unavailable();
      if (progress.installed()) return CompletableFuture.completedFuture(null);
      return group.installer().installAtLeast(generation, scope).thenCompose(ignored ->
          store.compareAndSetAuthority(authority, authority.markRevocationInstalled(target.tableGroupId(), generation), scope)
              .thenCompose(updated -> updated ? CompletableFuture.completedFuture(null)
                  : install(target, operationId, generation, scope, attempts + 1)));
    });
  }

  @Override
  public CompletionStage<Void> release(AuthorizationRevocation.TargetIdentity target, ByteKey operationId,
      long credentialGeneration, RequestScope scope) {
    return release(target, operationId, credentialGeneration, scope, 0);
  }

  private CompletionStage<Void> release(AuthorizationRevocation.TargetIdentity target, ByteKey operationId,
      long credentialGeneration, RequestScope scope, int attempts) {
    Group group = group(target);
    if (group == null || attempts >= MAX_ATTEMPTS) return unavailable();
    return store.readAuthority(target.authorityDigest(), scope).thenCompose(found -> {
      if (found.isEmpty()) return unavailable();
      var authority = found.orElseThrow();
      if (!matches(authority, target, group) || !operationId.equals(authority.revocationOperationId())
          || authority.credentialGeneration() != credentialGeneration) return unavailable();
      if (authority.state() == SubjectAuthority.State.ACTIVE) return CompletableFuture.completedFuture(null);
      if (authority.state() != SubjectAuthority.State.REVOKING
          || !authority.revocationTargets().getFirst().installed()) return unavailable();
      return store.compareAndSetAuthority(authority, authority.finishRevocation(SubjectAuthority.State.ACTIVE), scope)
          .thenCompose(updated -> updated ? CompletableFuture.completedFuture(null)
              : release(target, operationId, credentialGeneration, scope, attempts + 1));
    });
  }

  private Group group(AuthorizationRevocation.TargetIdentity target) {
    Group group = groups.get(target.tableGroupId());
    return group != null && group.scope().tableGroupId().equals(target.tableGroupId())
        && group.datasetDigest().equals(target.datasetDigest()) ? group : null;
  }

  private boolean matches(SubjectAuthority authority, AuthorizationRevocation.TargetIdentity target, Group group) {
    return authority.subjectDigest().equals(target.authorityDigest())
        && authority.trustDomain().equals(trustDomain) && authority.providerId().equals(group.providerId())
        && authority.scope().equals(group.scope());
  }

  private static <T> CompletionStage<T> unavailable() {
    return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
  }
}
