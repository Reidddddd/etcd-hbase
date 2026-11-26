package io.etcdhbase.server.auth;

import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.core.auth.AuthDeploymentRegistryStore;
import io.etcdhbase.core.auth.AuthSessionStore;
import io.etcdhbase.core.auth.TargetAuthorizationBarriers;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.AuthorizationBarrierInstaller;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Pre-provisioned fixture using the production registry transitions and target barrier executor. */
public final class AuthDeploymentFixture implements AuthDeploymentRegistryStore {
  public static final String IDENTITY_DOMAIN = "fixture-realm";
  private final TargetAuthorizationBarriers barriers;
  private final ByteKey configurationDigest;
  private AuthDeploymentRegistry current;

  public AuthDeploymentFixture(AuthSessionStore sessions, String trustDomain, String providerId,
      AuthenticationScope scope, ByteKey dataset, Clock clock) {
    configurationDigest = ByteKey.copyOf(new byte[32]);
    current = AuthDeploymentRegistry.initial(trustDomain, List.of(new AuthDeploymentRegistry.Group(
        scope.tableGroupId(), providerId, IDENTITY_DOMAIN, scope.connectionProfileId(), dataset,
        configurationDigest, true)));
    var installer = new AuthorizationBarrierInstaller(new Control(dataset, clock), clock);
    barriers = new TargetAuthorizationBarriers(sessions, trustDomain, Map.of(scope.tableGroupId(),
        new TargetAuthorizationBarriers.Group(providerId, scope, dataset, installer)));
  }

  public TargetAuthorizationBarriers barriers() { return barriers; }
  public ByteKey configurationDigest() { return configurationDigest; }

  @Override public synchronized CompletionStage<Optional<AuthDeploymentRegistry>> readRegistry(RequestScope scope) {
    return CompletableFuture.completedFuture(Optional.of(current));
  }

  @Override public CompletionStage<Boolean> createRegistryIfAbsent(AuthDeploymentRegistry initial, RequestScope scope) {
    throw new AssertionError("session services must not initialize the deployment registry");
  }

  @Override public synchronized CompletionStage<Boolean> compareAndSetRegistry(AuthDeploymentRegistry observed,
      AuthDeploymentRegistry replacement, RequestScope scope) {
    if (!current.equals(observed) || !observed.permitsReplacement(replacement)) {
      return CompletableFuture.completedFuture(false);
    }
    current = replacement;
    return CompletableFuture.completedFuture(true);
  }

  private static final class Control implements CommitControlStore {
    private ControlSnapshot current;

    private Control(ByteKey fingerprint, Clock clock) {
      long now = clock.millis();
      current = snapshot(CommitState.initial(fingerprint, 1)
          .claimOwner("fixture-owner", now, now + 3_600_000)
          .enableAuthorizationBarrier("fixture-owner", 1, now));
    }

    @Override public synchronized CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(current));
    }

    @Override public CompletableFuture<Boolean> initializeIfAbsent(CommitState state, RequestScope scope) {
      throw new AssertionError("session services must not initialize the control row");
    }

    @Override public synchronized CompletableFuture<Boolean> compareAndSet(ControlSnapshot observed,
        CommitState replacement, RequestScope scope) {
      if (current != observed) return CompletableFuture.completedFuture(false);
      current = snapshot(replacement);
      return CompletableFuture.completedFuture(true);
    }

    private static ControlSnapshot snapshot(CommitState state) {
      return new ControlSnapshot(state, ByteBuffer.allocate(Long.BYTES).putLong(state.stateVersion()).array());
    }
  }
}
