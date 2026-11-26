package io.etcdhbase.server.auth;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.core.auth.AuthDeploymentRegistryStore;
import io.etcdhbase.core.auth.AuthorizationRevocation;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class DeploymentAuthorizationAdmissionTest {
  @Test
  void rejectsAllSubjectsWhileConfigurationBarriersAreInstalling() {
    var a = group("a", 1);
    var b = group("b", 2);
    var pending = AuthDeploymentRegistry.initial("trust", List.of(a, b))
        .beginConfigurationUpdate(List.of(a));
    try (var scope = RequestScope.withoutDeadline()) {
      assertThrows(CompletionException.class, () -> admission(new ReadOnlyStore(pending), a, a.datasetDigest())
          .check(bytes(7, 32), 1, scope).toCompletableFuture().join());
      assertThrows(CompletionException.class, () -> admission(new ReadOnlyStore(pending), b, b.datasetDigest())
          .check(bytes(8, 32), 1, scope).toCompletableFuture().join());
    }
  }

  @Test
  void blocksOnlyAffectedSubjectsAndRejectsStaleIncarnations() {
    var a = group("a", 1);
    var b = group("b", 2);
    var registry = AuthDeploymentRegistry.initial("trust", List.of(a, b));
    var operation = AuthorizationRevocation.start(bytes(9, 16), bytes(8, 32), "provider", "realm", 4, 2, 1,
        List.of(new AuthorizationRevocation.TargetIdentity("a", a.datasetDigest(), bytes(3, 32)),
            new AuthorizationRevocation.TargetIdentity("b", b.datasetDigest(), bytes(4, 32))));
    var store = new ReadOnlyStore(registry.beginRevocation(operation));
    var admissionA = admission(store, a, a.datasetDigest());
    var admissionB = admission(store, b, b.datasetDigest());
    try (var scope = RequestScope.withoutDeadline()) {
      assertThrows(CompletionException.class, () -> admissionA.check(bytes(3, 32), 1, scope).toCompletableFuture().join());
      assertThrows(CompletionException.class, () -> admissionB.check(bytes(4, 32), 1, scope).toCompletableFuture().join());
      admissionA.check(bytes(7, 32), 1, scope).toCompletableFuture().join();
      assertThrows(CompletionException.class,
          () -> admissionA.check(bytes(7, 32), 0, scope).toCompletableFuture().join());
      assertThrows(CompletionException.class,
          () -> admission(store, a, bytes(6, 32)).check(bytes(7, 32), 1, scope).toCompletableFuture().join());
    }
  }

  private static DeploymentAuthorizationAdmission admission(AuthDeploymentRegistryStore store,
      AuthDeploymentRegistry.Group group, ByteKey dataset) {
    return new DeploymentAuthorizationAdmission(store, "trust", "provider", "realm",
        new AuthenticationScope(group.tableGroupId(), "profile"), dataset, group.configurationDigest());
  }

  private static AuthDeploymentRegistry.Group group(String id, int value) {
    return new AuthDeploymentRegistry.Group(id, "provider", "realm", "profile", bytes(value, 32),
        bytes(value + 10, 32), true);
  }

  private record ReadOnlyStore(AuthDeploymentRegistry value) implements AuthDeploymentRegistryStore {
    @Override public CompletionStage<Optional<AuthDeploymentRegistry>> readRegistry(RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.of(value));
    }
    @Override public CompletionStage<Boolean> createRegistryIfAbsent(AuthDeploymentRegistry initial,
        RequestScope scope) { throw new UnsupportedOperationException(); }
    @Override public CompletionStage<Boolean> compareAndSetRegistry(AuthDeploymentRegistry observed,
        AuthDeploymentRegistry replacement, RequestScope scope) { throw new UnsupportedOperationException(); }
  }

  private static ByteKey bytes(int value, int length) {
    byte[] data = new byte[length];
    java.util.Arrays.fill(data, (byte) value);
    return ByteKey.copyOf(data);
  }
}
