package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.AuthorizationRevocation;
import io.etcdhbase.core.auth.RegistryAuthConfigurationStore;
import io.etcdhbase.core.auth.RegistryRevocationStore;
import io.etcdhbase.core.auth.AuthSlot;
import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.auth.SubjectAuthority;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.probe.TemporaryHBaseClusterProcess;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import io.etcdhbase.spi.salt.SaltProfile;
import java.util.List;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Verifies auth-control provisioning and durable CAS against a disposable HBase cluster. */
@Timeout(300)
class HBaseAuthControlIT {
  private static final String TRUST_DOMAIN = "milvus";
  private static final AuthenticationScope SCOPE = new AuthenticationScope("auth-control", "local");
  private static final HashSaltStrategy SALT = new HashSaltStrategy(
      new SaltProfile(HashSaltStrategy.STRATEGY_ID, 1, 4, 2));

  @Test
  void provisionsExactDescriptorAndPersistsSessionAuthorityAndSlotCas() throws Exception {
    try (var cluster = TemporaryHBaseClusterProcess.start();
        var provider = new HBaseBackendIdentityProvider("m5-auth-control", SCOPE,
            cluster.clientConfiguration(), HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            2, 2, Duration.ofSeconds(30));
        var credentials = new BackendCredentials("m5-auth-admin", new char[0])) {
      var operator = provider.authenticate(credentials, SCOPE).toCompletableFuture()
          .get(30, TimeUnit.SECONDS);
      var binding = new AuthControlBinding(
          TableName.valueOf("default", "m5_auth_control"), TRUST_DOMAIN, SALT);
      try (RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(60))) {
        HBaseAuthControlInitializer.initializeFresh(provider, operator, binding, scope)
            .get(30, TimeUnit.SECONDS);
        var store = HBaseAuthSessionStore.open(provider, operator, binding, scope)
            .get(30, TimeUnit.SECONDS);
        verifyRegistryCasAndProjections(store, scope);

        AuthSession session = session();
        assertTrue(store.createSessionIfAbsent(session, scope).toCompletableFuture()
            .get(10, TimeUnit.SECONDS));
        assertFalse(store.createSessionIfAbsent(session, scope).toCompletableFuture()
            .get(10, TimeUnit.SECONDS));
        assertEquals(session, store.readSession(session.tokenDigest(), scope).toCompletableFuture()
            .get(10, TimeUnit.SECONDS).orElseThrow());

        AuthSlot empty = new AuthSlot(0, 0, null, null, AuthSlot.State.EMPTY);
        assertTrue(store.createSlotIfAbsent(empty, scope).toCompletableFuture()
            .get(10, TimeUnit.SECONDS));
        AuthSlot reserved = new AuthSlot(0, 1, session.sessionId(),
            "abcdefghijklmnopqrstuv", AuthSlot.State.RESERVED);
        assertTrue(store.compareAndSetSlot(empty, reserved, scope).toCompletableFuture()
            .get(10, TimeUnit.SECONDS));
        AuthSlot active = new AuthSlot(0, 1, session.sessionId(),
            "abcdefghijklmnopqrstuv", AuthSlot.State.ACTIVE);
        assertTrue(store.compareAndSetSlot(reserved, active, scope).toCompletableFuture()
            .get(10, TimeUnit.SECONDS));
        assertEquals(active, store.readSlot(0, scope).toCompletableFuture()
            .get(10, TimeUnit.SECONDS).orElseThrow());

        SubjectAuthority authority = new SubjectAuthority(session.subjectDigest(), TRUST_DOMAIN,
            session.providerId(), SCOPE, session.credentialGeneration(),
            session.authorizationGeneration(), SubjectAuthority.State.ACTIVE);
        assertTrue(store.createAuthorityIfAbsent(authority, scope).toCompletableFuture()
            .get(10, TimeUnit.SECONDS));
        SubjectAuthority revoking = authority.beginRevocation(
            authority.credentialGeneration() + 1, authority.authorizationGeneration() + 1);
        assertTrue(store.compareAndSetAuthority(authority, revoking, scope).toCompletableFuture()
            .get(10, TimeUnit.SECONDS));
        SubjectAuthority targeted = revoking.assignRevocationBarrierTarget(1);
        assertTrue(store.compareAndSetAuthority(revoking, targeted, scope).toCompletableFuture()
            .get(10, TimeUnit.SECONDS));
        SubjectAuthority installed = targeted.markRevocationInstalled(SCOPE.tableGroupId(), 1);
        assertTrue(store.compareAndSetAuthority(targeted, installed, scope).toCompletableFuture()
            .get(10, TimeUnit.SECONDS));
        SubjectAuthority lowerGenerationTerminal = new SubjectAuthority(
            authority.subjectDigest(), TRUST_DOMAIN, authority.providerId(), SCOPE,
            authority.credentialGeneration(), authority.authorizationGeneration(),
            SubjectAuthority.State.REVOKED);
        ExecutionException rejectedTransition = assertThrows(ExecutionException.class,
            () -> store.compareAndSetAuthority(installed, lowerGenerationTerminal, scope)
                .toCompletableFuture().get(10, TimeUnit.SECONDS));
        assertEquals(IllegalArgumentException.class, rejectedTransition.getCause().getClass());
        assertEquals(installed, store.readAuthority(authority.subjectDigest(), scope)
            .toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow());
        SubjectAuthority revoked = installed.finishRevocation(SubjectAuthority.State.REVOKED);
        assertTrue(store.compareAndSetAuthority(installed, revoked, scope).toCompletableFuture()
            .get(10, TimeUnit.SECONDS));
        assertEquals(revoked, store.readAuthority(authority.subjectDigest(), scope)
            .toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow());

        AuthSession retiring = withStatus(session, AuthSession.Status.RETIRING);
        assertTrue(store.compareAndSetSession(session, retiring, scope).toCompletableFuture()
            .get(10, TimeUnit.SECONDS));
        assertTrue(store.deleteSessionIfUnchanged(retiring, scope).toCompletableFuture()
            .get(10, TimeUnit.SECONDS));
        assertTrue(store.readSession(session.tokenDigest(), scope).toCompletableFuture()
            .get(10, TimeUnit.SECONDS).isEmpty());
      }
    }
  }

  /** Exercises the storage primitive, not installation of a business commit barrier. */
  private static void verifyRegistryCasAndProjections(HBaseAuthSessionStore store,
      RequestScope scope) throws Exception {
    var digest = ByteKey.copyOf(new byte[32]);
    var group = new AuthDeploymentRegistry.Group(SCOPE.tableGroupId(), "hbase-provider", "realm",
        SCOPE.connectionProfileId(), digest, digest, true);
    var initial = AuthDeploymentRegistry.initial(TRUST_DOMAIN, List.of(group));
    assertTrue(store.createRegistryIfAbsent(initial, scope).toCompletableFuture().get(10, TimeUnit.SECONDS));
    assertFalse(store.createRegistryIfAbsent(initial, scope).toCompletableFuture().get(10, TimeUnit.SECONDS));
    assertEquals(initial, store.readRegistry(scope).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow());
    var configuration = new RegistryAuthConfigurationStore(store)
        .readConfiguration(SCOPE.tableGroupId(), scope).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
    assertEquals(initial.configurationGeneration(), configuration.revision());
    assertEquals(group.configurationDigest(), configuration.configurationDigest());
    assertTrue(configuration.enabled());
    var operation = AuthorizationRevocation.start(ByteKey.copyOf(new byte[16]), digest,
        group.providerId(), group.backendIdentityDomain(), 1, 2, 1,
        List.of(new AuthorizationRevocation.TargetIdentity(SCOPE.tableGroupId(), digest, digest)));
    var reserved = initial.beginRevocation(operation);
    assertTrue(store.compareAndSetRegistry(initial, reserved, scope).toCompletableFuture().get(10, TimeUnit.SECONDS));
    assertFalse(store.compareAndSetRegistry(initial, reserved, scope).toCompletableFuture().get(10, TimeUnit.SECONDS));
    assertEquals(reserved, store.readRegistry(scope).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow());
    assertEquals(operation, new RegistryRevocationStore(store).readRevocation(operation.operationId(), scope)
        .toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow());
  }

  private static AuthSession session() {
    long created = System.currentTimeMillis();
    var envelope = new CredentialEnvelope("test-key", CredentialEnvelope.CURRENT_AAD_VERSION,
        new byte[CredentialEnvelope.NONCE_BYTES], new byte[CredentialEnvelope.TAG_BYTES]);
    return new AuthSession(ByteKey.copyOf(new byte[16]), ByteKey.copyOf(new byte[32]),
        ByteKey.copyOf(new byte[32]), TRUST_DOMAIN, "hbase-provider", SCOPE,
        ByteKey.copyOf(new byte[32]), 1, 1, created, created + 60_000, 0, 1,
        AuthSession.Status.ACTIVE, envelope);
  }

  private static AuthSession withStatus(AuthSession session, AuthSession.Status status) {
    return new AuthSession(session.sessionId(), session.tokenDigest(), session.subjectDigest(),
        session.trustDomain(), session.providerId(), session.scope(), session.datasetDigest(),
        session.credentialGeneration(), session.authorizationGeneration(),
        session.createdAtEpochMillis(), session.expiresAtEpochMillis(), session.slotIndex(),
        session.slotGeneration(), status, session.credentialEnvelope());
  }
}
