package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.AuthSessionStore;
import io.etcdhbase.core.auth.AuthSlot;
import io.etcdhbase.core.auth.AuthToken;
import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.auth.CredentialEnvelopeContext;
import io.etcdhbase.core.auth.CredentialEnvelopeProtector;
import io.etcdhbase.core.auth.SubjectAuthority;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.connection.HBaseConnectionRouter;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.server.auth.ProviderSessionIdentityManager;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.identity.BackendIdentityException.Reason;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.junit.jupiter.api.Test;

class AuthenticatedRequestIdentityDrainTest {
  private static final String TABLE_GROUP = "auth-group";
  private static final String PROVIDER_ID = "provider";
  private static final String TRUST_DOMAIN = "test-domain";
  private static final String PRINCIPAL = "milvus-service";
  private static final AuthenticationScope SCOPE =
      new AuthenticationScope(TABLE_GROUP, "profile");
  private static final ByteKey DATASET_DIGEST = filledKey(32, (byte) 0x21);
  private static final Instant NOW = Instant.parse("2026-11-24T00:00:00Z");

  @Test
  void cancelledRequestRetainsProviderCapacityUntilExceptionalPhysicalDrain() throws Exception {
    var base = mock(HBaseConnectionFactory.class);
    var router = new HBaseConnectionRouter(base);
    var firstProvider = provider();
    var nextProvider = provider();
    var firstConnection = mock(AsyncConnection.class);
    var fallbackConnection = mock(AsyncConnection.class);
    var firstToken = AuthToken.generate(new SecureRandom());
    var firstSession = session(PRINCIPAL, firstToken.digest(), (byte) 1);
    var nextSession = session("milvus-service-2", filledKey(32, (byte) 0x42), (byte) 2);
    var identities = new ProviderSessionIdentityManager(
        new ProviderFactory(firstProvider, nextProvider), router, SCOPE, PROVIDER_ID, 1);
    var store = mock(AuthSessionStore.class);
    var protector = mock(CredentialEnvelopeProtector.class);
    var sessionService = new AuthSessionService(store, protector, identities, TRUST_DOMAIN,
        PROVIDER_ID, SCOPE, DATASET_DIGEST, Duration.ofMinutes(5), 1,
        Clock.fixed(NOW, ZoneOffset.UTC), new SecureRandom());
    var resolver = new AuthenticatedRequestIdentityResolver(TABLE_GROUP,
        identity("service", "service-context", 0), filledKey(32, (byte) 0x51), sessionService);
    var firstIdentity = identity(PRINCIPAL, "request-context-1", 3);
    var nextIdentity = identity("milvus-service-2", "request-context-2", 3);
    var firstEnvelope = firstSession.credentialEnvelope();
    var scope = RequestScope.withoutDeadline();
    var physicalProducer = new CompletableFuture<Void>();

    when(firstProvider.providerId()).thenReturn(PROVIDER_ID);
    when(nextProvider.providerId()).thenReturn(PROVIDER_ID);
    when(firstProvider.authenticate(any(), eq(SCOPE)))
        .thenReturn(CompletableFuture.completedFuture(firstIdentity));
    when(nextProvider.authenticate(any(), eq(SCOPE)))
        .thenReturn(CompletableFuture.completedFuture(nextIdentity));
    when(firstProvider.connectionFor(firstIdentity))
        .thenReturn(CompletableFuture.completedFuture(firstConnection));
    when(base.connectionFor(firstIdentity))
        .thenReturn(CompletableFuture.completedFuture(fallbackConnection));
    when(store.readSession(eq(firstToken.digest()), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(firstSession)));
    when(store.readAuthority(eq(firstSession.subjectDigest()), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(authority(firstSession))));
    when(store.readSlot(eq(firstSession.slotIndex()), any(RequestScope.class)))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(slot(firstSession))));
    when(protector.decrypt(eq(firstEnvelope), any(CredentialEnvelopeContext.class)))
        .thenAnswer(ignored -> encodedCredentials(PRINCIPAL));

    try (firstToken; var secondCredentials = credentials("milvus-service-2")) {
      scope.trackPhysicalProducer(physicalProducer);
      String encodedToken = firstToken.valueForResponse();
      var requestIdentity = resolver.resolve(Optional.of(encodedToken), scope).join();
      requestIdentity.releaseAfterPhysicalDrain(scope);

      var cleanup = scope.cleanupCompletion().toCompletableFuture();
      assertSame(firstConnection,
          router.connectionFor(requestIdentity.identity()).toCompletableFuture().join());
      assertTrue(scope.cancel());
      assertFalse(cleanup.isDone(), "scope cleanup must wait for the physical producer");
      verify(firstProvider, never()).close();

      var capacityFailure = assertThrows(CompletionException.class,
          () -> identities.activate(nextSession, secondCredentials).toCompletableFuture().join());
      assertEquals(Reason.RESOURCE_LIMIT,
          ((BackendIdentityException) capacityFailure.getCause()).reason());

      physicalProducer.completeExceptionally(new IllegalStateException("producer stopped"));
      cleanup.get(1, TimeUnit.SECONDS);
      verify(firstProvider).close();
      assertSame(fallbackConnection,
          router.connectionFor(requestIdentity.identity()).toCompletableFuture().join());

      var recovered = identities.activate(nextSession, secondCredentials)
          .toCompletableFuture().join();
      assertSame(nextIdentity, recovered.identity());
      recovered.release().toCompletableFuture().join();
      verify(nextProvider).close();
    } finally {
      if (!scope.isClosed()) scope.close();
      if (!physicalProducer.isDone()) physicalProducer.complete(null);
      scope.cleanupCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS);
      identities.close();
      router.close();
    }
  }

  private static HBaseBackendIdentityProvider provider() {
    return mock(HBaseBackendIdentityProvider.class);
  }

  private static AuthorizedBackendIdentity identity(
      String subject, String context, long generation) {
    return new AuthorizedBackendIdentity(PROVIDER_ID, subject, context, SCOPE,
        Set.of(TABLE_GROUP), generation);
  }

  private static AuthSession session(String subject, ByteKey tokenDigest, byte marker) {
    ByteKey subjectDigest = subjectDigest(subject);
    ByteKey sessionId = filledKey(16, marker);
    var envelope = new CredentialEnvelope("test-key", CredentialEnvelope.CURRENT_AAD_VERSION,
        new byte[CredentialEnvelope.NONCE_BYTES], new byte[CredentialEnvelope.TAG_BYTES]);
    return new AuthSession(sessionId, tokenDigest, subjectDigest, TRUST_DOMAIN, PROVIDER_ID, SCOPE,
        DATASET_DIGEST, 3, 1, NOW.toEpochMilli() - 1_000, NOW.toEpochMilli() + 60_000,
        marker - 1, 1, AuthSession.Status.ACTIVE, envelope);
  }

  private static SubjectAuthority authority(AuthSession session) {
    return new SubjectAuthority(session.subjectDigest(), TRUST_DOMAIN, PROVIDER_ID, SCOPE,
        session.credentialGeneration(), session.authorizationGeneration(),
        SubjectAuthority.State.ACTIVE);
  }

  private static AuthSlot slot(AuthSession session) {
    return new AuthSlot(session.slotIndex(), session.slotGeneration(), session.sessionId(),
        "A".repeat(22), AuthSlot.State.ACTIVE);
  }

  private static BackendCredentials credentials(String principal) {
    return new BackendCredentials(principal, new char[] {'p'});
  }

  private static byte[] encodedCredentials(String principal) {
    byte[] principalBytes = principal.getBytes(StandardCharsets.UTF_8);
    byte[] secretBytes = new byte[] {'p'};
    return ByteBuffer.allocate(Integer.BYTES * 3 + principalBytes.length + secretBytes.length)
        .putInt(1).putInt(principalBytes.length).put(principalBytes)
        .putInt(secretBytes.length).put(secretBytes).array();
  }

  private static ByteKey subjectDigest(String subject) {
    try {
      var output = new ByteArrayOutputStream();
      var data = new DataOutputStream(output);
      data.write("etcd-hbase-auth-subject-v1".getBytes(StandardCharsets.UTF_8));
      write(data, TRUST_DOMAIN);
      write(data, PROVIDER_ID);
      write(data, SCOPE.tableGroupId());
      write(data, SCOPE.connectionProfileId());
      write(data, subject);
      data.flush();
      return ByteKey.copyOf(MessageDigest.getInstance("SHA-256").digest(output.toByteArray()));
    } catch (IOException | NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private static void write(DataOutputStream output, String value) throws IOException {
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    output.writeInt(encoded.length);
    output.write(encoded);
  }

  private static ByteKey filledKey(int size, byte value) {
    byte[] bytes = new byte[size];
    java.util.Arrays.fill(bytes, value);
    return ByteKey.copyOf(bytes);
  }

  private static final class ProviderFactory
      implements java.util.function.Supplier<HBaseBackendIdentityProvider> {
    private final HBaseBackendIdentityProvider first;
    private final HBaseBackendIdentityProvider second;
    private int creations;

    private ProviderFactory(HBaseBackendIdentityProvider first,
        HBaseBackendIdentityProvider second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public synchronized HBaseBackendIdentityProvider get() {
      if (creations++ == 0) return first;
      if (creations == 2) return second;
      throw new IllegalStateException("test provider factory exhausted");
    }
  }
}
