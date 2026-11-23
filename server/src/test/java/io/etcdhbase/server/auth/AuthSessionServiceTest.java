package io.etcdhbase.server.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.auth.AuthSessionStore;
import io.etcdhbase.core.auth.AuthSlot;
import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.auth.SubjectAuthority;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AuthSessionServiceTest {
  private static final AuthenticationScope SCOPE = new AuthenticationScope("group", "profile");
  private static final ByteKey DATASET = key(32, 9);

  @TempDir Path temporary;

  @Test
  void authenticatesPersistsOnlyDigestAndEnvelopeAndResolvesOnAnotherReplica() throws Exception {
    FakeStore store = new FakeStore();
    try (var keys = keyProvider("key-1", temporary.resolve("key.bin"))) {
      var protector = new AesGcmCredentialEnvelopeProtector(keys, new SecureRandom());
      FakeIdentityManager firstIdentities = new FakeIdentityManager();
      AuthSessionService firstReplica = service(store, protector, firstIdentities, 2);
      AuthSessionService.IssuedToken issued;
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3));
          var credentials = credentials()) {
        issued = firstReplica.authenticate(credentials, request).toCompletableFuture()
            .get(3, java.util.concurrent.TimeUnit.SECONDS);
      }

      assertEquals(1, store.sessions.size());
      AuthSession persisted = store.sessions.values().iterator().next();
      assertEquals(AuthSession.Status.ACTIVE, persisted.status());
      assertEquals(AuthSlot.State.ACTIVE, store.slots.get(persisted.slotIndex()).state());
      assertFalse(persisted.credentialEnvelope().toString().contains("password"));
      assertEquals(1, firstIdentities.restorableVerifications);

      FakeIdentityManager secondIdentities = new FakeIdentityManager();
      AuthSessionService secondReplica = service(store, protector, secondIdentities, 2);
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3))) {
        var resolved = secondReplica.resolve(issued.tokenForResponse(), request)
            .toCompletableFuture().get(3, java.util.concurrent.TimeUnit.SECONDS);
        assertEquals("milvus-service", resolved.identity().subject());
        assertEquals(persisted.subjectDigest(), resolved.session().subjectDigest());
        resolved.release().toCompletableFuture().get(3, java.util.concurrent.TimeUnit.SECONDS);
        resolved.release().toCompletableFuture().get(3, java.util.concurrent.TimeUnit.SECONDS);
      }
      assertEquals(1, secondIdentities.activations);
      assertEquals(1, secondIdentities.releasedIdentityLeases);
      assertEquals(0, secondIdentities.providerCloses,
          "releasing one request identity must not close the shared provider");
      assertTrue(issued.toString().contains("redacted"));
    }
  }

  @Test
  void globallyBoundedSlotsRejectASecondReplicaAndBackendFailureReleasesReservation()
      throws Exception {
    FakeStore store = new FakeStore();
    try (var keys = keyProvider("key-1", temporary.resolve("key.bin"))) {
      var protector = new AesGcmCredentialEnvelopeProtector(keys, new SecureRandom());
      AuthSessionService first = service(store, new FakeIdentityManager(), protector, 1);
      AuthSessionService second = service(store, new FakeIdentityManager(), protector, 1);
      AuthSessionService.IssuedToken issued;
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3));
          var credentials = credentials()) {
        issued = first.authenticate(credentials, request).toCompletableFuture()
            .get(3, java.util.concurrent.TimeUnit.SECONDS);
      }
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3));
          var credentials = credentials()) {
        var rejected = second.authenticate(credentials, request).toCompletableFuture();
        var failure = assertThrows(java.util.concurrent.ExecutionException.class,
            () -> rejected.get(3, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(AuthSessionException.Reason.RESOURCE_LIMIT,
            ((AuthSessionException) failure.getCause()).reason());
      }
      assertEquals(1, store.slots.size());
      assertEquals(AuthSlot.State.ACTIVE, store.slots.get(0).state());

      // Expire/retire the existing slot so an invalid login can reserve it and prove cleanup.
      AuthSlot active = store.slots.get(0);
      AuthSlot retiring = new AuthSlot(0, active.generation(), active.sessionId(),
          active.operationId(), AuthSlot.State.RETIRING);
      store.slots.put(0, retiring);
      store.sessions.clear();
      store.slots.put(0, new AuthSlot(0, retiring.generation(), null, null, AuthSlot.State.EMPTY));

      FakeIdentityManager bad = new FakeIdentityManager();
      bad.rejectCredentials = true;
      AuthSessionService invalidLogin = service(store, bad, protector, 1);
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3));
          var credentials = credentials()) {
        var failure = assertThrows(java.util.concurrent.ExecutionException.class,
            () -> invalidLogin.authenticate(credentials, request).toCompletableFuture()
                .get(3, java.util.concurrent.TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof AuthSessionException);
      }
      assertEquals(AuthSlot.State.EMPTY, store.slots.get(0).state());
      assertFalse(store.slots.get(0).sessionId() != null);
      assertTrue(issued.tokenForResponse().length() == 43);
    }
  }

  @Test
  void undeliveredTokenCleanupRetiresOnlyItsSessionAndSlotGeneration() throws Exception {
    FakeStore store = new FakeStore();
    try (var keys = keyProvider("key-1", temporary.resolve("key.bin"))) {
      var protector = new AesGcmCredentialEnvelopeProtector(keys, new SecureRandom());
      AuthSessionService auth = service(store, protector, new FakeIdentityManager(), 1);
      AuthSessionService.IssuedToken first;
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3));
          var credentials = credentials()) {
        first = auth.authenticate(credentials, request).toCompletableFuture()
            .get(3, java.util.concurrent.TimeUnit.SECONDS);
      }
      AuthSlot firstSlot = store.slots.get(0);
      assertEquals(AuthSlot.State.ACTIVE, firstSlot.state());
      assertEquals(1, store.sessions.size());

      auth.discardUndelivered(first).toCompletableFuture()
          .get(3, java.util.concurrent.TimeUnit.SECONDS);
      assertTrue(store.sessions.isEmpty());
      assertEquals(AuthSlot.State.EMPTY, store.slots.get(0).state());
      assertEquals(firstSlot.generation(), store.slots.get(0).generation());

      AuthSessionService.IssuedToken second;
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3));
          var credentials = credentials()) {
        second = auth.authenticate(credentials, request).toCompletableFuture()
            .get(3, java.util.concurrent.TimeUnit.SECONDS);
      }
      AuthSlot secondSlot = store.slots.get(0);
      assertEquals(AuthSlot.State.ACTIVE, secondSlot.state());
      assertTrue(secondSlot.generation() > firstSlot.generation());
      AuthSession secondSession = store.sessions.values().iterator().next();

      auth.discardUndelivered(first).toCompletableFuture()
          .get(3, java.util.concurrent.TimeUnit.SECONDS);
      assertEquals(AuthSlot.State.ACTIVE, store.slots.get(0).state());
      assertEquals(secondSlot.generation(), store.slots.get(0).generation());
      assertEquals(1, store.sessions.size());
      assertEquals(secondSession, store.sessions.values().iterator().next());

      auth.discardUndelivered(second).toCompletableFuture()
          .get(3, java.util.concurrent.TimeUnit.SECONDS);
      assertTrue(store.sessions.isEmpty());
      assertEquals(AuthSlot.State.EMPTY, store.slots.get(0).state());
      assertEquals(secondSlot.generation(), store.slots.get(0).generation());
    }
  }

  @Test
  void undeliveredCleanupRetainsTheSlotWhenTheSessionIdentityIsUncertain() throws Exception {
    FakeStore store = new FakeStore();
    try (var keys = keyProvider("key-1", temporary.resolve("key.bin"))) {
      var protector = new AesGcmCredentialEnvelopeProtector(keys, new SecureRandom());
      AuthSessionService auth = service(store, protector, new FakeIdentityManager(), 1);
      AuthSessionService.IssuedToken issued;
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3));
          var credentials = credentials()) {
        issued = auth.authenticate(credentials, request).toCompletableFuture()
            .get(3, java.util.concurrent.TimeUnit.SECONDS);
      }

      AuthSession active = store.sessions.values().iterator().next();
      AuthSession mismatched = new AuthSession(key(16, 0), active.tokenDigest(),
          active.subjectDigest(), active.trustDomain(), active.providerId(), active.scope(),
          active.datasetDigest(), active.credentialGeneration(), active.authorizationGeneration(),
          active.createdAtEpochMillis(), active.expiresAtEpochMillis(), active.slotIndex(),
          active.slotGeneration(), active.status(), active.credentialEnvelope());
      store.sessions.put(active.tokenDigest(), mismatched);

      var failure = assertThrows(java.util.concurrent.ExecutionException.class,
          () -> auth.discardUndelivered(issued).toCompletableFuture()
              .get(3, java.util.concurrent.TimeUnit.SECONDS));
      assertEquals(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE,
          ((AuthSessionException) failure.getCause()).reason());
      assertEquals(AuthSlot.State.RETIRING, store.slots.get(0).state());
      assertEquals(mismatched, store.sessions.get(active.tokenDigest()));
    }
  }

  @Test
  void unknownSessionWriteOutcomeKeepsTheGlobalSlotReserved() throws Exception {
    FakeStore store = new FakeStore();
    store.failSessionCreateWithoutWriting = true;
    try (var keys = keyProvider("key-1", temporary.resolve("key.bin"))) {
      var protector = new AesGcmCredentialEnvelopeProtector(keys, new SecureRandom());
      AuthSessionService auth = service(store, protector, new FakeIdentityManager(), 1);
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3));
          var credentials = credentials()) {
        var failure = assertThrows(java.util.concurrent.ExecutionException.class,
            () -> auth.authenticate(credentials, request).toCompletableFuture()
                .get(3, java.util.concurrent.TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof AuthSessionException);
      }
      assertEquals(AuthSlot.State.RESERVED, store.slots.get(0).state());
      assertTrue(store.sessions.isEmpty());
    }
  }

  @Test
  void revokedAuthorityRejectsAnOtherwiseActiveToken() throws Exception {
    FakeStore store = new FakeStore();
    try (var keys = keyProvider("key-1", temporary.resolve("key.bin"))) {
      var protector = new AesGcmCredentialEnvelopeProtector(keys, new SecureRandom());
      AuthSessionService auth = service(store, protector, new FakeIdentityManager(), 2);
      AuthSessionService.IssuedToken issued;
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3));
          var credentials = credentials()) {
        issued = auth.authenticate(credentials, request).toCompletableFuture()
            .get(3, java.util.concurrent.TimeUnit.SECONDS);
      }
      SubjectAuthority authority = store.authorities.values().iterator().next();
      store.authorities.put(authority.subjectDigest(), new SubjectAuthority(authority.subjectDigest(),
          authority.trustDomain(), authority.providerId(), authority.scope(),
          authority.credentialGeneration(), authority.authorizationGeneration() + 1,
          SubjectAuthority.State.REVOKED));
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3))) {
        var failure = assertThrows(java.util.concurrent.ExecutionException.class,
            () -> auth.resolve(issued.tokenForResponse(), request).toCompletableFuture()
                .get(3, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(AuthSessionException.Reason.INVALID_TOKEN,
            ((AuthSessionException) failure.getCause()).reason());
      }
    }
  }

  @Test
  void activationIsDiscardedWhenAuthorityChangesBeforeIdentityIsPublished() throws Exception {
    FakeStore store = new FakeStore();
    try (var keys = keyProvider("key-1", temporary.resolve("key.bin"))) {
      var protector = new AesGcmCredentialEnvelopeProtector(keys, new SecureRandom());
      AuthSessionService firstReplica = service(store, protector, new FakeIdentityManager(), 2);
      AuthSessionService.IssuedToken issued;
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3));
          var credentials = credentials()) {
        issued = firstReplica.authenticate(credentials, request).toCompletableFuture()
            .get(3, java.util.concurrent.TimeUnit.SECONDS);
      }

      AuthSession session = store.sessions.values().iterator().next();
      SubjectAuthority authority = store.authorities.get(session.subjectDigest());
      FakeIdentityManager activating = new FakeIdentityManager();
      activating.activation = new CompletableFuture<>();
      AuthSessionService secondReplica = service(store, protector, activating, 2);
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3))) {
        var resolution = secondReplica.resolve(issued.tokenForResponse(), request).toCompletableFuture();
        assertEquals(1, activating.activations);

        store.authorities.put(authority.subjectDigest(), new SubjectAuthority(
            authority.subjectDigest(), authority.trustDomain(), authority.providerId(), authority.scope(),
            authority.credentialGeneration(), authority.authorizationGeneration() + 1,
            SubjectAuthority.State.REVOKED));
        activating.activation.complete(
            new SessionIdentityLease(FakeIdentityManager.identity("milvus-service", session.scope()),
                () -> {
                  activating.releasedIdentityLeases++;
                  return CompletableFuture.completedFuture(null);
                }));

        var failure = assertThrows(java.util.concurrent.ExecutionException.class,
            () -> resolution.get(3, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(AuthSessionException.Reason.INVALID_TOKEN,
            ((AuthSessionException) failure.getCause()).reason());
        assertEquals(1, activating.releasedIdentityLeases);
      }
    }
  }

  @Test
  void activationFailureDoesNotReleaseOrCloseAnotherProviderOwnedContext() throws Exception {
    FakeStore store = new FakeStore();
    try (var keys = keyProvider("key-1", temporary.resolve("key.bin"))) {
      var protector = new AesGcmCredentialEnvelopeProtector(keys, new SecureRandom());
      FakeIdentityManager identities = new FakeIdentityManager();
      AuthorizedBackendIdentity otherIdentity = FakeIdentityManager.identity("other", SCOPE);
      identities.liveIdentity = otherIdentity;
      identities.activation = CompletableFuture.failedFuture(new BackendIdentityException(
          BackendIdentityException.Reason.BACKEND_UNAVAILABLE));
      AuthSessionService auth = service(store, protector, identities, 1);
      AuthSessionService.IssuedToken issued;
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3));
          var credentials = credentials()) {
        issued = auth.authenticate(credentials, request).toCompletableFuture()
            .get(3, java.util.concurrent.TimeUnit.SECONDS);
      }
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3))) {
        var failure = assertThrows(java.util.concurrent.ExecutionException.class,
            () -> auth.resolve(issued.tokenForResponse(), request).toCompletableFuture()
                .get(3, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE,
            ((AuthSessionException) failure.getCause()).reason());
      }
      assertEquals(0, identities.releasedIdentityLeases);
      assertEquals(0, identities.providerCloses);
      assertEquals(otherIdentity, identities.liveIdentity,
          "a failed activation must not retire a different identity");
    }
  }

  @Test
  void cancelledRequestReleasesIdentityThatActivatesLate() throws Exception {
    FakeStore store = new FakeStore();
    try (var keys = keyProvider("key-1", temporary.resolve("key.bin"))) {
      var protector = new AesGcmCredentialEnvelopeProtector(keys, new SecureRandom());
      FakeIdentityManager firstIdentities = new FakeIdentityManager();
      AuthSessionService first = service(store, protector, firstIdentities, 1);
      AuthSessionService.IssuedToken issued;
      try (RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3));
          var credentials = credentials()) {
        issued = first.authenticate(credentials, request).toCompletableFuture()
            .get(3, java.util.concurrent.TimeUnit.SECONDS);
      }

      FakeIdentityManager activating = new FakeIdentityManager();
      activating.activation = new CompletableFuture<>();
      AuthSessionService second = service(store, protector, activating, 1);
      RequestScope request = RequestScope.withTimeout(Duration.ofSeconds(3));
      var resolution = second.resolve(issued.tokenForResponse(), request).toCompletableFuture();
      request.cancel();
      AuthSession persisted = store.sessions.values().iterator().next();
      activating.activation.complete(new SessionIdentityLease(
          FakeIdentityManager.identity("milvus-service", persisted.scope()), () -> {
            activating.releasedIdentityLeases++;
            return CompletableFuture.completedFuture(null);
          }));

      var failure = assertThrows(java.util.concurrent.ExecutionException.class,
          () -> resolution.get(3, java.util.concurrent.TimeUnit.SECONDS));
      assertEquals(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE,
          ((AuthSessionException) failure.getCause()).reason());
      assertEquals(1, activating.releasedIdentityLeases);
      assertEquals(0, activating.providerCloses);
    }
  }

  private AuthSessionService service(FakeStore store, AesGcmCredentialEnvelopeProtector protector,
      FakeIdentityManager identities, int slots) {
    return service(store, identities, protector, slots);
  }

  private AuthSessionService service(FakeStore store, FakeIdentityManager identities,
      AesGcmCredentialEnvelopeProtector protector, int slots) {
    return new AuthSessionService(store, protector, identities, "milvus", "hbase-provider", SCOPE,
        DATASET, Duration.ofMinutes(5), slots, Clock.systemUTC(), new SecureRandom());
  }

  private FileCredentialKeyProvider keyProvider(String id, Path path) throws Exception {
    byte[] key = new byte[32];
    Arrays.fill(key, (byte) 0x5a);
    Files.write(path, key);
    Files.setPosixFilePermissions(path, Set.of(
        java.nio.file.attribute.PosixFilePermission.OWNER_READ,
        java.nio.file.attribute.PosixFilePermission.OWNER_WRITE));
    Arrays.fill(key, (byte) 0);
    return new FileCredentialKeyProvider(id, Map.of(id, path));
  }

  private static BackendCredentials credentials() {
    return new BackendCredentials("milvus-service", "password-1".toCharArray());
  }

  private static ByteKey key(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }

  private static final class FakeIdentityManager implements SessionIdentityManager {
    private int restorableVerifications;
    private int activations;
    private int discardedIdentities;
    private boolean rejectCredentials;
    private CompletableFuture<SessionIdentityLease> activation;
    private int releasedIdentityLeases;
    private int providerCloses;
    private AuthorizedBackendIdentity liveIdentity;

    @Override
    public CompletionStage<AuthorizedBackendIdentity> authenticate(
        BackendCredentials credentials, AuthenticationScope scope) {
      if (rejectCredentials) return CompletableFuture.failedFuture(new BackendIdentityException(
          BackendIdentityException.Reason.INVALID_CREDENTIALS));
      return CompletableFuture.completedFuture(identity(credentials.principal(), scope));
    }

    @Override
    public CompletionStage<Void> verifyRestorable(
        BackendCredentials credentials, AuthorizedBackendIdentity authenticatedIdentity) {
      restorableVerifications++;
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<SessionIdentityLease> activate(
        AuthSession session, BackendCredentials credentials) {
      activations++;
      if (activation != null) return activation;
      AuthorizedBackendIdentity activated = identity(credentials.principal(), session.scope());
      liveIdentity = activated;
      return CompletableFuture.completedFuture(new SessionIdentityLease(activated, () -> {
        releasedIdentityLeases++;
        if (liveIdentity == activated) liveIdentity = null;
        return CompletableFuture.completedFuture(null);
      }));
    }

    @Override
    public CompletionStage<Void> discardUnpublished(AuthorizedBackendIdentity identity) {
      discardedIdentities++;
      return CompletableFuture.completedFuture(null);
    }

    @Override public void close() { providerCloses++; }

    private static AuthorizedBackendIdentity identity(String subject, AuthenticationScope scope) {
      return new AuthorizedBackendIdentity("hbase-provider", subject, "opaque-context",
          scope, Set.of(scope.tableGroupId()), 3);
    }
  }

  private static final class FakeStore implements AuthSessionStore {
    private final Map<ByteKey, AuthSession> sessions = new HashMap<>();
    private final Map<Integer, AuthSlot> slots = new HashMap<>();
    private final Map<ByteKey, SubjectAuthority> authorities = new HashMap<>();
    private boolean failSessionCreateWithoutWriting;

    @Override public synchronized CompletionStage<Optional<AuthSession>> readSession(
        ByteKey digest, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(sessions.get(digest)));
    }
    @Override public synchronized CompletionStage<Boolean> createSessionIfAbsent(
        AuthSession session, RequestScope scope) {
      if (failSessionCreateWithoutWriting) {
        failSessionCreateWithoutWriting = false;
        return CompletableFuture.failedFuture(new IllegalStateException("simulated unknown write"));
      }
      if (sessions.containsKey(session.tokenDigest())) return CompletableFuture.completedFuture(false);
      sessions.put(session.tokenDigest(), session);
      return CompletableFuture.completedFuture(true);
    }
    @Override public synchronized CompletionStage<Boolean> deleteSessionIfUnchanged(
        AuthSession observed, RequestScope scope) {
      return CompletableFuture.completedFuture(sessions.remove(observed.tokenDigest(), observed));
    }
    @Override public synchronized CompletionStage<Boolean> compareAndSetSession(
        AuthSession observed, AuthSession replacement, RequestScope scope) {
      if (!sessions.getOrDefault(observed.tokenDigest(), observed).equals(observed)
          || !sessions.containsKey(observed.tokenDigest())) return CompletableFuture.completedFuture(false);
      sessions.put(observed.tokenDigest(), replacement);
      return CompletableFuture.completedFuture(true);
    }
    @Override public synchronized CompletionStage<Optional<AuthSlot>> readSlot(
        int index, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(slots.get(index)));
    }
    @Override public synchronized CompletionStage<Boolean> createSlotIfAbsent(
        AuthSlot slot, RequestScope scope) {
      if (slots.containsKey(slot.index())) return CompletableFuture.completedFuture(false);
      slots.put(slot.index(), slot);
      return CompletableFuture.completedFuture(true);
    }
    @Override public synchronized CompletionStage<Boolean> compareAndSetSlot(
        AuthSlot observed, AuthSlot replacement, RequestScope scope) {
      if (!observed.equals(slots.get(observed.index()))) return CompletableFuture.completedFuture(false);
      slots.put(observed.index(), replacement);
      return CompletableFuture.completedFuture(true);
    }
    @Override public synchronized CompletionStage<Optional<SubjectAuthority>> readAuthority(
        ByteKey subject, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(authorities.get(subject)));
    }
    @Override public synchronized CompletionStage<Boolean> createAuthorityIfAbsent(
        SubjectAuthority authority, RequestScope scope) {
      if (authorities.containsKey(authority.subjectDigest())) return CompletableFuture.completedFuture(false);
      authorities.put(authority.subjectDigest(), authority);
      return CompletableFuture.completedFuture(true);
    }
    @Override public synchronized CompletionStage<Boolean> compareAndSetAuthority(
        SubjectAuthority observed, SubjectAuthority replacement, RequestScope scope) {
      if (!observed.equals(authorities.get(observed.subjectDigest()))) {
        return CompletableFuture.completedFuture(false);
      }
      authorities.put(observed.subjectDigest(), replacement);
      return CompletableFuture.completedFuture(true);
    }
  }
}
