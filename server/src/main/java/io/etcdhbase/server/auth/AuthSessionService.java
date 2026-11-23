package io.etcdhbase.server.auth;

import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.auth.AuthSessionStore;
import io.etcdhbase.core.auth.AuthSlot;
import io.etcdhbase.core.auth.AuthToken;
import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.auth.CredentialEnvelopeContext;
import io.etcdhbase.core.auth.CredentialEnvelopeProtector;
import io.etcdhbase.core.auth.SubjectAuthority;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/** Persistent Authenticate/session service; the returned token is never written to auth-control. */
public final class AuthSessionService {
  public static final int MAX_SESSION_SLOTS = 1024;
  public static final int MAX_PRINCIPAL_CODE_POINTS = 256;
  public static final int MAX_SECRET_CHARACTERS = CredentialMaterialCodec.MAX_SECRET_CHARS;
  private static final int MAX_CAS_ATTEMPTS = 8;
  private static final int MAX_TOKEN_COLLISIONS = 4;
  private static final int SESSION_ID_BYTES = 16;
  private static final int OPERATION_ID_BYTES = 16;

  private final AuthSessionStore store;
  private final CredentialEnvelopeProtector protector;
  private final SessionIdentityManager identities;
  private final String trustDomain;
  private final String providerId;
  private final AuthenticationScope scope;
  private final ByteKey datasetDigest;
  private final Duration sessionTtl;
  private final int sessionSlots;
  private final Clock clock;
  private final SecureRandom random;

  public AuthSessionService(AuthSessionStore store, CredentialEnvelopeProtector protector,
      SessionIdentityManager identities, String trustDomain, String providerId,
      AuthenticationScope scope, ByteKey datasetDigest, Duration sessionTtl, int sessionSlots,
      Clock clock, SecureRandom random) {
    this.store = Objects.requireNonNull(store, "store");
    this.protector = Objects.requireNonNull(protector, "protector");
    this.identities = Objects.requireNonNull(identities, "identities");
    this.trustDomain = requireId(trustDomain, "trustDomain");
    this.providerId = requireId(providerId, "providerId");
    this.scope = Objects.requireNonNull(scope, "scope");
    this.datasetDigest = requireDigest(datasetDigest, "datasetDigest");
    this.sessionTtl = Objects.requireNonNull(sessionTtl, "sessionTtl");
    if (sessionTtl.compareTo(Duration.ofSeconds(1)) < 0
        || sessionTtl.compareTo(Duration.ofDays(30)) > 0
        || sessionSlots < 1 || sessionSlots > MAX_SESSION_SLOTS) {
      throw new IllegalArgumentException("authentication session limits are outside their range");
    }
    this.sessionSlots = sessionSlots;
    this.clock = Objects.requireNonNull(clock, "clock");
    this.random = Objects.requireNonNull(random, "random");
  }

  /** Reserves persistent capacity before performing any backend authentication work. */
  public CompletionStage<IssuedToken> authenticate(
      BackendCredentials credentials, RequestScope requestScope) {
    Objects.requireNonNull(credentials, "credentials");
    Objects.requireNonNull(requestScope, "requestScope");
    try {
      validateCredentials(credentials);
    } catch (RuntimeException invalid) {
      return CompletableFuture.failedFuture(invalid);
    }
    return reserveSlot(requestScope).thenCompose(reservation ->
        authenticateReserved(credentials, requestScope, reservation));
  }

  /**
   * Retires a successfully created session whose token was not handed to the RPC response.
   * The exact session and slot generation are carried privately by the issued token; a stale
   * cleanup therefore cannot release a later login that reused the slot.
   */
  public CompletionStage<Void> discardUndelivered(IssuedToken issued) {
    Objects.requireNonNull(issued, "issued");
    if (issued.owner != this) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "issued token belongs to another authentication service"));
    }
    RequestScope cleanupScope = RequestScope.withTimeout(Duration.ofSeconds(10));
    CompletionStage<Void> cleanup;
    try {
      cleanup = cleanupUndelivered(issued.session, new Reservation(issued.slot), cleanupScope);
    } catch (RuntimeException failure) {
      cleanupScope.close();
      return CompletableFuture.failedFuture(failure);
    } catch (Error fatal) {
      cleanupScope.close();
      throw fatal;
    }
    return cleanup.whenComplete((ignored, failure) -> cleanupScope.close());
  }

  /** Strongly checks session, authority and slot generations before local identity activation. */
  public CompletionStage<ResolvedIdentity> resolve(String encodedToken, RequestScope requestScope) {
    Objects.requireNonNull(requestScope, "requestScope");
    AuthToken token;
    try {
      token = AuthToken.parse(encodedToken);
    } catch (RuntimeException malformed) {
      return CompletableFuture.failedFuture(new AuthSessionException(
          AuthSessionException.Reason.INVALID_TOKEN));
    }
    ByteKey digest;
    try (token) {
      digest = token.digest();
    }
    return resolveByDigest(digest, requestScope);
  }

  /**
   * Resolves a session from a trusted internal reference after the receiver authenticates its
   * mTLS peer. The reference is not a client bearer token; the durable session fences still apply.
   */
  public CompletionStage<ResolvedIdentity> resolveByDigest(
      ByteKey digest, RequestScope requestScope) {
    Objects.requireNonNull(digest, "digest");
    Objects.requireNonNull(requestScope, "requestScope");
    if (digest.length() != AuthToken.DIGEST_BYTES) {
      return CompletableFuture.failedFuture(invalidToken());
    }
    return store.readSession(digest, requestScope).thenCompose(found -> {
      AuthSession session = found.orElseThrow(() -> invalidToken());
      long now = clock.millis();
      if (!session.trustDomain().equals(trustDomain) || !session.providerId().equals(providerId)
          || !session.scope().equals(scope) || !session.datasetDigest().equals(datasetDigest)
          || !session.isActiveAt(now)) return CompletableFuture.failedFuture(invalidToken());
      return store.readAuthority(session.subjectDigest(), requestScope).thenCombine(
          store.readSlot(session.slotIndex(), requestScope), (authority, slot) -> {
            SubjectAuthority current = authority.orElseThrow(AuthSessionService::invalidToken);
            AuthSlot currentSlot = slot.orElseThrow(AuthSessionService::invalidToken);
            if (!current.trustDomain().equals(trustDomain)
                || !current.providerId().equals(providerId) || !current.scope().equals(scope)
                || current.state() != SubjectAuthority.State.ACTIVE
                || current.credentialGeneration() != session.credentialGeneration()
                || current.authorizationGeneration() != session.authorizationGeneration()
                || currentSlot.state() != AuthSlot.State.ACTIVE
                || !session.sessionId().equals(currentSlot.sessionId())
                || session.slotGeneration() != currentSlot.generation()) {
              throw invalidToken();
            }
            return session;
          }).thenCompose(checked -> activate(checked, requestScope));
    });
  }

  private CompletionStage<ResolvedIdentity> activate(AuthSession session, RequestScope requestScope) {
    CredentialEnvelopeContext context = envelopeContext(session.sessionId(),
        session.credentialGeneration(), session.expiresAtEpochMillis());
    byte[] plaintext = protector.decrypt(session.credentialEnvelope(), context);
    BackendCredentials credentials;
    try {
      credentials = CredentialMaterialCodec.decode(plaintext);
    } catch (RuntimeException malformed) {
      Arrays.fill(plaintext, (byte) 0);
      return CompletableFuture.failedFuture(new AuthSessionException(
          AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
    }
    Arrays.fill(plaintext, (byte) 0);
    CompletionStage<SessionIdentityLease> activated;
    try {
      activated = identities.activate(session, credentials);
    } catch (RuntimeException failure) {
      credentials.close();
      return CompletableFuture.failedFuture(failure);
    }
    return activated.handle((identity, failure) -> {
      credentials.close();
      if (failure != null) {
        Throwable cause = unwrap(failure);
        if (cause instanceof Error fatal) throw fatal;
        throw new CompletionException(mapIdentityFailure(cause));
      }
      if (!identityMatches(identity.identity(), session)) {
        return discardResolvedIdentity(identity, new AuthSessionException(
            AuthSessionException.Reason.PERMISSION_DENIED));
      }
      return verifyResolvedSessionStillActive(session, requestScope)
          .handle((ignored, verificationFailure) -> verificationFailure == null
              ? CompletableFuture.completedFuture(new ResolvedIdentity(session, identity))
              : discardResolvedIdentity(identity, mapIdentityFailure(unwrap(verificationFailure))))
          .thenCompose(stage -> stage);
    }).thenCompose(stage -> stage);
  }

  /** Rechecks persistent fences after asynchronous activation, before exposing the local identity. */
  private CompletionStage<Void> verifyResolvedSessionStillActive(
      AuthSession expected, RequestScope requestScope) {
    if (requestScope.isClosed() || requestScope.isDeadlineExceeded()) {
      return CompletableFuture.failedFuture(new AuthSessionException(
          AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
    }
    return store.readSession(expected.tokenDigest(), requestScope).thenCombine(
        store.readAuthority(expected.subjectDigest(), requestScope), (session, authority) ->
            new ActiveSessionState(session, authority))
        .thenCombine(store.readSlot(expected.slotIndex(), requestScope), (state, slot) -> {
          long now = clock.millis();
          AuthSession currentSession = state.session().orElseThrow(AuthSessionService::invalidToken);
          SubjectAuthority currentAuthority = state.authority()
              .orElseThrow(AuthSessionService::invalidToken);
          AuthSlot currentSlot = slot.orElseThrow(AuthSessionService::invalidToken);
          if (requestScope.isClosed() || requestScope.isDeadlineExceeded()
              || !currentSession.equals(expected)
              || !currentSession.isActiveAt(now)
              || currentAuthority.state() != SubjectAuthority.State.ACTIVE
              || !currentAuthority.trustDomain().equals(trustDomain)
              || !currentAuthority.providerId().equals(providerId)
              || !currentAuthority.scope().equals(scope)
              || currentAuthority.credentialGeneration() != expected.credentialGeneration()
              || currentAuthority.authorizationGeneration() != expected.authorizationGeneration()
              || currentSlot.state() != AuthSlot.State.ACTIVE
              || currentSlot.generation() != expected.slotGeneration()
              || !expected.sessionId().equals(currentSlot.sessionId())) {
            throw invalidToken();
          }
          return null;
        });
  }

  /** An activated handle is not published to the request until all durable fences are rechecked. */
  private CompletionStage<ResolvedIdentity> discardResolvedIdentity(
      SessionIdentityLease lease, RuntimeException failure) {
    if (lease == null) return CompletableFuture.failedFuture(failure);
    CompletionStage<Void> released;
    try {
      released = lease.release();
    } catch (RuntimeException cleanupFailure) {
      failure.addSuppressed(cleanupFailure);
      return CompletableFuture.failedFuture(new AuthSessionException(
          AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
    }
    return released.handle((ignored, cleanupFailure) -> {
      if (cleanupFailure != null) {
        AuthSessionException unavailable = new AuthSessionException(
            AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE);
        unavailable.addSuppressed(failure);
        unavailable.addSuppressed(unwrap(cleanupFailure));
        return CompletableFuture.<ResolvedIdentity>failedFuture(unavailable);
      }
      return CompletableFuture.<ResolvedIdentity>failedFuture(failure);
    }).thenCompose(stage -> stage);
  }

  private CompletionStage<IssuedToken> authenticateReserved(BackendCredentials credentials,
      RequestScope requestScope, Reservation reservation) {
    CompletionStage<AuthorizedBackendIdentity> authentication;
    try {
      authentication = identities.authenticate(credentials, scope);
    } catch (RuntimeException failure) {
      return failAndRelease(reservation, requestScope, failure);
    }
    return authentication.handle((identity, failure) -> failure != null
        ? failAndRelease(reservation, requestScope, mapIdentityFailure(unwrap(failure)))
        : authenticateIdentity(credentials, requestScope, reservation, identity))
        .thenCompose(stage -> stage);
  }

  private CompletionStage<IssuedToken> authenticateIdentity(BackendCredentials credentials,
      RequestScope requestScope, Reservation reservation, AuthorizedBackendIdentity identity) {
    if (!identityMatchesScope(identity)) {
      CompletionStage<Void> discarded;
      try {
        discarded = identities.discardUnpublished(identity);
      } catch (RuntimeException failure) {
        return failAndRelease(reservation, requestScope, failure);
      }
      return discarded.thenCompose(ignored -> failAndRelease(reservation, requestScope,
          new AuthSessionException(AuthSessionException.Reason.PERMISSION_DENIED)));
    }
    CompletionStage<Void> restorable;
    try {
      restorable = identities.verifyRestorable(credentials, identity);
    } catch (RuntimeException failure) {
      return discardAndRelease(identity, reservation, requestScope, failure);
    }
    return restorable.thenCompose(ignored -> ensureAuthority(identity, requestScope)
        .thenCompose(authority -> discardAuthenticatedIdentity(identity)
            .thenCompose(released -> createSession(credentials, identity, authority, reservation,
                requestScope)))
        .exceptionallyCompose(failure -> isUnresolved(unwrap(failure))
            ? discardWithoutRelease(identity, unwrap(failure))
            : discardAndRelease(identity, reservation, requestScope,
                mapIdentityFailure(unwrap(failure)))));
  }

  private CompletionStage<Void> discardAuthenticatedIdentity(AuthorizedBackendIdentity identity) {
    try {
      return identities.discardUnpublished(identity);
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(new AuthSessionException(
          AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
    }
  }

  private CompletionStage<SubjectAuthority> ensureAuthority(
      AuthorizedBackendIdentity identity, RequestScope requestScope) {
    ByteKey subject = subjectDigest(identity.subject());
    return ensureAuthority(subject, identity, requestScope, 0);
  }

  private CompletionStage<SubjectAuthority> ensureAuthority(ByteKey subject,
      AuthorizedBackendIdentity identity, RequestScope requestScope, int attempts) {
    if (attempts >= MAX_CAS_ATTEMPTS) {
      return CompletableFuture.failedFuture(new AuthSessionException(
          AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
    }
    return store.readAuthority(subject, requestScope).thenCompose(existing -> {
      if (existing.isEmpty()) {
        SubjectAuthority created = new SubjectAuthority(subject, trustDomain, providerId, scope,
            identity.credentialGeneration(), 1, SubjectAuthority.State.ACTIVE);
        return store.createAuthorityIfAbsent(created, requestScope).thenCompose(success -> success
            ? CompletableFuture.completedFuture(created)
            : ensureAuthority(subject, identity, requestScope, attempts + 1));
      }
      SubjectAuthority observed = existing.orElseThrow();
      if (!observed.trustDomain().equals(trustDomain) || !observed.providerId().equals(providerId)
          || !observed.scope().equals(scope)
          || identity.credentialGeneration() < observed.credentialGeneration()) {
        return CompletableFuture.failedFuture(new AuthSessionException(
            AuthSessionException.Reason.PERMISSION_DENIED));
      }
      if (observed.state() == SubjectAuthority.State.ACTIVE
          && observed.credentialGeneration() == identity.credentialGeneration()) {
        return CompletableFuture.completedFuture(observed);
      }
      long generation;
      try {
        generation = Math.addExact(observed.authorizationGeneration(), 1);
      } catch (ArithmeticException exhausted) {
        return CompletableFuture.failedFuture(new AuthSessionException(
            AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
      }
      SubjectAuthority replacement = new SubjectAuthority(subject, trustDomain, providerId, scope,
          identity.credentialGeneration(), generation, SubjectAuthority.State.ACTIVE);
      return store.compareAndSetAuthority(observed, replacement, requestScope)
          .thenCompose(success -> success ? CompletableFuture.completedFuture(replacement)
              : ensureAuthority(subject, identity, requestScope, attempts + 1));
    });
  }

  private CompletionStage<IssuedToken> createSession(BackendCredentials credentials,
      AuthorizedBackendIdentity identity, SubjectAuthority authority, Reservation reservation,
      RequestScope requestScope) {
    long createdAt = clock.millis();
    long expiresAt;
    try {
      expiresAt = Math.addExact(createdAt, sessionTtl.toMillis());
    } catch (ArithmeticException overflow) {
      return CompletableFuture.failedFuture(new AuthSessionException(
          AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
    }
    CredentialEnvelopeContext envelopeContext = envelopeContext(reservation.slot().sessionId(),
        identity.credentialGeneration(), expiresAt);
    byte[] plaintext;
    try {
      plaintext = CredentialMaterialCodec.encode(credentials);
    } catch (RuntimeException invalid) {
      return CompletableFuture.failedFuture(invalid);
    }
    CredentialEnvelope envelope;
    try {
      envelope = protector.encrypt(plaintext, envelopeContext);
    } finally {
      Arrays.fill(plaintext, (byte) 0);
    }
    return persistWithFreshToken(identity, authority, reservation, envelope, createdAt, expiresAt,
        requestScope, 0);
  }

  private CompletionStage<IssuedToken> persistWithFreshToken(AuthorizedBackendIdentity identity,
      SubjectAuthority authority, Reservation reservation, CredentialEnvelope envelope,
      long createdAt, long expiresAt, RequestScope requestScope, int collision) {
    if (collision >= MAX_TOKEN_COLLISIONS) {
      return CompletableFuture.failedFuture(new AuthSessionException(
          AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
    }
    AuthSession preparing;
    String responseToken;
    try (AuthToken token = AuthToken.generate(random)) {
      responseToken = token.valueForResponse();
      preparing = new AuthSession(reservation.slot().sessionId(), token.digest(),
          subjectDigest(identity.subject()), trustDomain, providerId, scope, datasetDigest,
          identity.credentialGeneration(), authority.authorizationGeneration(), createdAt, expiresAt,
          reservation.slot().index(), reservation.slot().generation(), AuthSession.Status.PREPARING,
          envelope);
    }
    AuthSession attempt = preparing;
    String issuedValue = responseToken;
    CompletionStage<IssuedToken> operation = store.createSessionIfAbsent(attempt, requestScope).thenCompose(created -> {
      if (!created) {
        return store.readSession(attempt.tokenDigest(), requestScope).thenCompose(found -> {
          if (found.isPresent() && found.orElseThrow().sessionId().equals(attempt.sessionId())) {
            return activateSession(attempt, authority, reservation, issuedValue, requestScope);
          }
          return persistWithFreshToken(identity, authority, reservation, envelope, createdAt,
              expiresAt, requestScope, collision + 1);
        });
      }
      return activateSession(attempt, authority, reservation, issuedValue, requestScope);
    }).exceptionallyCompose(failure -> resolveUnknownSessionCreate(attempt, authority,
        reservation, issuedValue, requestScope, unwrap(failure)));
    return operation.exceptionallyCompose(failure -> isUnresolved(unwrap(failure))
        ? CompletableFuture.failedFuture(unwrap(failure))
        : cleanupUndelivered(attempt, reservation, requestScope).handle((ignored, cleanupFailure) -> {
          if (cleanupFailure != null) failure.addSuppressed(unwrap(cleanupFailure));
          throw new CompletionException(unwrap(failure));
        }));
  }

  private CompletionStage<IssuedToken> resolveUnknownSessionCreate(AuthSession preparing,
      SubjectAuthority authority, Reservation reservation, String token,
      RequestScope requestScope, Throwable original) {
    return store.readSession(preparing.tokenDigest(), requestScope).thenCompose(found -> {
      if (found.isPresent() && found.orElseThrow().sessionId().equals(preparing.sessionId())) {
        return activateSession(preparing, authority, reservation, token, requestScope);
      }
      if (found.isEmpty()) return CompletableFuture.failedFuture(new UnresolvedAuthSessionWrite());
      return CompletableFuture.failedFuture(new AuthSessionException(
          AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
    }).exceptionallyCompose(readFailure -> CompletableFuture.failedFuture(
        new UnresolvedAuthSessionWrite()));
  }

  private CompletionStage<IssuedToken> activateSession(AuthSession preparing,
      SubjectAuthority authority, Reservation reservation, String token, RequestScope requestScope) {
    AuthSlot activeSlot = new AuthSlot(reservation.slot().index(), reservation.slot().generation(),
        reservation.slot().sessionId(), reservation.slot().operationId(), AuthSlot.State.ACTIVE);
    AuthSession activeSession = withStatus(preparing, AuthSession.Status.ACTIVE);
    return store.compareAndSetSlot(reservation.slot(), activeSlot, requestScope)
        .thenCompose(slotUpdated -> {
          if (!slotUpdated) return CompletableFuture.failedFuture(new AuthSessionException(
              AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
          return store.compareAndSetSession(preparing, activeSession, requestScope);
        }).thenCompose(sessionUpdated -> {
          if (!sessionUpdated) return CompletableFuture.failedFuture(new AuthSessionException(
              AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
          if (requestScope.isClosed() || requestScope.isDeadlineExceeded()) {
            return CompletableFuture.failedFuture(new AuthSessionException(
                AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
          }
          return verifyPublished(activeSession, activeSlot, authority, requestScope)
              .thenApply(ignored -> new IssuedToken(this, token,
                  activeSession.expiresAtEpochMillis(), activeSession, activeSlot));
        });
  }

  private CompletionStage<Void> verifyPublished(AuthSession session, AuthSlot slot,
      SubjectAuthority expectedAuthority, RequestScope requestScope) {
    return store.readSession(session.tokenDigest(), requestScope).thenCombine(
        store.readAuthority(session.subjectDigest(), requestScope), (current, authority) -> {
          AuthSession currentSession = current.orElseThrow(AuthSessionService::invalidToken);
          SubjectAuthority currentAuthority = authority.orElseThrow(AuthSessionService::invalidToken);
          if (currentSession.status() != AuthSession.Status.ACTIVE
              || !currentSession.sessionId().equals(session.sessionId())
              || currentAuthority.state() != SubjectAuthority.State.ACTIVE
              || currentAuthority.authorizationGeneration()
                  != expectedAuthority.authorizationGeneration()
              || currentAuthority.credentialGeneration() != session.credentialGeneration()) {
            throw invalidToken();
          }
          return currentSession;
        }).thenCombine(store.readSlot(slot.index(), requestScope), (currentSession, currentSlot) -> {
          AuthSlot value = currentSlot.orElseThrow(AuthSessionService::invalidToken);
          if (value.state() != AuthSlot.State.ACTIVE
              || value.generation() != session.slotGeneration()
              || !value.sessionId().equals(session.sessionId())) throw invalidToken();
          return null;
        });
  }

  private CompletionStage<Reservation> reserveSlot(RequestScope requestScope) {
    int start = random.nextInt(sessionSlots);
    CompletableFuture<Optional<Reservation>> result = CompletableFuture.completedFuture(Optional.empty());
    for (int offset = 0; offset < sessionSlots; offset++) {
      int index = (start + offset) % sessionSlots;
      result = result.thenCompose(found -> found.isPresent()
          ? CompletableFuture.completedFuture(found)
          : reserveOne(index, requestScope));
    }
    return result.thenApply(found -> found.orElseThrow(() -> new AuthSessionException(
        AuthSessionException.Reason.RESOURCE_LIMIT)));
  }

  private CompletionStage<Optional<Reservation>> reserveOne(int index, RequestScope requestScope) {
    return store.readSlot(index, requestScope).thenCompose(found -> {
      if (found.isEmpty()) {
        AuthSlot initial = new AuthSlot(index, 0, null, null, AuthSlot.State.EMPTY);
        return store.createSlotIfAbsent(initial, requestScope).thenCompose(created -> created
            ? reserveEmpty(initial, requestScope)
            : store.readSlot(index, requestScope).thenCompose(current -> current.isEmpty()
                ? CompletableFuture.failedFuture(new AuthSessionException(
                    AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE))
                : reserveObserved(current.orElseThrow(), requestScope)));
      }
      return reserveObserved(found.orElseThrow(), requestScope);
    });
  }

  private CompletionStage<Optional<Reservation>> reserveObserved(
      AuthSlot observed, RequestScope requestScope) {
    if (observed.state() != AuthSlot.State.EMPTY) return CompletableFuture.completedFuture(Optional.empty());
    return reserveEmpty(observed, requestScope);
  }

  private CompletionStage<Optional<Reservation>> reserveEmpty(
      AuthSlot empty, RequestScope requestScope) {
    long generation;
    try {
      generation = Math.addExact(empty.generation(), 1);
    } catch (ArithmeticException exhausted) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
    byte[] sessionIdBytes = new byte[SESSION_ID_BYTES];
    byte[] operationIdBytes = new byte[OPERATION_ID_BYTES];
    random.nextBytes(sessionIdBytes);
    random.nextBytes(operationIdBytes);
    String operationId = Base64.getUrlEncoder().withoutPadding().encodeToString(operationIdBytes);
    Arrays.fill(operationIdBytes, (byte) 0);
    AuthSlot reserved = new AuthSlot(empty.index(), generation,
        ByteKey.copyOf(sessionIdBytes), operationId, AuthSlot.State.RESERVED);
    Arrays.fill(sessionIdBytes, (byte) 0);
    return store.compareAndSetSlot(empty, reserved, requestScope)
        .thenApply(success -> success ? Optional.of(new Reservation(reserved)) : Optional.empty());
  }

  private CompletionStage<IssuedToken> discardAndRelease(AuthorizedBackendIdentity identity,
      Reservation reservation, RequestScope requestScope, Throwable failure) {
    CompletionStage<Void> discarded;
    try {
      discarded = identities.discardUnpublished(identity);
    } catch (RuntimeException cleanup) {
      failure.addSuppressed(cleanup);
      return failAndRelease(reservation, requestScope, failure);
    }
    return discarded.handle((ignored, cleanupFailure) -> {
      if (cleanupFailure != null) failure.addSuppressed(unwrap(cleanupFailure));
      return null;
    }).thenCompose(ignored -> failAndRelease(reservation, requestScope, failure));
  }

  private CompletionStage<IssuedToken> discardWithoutRelease(
      AuthorizedBackendIdentity identity, Throwable failure) {
    CompletionStage<Void> discarded;
    try {
      discarded = identities.discardUnpublished(identity);
    } catch (RuntimeException cleanup) {
      failure.addSuppressed(cleanup);
      return CompletableFuture.failedFuture(new AuthSessionException(
          AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
    }
    return discarded.handle((ignored, cleanupFailure) -> {
      if (cleanupFailure != null) failure.addSuppressed(unwrap(cleanupFailure));
      return null;
    }).thenCompose(ignored -> CompletableFuture.failedFuture(new AuthSessionException(
        AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE)));
  }

  private CompletionStage<IssuedToken> failAndRelease(
      Reservation reservation, RequestScope requestScope, Throwable failure) {
    return cleanupUndelivered(null, reservation, requestScope)
        .handle((ignored, cleanupFailure) -> {
          if (cleanupFailure != null) failure.addSuppressed(unwrap(cleanupFailure));
          throw new CompletionException(failure);
        });
  }

  private CompletionStage<Void> cleanupUndelivered(AuthSession expectedSession,
      Reservation reservation, RequestScope requestScope) {
    boolean ownedScope = requestScope.isClosed() || requestScope.isDeadlineExceeded();
    RequestScope cleanupScope = ownedScope
        ? RequestScope.withTimeout(Duration.ofSeconds(10)) : requestScope;
    CompletionStage<Void> safeToInspect = ownedScope
        ? requestScope.physicalProducersCompletion().toCompletableFuture().orTimeout(
            10, java.util.concurrent.TimeUnit.SECONDS).thenApply(ignored -> null)
        : CompletableFuture.completedFuture(null);
    CompletionStage<Void> cleanup = safeToInspect.thenCompose(ignored ->
        store.readSlot(reservation.slot().index(), cleanupScope))
        .thenCompose(found -> {
          if (found.isEmpty()) return CompletableFuture.completedFuture(null);
          AuthSlot current = found.orElseThrow();
          if (current.generation() != reservation.slot().generation()
              || !reservation.slot().sessionId().equals(current.sessionId())) {
            return CompletableFuture.completedFuture(null);
          }
          if (current.state() == AuthSlot.State.EMPTY) return CompletableFuture.completedFuture(null);
          CompletionStage<AuthSlot> retirement;
          if (current.state() == AuthSlot.State.RETIRING) {
            retirement = CompletableFuture.completedFuture(current);
          } else {
            AuthSlot retiring = new AuthSlot(current.index(), current.generation(), current.sessionId(),
                current.operationId(), AuthSlot.State.RETIRING);
            retirement = store.compareAndSetSlot(current, retiring, cleanupScope).thenCompose(changed ->
                changed ? CompletableFuture.completedFuture(retiring)
                    : CompletableFuture.failedFuture(new AuthSessionException(
                        AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE)));
          }
          return retirement.thenCompose(retiring -> removeUndeliveredSession(expectedSession,
              reservation, cleanupScope).thenCompose(ignored -> {
                AuthSlot empty = new AuthSlot(retiring.index(), retiring.generation(), null, null,
                    AuthSlot.State.EMPTY);
                return store.compareAndSetSlot(retiring, empty, cleanupScope).thenApply(released -> {
                  if (!released) throw new AuthSessionException(
                      AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE);
                  return null;
                });
              }));
        });
    if (ownedScope) cleanup = cleanup.whenComplete((ignored, failure) -> cleanupScope.close());
    return cleanup;
  }

  private CompletionStage<Void> removeUndeliveredSession(AuthSession expectedSession,
      Reservation reservation, RequestScope cleanupScope) {
    if (expectedSession == null) return CompletableFuture.completedFuture(null);
    return store.readSession(expectedSession.tokenDigest(), cleanupScope).thenCompose(found -> {
      if (found.isEmpty()) return CompletableFuture.completedFuture(null);
      AuthSession current = found.orElseThrow();
      if (!current.sessionId().equals(reservation.slot().sessionId())
          || current.slotIndex() != reservation.slot().index()
          || current.slotGeneration() != reservation.slot().generation()
          || !current.tokenDigest().equals(expectedSession.tokenDigest())
          || !current.sessionId().equals(expectedSession.sessionId())
          || current.slotIndex() != expectedSession.slotIndex()
          || current.slotGeneration() != expectedSession.slotGeneration()) {
        return CompletableFuture.failedFuture(new AuthSessionException(
            AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE));
      }
      if (current.status() == AuthSession.Status.PREPARING
          || current.status() == AuthSession.Status.ACTIVE) {
        AuthSession retiring = withStatus(current, AuthSession.Status.RETIRING);
        return store.compareAndSetSession(current, retiring, cleanupScope).thenCompose(changed ->
            changed ? store.deleteSessionIfUnchanged(retiring, cleanupScope).thenApply(deleted -> {
              if (!deleted) throw new AuthSessionException(
                  AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE);
              return null;
            }) : CompletableFuture.failedFuture(new AuthSessionException(
                AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE)));
      }
      return store.deleteSessionIfUnchanged(current, cleanupScope).thenApply(deleted -> {
        if (!deleted) throw new AuthSessionException(
            AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE);
        return null;
      });
    });
  }

  private CredentialEnvelopeContext envelopeContext(
      ByteKey sessionId, long credentialGeneration, long expiresAt) {
    return new CredentialEnvelopeContext(trustDomain, sessionId, providerId, scope,
        credentialGeneration, expiresAt);
  }

  private ByteKey subjectDigest(String subject) {
    try {
      var output = new ByteArrayOutputStream();
      var data = new DataOutputStream(output);
      data.write("etcd-hbase-auth-subject-v1".getBytes(StandardCharsets.UTF_8));
      write(data, trustDomain);
      write(data, providerId);
      write(data, scope.tableGroupId());
      write(data, scope.connectionProfileId());
      write(data, subject);
      data.flush();
      return ByteKey.copyOf(MessageDigest.getInstance("SHA-256").digest(output.toByteArray()));
    } catch (IOException | NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("subject digest is unavailable", impossible);
    }
  }

  private boolean identityMatchesScope(AuthorizedBackendIdentity identity) {
    return identity != null && identity.providerId().equals(providerId)
        && identity.scope().equals(scope)
        && identity.allowedTableGroupIds().contains(scope.tableGroupId());
  }

  private boolean identityMatches(AuthorizedBackendIdentity identity, AuthSession session) {
    return identityMatchesScope(identity) && identity.credentialGeneration() == session.credentialGeneration()
        && subjectDigest(identity.subject()).equals(session.subjectDigest());
  }

  private static void validateCredentials(BackendCredentials credentials) {
    String principal = credentials.principal();
    if (principal.isBlank() || principal.codePointCount(0, principal.length()) > MAX_PRINCIPAL_CODE_POINTS
        || principal.codePoints().anyMatch(Character::isISOControl)) {
      throw new IllegalArgumentException("authentication principal is outside its size limit");
    }
    char[] secret = credentials.copySecret(MAX_SECRET_CHARACTERS);
    try {
      if (secret.length == 0) throw new IllegalArgumentException("authentication secret is empty");
    } finally {
      Arrays.fill(secret, '\0');
    }
  }

  private static AuthSession withStatus(AuthSession session, AuthSession.Status status) {
    return new AuthSession(session.sessionId(), session.tokenDigest(), session.subjectDigest(),
        session.trustDomain(), session.providerId(), session.scope(), session.datasetDigest(),
        session.credentialGeneration(), session.authorizationGeneration(), session.createdAtEpochMillis(),
        session.expiresAtEpochMillis(), session.slotIndex(), session.slotGeneration(), status,
        session.credentialEnvelope());
  }

  private static void write(DataOutputStream output, String value) throws IOException {
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    output.writeInt(encoded.length);
    output.write(encoded);
  }

  private static ByteKey requireDigest(ByteKey value, String name) {
    Objects.requireNonNull(value, name);
    if (value.length() != 32) throw new IllegalArgumentException(name + " must be SHA-256");
    return value;
  }

  private static String requireId(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!value.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException(name + " must be a bounded configured identifier");
    }
    return value;
  }

  private static Throwable unwrap(Throwable failure) {
    while ((failure instanceof CompletionException) && failure.getCause() != null) {
      failure = failure.getCause();
    }
    return failure;
  }

  private static RuntimeException mapIdentityFailure(Throwable failure) {
    if (isUnresolved(failure)) {
      return new AuthSessionException(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE);
    }
    if (failure instanceof AuthSessionException auth) return auth;
    if (failure instanceof io.etcdhbase.spi.identity.BackendIdentityException backend) {
      return switch (backend.reason()) {
        case INVALID_CREDENTIALS -> new AuthSessionException(
            AuthSessionException.Reason.INVALID_CREDENTIALS);
        case INSUFFICIENT_PERMISSION -> new AuthSessionException(
            AuthSessionException.Reason.PERMISSION_DENIED);
        case RESOURCE_LIMIT -> new AuthSessionException(AuthSessionException.Reason.RESOURCE_LIMIT);
        case TIMEOUT, CANCELLED, BACKEND_UNAVAILABLE -> new AuthSessionException(
            AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE);
      };
    }
    return new AuthSessionException(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE);
  }

  private static AuthSessionException invalidToken() {
    return new AuthSessionException(AuthSessionException.Reason.INVALID_TOKEN);
  }

  private static boolean isUnresolved(Throwable failure) {
    return failure instanceof UnresolvedAuthSessionWrite;
  }

  private record Reservation(AuthSlot slot) {}

  private record ActiveSessionState(
      Optional<AuthSession> session, Optional<SubjectAuthority> authority) {}

  public static final class ResolvedIdentity {
    private final AuthSession session;
    private final SessionIdentityLease identityLease;

    private ResolvedIdentity(AuthSession session, SessionIdentityLease identityLease) {
      this.session = Objects.requireNonNull(session, "session");
      this.identityLease = Objects.requireNonNull(identityLease, "identityLease");
    }

    public AuthSession session() { return session; }
    public AuthorizedBackendIdentity identity() { return identityLease.identity(); }
    public CompletionStage<Void> release() { return identityLease.release(); }
  }

  /** Response token has a redacted diagnostic representation and is not a persistent object. */
  public static final class IssuedToken {
    private final AuthSessionService owner;
    private final String token;
    private final long expiresAtEpochMillis;
    private final AuthSession session;
    private final AuthSlot slot;
    private IssuedToken(AuthSessionService owner, String token, long expiresAtEpochMillis,
        AuthSession session, AuthSlot slot) {
      this.owner = Objects.requireNonNull(owner, "owner");
      this.token = Objects.requireNonNull(token, "token");
      this.expiresAtEpochMillis = expiresAtEpochMillis;
      this.session = Objects.requireNonNull(session, "session");
      this.slot = Objects.requireNonNull(slot, "slot");
    }
    public String tokenForResponse() { return token; }
    public long expiresAtEpochMillis() { return expiresAtEpochMillis; }
    @Override public String toString() { return "IssuedToken[token=redacted]"; }
  }
}
