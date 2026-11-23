package io.etcdhbase.server.auth;

import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.hbase.connection.HBaseConnectionRouter;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.identity.BackendIdentityException.Reason;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Owns one isolated HBase provider container per authentication or request activation. */
public final class ProviderSessionIdentityManager implements SessionIdentityManager {
  private static final long NEGATIVE_CACHE_TTL_NANOS = TimeUnit.SECONDS.toNanos(1);
  private static final int MAX_NEGATIVE_CACHE_ENTRIES = 1024;

  private final Object gate = new Object();
  private final Supplier<HBaseBackendIdentityProvider> providerFactory;
  private final HBaseConnectionRouter connections;
  private final AuthenticationScope scope;
  private final String providerId;
  private final int maximumContainers;
  private final Set<HBaseBackendIdentityProvider> containers =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private final Map<HBaseBackendIdentityProvider, CompletableFuture<Void>> closingProviders =
      new IdentityHashMap<>();
  private final Map<String, OwnedIdentity> unpublished = new java.util.HashMap<>();
  private final Map<String, OwnedIdentity> active = new java.util.HashMap<>();
  private final Map<ActivationKey, ActivationEntry> activations = new HashMap<>();
  private final LinkedHashMap<ActivationKey, NegativeActivation> negativeActivations =
      new LinkedHashMap<>(16, 0.75f, true);
  private final int maximumNegativeEntries;
  private boolean closed;

  public ProviderSessionIdentityManager(Supplier<HBaseBackendIdentityProvider> providerFactory,
      HBaseConnectionRouter connections, AuthenticationScope scope, String providerId,
      int maximumContainers) {
    this.providerFactory = Objects.requireNonNull(providerFactory, "providerFactory");
    this.connections = Objects.requireNonNull(connections, "connections");
    this.scope = Objects.requireNonNull(scope, "scope");
    if (providerId == null || !providerId.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException("backend provider ID must be a configured identifier");
    }
    this.providerId = providerId;
    if (maximumContainers < 1 || maximumContainers > 100_000) {
      throw new IllegalArgumentException("session identity container limit is outside its range");
    }
    this.maximumContainers = maximumContainers;
    this.maximumNegativeEntries = Math.min(maximumContainers, MAX_NEGATIVE_CACHE_ENTRIES);
  }

  @Override
  public CompletionStage<AuthorizedBackendIdentity> authenticate(
      BackendCredentials credentials, AuthenticationScope requestedScope) {
    if (!scope.equals(requestedScope)) return rejected(Reason.INSUFFICIENT_PERMISSION);
    HBaseBackendIdentityProvider provider = acquireContainer();
    if (provider == null) return rejected(Reason.RESOURCE_LIMIT);
    CompletionStage<AuthorizedBackendIdentity> authentication;
    try {
      authentication = provider.authenticate(credentials, scope);
    } catch (RuntimeException | Error failure) {
      closeAfterFailure(provider, failure);
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    return authentication.handle((identity, failure) -> {
      if (failure != null) {
        Throwable cause = unwrap(failure);
        return closeAfterFailure(provider, cause)
            .thenCompose(ignored -> CompletableFuture.<AuthorizedBackendIdentity>failedFuture(cause));
      }
      BackendIdentityException rejection = null;
      synchronized (gate) {
        if (closed) rejection = new BackendIdentityException(Reason.CANCELLED);
        else if (!validIdentity(identity)
            || unpublished.containsKey(identity.contextRef())
            || active.containsKey(identity.contextRef())) {
          rejection = new BackendIdentityException(Reason.INSUFFICIENT_PERMISSION);
        } else {
          unpublished.put(identity.contextRef(), new OwnedIdentity(identity, provider));
          return CompletableFuture.completedFuture(identity);
        }
      }
      return this.<AuthorizedBackendIdentity>closeThenFail(provider, rejection);
    }).thenCompose(stage -> stage);
  }

  @Override
  public CompletionStage<Void> verifyRestorable(
      BackendCredentials credentials, AuthorizedBackendIdentity authenticatedIdentity) {
    Objects.requireNonNull(authenticatedIdentity, "authenticatedIdentity");
    HBaseBackendIdentityProvider provider = acquireContainer();
    if (provider == null) return rejected(Reason.RESOURCE_LIMIT);
    CompletionStage<AuthorizedBackendIdentity> authentication;
    try {
      authentication = provider.authenticate(credentials, scope);
    } catch (RuntimeException | Error failure) {
      closeAfterFailure(provider, failure);
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    return authentication.handle((restored, failure) -> {
      if (failure != null) return closeAfterFailure(provider, unwrap(failure))
          .thenCompose(ignored -> CompletableFuture.<Void>failedFuture(unwrap(failure)));
      if (restored == null || !sameAuthorization(authenticatedIdentity, restored)) {
        return closeProvider(provider).thenCompose(ignored -> CompletableFuture.<Void>failedFuture(
            new BackendIdentityException(Reason.INSUFFICIENT_PERMISSION)));
      }
      return closeProvider(provider);
    }).thenCompose(stage -> stage);
  }

  @Override
  public CompletionStage<SessionIdentityLease> activate(
      AuthSession session, BackendCredentials credentials) {
    Objects.requireNonNull(session, "session");
    Objects.requireNonNull(credentials, "credentials");
    if (!session.providerId().equals(providerId) || !session.scope().equals(scope)) {
      return rejected(Reason.INSUFFICIENT_PERMISSION);
    }
    ActivationKey key = ActivationKey.from(session);
    ActivationEntry entry;
    BorrowRequest borrow;
    boolean startActivation;
    synchronized (gate) {
      if (closed) return rejected(Reason.CANCELLED);
      NegativeActivation negative = negativeActivationLocked(key, System.nanoTime());
      if (negative != null) return rejected(negative.reason());

      entry = activations.get(key);
      if (entry == null) {
        HBaseBackendIdentityProvider provider = acquireContainerLocked();
        if (provider == null) return rejected(Reason.RESOURCE_LIMIT);
        entry = new ActivationEntry(key, provider);
        activations.put(key, entry);
        startActivation = true;
      } else {
        if (entry.state == ActivationState.RETIRING || entry.state == ActivationState.CLOSE_FAILED) {
          return rejected(Reason.RESOURCE_LIMIT);
        }
        startActivation = false;
      }
      entry.pendingBorrowers++;
      borrow = new BorrowRequest(entry);
    }

    ActivationEntry selectedEntry = entry;
    selectedEntry.activation.whenComplete((owned, failure) ->
        completeBorrow(borrow, owned, failure));
    borrow.result.whenComplete((ignored, failure) -> {
      if (borrow.result.isCancelled()) cancelBorrow(borrow);
    });
    if (startActivation) startActivation(selectedEntry, credentials);
    return borrow.result;
  }

  @Override
  public CompletionStage<Void> discardUnpublished(AuthorizedBackendIdentity identity) {
    Objects.requireNonNull(identity, "identity");
    OwnedIdentity owned;
    synchronized (gate) {
      owned = unpublished.get(identity.contextRef());
      if (owned == null) return CompletableFuture.completedFuture(null);
      if (owned.identity() != identity) {
        return rejected(Reason.INSUFFICIENT_PERMISSION);
      }
    }
    return closeProvider(owned.provider()).thenRun(() -> {
      synchronized (gate) {
        unpublished.remove(identity.contextRef(), owned);
      }
    });
  }

  /** Closes only containers owned by this session manager; the runtime owns its base provider. */
  @Override
  public void close() {
    ArrayList<HBaseBackendIdentityProvider> snapshot;
    ArrayList<OwnedIdentity> ownedIdentities;
    synchronized (gate) {
      closed = true;
      snapshot = new ArrayList<>(containers);
      ownedIdentities = new ArrayList<>(active.values());
    }
    Set<OwnedIdentity> unrouted = Collections.newSetFromMap(new IdentityHashMap<>());
    Throwable first = null;
    for (OwnedIdentity publishedIdentity : ownedIdentities) {
      try {
        connections.unregister(publishedIdentity.identity(), publishedIdentity.provider())
            .toCompletableFuture().join();
        unrouted.add(publishedIdentity);
      } catch (RuntimeException | Error failure) {
        first = appendFailure(first, unwrap(failure));
      }
    }
    for (HBaseBackendIdentityProvider provider : snapshot) {
      boolean providerClosed = false;
      try {
        closeProvider(provider).toCompletableFuture().join();
        providerClosed = true;
      } catch (RuntimeException | Error failure) {
        first = appendFailure(first, unwrap(failure));
      }
      ArrayList<ActivationEntry> settledActivations = new ArrayList<>();
      synchronized (gate) {
        if (providerClosed) {
          active.values().removeIf(value -> value.provider() == provider
              && unrouted.contains(value));
          activations.values().removeIf(entry -> {
            if (entry.provider != provider) return false;
            entry.state = ActivationState.CLOSED;
            settledActivations.add(entry);
            return true;
          });
        } else {
          activations.values().stream().filter(entry -> entry.provider == provider)
              .forEach(entry -> {
                entry.state = ActivationState.CLOSE_FAILED;
                settledActivations.add(entry);
              });
        }
      }
      for (ActivationEntry entry : settledActivations) {
        entry.activation.completeExceptionally(new BackendIdentityException(Reason.CANCELLED));
      }
    }
    if (first instanceof Error fatal) throw fatal;
    if (first instanceof RuntimeException failure) throw failure;
  }

  private HBaseBackendIdentityProvider acquireContainer() {
    synchronized (gate) {
      return acquireContainerLocked();
    }
  }

  private HBaseBackendIdentityProvider acquireContainerLocked() {
    if (closed || containers.size() >= maximumContainers) return null;
    HBaseBackendIdentityProvider provider = Objects.requireNonNull(
        providerFactory.get(), "identity provider factory result");
    containers.add(provider);
    return provider;
  }

  private void startActivation(ActivationEntry entry, BackendCredentials credentials) {
    BackendCredentials ownedCredentials = copyCredentials(credentials);
    CompletionStage<AuthorizedBackendIdentity> authentication;
    try {
      authentication = Objects.requireNonNull(
          entry.provider.authenticate(ownedCredentials, scope), "identity activation stage");
    } catch (RuntimeException failure) {
      ownedCredentials.close();
      failActivation(entry, failure);
      return;
    } catch (Error fatal) {
      ownedCredentials.close();
      failActivation(entry, fatal);
      throw fatal;
    }
    authentication.whenComplete((identity, failure) -> {
      ownedCredentials.close();
      if (failure != null) failActivation(entry, unwrap(failure));
      else completeActivation(entry, identity);
    });
  }

  private static BackendCredentials copyCredentials(BackendCredentials credentials) {
    char[] secret = credentials.copySecret();
    try {
      return new BackendCredentials(credentials.principal(), secret);
    } finally {
      Arrays.fill(secret, '\0');
    }
  }

  private void completeActivation(ActivationEntry entry, AuthorizedBackendIdentity identity) {
    Throwable rejection = null;
    OwnedIdentity owned = null;
    boolean abandon = false;
    synchronized (gate) {
      if (entry.state != ActivationState.LOADING) return;
      if (closed) {
        rejection = new BackendIdentityException(Reason.CANCELLED);
      } else if (entry.pendingBorrowers == 0) {
        entry.state = ActivationState.RETIRING;
        abandon = true;
      } else if (!validIdentity(identity)
          || active.containsKey(identity.contextRef())
          || unpublished.containsKey(identity.contextRef())) {
        rejection = new BackendIdentityException(Reason.INSUFFICIENT_PERMISSION);
      } else {
        try {
          connections.register(identity, entry.provider);
          owned = new OwnedIdentity(identity, entry.provider);
          active.put(identity.contextRef(), owned);
          entry.owned = owned;
          entry.state = ActivationState.ACTIVE;
        } catch (RuntimeException | Error registrationFailure) {
          rejection = registrationFailure;
        }
      }
    }
    if (owned != null) {
      entry.activation.complete(owned);
      return;
    }
    if (rejection == null && abandon) {
      finishUnusedActivation(entry, new BackendIdentityException(Reason.CANCELLED));
      return;
    }
    failActivation(entry, rejection == null
        ? new BackendIdentityException(Reason.BACKEND_UNAVAILABLE) : rejection);
  }

  private void finishUnusedActivation(ActivationEntry entry, Throwable failure) {
    closeProvider(entry.provider).whenComplete((ignored, closeFailure) -> {
      if (closeFailure != null) failure.addSuppressed(unwrap(closeFailure));
      synchronized (gate) {
        entry.state = closeFailure == null ? ActivationState.CLOSED : ActivationState.CLOSE_FAILED;
        if (closeFailure == null) activations.remove(entry.key, entry);
      }
      entry.activation.completeExceptionally(failure);
    });
  }

  private void failActivation(ActivationEntry entry, Throwable failure) {
    synchronized (gate) {
      if (entry.state != ActivationState.LOADING) return;
      entry.state = ActivationState.RETIRING;
    }
    closeProvider(entry.provider).whenComplete((ignored, closeFailure) -> {
      if (closeFailure != null && closeFailure != failure) {
        failure.addSuppressed(unwrap(closeFailure));
      }
      synchronized (gate) {
        entry.state = closeFailure == null ? ActivationState.CLOSED : ActivationState.CLOSE_FAILED;
        if (closeFailure == null) activations.remove(entry.key, entry);
        cacheNegativeActivationLocked(entry.key, failure, System.nanoTime());
      }
      entry.activation.completeExceptionally(failure);
    });
  }

  private void completeBorrow(BorrowRequest borrow, OwnedIdentity owned, Throwable loadFailure) {
    if (!borrow.pending.compareAndSet(true, false)) return;
    ActivationEntry entry = borrow.entry;
    Throwable failure = loadFailure == null ? null : unwrap(loadFailure);
    SessionIdentityLease lease = null;
    boolean retire = false;
    synchronized (gate) {
      entry.pendingBorrowers--;
      if (failure == null && entry.state == ActivationState.ACTIVE && !closed) {
        entry.borrowers++;
        OwnedIdentity selected = owned;
        lease = new SessionIdentityLease(selected.identity(), () -> releaseBorrow(entry, selected));
      } else if (failure == null) {
        failure = new BackendIdentityException(Reason.CANCELLED);
      }
      if (entry.state == ActivationState.ACTIVE
          && entry.pendingBorrowers == 0 && entry.borrowers == 0) {
        entry.state = ActivationState.RETIRING;
        retire = true;
      }
    }
    if (lease == null) borrow.result.completeExceptionally(Objects.requireNonNull(failure));
    else if (!borrow.result.complete(lease)) lease.release();
    if (retire) retireEntry(entry);
  }

  private void cancelBorrow(BorrowRequest borrow) {
    if (!borrow.pending.compareAndSet(true, false)) return;
    ActivationEntry entry = borrow.entry;
    boolean retire = false;
    synchronized (gate) {
      entry.pendingBorrowers--;
      if (entry.state == ActivationState.ACTIVE
          && entry.pendingBorrowers == 0 && entry.borrowers == 0) {
        entry.state = ActivationState.RETIRING;
        retire = true;
      }
    }
    if (retire) retireEntry(entry);
  }

  private CompletionStage<Void> releaseBorrow(ActivationEntry entry, OwnedIdentity owned) {
    boolean retire = false;
    synchronized (gate) {
      if (entry.state == ActivationState.CLOSED) return CompletableFuture.completedFuture(null);
      if (entry.owned != owned || entry.borrowers <= 0) {
        return CompletableFuture.failedFuture(new BackendIdentityException(Reason.CANCELLED));
      }
      entry.borrowers--;
      if (entry.state == ActivationState.ACTIVE
          && entry.pendingBorrowers == 0 && entry.borrowers == 0) {
        entry.state = ActivationState.RETIRING;
        retire = true;
      }
    }
    return retire ? retireEntry(entry) : CompletableFuture.completedFuture(null);
  }

  private CompletionStage<Void> retireEntry(ActivationEntry entry) {
    OwnedIdentity owned = entry.owned;
    if (owned == null) return CompletableFuture.completedFuture(null);
    CompletionStage<Void> retirement;
    try {
      retirement = connections.unregister(owned.identity(), owned.provider())
          .thenCompose(ignored -> closeProvider(owned.provider()));
    } catch (RuntimeException | Error failure) {
      retirement = CompletableFuture.failedFuture(failure);
    }
    return retirement.whenComplete((ignored, failure) -> {
      synchronized (gate) {
        if (failure == null) {
          active.remove(owned.identity().contextRef(), owned);
          activations.remove(entry.key, entry);
          entry.state = ActivationState.CLOSED;
        } else {
          entry.state = ActivationState.CLOSE_FAILED;
        }
      }
    });
  }

  private NegativeActivation negativeActivationLocked(ActivationKey key, long nowNanos) {
    NegativeActivation negative = negativeActivations.get(key);
    if (negative == null) return null;
    if (nowNanos - negative.expiresAtNanos() >= 0) {
      negativeActivations.remove(key);
      return null;
    }
    return negative;
  }

  private void cacheNegativeActivationLocked(
      ActivationKey key, Throwable failure, long nowNanos) {
    Throwable cause = unwrap(failure);
    if (!(cause instanceof BackendIdentityException backendFailure)
        || (backendFailure.reason() != Reason.INVALID_CREDENTIALS
            && backendFailure.reason() != Reason.INSUFFICIENT_PERMISSION)) return;
    negativeActivations.put(key,
        new NegativeActivation(backendFailure.reason(), nowNanos + NEGATIVE_CACHE_TTL_NANOS));
    while (negativeActivations.size() > maximumNegativeEntries) {
      ActivationKey eldest = negativeActivations.keySet().iterator().next();
      negativeActivations.remove(eldest);
    }
  }

  private CompletionStage<Void> closeProvider(HBaseBackendIdentityProvider provider) {
    CompletableFuture<Void> closing;
    try {
      synchronized (gate) {
        if (!containers.contains(provider)) return CompletableFuture.completedFuture(null);
        CompletableFuture<Void> existing = closingProviders.get(provider);
        if (existing != null) return existing.minimalCompletionStage();
        closing = new CompletableFuture<>();
        closingProviders.put(provider, closing);
      }
    } catch (RuntimeException | Error failure) {
      return CompletableFuture.failedFuture(failure);
    }
    Throwable closeFailure = null;
    try {
      provider.close();
    } catch (RuntimeException | Error failure) {
      closeFailure = failure;
    }
    if (closeFailure == null) {
      synchronized (gate) {
        containers.remove(provider);
        unpublished.values().removeIf(value -> value.provider() == provider);
      }
    }
    if (closeFailure == null) closing.complete(null);
    else closing.completeExceptionally(closeFailure);
    synchronized (gate) {
      closingProviders.remove(provider, closing);
    }
    return closing.minimalCompletionStage();
  }

  private CompletionStage<Void> closeAfterFailure(
      HBaseBackendIdentityProvider provider, Throwable failure) {
    return closeProvider(provider).handle((ignored, closeFailure) -> {
      if (closeFailure != null && closeFailure != failure) failure.addSuppressed(unwrap(closeFailure));
      return null;
    });
  }

  private <T> CompletionStage<T> closeThenFail(
      HBaseBackendIdentityProvider provider, BackendIdentityException failure) {
    CompletionStage<Void> cleanup = closeProvider(provider).handle((ignored, closeFailure) -> {
      if (closeFailure != null) failure.addSuppressed(unwrap(closeFailure));
      return null;
    });
    return cleanup.thenCompose(ignored -> CompletableFuture.<T>failedFuture(failure));
  }

  private static Throwable appendFailure(Throwable first, Throwable failure) {
    if (first == null) return failure;
    if (first != failure) first.addSuppressed(failure);
    return first;
  }

  private static boolean sameAuthorization(AuthorizedBackendIdentity expected,
      AuthorizedBackendIdentity actual) {
    return expected.providerId().equals(actual.providerId())
        && expected.subject().equals(actual.subject())
        && expected.scope().equals(actual.scope())
        && expected.allowedTableGroupIds().equals(actual.allowedTableGroupIds())
        && expected.credentialGeneration() == actual.credentialGeneration();
  }

  private boolean validIdentity(AuthorizedBackendIdentity identity) {
    return identity != null && identity.providerId().equals(providerId)
        && identity.scope().equals(scope)
        && identity.allowedTableGroupIds().contains(scope.tableGroupId());
  }

  private static Throwable unwrap(Throwable failure) {
    while ((failure instanceof java.util.concurrent.CompletionException)
        && failure.getCause() != null) failure = failure.getCause();
    return failure;
  }

  private static <T> CompletableFuture<T> rejected(Reason reason) {
    return CompletableFuture.failedFuture(new BackendIdentityException(reason));
  }

  private record ActivationKey(String trustDomain, String providerId, AuthenticationScope scope,
      ByteKey datasetDigest, ByteKey subjectDigest, long credentialGeneration,
      long authorizationGeneration) {
    private static ActivationKey from(AuthSession session) {
      return new ActivationKey(session.trustDomain(), session.providerId(), session.scope(),
          session.datasetDigest(), session.subjectDigest(), session.credentialGeneration(),
          session.authorizationGeneration());
    }
  }

  private static final class ActivationEntry {
    private final ActivationKey key;
    private final HBaseBackendIdentityProvider provider;
    private final CompletableFuture<OwnedIdentity> activation = new CompletableFuture<>();
    private ActivationState state = ActivationState.LOADING;
    private int pendingBorrowers;
    private int borrowers;
    private OwnedIdentity owned;

    private ActivationEntry(ActivationKey key, HBaseBackendIdentityProvider provider) {
      this.key = key;
      this.provider = provider;
    }
  }

  private static final class BorrowRequest {
    private final ActivationEntry entry;
    private final CompletableFuture<SessionIdentityLease> result = new CompletableFuture<>();
    private final AtomicBoolean pending = new AtomicBoolean(true);

    private BorrowRequest(ActivationEntry entry) {
      this.entry = entry;
    }
  }

  private record NegativeActivation(Reason reason, long expiresAtNanos) {}

  private enum ActivationState {
    LOADING,
    ACTIVE,
    RETIRING,
    CLOSE_FAILED,
    CLOSED
  }

  private record OwnedIdentity(
      AuthorizedBackendIdentity identity, HBaseBackendIdentityProvider provider) {}

}
