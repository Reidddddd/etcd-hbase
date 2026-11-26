package io.etcdhbase.hbase.identity;

import io.etcdhbase.hbase.api.HBaseAccessVerifier;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.api.HBaseCredentialAuthenticator;
import io.etcdhbase.hbase.api.HBaseUserContext;
import io.etcdhbase.hbase.error.HBaseFailures;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendIdentityBorrow;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.identity.BackendIdentityException.Reason;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;

/**
 * One configured table group's bounded authentication and connection owner.
 *
 * <p>Credential conversion, connection creation, and backend permission verification must all
 * succeed before publication. The verified connection is then reused by that identity. No etcd
 * sessions or tokens are created here. Contexts remain counted until identity retirement and
 * provider-owned connection/subject cleanup both complete.
 *
 * <p>At most {@code authenticationWorkers} accepted attempts may await result delivery. Each
 * reserves an independent notification worker until synchronous caller callbacks return; other
 * attempts fail promptly with {@link Reason#RESOURCE_LIMIT}. Retained identities and unfinished
 * resource cleanup are additionally bounded by {@code maximumContexts}.
 */
public final class HBaseBackendIdentityProvider
    implements BackendIdentityProvider, HBaseConnectionFactory {
  private static final int MAX_CLOSED_CLEANUP_RETRIES = 3;
  private static final long CLEANUP_RETRY_DELAY_MILLIS = 25;

  @FunctionalInterface
  interface ConnectionOpener {
    CompletionStage<AsyncConnection> open(Configuration configuration, User user);
  }

  private final Object gate = new Object();
  private final String providerId;
  private final AuthenticationScope scope;
  private final Configuration configuration;
  private final HBaseCredentialAuthenticator authenticator;
  private final HBaseAccessVerifier verifier;
  private final ConnectionOpener opener;
  private final int maximumContexts;
  private final int maximumNotifications;
  private final Duration timeout;
  private final Map<String, Attempt> contexts = new HashMap<>();
  private final LinkedHashMap<String, AuthorizedBackendIdentity> retiredIdentities =
      new LinkedHashMap<>();
  private final ThreadPoolExecutor workers;
  private final ThreadPoolExecutor completions;
  private final ThreadPoolExecutor notifications;
  private final ThreadPoolExecutor accessNotifications;
  private final ScheduledThreadPoolExecutor deadlines;
  private int pendingNotifications;
  private int pendingAccessChecks;
  private CompletableFuture<Void> accessChecksDrained = CompletableFuture.completedFuture(null);
  private volatile boolean closed;

  public HBaseBackendIdentityProvider(
      String providerId,
      AuthenticationScope scope,
      Configuration configuration,
      HBaseCredentialAuthenticator authenticator,
      HBaseAccessVerifier verifier,
      int maximumContexts,
      int authenticationWorkers,
      Duration timeout) {
    this(
        providerId,
        scope,
        configuration,
        authenticator,
        verifier,
        maximumContexts,
        authenticationWorkers,
        timeout,
        ConnectionFactory::createAsyncConnection);
  }

  HBaseBackendIdentityProvider(
      String providerId,
      AuthenticationScope scope,
      Configuration configuration,
      HBaseCredentialAuthenticator authenticator,
      HBaseAccessVerifier verifier,
      int maximumContexts,
      int authenticationWorkers,
      Duration timeout,
      ConnectionOpener opener) {
    if (providerId == null || !providerId.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException("backend provider ID must be a configured identifier");
    }
    if (maximumContexts < 1
        || maximumContexts > 1024
        || authenticationWorkers < 1
        || authenticationWorkers > Math.min(maximumContexts, 32)) {
      throw new IllegalArgumentException("backend authentication capacity is invalid");
    }
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofMinutes(5)) > 0) {
      throw new IllegalArgumentException("backend authentication timeout must be within 5 minutes");
    }
    this.providerId = providerId;
    this.scope = Objects.requireNonNull(scope, "scope");
    this.configuration = new Configuration(Objects.requireNonNull(configuration, "configuration"));
    this.authenticator = Objects.requireNonNull(authenticator, "authenticator");
    authenticator.validateConfiguration(this.configuration);
    this.configuration.setBoolean(HBaseCredentialAuthenticator.SIMPLE_FALLBACK, false);
    this.verifier = Objects.requireNonNull(verifier, "verifier");
    this.opener = Objects.requireNonNull(opener, "opener");
    this.maximumContexts = maximumContexts;
    this.maximumNotifications = authenticationWorkers;
    this.timeout = timeout;
    workers = pool(authenticationWorkers, maximumContexts, "hbase-auth-");
    completions = pool(1, maximumContexts * 3, "hbase-auth-completion-");
    // Reserve one notification slot before accepting backend work. A blocked caller callback must
    // not occupy the thread reserved for another accepted attempt's result or deadline.
    notifications =
        pool(authenticationWorkers, authenticationWorkers, "hbase-auth-notification-");
    accessNotifications =
        pool(authenticationWorkers, authenticationWorkers, "hbase-access-notification-");
    deadlines =
        new ScheduledThreadPoolExecutor(
            1, Thread.ofPlatform().daemon().name("hbase-auth-deadline-", 0).factory());
    deadlines.setRemoveOnCancelPolicy(true);
    deadlines.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
  }

  @Override
  public String providerId() {
    return providerId;
  }

  /** True only after every accepted identity attempt has drained and released its resources. */
  public boolean isFullyDrained() {
    synchronized (gate) {
      return contexts.isEmpty();
    }
  }

  /** Completes after accepted access checks and their result callbacks have both drained. */
  public CompletionStage<Void> accessChecksDrained() {
    synchronized (gate) {
      return accessChecksDrained.minimalCompletionStage();
    }
  }

  @Override
  public CompletionStage<AuthorizedBackendIdentity> authenticate(
      BackendCredentials credentials, AuthenticationScope requestedScope) {
    Objects.requireNonNull(credentials, "credentials");
    Objects.requireNonNull(requestedScope, "requestedScope");
    synchronized (gate) {
      if (closed) {
        return rejected(Reason.BACKEND_UNAVAILABLE);
      }
      if (!scope.equals(requestedScope)) {
        return rejected(Reason.INSUFFICIENT_PERMISSION);
      }
      if (contexts.size() >= maximumContexts || pendingNotifications >= maximumNotifications) {
        return rejected(Reason.RESOURCE_LIMIT);
      }
      BackendCredentials copy;
      try {
        HBaseAuthenticators.requirePrincipal(credentials.principal());
        char[] secret = credentials.copySecret(32_768);
        try {
          copy = new BackendCredentials(credentials.principal(), secret);
        } finally {
          Arrays.fill(secret, '\0');
        }
      } catch (IllegalArgumentException | IllegalStateException failure) {
        return rejected(Reason.INVALID_CREDENTIALS);
      }
      return start(new Attempt(copy, () -> authenticator.authenticate(copy, scope)));
    }
  }

  /**
   * Trusted operator/bootstrap entrypoint for keytab, ticket-cache, or other controlled identity
   * activation. Never expose this as client Authenticate or derive its source from a
   * client-supplied username/path. The source runs on a bounded worker; the provider owns its
   * returned context. The same backend permission check and resource limits apply as for submitted
   * credentials.
   */
  public CompletionStage<AuthorizedBackendIdentity> activateTrusted(
      Callable<? extends HBaseUserContext> source) {
    Objects.requireNonNull(source, "source");
    synchronized (gate) {
      if (closed) {
        return rejected(Reason.BACKEND_UNAVAILABLE);
      }
      if (contexts.size() >= maximumContexts || pendingNotifications >= maximumNotifications) {
        return rejected(Reason.RESOURCE_LIMIT);
      }
      return start(new Attempt(null, source));
    }
  }

  /** Called while holding gate, so accepted work is submitted before shutdown can start. */
  private CompletionStage<AuthorizedBackendIdentity> start(Attempt attempt) {
    contexts.put(attempt.reference, attempt);
    pendingNotifications++;
    attempt.deadline =
        deadlines.schedule(
            () -> attempt.stop(Reason.TIMEOUT), timeout.toNanos(), TimeUnit.NANOSECONDS);
    attempt.begin();
    return attempt.result;
  }

  @Override
  public CompletionStage<AsyncConnection> connectionFor(AuthorizedBackendIdentity identity) {
    Objects.requireNonNull(identity, "identity");
    synchronized (gate) {
      if (closed) {
        return rejected(Reason.BACKEND_UNAVAILABLE);
      }
      Attempt attempt = contexts.get(identity.contextRef());
      if (attempt == null) {
        return rejected(Reason.INSUFFICIENT_PERMISSION);
      }
      synchronized (attempt) {
        if (attempt.identity != identity || attempt.stopped || attempt.connection == null) {
          return rejected(Reason.INSUFFICIENT_PERMISSION);
        }
        return CompletableFuture.completedFuture(attempt.connection);
      }
    }
  }

  @Override
  public BackendIdentityBorrow borrow(AuthorizedBackendIdentity identity)
      throws BackendIdentityException {
    Objects.requireNonNull(identity, "identity");
    synchronized (gate) {
      if (closed) throw new BackendIdentityException(Reason.BACKEND_UNAVAILABLE);
      Attempt attempt = contexts.get(identity.contextRef());
      if (attempt == null) throw new BackendIdentityException(Reason.INSUFFICIENT_PERMISSION);
      synchronized (attempt) {
        if (attempt.identity != identity || attempt.stopped || attempt.retirementRequested
            || attempt.resourcesClosed || attempt.connection == null) {
          throw new BackendIdentityException(Reason.INSUFFICIENT_PERMISSION);
        }
        attempt.borrowers++;
      }
      return new IdentityBorrow(attempt, identity);
    }
  }

  /** Rechecks the configured permissions on this identity's actual backend connection. */
  public CompletionStage<Void> verifyCurrentAccess(AuthorizedBackendIdentity identity) {
    Objects.requireNonNull(identity, "identity");
    synchronized (gate) {
      if (closed) return rejected(Reason.BACKEND_UNAVAILABLE);
      if (pendingAccessChecks >= maximumNotifications) return rejected(Reason.RESOURCE_LIMIT);
      Attempt attempt = contexts.get(identity.contextRef());
      if (attempt == null) return rejected(Reason.INSUFFICIENT_PERMISSION);
      final BackendIdentityBorrow held;
      try {
        held = borrow(identity);
      } catch (BackendIdentityException failure) {
        return CompletableFuture.failedFuture(failure);
      }
      if (pendingAccessChecks++ == 0) accessChecksDrained = new CompletableFuture<>();
      return new AccessRecheck(attempt, identity, held, attempt.connection).start();
    }
  }

  @Override
  public CompletionStage<Void> retire(AuthorizedBackendIdentity identity) {
    return retireIdentity(identity);
  }

  /** Releases a provider-owned identity that has not been handed to a business request. */
  public CompletionStage<Void> discardUnpublished(AuthorizedBackendIdentity identity) {
    return retireIdentity(identity);
  }

  /** Releases one published identity without closing this provider or other identity contexts. */
  public CompletionStage<Void> release(AuthorizedBackendIdentity identity) {
    return retireIdentity(identity);
  }

  private CompletionStage<Void> retireIdentity(AuthorizedBackendIdentity identity) {
    Objects.requireNonNull(identity, "identity");
    Attempt attempt;
    synchronized (gate) {
      attempt = contexts.get(identity.contextRef());
      if (attempt == null) {
        return retiredIdentities.get(identity.contextRef()) == identity
            ? CompletableFuture.completedFuture(null)
            : rejected(Reason.INSUFFICIENT_PERMISSION);
      }
    }
    return attempt.retire(identity);
  }

  @Override
  public void close() {
    ArrayList<Attempt> snapshot;
    synchronized (gate) {
      closed = true;
      snapshot = new ArrayList<>(contexts.values());
    }
    for (Attempt attempt : snapshot) {
      attempt.stop(Reason.CANCELLED);
    }
    workers.shutdown();
    int failures = 0;
    for (Attempt attempt : snapshot) {
      if (!attempt.cleanup()) {
        failures++;
      }
    }
    shutdownWhenEmpty();
    if (failures != 0) {
      throw new IllegalStateException(
          "backend authentication resource cleanup failed: " + failures);
    }
  }

  private void retire(Attempt attempt, AuthorizedBackendIdentity identity) {
    synchronized (gate) {
      contexts.remove(attempt.reference, attempt);
      if (identity != null) {
        retiredIdentities.put(identity.contextRef(), identity);
        while (retiredIdentities.size() > maximumContexts) {
          String eldest = retiredIdentities.keySet().iterator().next();
          retiredIdentities.remove(eldest);
        }
      }
    }
    shutdownWhenEmpty();
  }

  private void releaseNotification(Attempt attempt) {
    synchronized (gate) {
      pendingNotifications--;
    }
    attempt.retireWhenFinished();
  }

  private void shutdownWhenEmpty() {
    synchronized (gate) {
      if (closed && contexts.isEmpty()) {
        completions.shutdown();
        deadlines.shutdown();
        notifications.shutdown();
        accessNotifications.shutdown();
      }
    }
  }

  private static ThreadPoolExecutor pool(int threads, int queueSize, String name) {
    var executor =
        new ThreadPoolExecutor(
            threads,
            threads,
            30,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(queueSize),
            Thread.ofPlatform().daemon().name(name, 0).factory(),
            new ThreadPoolExecutor.AbortPolicy());
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  private static <T> CompletableFuture<T> rejected(Reason reason) {
    return CompletableFuture.failedFuture(new BackendIdentityException(reason));
  }

  /** Owns the verification borrow until the original backend operation physically completes. */
  private final class AccessRecheck {
    private final Attempt attempt;
    private final AuthorizedBackendIdentity identity;
    private final BackendIdentityBorrow held;
    private final AsyncConnection connection;
    private final CompletableFuture<Void> result = new CompletableFuture<>();
    private ScheduledFuture<?> deadline;
    private boolean settled;
    private boolean drained;
    private boolean notified;

    private AccessRecheck(Attempt attempt, AuthorizedBackendIdentity identity,
        BackendIdentityBorrow held, AsyncConnection connection) {
      this.attempt = attempt;
      this.identity = identity;
      this.held = held;
      this.connection = connection;
    }

    private CompletionStage<Void> start() {
      try {
        deadline = deadlines.schedule(() -> settle(Reason.TIMEOUT),
            timeout.toNanos(), TimeUnit.NANOSECONDS);
        CompletableFuture.supplyAsync(
            () -> Objects.requireNonNull(verifier.verify(connection, scope), "access verification stage"),
            completions).thenCompose(stage -> stage).whenComplete(this::finish);
      } catch (RuntimeException failure) {
        finish(null, failure);
      }
      return result.minimalCompletionStage();
    }

    private void finish(Void ignored, Throwable failure) {
      if (deadline != null) deadline.cancel(false);
      Reason reason = failure == null ? null : switch (HBaseFailures.classify(failure).category()) {
        case AUTHENTICATION -> Reason.INVALID_CREDENTIALS;
        case PERMISSION -> Reason.INSUFFICIENT_PERMISSION;
        case RESOURCE_LIMIT -> Reason.RESOURCE_LIMIT;
        case TIMEOUT -> Reason.TIMEOUT;
        case CANCELLED -> Reason.CANCELLED;
        default -> Reason.BACKEND_UNAVAILABLE;
      };
      settle(reason);
      held.release().whenComplete((released, releaseFailure) -> markDrained());
    }

    private void settle(Reason failure) {
      synchronized (this) {
        if (settled) return;
        synchronized (attempt) {
          if (failure == null && (attempt.stopped || attempt.identity != identity)) {
            failure = Reason.CANCELLED;
          }
        }
        settled = true;
        if (failure != null && failure != Reason.RESOURCE_LIMIT) attempt.retire(identity);
        Reason selected = failure;
        // Submit while the verification borrow still prevents the executor from being shut down.
        accessNotifications.execute(() -> {
          try {
            if (selected == null) result.complete(null);
            else result.completeExceptionally(new BackendIdentityException(selected));
          } finally {
            markNotified();
          }
        });
      }
    }

    private void markDrained() {
      boolean release;
      synchronized (this) {
        drained = true;
        release = notified;
      }
      if (release) releaseCapacity();
    }

    private void markNotified() {
      boolean release;
      synchronized (this) {
        notified = true;
        release = drained;
      }
      if (release) releaseCapacity();
    }

    private void releaseCapacity() {
      CompletableFuture<Void> drainedChecks = null;
      synchronized (gate) {
        pendingAccessChecks--;
        if (pendingAccessChecks == 0) drainedChecks = accessChecksDrained;
      }
      if (drainedChecks != null) drainedChecks.complete(null);
    }
  }

  private final class IdentityBorrow implements BackendIdentityBorrow {
    private final Attempt attempt;
    private final AuthorizedBackendIdentity identity;
    private final AtomicReference<CompletableFuture<Void>> release = new AtomicReference<>();

    private IdentityBorrow(Attempt attempt, AuthorizedBackendIdentity identity) {
      this.attempt = attempt;
      this.identity = identity;
    }

    @Override
    public AuthorizedBackendIdentity identity() {
      return identity;
    }

    @Override
    public CompletionStage<Void> release() {
      CompletableFuture<Void> completion = release.get();
      if (completion == null) {
        CompletableFuture<Void> created = new CompletableFuture<>();
        if (release.compareAndSet(null, created)) {
          completion = created;
          try {
            attempt.releaseBorrow().whenComplete((ignored, failure) -> {
              if (failure == null) created.complete(null);
              else created.completeExceptionally(failure);
            });
          } catch (RuntimeException failure) {
            created.completeExceptionally(failure);
          } catch (Error fatal) {
            created.completeExceptionally(fatal);
            throw fatal;
          }
        } else {
          completion = release.get();
        }
      }
      return completion.minimalCompletionStage();
    }
  }

  private final class Attempt {
    final String reference = UUID.randomUUID().toString();
    final BackendCredentials credentials;
    final Callable<? extends HBaseUserContext> userSource;
    final CompletableFuture<AuthorizedBackendIdentity> result = new CompletableFuture<>();
    volatile ScheduledFuture<?> deadline;
    HBaseUserContext user;
    AsyncConnection connection;
    AuthorizedBackendIdentity identity;
    Thread loginWorker;
    boolean stopped;
    boolean workFinished;
    boolean cleaning;
    boolean resourcesClosed;
    boolean notificationScheduled;
    boolean notificationFinished;
    boolean retirementRequested;
    boolean retirementFinalized;
    boolean cleanupRetryScheduled;
    int cleanupRetryCount;
    int borrowers;
    CompletableFuture<Void> retirementCompletion = new CompletableFuture<>();

    Attempt(BackendCredentials credentials, Callable<? extends HBaseUserContext> userSource) {
      this.credentials = credentials;
      this.userSource = userSource;
      result.whenComplete(
          (ignored, failure) -> {
            if (result.isCancelled()) {
              stop(Reason.CANCELLED);
              // Cancellation may arrive after finish has checked whether cleanup is necessary.
              synchronized (this) {
                if (!resourcesClosed) {
                  completions.execute(this::cleanup);
                }
              }
            }
          });
    }

    void begin() {
      CompletableFuture.supplyAsync(this::openUser, workers)
          .thenCompose(
              context -> {
                synchronized (this) {
                  if (stopped) {
                    return HBaseBackendIdentityProvider.<AsyncConnection>rejected(Reason.CANCELLED);
                  }
                }
                // Keep the original future: cancelling it can discard a late connection resource.
                return opener.open(new Configuration(configuration), context.user());
              })
          .thenComposeAsync(
              opened -> {
                synchronized (this) {
                  connection = Objects.requireNonNull(opened, "backend connection");
                  if (stopped) {
                    return HBaseBackendIdentityProvider.<Void>rejected(Reason.CANCELLED);
                  }
                }
                return verifier.verify(opened, scope);
              },
              completions)
          .whenCompleteAsync((ignored, failure) -> finish(failure), completions);
    }

    HBaseUserContext openUser() {
      try {
        synchronized (this) {
          if (stopped) {
            throw new CompletionException(new BackendIdentityException(Reason.CANCELLED));
          }
          loginWorker = Thread.currentThread();
        }
        HBaseUserContext opened = userSource.call();
        synchronized (this) {
          user = Objects.requireNonNull(opened, "backend user context");
        }
        return opened;
      } catch (IllegalArgumentException failure) {
        throw new CompletionException(new BackendIdentityException(Reason.INVALID_CREDENTIALS));
      } catch (Exception failure) {
        throw new CompletionException(failure);
      } finally {
        if (credentials != null) {
          credentials.close();
        }
        synchronized (this) {
          loginWorker = null;
        }
      }
    }

    void finish(Throwable failure) {
      AuthorizedBackendIdentity published = null;
      synchronized (this) {
        workFinished = true;
        if (failure != null || closed || result.isCancelled()) {
          stopped = true;
        }
        if (!stopped) {
          try {
            var generation = Objects.requireNonNull(user.credentialGeneration(),
                "credential generation attestation");
            String subject = user.user().getName();
            published = generation.isPresent()
                ? new AuthorizedBackendIdentity(providerId, subject, reference, scope,
                    Set.of(scope.tableGroupId()), generation.getAsLong())
                : AuthorizedBackendIdentity.withUnknownCredentialGeneration(providerId, subject,
                    reference, scope, Set.of(scope.tableGroupId()));
            identity = published;
          } catch (RuntimeException invalidContext) {
            stopped = true;
            failure = new BackendIdentityException(Reason.INVALID_CREDENTIALS);
          }
        }
      }
      deadline.cancel(false);
      if (published != null) {
        notifyResult(published, null);
        // A ready identity is retained; cancellation and close own its eventual cleanup.
        return;
      } else if (failure != null) {
        Reason reason =
            switch (HBaseFailures.classify(failure).category()) {
              case AUTHENTICATION -> Reason.INVALID_CREDENTIALS;
              case PERMISSION -> Reason.INSUFFICIENT_PERMISSION;
              case RESOURCE_LIMIT -> Reason.RESOURCE_LIMIT;
              case TIMEOUT -> Reason.TIMEOUT;
              case CANCELLED -> Reason.CANCELLED;
              default -> Reason.BACKEND_UNAVAILABLE;
            };
        notifyResult(null, reason);
      } else {
        notifyResult(null, Reason.CANCELLED);
      }
      cleanup();
    }

    void stop(Reason reason) {
      synchronized (this) {
        if (reason == Reason.TIMEOUT && identity != null) {
          return;
        }
        stopped = true;
        retirementRequested = true;
        if (loginWorker != null) {
          loginWorker.interrupt();
        }
      }
      notifyResult(null, reason);
    }

    private void notifyResult(AuthorizedBackendIdentity published, Reason reason) {
      synchronized (this) {
        if (notificationScheduled) {
          return;
        }
        notificationScheduled = true;
      }
      notifications.execute(() -> deliverResult(published, reason));
    }

    private void deliverResult(AuthorizedBackendIdentity published, Reason reason) {
      try {
        synchronized (this) {
          if (published != null && stopped) {
            published = null;
            reason = Reason.CANCELLED;
          }
        }
        if (published == null) {
          result.completeExceptionally(new BackendIdentityException(reason));
        } else if (!result.complete(published)) {
          stop(Reason.CANCELLED);
          cleanup();
        }
      } finally {
        synchronized (this) {
          notificationFinished = true;
        }
        releaseNotification(this);
      }
    }

    private void retireWhenFinished() {
      AuthorizedBackendIdentity retiredIdentity;
      synchronized (this) {
        if (!resourcesClosed || !notificationFinished || borrowers != 0 || retirementFinalized) return;
        retirementFinalized = true;
        retiredIdentity = identity;
      }
      HBaseBackendIdentityProvider.this.retire(this, retiredIdentity);
    }

    private CompletionStage<Void> retire(AuthorizedBackendIdentity expected) {
      CompletableFuture<Void> retirement;
      synchronized (this) {
        if (identity != expected) {
          return HBaseBackendIdentityProvider.rejected(Reason.INSUFFICIENT_PERMISSION);
        }
        if (resourcesClosed) return CompletableFuture.completedFuture(null);
        if (retirementCompletion.isCompletedExceptionally()) {
          retirementCompletion = new CompletableFuture<>();
        }
        retirementRequested = true;
        stopped = true;
        retirement = retirementCompletion;
      }
      notifyResult(null, Reason.CANCELLED);
      cleanup();
      return retirement.minimalCompletionStage();
    }

    private CompletionStage<Void> releaseBorrow() {
      boolean cleanupRequired;
      synchronized (this) {
        if (borrowers <= 0) {
          return CompletableFuture.failedFuture(
              new IllegalStateException("backend identity borrow accounting underflow"));
        }
        borrowers--;
        cleanupRequired = stopped && borrowers == 0;
      }
      if (cleanupRequired) cleanup();
      return CompletableFuture.completedFuture(null);
    }

    /** Retains capacity after failed cleanup and retries on a bounded schedule. */
    boolean cleanup() {
      synchronized (this) {
        if (!stopped || !workFinished || cleaning || borrowers != 0 || resourcesClosed) {
          return true;
        }
        cleaning = true;
      }
      Throwable cleanupFailure = null;
      try {
        if (connection != null) {
          AsyncConnection selectedConnection = connection;
          try {
            selectedConnection.close();
            connection = null;
          } catch (IOException | RuntimeException failure) {
            cleanupFailure = failure;
          }
        }
        if (cleanupFailure == null && user != null) {
          HBaseUserContext selectedUser = user;
          try {
            selectedUser.close();
            user = null;
          } catch (IOException | RuntimeException failure) {
            cleanupFailure = failure;
          }
        }
      } catch (Error fatal) {
        synchronized (this) {
          cleaning = false;
        }
        throw fatal;
      }
      boolean success;
      CompletableFuture<Void> completedRetirement;
      synchronized (this) {
        success = connection == null && user == null;
        resourcesClosed = success;
        cleaning = false;
        completedRetirement = success && retirementRequested ? retirementCompletion : null;
        if (!success && retirementRequested && !retirementCompletion.isDone()) {
          retirementCompletion.completeExceptionally(
              new IllegalStateException("backend identity resource cleanup failed"));
        }
      }
      if (success) {
        // Resource cleanup may finish while a caller's result callback is still running. Do not
        // make that callback wait for itself; its notification capacity stays reserved separately.
        if (completedRetirement != null) completedRetirement.complete(null);
        retireWhenFinished();
      } else {
        scheduleClosedCleanupRetry();
      }
      return success;
    }

    private void scheduleClosedCleanupRetry() {
      int retry;
      synchronized (this) {
        if (!closed || !stopped || !workFinished || borrowers != 0 || resourcesClosed
            || cleanupRetryScheduled || cleanupRetryCount >= MAX_CLOSED_CLEANUP_RETRIES) {
          return;
        }
        retry = ++cleanupRetryCount;
        cleanupRetryScheduled = true;
      }
      try {
        deadlines.schedule(() -> {
          try {
            completions.execute(() -> {
              synchronized (Attempt.this) {
                cleanupRetryScheduled = false;
              }
              if (!cleanup()) scheduleClosedCleanupRetry();
            });
          } catch (RejectedExecutionException rejected) {
            synchronized (Attempt.this) {
              cleanupRetryScheduled = false;
            }
          }
        }, CLEANUP_RETRY_DELAY_MILLIS * retry, TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException rejected) {
        synchronized (this) {
          cleanupRetryScheduled = false;
        }
      }
    }
  }
}
