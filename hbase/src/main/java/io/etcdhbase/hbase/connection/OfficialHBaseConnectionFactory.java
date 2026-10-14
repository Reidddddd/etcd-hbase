package io.etcdhbase.hbase.connection;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * Bounded, identity-scoped manager for official HBase {@link AsyncConnection}s.
 *
 * <p>Entries are retained until this manager closes. The hard limit rejects new identities instead
 * of evicting a connection that may still be serving requests. Failed creations are removed so a
 * later request can retry. The configuration constructor is only for explicit development
 * no-auth identities; target authentication requires a provider-specific connection creator after
 * the target mechanism is known. Each entry serializes connection publication with shutdown while
 * keeping future callbacks outside the global manager lock.
 */
public final class OfficialHBaseConnectionFactory implements HBaseConnectionFactory {
  private static final System.Logger LOGGER =
      System.getLogger(OfficialHBaseConnectionFactory.class.getName());

  private final Object lock = new Object();
  private final int maxConnections;
  private final Function<AuthorizedBackendIdentity, ? extends CompletionStage<AsyncConnection>>
      connectionCreator;
  private final Supplier<CompletableFuture<AsyncConnection>> connectionFutureFactory;
  private final Map<IdentityKey, Entry> connections = new HashMap<>();
  private boolean closed;

  /** Creates connections with the configured process identity for loopback development only. */
  public OfficialHBaseConnectionFactory(
      Configuration configuration, String connectionProfileId, int maxConnections) {
    this(maxConnections, developmentCreator(configuration, connectionProfileId));
  }

  /**
   * Creates a bounded manager with a provider-specific connection creator.
   *
   * <p>The creator receives an immutable identity context. It must not log that context or expose
   * its opaque reference in exceptions.
   */
  public OfficialHBaseConnectionFactory(
      int maxConnections,
      Function<AuthorizedBackendIdentity, ? extends CompletionStage<AsyncConnection>>
          connectionCreator) {
    this(maxConnections, connectionCreator, CompletableFuture::new);
  }

  /** Package-private future seam lets concurrency tests pause connection publication. */
  OfficialHBaseConnectionFactory(
      int maxConnections,
      Function<AuthorizedBackendIdentity, ? extends CompletionStage<AsyncConnection>>
          connectionCreator,
      Supplier<CompletableFuture<AsyncConnection>> connectionFutureFactory) {
    if (maxConnections < 1) {
      throw new IllegalArgumentException("maxConnections must be positive");
    }
    this.maxConnections = maxConnections;
    this.connectionCreator = Objects.requireNonNull(connectionCreator, "connectionCreator");
    this.connectionFutureFactory =
        Objects.requireNonNull(connectionFutureFactory, "connectionFutureFactory");
  }

  @Override
  public CompletionStage<AsyncConnection> connectionFor(AuthorizedBackendIdentity identity) {
    Objects.requireNonNull(identity, "identity");
    IdentityKey key = IdentityKey.from(identity);
    Entry entry;
    boolean create;
    synchronized (lock) {
      if (closed) {
        return failed(new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
      entry = connections.get(key);
      if (entry != null) {
        create = false;
      } else if (connections.size() >= maxConnections) {
        return failed(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
      } else {
        entry = new Entry(connectionFutureFactory.get());
        connections.put(key, entry);
        create = true;
      }
    }

    if (create) {
      startConnection(key, entry, identity);
    }
    // Cancelling one caller's dependent future must not cancel creation shared with other callers.
    return entry.connection.thenApply(Function.identity());
  }

  @Override
  public void close() {
    List<Entry> toClose = new ArrayList<>();
    List<Entry> toFail = new ArrayList<>();
    synchronized (lock) {
      if (closed) {
        return;
      }
      closed = true;
      for (Entry entry : connections.values()) {
        if (entry.state == EntryState.READY) {
          toClose.add(entry);
          entry.state = EntryState.CLOSED;
        } else if (entry.state == EntryState.CREATING) {
          entry.state = EntryState.CLOSED;
          toFail.add(entry);
        }
      }
      connections.clear();
    }

    for (Entry entry : toFail) {
      synchronized (entry.publicationLock) {
        entry.connection.completeExceptionally(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE));
      }
    }

    int closeFailures = 0;
    for (Entry entry : toClose) {
      synchronized (entry.publicationLock) {
        if (!entry.connection.isDone()) {
          entry.connection.completeExceptionally(
              new BackendFailure(BackendFailure.Category.UNAVAILABLE));
        }
        if (!closeConnection(entry.readyConnection)) {
          closeFailures++;
        }
      }
    }
    if (closeFailures > 0) {
      throw new IllegalStateException(
          "failed to close " + closeFailures + " managed HBase connection(s)");
    }
  }

  private void startConnection(IdentityKey key, Entry entry, AuthorizedBackendIdentity identity) {
    CompletionStage<AsyncConnection> creation;
    try {
      creation =
          Objects.requireNonNull(connectionCreator.apply(identity), "connection creation stage");
    } catch (RuntimeException exception) {
      completeFailure(key, entry, exception);
      return;
    } catch (Error error) {
      completeFailure(key, entry, error);
      throw error;
    }

    try {
      creation.whenComplete(
          (connection, failure) -> {
            if (failure != null) {
              completeFailure(key, entry, failure);
            } else if (connection == null) {
              completeFailure(
                  key, entry, new IllegalStateException("connection creator returned null"));
            } else {
              completeSuccess(key, entry, connection);
            }
          });
    } catch (RuntimeException exception) {
      completeFailure(key, entry, exception);
    } catch (Error error) {
      completeFailure(key, entry, error);
      throw error;
    }
  }

  private void completeSuccess(IdentityKey key, Entry entry, AsyncConnection connection) {
    synchronized (entry.publicationLock) {
      boolean accepted;
      synchronized (lock) {
        accepted =
            !closed
                && connections.get(key) == entry
                && entry.state == EntryState.CREATING;
        if (accepted) {
          entry.state = EntryState.READY;
          entry.readyConnection = connection;
        }
      }
      if (!accepted) {
        closeConnection(connection);
        entry.connection.completeExceptionally(
            new BackendFailure(BackendFailure.Category.UNAVAILABLE));
        return;
      }
      if (!entry.connection.complete(connection)) {
        closeConnection(connection);
      }
    }
  }

  private void completeFailure(IdentityKey key, Entry entry, Throwable failure) {
    Throwable cause = unwrap(failure);
    BackendFailure safeFailure = classifyFailure(cause);
    synchronized (entry.publicationLock) {
      boolean accepted;
      synchronized (lock) {
        accepted =
            connections.get(key) == entry && entry.state == EntryState.CREATING;
        if (accepted) {
          connections.remove(key);
          entry.state = EntryState.FAILED;
        }
      }
      if (accepted) {
        entry.connection.completeExceptionally(safeFailure);
      }
    }
  }

  private static BackendFailure classifyFailure(Throwable cause) {
    if (cause instanceof BackendFailure backendFailure) {
      return backendFailure;
    }
    if (cause instanceof BackendIdentityException identityException) {
      BackendFailure.Category category =
          switch (identityException.reason()) {
            case INVALID_CREDENTIALS -> BackendFailure.Category.AUTHENTICATION;
            case INSUFFICIENT_PERMISSION -> BackendFailure.Category.PERMISSION;
            case BACKEND_UNAVAILABLE -> BackendFailure.Category.UNAVAILABLE;
          };
      return new BackendFailure(category, cause);
    }
    if (cause instanceof CancellationException || cause instanceof InterruptedException) {
      return new BackendFailure(BackendFailure.Category.CANCELLED, cause);
    }
    if (cause instanceof TimeoutException) {
      return new BackendFailure(BackendFailure.Category.TIMEOUT, cause);
    }
    return new BackendFailure(BackendFailure.Category.UNAVAILABLE, cause);
  }

  private boolean closeConnection(AsyncConnection connection) {
    try {
      connection.close();
      return true;
    } catch (IOException | RuntimeException failure) {
      LOGGER.log(System.Logger.Level.WARNING, "Failed to close a managed HBase connection");
      return false;
    }
  }

  private static Function<AuthorizedBackendIdentity, CompletionStage<AsyncConnection>>
      developmentCreator(Configuration configuration, String connectionProfileId) {
    Configuration snapshot =
        new Configuration(Objects.requireNonNull(configuration, "configuration"));
    String requiredProfile = requireText(connectionProfileId, "connectionProfileId");
    return identity -> {
      if (!BackendIdentityProvider.DEVELOPMENT_NO_AUTH_PROVIDER_ID.equals(identity.providerId())) {
        return failed(new BackendFailure(BackendFailure.Category.AUTHENTICATION));
      }
      if (!requiredProfile.equals(identity.scope().connectionProfileId())) {
        return failed(new BackendFailure(BackendFailure.Category.PERMISSION));
      }
      return ConnectionFactory.createAsyncConnection(new Configuration(snapshot));
    };
  }

  private static <T> CompletionStage<T> failed(Throwable failure) {
    return CompletableFuture.failedFuture(failure);
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable current = failure;
    while ((current instanceof java.util.concurrent.CompletionException
            || current instanceof java.util.concurrent.ExecutionException)
        && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static final class Entry {
    private final CompletableFuture<AsyncConnection> connection;
    private final Object publicationLock = new Object();
    private EntryState state = EntryState.CREATING;
    private AsyncConnection readyConnection;

    private Entry(CompletableFuture<AsyncConnection> connection) {
      this.connection = Objects.requireNonNull(connection, "connection future");
    }
  }

  private enum EntryState {
    CREATING,
    READY,
    FAILED,
    CLOSED
  }

  private record IdentityKey(
      String providerId,
      String subject,
      String contextRef,
      String tableGroupId,
      String connectionProfileId,
      long credentialGeneration) {
    private static IdentityKey from(AuthorizedBackendIdentity identity) {
      return new IdentityKey(
          identity.providerId(),
          identity.subject(),
          identity.contextRef(),
          identity.scope().tableGroupId(),
          identity.scope().connectionProfileId(),
          identity.credentialGeneration());
    }

    @Override
    public String toString() {
      return "IdentityKey[redacted]";
    }
  }
}
