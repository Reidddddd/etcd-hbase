package io.etcdhbase.hbase.connection;

import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.identity.BackendIdentityException.Reason;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.hadoop.hbase.client.AsyncConnection;

/** Routes request identities to isolated provider containers; the configured base provider is external. */
public final class HBaseConnectionRouter implements HBaseConnectionFactory {
  private final Object gate = new Object();
  private final HBaseConnectionFactory base;
  private final Map<String, Route> routes = new HashMap<>();
  private boolean closed;

  public HBaseConnectionRouter(HBaseConnectionFactory base) {
    this.base = Objects.requireNonNull(base, "base");
  }

  /** Registers an identity only after its owning provider has completed backend verification. */
  public void register(AuthorizedBackendIdentity identity, HBaseBackendIdentityProvider owner) {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(owner, "owner");
    if (!owner.providerId().equals(identity.providerId())) {
      throw new IllegalArgumentException("identity provider does not match its owner");
    }
    synchronized (gate) {
      if (closed) throw new IllegalStateException("connection router is closed");
      Route previous = routes.putIfAbsent(identity.contextRef(), new Route(identity, owner));
      if (previous != null) throw new IllegalStateException("identity context is already routed");
    }
  }

  /** Removes exactly the isolated provider route; its session manager owns provider closure. */
  public CompletionStage<Void> unregister(
      AuthorizedBackendIdentity identity, HBaseBackendIdentityProvider owner) {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(owner, "owner");
    synchronized (gate) {
      Route route = routes.get(identity.contextRef());
      if (route == null) return CompletableFuture.completedFuture(null);
      if (route.identity() != identity || route.owner() != owner) {
        return CompletableFuture.failedFuture(new BackendIdentityException(
            Reason.INSUFFICIENT_PERMISSION));
      }
      routes.remove(identity.contextRef(), route);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public CompletionStage<AsyncConnection> connectionFor(AuthorizedBackendIdentity identity) {
    Objects.requireNonNull(identity, "identity");
    Route route;
    synchronized (gate) {
      if (closed) return CompletableFuture.failedFuture(
          new BackendIdentityException(Reason.BACKEND_UNAVAILABLE));
      route = routes.get(identity.contextRef());
      if (route != null && route.identity() != identity) {
        return CompletableFuture.failedFuture(
            new BackendIdentityException(Reason.INSUFFICIENT_PERMISSION));
      }
    }
    return route == null ? base.connectionFor(identity) : route.owner().connectionFor(identity);
  }

  /** Marks routing unavailable; active routes must be retired by their owning session managers. */
  @Override
  public void close() {
    synchronized (gate) {
      closed = true;
      if (!routes.isEmpty()) {
        throw new IllegalStateException("identity routes must drain before the router closes");
      }
    }
  }

  int routedIdentityCount() {
    synchronized (gate) {
      return routes.size();
    }
  }

  private record Route(AuthorizedBackendIdentity identity, HBaseBackendIdentityProvider owner) {}
}
