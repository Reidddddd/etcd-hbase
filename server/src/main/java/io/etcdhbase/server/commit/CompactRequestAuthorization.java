package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseAccessVerifier;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.error.HBaseFailures;
import io.etcdhbase.hbase.identity.HBaseAccessVerifiers;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.access.Permission.Action;

/** Binds Compact to one request identity and rechecks its actual table-group ADMIN permissions. */
final class CompactRequestAuthorization {
  private final AuthenticatedRequestIdentityResolver identities;
  private final HBaseConnectionFactory connections;
  private final HBaseAccessVerifier management;

  CompactRequestAuthorization(String tableGroup, AuthorizedBackendIdentity serviceIdentity,
      ByteKey serviceAuthorization, AuthSessionService sessions,
      HBaseConnectionFactory connections, List<TableName> tables) {
    identities = new AuthenticatedRequestIdentityResolver(
        tableGroup, serviceIdentity, serviceAuthorization, sessions);
    this.connections = Objects.requireNonNull(connections, "connections");
    management = HBaseAccessVerifiers.tablePermissions(tables.stream()
        .map(table -> new HBaseAccessVerifiers.TableAccess(table, Set.of(Action.ADMIN))).toList());
  }

  CompletableFuture<AuthenticatedRequestIdentityResolver.ResolvedRequestIdentity> resolve(
      Optional<String> token, RequestScope scope) {
    return identities.resolve(token, scope).thenApply(identity -> {
      identity.releaseAfterPhysicalDrain(scope);
      return identity;
    });
  }

  CommitAuthorizationProof captureProof(
      AuthenticatedRequestIdentityResolver.ResolvedRequestIdentity identity, long generation) {
    ByteKey proofIdentity = identity.sessionIdentity() == null
        ? identity.authorizationReference()
        : SessionAuthorizationContextReference.fromSession(identity.sessionIdentity().session())
            .publicationProofIdentity(identity.authorizationDigest());
    return CommitAuthorizationProof.capture(proofIdentity, generation,
        (expected, currentGeneration, scope) -> {
          if (!proofIdentity.equals(expected)) {
            return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.PERMISSION));
          }
          return identities.revalidate(identity, scope).thenCompose(ignored ->
              verifyManagement(identity.identity(), scope));
        });
  }

  private CompletableFuture<Void> verifyManagement(
      AuthorizedBackendIdentity identity, RequestScope scope) {
    return startBackendOperation(scope, () -> connections.connectionFor(identity))
        .thenCompose(connection -> startBackendOperation(scope, () -> management.verify(connection, identity.scope())))
        .exceptionallyCompose(failure -> {
          Throwable cause = CompletionFailures.unwrapCompletion(failure);
          if (cause instanceof Error fatal) return CompletableFuture.failedFuture(fatal);
          return CompletableFuture.failedFuture(cause instanceof UnsupportedOperationException
              ? new BackendFailure(BackendFailure.Category.UNAVAILABLE, cause)
              : HBaseFailures.classify(cause));
        });
  }

  /** Keep the user's borrow until the real producer drains, even after its request copy is cancelled. */
  private static <T> CompletableFuture<T> startBackendOperation(
      RequestScope scope, Supplier<? extends CompletionStage<T>> start) {
    var drained = new CompletableFuture<Void>();
    var result = new CompletableFuture<T>();
    boolean accepted = scope.startIfOpen(() -> {
      scope.trackPhysicalProducer(drained);
      try {
        start.get().whenComplete((value, failure) -> {
          try {
            if (failure == null) result.complete(value);
            else result.completeExceptionally(failure);
          } finally {
            drained.complete(null);
          }
        });
      } catch (RuntimeException | Error failure) {
        result.completeExceptionally(failure);
        drained.complete(null);
        if (failure instanceof Error fatal) throw fatal;
      }
    });
    if (!accepted) return CompletableFuture.failedFuture(new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED));
    return scope.track(result.copy());
  }
}
