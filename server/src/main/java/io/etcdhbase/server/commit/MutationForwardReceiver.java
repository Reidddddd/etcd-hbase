package io.etcdhbase.server.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitAuthorizationProof;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/** Revalidates a forwarded request at the owner before dispatching it to the commit kernel. */
public final class MutationForwardReceiver {
  private static final long MAX_LOCAL_TIMEOUT_NANOS = MutationForwardRequest.MAX_TIMEOUT_NANOS;

  public record VerifiedAuthorization(
      AuthorizedBackendIdentity identity, ByteKey authorizationScopeDigest,
      CommitAuthorizationProof publicationProof, Runnable retireUserIdentity) {
    /** Service-only authorization has no session identity to retire. */
    public VerifiedAuthorization(AuthorizedBackendIdentity identity, ByteKey authorizationScopeDigest,
        CommitAuthorizationProof publicationProof) {
      this(identity, authorizationScopeDigest, publicationProof, () -> { });
    }

    public VerifiedAuthorization {
      Objects.requireNonNull(identity, "identity");
      Objects.requireNonNull(authorizationScopeDigest, "authorizationScopeDigest");
      Objects.requireNonNull(publicationProof, "publicationProof");
      Objects.requireNonNull(retireUserIdentity, "retireUserIdentity");
      if (authorizationScopeDigest.length() != 32) {
        throw new IllegalArgumentException("authorization scope digest must be SHA-256");
      }
    }
  }

  @FunctionalInterface
  public interface AuthorizationResolver {
    /**
     * Authenticates the transport peer and resolves the opaque context reference. The peer value
     * must come from the authenticated channel, never from an untrusted request field.
     */
    CompletableFuture<VerifiedAuthorization> verifyAndResolve(
        String authenticatedPeerIdentity, MutationForwardRequest request,
        long expectedAuthorizationBarrierGeneration, RequestScope scope);
  }

  @FunctionalInterface
  public interface MutationProcessor {
    /**
     * Processes or resolves this durable invocation. Implementations must use the returned
     * binding and never allocate a second txnId for the same invocation. They may be invoked
     * before returning Rejected, but only when they can prove no mutation was dispatched for
     * the invocation.
     */
    CompletableFuture<OwnerForwarding.Reply> process(
        MutationForwardRequest request,
        InvocationBinding binding,
        VerifiedAuthorization authorization,
        RequestScope scope);
  }

  private final CommitControlStore control;
  private final InvocationService invocations;
  private final AuthorizationResolver authorizations;
  private final MutationProcessor processor;
  private final Clock clock;

  public MutationForwardReceiver(
      CommitControlStore control,
      InvocationService invocations,
      AuthorizationResolver authorizations,
      MutationProcessor processor) {
    this(control, invocations, authorizations, processor, Clock.systemUTC());
  }

  MutationForwardReceiver(
      CommitControlStore control,
      InvocationService invocations,
      AuthorizationResolver authorizations,
      MutationProcessor processor,
      Clock clock) {
    this.control = Objects.requireNonNull(control, "control");
    this.invocations = Objects.requireNonNull(invocations, "invocations");
    this.authorizations = Objects.requireNonNull(authorizations, "authorizations");
    this.processor = Objects.requireNonNull(processor, "processor");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  /** A returned Rejected reply guarantees no mutation was dispatched for this invocation. */
  public CompletableFuture<OwnerForwarding.Reply> receive(
      String authenticatedPeerIdentity, MutationForwardRequest request, RequestScope scope) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(scope, "scope");
    long requestBudget = Math.min(request.remainingTimeoutNanos(), MAX_LOCAL_TIMEOUT_NANOS);
    RequestScope effectiveScope = scope.childWithTimeoutNanos(requestBudget);
    CompletableFuture<Void> effectiveDrain = CompletableFuture.allOf(
        effectiveScope.cleanupCompletion().toCompletableFuture(),
        effectiveScope.physicalProducersCompletion().toCompletableFuture());
    RequestScope.ScopedCleanup parentRegistration = scope.registerScopedCleanup(
        () -> {
          if (scope.isCancelled()) effectiveScope.cancel();
          else effectiveScope.close();
        },
        effectiveDrain);
    try {
      return receiveWithinScope(authenticatedPeerIdentity, request, effectiveScope)
          .whenComplete((reply, failure) -> closeScopeRegistration(parentRegistration));
    } catch (RuntimeException | Error failure) {
      closeScopeRegistration(parentRegistration);
      throw failure;
    }
  }

  private CompletableFuture<OwnerForwarding.Reply> receiveWithinScope(
      String authenticatedPeerIdentity, MutationForwardRequest request, RequestScope scope) {
    if (!validPeerIdentity(authenticatedPeerIdentity)
        || request.hopCount() != MutationForwardRequest.MAX_HOPS) {
      return unknown(BackendFailure.Category.PERMISSION);
    }
    if (scope.isDeadlineExceeded() || scope.isClosed()) {
      return unknown(scopeFailureCategory(scope));
    }
    return control.read(scope).handle((snapshot, failure) -> new ControlRead(snapshot, failure))
        .thenCompose(read -> {
          if (scope.isDeadlineExceeded() || scope.isClosed()) {
            return unknown(scopeFailureCategory(scope));
          }
          if (read.failure() != null) return unknown(category(read.failure()));
          if (read.snapshot().isEmpty()) return unknown(BackendFailure.Category.UNAVAILABLE);
          CommitState state = read.snapshot().orElseThrow().state();
          if (!isCurrentOwner(state, request.expectedOwnerEpoch())
              || state.publication().isPresent()) {
            return unknown(BackendFailure.Category.UNAVAILABLE);
          }
          return authorizeAndBind(authenticatedPeerIdentity, request, scope, state);
        });
  }

  private CompletableFuture<OwnerForwarding.Reply> authorizeAndBind(
      String peerIdentity, MutationForwardRequest request, RequestScope scope, CommitState observed) {
    CompletableFuture<VerifiedAuthorization> authorization;
    try {
      var authorizationReference = new AtomicReference<CompletableFuture<VerifiedAuthorization>>();
      if (!scope.startIfOpen(() -> authorizationReference.set(
          authorizations.verifyAndResolve(peerIdentity, request,
              observed.authorizationBarrierGeneration(), scope)))) {
        return unknown(scopeFailureCategory(scope));
      }
      authorization = authorizationReference.get();
    } catch (RuntimeException failure) {
      return unknown(scope.isDeadlineExceeded() || scope.isClosed()
          ? scopeFailureCategory(scope) : category(failure));
    }
    if (authorization == null) return unknown(BackendFailure.Category.UNKNOWN);
    return authorization.handle((verified, failure) -> new AuthorizationResult(verified, failure))
        .thenCompose(result -> {
          if (scope.isDeadlineExceeded() || scope.isClosed()) {
            return unknown(scopeFailureCategory(scope));
          }
          if (result.failure() != null) return unknown(category(result.failure()));
          if (!matchesAuthorization(request, result.authorization())
              || result.authorization().publicationProof().authorizationBarrierGeneration()
                  != observed.authorizationBarrierGeneration()) {
            return unknown(BackendFailure.Category.PERMISSION);
          }
          InvocationBinding proposed = InvocationBinding.begin(
              request.invocationId(), request.requestDigest(),
              result.authorization().authorizationScopeDigest(), TransactionId.create(),
              observed.ownerEpoch(), observed.publishedSequence(), observed.publishedRevision());
          return bindAndDispatch(request, scope, observed, result.authorization(), proposed);
        });
  }

  private CompletableFuture<OwnerForwarding.Reply> bindAndDispatch(
      MutationForwardRequest request,
      RequestScope scope,
      CommitState observed,
      VerifiedAuthorization authorization,
      InvocationBinding proposed) {
    CompletableFuture<InvocationBinding> bound;
    try {
      var bindingReference = new AtomicReference<CompletableFuture<InvocationBinding>>();
      if (!scope.startIfOpen(() -> bindingReference.set(invocations.bindOrJoin(proposed, scope)))) {
        return unknown(scopeFailureCategory(scope));
      }
      bound = bindingReference.get();
    } catch (RuntimeException failure) {
      return unknown(scope.isDeadlineExceeded() || scope.isClosed()
          ? scopeFailureCategory(scope) : category(failure));
    }
    if (bound == null) return unknown(BackendFailure.Category.UNKNOWN);
    return bound.handle((binding, failure) -> new BindingResult(binding, failure))
        .thenCompose(result -> {
          if (scope.isDeadlineExceeded() || scope.isClosed()) {
            return unknown(scopeFailureCategory(scope));
          }
          if (result.failure() != null) return unknown(category(result.failure()));
          if (!bindingBelongsToObservedAttempt(result.binding(), observed)) {
            return unknown(BackendFailure.Category.UNAVAILABLE);
          }
          return verifyOwnerBeforeDispatch(request, scope, observed, authorization, result.binding());
        });
  }

  private static boolean bindingBelongsToObservedAttempt(
      InvocationBinding binding, CommitState observed) {
    if (binding.state() != InvocationBinding.State.PREPARING) {
      return true;
    }
    // A SEALED invocation may already have been selected and archived before its reply was lost.
    // Let the operation processor resolve that durable outcome; it must not prepare the old base
    // again. Only PREPARING bindings can still be dispatched as new work under the observed base.
    return binding.preparedOwnerEpoch() == observed.ownerEpoch()
        && binding.baseSequence() == observed.publishedSequence()
        && binding.baseRevision() == observed.publishedRevision();
  }

  private CompletableFuture<OwnerForwarding.Reply> verifyOwnerBeforeDispatch(
      MutationForwardRequest request,
      RequestScope scope,
      CommitState observed,
      VerifiedAuthorization authorization,
      InvocationBinding binding) {
    return control.read(scope).handle((snapshot, failure) -> new ControlRead(snapshot, failure))
        .thenCompose(read -> {
          if (scope.isDeadlineExceeded() || scope.isClosed()) {
            return unknown(scopeFailureCategory(scope));
          }
          if (read.failure() != null) return unknown(category(read.failure()));
          if (read.snapshot().isEmpty()) return unknown(BackendFailure.Category.UNAVAILABLE);
          CommitState current = read.snapshot().orElseThrow().state();
          if (!isCurrentOwner(current, request.expectedOwnerEpoch())
              || !current.ownerInstance().equals(observed.ownerInstance())
              || current.publication().isPresent()
              || current.publishedSequence() != observed.publishedSequence()
              || current.publishedRevision() != observed.publishedRevision()) {
            return unknown(BackendFailure.Category.UNAVAILABLE);
          }
          // Only PREPARING work can still be dispatched as a new mutation. A non-preparing
          // invocation may already have a durable selected outcome that its processor must resolve.
          if (binding.state() == InvocationBinding.State.PREPARING
              && authorization.publicationProof().authorizationBarrierGeneration()
                  != current.authorizationBarrierGeneration()) {
            return unknown(BackendFailure.Category.PERMISSION);
          }
          var processingReference = new AtomicReference<CompletableFuture<OwnerForwarding.Reply>>();
          try {
            if (!scope.startIfOpen(() -> processingReference.set(processor.process(
                request, binding, authorization, scope)))) {
              return unknown(scopeFailureCategory(scope));
            }
          } catch (RuntimeException failure) {
            return unknown(scope.isDeadlineExceeded() || scope.isClosed()
                ? scopeFailureCategory(scope) : category(failure));
          }
          CompletableFuture<OwnerForwarding.Reply> processing = processingReference.get();
          if (processing == null) {
            return unknown(BackendFailure.Category.UNKNOWN);
          }
          return processing;
        });
  }

  private boolean isCurrentOwner(CommitState state, long expectedEpoch) {
    return state.ownerInstance().isPresent() && state.ownerEpoch() == expectedEpoch
        && !state.ownerLeaseExpired(clock.millis());
  }

  private static boolean matchesAuthorization(
      MutationForwardRequest request, VerifiedAuthorization verified) {
    if (verified == null) return false;
    AuthorizedBackendIdentity identity = verified.identity();
    // The reference belongs to the caller's process-local auth context. A receiving replica
    // resolves it through its trusted peer/provider boundary; comparing it with this process's
    // local reference would reject the same authorized identity on every other replica.
    return identity.scope().tableGroupId().equals(request.tableGroupId())
        && identity.allowedTableGroupIds().contains(request.tableGroupId())
        && verified.authorizationScopeDigest().equals(request.authorizationScopeDigest());
  }

  private static boolean validPeerIdentity(String identity) {
    return identity != null && !identity.isBlank() && identity.length() <= 512
        && identity.codePoints().noneMatch(Character::isISOControl);
  }

  private static CompletableFuture<OwnerForwarding.Reply> unknown(BackendFailure.Category category) {
    return CompletableFuture.completedFuture(new OwnerForwarding.Unknown(category));
  }

  private static void closeScopeRegistration(RequestScope.ScopedCleanup registration) {
    try {
      registration.close();
    } catch (Exception failure) {
      throw new IllegalStateException("failed to close forwarded request scope", failure);
    }
  }

  private static BackendFailure.Category scopeFailureCategory(RequestScope scope) {
    return scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED;
  }

  private static BackendFailure.Category category(Throwable failure) {
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    return cause instanceof BackendFailure backend
        ? backend.category() : BackendFailure.Category.UNKNOWN;
  }

  private record ControlRead(Optional<ControlSnapshot> snapshot, Throwable failure) {}
  private record AuthorizationResult(VerifiedAuthorization authorization, Throwable failure) {}
  private record BindingResult(InvocationBinding binding, Throwable failure) {}
}
