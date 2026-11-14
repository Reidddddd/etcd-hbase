package io.etcdhbase.server.commit;

import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.error.CompletionFailures;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.lease.LeaseFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/** Reads the authoritative owner and forwards at most once; a post-send failure remains unknown. */
public final class MutationRelay {
  public sealed interface Result permits KnownResult, KnownRejection, SemanticRejection, LeaseRejection, ResultUnknown {}
  public record KnownResult(byte[] responseBytes) implements Result {
    public KnownResult { responseBytes = responseBytes.clone(); }
    @Override public byte[] responseBytes() { return responseBytes.clone(); }
  }
  public record KnownRejection(BackendFailure.Category category) implements Result {
    public KnownRejection { Objects.requireNonNull(category, "category"); }
  }
  public record SemanticRejection(KvFailure.Reason reason) implements Result {
    public SemanticRejection { Objects.requireNonNull(reason, "reason"); }
  }
  public record LeaseRejection(LeaseFailure.Reason reason) implements Result {
    public LeaseRejection { Objects.requireNonNull(reason, "reason"); }
  }
  public record ResultUnknown(BackendFailure.Category category) implements Result {
    public ResultUnknown {
      Objects.requireNonNull(category, "category");
    }
  }

  private final CommitControlStore control;
  private final OwnerForwarding.Directory directory;
  private final OwnerForwarding.AuthenticatedChannel channel;

  public MutationRelay(CommitControlStore control, OwnerForwarding.Directory directory,
      OwnerForwarding.AuthenticatedChannel channel) {
    this.control = Objects.requireNonNull(control, "control");
    this.directory = Objects.requireNonNull(directory, "directory");
    this.channel = Objects.requireNonNull(channel, "channel");
  }

  public CompletableFuture<Result> forward(MutationForwardRequest request, RequestScope scope) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(scope, "scope");
    if (request.hopCount() >= MutationForwardRequest.MAX_HOPS) {
      return CompletableFuture.failedFuture(new IllegalStateException("mutation forward hop limit exceeded"));
    }
    return control.read(scope)
        .thenCompose(snapshot -> requireCurrentOwner(snapshot, request))
        .thenCompose(owner -> resolveRouteAndSend(owner, request, scope));
  }

  private CompletableFuture<CurrentOwner> requireCurrentOwner(
      Optional<ControlSnapshot> snapshot,
      MutationForwardRequest request) {
    if (snapshot.isEmpty()) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("authoritative M2 control state is missing"));
    }
    CommitState state = snapshot.orElseThrow().state();
    if (state.ownerInstance().isEmpty()
        || state.ownerEpoch() != request.expectedOwnerEpoch()) {
      return CompletableFuture.failedFuture(
          new BackendFailure(BackendFailure.Category.UNAVAILABLE));
    }
    return CompletableFuture.completedFuture(
        new CurrentOwner(state.ownerInstance().orElseThrow(), state.ownerEpoch()));
  }

  private CompletableFuture<Result> resolveRouteAndSend(
      CurrentOwner owner, MutationForwardRequest request, RequestScope scope) {
    return directory.resolve(request.tableGroupId(), owner.instance(), owner.epoch())
        .thenCompose(route -> sendToVerifiedRoute(owner, route, request, scope));
  }

  private CompletableFuture<Result> sendToVerifiedRoute(
      CurrentOwner owner,
      OwnerForwarding.Route route,
      MutationForwardRequest request,
      RequestScope scope) {
    if (!route.ownerInstance().equals(owner.instance()) || route.ownerEpoch() != owner.epoch()) {
      return CompletableFuture.failedFuture(new IllegalStateException(
          "owner directory result differs from authoritative control state"));
    }
    long remaining = Math.min(request.remainingTimeoutNanos(), scope.remainingDeadlineNanos());
    if (remaining <= 0) {
      return CompletableFuture.completedFuture(new KnownRejection(BackendFailure.Category.TIMEOUT));
    }
    MutationForwardRequest forwarded = request.forwardedOnce(owner.epoch(), remaining);
    return sendOnce(route, forwarded, scope);
  }

  private CompletableFuture<Result> sendOnce(
      OwnerForwarding.Route route, MutationForwardRequest request, RequestScope scope) {
    AtomicReference<CompletableFuture<OwnerForwarding.Reply>> sentReference = new AtomicReference<>();
    boolean started;
    try {
      started = scope.startIfOpen(() -> sentReference.set(channel.sendOnce(route, request, scope)));
    } catch (RuntimeException failure) {
      return CompletableFuture.completedFuture(new ResultUnknown(classifyUnknown(failure)));
    }
    if (!started) {
      BackendFailure.Category category = scope.isDeadlineExceeded()
          ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED;
      return CompletableFuture.completedFuture(new KnownRejection(category));
    }
    CompletableFuture<OwnerForwarding.Reply> sent = sentReference.get();
    if (sent == null) {
      return CompletableFuture.completedFuture(new ResultUnknown(BackendFailure.Category.UNKNOWN));
    }
    return sent.<Result>handle(MutationRelay::mapReply);
  }

  private static Result mapReply(OwnerForwarding.Reply reply, Throwable failure) {
    if (failure != null || reply == null) {
      return new ResultUnknown(failure == null
          ? BackendFailure.Category.UNKNOWN : classifyUnknown(failure));
    }
    if (reply instanceof OwnerForwarding.Completed completed) {
      return new KnownResult(completed.operationResult());
    }
    if (reply instanceof OwnerForwarding.Rejected rejected) {
      return new KnownRejection(rejected.category());
    }
    if (reply instanceof OwnerForwarding.SemanticRejected rejected) {
      return new SemanticRejection(rejected.reason());
    }
    if (reply instanceof OwnerForwarding.LeaseRejected rejected) {
      return new LeaseRejection(rejected.reason());
    }
    return new ResultUnknown(((OwnerForwarding.Unknown) reply).category());
  }

  private static BackendFailure.Category classifyUnknown(Throwable failure) {
    Throwable cause = CompletionFailures.unwrapCompletion(failure);
    if (cause instanceof BackendFailure backend) {
      return backend.category();
    }
    return BackendFailure.Category.UNKNOWN;
  }

  private record CurrentOwner(String instance, long epoch) {}
}
