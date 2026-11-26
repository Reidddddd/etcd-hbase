package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ResultDirectory;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.InvocationMessages;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Ends one invocation's declared result window and then retires the result it used to expose.
 *
 * <p>§6.2 of the M5 design requires the window to close first and the request digest plus a
 * non-replayable proof to be retained afterwards. The retained binding (now {@code WINDOW_CLOSED})
 * *is* that proof: it keeps the request and authorization digests, so a retry of the same request
 * can never be executed as a fresh write once the result is gone.
 */
public final class InvocationWindowCloser {
  private final SystemRecordStore records;
  private final InvocationService invocations;
  private final SystemRecordRetirement retirement;

  public InvocationWindowCloser(SystemRecordStore records, InvocationService invocations,
      SystemRecordRetirement retirement) {
    this.records = Objects.requireNonNull(records, "records");
    this.invocations = Objects.requireNonNull(invocations, "invocations");
    this.retirement = Objects.requireNonNull(retirement, "retirement");
  }

  /** Why one invocation could not be closed; the record is retained in every such case. */
  public enum Skip {
    STILL_PREPARING,
    ALREADY_CLOSED,
    NO_RESULT_REFERENCE,
    RESULT_STILL_REFERENCED
  }

  public sealed interface Outcome permits Closed, Retained {
    InvocationIdView invocation();
  }

  /** The window is closed and the result has been retired (or was already gone). */
  public record Closed(InvocationIdView invocation, boolean resultRetired) implements Outcome {}

  /** The invocation was deliberately kept; {@code skip} explains why. */
  public record Retained(InvocationIdView invocation, Skip skip) implements Outcome {}

  /** Identity of the invocation this outcome refers to, kept separate from the durable binding. */
  public record InvocationIdView(ByteKey identity) {
    public InvocationIdView {
      Objects.requireNonNull(identity, "identity");
    }
  }

  /**
   * Closes one window. The order is deliberate: the invocation CAS to {@code WINDOW_CLOSED} succeeds
   * first, so a crash before the result retirement leaves a closed window that is still safe to
   * serve as "cannot query", never an open window with a missing result.
   */
  public CompletableFuture<Outcome> close(ByteKey identity, SystemRecordRetirement.Permit permit,
      RequestScope scope) {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(permit, "permit");
    Objects.requireNonNull(scope, "scope");
    var view = new InvocationIdView(identity);
    return records.read(SystemRecordType.INVOCATION, identity, scope).thenCompose(found -> {
      if (found.isEmpty()) {
        return CompletableFuture.completedFuture(new Retained(view, Skip.ALREADY_CLOSED));
      }
      InvocationBinding binding;
      try {
        binding = InvocationMessages.decode(found.orElseThrow().bytes());
      } catch (RuntimeException malformed) {
        // An undecodable binding might still be serving; retaining it is the only safe answer.
        return CompletableFuture.completedFuture(new Retained(view, Skip.NO_RESULT_REFERENCE));
      }
      if (binding.state() == InvocationBinding.State.WINDOW_CLOSED) {
        return CompletableFuture.completedFuture(new Retained(view, Skip.ALREADY_CLOSED));
      }
      if (binding.state() == InvocationBinding.State.PREPARING
          || binding.state() == InvocationBinding.State.SEALED) {
        return CompletableFuture.completedFuture(new Retained(view, Skip.STILL_PREPARING));
      }
      var reference = binding.resultReference();
      if (reference.isEmpty()) {
        return CompletableFuture.completedFuture(new Retained(view, Skip.NO_RESULT_REFERENCE));
      }
      return closeAndRetire(identity, binding, reference.orElseThrow(), permit, scope, view);
    });
  }

  private CompletableFuture<Outcome> closeAndRetire(ByteKey identity, InvocationBinding binding,
      ByteKey reference, SystemRecordRetirement.Permit permit, RequestScope scope,
      InvocationIdView view) {
    var resultIdentity = ResultIdentity.from(binding);
    // Close the window before touching the result: the retained binding becomes the proof that this
    // request was already executed, so a later retry can never run as a new write.
    return invocations.transition(binding, binding.closeQueryWindow(), scope)
        .thenCompose(closed -> retireResult(ResultDirectory.systemIdentity(resultIdentity),
            permit, scope).thenApply(retired -> (Outcome) new Closed(view, retired)));
  }

  private CompletableFuture<Boolean> retireResult(ByteKey target,
      SystemRecordRetirement.Permit permit, RequestScope scope) {
    return records.read(SystemRecordType.RESULT, target, scope).thenCompose(found -> {
      if (found.isEmpty()) return CompletableFuture.completedFuture(false);
      return retirement.retire(SystemRecordType.RESULT, target,
              ByteKey.copyOf(found.orElseThrow().bytes()), permit,
              SystemRecordRetirement.Reason.EXPIRED_INVOCATION, scope)
          .thenApply(outcome -> outcome.retiredNow() || outcome.alreadyRetired());
    });
  }
}
