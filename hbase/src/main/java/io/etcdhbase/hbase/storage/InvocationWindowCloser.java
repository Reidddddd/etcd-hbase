package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ResultDirectory;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.InvocationMessages;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Closes a protected result window before collecting its result through the actual reference graph. */
public final class InvocationWindowCloser {
  private final HBaseSystemRecordStore records;
  private final InvocationService invocations;
  private final SystemRecordGcAuthority authority;
  private final HBaseSystemRecordClosure closure;

  public InvocationWindowCloser(HBaseSystemRecordStore records) {
    this.records = Objects.requireNonNull(records, "records");
    invocations = new InvocationService(new HBaseInvocationRepository(records));
    authority = new SystemRecordGcAuthority(records);
    closure = new HBaseSystemRecordClosure(records);
  }

  public enum Skip {
    STILL_PREPARING, ALREADY_CLOSED, NO_RESULT_REFERENCE, STILL_PROTECTED,
    RESULT_STILL_REFERENCED, RESULT_CHANGED
  }

  public sealed interface Outcome permits Closed, Retained {
    ByteKey invocation();
  }

  /** The binding retains the non-replay proof, even if another durable reference keeps the result. */
  public record Closed(ByteKey invocation, boolean resultRetired) implements Outcome {}

  /** RESULT skips retain the payload after closing the window; other skips leave the binding unchanged. */
  public record Retained(ByteKey invocation, Skip skip) implements Outcome {}

  public CompletableFuture<Outcome> close(ByteKey identity, SystemRecordRetirement.Permit permit,
      RequestScope scope) {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(permit, "permit");
    Objects.requireNonNull(scope, "scope");
    return authority.validate(permit, scope).thenCompose(ignored ->
        records.read(SystemRecordType.INVOCATION, identity, scope)).thenCompose(found -> {
      if (found.isEmpty()) return retained(identity, Skip.ALREADY_CLOSED);
      var binding = InvocationMessages.decode(found.orElseThrow().bytes());
      SystemRecordReferences.requireIdentity(identity, ByteKey.copyOf(binding.invocationId().bytes()));
      if (binding.state() == InvocationBinding.State.WINDOW_CLOSED) return retained(identity, Skip.ALREADY_CLOSED);
      if (binding.state() == InvocationBinding.State.PREPARING || binding.state() == InvocationBinding.State.SEALED) {
        return retained(identity, Skip.STILL_PREPARING);
      }
      if (binding.resultReference().isEmpty()) return retained(identity, Skip.NO_RESULT_REFERENCE);
      return closeAndCollect(identity, binding, permit, scope);
    });
  }

  private CompletableFuture<Outcome> closeAndCollect(ByteKey identity, InvocationBinding binding,
      SystemRecordRetirement.Permit permit, RequestScope scope) {
    return authority.validate(permit, scope).thenCompose(protections -> {
      if (Math.addExact(binding.baseRevision(), 1) >= permit.safeRevision()
          || Math.addExact(binding.baseSequence(), 1) >= permit.safeSequence()
          || protections.stream().anyMatch(protection -> protection.transactionId()
              .filter(binding.transactionId()::equals).isPresent())) {
        return retained(identity, Skip.STILL_PROTECTED);
      }
      // The CAS keeps the digest/non-replay binding before any result can disappear.
      return invocations.transition(binding, binding.closeQueryWindow(), scope).thenCompose(closed ->
          closure.collectResult(ResultDirectory.systemIdentity(ResultIdentity.from(binding)), permit, scope)
              .thenApply(result -> (Outcome) switch (result) {
                case ABSENT -> new Closed(identity, false);
                case RETIRED -> new Closed(identity, true);
                case REFERENCED -> new Retained(identity, Skip.RESULT_STILL_REFERENCED);
                case CHANGED -> new Retained(identity, Skip.RESULT_CHANGED);
              }));
    });
  }

  private static CompletableFuture<Outcome> retained(ByteKey identity, Skip skip) {
    return CompletableFuture.completedFuture(new Retained(identity, skip));
  }
}
