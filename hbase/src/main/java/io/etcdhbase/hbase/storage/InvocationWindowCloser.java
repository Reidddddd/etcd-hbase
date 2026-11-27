package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitDecision;
import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.InvocationMessages;
import io.etcdhbase.hbase.codec.ManifestMessages;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Closes eligible query windows while preserving durable invocation identities against replay. */
public final class InvocationWindowCloser {
  private final SystemRecordStore records;
  private final HBaseSystemRecordStore retirement;
  private final SystemRecordGcAuthority authority;

  public InvocationWindowCloser(HBaseSystemRecordStore records) {
    this(records, records, new SystemRecordGcAuthority(records));
    records.requireGcFencing();
  }

  InvocationWindowCloser(SystemRecordStore records, HBaseSystemRecordStore retirement,
      SystemRecordGcAuthority authority) {
    this.records = Objects.requireNonNull(records, "records");
    this.retirement = Objects.requireNonNull(retirement, "retirement");
    this.authority = Objects.requireNonNull(authority, "authority");
  }

  public enum Skip {
    UNRESOLVED, ALREADY_CLOSED, STILL_PROTECTED, ROW_CHANGED
  }

  public sealed interface Outcome permits Closed, Retained {
    ByteKey invocation();
  }

  /** The result is collected separately through the actual reference graph. */
  public record Closed(ByteKey invocation) implements Outcome {}

  /** An unresolved, protected or concurrently changed binding is retained unchanged. */
  public record Retained(ByteKey invocation, Skip skip) implements Outcome {}

  /** The checkpoint worker visits RESULT separately, keeping each invocation page bounded. */
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
      if (binding.state() == InvocationBinding.State.PREPARING) {
        return retained(identity, Skip.UNRESOLVED);
      }
      return closeBinding(identity, binding, permit, scope);
    });
  }

  private CompletableFuture<Outcome> closeBinding(ByteKey identity, InvocationBinding binding,
      SystemRecordRetirement.Permit permit, RequestScope scope) {
    return authority.validate(permit, scope).thenCompose(protections -> {
      if (binding.baseRevision() >= permit.safeRevision() - 1
          || binding.baseSequence() >= permit.safeSequence() - 1
          || protections.stream().anyMatch(protection -> protection.transactionId()
              .filter(binding.transactionId()::equals).isPresent())) {
        return retained(identity, Skip.STILL_PROTECTED);
      }
      return resolvedWindow(binding, scope).thenCompose(resolved -> {
        if (resolved.isEmpty()) return retained(identity, Skip.UNRESOLVED);
        // Compare the original bytes even when a finalized SEALED attempt was resolved above.
        return retirement.closeInvocationWindow(identity, ByteKey.copyOf(InvocationMessages.encode(binding)),
            ByteKey.copyOf(InvocationMessages.encode(resolved.orElseThrow().closeQueryWindow())), permit, scope)
            .thenApply(closed -> closed ? new Closed(identity) : new Retained(identity, Skip.ROW_CHANGED));
      });
    });
  }

  private CompletableFuture<Optional<InvocationBinding>> resolvedWindow(
      InvocationBinding binding, RequestScope scope) {
    if (binding.state() != InvocationBinding.State.SEALED) {
      return CompletableFuture.completedFuture(Optional.of(binding));
    }
    var outcomes = new CommitOutcomeResolver(retirement.controlStore(), new HBaseCommitDecisionRepository(records));
    return outcomes.resolve(binding, scope).thenCompose(outcome -> switch (outcome.state()) {
      case SELECTED_FINALIZED -> selectedWindow(binding, outcome.decision().orElseThrow(), scope)
          .thenApply(Optional::of);
      case NOT_SELECTED -> CompletableFuture.completedFuture(
          Optional.of(binding.notSelected(outcome.notSelectedProof().orElseThrow())));
      default -> CompletableFuture.completedFuture(Optional.empty());
    });
  }

  private CompletableFuture<InvocationBinding> selectedWindow(
      InvocationBinding binding, CommitDecision decision, RequestScope scope) {
    return records.read(SystemRecordType.MANIFEST, decision.manifestRoot(), scope).thenApply(found -> {
      var root = ManifestMessages.decodeRoot(found.orElseThrow(
          () -> new IllegalStateException("finalized result manifest is missing")).bytes());
      var reference = root.resultReference().orElseThrow(
          () -> new IllegalStateException("finalized invocation has no result reference"));
      if (!root.systemIdentity().equals(decision.manifestRoot()) || !root.digest().equals(decision.manifestDigest())
          || !root.transactionId().equals(binding.transactionId()) || root.ownerEpoch() != binding.preparedOwnerEpoch()
          || root.baseSequence() != binding.baseSequence() || root.baseRevision() != binding.baseRevision()
          || !reference.identity().matches(binding)) {
        throw new IllegalStateException("finalized result manifest differs from invocation");
      }
      return binding.selected(reference.token());
    });
  }

  private static CompletableFuture<Outcome> retained(ByteKey identity, Skip skip) {
    return CompletableFuture.completedFuture(new Retained(identity, skip));
  }
}
