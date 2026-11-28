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
  private final ResultWindowRetention retention;

  public InvocationWindowCloser(HBaseSystemRecordStore records) {
    this(records, ResultWindowRetention.unconfigured());
  }

  public InvocationWindowCloser(HBaseSystemRecordStore records, ResultWindowRetention retention) {
    this(records, records, new SystemRecordGcAuthority(records), retention);
    records.requireGcFencing();
  }

  InvocationWindowCloser(SystemRecordStore records, HBaseSystemRecordStore retirement,
      SystemRecordGcAuthority authority, ResultWindowRetention retention) {
    this.retention = Objects.requireNonNull(retention, "retention");
    this.records = Objects.requireNonNull(records, "records");
    this.retirement = Objects.requireNonNull(retirement, "retirement");
    this.authority = Objects.requireNonNull(authority, "authority");
  }

  public enum Skip {
    UNRESOLVED, ALREADY_CLOSED, STILL_PROTECTED, ROW_CHANGED, WINDOW_RETAINED
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
        return closeRetainedWindow(identity, binding, resolved.orElseThrow(), permit, scope);
      });
    });
  }

  private CompletableFuture<Outcome> closeRetainedWindow(ByteKey identity, InvocationBinding original,
      InvocationBinding resolved, SystemRecordRetirement.Permit permit, RequestScope scope) {
    var now = retention.time().observe();
    if (!now.healthy()) return retained(identity, Skip.WINDOW_RETAINED);
    return retirement.readInvocationWindowEvidence(identity, scope).thenCompose(found -> {
      var witness = found.map(InvocationWindowEvidence::decode);
      if (witness.isEmpty() || witness.orElseThrow().minimumMillis() < retention.minimum().toMillis()) {
        return startRetentionWindow(identity, original, found, permit, scope);
      }
      // A healthy lower bound proves expiry permanently; a later clock failure cannot make
      // real time precede an already-proved deadline. Missing/unhealthy bounds never authorize.
      var checkedTime = retention.time().observe();
      if (!checkedTime.healthy() || checkedTime.nowLowerEpochMillis()
          < witness.orElseThrow().notBeforeEpochMillis()) return retained(identity, Skip.WINDOW_RETAINED);
      return retirement.closeInvocationWindow(identity, ByteKey.copyOf(InvocationMessages.encode(original)),
          ByteKey.copyOf(InvocationMessages.encode(resolved.closeQueryWindow())), found.orElseThrow(), permit, scope)
          .thenApply(closed -> closed ? new Closed(identity) : new Retained(identity, Skip.ROW_CHANGED));
    });
  }

  private CompletableFuture<Outcome> startRetentionWindow(ByteKey identity, InvocationBinding binding,
      Optional<ByteKey> previous, SystemRecordRetirement.Permit permit, RequestScope scope) {
    var now = retention.time().observe();
    if (!now.healthy()) return retained(identity, Skip.WINDOW_RETAINED);
    long notBefore;
    try {
      notBefore = Math.addExact(now.nowUpperEpochMillis(), retention.minimum().toMillis());
    } catch (ArithmeticException exhausted) {
      return retained(identity, Skip.WINDOW_RETAINED);
    }
    var evidence = new InvocationWindowEvidence(retention.minimum().toMillis(), notBefore);
    return retirement.persistInvocationWindowEvidence(identity,
        ByteKey.copyOf(InvocationMessages.encode(binding)), previous, evidence.encode(), permit, scope)
        .thenApply(recorded -> new Retained(identity, recorded ? Skip.WINDOW_RETAINED : Skip.ROW_CHANGED));
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
