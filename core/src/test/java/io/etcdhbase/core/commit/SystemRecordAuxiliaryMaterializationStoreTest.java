package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.lease.ExpiryCandidateIdentity;
import io.etcdhbase.core.lease.ExpiryCandidateRecord;
import io.etcdhbase.core.lease.LeaseEnumerationPointer;
import io.etcdhbase.core.lease.LeaseBindingVersion;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseRecordIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.request.RequestScope;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class SystemRecordAuxiliaryMaterializationStoreTest {
  @Test
  void immutableVersionsArePreflightedAndAppliedIdempotently() {
    var records = new MemorySystemRecordStore();
    var materialization = materialization(records);
    TransactionId transactionId = TransactionId.create();
    long leaseId = -11;
    PreparedAuxiliaryRecord state = stateRecord(transactionId, 0, leaseId, 1, new byte[] {1, 2});
    CommitDecision decision = decision(transactionId, 1);

    try (RequestScope scope = scope()) {
      materialization.preflight(state, scope).join();
      assertEquals(AuxiliaryMaterializationStore.ApplyResult.APPLIED,
          materialization.applySelected(decision, state, scope).join());
      assertEquals(AuxiliaryMaterializationStore.ApplyResult.ALREADY_APPLIED,
          materialization.applySelected(decision, state, scope).join());

      PreparedAuxiliaryRecord conflict = stateRecord(transactionId, 0, leaseId, 1,
          new byte[] {9, 9});
      assertThrows(CompletionException.class, () -> materialization.preflight(conflict, scope).join());
      assertThrows(CompletionException.class,
          () -> materialization.applySelected(decision, conflict, scope).join());
    }
  }

  @Test
  void enumerationPointerOnlyMovesForwardAndUnknownCasIsResolvedByReadback() {
    var records = new MemorySystemRecordStore();
    var materialization = materialization(records);
    long leaseId = 23;
    ByteKey identity = LeaseRecordIdentity.enumeration(leaseId);
    TransactionId firstTransaction = TransactionId.create();
    PreparedAuxiliaryRecord first = pointerRecord(firstTransaction, 0, leaseId, 1);

    try (RequestScope scope = scope()) {
      records.failNextCasAfterApply = true;
      assertEquals(AuxiliaryMaterializationStore.ApplyResult.ALREADY_APPLIED,
          materialization.applySelected(decision(firstTransaction, 1), first, scope).join());
      assertEquals(new LeaseEnumerationPointer(1).encode(), records.value(SystemRecordType.LEASE_ENUMERATION, identity));

      TransactionId secondTransaction = TransactionId.create();
      PreparedAuxiliaryRecord second = pointerRecord(secondTransaction, 1, leaseId, 2);
      materialization.preflight(second, scope).join();
      assertEquals(AuxiliaryMaterializationStore.ApplyResult.APPLIED,
          materialization.applySelected(decision(secondTransaction, 2), second, scope).join());
      assertEquals(new LeaseEnumerationPointer(2).encode(), records.value(SystemRecordType.LEASE_ENUMERATION, identity));
      assertEquals(AuxiliaryMaterializationStore.ApplyResult.SUPERSEDED,
          materialization.applySelected(decision(firstTransaction, 1), first, scope).join());

      PreparedAuxiliaryRecord stale = pointerRecord(TransactionId.create(), 0, leaseId, 1);
      assertThrows(CompletionException.class, () -> materialization.preflight(stale, scope).join());
    }
  }

  @Test
  void expiryCandidateMustNameTheSelectedLeaseVersion() {
    var records = new MemorySystemRecordStore();
    var materialization = materialization(records);
    TransactionId transactionId = TransactionId.create();
    PreparedAuxiliaryRecord candidate = PreparedAuxiliaryRecord.create(transactionId, 1, 0, 0,
        SystemRecordType.EXPIRY_CANDIDATE,
        new ExpiryCandidateIdentity(100_000, 17, 1, 0).encode(),
        new ExpiryCandidateRecord(2).encode());

    try (RequestScope scope = scope()) {
      assertThrows(CompletionException.class, () -> materialization.preflight(candidate, scope).join());
    }
  }

  @Test
  void preflightRejectsStateWithDifferentIdentityOrSequence() {
    var records = new MemorySystemRecordStore();
    TransactionId transactionId = TransactionId.create();
    PreparedAuxiliaryRecord record = stateRecord(transactionId, 0, -11, 1, new byte[] {1});
    try (RequestScope scope = scope()) {
      var wrongId = new SystemRecordAuxiliaryMaterializationStore(records,
          ignored -> LeaseState.grant(new LeaseIdentity(-12, 1), 1, 1, 1));
      var wrongSequence = new SystemRecordAuxiliaryMaterializationStore(records,
          ignored -> LeaseState.grant(new LeaseIdentity(-11, 1), 1, 1, 2));
      var corrupt = new SystemRecordAuxiliaryMaterializationStore(records, ignored -> {
        throw new IllegalArgumentException("invalid payload");
      });
      assertThrows(CompletionException.class, () -> wrongId.preflight(record, scope).join());
      assertThrows(CompletionException.class, () -> wrongSequence.preflight(record, scope).join());
      assertThrows(CompletionException.class, () -> corrupt.preflight(record, scope).join());
    }
  }

  @Test
  void leaseBindingVersionsAreValidatedAndAppliedIdempotently() {
    var records = new MemorySystemRecordStore();
    ByteKey key = ByteKey.copyOf(new byte[] {'k'});
    LeaseBindingVersion binding = new LeaseBindingVersion(
        new LeaseIdentity(-11, 1), key, true, 1, 1, 1);
    var materialization = new SystemRecordAuxiliaryMaterializationStore(records,
        payload -> LeaseState.grant(new LeaseIdentity(-11, 1), 1, 1, 1), ignored -> binding);
    TransactionId transactionId = TransactionId.create();
    PreparedAuxiliaryRecord record = PreparedAuxiliaryRecord.create(transactionId, 1, 0, 0,
        SystemRecordType.LEASE_BINDING, binding.recordIdentity().encode(),
        ByteKey.copyOf(new byte[] {1}));
    CommitDecision decision = decision(transactionId, 1);

    try (RequestScope scope = scope()) {
      materialization.preflight(record, scope).join();
      assertEquals(AuxiliaryMaterializationStore.ApplyResult.APPLIED,
          materialization.applySelected(decision, record, scope).join());
      assertEquals(AuxiliaryMaterializationStore.ApplyResult.ALREADY_APPLIED,
          materialization.applySelected(decision, record, scope).join());

      var missingDecoder = materialization(records);
      assertThrows(CompletionException.class, () -> missingDecoder.preflight(record, scope).join());

      LeaseBindingVersion mismatchedBinding = new LeaseBindingVersion(
          new LeaseIdentity(-11, 1), ByteKey.copyOf(new byte[] {'x'}), true, 1, 1, 1);
      var mismatchedDecoder = new SystemRecordAuxiliaryMaterializationStore(records,
          payload -> LeaseState.grant(new LeaseIdentity(-11, 1), 1, 1, 1),
          ignored -> mismatchedBinding);
      assertThrows(CompletionException.class,
          () -> mismatchedDecoder.preflight(record, scope).join());
    }
  }

  private static SystemRecordAuxiliaryMaterializationStore materialization(
      MemorySystemRecordStore records) {
    return new SystemRecordAuxiliaryMaterializationStore(records,
        payload -> LeaseState.grant(new LeaseIdentity(-11, 1), 1, payload.bytes()[0], 1));
  }

  private static PreparedAuxiliaryRecord stateRecord(TransactionId transactionId,
      long baseSequence, long leaseId, long sequence, byte[] payload) {
    return PreparedAuxiliaryRecord.create(transactionId, 1, baseSequence, 0,
        SystemRecordType.LEASE_STATE, LeaseRecordIdentity.stateVersion(leaseId, sequence),
        ByteKey.copyOf(payload));
  }

  private static PreparedAuxiliaryRecord pointerRecord(TransactionId transactionId,
      long baseSequence, long leaseId, long sequence) {
    return PreparedAuxiliaryRecord.create(transactionId, 1, baseSequence, 0,
        SystemRecordType.LEASE_ENUMERATION, LeaseRecordIdentity.enumeration(leaseId),
        new LeaseEnumerationPointer(sequence).encode());
  }

  private static CommitDecision decision(TransactionId transactionId, long sequence) {
    return new CommitDecision(transactionId, InvocationId.create(), sequence, 1, 1,
        ByteKey.copyOf(new byte[] {1}), ByteKey.copyOf(new byte[32]), false);
  }

  private static RequestScope scope() {
    return RequestScope.withTimeout(Duration.ofMinutes(1));
  }

  private static final class MemorySystemRecordStore implements SystemRecordStore {
    private final Map<Key, ByteKey> rows = new HashMap<>();
    private boolean failNextCasAfterApply;

    @Override
    public CompletableFuture<Optional<ByteKey>> read(SystemRecordType type, ByteKey identity,
        RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(rows.get(new Key(type, identity))));
    }

    @Override
    public int bucket(SystemRecordType type, ByteKey identity) {
      return 0;
    }

    @Override
    public CompletableFuture<CreateResult> createIfAbsent(SystemRecordType type, ByteKey identity,
        ByteKey value, RequestScope scope) {
      Key key = new Key(type, identity);
      ByteKey old = rows.putIfAbsent(key, value);
      return CompletableFuture.completedFuture(old == null ? CreateResult.CREATED
          : old.equals(value) ? CreateResult.ALREADY_PRESENT : CreateResult.CONFLICT);
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(SystemRecordType type, ByteKey identity,
        Optional<ByteKey> expected, ByteKey replacement, RequestScope scope) {
      Key key = new Key(type, identity);
      ByteKey current = rows.get(key);
      if (!Optional.ofNullable(current).equals(expected)) {
        return CompletableFuture.completedFuture(false);
      }
      rows.put(key, replacement);
      if (failNextCasAfterApply) {
        failNextCasAfterApply = false;
        return CompletableFuture.failedFuture(new BackendFailure(BackendFailure.Category.UNKNOWN));
      }
      return CompletableFuture.completedFuture(true);
    }

    private ByteKey value(SystemRecordType type, ByteKey identity) {
      return rows.get(new Key(type, identity));
    }

    private record Key(SystemRecordType type, ByteKey identity) {}
  }
}
