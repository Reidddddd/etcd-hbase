package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.SystemRecordRead;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.codec.CommitControlMessages;
import io.etcdhbase.hbase.connection.HBaseRequestContext;
import io.etcdhbase.hbase.error.HBaseFailures;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

/** HBase adapter for the one authoritative M2 control row; connections remain factory-owned. */
public final class HBaseCommitControlStore implements CommitControlStore {
  private static final byte[] STATE = {'s'};

  private final AsyncConnection connection;
  private final SystemTableBinding binding;
  private final boolean auxiliarySupported;
  private final boolean leaseBindingsSupported;
  private final boolean leaseExpiryCheckpointsSupported;
  private final boolean eventRevisionIndexSupported;

  private HBaseCommitControlStore(AsyncConnection connection, SystemTableBinding binding,
      boolean auxiliarySupported, boolean leaseBindingsSupported,
      boolean leaseExpiryCheckpointsSupported, boolean eventRevisionIndexSupported) {
    this.connection = connection;
    this.binding = binding;
    this.auxiliarySupported = auxiliarySupported;
    this.leaseBindingsSupported = leaseBindingsSupported;
    this.leaseExpiryCheckpointsSupported = leaseExpiryCheckpointsSupported;
    this.eventRevisionIndexSupported = eventRevisionIndexSupported;
  }

  public boolean supportsAuxiliary() { return auxiliarySupported; }
  public boolean supportsLeaseBindings() { return leaseBindingsSupported; }
  public boolean supportsLeaseExpiryCheckpoints() { return leaseExpiryCheckpointsSupported; }
  public boolean supportsEventRevisionIndex() { return eventRevisionIndexSupported; }

  public static CompletableFuture<HBaseCommitControlStore> open(
      HBaseConnectionFactory factory,
      AuthorizedBackendIdentity identity,
      SystemTableBinding binding,
      RequestScope scope) {
    var context = new HBaseRequestContext(scope);
    return context.borrowConnection(factory, identity).thenCompose(connection ->
        startRequest(scope, () -> connection.getAdmin().getDescriptor(binding.table())
            .thenApply(descriptor -> {
              binding.validate(descriptor);
              return new HBaseCommitControlStore(connection, binding,
                  binding.supportsAuxiliary(descriptor), binding.supportsLeaseBindings(descriptor),
                  binding.supportsLeaseExpiryCheckpoints(descriptor),
                  binding.supportsEventRevisionIndex(descriptor));
            }))).exceptionallyCompose(failure -> failed(failure));
  }

  @Override
  public CompletableFuture<Optional<ControlSnapshot>> read(RequestScope scope) {
    return readWithSize(scope).thenApply(SystemRecordRead::value);
  }

  @Override
  public CompletableFuture<SystemRecordRead<ControlSnapshot>> readWithSize(RequestScope scope) {
    long requestPayloadBytes = (long) binding.controlRow().length + binding.family().length;
    return execute(scope, requestPayloadBytes, table -> {
      var get = new Get(binding.controlRow()).setConsistency(Consistency.STRONG)
          .addFamily(binding.family());
      return table.get(get).thenApply(row -> {
        Optional<ControlSnapshot> snapshot = snapshot(row);
        long encodedBytes = snapshot.map(value -> Math.addExact((long) binding.controlRow().length,
            value.comparisonToken().length)).orElse(0L);
        return new SystemRecordRead<>(snapshot, encodedBytes);
      });
    });
  }

  @Override
  public CompletableFuture<Boolean> initializeIfAbsent(CommitState initialState, RequestScope scope) {
    if (!initialState.profileFingerprint().equals(binding.profileFingerprint())
        || initialState.ownerEpoch() != 0 || initialState.ownerLeaseDeadlineEpochMillis() != 0
        || initialState.stateVersion() != 1 || initialState.publishedSequence() != 0
        || initialState.finalizedSequence() != 0 || initialState.compactRevision() != 0
        || initialState.supportsAuxiliaryRecords() && !auxiliarySupported
        || initialState.supportsLeaseBindings() && !leaseBindingsSupported
        || initialState.supportsLeaseExpiryCheckpoints() && !leaseExpiryCheckpointsSupported
        || initialState.supportsEventRevisionIndex() && !eventRevisionIndexSupported
        || initialState.publication().isPresent()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "control initialization requires a fenced empty initial state"));
    }
    byte[] encodedState = CommitControlMessages.encode(initialState);
    long requestPayloadBytes = (long) binding.controlRow().length + binding.family().length
        + STATE.length + encodedState.length;
    return execute(scope, requestPayloadBytes, table -> {
      Put put = controlPut(initialState);
      var check = CheckAndMutate.newBuilder(binding.controlRow())
          .ifNotExists(binding.family(), STATE).build(put);
      return table.checkAndMutate(check).thenApply(result -> result.isSuccess());
    });
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(
      ControlSnapshot observed, CommitState replacement, RequestScope scope) {
    if (!Arrays.equals(CommitControlMessages.encode(observed.state()), observed.comparisonToken())
        || !observed.state().profileFingerprint().equals(binding.profileFingerprint())
        || !observed.state().profileFingerprint().equals(replacement.profileFingerprint())
        || replacement.stateVersion() != Math.addExact(observed.state().stateVersion(), 1)
        || !isValidAuthorizationBarrierTransition(observed.state(), replacement)
        || replacement.supportsEventRevisionIndex() && !eventRevisionIndexSupported) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "control replacement must advance the exact observed state"));
    }
    if (replacement.formatVersion() != observed.state().formatVersion()) {
      try {
        CommitState state = observed.state();
        boolean auxiliaryUpgrade = state.formatVersion() == CommitState.FORMAT_VERSION
            && replacement.formatVersion() == CommitState.AUXILIARY_FORMAT_VERSION
            && auxiliarySupported
            && state.ownerInstance().isPresent()
            && Arrays.equals(CommitControlMessages.encode(replacement), CommitControlMessages.encode(
                state.upgradeAuxiliaryFormat(state.ownerInstance().orElseThrow(), state.ownerEpoch(), 0)));
        boolean leaseBindingUpgrade = state.formatVersion() == CommitState.AUXILIARY_FORMAT_VERSION
            && replacement.formatVersion() == CommitState.LEASE_BINDING_FORMAT_VERSION
            && auxiliarySupported && leaseBindingsSupported && state.ownerInstance().isPresent()
            && Arrays.equals(CommitControlMessages.encode(replacement), CommitControlMessages.encode(
                state.upgradeLeaseBindingFormat(state.ownerInstance().orElseThrow(), state.ownerEpoch(), 0)));
        boolean leaseExpiryUpgrade = state.formatVersion() == CommitState.LEASE_BINDING_FORMAT_VERSION
            && replacement.formatVersion() == CommitState.LEASE_EXPIRY_FORMAT_VERSION
            && auxiliarySupported && leaseBindingsSupported && leaseExpiryCheckpointsSupported
            && state.ownerInstance().isPresent()
            && Arrays.equals(CommitControlMessages.encode(replacement), CommitControlMessages.encode(
                state.upgradeLeaseExpiryFormat(state.ownerInstance().orElseThrow(), state.ownerEpoch(), 0)));
        boolean eventIndexUpgrade = state.formatVersion() == CommitState.LEASE_EXPIRY_FORMAT_VERSION
            && replacement.formatVersion() == CommitState.EVENT_INDEX_FORMAT_VERSION
            && eventRevisionIndexSupported && state.ownerInstance().isPresent()
            && Arrays.equals(CommitControlMessages.encode(replacement), CommitControlMessages.encode(
                state.upgradeEventIndexFormat(state.ownerInstance().orElseThrow(), state.ownerEpoch(), 0)));
        boolean eventRegistryUpgrade = state.formatVersion() == CommitState.EVENT_INDEX_FORMAT_VERSION
            && replacement.formatVersion() == CommitState.EVENT_REGISTRY_FORMAT_VERSION
            && eventRevisionIndexSupported && state.ownerInstance().isPresent()
            && state.pendingEventRegistryNamespace().isPresent()
            && Arrays.equals(CommitControlMessages.encode(replacement), CommitControlMessages.encode(
                state.upgradeEventRegistryFormat(state.ownerInstance().orElseThrow(), state.ownerEpoch(), 0,
                    state.pendingEventRegistryNamespace().orElseThrow())));
        if (!auxiliaryUpgrade && !leaseBindingUpgrade && !leaseExpiryUpgrade && !eventIndexUpgrade
            && !eventRegistryUpgrade) {
          return CompletableFuture.failedFuture(new IllegalArgumentException("invalid control format upgrade"));
        }
      } catch (IllegalArgumentException | IllegalStateException | ArithmeticException failure) {
        return CompletableFuture.failedFuture(failure);
      }
    }
    byte[] encodedReplacement = CommitControlMessages.encode(replacement);
    long requestPayloadBytes = (long) binding.controlRow().length + binding.family().length
        + STATE.length + observed.comparisonToken().length + encodedReplacement.length;
    return execute(scope, requestPayloadBytes, table -> {
      var condition = CheckAndMutate.newBuilder(binding.controlRow())
          .ifEquals(binding.family(), STATE, observed.comparisonToken())
          .build(controlPut(replacement));
      return table.checkAndMutate(condition).thenApply(result -> result.isSuccess());
    });
  }

  static boolean isValidAuthorizationBarrierTransition(
      CommitState observed, CommitState replacement) {
    long currentGeneration = observed.authorizationBarrierGeneration();
    long nextGeneration = replacement.authorizationBarrierGeneration();
    if (!observed.supportsAuthorizationBarrier()) {
      if (!replacement.supportsAuthorizationBarrier()) return nextGeneration == currentGeneration;
      if (nextGeneration != 0 || observed.ownerInstance().isEmpty()) return false;
      try {
        CommitState expected = observed.enableAuthorizationBarrier(
            observed.ownerInstance().orElseThrow(), observed.ownerEpoch(), 0);
        return Arrays.equals(CommitControlMessages.encode(expected),
            CommitControlMessages.encode(replacement));
      } catch (IllegalArgumentException | IllegalStateException | ArithmeticException invalid) {
        return false;
      }
    }
    if (!replacement.supportsAuthorizationBarrier()) return false;
    if (nextGeneration < currentGeneration) return false;
    if (nextGeneration == currentGeneration) return true;
    if (!observed.supportsAuthorizationBarrier() || !replacement.supportsAuthorizationBarrier()
        || observed.ownerInstance().isEmpty()) {
      return false;
    }
    try {
      if (nextGeneration != Math.addExact(currentGeneration, 1)) return false;
      CommitState expected = observed.advanceAuthorizationBarrier(
          observed.ownerInstance().orElseThrow(), observed.ownerEpoch(), currentGeneration);
      return Arrays.equals(CommitControlMessages.encode(expected),
          CommitControlMessages.encode(replacement));
    } catch (IllegalArgumentException | IllegalStateException | ArithmeticException invalid) {
      return false;
    }
  }

  private Optional<ControlSnapshot> snapshot(Result row) {
    if (row.isEmpty()) return Optional.empty();
    var cells = row.rawCells();
    if (cells == null || cells.length != 1
        || !Arrays.equals(CellUtil.cloneQualifier(cells[0]), STATE)) {
      throw new IllegalArgumentException("control row contains an unexpected cell layout");
    }
    byte[] value = row.getValue(binding.family(), STATE);
    if (value == null || value.length == 0) {
      throw new IllegalArgumentException("control row is missing its state cell");
    }
    CommitState state = CommitControlMessages.decode(value);
    if (state.supportsEventRevisionIndex() && !eventRevisionIndexSupported) {
      throw new IllegalStateException("event index control format requires its table capability");
    }
    if (!state.profileFingerprint().equals(binding.profileFingerprint())) {
      throw new IllegalArgumentException("control state profile differs from its system table binding");
    }
    return Optional.of(new ControlSnapshot(state, value));
  }

  private Put controlPut(CommitState state) {
    return new Put(binding.controlRow()).setDurability(Durability.SYNC_WAL)
        .addColumn(binding.family(), STATE, CommitControlMessages.encode(state));
  }

  private <T> CompletableFuture<T> execute(RequestScope scope,
      java.util.function.Function<AsyncTable<?>, CompletableFuture<T>> operation) {
    return executeRequest(scope, 0, operation);
  }

  private <T> CompletableFuture<T> execute(RequestScope scope, long logicalRequestPayloadBytes,
      java.util.function.Function<AsyncTable<?>, CompletableFuture<T>> operation) {
    return executeRequest(scope, logicalRequestPayloadBytes, operation);
  }

  private <T> CompletableFuture<T> executeRequest(RequestScope scope, long logicalRequestPayloadBytes,
      java.util.function.Function<AsyncTable<?>, CompletableFuture<T>> operation) {
    return startRequest(scope, logicalRequestPayloadBytes, () -> {
      long timeout = Math.max(1, Math.min(TimeUnit.SECONDS.toNanos(30), scope.remainingDeadlineNanos()));
      AsyncTable<?> table = connection.getTableBuilder(binding.table())
          .setOperationTimeout(timeout, TimeUnit.NANOSECONDS)
          .setRpcTimeout(timeout, TimeUnit.NANOSECONDS).build();
      return operation.apply(table);
    });
  }

  private static <T> CompletableFuture<T> startRequest(
      RequestScope scope, Supplier<CompletableFuture<T>> operation) {
    return HBaseRequestCompletion.startRequest(scope, operation);
  }

  private static <T> CompletableFuture<T> startRequest(RequestScope scope,
      long logicalRequestPayloadBytes, Supplier<CompletableFuture<T>> operation) {
    return HBaseRequestCompletion.startRequest(scope, logicalRequestPayloadBytes, operation);
  }

  private static <T> CompletableFuture<T> failed(Throwable failure) {
    return CompletableFuture.failedFuture(HBaseFailures.classify(failure));
  }
}
