package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.core.auth.AuthDeploymentRegistryStore;
import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.auth.AuthSessionStore;
import io.etcdhbase.core.auth.AuthSlot;
import io.etcdhbase.core.auth.SubjectAuthority;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.codec.AuthMessages;
import io.etcdhbase.hbase.codec.AuthDeploymentRegistryMessages;
import io.etcdhbase.hbase.connection.HBaseRequestContext;
import io.etcdhbase.hbase.error.HBaseFailures;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

/** Strong-read, exact-byte CAS store for the isolated J50 authentication-control table. */
public final class HBaseAuthSessionStore
    implements AuthSessionStore, AuthDeploymentRegistryStore {
  public static final int MAX_RECORD_BYTES = AuthMessages.MAX_RECORD_BYTES;
  private static final byte[] VALUE = {'r'};

  private final AsyncConnection connection;
  private final AuthControlBinding binding;

  HBaseAuthSessionStore(AsyncConnection connection, AuthControlBinding binding) {
    this.connection = Objects.requireNonNull(connection, "connection");
    this.binding = Objects.requireNonNull(binding, "binding");
  }

  /** Opens the pre-provisioned control table using the independently authorized service identity. */
  public static CompletableFuture<HBaseAuthSessionStore> open(
      HBaseConnectionFactory connections, AuthorizedBackendIdentity identity,
      AuthControlBinding binding, RequestScope scope) {
    Objects.requireNonNull(connections, "connections");
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(binding, "binding");
    Objects.requireNonNull(scope, "scope");
    var request = new HBaseRequestContext(scope);
    return request.borrowConnection(connections, identity).thenCompose(connection ->
        HBaseRequestCompletion.startRequest(scope, () ->
            connection.getAdmin().getDescriptor(binding.table()).thenApply(descriptor -> {
              binding.validate(descriptor);
              return new HBaseAuthSessionStore(connection, binding);
            }))).exceptionallyCompose(failure -> CompletableFuture.failedFuture(
                HBaseFailures.classify(failure)));
  }

  @Override
  public CompletionStage<Optional<AuthDeploymentRegistry>> readRegistry(RequestScope scope) {
    byte[] row = binding.keyCodec().deploymentRegistry(binding.trustDomain());
    return execute(scope, (table, timeout) -> table.get(new Get(row)
        .setConsistency(Consistency.STRONG).addColumn(binding.family(), VALUE)))
        .thenApply(result -> {
          try {
            return decode(result, row).map(bytes -> {
              var registry = AuthDeploymentRegistryMessages.decode(bytes);
              if (!registry.trustDomain().equals(binding.trustDomain())) throw invalidRecord();
              return registry;
            });
          } catch (IllegalArgumentException malformed) {
            throw new AuthSessionException(AuthSessionException.Reason.DEPENDENCY_UNAVAILABLE);
          }
        });
  }

  @Override
  public CompletionStage<Boolean> createRegistryIfAbsent(AuthDeploymentRegistry initial,
      RequestScope scope) {
    Objects.requireNonNull(initial, "initial");
    if (!initial.trustDomain().equals(binding.trustDomain()) || initial.configurationGeneration() != 1
        || initial.revision() != 1 || initial.revocation() != null) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid initial deployment registry"));
    }
    return create(binding.keyCodec().deploymentRegistry(binding.trustDomain()),
        AuthDeploymentRegistryMessages.encode(initial), scope);
  }

  @Override
  public CompletionStage<Boolean> compareAndSetRegistry(AuthDeploymentRegistry observed,
      AuthDeploymentRegistry replacement, RequestScope scope) {
    Objects.requireNonNull(observed, "observed");
    if (!observed.trustDomain().equals(binding.trustDomain()) || !observed.permitsReplacement(replacement)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("invalid deployment registry transition"));
    }
    return compareAndSet(binding.keyCodec().deploymentRegistry(binding.trustDomain()),
        AuthDeploymentRegistryMessages.encode(observed), AuthDeploymentRegistryMessages.encode(replacement),
        scope);
  }

  @Override
  public CompletionStage<Optional<AuthSession>> readSession(ByteKey tokenDigest, RequestScope scope) {
    requireDigest(tokenDigest, "tokenDigest");
    byte[] row = binding.keyCodec().session(tokenDigest);
    return read(row, scope).thenApply(value -> value.map(bytes -> {
      AuthSession session = AuthMessages.decodeSession(bytes);
      if (!session.tokenDigest().equals(tokenDigest)
          || !session.trustDomain().equals(binding.trustDomain())) throw invalidRecord();
      return session;
    }));
  }

  @Override
  public CompletionStage<Boolean> createSessionIfAbsent(AuthSession session, RequestScope scope) {
    Objects.requireNonNull(session, "session");
    if (!session.trustDomain().equals(binding.trustDomain())) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "session trust domain does not match its auth-control table"));
    }
    byte[] row = binding.keyCodec().session(session.tokenDigest());
    return create(row, AuthMessages.encode(session), scope);
  }

  @Override
  public CompletionStage<Boolean> deleteSessionIfUnchanged(AuthSession observed, RequestScope scope) {
    Objects.requireNonNull(observed, "observed");
    if (observed.status() == AuthSession.Status.ACTIVE
        || observed.status() == AuthSession.Status.PREPARING) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "active auth sessions must be retired before deletion"));
    }
    byte[] row = binding.keyCodec().session(observed.tokenDigest());
    byte[] expected = AuthMessages.encode(observed);
    return execute(scope, (table, timeout) -> table.checkAndMutate(
        CheckAndMutate.newBuilder(row).ifEquals(binding.family(), VALUE, expected)
            .build(new Delete(row).setDurability(Durability.SYNC_WAL)))
        .thenApply(result -> result.isSuccess()));
  }

  @Override
  public CompletionStage<Boolean> compareAndSetSession(
      AuthSession observed, AuthSession replacement, RequestScope scope) {
    Objects.requireNonNull(observed, "observed");
    Objects.requireNonNull(replacement, "replacement");
    if (!observed.tokenDigest().equals(replacement.tokenDigest())
        || !observed.sessionId().equals(replacement.sessionId())
        || !sameSessionExceptStatus(observed, replacement)
        || !allowedSessionTransition(observed.status(), replacement.status())) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "session CAS must preserve identity and follow a terminal-safe transition"));
    }
    return compareAndSet(binding.keyCodec().session(observed.tokenDigest()),
        AuthMessages.encode(observed), AuthMessages.encode(replacement), scope);
  }

  @Override
  public CompletionStage<Optional<AuthSlot>> readSlot(int index, RequestScope scope) {
    byte[] row = binding.keyCodec().slot(index);
    return read(row, scope).thenApply(value -> value.map(bytes -> {
      AuthSlot slot = AuthMessages.decodeSlot(bytes);
      if (slot.index() != index) throw invalidRecord();
      return slot;
    }));
  }

  @Override
  public CompletionStage<Boolean> createSlotIfAbsent(AuthSlot slot, RequestScope scope) {
    Objects.requireNonNull(slot, "slot");
    if (slot.state() != AuthSlot.State.EMPTY || slot.generation() != 0) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "new auth slots must start empty at generation zero"));
    }
    return create(binding.keyCodec().slot(slot.index()), AuthMessages.encode(slot), scope);
  }

  @Override
  public CompletionStage<Boolean> compareAndSetSlot(
      AuthSlot observed, AuthSlot replacement, RequestScope scope) {
    Objects.requireNonNull(observed, "observed");
    Objects.requireNonNull(replacement, "replacement");
    if (observed.index() != replacement.index()
        || !allowedSlotTransition(observed, replacement)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "slot CAS must preserve its index and follow a bounded transition"));
    }
    return compareAndSet(binding.keyCodec().slot(observed.index()),
        AuthMessages.encode(observed), AuthMessages.encode(replacement), scope);
  }

  @Override
  public CompletionStage<Optional<SubjectAuthority>> readAuthority(
      ByteKey subjectDigest, RequestScope scope) {
    requireDigest(subjectDigest, "subjectDigest");
    byte[] row = binding.keyCodec().authority(subjectDigest);
    return read(row, scope).thenApply(value -> value.map(bytes -> {
      SubjectAuthority authority = AuthMessages.decodeAuthority(bytes);
      if (!authority.subjectDigest().equals(subjectDigest)
          || !authority.trustDomain().equals(binding.trustDomain())) throw invalidRecord();
      return authority;
    }));
  }

  @Override
  public CompletionStage<Boolean> createAuthorityIfAbsent(
      SubjectAuthority authority, RequestScope scope) {
    Objects.requireNonNull(authority, "authority");
    if (!authority.trustDomain().equals(binding.trustDomain())
        || authority.state() != SubjectAuthority.State.ACTIVE
        || !authority.revocationTargets().isEmpty()) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "new auth authority must be active in its bound trust domain"));
    }
    return create(binding.keyCodec().authority(authority.subjectDigest()),
        AuthMessages.encode(authority), scope);
  }

  @Override
  public CompletionStage<Boolean> compareAndSetAuthority(
      SubjectAuthority observed, SubjectAuthority replacement, RequestScope scope) {
    Objects.requireNonNull(observed, "observed");
    Objects.requireNonNull(replacement, "replacement");
    if (!observed.allowsTransitionTo(replacement)) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "authority CAS must preserve scope and follow a safe revocation transition"));
    }
    return compareAndSet(binding.keyCodec().authority(observed.subjectDigest()),
        AuthMessages.encode(observed), AuthMessages.encode(replacement), scope);
  }

  private CompletableFuture<Optional<byte[]>> read(byte[] row, RequestScope scope) {
    return execute(scope, (table, timeout) -> table.get(new Get(row)
        .setConsistency(Consistency.STRONG).addColumn(binding.family(), VALUE))
        .thenApply(result -> decode(result, row)));
  }

  private CompletableFuture<Boolean> create(byte[] row, byte[] value, RequestScope scope) {
    if (value.length == 0 || value.length > MAX_RECORD_BYTES) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "auth-control value exceeds its size limit"));
    }
    Put put = new Put(row).setDurability(Durability.SYNC_WAL)
        .addColumn(binding.family(), VALUE, value);
    return execute(scope, (table, timeout) -> table.checkAndMutate(
        CheckAndMutate.newBuilder(row).ifNotExists(binding.family(), VALUE).build(put))
        .thenApply(result -> result.isSuccess()));
  }

  private CompletableFuture<Boolean> compareAndSet(
      byte[] row, byte[] expected, byte[] replacement, RequestScope scope) {
    if (expected.length == 0 || expected.length > MAX_RECORD_BYTES
        || replacement.length == 0 || replacement.length > MAX_RECORD_BYTES) {
      return CompletableFuture.failedFuture(new IllegalArgumentException(
          "auth-control value exceeds its size limit"));
    }
    Put put = new Put(row).setDurability(Durability.SYNC_WAL)
        .addColumn(binding.family(), VALUE, replacement);
    return execute(scope, (table, timeout) -> table.checkAndMutate(CheckAndMutate
        .newBuilder(row).ifEquals(binding.family(), VALUE, expected).build(put))
        .thenApply(result -> result.isSuccess()));
  }

  private <T> CompletableFuture<T> execute(RequestScope scope,
      TableOperation<T> operation) {
    Objects.requireNonNull(scope, "scope");
    return HBaseRequestCompletion.startRequest(scope, () -> {
      long timeout = Math.max(1, Math.min(TimeUnit.SECONDS.toNanos(30),
          scope.remainingDeadlineNanos()));
      AsyncTable<?> table = connection.getTableBuilder(binding.table())
          .setOperationTimeout(timeout, TimeUnit.NANOSECONDS)
          .setRpcTimeout(timeout, TimeUnit.NANOSECONDS)
          .setReadRpcTimeout(timeout, TimeUnit.NANOSECONDS)
          .setWriteRpcTimeout(timeout, TimeUnit.NANOSECONDS)
          .build();
      return operation.apply(table, timeout);
    });
  }

  private Optional<byte[]> decode(Result result, byte[] expectedRow) {
    if (result.isEmpty()) return Optional.empty();
    if (!Arrays.equals(expectedRow, result.getRow())) throw invalidRecord();
    Cell[] cells = result.rawCells();
    if (cells == null || cells.length != 1
        || !CellUtil.matchingFamily(cells[0], binding.family())
        || !CellUtil.matchingQualifier(cells[0], VALUE)) {
      throw invalidRecord();
    }
    int length = cells[0].getValueLength();
    if (length <= 0 || length > MAX_RECORD_BYTES) throw invalidRecord();
    byte[] value = result.getValue(binding.family(), VALUE);
    if (value == null || value.length != length) throw invalidRecord();
    return Optional.of(value);
  }

  private static void requireDigest(ByteKey value, String name) {
    Objects.requireNonNull(value, name);
    if (value.length() != 32) throw new IllegalArgumentException(name + " must be SHA-256");
  }

  private static boolean sameSessionExceptStatus(AuthSession left, AuthSession right) {
    return left.tokenDigest().equals(right.tokenDigest())
        && left.subjectDigest().equals(right.subjectDigest())
        && left.trustDomain().equals(right.trustDomain())
        && left.providerId().equals(right.providerId())
        && left.scope().equals(right.scope())
        && left.datasetDigest().equals(right.datasetDigest())
        && left.credentialGeneration() == right.credentialGeneration()
        && left.authorizationGeneration() == right.authorizationGeneration()
        && left.createdAtEpochMillis() == right.createdAtEpochMillis()
        && left.expiresAtEpochMillis() == right.expiresAtEpochMillis()
        && left.slotIndex() == right.slotIndex()
        && left.slotGeneration() == right.slotGeneration()
        && left.credentialEnvelope().equals(right.credentialEnvelope());
  }

  private static boolean allowedSessionTransition(AuthSession.Status oldState,
      AuthSession.Status newState) {
    return switch (oldState) {
      case PREPARING -> newState == AuthSession.Status.ACTIVE
          || newState == AuthSession.Status.RETIRING || newState == AuthSession.Status.REVOKED
          || newState == AuthSession.Status.EXPIRED;
      case ACTIVE -> newState == AuthSession.Status.RETIRING
          || newState == AuthSession.Status.REVOKED || newState == AuthSession.Status.EXPIRED;
      case RETIRING -> newState == AuthSession.Status.REVOKED
          || newState == AuthSession.Status.EXPIRED;
      case REVOKED, EXPIRED -> false;
    };
  }

  private static boolean allowedSlotTransition(AuthSlot oldSlot, AuthSlot newSlot) {
    if (oldSlot.state() == AuthSlot.State.EMPTY && newSlot.state() == AuthSlot.State.RESERVED) {
      return oldSlot.generation() < Long.MAX_VALUE
          && newSlot.generation() == oldSlot.generation() + 1;
    }
    if (oldSlot.generation() != newSlot.generation()) return false;
    return switch (oldSlot.state()) {
      case EMPTY -> false;
      case RESERVED -> sameReservation(oldSlot, newSlot)
          && (newSlot.state() == AuthSlot.State.ACTIVE
              || newSlot.state() == AuthSlot.State.RETIRING);
      case ACTIVE -> sameReservation(oldSlot, newSlot)
          && newSlot.state() == AuthSlot.State.RETIRING;
      case RETIRING -> newSlot.state() == AuthSlot.State.EMPTY
          && newSlot.sessionId() == null && newSlot.operationId() == null;
    };
  }

  private static boolean sameReservation(AuthSlot left, AuthSlot right) {
    return Objects.equals(left.sessionId(), right.sessionId())
        && Objects.equals(left.operationId(), right.operationId());
  }

  private static IllegalArgumentException invalidRecord() {
    return new IllegalArgumentException("auth-control row violates its record format");
  }

  @FunctionalInterface
  private interface TableOperation<T> {
    CompletableFuture<T> apply(AsyncTable<?> table, long timeoutNanos);
  }
}
