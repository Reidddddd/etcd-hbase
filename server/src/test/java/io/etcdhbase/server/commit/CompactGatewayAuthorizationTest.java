package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import etcdserverpb.KVGrpc;
import etcdserverpb.Rpc;
import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.AuthSessionException;
import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.CommitControlStore;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitState;
import io.etcdhbase.core.commit.CompactionOperation;
import io.etcdhbase.core.commit.CompactionService;
import io.etcdhbase.core.commit.ControlSnapshot;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.service.AuthService;
import io.etcdhbase.grpc.service.KvService;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.server.auth.AuthSessionService;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.TablePermission;
import org.junit.jupiter.api.Test;

/** Uses the real gateway and compaction state machine; only auth-control and HBase RPCs are doubles. */
class CompactGatewayAuthorizationTest {
  private static final String GROUP = "compact-auth";
  private static final String TOKEN = "opaque-test-token";
  private static final AuthenticationScope AUTH_SCOPE = new AuthenticationScope(GROUP, "profile");
  private static final AuthorizedBackendIdentity SERVICE = identity("background");
  private static final AuthorizedBackendIdentity USER = identity("manager");
  private static final List<TableName> TABLES = List.of(TableName.valueOf("compact-system"),
      TableName.valueOf("compact-history"), TableName.valueOf("compact-current"));

  @Test
  void missingAndInvalidTokensCannotCreateOperationsOrAdvanceControl() {
    for (Optional<String> token : List.of(Optional.<String>empty(), Optional.of("invalid"))) {
      var fixture = new Fixture();
      try (var scope = RequestScope.withoutDeadline()) {
        fixture.call(token, false, scope).requireFailure(Status.Code.UNAUTHENTICATED);
        fixture.requireUnchanged();
        verify(fixture.connections, never()).connectionFor(any());
      }
    }
  }

  @Test
  void revokedSessionIsRejectedBeforeCreatingAnOperation() {
    var fixture = new Fixture();
    fixture.revalidation = ignored -> CompletableFuture.failedFuture(invalidToken());
    try (var scope = RequestScope.withoutDeadline()) {
      fixture.call(Optional.of(TOKEN), false, scope).requireFailure(Status.Code.UNAUTHENTICATED);
      fixture.requireUnchanged();
      verify(fixture.connections, never()).connectionFor(any());
    }
  }

  @Test
  void ordinaryUserCannotCompactEvenWhenPhysicalCollectionIsUnavailable() {
    var deniedPermissions = List.of(List.of(false, true, true),
        List.of(true, false, true), List.of(true, true, false));
    for (boolean physical : List.of(false, true)) {
      for (var permissions : deniedPermissions) {
        var fixture = new Fixture();
        fixture.permissions = ignored -> CompletableFuture.completedFuture(permissions);
        try (var scope = RequestScope.withoutDeadline()) {
          fixture.call(Optional.of(TOKEN), physical, scope).requireFailure(Status.Code.PERMISSION_DENIED);
          fixture.requireUnchanged();
          verify(fixture.connections, never()).connectionFor(SERVICE);
        }
      }
    }
  }

  @Test
  void authenticationDisabledStillRequiresBackendAdminPermission() {
    for (boolean allowed : List.of(false, true)) {
      var fixture = new Fixture(false);
      fixture.permissions = ignored -> CompletableFuture.completedFuture(List.of(allowed, allowed, allowed));
      try (var scope = RequestScope.withoutDeadline()) {
        var observer = fixture.call(Optional.empty(), false, scope);
        if (allowed) {
          assertTrue(observer.completed);
          assertEquals(5, fixture.state.get().compactRevision());
        } else {
          observer.requireFailure(Status.Code.PERMISSION_DENIED);
          fixture.requireUnchanged();
        }
      }
    }
  }

  @Test
  void unavailableManagementPermissionCheckCannotPersistWork() {
    var fixture = new Fixture();
    fixture.permissions = ignored -> CompletableFuture.failedFuture(new UnsupportedOperationException());
    try (var scope = RequestScope.withoutDeadline()) {
      fixture.call(Optional.of(TOKEN), false, scope).requireFailure(Status.Code.UNAVAILABLE);
      fixture.requireUnchanged();
    }
  }

  @Test
  void authorizedManagerSelectsWithItsOwnIdentityAndChecksEveryTableTwice() throws Exception {
    var fixture = new Fixture();
    try (var scope = RequestScope.withoutDeadline()) {
      var observer = fixture.call(Optional.of(TOKEN), false, scope);
      assertTrue(observer.completed);
      assertEquals(1, observer.responses);
      assertEquals(5, fixture.state.get().compactRevision());
      assertEquals(2, fixture.permissionChecks.get());
      assertEquals(AuthorizationScopeDigest.fromIdentity(USER),
          CompactionOperation.decode(fixture.values.values().iterator().next()).authorizationDigest());
      verify(fixture.held, never()).release();
    }
    verify(fixture.held).release();
    verify(fixture.connections, never()).connectionFor(SERVICE);
  }

  @Test
  void managementPermissionRevokedBeforeSelectionCannotAdvanceControl() {
    var fixture = new Fixture();
    fixture.permissions = attempt -> CompletableFuture.completedFuture(
        attempt == 1 ? List.of(true, true, true) : List.of(false, true, true));
    try (var scope = RequestScope.withoutDeadline()) {
      fixture.call(Optional.of(TOKEN), false, scope).requireFailure(Status.Code.PERMISSION_DENIED);
      fixture.requirePreparedWithoutSelection();
    }
  }

  @Test
  void sessionRevokedBeforeSelectionCannotAdvanceControl() {
    var fixture = new Fixture();
    fixture.revalidation = attempt -> attempt == 1
        ? CompletableFuture.completedFuture(fixture.resolved())
        : CompletableFuture.failedFuture(invalidToken());
    try (var scope = RequestScope.withoutDeadline()) {
      fixture.call(Optional.of(TOKEN), false, scope).requireFailure(Status.Code.UNAUTHENTICATED);
      fixture.requirePreparedWithoutSelection();
    }
  }

  @Test
  void cancellationRetainsUserBorrowUntilLateConnectionDrains() {
    var fixture = new Fixture();
    var connection = new CompletableFuture<AsyncConnection>();
    when(fixture.connections.connectionFor(USER)).thenReturn(connection);
    try (var scope = RequestScope.withoutDeadline()) {
      fixture.call(Optional.of(TOKEN), false, scope);
      scope.cancel();
      fixture.requireUnchanged();
      verify(fixture.held, never()).release();
      assertFalse(connection.isCancelled());
      connection.complete(fixture.connection);
      scope.cleanupCompletion().toCompletableFuture().join();
      verify(fixture.held).release();
      verify(fixture.admin, never()).hasUserPermissions(anyList());
    }
  }

  @Test
  void cancellationRetainsUserBorrowUntilLatePermissionRpcDrains() {
    var fixture = new Fixture();
    var permission = new CompletableFuture<List<Boolean>>();
    fixture.permissions = ignored -> permission;
    try (var scope = RequestScope.withoutDeadline()) {
      fixture.call(Optional.of(TOKEN), false, scope);
      scope.cancel();
      fixture.requireUnchanged();
      verify(fixture.held, never()).release();
      assertFalse(permission.isCancelled());
      permission.complete(List.of(true, true, true));
      scope.cleanupCompletion().toCompletableFuture().join();
      verify(fixture.held).release();
    }
  }

  @Test
  void rpcMetadataSelectsTheRequestSessionInsteadOfTheBackgroundIdentity() throws Exception {
    var fixture = new Fixture();
    try (var node = M5GatewayAuthenticationRpcTest.RpcNode.start(
        new KvService().withCompact(fixture.gateway()), new AuthService())) {
      var client = KVGrpc.newBlockingStub(node.channel()).withDeadlineAfter(3, TimeUnit.SECONDS);
      assertEquals(Status.Code.UNAUTHENTICATED,
          assertThrows(StatusRuntimeException.class, () -> client.compact(request(false)))
              .getStatus().getCode());
      fixture.requireUnchanged();
      var headers = new Metadata();
      headers.put(Metadata.Key.of("token", Metadata.ASCII_STRING_MARSHALLER), TOKEN);
      var authenticated = KVGrpc.newBlockingStub(ClientInterceptors.intercept(node.channel(),
          MetadataUtils.newAttachHeadersInterceptor(headers))).withDeadlineAfter(3, TimeUnit.SECONDS);
      assertEquals(12, authenticated.compact(request(false)).getHeader().getRevision());
      assertEquals(5, fixture.state.get().compactRevision());
      verify(fixture.connections, never()).connectionFor(SERVICE);
    }
  }

  private static Rpc.CompactionRequest request(boolean physical) {
    return Rpc.CompactionRequest.newBuilder().setRevision(5).setPhysical(physical).build();
  }

  private static AuthorizedBackendIdentity identity(String subject) {
    return new AuthorizedBackendIdentity("provider", subject, subject + "-context", AUTH_SCOPE, Set.of(GROUP), 1);
  }

  private static AuthSessionException invalidToken() {
    return new AuthSessionException(AuthSessionException.Reason.INVALID_TOKEN);
  }

  private static final class Fixture {
    private final CommitControlStore control = mock(CommitControlStore.class);
    private final SystemRecordStore records = mock(SystemRecordStore.class);
    private final Map<ByteKey, ByteKey> values = new HashMap<>();
    private final AtomicReference<CommitState> state = new AtomicReference<>(CommitState.restore(
        CommitState.FORMAT_VERSION, ByteKey.copyOf(new byte[32]), Optional.of("owner"), 4,
        Long.MAX_VALUE, 2, 2, 12, 2, 12, Optional.empty(), 0,
        Optional.empty(), Optional.empty(), 0, true, 0));
    private final CommitState initialState = state.get();
    private final AuthSessionService sessions = mock(AuthSessionService.class);
    private final boolean authenticated;
    private final HBaseConnectionFactory connections = mock(HBaseConnectionFactory.class);
    private final AsyncConnection connection = mock(AsyncConnection.class);
    private final AsyncAdmin admin = mock(AsyncAdmin.class);
    private final AtomicInteger permissionChecks = new AtomicInteger();
    private final AtomicInteger sessionChecks = new AtomicInteger();
    private final AuthSessionService.ResolvedIdentity held = resolved();
    private IntFunction<CompletableFuture<List<Boolean>>> permissions =
        ignored -> CompletableFuture.completedFuture(List.of(true, true, true));
    private IntFunction<CompletionStage<AuthSessionService.ResolvedIdentity>> revalidation =
        ignored -> CompletableFuture.completedFuture(resolved());

    private Fixture() {
      this(true);
    }

    private Fixture(boolean authenticated) {
      this.authenticated = authenticated;
      configureControl();
      configureRecords();
      when(sessions.resolve(anyString(), any())).thenReturn(CompletableFuture.failedFuture(invalidToken()));
      when(sessions.resolve(eq(TOKEN), any())).thenReturn(CompletableFuture.completedFuture(held));
      when(sessions.resolveByDigest(any(), any())).thenAnswer(ignored ->
          revalidation.apply(sessionChecks.incrementAndGet()));
      when(connections.connectionFor(USER)).thenReturn(CompletableFuture.completedFuture(connection));
      if (!authenticated) {
        when(connections.connectionFor(SERVICE)).thenReturn(CompletableFuture.completedFuture(connection));
      }
      when(connection.getAdmin()).thenReturn(admin);
      when(admin.hasUserPermissions(anyList())).thenAnswer(call -> {
        List<Permission> requested = call.getArgument(0);
        assertEquals(TABLES, requested.stream().map(value -> ((TablePermission) value).getTableName()).toList());
        assertTrue(requested.stream().allMatch(value -> Set.of(value.getActions()).equals(Set.of(Permission.Action.ADMIN))));
        return permissions.apply(permissionChecks.incrementAndGet());
      });
    }

    private void configureControl() {
      when(control.read(any())).thenAnswer(ignored -> CompletableFuture.completedFuture(Optional.of(
          new ControlSnapshot(state.get(), ByteBuffer.allocate(8).putLong(state.get().stateVersion()).array()))));
      when(control.compareAndSet(any(), any(), any())).thenAnswer(call -> CompletableFuture.completedFuture(
          state.compareAndSet(((ControlSnapshot) call.getArgument(0)).state(), call.getArgument(1))));
    }

    private void configureRecords() {
      when(records.read(any(), any(), any())).thenAnswer(call ->
          CompletableFuture.completedFuture(Optional.ofNullable(values.get(call.<ByteKey>getArgument(1)))));
      when(records.createIfAbsent(any(), any(), any(), any())).thenAnswer(call -> {
        ByteKey previous = values.putIfAbsent(call.getArgument(1), call.getArgument(2));
        return CompletableFuture.completedFuture(previous == null ? SystemRecordStore.CreateResult.CREATED
            : previous.equals(call.getArgument(2)) ? SystemRecordStore.CreateResult.ALREADY_PRESENT
            : SystemRecordStore.CreateResult.CONFLICT);
      });
      when(records.compareAndSet(any(), any(), any(), any(), any())).thenAnswer(call -> {
        boolean matched = Optional.ofNullable(values.get(call.<ByteKey>getArgument(1))).equals(call.getArgument(2));
        if (matched) values.put(call.getArgument(1), call.getArgument(3));
        return CompletableFuture.completedFuture(matched);
      });
    }

    private AuthSessionService.ResolvedIdentity resolved() {
      var result = mock(AuthSessionService.ResolvedIdentity.class);
      var envelope = new CredentialEnvelope("key", CredentialEnvelope.CURRENT_AAD_VERSION,
          new byte[CredentialEnvelope.NONCE_BYTES], new byte[CredentialEnvelope.TAG_BYTES]);
      var session = new AuthSession(ByteKey.copyOf(new byte[16]), ByteKey.copyOf(new byte[32]),
          ByteKey.copyOf(new byte[32]), "domain", "provider", AUTH_SCOPE, ByteKey.copyOf(new byte[32]),
          1, 1, 1, 2, 0, 1, AuthSession.Status.ACTIVE, envelope);
      when(result.identity()).thenReturn(USER);
      when(result.session()).thenReturn(session);
      when(result.release()).thenReturn(CompletableFuture.completedFuture(null));
      return result;
    }

    private CompactGateway gateway() {
      var authorization = new CompactRequestAuthorization(GROUP, SERVICE,
          AuthorizationScopeDigest.fromIdentity(SERVICE), authenticated ? sessions : null, connections, TABLES);
      return new CompactGateway(authorization, 7, 9, new CompactionService(control, records, Clock.systemUTC()),
          (bytes, scope, work) -> work.apply(scope),
          () -> Optional.of(new CommitOwnerService.OwnerToken("owner", 4)),
          scope -> CompletableFuture.completedFuture(state.get()), Optional.empty());
    }

    private RecordingObserver call(Optional<String> token, boolean physical, RequestScope scope) {
      var observer = new RecordingObserver();
      gateway().accept(request(physical), token, scope, observer);
      return observer;
    }

    private void requireUnchanged() {
      assertTrue(values.isEmpty());
      assertSame(initialState, state.get());
      verify(records, never()).createIfAbsent(any(), any(), any(), any());
      verify(records, never()).compareAndSet(any(), any(), any(), any(), any());
      verify(control, never()).compareAndSet(any(), any(), any());
    }

    private void requirePreparedWithoutSelection() {
      assertEquals(1, values.size());
      assertEquals(CompactionOperation.Stage.PREPARED,
          CompactionOperation.decode(values.values().iterator().next()).stage());
      assertSame(initialState, state.get());
      verify(control, never()).compareAndSet(any(), any(), any());
    }
  }

  private static final class RecordingObserver implements StreamObserver<Rpc.CompactionResponse> {
    private int responses;
    private boolean completed;
    private Throwable failure;
    @Override public void onNext(Rpc.CompactionResponse response) { responses++; }
    @Override public void onCompleted() { completed = true; }
    @Override public void onError(Throwable value) { failure = value; }
    private void requireFailure(Status.Code expected) {
      assertEquals(0, responses);
      assertFalse(completed);
      assertEquals(expected, assertInstanceOf(StatusRuntimeException.class, failure).getStatus().getCode());
    }
  }
}
