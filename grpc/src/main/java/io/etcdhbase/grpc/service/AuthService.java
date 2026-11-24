package io.etcdhbase.grpc.service;

import etcdserverpb.AuthGrpc;
import etcdserverpb.Rpc;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.function.BiConsumer;

/** Auth service surface with an optional Authenticate handler; management methods remain unimplemented. */
public final class AuthService extends AuthGrpc.AuthImplBase {
  private final BiConsumer<Rpc.AuthenticateRequest, StreamObserver<Rpc.AuthenticateResponse>>
      authenticateHandler;

  public AuthService() {
    this.authenticateHandler = null;
  }

  public AuthService(
      BiConsumer<Rpc.AuthenticateRequest, StreamObserver<Rpc.AuthenticateResponse>>
          authenticateHandler) {
    this.authenticateHandler = Objects.requireNonNull(authenticateHandler, "authenticateHandler");
  }

  @Override
  public void authEnable(
      Rpc.AuthEnableRequest request, StreamObserver<Rpc.AuthEnableResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.AuthEnable");
  }

  @Override
  public void authDisable(
      Rpc.AuthDisableRequest request, StreamObserver<Rpc.AuthDisableResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.AuthDisable");
  }

  @Override
  public void authStatus(
      Rpc.AuthStatusRequest request, StreamObserver<Rpc.AuthStatusResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.AuthStatus");
  }

  @Override
  public void authenticate(
      Rpc.AuthenticateRequest request, StreamObserver<Rpc.AuthenticateResponse> responseObserver) {
    if (authenticateHandler == null) {
      UnimplementedStreams.failUnary(responseObserver, "Auth.Authenticate");
    } else {
      authenticateHandler.accept(request, responseObserver);
    }
  }

  @Override
  public void userAdd(
      Rpc.AuthUserAddRequest request, StreamObserver<Rpc.AuthUserAddResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.UserAdd");
  }

  @Override
  public void userGet(
      Rpc.AuthUserGetRequest request, StreamObserver<Rpc.AuthUserGetResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.UserGet");
  }

  @Override
  public void userList(
      Rpc.AuthUserListRequest request, StreamObserver<Rpc.AuthUserListResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.UserList");
  }

  @Override
  public void userDelete(
      Rpc.AuthUserDeleteRequest request,
      StreamObserver<Rpc.AuthUserDeleteResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.UserDelete");
  }

  @Override
  public void userChangePassword(
      Rpc.AuthUserChangePasswordRequest request,
      StreamObserver<Rpc.AuthUserChangePasswordResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.UserChangePassword");
  }

  @Override
  public void userGrantRole(
      Rpc.AuthUserGrantRoleRequest request,
      StreamObserver<Rpc.AuthUserGrantRoleResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.UserGrantRole");
  }

  @Override
  public void userRevokeRole(
      Rpc.AuthUserRevokeRoleRequest request,
      StreamObserver<Rpc.AuthUserRevokeRoleResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.UserRevokeRole");
  }

  @Override
  public void roleAdd(
      Rpc.AuthRoleAddRequest request, StreamObserver<Rpc.AuthRoleAddResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.RoleAdd");
  }

  @Override
  public void roleGet(
      Rpc.AuthRoleGetRequest request, StreamObserver<Rpc.AuthRoleGetResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.RoleGet");
  }

  @Override
  public void roleList(
      Rpc.AuthRoleListRequest request, StreamObserver<Rpc.AuthRoleListResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.RoleList");
  }

  @Override
  public void roleDelete(
      Rpc.AuthRoleDeleteRequest request,
      StreamObserver<Rpc.AuthRoleDeleteResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.RoleDelete");
  }

  @Override
  public void roleGrantPermission(
      Rpc.AuthRoleGrantPermissionRequest request,
      StreamObserver<Rpc.AuthRoleGrantPermissionResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.RoleGrantPermission");
  }

  @Override
  public void roleRevokePermission(
      Rpc.AuthRoleRevokePermissionRequest request,
      StreamObserver<Rpc.AuthRoleRevokePermissionResponse> responseObserver) {
    UnimplementedStreams.failUnary(responseObserver, "Auth.RoleRevokePermission");
  }
}
