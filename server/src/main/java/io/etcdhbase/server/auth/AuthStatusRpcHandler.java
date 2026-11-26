package io.etcdhbase.server.auth;

import etcdserverpb.Rpc;
import io.etcdhbase.core.auth.AuthConfigurationRecord;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.grpc.RequestScopeInterceptor;
import io.etcdhbase.grpc.protocol.ErrorProjection;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** Projects the persisted bridge configuration, not an etcd-native RBAC revision. */
public final class AuthStatusRpcHandler
    implements BiConsumer<Rpc.AuthStatusRequest, StreamObserver<Rpc.AuthStatusResponse>> {
  private final Function<RequestScope, CompletionStage<AuthConfigurationRecord>> configuration;
  private final Function<RequestScope, CompletionStage<Rpc.ResponseHeader>> header;
  private final Supplier<RequestScope> scopes;

  public AuthStatusRpcHandler(
      Function<RequestScope, CompletionStage<AuthConfigurationRecord>> configuration,
      Function<RequestScope, CompletionStage<Rpc.ResponseHeader>> header) {
    this(configuration, header, RequestScopeInterceptor::currentScope);
  }

  AuthStatusRpcHandler(Function<RequestScope, CompletionStage<AuthConfigurationRecord>> configuration,
      Function<RequestScope, CompletionStage<Rpc.ResponseHeader>> header, Supplier<RequestScope> scopes) {
    this.configuration = Objects.requireNonNull(configuration, "configuration");
    this.header = Objects.requireNonNull(header, "header");
    this.scopes = Objects.requireNonNull(scopes, "scopes");
  }

  @Override
  public void accept(Rpc.AuthStatusRequest request, StreamObserver<Rpc.AuthStatusResponse> observer) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(observer, "observer");
    RequestScope scope = Objects.requireNonNull(scopes.get(), "request scope");
    CompletionStage<Rpc.AuthStatusResponse> response;
    try {
      response = Objects.requireNonNull(configuration.apply(scope), "configuration stage")
          .thenCompose(record -> Objects.requireNonNull(header.apply(scope), "header stage")
              .thenApply(responseHeader -> Rpc.AuthStatusResponse.newBuilder()
                  .setEnabled(record.enabled()).setAuthRevision(record.revision())
                  .setHeader(responseHeader).build()));
    } catch (RuntimeException failure) {
      scope.startIfOpen(() -> observer.onError(ErrorProjection.project(failure)));
      return;
    }
    response.whenComplete((value, failure) -> scope.startIfOpen(() -> {
      if (failure != null) observer.onError(ErrorProjection.project(failure));
      else {
        observer.onNext(value);
        observer.onCompleted();
      }
    }));
  }
}
