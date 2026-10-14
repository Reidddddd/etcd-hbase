package io.etcdhbase.grpc.protocol;

import etcdserverpb.Rpc;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.util.Arrays;
import java.util.Objects;

/** Maps credentials from an Authenticate request while retaining the listener-bound scope. */
public final class AuthenticateCredentialMapper {
  private AuthenticateCredentialMapper() {}

  public static MappedCredentials map(
      Rpc.AuthenticateRequest request, AuthenticationScope listenerBoundScope) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(listenerBoundScope, "listenerBoundScope");
    char[] password = request.getPassword().toCharArray();
    try {
      BackendCredentials credentials = new BackendCredentials(request.getName(), password);
      return new MappedCredentials(listenerBoundScope, credentials);
    } finally {
      Arrays.fill(password, '\0');
    }
  }

  /** Owns both the mapped credential and the scope bound to the serving listener. */
  public record MappedCredentials(
      AuthenticationScope scope, BackendCredentials credentials) implements AutoCloseable {
    public MappedCredentials {
      Objects.requireNonNull(scope, "scope");
      Objects.requireNonNull(credentials, "credentials");
    }

    @Override
    public void close() {
      credentials.close();
    }
  }
}
