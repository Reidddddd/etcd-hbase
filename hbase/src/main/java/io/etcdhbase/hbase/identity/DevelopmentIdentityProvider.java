package io.etcdhbase.hbase.identity;

import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.identity.BackendIdentityException;
import io.etcdhbase.spi.identity.BackendIdentityException.Reason;
import io.etcdhbase.spi.identity.BackendIdentityProvider;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Synthetic identity provider used only by an explicitly selected DEV_NO_AUTH profile. */
public final class DevelopmentIdentityProvider implements BackendIdentityProvider {
  public static final String PROVIDER_ID = DEVELOPMENT_NO_AUTH_PROVIDER_ID;
  private static final int MAX_PRINCIPAL_CODE_POINTS = 256;

  @Override
  public String providerId() {
    return PROVIDER_ID;
  }

  @Override
  public CompletionStage<AuthorizedBackendIdentity> authenticate(
      BackendCredentials credentials, AuthenticationScope scope) {
    Objects.requireNonNull(credentials, "credentials");
    Objects.requireNonNull(scope, "scope");
    String principal = credentials.principal();
    if (!isSafePrincipal(principal)) {
      return CompletableFuture.failedFuture(
          new BackendIdentityException(Reason.INVALID_CREDENTIALS));
    }
    char[] secretCopy;
    try {
      secretCopy = credentials.copySecret();
    } catch (IllegalStateException exception) {
      return CompletableFuture.failedFuture(
          new BackendIdentityException(Reason.INVALID_CREDENTIALS));
    }
    Arrays.fill(secretCopy, '\0');
    return CompletableFuture.completedFuture(
        new AuthorizedBackendIdentity(
            PROVIDER_ID,
            principal,
            "dev-no-auth",
            scope,
            Set.of(scope.tableGroupId()),
            0));
  }

  private static boolean isSafePrincipal(String principal) {
    if (principal.codePointCount(0, principal.length()) > MAX_PRINCIPAL_CODE_POINTS) {
      return false;
    }
    return principal.codePoints().noneMatch(Character::isISOControl);
  }

  @Override
  public void close() {
    // Stateless development-only provider.
  }
}
