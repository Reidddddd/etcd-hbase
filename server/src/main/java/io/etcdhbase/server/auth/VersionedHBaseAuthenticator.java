package io.etcdhbase.server.auth;

import io.etcdhbase.hbase.api.HBaseCredentialAuthenticator;
import io.etcdhbase.hbase.api.HBaseUserContext;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.CredentialSource;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.CredentialVersion;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.IdentityCredential;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;

/** Binds the exact submitted credential to an operator-controlled version before backend access. */
public final class VersionedHBaseAuthenticator implements HBaseCredentialAuthenticator {
  private final HBaseCredentialAuthenticator backend;
  private final Map<String, CredentialVersion> versions;
  private final String secretDirectory;
  private final Map<String, String> environment;

  public VersionedHBaseAuthenticator(HBaseCredentialAuthenticator backend,
      List<CredentialVersion> bindings, String secretDirectory, Map<String, String> environment) {
    this.backend = Objects.requireNonNull(backend, "backend");
    this.versions = Objects.requireNonNull(bindings, "bindings").stream().collect(
        java.util.stream.Collectors.toUnmodifiableMap(CredentialVersion::principal, value -> value));
    this.secretDirectory = secretDirectory;
    this.environment = Objects.requireNonNull(environment, "environment");
  }

  @Override
  public void validateConfiguration(Configuration configuration) {
    backend.validateConfiguration(configuration);
  }

  @Override
  public HBaseUserContext authenticate(BackendCredentials credentials, AuthenticationScope scope)
      throws Exception {
    CredentialVersion binding = versions.get(credentials.principal());
    if (binding == null) return backend.authenticate(credentials, scope);
    var source = new IdentityCredential(CredentialSource.CREDENTIAL_REFERENCE, null,
        binding.credentialReference());
    try (BackendCredentials configured = CredentialReferenceResolver.resolveIdentity(
        source, secretDirectory, environment)) {
      if (!configured.principal().equals(binding.principal())) {
        throw new IllegalStateException("configured credential principal does not match its binding");
      }
      if (!sameSecret(configured, credentials)) {
        throw new IllegalArgumentException("credential does not match its configured version");
      }
    }
    HBaseUserContext authenticated = Objects.requireNonNull(
        backend.authenticate(credentials, scope), "backend user context");
    try {
      if (!binding.principal().equals(authenticated.user().getName())) {
        throw new IllegalArgumentException("backend user does not match the configured credential");
      }
      return new VersionedContext(authenticated, binding.generation());
    } catch (RuntimeException | Error failure) {
      try { authenticated.close(); }
      catch (Exception cleanup) { failure.addSuppressed(cleanup); }
      throw failure;
    }
  }

  private static boolean sameSecret(BackendCredentials configured, BackendCredentials submitted) {
    char[] expected = configured.copySecret(CredentialMaterialCodec.MAX_SECRET_CHARS);
    char[] actual = null;
    try {
      actual = submitted.copySecret(CredentialMaterialCodec.MAX_SECRET_CHARS);
      int difference = expected.length ^ actual.length;
      int maximum = Math.max(expected.length, actual.length);
      for (int index = 0; index < maximum; index++) {
        char left = index < expected.length ? expected[index] : 0;
        char right = index < actual.length ? actual[index] : 0;
        difference |= left ^ right;
      }
      return difference == 0;
    } finally {
      Arrays.fill(expected, '\0');
      if (actual != null) Arrays.fill(actual, '\0');
    }
  }

  private record VersionedContext(HBaseUserContext delegate, long generation)
      implements HBaseUserContext {
    @Override public User user() { return delegate.user(); }
    @Override public OptionalLong credentialGeneration() { return OptionalLong.of(generation); }
    @Override public void close() throws IOException { delegate.close(); }
  }
}
