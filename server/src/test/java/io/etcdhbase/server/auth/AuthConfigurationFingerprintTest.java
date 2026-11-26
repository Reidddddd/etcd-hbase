package io.etcdhbase.server.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.server.config.M3AuthenticationConfiguration;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.AuthControl;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.BackgroundIdentity;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.ClientProfile;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.CredentialKeyRing;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.CredentialSource;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.CredentialVersion;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.EndpointBinding;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.IdentityCredential;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.KeyReference;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.util.List;
import org.junit.jupiter.api.Test;

class AuthConfigurationFingerprintTest {
  private static final AuthenticationScope SCOPE = new AuthenticationScope("group-a", "profile");
  private static final ByteKey DATASET = ByteKey.copyOf(new byte[32]);
  private static final IdentityCredential IDENTITY = new IdentityCredential(
      CredentialSource.CREDENTIAL_REFERENCE, null, "env://SERVICE_IDENTITY");

  @Test
  void equivalentUnorderedBindingsHaveTheSameFingerprint() {
    var first = configuration(false, 60_000);
    var reordered = configuration(true, 60_000);
    assertEquals(AuthConfigurationFingerprint.compute(first, SCOPE, DATASET),
        AuthConfigurationFingerprint.compute(reordered, SCOPE, DATASET));
  }

  @Test
  void changedPolicyScopeOrDatasetChangesTheFingerprint() {
    var first = AuthConfigurationFingerprint.compute(configuration(false, 60_000), SCOPE, DATASET);
    assertNotEquals(first, AuthConfigurationFingerprint.compute(configuration(false, 120_000),
        SCOPE, DATASET));
    assertNotEquals(first, AuthConfigurationFingerprint.compute(configuration(false, 60_000),
        new AuthenticationScope("group-b", "profile"), DATASET));
    byte[] changed = new byte[32];
    changed[0] = 1;
    assertNotEquals(first, AuthConfigurationFingerprint.compute(configuration(false, 60_000),
        SCOPE, ByteKey.copyOf(changed)));
  }

  private static M3AuthenticationConfiguration configuration(boolean reverse, long ttl) {
    var clients = List.of(
        new ClientProfile("client-a", "profile", "kerberos-password", null, "/etc/a.xml",
            ordered(List.of(new CredentialVersion("alice", 1, "env://ALICE"),
                new CredentialVersion("bob", 2, "env://BOB")), reverse)),
        new ClientProfile("client-b", "profile", "kerberos-password", null, "/etc/b.xml"));
    var endpoints = List.of(new EndpointBinding("group-a", "provider", "client-a"),
        new EndpointBinding("group-b", "provider", "client-b"));
    var backgrounds = List.of(
        new BackgroundIdentity("group-a", "provider", "client-a", IDENTITY,
            ordered(List.of("group-a", "group-b"), reverse)),
        new BackgroundIdentity("group-b", "provider", "client-b", IDENTITY,
            ordered(List.of("group-a", "group-b"), reverse)));
    var keys = List.of(new KeyReference("key-a", "secret://key-a"),
        new KeyReference("key-b", "secret://key-b"));
    return new M3AuthenticationConfiguration("trust", ttl, 8, 8, "/secrets",
        new AuthControl("profile", "salt", "auth_control", "provider", "client-a", IDENTITY),
        ordered(clients, reverse), ordered(endpoints, reverse), ordered(backgrounds, reverse),
        new CredentialKeyRing("key-a", ordered(keys, reverse)));
  }

  private static <T> List<T> ordered(List<T> values, boolean reverse) {
    return reverse ? values.reversed() : values;
  }
}
