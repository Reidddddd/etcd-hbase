package io.etcdhbase.server.auth;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.server.config.M3AuthenticationConfiguration.CredentialSource;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.IdentityCredential;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CredentialReferenceResolverTest {
  @TempDir Path directory;

  @Test
  void resolvesPrivateFileAsPrincipalAndPasswordAndClearsCallerBuffer() throws Exception {
    Path secretDirectory = Files.createDirectory(directory.resolve("secrets"));
    Path credential = secretDirectory.resolve("backend-credentials");
    byte[] material = "backend-user\nprivate-password".getBytes(StandardCharsets.UTF_8);
    Files.write(credential, material);
    Files.setPosixFilePermissions(credential, Set.of(PosixFilePermission.OWNER_READ,
        PosixFilePermission.OWNER_WRITE));
    var identity = new IdentityCredential(CredentialSource.CREDENTIAL_REFERENCE, null,
        "secret://backend-credentials");

    try (var credentials = CredentialReferenceResolver.resolveIdentity(identity,
        secretDirectory.toString(), Map.of())) {
      assertEquals("backend-user", credentials.principal());
      assertArrayEquals("private-password".toCharArray(), credentials.copySecret());
    }
  }

  @Test
  void rejectsTraversalAndGroupReadableCredentialFiles() throws Exception {
    Path secretDirectory = Files.createDirectory(directory.resolve("secrets"));
    assertThrows(IllegalStateException.class, () -> CredentialReferenceResolver.resolveSecretFile(
        "secret://../outside", secretDirectory.toString()));

    Path credential = secretDirectory.resolve("shared");
    Files.writeString(credential, "backend-user\nprivate-password");
    Files.setPosixFilePermissions(credential, Set.of(PosixFilePermission.OWNER_READ,
        PosixFilePermission.GROUP_READ));
    var identity = new IdentityCredential(CredentialSource.CREDENTIAL_REFERENCE, null,
        "secret://shared");
    assertThrows(IllegalStateException.class, () -> CredentialReferenceResolver.resolveIdentity(
        identity, secretDirectory.toString(), Map.of()));
  }

  @Test
  void resolvesEnvironmentReferenceWithoutEchoingSecret() {
    var identity = new IdentityCredential(CredentialSource.CREDENTIAL_REFERENCE, null,
        "env://ETCD_HBASE_BACKEND_CREDENTIAL");
    try (var credentials = CredentialReferenceResolver.resolveIdentity(identity, null,
        Map.of("ETCD_HBASE_BACKEND_CREDENTIAL", "backend-user\nprivate-password"))) {
      assertEquals("backend-user", credentials.principal());
      assertArrayEquals("private-password".toCharArray(), credentials.copySecret());
    }
    assertThrows(IllegalStateException.class, () -> CredentialReferenceResolver.resolveIdentity(
        identity, null, Map.of()));
  }
}
