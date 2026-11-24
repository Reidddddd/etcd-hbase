package io.etcdhbase.server.auth;

import io.etcdhbase.server.config.M3AuthenticationConfiguration.IdentityCredential;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.CredentialSource;
import io.etcdhbase.spi.identity.BackendCredentials;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Resolves operator-owned identity material without copying credential values into diagnostics. */
public final class CredentialReferenceResolver {
  private static final int MAX_MATERIAL_BYTES = CredentialMaterialCodec.MAX_MATERIAL_BYTES;
  private static final Set<PosixFilePermission> FORBIDDEN_PERMISSIONS = EnumSet.of(
      PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE,
      PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ,
      PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE);

  private CredentialReferenceResolver() {}

  public static BackendCredentials resolveIdentity(IdentityCredential identity, String secretDirectory,
      Map<String, String> environment) {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(environment, "environment");
    if (identity.source() != CredentialSource.CREDENTIAL_REFERENCE) {
      throw new IllegalArgumentException("identity source does not contain password credentials");
    }
    byte[] material = readReference(identity.credentialReference(), secretDirectory, environment);
    try {
      int separator = -1;
      for (int index = 0; index < material.length; index++) {
        if (material[index] == '\n') {
          separator = index;
          break;
        }
      }
      if (separator < 1 || separator == material.length - 1) throw unavailable();
      String principal = decodeText(material, 0, separator);
      int passwordStart = separator + 1;
      if (material[separator - 1] == '\r') throw unavailable();
      CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(material, passwordStart, material.length - passwordStart));
      if (decoded.remaining() == 0 || decoded.remaining() > CredentialMaterialCodec.MAX_SECRET_CHARS) {
        throw unavailable();
      }
      char[] password = new char[decoded.remaining()];
      decoded.get(password);
      if (decoded.hasArray()) Arrays.fill(decoded.array(), '\0');
      try {
        if (principal.isBlank() || principal.length() > 256
            || principal.codePoints().anyMatch(Character::isISOControl)) throw unavailable();
        return new BackendCredentials(principal, password);
      } finally {
        Arrays.fill(password, '\0');
      }
    } catch (CharacterCodingException | RuntimeException failure) {
      throw unavailable();
    } finally {
      Arrays.fill(material, (byte) 0);
    }
  }

  public static Path resolveSecretFile(String reference, String secretDirectory) {
    if (reference == null || !reference.startsWith("secret://") || secretDirectory == null) {
      throw unavailable();
    }
    try {
      Path root = Path.of(secretDirectory).toAbsolutePath().normalize().toRealPath();
      Path candidate = root.resolve(reference.substring("secret://".length())).normalize();
      if (!candidate.startsWith(root) || candidate.equals(root)) throw unavailable();
      Path real = candidate.toRealPath();
      if (!real.startsWith(root) || !Files.isRegularFile(real, LinkOption.NOFOLLOW_LINKS)) {
        throw unavailable();
      }
      return real;
    } catch (IOException | SecurityException | UnsupportedOperationException failure) {
      throw unavailable();
    }
  }

  public static Path resolvePrivateSecretFile(String reference, String secretDirectory) {
    Path secret = resolveSecretFile(reference, secretDirectory);
    try {
      Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(secret);
      if (!permissions.contains(PosixFilePermission.OWNER_READ)
          || permissions.stream().anyMatch(FORBIDDEN_PERMISSIONS::contains)) throw unavailable();
      return secret;
    } catch (IOException | SecurityException | UnsupportedOperationException failure) {
      throw unavailable();
    }
  }

  private static byte[] readReference(String reference, String secretDirectory,
      Map<String, String> environment) {
    if (reference.startsWith("env://")) {
      String value = environment.get(reference.substring("env://".length()));
      if (value == null) throw unavailable();
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      if (bytes.length == 0 || bytes.length > MAX_MATERIAL_BYTES) {
        Arrays.fill(bytes, (byte) 0);
        throw unavailable();
      }
      return bytes;
    }
    Path secret = resolvePrivateSecretFile(reference, secretDirectory);
    try {
      byte[] bytes = Files.readAllBytes(secret);
      if (bytes.length == 0 || bytes.length > MAX_MATERIAL_BYTES) {
        Arrays.fill(bytes, (byte) 0);
        throw unavailable();
      }
      return bytes;
    } catch (IOException | SecurityException | UnsupportedOperationException failure) {
      throw unavailable();
    }
  }

  private static String decodeText(byte[] input, int offset, int length)
      throws CharacterCodingException {
    return StandardCharsets.UTF_8.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(input, offset, length)).toString();
  }

  private static IllegalStateException unavailable() {
    return new IllegalStateException("configured identity credential is unavailable or invalid");
  }
}
