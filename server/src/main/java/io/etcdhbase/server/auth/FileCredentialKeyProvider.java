package io.etcdhbase.server.auth;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/** Reads 256-bit keys from explicitly configured, owner-readable secret files. */
public final class FileCredentialKeyProvider implements CredentialKeyProvider {
  public static final int KEY_BYTES = 32;
  private static final Set<PosixFilePermission> FORBIDDEN_PERMISSIONS = EnumSet.of(
      PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE,
      PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ,
      PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE);

  private final String activeKeyId;
  private final Map<String, Path> keyFiles;
  private final AtomicBoolean closed = new AtomicBoolean();

  public FileCredentialKeyProvider(String activeKeyId, Map<String, Path> keyFiles) {
    this.activeKeyId = requireKeyId(activeKeyId);
    Objects.requireNonNull(keyFiles, "keyFiles");
    if (keyFiles.isEmpty() || keyFiles.size() > 16) {
      throw new IllegalArgumentException("configured credential key count is outside its limit");
    }
    var copy = new java.util.LinkedHashMap<String, Path>();
    keyFiles.forEach((id, path) -> {
      String checkedId = requireKeyId(id);
      Path checkedPath = Objects.requireNonNull(path, "key path").toAbsolutePath().normalize();
      if (copy.putIfAbsent(checkedId, checkedPath) != null) {
        throw new IllegalArgumentException("duplicate credential key reference");
      }
    });
    if (!copy.containsKey(this.activeKeyId)) {
      throw new IllegalArgumentException("active credential key reference is not configured");
    }
    this.keyFiles = Map.copyOf(copy);
  }

  @Override
  public String activeKeyId() {
    ensureOpen();
    return activeKeyId;
  }

  @Override
  public byte[] keyBytes(String keyId) {
    ensureOpen();
    String checkedId = requireKeyId(keyId);
    Path path = keyFiles.get(checkedId);
    if (path == null) throw new IllegalArgumentException("credential key reference is unavailable");
    try {
      if (!Files.isRegularFile(path)) {
        throw keyUnavailable();
      }
      requirePrivateFile(path);
      try (InputStream input = Files.newInputStream(path)) {
        byte[] key = input.readNBytes(KEY_BYTES + 1);
        if (key.length != KEY_BYTES) {
          java.util.Arrays.fill(key, (byte) 0);
          throw keyUnavailable();
        }
        return key;
      }
    } catch (IOException | UnsupportedOperationException failure) {
      throw keyUnavailable();
    }
  }

  @Override
  public void close() {
    closed.set(true);
  }

  private static void requirePrivateFile(Path path) throws IOException {
    Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
    if (permissions.contains(PosixFilePermission.OWNER_READ)
        && permissions.stream().noneMatch(FORBIDDEN_PERMISSIONS::contains)) {
      return;
    }
    throw keyUnavailable();
  }

  private static String requireKeyId(String value) {
    Objects.requireNonNull(value, "keyId");
    if (!value.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException("credential key reference ID is invalid");
    }
    return value;
  }

  private void ensureOpen() {
    if (closed.get()) throw new IllegalStateException("credential key provider is closed");
  }

  private static IllegalStateException keyUnavailable() {
    return new IllegalStateException("credential key reference is unavailable or insecure");
  }
}
