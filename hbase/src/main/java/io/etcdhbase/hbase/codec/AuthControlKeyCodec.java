package io.etcdhbase.hbase.codec;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.spi.salt.SaltStrategy;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.hbase.HConstants;

/** Salted, length-prefixed row keys for the isolated authentication-control table. */
public final class AuthControlKeyCodec {
  public enum Kind {
    SESSION(1, 32),
    SUBJECT_AUTHORITY(2, 32),
    SLOT(3, Integer.BYTES),
    AUTH_DEPLOYMENT_REGISTRY(8, 32);

    private final int id;
    private final int identityLength;

    Kind(int id, int identityLength) {
      this.id = id;
      this.identityLength = identityLength;
    }

    public int id() {
      return id;
    }

    public int identityLength() {
      return identityLength;
    }
  }

  public record DecodedKey(Kind kind, ByteKey identity) {
    public DecodedKey {
      Objects.requireNonNull(kind, "kind");
      Objects.requireNonNull(identity, "identity");
    }
  }

  public static final int FORMAT_VERSION = 1;
  private static final int MAX_IDENTITY_BYTES = 32;
  private final SaltStrategy salt;

  public AuthControlKeyCodec(SaltStrategy salt) {
    this.salt = Objects.requireNonNull(salt, "salt");
  }

  public byte[] session(ByteKey tokenDigest) {
    return encode(Kind.SESSION, tokenDigest);
  }

  public byte[] authority(ByteKey subjectDigest) {
    return encode(Kind.SUBJECT_AUTHORITY, subjectDigest);
  }

  public byte[] deploymentRegistry(String trustDomain) {
    if (trustDomain == null || !trustDomain.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException("invalid auth deployment binding");
    }
    try {
      var digest = MessageDigest.getInstance("SHA-256");
      digest.update("etcd-hbase/auth-deployment-registry/v1\0".getBytes(StandardCharsets.UTF_8));
      return encode(Kind.AUTH_DEPLOYMENT_REGISTRY,
          ByteKey.copyOf(digest.digest(trustDomain.getBytes(StandardCharsets.UTF_8))));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 unavailable", impossible);
    }
  }

  public byte[] slot(int index) {
    if (index < 0) throw new IllegalArgumentException("slot index must not be negative");
    return encode(Kind.SLOT, ByteKey.copyOf(ByteBuffer.allocate(Integer.BYTES).putInt(index).array()));
  }

  public byte[] encode(Kind kind, ByteKey identity) {
    validate(kind, identity);
    byte[] identifier = identifier(kind, identity);
    byte[] prefix = checkedPrefix(salt.salt(identifier));
    long rowLength = (long) prefix.length + 1 + Integer.BYTES + identity.length();
    if (rowLength > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("auth-control row exceeds the HBase row limit");
    }
    return ByteBuffer.allocate((int) rowLength).put(prefix).put((byte) kind.id())
        .putInt(identity.length()).put(identity.bytes()).array();
  }

  public DecodedKey decode(byte[] row) {
    Objects.requireNonNull(row, "row");
    int prefixBytes = salt.profile().saltWidthBytes();
    if (row.length < prefixBytes + 1 + Integer.BYTES + 1
        || row.length > HConstants.MAX_ROW_LENGTH) {
      throw invalidRow();
    }
    ByteBuffer encoded = ByteBuffer.wrap(row);
    byte[] prefix = new byte[prefixBytes];
    encoded.get(prefix);
    Kind kind = kind(Byte.toUnsignedInt(encoded.get()));
    int identityLength = encoded.getInt();
    if (identityLength != encoded.remaining() || identityLength != kind.identityLength()) {
      throw invalidRow();
    }
    byte[] identityBytes = new byte[identityLength];
    encoded.get(identityBytes);
    ByteKey identity = ByteKey.copyOf(identityBytes);
    if (!Arrays.equals(prefix, checkedPrefix(salt.salt(identifier(kind, identity))))) {
      throw invalidRow();
    }
    return new DecodedKey(kind, identity);
  }

  private static byte[] identifier(Kind kind, ByteKey identity) {
    return ByteBuffer.allocate(1 + Integer.BYTES + identity.length())
        .put((byte) kind.id()).putInt(identity.length()).put(identity.bytes()).array();
  }

  private static void validate(Kind kind, ByteKey identity) {
    Objects.requireNonNull(kind, "kind");
    Objects.requireNonNull(identity, "identity");
    if (identity.length() != kind.identityLength() || identity.length() > MAX_IDENTITY_BYTES) {
      throw new IllegalArgumentException("auth-control identity length is invalid");
    }
  }

  private byte[] checkedPrefix(byte[] prefix) {
    Objects.requireNonNull(prefix, "salt prefix");
    if (prefix.length != salt.profile().saltWidthBytes()) {
      throw new IllegalArgumentException("salt strategy returned an invalid prefix width");
    }
    return prefix;
  }

  private static Kind kind(int id) {
    return switch (id) {
      case 1 -> Kind.SESSION;
      case 2 -> Kind.SUBJECT_AUTHORITY;
      case 3 -> Kind.SLOT;
      case 8 -> Kind.AUTH_DEPLOYMENT_REGISTRY;
      default -> throw invalidRow();
    };
  }

  private static IllegalArgumentException invalidRow() {
    return new IllegalArgumentException("auth-control row violates its key format");
  }
}
