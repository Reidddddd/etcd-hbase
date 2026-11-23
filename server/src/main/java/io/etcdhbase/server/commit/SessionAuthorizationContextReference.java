package io.etcdhbase.server.commit;

import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Secret-free owner-forwarding reference to one durable, currently validated auth session. */
public record SessionAuthorizationContextReference(
    ByteKey tokenDigest,
    ByteKey sessionId,
    int slotIndex,
    long slotGeneration,
    long credentialGeneration,
    long authorizationGeneration) {
  private static final byte FORMAT_VERSION = 1;
  private static final int ENCODED_BYTES = Byte.BYTES + 32 + 16 + Integer.BYTES + 3 * Long.BYTES;

  public SessionAuthorizationContextReference {
    requireLength(tokenDigest, 32, "tokenDigest");
    requireLength(sessionId, 16, "sessionId");
    if (slotIndex < 0 || slotGeneration < 1 || credentialGeneration < 0
        || authorizationGeneration < 1) {
      throw new IllegalArgumentException("invalid session authorization reference");
    }
  }

  public static SessionAuthorizationContextReference fromSession(AuthSession session) {
    Objects.requireNonNull(session, "session");
    if (session.status() != AuthSession.Status.ACTIVE) {
      throw new IllegalArgumentException("only active sessions can be forwarded");
    }
    return new SessionAuthorizationContextReference(session.tokenDigest(), session.sessionId(),
        session.slotIndex(), session.slotGeneration(), session.credentialGeneration(),
        session.authorizationGeneration());
  }

  public static SessionAuthorizationContextReference parse(ByteKey encoded) {
    Objects.requireNonNull(encoded, "encoded");
    if (encoded.length() != ENCODED_BYTES) {
      throw new IllegalArgumentException("invalid session authorization reference");
    }
    ByteBuffer input = ByteBuffer.wrap(encoded.bytes());
    if (input.get() != FORMAT_VERSION) {
      throw new IllegalArgumentException("unsupported session authorization reference");
    }
    byte[] tokenDigest = new byte[32];
    byte[] sessionId = new byte[16];
    input.get(tokenDigest).get(sessionId);
    try {
      return new SessionAuthorizationContextReference(ByteKey.copyOf(tokenDigest),
          ByteKey.copyOf(sessionId), input.getInt(), input.getLong(), input.getLong(), input.getLong());
    } finally {
      java.util.Arrays.fill(tokenDigest, (byte) 0);
      java.util.Arrays.fill(sessionId, (byte) 0);
    }
  }

  public ByteKey encode() {
    return ByteKey.copyOf(ByteBuffer.allocate(ENCODED_BYTES).put(FORMAT_VERSION)
        .put(tokenDigest.bytes()).put(sessionId.bytes()).putInt(slotIndex).putLong(slotGeneration)
        .putLong(credentialGeneration).putLong(authorizationGeneration).array());
  }

  public boolean matches(AuthSession session) {
    return session != null && session.status() == AuthSession.Status.ACTIVE
        && tokenDigest.equals(session.tokenDigest()) && sessionId.equals(session.sessionId())
        && slotIndex == session.slotIndex() && slotGeneration == session.slotGeneration()
        && credentialGeneration == session.credentialGeneration()
        && authorizationGeneration == session.authorizationGeneration();
  }

  private static void requireLength(ByteKey value, int expected, String name) {
    Objects.requireNonNull(value, name);
    if (value.length() != expected) throw new IllegalArgumentException(name + " has an invalid length");
  }
}
