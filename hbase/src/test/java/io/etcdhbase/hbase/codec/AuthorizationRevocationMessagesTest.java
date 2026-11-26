package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.auth.AuthorizationRevocation;
import io.etcdhbase.core.bytes.ByteKey;
import java.util.List;
import org.junit.jupiter.api.Test;

class AuthorizationRevocationMessagesTest {
  @Test
  void preservesFrozenTargetsAndPartialProgress() {
    var operation = AuthorizationRevocation.start(bytes(1, 16), bytes(2, 32), "provider", "realm", 2, 2, 8,
        List.of(new AuthorizationRevocation.TargetIdentity("group-a", bytes(3, 32), bytes(4, 32)),
            new AuthorizationRevocation.TargetIdentity("group-b", bytes(5, 32), bytes(6, 32))))
        .assignBarrier("group-a", 9).acknowledge("group-a", 9)
        .assignBarrier("group-b", 12);
    assertEquals(operation, AuthorizationRevocationMessages.decode(
        AuthorizationRevocationMessages.encode(operation)));
  }

  @Test
  void rejectsTruncationAndChangedPayload() {
    var operation = AuthorizationRevocation.start(bytes(1, 16), bytes(2, 32), "provider", "realm", 2, 2, 8,
        List.of(new AuthorizationRevocation.TargetIdentity("group-a", bytes(3, 32), bytes(4, 32))));
    byte[] encoded = AuthorizationRevocationMessages.encode(operation);
    byte[] corrupted = encoded.clone();
    corrupted[corrupted.length / 2] ^= 1;
    assertThrows(IllegalArgumentException.class, () -> AuthorizationRevocationMessages.decode(corrupted));
    assertThrows(IllegalArgumentException.class, () -> AuthorizationRevocationMessages.decode(
        java.util.Arrays.copyOf(encoded, encoded.length - 1)));
    assertThrows(IllegalArgumentException.class, () -> AuthorizationRevocationMessages.decode(new byte[0]));
  }

  private static ByteKey bytes(int value, int length) {
    byte[] raw = new byte[length];
    java.util.Arrays.fill(raw, (byte) value);
    return ByteKey.copyOf(raw);
  }
}
