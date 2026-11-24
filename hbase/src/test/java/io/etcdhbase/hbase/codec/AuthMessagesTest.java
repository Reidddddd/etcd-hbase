package io.etcdhbase.hbase.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.AuthSlot;
import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.auth.SubjectAuthority;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.hbase.auth.proto.AuthProtos;
import io.etcdhbase.spi.identity.AuthenticationScope;
import org.junit.jupiter.api.Test;

class AuthMessagesTest {
  private static final AuthenticationScope SCOPE = new AuthenticationScope("group", "profile");

  @Test
  void eachControlRecordRoundTripsWithoutSerializingBearerOrBackendHandle() {
    AuthSession session = session();
    assertEquals(session, AuthMessages.decodeSession(AuthMessages.encode(session)));

    AuthSlot empty = new AuthSlot(4, 12, null, null, AuthSlot.State.EMPTY);
    assertEquals(empty, AuthMessages.decodeSlot(AuthMessages.encode(empty)));
    AuthSlot reserved = new AuthSlot(4, 13, key(16, 6), "AAAAAAAAAAAAAAAAAAAAAA",
        AuthSlot.State.RESERVED);
    assertEquals(reserved, AuthMessages.decodeSlot(AuthMessages.encode(reserved)));

    SubjectAuthority authority = new SubjectAuthority(key(32, 8), "milvus", "hbase-provider",
        SCOPE, 3, 5, SubjectAuthority.State.ACTIVE);
    assertEquals(authority, AuthMessages.decodeAuthority(AuthMessages.encode(authority)));
  }

  @Test
  void rejectsUnknownFieldsMissingFieldsUnknownEnumsAndMalformedLengths() throws Exception {
    byte[] encoded = AuthMessages.encode(session());
    byte[] withUnknownField = java.util.Arrays.copyOf(encoded, encoded.length + 3);
    withUnknownField[encoded.length] = (byte) 0xf0; // field 30, varint
    withUnknownField[encoded.length + 1] = 0x01;
    withUnknownField[encoded.length + 2] = 0x01;
    assertThrows(IllegalArgumentException.class,
        () -> AuthMessages.decodeSession(withUnknownField));

    AuthProtos.AuthSessionRecord unsupported = AuthProtos.AuthSessionRecord.parseFrom(encoded)
        .toBuilder().setFormatVersion(AuthMessages.FORMAT_VERSION + 1).build();
    assertThrows(IllegalArgumentException.class,
        () -> AuthMessages.decodeSession(unsupported.toByteArray()));
    assertThrows(IllegalArgumentException.class,
        () -> AuthMessages.decodeSession(new byte[0]));

    AuthProtos.AuthSlotRecord unknownState = AuthProtos.AuthSlotRecord.parseFrom(
        AuthMessages.encode(new AuthSlot(1, 0, null, null, AuthSlot.State.EMPTY)))
        .toBuilder().setStatusValue(99).build();
    assertThrows(IllegalArgumentException.class,
        () -> AuthMessages.decodeSlot(unknownState.toByteArray()));

    AuthProtos.AuthSessionRecord badDigest = AuthProtos.AuthSessionRecord.parseFrom(encoded)
        .toBuilder().setTokenDigest(com.google.protobuf.ByteString.copyFrom(new byte[31])).build();
    assertThrows(IllegalArgumentException.class,
        () -> AuthMessages.decodeSession(badDigest.toByteArray()));
  }

  private static AuthSession session() {
    byte[] ciphertext = new byte[CredentialEnvelope.TAG_BYTES + 3];
    java.util.Arrays.fill(ciphertext, (byte) 0x4d);
    return new AuthSession(key(16, 1), key(32, 2), key(32, 3), "milvus", "hbase-provider",
        SCOPE, key(32, 4), 3, 5, 100, 200, 4, 13, AuthSession.Status.ACTIVE,
        new CredentialEnvelope("key-1", 1, new byte[CredentialEnvelope.NONCE_BYTES], ciphertext));
  }

  private static ByteKey key(int length, int value) {
    byte[] bytes = new byte[length];
    java.util.Arrays.fill(bytes, (byte) value);
    return ByteKey.copyOf(bytes);
  }
}
