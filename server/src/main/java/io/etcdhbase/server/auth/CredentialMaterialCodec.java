package io.etcdhbase.server.auth;

import io.etcdhbase.spi.identity.BackendCredentials;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Bounded binary encoding for provider credentials; secret characters never become a String. */
final class CredentialMaterialCodec {
  static final int FORMAT_VERSION = 1;
  static final int MAX_PRINCIPAL_BYTES = 1024;
  static final int MAX_SECRET_CHARS = 32_768;
  static final int MAX_SECRET_UTF8_BYTES = 60_000;
  static final int MAX_MATERIAL_BYTES = 62 * 1024;
  private static final int HEADER_BYTES = Integer.BYTES * 3;

  private CredentialMaterialCodec() {}

  static byte[] encode(BackendCredentials credentials) {
    Objects.requireNonNull(credentials, "credentials");
    byte[] principal = credentials.principal().getBytes(StandardCharsets.UTF_8);
    char[] secret = null;
    byte[] secretBytes = null;
    try {
      secret = credentials.copySecret(MAX_SECRET_CHARS);
      if (principal.length == 0 || principal.length > MAX_PRINCIPAL_BYTES || secret.length == 0) {
        throw invalid();
      }
      ByteBuffer encodedSecret = StandardCharsets.UTF_8.newEncoder()
          .onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
          .encode(CharBuffer.wrap(secret));
      if (encodedSecret.remaining() > MAX_SECRET_UTF8_BYTES) throw invalid();
      secretBytes = new byte[encodedSecret.remaining()];
      encodedSecret.get(secretBytes);
      if (encodedSecret.hasArray()) Arrays.fill(encodedSecret.array(), (byte) 0);
      int length = Math.addExact(HEADER_BYTES,
          Math.addExact(principal.length, secretBytes.length));
      if (length > MAX_MATERIAL_BYTES) throw invalid();
      return ByteBuffer.allocate(length).putInt(FORMAT_VERSION).putInt(principal.length)
          .put(principal).putInt(secretBytes.length).put(secretBytes).array();
    } catch (CharacterCodingException | ArithmeticException malformed) {
      throw invalid();
    } finally {
      if (secret != null) Arrays.fill(secret, '\0');
      if (secretBytes != null) Arrays.fill(secretBytes, (byte) 0);
      Arrays.fill(principal, (byte) 0);
    }
  }

  static BackendCredentials decode(byte[] encoded) {
    Objects.requireNonNull(encoded, "encoded");
    if (encoded.length < HEADER_BYTES + 2 || encoded.length > MAX_MATERIAL_BYTES) throw invalid();
    byte[] principalBytes = null;
    byte[] secretBytes = null;
    char[] secret = null;
    try {
      ByteBuffer input = ByteBuffer.wrap(encoded);
      if (input.getInt() != FORMAT_VERSION) throw invalid();
      int principalLength = input.getInt();
      if (principalLength < 1 || principalLength > MAX_PRINCIPAL_BYTES
          || principalLength > input.remaining() - Integer.BYTES) throw invalid();
      principalBytes = new byte[principalLength];
      input.get(principalBytes);
      int secretLength = input.getInt();
      if (secretLength < 1 || secretLength > MAX_SECRET_UTF8_BYTES
          || secretLength != input.remaining()) throw invalid();
      secretBytes = new byte[secretLength];
      input.get(secretBytes);
      String principal = StandardCharsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(principalBytes)).toString();
      CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(secretBytes));
      if (decoded.remaining() > MAX_SECRET_CHARS) throw invalid();
      secret = new char[decoded.remaining()];
      decoded.get(secret);
      if (decoded.hasArray()) Arrays.fill(decoded.array(), '\0');
      BackendCredentials credentials = new BackendCredentials(principal, secret);
      return credentials;
    } catch (CharacterCodingException | java.nio.BufferUnderflowException malformed) {
      throw invalid();
    } finally {
      if (principalBytes != null) Arrays.fill(principalBytes, (byte) 0);
      if (secretBytes != null) Arrays.fill(secretBytes, (byte) 0);
      if (secret != null) Arrays.fill(secret, '\0');
    }
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("credential material violates its size or format limit");
  }
}
