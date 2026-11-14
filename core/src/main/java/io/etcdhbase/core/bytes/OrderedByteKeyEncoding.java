package io.etcdhbase.core.bytes;

import java.io.ByteArrayOutputStream;
import java.util.Objects;

/** Escapes zero bytes and terminates keys while preserving unsigned byte order. */
public final class OrderedByteKeyEncoding {
  public record Decoded(ByteKey key, int nextOffset) {
    public Decoded {
      Objects.requireNonNull(key, "key");
    }
  }

  private OrderedByteKeyEncoding() {}

  public static long encodedLength(byte[] key) {
    Objects.requireNonNull(key, "key");
    long length = (long) key.length + 2;
    for (byte part : key) if (part == 0) length++;
    return length;
  }

  public static byte[] encode(byte[] key) {
    Objects.requireNonNull(key, "key");
    byte[] encoded = new byte[Math.toIntExact(encodedLength(key))];
    int offset = 0;
    for (byte part : key) {
      encoded[offset++] = part;
      if (part == 0) encoded[offset++] = (byte) 0xff;
    }
    encoded[offset++] = 0;
    encoded[offset] = 0;
    return encoded;
  }

  public static Decoded decode(byte[] encoded, int start) {
    Objects.requireNonNull(encoded, "encoded");
    if (start < 0 || start > encoded.length) throw new IndexOutOfBoundsException("invalid key offset");
    var decoded = new ByteArrayOutputStream(encoded.length - start);
    int offset = start;
    while (offset < encoded.length) {
      byte part = encoded[offset++];
      if (part != 0) {
        decoded.write(part);
        continue;
      }
      if (offset == encoded.length) break;
      int escaped = Byte.toUnsignedInt(encoded[offset++]);
      if (escaped == 0) return new Decoded(ByteKey.copyOf(decoded.toByteArray()), offset);
      if (escaped != 0xff) throw new IllegalArgumentException("invalid ordered key escape");
      decoded.write(0);
    }
    throw new IllegalArgumentException("missing ordered key terminator");
  }
}
