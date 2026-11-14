package io.etcdhbase.hbase.codec;

import io.etcdhbase.core.bytes.OrderedByteKeyEncoding;
import java.util.Objects;

/** Shared ordered byte encoding: zero escapes to 00 ff and 00 00 terminates the key. */
final class OrderedKeyBytes {
  record Decoded(byte[] key, int nextOffset) {}

  private OrderedKeyBytes() {}

  static long encodedLength(byte[] key) {
    return OrderedByteKeyEncoding.encodedLength(Objects.requireNonNull(key, "key"));
  }

  static byte[] encode(byte[] key) {
    return OrderedByteKeyEncoding.encode(Objects.requireNonNull(key, "key"));
  }

  static Decoded decode(byte[] encoded, int start) {
    var decoded = OrderedByteKeyEncoding.decode(Objects.requireNonNull(encoded, "encoded"), start);
    return new Decoded(decoded.key().bytes(), decoded.nextOffset());
  }
}
