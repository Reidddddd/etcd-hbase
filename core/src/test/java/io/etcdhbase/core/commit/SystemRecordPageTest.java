package io.etcdhbase.core.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SystemRecordPageTest {
  @Test
  void preservesStorageOrderForLengthFramedIdentities() {
    var shortIdentity = ByteKey.copyOf(new byte[] {9});
    var longIdentity = ByteKey.copyOf(new byte[] {1, 1});
    var value = ByteKey.copyOf(new byte[] {2});
    var entries = List.of(new SystemRecordEntry(shortIdentity, value), new SystemRecordEntry(longIdentity, value));
    // The physical codec puts identity length first, so this is valid storage order.
    assertEquals(entries, new SystemRecordPage(entries, Optional.of(longIdentity), true, 5).entries());
  }

  @Test
  void rejectsDuplicateIdentitiesRegardlessOfStorageEncoding() {
    var entry = new SystemRecordEntry(ByteKey.copyOf(new byte[] {1}), ByteKey.copyOf(new byte[] {2}));
    assertThrows(IllegalArgumentException.class,
        () -> new SystemRecordPage(List.of(entry, entry), Optional.of(entry.identity()), true, 4));
  }
}
