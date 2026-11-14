package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/** Flat, bounded directory whose page ranges cover every auxiliary ordinal exactly once. */
public final class AuxiliaryDirectory {
  private final TransactionId transactionId;
  private final List<AuxiliaryPageReference> pages;
  private final long itemCount;
  private final long totalBytes;
  private final ByteKey digest;

  private AuxiliaryDirectory(TransactionId transactionId, List<AuxiliaryPageReference> pages,
      ByteKey expectedDigest) {
    this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
    this.pages = List.copyOf(pages);
    if (this.pages.isEmpty() || this.pages.size() > AuxiliaryReference.MAX_PAGES
        || expectedDigest != null && expectedDigest.length() != 32) {
      throw new IllegalArgumentException("invalid auxiliary directory");
    }
    long count = 0;
    long bytes = 0;
    for (int index = 0; index < this.pages.size(); index++) {
      AuxiliaryPageReference page = this.pages.get(index);
      if (page.pageIndex() != index || page.firstOrdinal() != count) {
        throw new IllegalArgumentException("auxiliary directory pages are not contiguous");
      }
      count = Math.addExact(count, page.itemCount());
      bytes = Math.addExact(bytes, page.payloadBytes());
    }
    if (count > AuxiliaryReference.MAX_RECORDS || bytes > AuxiliaryReference.MAX_TOTAL_BYTES) {
      throw new IllegalArgumentException("auxiliary directory exceeds its budget");
    }
    this.itemCount = count;
    this.totalBytes = bytes;
    this.digest = calculateDigest(transactionId, this.pages);
    if (expectedDigest != null && !this.digest.equals(expectedDigest)) {
      throw new IllegalArgumentException("auxiliary directory digest mismatch");
    }
  }

  public static AuxiliaryDirectory create(TransactionId transactionId,
      List<AuxiliaryPageReference> pages) {
    return new AuxiliaryDirectory(transactionId, pages, null);
  }

  public static AuxiliaryDirectory restore(TransactionId transactionId,
      List<AuxiliaryPageReference> pages, ByteKey digest) {
    return new AuxiliaryDirectory(transactionId, pages, Objects.requireNonNull(digest, "digest"));
  }

  public TransactionId transactionId() { return transactionId; }
  public List<AuxiliaryPageReference> pages() { return pages; }
  public long itemCount() { return itemCount; }
  public long totalBytes() { return totalBytes; }
  public ByteKey digest() { return digest; }

  public AuxiliaryReference reference() {
    return new AuxiliaryReference(systemIdentity(), digest, itemCount, totalBytes, pages.size());
  }

  public ByteKey systemIdentity() {
    return identityFor(transactionId);
  }

  static ByteKey identityFor(TransactionId transactionId) {
    Objects.requireNonNull(transactionId, "transactionId");
    return ByteKey.copyOf(ByteBuffer.allocate(16 + 1)
        .put(transactionId.bytes()).put(ManifestRecordFormat.AUXILIARY_DIRECTORY).array());
  }

  private static ByteKey calculateDigest(TransactionId transactionId,
      List<AuxiliaryPageReference> pages) {
    return ManifestDigest.sha256(output -> {
      output.writeInt(1);
      output.write(transactionId.bytes());
      output.writeInt(pages.size());
      for (AuxiliaryPageReference page : pages) {
        output.writeInt(page.pageIndex());
        ManifestDigest.writeBytes(output, page.identity().bytes());
        output.write(page.digest().bytes());
        output.writeLong(page.firstOrdinal());
        output.writeLong(page.lastOrdinal());
        output.writeLong(page.itemCount());
        output.writeLong(page.payloadBytes());
      }
    });
  }
}
