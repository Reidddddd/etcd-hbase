package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/** Bounded contiguous leaf page for one transaction's auxiliary mutation references. */
public final class AuxiliaryPage {
  public static final int MAX_RECORDS = 64;
  public static final int MAX_PAGE_BYTES = 1024 * 1024;

  private final TransactionId transactionId;
  private final int pageIndex;
  private final List<AuxiliaryRecordReference> records;
  private final long payloadBytes;
  private final ByteKey digest;

  private AuxiliaryPage(TransactionId transactionId, int pageIndex,
      List<AuxiliaryRecordReference> records, ByteKey expectedDigest) {
    this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
    this.records = List.copyOf(records);
    if (pageIndex < 0 || this.records.isEmpty() || this.records.size() > MAX_RECORDS
        || expectedDigest != null && expectedDigest.length() != 32) {
      throw new IllegalArgumentException("invalid auxiliary page");
    }
    int encodedBytes = 64;
    long totalBytes = 0;
    for (int index = 0; index < this.records.size(); index++) {
      AuxiliaryRecordReference record = this.records.get(index);
      if (index > 0 && this.records.get(index - 1).ordinal() + 1 != record.ordinal()) {
        throw new IllegalArgumentException("auxiliary page ordinals are not contiguous");
      }
      encodedBytes = Math.addExact(encodedBytes,
          Math.addExact(64, record.identity().length()));
      totalBytes = Math.addExact(totalBytes, record.encodedBytes());
    }
    if (encodedBytes > MAX_PAGE_BYTES) {
      throw new IllegalArgumentException("auxiliary page exceeds its byte budget");
    }
    this.pageIndex = pageIndex;
    this.payloadBytes = totalBytes;
    this.digest = calculateDigest(transactionId, pageIndex, this.records);
    if (expectedDigest != null && !this.digest.equals(expectedDigest)) {
      throw new IllegalArgumentException("auxiliary page digest mismatch");
    }
  }

  public static AuxiliaryPage create(TransactionId transactionId, int pageIndex,
      List<AuxiliaryRecordReference> records) {
    return new AuxiliaryPage(transactionId, pageIndex, records, null);
  }

  public static AuxiliaryPage restore(TransactionId transactionId, int pageIndex,
      List<AuxiliaryRecordReference> records, ByteKey digest) {
    return new AuxiliaryPage(transactionId, pageIndex, records,
        Objects.requireNonNull(digest, "digest"));
  }

  public TransactionId transactionId() { return transactionId; }
  public int pageIndex() { return pageIndex; }
  public List<AuxiliaryRecordReference> records() { return records; }
  public long itemCount() { return records.size(); }
  public long payloadBytes() { return payloadBytes; }
  public ByteKey digest() { return digest; }
  public long firstOrdinal() { return records.getFirst().ordinal(); }
  public long lastOrdinal() { return records.getLast().ordinal(); }

  public AuxiliaryPageReference reference() {
    return new AuxiliaryPageReference(pageIndex, systemIdentity(), digest, firstOrdinal(),
        lastOrdinal(), itemCount(), payloadBytes);
  }

  public ByteKey systemIdentity() {
    return ByteKey.copyOf(ByteBuffer.allocate(16 + 1 + Integer.BYTES)
        .put(transactionId.bytes()).put(ManifestRecordFormat.AUXILIARY_PAGE)
        .putInt(pageIndex).array());
  }

  private static ByteKey calculateDigest(TransactionId transactionId, int pageIndex,
      List<AuxiliaryRecordReference> records) {
    return ManifestDigest.sha256(output -> {
      output.writeInt(1);
      output.write(transactionId.bytes());
      output.writeInt(pageIndex);
      output.writeInt(records.size());
      for (AuxiliaryRecordReference record : records) {
        output.writeLong(record.ordinal());
        ManifestDigest.writeBytes(output, record.identity().bytes());
        output.write(record.digest().bytes());
        output.writeInt(record.encodedBytes());
      }
    });
  }
}
