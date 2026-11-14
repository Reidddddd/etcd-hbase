package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/** Bounded contiguous page reference stored in the flat auxiliary directory. */
public record AuxiliaryPageReference(int pageIndex, ByteKey identity, ByteKey digest,
    long firstOrdinal, long lastOrdinal, long itemCount, long payloadBytes) {
  public AuxiliaryPageReference {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(digest, "digest");
    if (pageIndex < 0 || identity.length() == 0 || digest.length() != 32 || firstOrdinal < 0
        || lastOrdinal < firstOrdinal || itemCount <= 0 || lastOrdinal - firstOrdinal + 1 != itemCount
        || itemCount > AuxiliaryPage.MAX_RECORDS || payloadBytes <= 0) {
      throw new IllegalArgumentException("invalid auxiliary page reference");
    }
  }

  public static AuxiliaryPageReference from(AuxiliaryPage page) {
    Objects.requireNonNull(page, "page");
    return new AuxiliaryPageReference(page.pageIndex(), page.systemIdentity(), page.digest(),
        page.firstOrdinal(), page.lastOrdinal(), page.itemCount(), page.payloadBytes());
  }

  public boolean matches(AuxiliaryPage page) {
    Objects.requireNonNull(page, "page");
    return pageIndex == page.pageIndex() && identity.equals(page.systemIdentity())
        && digest.equals(page.digest()) && firstOrdinal == page.firstOrdinal()
        && lastOrdinal == page.lastOrdinal() && itemCount == page.itemCount()
        && payloadBytes == page.payloadBytes();
  }
}
