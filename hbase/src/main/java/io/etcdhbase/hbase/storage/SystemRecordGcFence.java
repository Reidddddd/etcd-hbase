package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.commit.SystemRecordRetirement;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

/** Same-row, monotonic maintenance fence; owner epochs also distinguish owner incarnations. */
record SystemRecordGcFence(long ownerEpoch, long compactGeneration) implements Comparable<SystemRecordGcFence> {
  static final byte[] COLUMN = {'g'};
  static final int BYTES = 2 * Long.BYTES;
  static final long TIMESTAMP = Long.MAX_VALUE - 2;

  SystemRecordGcFence {
    if (ownerEpoch <= 0 || compactGeneration <= 0) throw new IllegalArgumentException("invalid GC row fence");
  }

  static SystemRecordGcFence from(SystemRecordRetirement.Permit permit) {
    return new SystemRecordGcFence(permit.authority().owner().epoch(), permit.generation());
  }

  byte[] encode() {
    return ByteBuffer.allocate(BYTES).putLong(ownerEpoch).putLong(compactGeneration).array();
  }

  static Optional<SystemRecordGcFence> read(Result result, byte[] family) {
    Cell cell = null;
    Cell[] cells = result.rawCells();
    if (cells != null) for (Cell candidate : cells) {
      if (!org.apache.hadoop.hbase.CellUtil.matchingFamily(candidate, family)) continue;
      if (org.apache.hadoop.hbase.CellUtil.matchingQualifier(candidate, InvocationWindowEvidence.COLUMN)) {
        if (candidate.getTimestamp() != InvocationWindowEvidence.TIMESTAMP) {
          throw new IllegalArgumentException("invalid invocation window evidence timestamp");
        }
        InvocationWindowEvidence.decode(io.etcdhbase.core.bytes.ByteKey.copyOf(
            org.apache.hadoop.hbase.CellUtil.cloneValue(candidate)));
        continue;
      }
      if (!org.apache.hadoop.hbase.CellUtil.matchingQualifier(candidate, COLUMN)) {
        throw new IllegalArgumentException("unexpected GC maintenance column");
      }
      if (cell != null) throw new IllegalArgumentException("duplicate GC row-fence cell");
      cell = candidate;
    }
    if (cell == null) return Optional.empty();
    if (cell.getValueLength() != BYTES || cell.getTimestamp() != TIMESTAMP) {
      throw new IllegalArgumentException("invalid GC row-fence cell");
    }
    var input = ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    return Optional.of(new SystemRecordGcFence(input.getLong(), input.getLong()));
  }

  @Override public int compareTo(SystemRecordGcFence other) {
    int owner = Long.compare(ownerEpoch, other.ownerEpoch);
    return owner == 0 ? Long.compare(compactGeneration, other.compactGeneration) : owner;
  }
}
