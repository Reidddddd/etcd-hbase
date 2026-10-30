package io.etcdhbase.core.storage;

import java.util.List;

/** Cost is returned Cell data and client scan RPC metrics, not HFile/disk IO or META RPCs. */
public record StoragePage(List<StoredRecord> records, boolean exhausted, Cost cost) {
  public StoragePage {
    records = List.copyOf(records);
    if (cost == null) throw new NullPointerException("cost");
  }

  public record Cost(long physicalRows, long returnedCellBytes, long scanRpcCalls) {
    public Cost {
      if (physicalRows < 0 || returnedCellBytes < 0 || scanRpcCalls < 0) {
        throw new IllegalArgumentException("negative storage cost");
      }
    }
  }
}
