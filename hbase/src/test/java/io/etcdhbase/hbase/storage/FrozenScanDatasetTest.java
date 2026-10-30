package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.KeyRange;
import org.junit.jupiter.api.Test;

class FrozenScanDatasetTest {
  @Test
  void seedMutationOrderAndOracleAreFrozenIndependentlyOfTheStorageAdapter() {
    var dataset = FrozenScanDataset.create();
    assertEquals(78, dataset.keys().size());
    assertEquals(118, dataset.mutations().size());
    assertEquals(78, dataset.liveView(KeyRange.all(), 78).size());
    assertEquals(66, dataset.liveView(KeyRange.all(), Long.MAX_VALUE).size());
    assertEquals("80e44c6c5c8acadad794501fa41b4bb3de0679454bfbd91b001fb3975e1dab96", dataset.digest());
    assertEquals(dataset.digest(), FrozenScanDataset.create().digest());
    assertTrue(dataset.keys().stream().anyMatch(key -> key.length() == 2048));
  }
}
