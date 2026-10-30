package io.etcdhbase.core.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.storage.StoragePage;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StorageCostTest {
  @Test
  void costAddsReturnedDataAndAvailableObservationsWithoutInventingMissingCounters() {
    var first = new StoragePage.Cost(1, 10, 2, Map.of(StoragePage.Observation.SERVER_ROWS_SCANNED, 7L));
    var result = first.plus(new StoragePage.Cost(2, 20, 3));
    assertEquals(3, result.physicalRows());
    assertEquals(30, result.returnedCellBytes());
    assertEquals(Map.of(StoragePage.Observation.SERVER_ROWS_SCANNED, 7L), result.observations());
    assertThrows(ArithmeticException.class, () -> result.plus(new StoragePage.Cost(Long.MAX_VALUE, 0, 0)));
    assertThrows(IllegalArgumentException.class, () -> new StoragePage.Cost(0, 0, 0,
        Map.of(StoragePage.Observation.FS_BYTES, -1L)));
  }
}
