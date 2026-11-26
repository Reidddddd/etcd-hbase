package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class HBaseSystemRecordRetirementApiTest {
  @Test
  void publicStoreApiCannotBypassTheClosureDecision() {
    assertTrue(Arrays.stream(HBaseSystemRecordStore.class.getMethods())
        .noneMatch(method -> method.getName().equals("retire")),
        "ordinary callers must use the GC closure rather than a raw retirement mutation");
  }
}
