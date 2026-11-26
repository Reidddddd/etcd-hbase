package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.*;

import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.RetirementTombstone;
import io.etcdhbase.core.commit.SystemRecordRetirement;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.hbase.codec.SystemRecordRetirementMessages;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.junit.jupiter.api.Test;

class SystemRecordGcFenceTest {
  private static final byte[] FAMILY = {'s'};
  private static final byte[] ROW = {1};

  @Test
  void newerOwnerDominatesEveryOlderCompactGeneration() {
    assertTrue(new SystemRecordGcFence(2, 1).compareTo(new SystemRecordGcFence(1, 99)) > 0);
    assertTrue(new SystemRecordGcFence(2, 2).compareTo(new SystemRecordGcFence(2, 1)) > 0);
    assertEquals(0, new SystemRecordGcFence(2, 2).compareTo(new SystemRecordGcFence(2, 2)));
    assertThrows(IllegalArgumentException.class, () -> new SystemRecordGcFence(0, 1));
    assertThrows(IllegalArgumentException.class, () -> new SystemRecordGcFence(1, 0));
  }

  @Test
  void livePayloadKeepsItsValueAndRejectsCorruptOrDuplicateFenceCells() {
    var value = cell(new byte[] {'r'}, 10, new byte[] {7});
    var fence = new SystemRecordGcFence(2, 3);
    var valid = result(value, cell(SystemRecordGcFence.COLUMN, SystemRecordGcFence.TIMESTAMP, fence.encode()));
    assertEquals(fence, SystemRecordGcFence.read(valid, FAMILY).orElseThrow());
    assertArrayEquals(new byte[] {7}, HBaseSystemRecordStore.decode(valid, FAMILY).orElseThrow().bytes());
    for (byte[] invalid : List.of(new byte[15], new byte[17], new byte[16],
        ByteBuffer.allocate(16).putLong(1).putLong(-1).array())) {
      assertThrows(IllegalArgumentException.class, () -> HBaseSystemRecordStore.decode(result(value,
          cell(SystemRecordGcFence.COLUMN, SystemRecordGcFence.TIMESTAMP, invalid)), FAMILY));
    }
    assertThrows(IllegalArgumentException.class, () -> HBaseSystemRecordStore.decode(result(value,
        cell(SystemRecordGcFence.COLUMN, 10, fence.encode())), FAMILY));
    assertThrows(IllegalArgumentException.class, () -> HBaseSystemRecordStore.decode(result(value,
        cell(SystemRecordGcFence.COLUMN, SystemRecordGcFence.TIMESTAMP, fence.encode()),
        cell(SystemRecordGcFence.COLUMN, SystemRecordGcFence.TIMESTAMP - 1, fence.encode())), FAMILY));
  }

  @Test
  void retiredLayoutBindsTheFenceToTheRecordedCompactGeneration() {
    var tombstone = new RetirementTombstone(SystemRecordType.MANIFEST, ByteKey.copyOf(ROW),
        ByteKey.copyOf(new byte[32]), 3, InvocationId.create(),
        SystemRecordRetirement.Reason.UNREFERENCED_CLOSURE, 8, 9);
    var guard = cell(new byte[] {'r'}, Long.MAX_VALUE - 1, new byte[] {0});
    var marker = cell(new byte[] {'t'}, Long.MAX_VALUE - 1, SystemRecordRetirementMessages.encode(tombstone));
    var matching = cell(SystemRecordGcFence.COLUMN, SystemRecordGcFence.TIMESTAMP, new SystemRecordGcFence(2, 3).encode());
    assertTrue(HBaseSystemRecordStore.decode(result(guard, marker, matching), FAMILY).isEmpty());
    assertThrows(IllegalArgumentException.class, () -> HBaseSystemRecordStore.decode(result(guard, marker,
        cell(SystemRecordGcFence.COLUMN, SystemRecordGcFence.TIMESTAMP, new SystemRecordGcFence(2, 4).encode())), FAMILY));
    assertThrows(IllegalArgumentException.class, () -> HBaseSystemRecordStore.decode(result(matching), FAMILY));
    assertThrows(IllegalArgumentException.class, () -> HBaseSystemRecordStore.decode(result(guard, marker, matching,
        cell(new byte[] {'x'}, 1, new byte[] {0})), FAMILY));
  }

  private static KeyValue cell(byte[] qualifier, long timestamp, byte[] value) {
    return new KeyValue(ROW, FAMILY, qualifier, timestamp, value);
  }
  private static Result result(Cell... cells) {
    var ordered = new ArrayList<Cell>(List.of(cells));
    ordered.sort(CellComparator.getInstance());
    return Result.create(ordered);
  }
}
