package io.etcdhbase.hbase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.LeaseTimeObservation;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class InvocationWindowEvidenceTest {
  @Test
  void roundTripsOnlyBoundedCompleteEvidenceAndRejectsMissingOrCorruptTime() {
    var evidence = new InvocationWindowEvidence(1_800_000, 5_400_000);
    assertEquals(evidence, InvocationWindowEvidence.decode(evidence.encode()));
    assertThrows(IllegalArgumentException.class, () -> InvocationWindowEvidence.decode(
        ByteKey.copyOf(new byte[0])));
    assertThrows(IllegalArgumentException.class, () -> InvocationWindowEvidence.decode(
        ByteKey.copyOf(ByteBuffer.allocate(20).putInt(2).putLong(1).putLong(2).array())));
    assertThrows(IllegalArgumentException.class, () -> new InvocationWindowEvidence(0, 100));
    assertThrows(IllegalArgumentException.class, () -> new InvocationWindowEvidence(100, 99));
  }

  @Test
  void absentTrustedClockIsFailClosedAndRetentionIsExplicitlyBounded() {
    assertEquals(Duration.ofMinutes(30), ResultWindowRetention.DEFAULT_MINIMUM);
    assertEquals(false, ResultWindowRetention.unconfigured().time().observe().healthy());
    var time = new LeaseTimeObservation(1, 1, true);
    assertThrows(IllegalArgumentException.class, () -> new ResultWindowRetention(Duration.ZERO, () -> time));
    assertThrows(IllegalArgumentException.class, () -> new ResultWindowRetention(Duration.ofDays(31), () -> time));
    assertThrows(IllegalArgumentException.class, () -> new ResultWindowRetention(Duration.ofNanos(1), () -> time));
  }
}
