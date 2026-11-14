package io.etcdhbase.core.lease;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.error.BackendFailure;
import org.junit.jupiter.api.Test;

class LeaseCostAdmissionTest {
  private static final LeaseCostAdmission.Limits LIMITS =
      new LeaseCostAdmission.Limits(2, 4, 33_000);

  @Test
  void acceptsAccountingExactlyAtEachConfiguredBoundary() throws Exception {
    var admission = new LeaseCostAdmission(LIMITS);

    // Two active keys reserve 32,800 bytes for their future unbound versions.
    assertDoesNotThrow(() -> admission.requireWithin(2, 4, 33_000, 200));
  }

  @Test
  void rejectsCountKeyBytesAndRevokeCostOverTheConfiguredBoundary() {
    var admission = new LeaseCostAdmission(LIMITS);

    assertResourceLimit(() -> admission.requireWithin(3, 4, 33_000, 0));
    assertResourceLimit(() -> admission.requireWithin(2, 5, 33_000, 0));
    assertResourceLimit(() -> admission.requireWithin(2, 4, 33_001, 0));
    assertResourceLimit(() -> admission.requireWithin(2, 4, 33_000, 201));
  }

  @Test
  void releasesTheBindingReserveIntoRetainedHistoryWhenRevoked() {
    var admission = new LeaseCostAdmission(LIMITS);

    assertDoesNotThrow(() -> admission.requireWithin(0, 0, 0, 33_000));
    assertResourceLimit(() -> admission.requireWithin(0, 0, 0, 33_001));
  }

  @Test
  void rejectsAccountingUnderflowAsCorruptState() {
    var admission = new LeaseCostAdmission(LIMITS);

    assertThrows(IllegalStateException.class, () -> admission.requireWithin(-1, 0, 0));
    assertThrows(IllegalStateException.class, () -> admission.requireWithin(0, -1, 0));
    assertThrows(IllegalStateException.class, () -> admission.requireWithin(0, 0, -1));
    assertThrows(IllegalStateException.class, () -> admission.requireWithin(0, 0, 0, -1));
  }

  @Test
  void rejectsKeyAndValueSizeArithmeticThatCannotBeRepresented() {
    var admission = new LeaseCostAdmission(LIMITS);
    ByteKey key = ByteKey.copyOf(new byte[] {'k'});

    assertThrows(IllegalArgumentException.class,
        () -> admission.revokeCostBytes(key, Long.MAX_VALUE));
    assertThrows(IllegalArgumentException.class,
        () -> admission.revokeCostBytes(key, -1));
  }

  @Test
  void maximumAdmittedValueFitsTheAtomicRevocationPointReadBound() {
    var admission = new LeaseCostAdmission(LeaseCostAdmission.DEFAULT_LIMITS);
    ByteKey key = ByteKey.copyOf(new byte[] {'k'});
    long maximumValueBytes = (LeaseCostAdmission.MAXIMUM_SUPPORTED_REVOKE_COST_BYTES
        - (16L << 10) - (8L * key.length())) / 4;
    long currentBindingHistory = admission.bindingHistoryRecordBytes(key);
    long retainedPointBytes = 256L + key.length() + maximumValueBytes;

    assertTrue(retainedPointBytes <= LeaseCostAdmission.MAXIMUM_REVOCATION_POINT_BYTES);
    assertDoesNotThrow(() -> admission.requireWithin(1, key.length(),
        admission.revokeCostBytes(key, maximumValueBytes), currentBindingHistory));
    assertResourceLimit(() -> admission.requireWithin(1, key.length(),
        admission.revokeCostBytes(key, maximumValueBytes + 1), currentBindingHistory));
  }

  private static void assertResourceLimit(CheckedAction action) {
    BackendFailure failure = assertThrows(BackendFailure.class, action::run);
    assertEquals(BackendFailure.Category.RESOURCE_LIMIT, failure.category());
  }

  @FunctionalInterface
  private interface CheckedAction {
    void run() throws Exception;
  }
}
