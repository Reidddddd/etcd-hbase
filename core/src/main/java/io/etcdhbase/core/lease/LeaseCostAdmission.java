package io.etcdhbase.core.lease;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.bytes.OrderedByteKeyEncoding;
import io.etcdhbase.core.commit.PreparedAuxiliaryRecord;
import io.etcdhbase.core.error.BackendFailure;
import java.util.Objects;

/** Bounds accepted Lease bindings by the resources needed for one atomic revoke decision. */
public final class LeaseCostAdmission {
  public static final int MAXIMUM_SUPPORTED_BINDINGS = 1_024;
  public static final long MAXIMUM_SUPPORTED_KEY_BYTES = 4L << 20;
  public static final long MAXIMUM_SUPPORTED_REVOKE_COST_BYTES = 32L << 20;
  /** A single admitted value stays below this bound because revoke accounting charges 4x bytes. */
  public static final long MAXIMUM_REVOCATION_POINT_BYTES = 10L << 20;
  public static final int MAXIMUM_BINDING_HISTORY_RECORDS =
      Math.toIntExact(MAXIMUM_SUPPORTED_REVOKE_COST_BYTES / (16L << 10));
  public static final Limits DEFAULT_LIMITS = new Limits(MAXIMUM_SUPPORTED_BINDINGS,
      MAXIMUM_SUPPORTED_KEY_BYTES, MAXIMUM_SUPPORTED_REVOKE_COST_BYTES);

  private static final long PER_BINDING_OVERHEAD_BYTES = 16L << 10;
  private static final long KEY_COPY_FACTOR = 8;
  private static final long VALUE_COPY_FACTOR = 4;
  private static final int MAX_BINDING_PAYLOAD_KEY_BYTES = 30 * 1024;

  public record Limits(int maximumBindings, long maximumKeyBytes, long maximumRevokeCostBytes) {
    public Limits {
      if (maximumBindings < 1 || maximumBindings > MAXIMUM_SUPPORTED_BINDINGS
          || maximumKeyBytes < 1 || maximumKeyBytes > MAXIMUM_SUPPORTED_KEY_BYTES
          || maximumRevokeCostBytes < 1
          || maximumRevokeCostBytes > MAXIMUM_SUPPORTED_REVOKE_COST_BYTES) {
        throw new IllegalArgumentException("invalid Lease revocation capacity limits");
      }
    }
  }

  private final Limits limits;

  public LeaseCostAdmission(Limits limits) {
    this.limits = Objects.requireNonNull(limits, "limits");
  }

  /** Returns a conservative bound for the key, retained value, event, and auxiliary pages. */
  public void requireBindingSupported(ByteKey key) throws BackendFailure {
    Objects.requireNonNull(key, "key");
    if (key.length() == 0) {
      throw new IllegalArgumentException("Lease binding key must not be empty");
    }
    long identityBytes = Math.addExact(Long.BYTES * 3L,
        OrderedByteKeyEncoding.encodedLength(key.bytes()));
    if (identityBytes > PreparedAuxiliaryRecord.MAX_TARGET_IDENTITY_BYTES
        || key.length() > MAX_BINDING_PAYLOAD_KEY_BYTES) {
      throw resourceLimit();
    }
  }

  public long revokeCostBytes(ByteKey key, long valueBytes) {
    Objects.requireNonNull(key, "key");
    if (key.length() == 0 || valueBytes < 0) {
      throw new IllegalArgumentException("invalid Lease binding size");
    }
    try {
      return Math.addExact(PER_BINDING_OVERHEAD_BYTES,
          Math.addExact(Math.multiplyExact(KEY_COPY_FACTOR, key.length()),
              Math.multiplyExact(VALUE_COPY_FACTOR, valueBytes)));
    } catch (ArithmeticException overflow) {
      throw new IllegalArgumentException("Lease revoke cost overflows its representation", overflow);
    }
  }

  /** Conservative durable and read cost for one immutable bound/unbound history version. */
  public long bindingHistoryRecordBytes(ByteKey key) {
    Objects.requireNonNull(key, "key");
    if (key.length() == 0) {
      throw new IllegalArgumentException("Lease binding key must not be empty");
    }
    try {
      return Math.addExact(PER_BINDING_OVERHEAD_BYTES,
          Math.multiplyExact(KEY_COPY_FACTOR, key.length()));
    } catch (ArithmeticException overflow) {
      throw new IllegalArgumentException("Lease binding history cost overflows its representation", overflow);
    }
  }

  public void requireWithin(long bindingCount, long keyBytes, long revokeCostBytes)
      throws BackendFailure {
    requireWithin(bindingCount, keyBytes, revokeCostBytes, 0);
  }

  public void requireWithin(long bindingCount, long keyBytes, long revokeCostBytes,
      long bindingHistoryBytes) throws BackendFailure {
    if (bindingCount < 0 || keyBytes < 0 || revokeCostBytes < 0 || bindingHistoryBytes < 0) {
      throw new IllegalStateException("Lease binding accounting underflow");
    }
    if (bindingCount > limits.maximumBindings() || keyBytes > limits.maximumKeyBytes()
        || revokeCostBytes > limits.maximumRevokeCostBytes()) {
      throw resourceLimit();
    }

    // Reserve the future unbound version for every live key. Otherwise a Lease could be
    // accepted at the history limit and later become impossible to revoke atomically.
    long reservedUnbindHistoryBytes = Math.addExact(
        Math.multiplyExact(bindingCount, PER_BINDING_OVERHEAD_BYTES),
        Math.multiplyExact(KEY_COPY_FACTOR, keyBytes));
    if (reservedUnbindHistoryBytes > limits.maximumRevokeCostBytes()
        || bindingHistoryBytes > limits.maximumRevokeCostBytes() - reservedUnbindHistoryBytes) {
      throw resourceLimit();
    }
  }

  /** Fails startup when a stricter activation would strand an already accepted Lease. */
  public void requireExistingStateFits(LeaseState state) throws BackendFailure {
    Objects.requireNonNull(state, "state");
    requireWithin(state.boundKeyCount(), state.boundKeyBytes(), state.revokeCostBytes(),
        state.bindingHistoryBytes());
  }

  public Limits limits() { return limits; }

  private static BackendFailure resourceLimit() {
    return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
  }

}
