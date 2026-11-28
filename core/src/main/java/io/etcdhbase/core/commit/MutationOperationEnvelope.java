package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.lease.LeaseExpiryStepCommand;
import java.util.Arrays;
import java.util.Objects;

/** Distinguishes tagged operations from existing raw protobuf Put payloads. */
public final class MutationOperationEnvelope {
  // A zero protobuf tag cannot start a valid raw PutRequest, so old payloads cannot collide.
  private static final byte[] TXN_V1 = {0, 'T', 'X', 1};
  private static final byte[] DELETE_V1 = {0, 'D', 'L', 1};
  private static final byte[] LEASE_GRANT_V1 = {0, 'L', 'G', 1};
  private static final byte[] LEASE_KEEP_ALIVE_V1 = {0, 'L', 'K', 1};
  private static final byte[] LEASE_REVOKE_V1 = {0, 'L', 'R', 1};
  private static final byte[] LEASE_EXPIRY_STEP_V1 = {0, 'L', 'X', 1};

  private static final byte[] COMPACT_V1 = {0, 'C', 'P', 1};

  public enum Kind {
    PUT, TRANSACTION, DELETE_RANGE, LEASE_GRANT, LEASE_KEEP_ALIVE,
    LEASE_REVOKE, LEASE_EXPIRY_STEP, COMPACT
  }

  private MutationOperationEnvelope() {}

  public static ByteKey transaction(byte[] payload) {
    return tagged(TXN_V1, payload);
  }

  public static ByteKey deleteRange(byte[] payload) {
    return tagged(DELETE_V1, payload);
  }

  public static ByteKey leaseGrant(byte[] payload) {
    return tagged(LEASE_GRANT_V1, payload);
  }

  public static ByteKey leaseKeepAlive(byte[] payload) {
    return tagged(LEASE_KEEP_ALIVE_V1, payload);
  }

  public static ByteKey leaseRevoke(byte[] payload) {
    return tagged(LEASE_REVOKE_V1, payload);
  }

  /** Internal bounded command carrying an expected checkpoint and one scan result. */
  public static ByteKey leaseExpiryStep(byte[] payload) {
    return tagged(LEASE_EXPIRY_STEP_V1, payload);
  }

  /** A fixed revision/physical command; Compact has its own durable operation record. */
  public static ByteKey compact(byte[] payload) {
    requireCompactPayload(payload);
    return tagged(COMPACT_V1, payload);
  }

  private static void requireCompactPayload(byte[] payload) {
    if (payload.length != Long.BYTES + 1 || payload[Long.BYTES] < 0 || payload[Long.BYTES] > 1) {
      throw new IllegalArgumentException("invalid bounded Compact command");
    }
  }

  public static byte[] compactBytes(ByteKey operation) {
    if (kind(operation) != Kind.COMPACT) throw new IllegalArgumentException("operation is not Compact");
    return payload(operation, COMPACT_V1);
  }

  private static ByteKey tagged(byte[] prefix, byte[] payload) {
    Objects.requireNonNull(payload, "payload");
    if (payload.length > MutationForwardRequest.MAX_OPERATION_BYTES - prefix.length) {
      throw new IllegalArgumentException("mutation operation exceeds its byte bound");
    }
    byte[] encoded = Arrays.copyOf(prefix, prefix.length + payload.length);
    System.arraycopy(payload, 0, encoded, prefix.length, payload.length);
    return ByteKey.copyOf(encoded);
  }

  public static boolean isTransaction(ByteKey operation) {
    return kind(operation) == Kind.TRANSACTION;
  }

  public static Kind kind(ByteKey operation) {
    Objects.requireNonNull(operation, "operation");
    byte[] bytes = operation.bytes();
    if (bytes.length == 0) throw new IllegalArgumentException("empty mutation operation");
    if (bytes[0] != 0) return Kind.PUT;
    if (bytes.length < TXN_V1.length || bytes.length > MutationForwardRequest.MAX_OPERATION_BYTES) {
      throw new IllegalArgumentException("unsupported mutation operation envelope");
    }
    byte[] prefix = Arrays.copyOf(bytes, TXN_V1.length);
    if (Arrays.equals(TXN_V1, prefix)) return Kind.TRANSACTION;
    if (Arrays.equals(DELETE_V1, prefix)) return Kind.DELETE_RANGE;
    if (Arrays.equals(LEASE_GRANT_V1, prefix)) return Kind.LEASE_GRANT;
    if (Arrays.equals(LEASE_KEEP_ALIVE_V1, prefix)) return Kind.LEASE_KEEP_ALIVE;
    if (Arrays.equals(LEASE_REVOKE_V1, prefix)) return Kind.LEASE_REVOKE;
    if (Arrays.equals(COMPACT_V1, prefix)) {
      requireCompactPayload(Arrays.copyOfRange(bytes, COMPACT_V1.length, bytes.length));
      return Kind.COMPACT;
    }
    if (Arrays.equals(LEASE_EXPIRY_STEP_V1, prefix)) {
      if (bytes.length <= LEASE_EXPIRY_STEP_V1.length) {
        throw new IllegalArgumentException("empty Lease expiry step envelope");
      }
      LeaseExpiryStepCommand.decode(ByteKey.copyOf(
          Arrays.copyOfRange(bytes, LEASE_EXPIRY_STEP_V1.length, bytes.length)));
      return Kind.LEASE_EXPIRY_STEP;
    }
    throw new IllegalArgumentException("unsupported mutation operation envelope");
  }

  public static byte[] transactionBytes(ByteKey operation) {
    if (!isTransaction(operation)) throw new IllegalArgumentException("operation is not a transaction");
    byte[] bytes = operation.bytes();
    return Arrays.copyOfRange(bytes, TXN_V1.length, bytes.length);
  }

  public static byte[] deleteRangeBytes(ByteKey operation) {
    if (kind(operation) != Kind.DELETE_RANGE) throw new IllegalArgumentException("operation is not a delete");
    byte[] bytes = operation.bytes();
    return Arrays.copyOfRange(bytes, DELETE_V1.length, bytes.length);
  }

  public static byte[] leaseGrantBytes(ByteKey operation) {
    if (kind(operation) != Kind.LEASE_GRANT) throw new IllegalArgumentException("operation is not a lease grant");
    byte[] bytes = operation.bytes();
    return Arrays.copyOfRange(bytes, LEASE_GRANT_V1.length, bytes.length);
  }

  public static byte[] leaseKeepAliveBytes(ByteKey operation) {
    if (kind(operation) != Kind.LEASE_KEEP_ALIVE) {
      throw new IllegalArgumentException("operation is not a Lease KeepAlive");
    }
    byte[] bytes = operation.bytes();
    return Arrays.copyOfRange(bytes, LEASE_KEEP_ALIVE_V1.length, bytes.length);
  }

  public static byte[] leaseRevokeBytes(ByteKey operation) {
    if (kind(operation) != Kind.LEASE_REVOKE) {
      throw new IllegalArgumentException("operation is not a Lease Revoke");
    }
    return payload(operation, LEASE_REVOKE_V1);
  }

  public static byte[] leaseExpiryStepBytes(ByteKey operation) {
    if (kind(operation) != Kind.LEASE_EXPIRY_STEP) {
      throw new IllegalArgumentException("operation is not a Lease expiry step");
    }
    return payload(operation, LEASE_EXPIRY_STEP_V1);
  }

  private static byte[] payload(ByteKey operation, byte[] tag) {
    byte[] bytes = operation.bytes();
    return Arrays.copyOfRange(bytes, tag.length, bytes.length);
  }

  private static boolean startsWith(byte[] bytes, byte[] prefix) {
    if (bytes.length < prefix.length) return false;
    for (int index = 0; index < prefix.length; index++) {
      if (bytes[index] != prefix[index]) return false;
    }
    return true;
  }
}
