package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Arrays;
import java.util.Objects;

/** Distinguishes tagged operations from existing raw protobuf Put payloads. */
public final class MutationOperationEnvelope {
  // A zero protobuf tag cannot start a valid raw PutRequest, so old payloads cannot collide.
  private static final byte[] TXN_V1 = {0, 'T', 'X', 1};
  private static final byte[] DELETE_V1 = {0, 'D', 'L', 1};
  private static final byte[] LEASE_GRANT_V1 = {0, 'L', 'G', 1};
  private static final byte[] LEASE_EXPIRE_EMPTY_V1 = {0, 'L', 'E', 1};

  public enum Kind { PUT, TRANSACTION, DELETE_RANGE, LEASE_GRANT, LEASE_EXPIRE_EMPTY }

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

  /** Encodes an internal empty-Lease expiry candidate identity, not an etcd wire RPC. */
  public static ByteKey leaseExpireEmpty(byte[] candidateIdentity) {
    if (candidateIdentity == null || candidateIdentity.length != Long.BYTES * 4) {
      throw new IllegalArgumentException("empty-Lease expiry identity must contain 32 bytes");
    }
    return tagged(LEASE_EXPIRE_EMPTY_V1, candidateIdentity);
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
    if (Arrays.equals(LEASE_EXPIRE_EMPTY_V1, prefix)) {
      if (bytes.length != LEASE_EXPIRE_EMPTY_V1.length + Long.BYTES * 4) {
        throw new IllegalArgumentException("invalid empty-Lease expiry envelope length");
      }
      return Kind.LEASE_EXPIRE_EMPTY;
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

  public static byte[] leaseExpireEmptyBytes(ByteKey operation) {
    if (kind(operation) != Kind.LEASE_EXPIRE_EMPTY) {
      throw new IllegalArgumentException("operation is not an empty-Lease expiry");
    }
    byte[] bytes = operation.bytes();
    return Arrays.copyOfRange(bytes, LEASE_EXPIRE_EMPTY_V1.length, bytes.length);
  }
}
