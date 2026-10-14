package io.etcdhbase.spi.salt;

/** Immutable parameters that define a persistent salt prefix format. */
public record SaltProfile(String strategyId, int encodingVersion, int bucketCount, int saltWidthBytes) {
  public SaltProfile {
    if (strategyId == null || strategyId.isBlank()) {
      throw new IllegalArgumentException("strategyId must not be blank");
    }
    if (encodingVersion < 1) {
      throw new IllegalArgumentException("encodingVersion must be positive");
    }
    if (bucketCount < 1) {
      throw new IllegalArgumentException("bucketCount must be positive");
    }
    if (saltWidthBytes < 1 || saltWidthBytes > Integer.BYTES) {
      throw new IllegalArgumentException("saltWidthBytes must be between 1 and 4");
    }
    long bucketCapacity = 1L << (Byte.SIZE * saltWidthBytes);
    if (bucketCount > bucketCapacity) {
      throw new IllegalArgumentException("salt width cannot encode every bucket");
    }
  }

  /** Encodes a bucket number as an unsigned fixed-width big-endian prefix. */
  public byte[] encodeBucket(int bucketNumber) {
    if (bucketNumber < 0 || bucketNumber >= bucketCount) {
      throw new IllegalArgumentException("bucketNumber must be within the configured bucket range");
    }
    byte[] prefix = new byte[saltWidthBytes];
    int value = bucketNumber;
    for (int index = prefix.length - 1; index >= 0; index--) {
      prefix[index] = (byte) value;
      value >>>= Byte.SIZE;
    }
    return prefix;
  }
}
