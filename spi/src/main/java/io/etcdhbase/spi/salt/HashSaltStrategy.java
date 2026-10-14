package io.etcdhbase.spi.salt;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32;

/** Public deterministic CRC32 candidate for complete logical record identifiers. */
public final class HashSaltStrategy implements SaltStrategy {
  public static final String STRATEGY_ID = "crc32-bucket-v1";
  public static final SaltProfile PUBLIC_PROFILE = new SaltProfile(STRATEGY_ID, 1, 16, 2);

  private final SaltProfile profile;

  public HashSaltStrategy() {
    this(PUBLIC_PROFILE);
  }

  public HashSaltStrategy(SaltProfile profile) {
    this.profile = Objects.requireNonNull(profile, "profile");
    if (!STRATEGY_ID.equals(profile.strategyId()) || profile.encodingVersion() != 1) {
      throw new IllegalArgumentException("HashSaltStrategy requires crc32-bucket-v1 profile version 1");
    }
  }

  @Override
  public String id() {
    return STRATEGY_ID;
  }

  @Override
  public SaltProfile profile() {
    return profile;
  }

  @Override
  public byte[] salt(byte[] logicalIdentifier) {
    Objects.requireNonNull(logicalIdentifier, "logicalIdentifier");
    CRC32 crc32 = new CRC32();
    crc32.update(logicalIdentifier);
    int bucketNumber = (int) (crc32.getValue() % profile.bucketCount());
    return profile.encodeBucket(bucketNumber);
  }

  @Override
  public List<byte[]> splitKeys() {
    List<byte[]> keys = new ArrayList<>(profile.bucketCount() - 1);
    for (int bucketNumber = 1; bucketNumber < profile.bucketCount(); bucketNumber++) {
      keys.add(profile.encodeBucket(bucketNumber));
    }
    return List.copyOf(keys);
  }
}
