package io.etcdhbase.spi.salt;

import java.util.List;

/**
 * Creates a fixed-width prefix for a complete logical record identifier.
 *
 * <p>Implementations must be deterministic for their immutable profile, must not modify the input,
 * and must return caller-owned byte arrays. A range scan must use the same profile and boundaries
 * as point reads; implementations must not add a cluster-routing identifier.
 */
public interface SaltStrategy {
  String id();

  SaltProfile profile();

  byte[] salt(byte[] logicalIdentifier);

  /** Returns the sorted region split boundaries as independent arrays. */
  List<byte[]> splitKeys();
}
