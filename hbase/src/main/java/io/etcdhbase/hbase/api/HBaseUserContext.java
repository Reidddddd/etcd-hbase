package io.etcdhbase.hbase.api;

import java.io.IOException;
import java.util.OptionalLong;
import org.apache.hadoop.hbase.security.User;

/**
 * Owns one isolated backend user and its credentials; close its connections before this context.
 */
public interface HBaseUserContext extends AutoCloseable {
  /** A borrowed user for the HBase adapter only. Never expose it through the common SPI. */
  User user();

  /**
   * Version attested for the authenticated credentials by the identity backend. A known version
   * must be comparable across replicas and increase on rotation within the same credential family.
   * Return empty when that contract cannot be established; an arbitrary local counter is unsafe.
   */
  default OptionalLong credentialGeneration() {
    return OptionalLong.empty();
  }

  @Override
  void close() throws IOException;
}
