package io.etcdhbase.server.config;

import io.etcdhbase.server.config.RuntimeConfiguration.HBaseProfile;

/** Canonical HBase target and namespace identity shared by validation and administration. */
public record HBaseBackendScope(String canonicalQuorum, int clientPort, String namespace) {
  /**
   * Returns the locally normalized scope for a profile. Host literals are canonicalized without
   * DNS resolution, using the same rules as runtime configuration validation.
   */
  public static HBaseBackendScope of(HBaseProfile profile) {
    return RuntimeConfigurationValidator.backendScope(profile);
  }
}
