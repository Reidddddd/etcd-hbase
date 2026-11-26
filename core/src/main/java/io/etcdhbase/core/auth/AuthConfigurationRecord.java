package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/** Read-only status for one table group at the deployment's configuration generation. */
public record AuthConfigurationRecord(String trustDomain, String tableGroupId,
    ByteKey configurationDigest, boolean enabled, long revision) {
  public AuthConfigurationRecord {
    if (trustDomain == null || !trustDomain.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")
        || tableGroupId == null || !tableGroupId.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException("invalid auth configuration binding");
    }
    Objects.requireNonNull(configurationDigest, "configurationDigest");
    if (configurationDigest.length() != 32 || revision < 1) {
      throw new IllegalArgumentException("invalid auth configuration revision or digest");
    }
  }
}
