package io.etcdhbase.spi.identity;

import java.util.Objects;
import java.util.regex.Pattern;

/** The configured table group and connection profile for one backend authentication. */
public record AuthenticationScope(String tableGroupId, String connectionProfileId) {
  private static final Pattern ID_PATTERN = Pattern.compile("[A-Za-z][A-Za-z0-9._-]{0,63}");

  public AuthenticationScope {
    tableGroupId = requireId(tableGroupId, "tableGroupId");
    connectionProfileId = requireId(connectionProfileId, "connectionProfileId");
  }

  private static String requireId(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!ID_PATTERN.matcher(value).matches()) {
      throw new IllegalArgumentException(name + " must be a valid configured identifier");
    }
    return value;
  }
}
