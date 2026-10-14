package io.etcdhbase.spi.identity;

import java.util.Objects;

/** Scope in which a backend identity is authenticated and authorized. */
public record BackendIdentityRequest(String tableGroupId, String connectionProfileId) {
  public BackendIdentityRequest {
    tableGroupId = requireText(tableGroupId, "tableGroupId");
    connectionProfileId = requireText(connectionProfileId, "connectionProfileId");
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
