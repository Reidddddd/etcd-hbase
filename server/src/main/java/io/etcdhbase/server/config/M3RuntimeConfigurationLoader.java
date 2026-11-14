package io.etcdhbase.server.config;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.etcdhbase.admin.config.StrictConfigurationJson;
import io.etcdhbase.core.lease.LeaseCostAdmission;
import java.io.InputStream;

/** Loads the explicit M3 activation file without resolving credentials or contacting HBase. */
public final class M3RuntimeConfigurationLoader {
  private final StrictConfigurationJson json = new StrictConfigurationJson();

  public M3RuntimeConfiguration load(InputStream input) {
    var tree = json.read(input);
    if (tree instanceof ObjectNode object) {
      if (!object.has("read_cache_enabled")) object.put("read_cache_enabled", false);
      else if (object.path("read_cache_enabled").isBoolean() && object.path("read_cache_enabled").booleanValue()) {
        throw new M3RuntimeConfiguration.CacheActivationUnavailable();
      }
      if (!object.has("m4_lease_api_enabled")) object.put("m4_lease_api_enabled", false);
      else if (!object.path("m4_lease_api_enabled").isBoolean()) {
        throw new IllegalArgumentException("m4_lease_api_enabled must be a boolean");
      }
      boolean m4LeaseApiEnabled = object.path("m4_lease_api_enabled").booleanValue();
      if (!object.has("lease_min_ttl_seconds")) {
        if (m4LeaseApiEnabled) {
          throw new IllegalArgumentException("M4 Lease activation requires lease_min_ttl_seconds");
        }
        object.put("lease_min_ttl_seconds", 1);
      }
      if (!object.has("lease_clock_uncertainty_ms")) {
        if (m4LeaseApiEnabled) {
          throw new IllegalArgumentException("M4 Lease activation requires lease_clock_uncertainty_ms");
        }
        object.put("lease_clock_uncertainty_ms", 1_000);
      }
      if (!object.has("lease_maximum_bindings")) {
        if (m4LeaseApiEnabled) {
          throw new IllegalArgumentException("M4 Lease activation requires lease_maximum_bindings");
        }
        object.put("lease_maximum_bindings", LeaseCostAdmission.DEFAULT_LIMITS.maximumBindings());
      }
      if (!object.has("lease_maximum_key_bytes")) {
        if (m4LeaseApiEnabled) {
          throw new IllegalArgumentException("M4 Lease activation requires lease_maximum_key_bytes");
        }
        object.put("lease_maximum_key_bytes", LeaseCostAdmission.DEFAULT_LIMITS.maximumKeyBytes());
      }
      if (!object.has("lease_maximum_revoke_cost_bytes")) {
        if (m4LeaseApiEnabled) {
          throw new IllegalArgumentException("M4 Lease activation requires lease_maximum_revoke_cost_bytes");
        }
        object.put("lease_maximum_revoke_cost_bytes",
            LeaseCostAdmission.DEFAULT_LIMITS.maximumRevokeCostBytes());
      }
    }
    return json.bind(tree, M3RuntimeConfiguration.class);
  }
}
