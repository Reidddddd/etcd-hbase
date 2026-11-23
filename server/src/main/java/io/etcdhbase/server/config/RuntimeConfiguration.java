package io.etcdhbase.server.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.etcdhbase.admin.config.InitializationConfiguration;
import io.etcdhbase.admin.config.InitializationConfiguration.HBaseProfile;
import io.etcdhbase.admin.config.InitializationConfiguration.SaltProfile;
import io.etcdhbase.admin.config.InitializationConfiguration.TableGroup;
import java.nio.file.Path;
import java.nio.file.InvalidPathException;
import java.util.List;
import java.util.Objects;

/** Service configuration reuses the exact initialization models and validation rules. */
public record RuntimeConfiguration(
    int schemaVersion,
    List<Listener> listeners,
    List<HBaseProfile> hbaseProfiles,
    List<SaltProfile> saltProfiles,
    List<TableGroup> tableGroups,
    ResourceBudgets resources,
    @JsonProperty(required = false) M3AuthenticationConfiguration authentication) {
  public RuntimeConfiguration {
    listeners = List.copyOf(listeners);
    hbaseProfiles = List.copyOf(hbaseProfiles);
    saltProfiles = List.copyOf(saltProfiles);
    tableGroups = List.copyOf(tableGroups);
  }

  /** Source-compatible constructor for pre-M5 development configurations. */
  public RuntimeConfiguration(int schemaVersion, List<Listener> listeners,
      List<HBaseProfile> hbaseProfiles, List<SaltProfile> saltProfiles,
      List<TableGroup> tableGroups, ResourceBudgets resources) {
    this(schemaVersion, listeners, hbaseProfiles, saltProfiles, tableGroups, resources, null);
  }

  public InitializationConfiguration initialization() {
    return new InitializationConfiguration(schemaVersion, hbaseProfiles, saltProfiles, tableGroups);
  }

  public record Listener(String id, String bindHost, int port, String tableGroupId,
      @JsonProperty(required = false) Tls tls) {
    public Listener(String id, String bindHost, int port, String tableGroupId) {
      this(id, bindHost, port, tableGroupId, null);
    }
  }

  /** Server-side TLS material is mounted from a trusted secret and never embedded in JSON. */
  public record Tls(String certificateFile, String privateKeyFile) {
    public Tls {
      certificateFile = requireAbsolutePath(certificateFile, "TLS certificate");
      privateKeyFile = requireAbsolutePath(privateKeyFile, "TLS private key");
      if (certificateFile.equals(privateKeyFile)) {
        throw new IllegalArgumentException("TLS certificate and private key references must differ");
      }
    }

    private static String requireAbsolutePath(String value, String name) {
      Objects.requireNonNull(value, name);
      if (value.length() > 4096 || value.codePoints().anyMatch(Character::isISOControl)) {
        throw new IllegalArgumentException(name + " path is invalid");
      }
      try {
        Path path = Path.of(value);
        if (!path.isAbsolute()) {
          throw new IllegalArgumentException(name + " must be an absolute mounted path");
        }
        return path.normalize().toString();
      } catch (InvalidPathException | SecurityException invalidPath) {
        throw new IllegalArgumentException(name + " path is invalid");
      }
    }
  }

  /** Queueing is disabled until a cancellable queue exists; scanner limits belong to adapters. */
  public record ResourceBudgets(
      int maxConfigBytes,
      int maxConcurrentRequests,
      int requestDeadlineMs,
      int maxRpcBytes,
      int maxRequestBytes,
      int maxScannersPerTable,
      int maxQueuedRequests) {}
}
