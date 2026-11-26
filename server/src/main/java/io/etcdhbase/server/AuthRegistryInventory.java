package io.etcdhbase.server;

import io.etcdhbase.admin.TableGroupPlanFactory;
import io.etcdhbase.admin.config.HBaseBackendScope;
import io.etcdhbase.admin.config.StrictConfigurationJson;
import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.server.auth.AuthConfigurationFingerprint;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfigurationLoader;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/** Builds a complete operator-supplied inventory using the same bindings as the runtime. */
final class AuthRegistryInventory {
  public record Input(List<GroupInput> groups) {
    public Input { groups = List.copyOf(Objects.requireNonNull(groups, "groups")); }
  }

  public record GroupInput(String configurationFile, String tableGroupId, String clusterId,
      boolean enabled) {}

  private AuthRegistryInventory() {}

  static List<AuthDeploymentRegistry.Group> load(InputStream input, RuntimeConfiguration control)
      throws IOException {
    var json = new StrictConfigurationJson();
    Input inventory = json.bind(json.read(input), Input.class);
    var groups = new java.util.ArrayList<AuthDeploymentRegistry.Group>();
    String trustDomain = Objects.requireNonNull(control.authentication(), "authentication").trustDomain();
    for (GroupInput item : inventory.groups()) {
      Path path = Path.of(item.configurationFile());
      if (!path.isAbsolute()) throw new IllegalArgumentException("inventory configuration path must be absolute");
      RuntimeConfiguration config;
      try (var stream = Files.newInputStream(path)) {
        config = new RuntimeConfigurationLoader().load(stream);
      }
      requireSameControl(control, config);
      long clusterId;
      try {
        clusterId = Long.parseUnsignedLong(item.clusterId());
      } catch (NumberFormatException invalid) {
        throw new IllegalArgumentException("inventory cluster ID must be an unsigned decimal value");
      }
      if (clusterId == 0) throw new IllegalArgumentException("inventory cluster ID must be nonzero");
      groups.add(group(config, item.tableGroupId(), clusterId, item.enabled()));
    }
    return AuthDeploymentRegistry.initial(trustDomain, groups).groups();
  }

  private static void requireSameControl(RuntimeConfiguration expected, RuntimeConfiguration actual) {
    var expectedAuth = expected.authentication();
    var actualAuth = actual.authentication();
    if (actualAuth == null || !expectedAuth.trustDomain().equals(actualAuth.trustDomain())
        || !expectedAuth.authControl().tableName().equals(actualAuth.authControl().tableName())
        || !controlScope(expected).equals(controlScope(actual))
        || !controlSalt(expected).equals(controlSalt(actual))) {
      throw new IllegalArgumentException("inventory auth-control binding does not match");
    }
  }

  private static HBaseBackendScope controlScope(RuntimeConfiguration config) {
    String id = config.authentication().authControl().hbaseProfileId();
    return HBaseBackendScope.of(config.hbaseProfiles().stream()
        .filter(profile -> profile.id().equals(id)).findFirst().orElseThrow());
  }

  private static io.etcdhbase.admin.config.InitializationConfiguration.SaltProfile controlSalt(
      RuntimeConfiguration config) {
    String id = config.authentication().authControl().saltProfileId();
    return config.saltProfiles().stream().filter(profile -> profile.id().equals(id))
        .findFirst().orElseThrow();
  }

  static AuthDeploymentRegistry.Group group(RuntimeConfiguration configuration, String tableGroupId,
      long clusterId, boolean enabled) {
    var auth = Objects.requireNonNull(configuration.authentication(), "authentication");
    var source = TableGroupPlanFactory.create(configuration.initialization(), tableGroupId, null);
    var endpoint = auth.endpointBindings().stream()
        .filter(binding -> binding.tableGroupId().equals(tableGroupId)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException("inventory group has no endpoint binding"));
    var client = auth.clientProfiles().stream()
        .filter(profile -> profile.id().equals(endpoint.clientProfileId())).findFirst().orElseThrow();
    if (client.backendIdentityDomain() == null || !endpoint.providerId().equals(source.identityProviderId())) {
      throw new IllegalArgumentException("inventory endpoint identity binding is incomplete");
    }
    var scope = new AuthenticationScope(tableGroupId, source.hbaseProfile().id());
    var dataset = M3RuntimeGroups.datasetDigest(source.fingerprint(), auth.trustDomain(), clusterId);
    return new AuthDeploymentRegistry.Group(tableGroupId, endpoint.providerId(), client.backendIdentityDomain(),
        scope.connectionProfileId(), dataset, AuthConfigurationFingerprint.compute(auth, scope, dataset), enabled);
  }
}
