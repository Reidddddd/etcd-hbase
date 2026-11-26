package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.admin.TableGroupPlanFactory;
import io.etcdhbase.server.auth.AuthConfigurationFingerprint;
import io.etcdhbase.server.config.M3AuthenticationConfiguration;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.*;
import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfigurationLoader;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AuthRegistryInventoryTest {
  @Test
  void inventoryUsesTheRuntimeDatasetAndConfigurationBindings() throws Exception {
    var config = configuration("realm");
    var group = AuthRegistryInventory.group(config, "local", 42, true);
    var source = TableGroupPlanFactory.create(config.initialization(), "local", null);
    var dataset = M3RuntimeGroups.datasetDigest(source.fingerprint(), "trust", 42);
    assertEquals(dataset, group.datasetDigest());
    assertEquals(AuthConfigurationFingerprint.compute(config.authentication(),
        new AuthenticationScope("local", "local-hbase"), dataset), group.configurationDigest());
    assertEquals("realm", group.backendIdentityDomain());
    assertNotEquals(group.datasetDigest(), AuthRegistryInventory.group(config, "local", 43, true).datasetDigest());
    assertThrows(IllegalArgumentException.class,
        () -> AuthRegistryInventory.group(configuration(null), "local", 42, true));
  }

  private RuntimeConfiguration configuration(String identityDomain) throws Exception {
    RuntimeConfiguration base;
    try (var input = Files.newInputStream(WireProcesses.repositoryRoot().resolve("config/examples/m3/server.json"))) {
      base = new RuntimeConfigurationLoader().load(input, Map.of());
    }
    var identity = new IdentityCredential(CredentialSource.CREDENTIAL_REFERENCE, null, "env://IDENTITY");
    var client = new ClientProfile("client", "local-hbase", "kerberos-password", null,
        "/hbase-site.xml", List.of(), identityDomain);
    var auth = new M3AuthenticationConfiguration("trust", 60_000, 4, 4, "/secrets",
        new AuthControl("local-hbase", base.saltProfiles().getFirst().id(), "auth_control",
            "control", "client", identity), List.of(client),
        List.of(new EndpointBinding("local", "dev-no-auth", "client")),
        List.of(new BackgroundIdentity("local", "dev-no-auth", "client", identity, List.of("local"))),
        new CredentialKeyRing("key", List.of(new KeyReference("key", "secret://key"))));
    return new RuntimeConfiguration(base.schemaVersion(), base.listeners(), base.hbaseProfiles(),
        base.saltProfiles(), base.tableGroups(), base.resources(), auth);
  }
}
