package io.etcdhbase.server.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.ClientProfile;
import io.etcdhbase.server.config.M3AuthenticationConfiguration.CredentialVersion;
import java.util.List;
import org.junit.jupiter.api.Test;

class CredentialVersionConfigurationTest {
  private final ObjectMapper mapper = new ObjectMapper()
      .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

  @Test
  void loadsCredentialVersionBindingsAndPreservesMissingSource() throws Exception {
    String common = "\"id\":\"client\",\"hbase_profile_id\":\"profile\","
        + "\"authenticator_id\":\"kerberos-password\",\"hbase_site_file\":\"/site.xml\"";
    ClientProfile versioned = mapper.readValue("{" + common + ",\"credential_versions\":[{"
        + "\"principal\":\"alice@EXAMPLE\",\"generation\":7,"
        + "\"credential_reference\":\"secret://alice-v7\"}]}", ClientProfile.class);
    assertEquals(List.of(new CredentialVersion("alice@EXAMPLE", 7, "secret://alice-v7")),
        versioned.credentialVersions());
    assertEquals(List.of(), mapper.readValue("{" + common + "}", ClientProfile.class)
        .credentialVersions());
  }

  @Test
  void rejectsInvalidAndDuplicateVersionBindings() {
    assertThrows(IllegalArgumentException.class,
        () -> new CredentialVersion("alice", 0, "secret://alice"));
    var binding = new CredentialVersion("alice", 1, "secret://alice-v1");
    assertThrows(IllegalArgumentException.class, () -> new ClientProfile("client", "profile",
        "kerberos-password", null, "/site.xml", List.of(binding, binding)));
  }

  @Test
  void loadsAnExplicitBackendIdentityDomain() throws Exception {
    String json = "{\"id\":\"client\",\"hbase_profile_id\":\"profile\","
        + "\"authenticator_id\":\"kerberos-password\",\"hbase_site_file\":\"/site.xml\","
        + "\"backend_identity_domain\":\"realm-a\"}";
    assertEquals("realm-a", mapper.readValue(json, ClientProfile.class).backendIdentityDomain());
    assertThrows(IllegalArgumentException.class, () -> new ClientProfile("client", "profile",
        "kerberos-password", null, "/site.xml", List.of(), "invalid domain"));
  }
}
