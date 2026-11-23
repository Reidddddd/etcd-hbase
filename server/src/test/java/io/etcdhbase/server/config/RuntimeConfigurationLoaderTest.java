package io.etcdhbase.server.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class RuntimeConfigurationLoaderTest {
  private final ObjectMapper mapper = new ObjectMapper();
  private final RuntimeConfigurationLoader loader = new RuntimeConfigurationLoader();

  static byte[] example() throws Exception {
    return Files.readAllBytes(Path.of("../config/examples/j06/server.json"));
  }

  @Test
  void loadsSharedModelsAndAppliesOverridesAfterTheFile() throws Exception {
    var configuration =
        loader.load(
            new ByteArrayInputStream(example()),
            Map.of("ETCD_HBASE_LISTENER_PORT", "12345", "ETCD_HBASE_JDK21_HOME", "ignored"));
    assertEquals(12345, configuration.listeners().getFirst().port());
    assertEquals(
        2, configuration.initialization().tableGroups().getFirst().historyCandidates().size());
    assertThrows(UnsupportedOperationException.class, () -> configuration.listeners().clear());
  }

  @Test
  void refusesMissingNullCoercedUnknownAndDuplicateProperties() throws Exception {
    rejects(tree -> ((ObjectNode) tree.path("resources")).remove("request_deadline_ms"));
    rejects(tree -> ((ObjectNode) tree.path("listeners").path(0)).putNull("port"));
    rejects(tree -> ((ObjectNode) tree.path("listeners").path(0)).put("port", "0"));
    rejects(tree -> ((ObjectNode) tree).put("password", "private-sentinel"));
    rejects(
        tree -> ((ObjectNode) tree.path("hbase_profiles").path(0)).put("authentication_mode", 0));
    String duplicate =
        new String(example(), StandardCharsets.UTF_8)
            .replace("\"schema_version\": 1", "\"schema_version\": 1, \"schema_version\": 1");
    assertThrows(
        IllegalArgumentException.class, () -> load(duplicate.getBytes(StandardCharsets.UTF_8)));
    assertThrows(IllegalArgumentException.class, () -> load(new byte[] {(byte) 0xc3, 0x28}));
    assertThrows(IllegalArgumentException.class, () -> load(new byte[65_537]));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            load(
                (new String(example(), StandardCharsets.UTF_8) + "{}")
                    .getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void rejectsNumericBooleanAndCollectionCoercionIntoStrings() throws Exception {
    rejects(tree -> ((ObjectNode) tree.path("listeners").path(0)).put("id", 123));
    rejects(tree -> ((ObjectNode) tree.path("listeners").path(0)).put("id", true));
    rejects(tree -> ((ObjectNode) tree.path("hbase_profiles").path(0)).put("namespace", 123));
    rejects(
        tree ->
            ((com.fasterxml.jackson.databind.node.ArrayNode)
                    tree.path("salt_profiles").path(0).path("split_keys_hex"))
                .set(0, mapper.getNodeFactory().numberNode(1)));
    rejects(
        tree ->
            ((com.fasterxml.jackson.databind.node.ArrayNode)
                    tree.path("table_groups")
                        .path(0)
                        .path("system_table")
                        .path("record_categories"))
                .set(0, mapper.getNodeFactory().numberNode(1)));
  }

  @Test
  void enforcesSharedColumnFamilyBoundsAndAllowsTheirValidBoundary() throws Exception {
    for (String name : new String[] {"2data", "data.column", "a".repeat(33)}) {
      rejects(tree -> family(tree).put("name", name));
    }
    rejects(tree -> family(tree).put("max_versions", 1025));
    rejects(tree -> family(tree).put("min_versions", -1));
    rejects(tree -> family(tree).put("ttl_seconds", Integer.MAX_VALUE));
    var tree = mapper.readTree(example());
    family(tree)
        .put("name", "a".repeat(32))
        .put("max_versions", 1024)
        .put("ttl_seconds", Integer.MAX_VALUE - 1);
    load(mapper.writeValueAsBytes(tree));
  }

  private static ObjectNode family(JsonNode tree) {
    return (ObjectNode)
        tree.path("table_groups").path(0).path("system_table").path("column_families").path(0);
  }

  @Test
  void refusesInvalidBudgetsUnknownOverridesAndSensitiveEchoes() throws Exception {
    for (String field :
        new String[] {
          "max_concurrent_requests",
          "request_deadline_ms",
          "max_rpc_bytes",
          "max_scanners_per_table"
        }) {
      rejects(tree -> ((ObjectNode) tree.path("resources")).put(field, 0));
    }
    rejects(tree -> ((ObjectNode) tree.path("resources")).put("max_config_bytes", 1));
    rejects(tree -> ((ObjectNode) tree.path("resources")).put("max_queued_requests", 1));
    for (var environment :
        java.util.List.of(
            Map.of("ETCD_HBASE_PASSWORD", "private-sentinel"),
            Map.of("ETCD_HBASE_LISTENER_PORT", "private-sentinel"),
            Map.of("ETCD_HBASE_LISTENER_PORT", "9999999999"))) {
      var error =
          assertThrows(
              IllegalArgumentException.class,
              () -> loader.load(new ByteArrayInputStream(example()), environment));
      assertFalse(error.toString().contains("private-sentinel"));
      assertNull(error.getCause());
    }
  }

  @Test
  void rejectsWildcardAndNonlocalAnonymousBindings() throws Exception {
    for (String host : new String[] {"*", "0.0.0.0", "::", "example.invalid", "192.0.2.1"}) {
      rejects(tree -> ((ObjectNode) tree.path("listeners").path(0)).put("bind_host", host));
    }
    rejects(tree -> ((ObjectNode) tree.path("listeners").path(0)).put("table_group_id", "missing"));
  }

  @Test
  void recognizesLoopbackAliasesWithoutResolvingHostnames() throws Exception {
    for (String host : new String[] {"LOCALHOST.", "127.0.0.2", "::1", "[0:0:0:0:0:0:0:1]"}) {
      var tree = mapper.readTree(example());
      ((ObjectNode) tree.path("listeners").path(0)).put("bind_host", host);
      assertEquals(host, load(mapper.writeValueAsBytes(tree)).listeners().getFirst().bindHost());
    }
    assertEquals(
        "host:unresolvable.invalid",
        ListenerAddress.parse("Unresolvable.Invalid.").scopeIdentity());
  }

  @Test
  void loadsOptionalListenerTlsAndRejectsUnsafeMaterialReferences() throws Exception {
    var tree = mapper.readTree(example());
    var tls = ((ObjectNode) tree.path("listeners").path(0)).putObject("tls");
    tls.put("certificate_file", "/run/secrets/server.crt");
    tls.put("private_key_file", "/run/secrets/server.key");
    var loaded = load(mapper.writeValueAsBytes(tree));
    assertEquals("/run/secrets/server.crt", loaded.listeners().getFirst().tls().certificateFile());
    assertEquals("/run/secrets/server.key", loaded.listeners().getFirst().tls().privateKeyFile());

    rejects(configuration -> listenerTls(configuration)
        .put("certificate_file", "relative/server.crt"));
    rejects(configuration -> listenerTls(configuration)
        .put("private_key_file", "/run/secrets/server.crt"));
    rejects(configuration -> listenerTls(configuration).put("password", "private-sentinel"));
  }

  @Test
  void detectsFixedPortAliasesButAllowsIndependentEphemeralBindings() throws Exception {
    var tree = mapper.readTree(example());
    var listeners = (com.fasterxml.jackson.databind.node.ArrayNode) tree.path("listeners");
    var first = (ObjectNode) listeners.get(0);
    first.put("port", 12345);
    var second = first.deepCopy().put("id", "second").put("bind_host", "LOCALHOST.");
    listeners.add(second);
    assertThrows(IllegalArgumentException.class, () -> load(mapper.writeValueAsBytes(tree)));
    first.put("port", 0);
    second.put("port", 0);
    assertEquals(2, load(mapper.writeValueAsBytes(tree)).listeners().size());
  }

  @Test
  void distinguishesActualLoopbackSocketsWhileRejectingEquivalentIpForms() throws Exception {
    var tree = mapper.readTree(example());
    var listeners = (com.fasterxml.jackson.databind.node.ArrayNode) tree.path("listeners");
    var first = (ObjectNode) listeners.get(0);
    first.put("port", 12345);
    var second = first.deepCopy().put("id", "second").put("bind_host", "127.0.0.2");
    listeners.add(second);
    assertEquals(2, load(mapper.writeValueAsBytes(tree)).listeners().size());
    second.put("bind_host", "[::ffff:127.0.0.1]");
    assertThrows(IllegalArgumentException.class, () -> load(mapper.writeValueAsBytes(tree)));
    first.put("bind_host", "::1");
    second.put("bind_host", "[0:0:0:0:0:0:0:1]");
    assertThrows(IllegalArgumentException.class, () -> load(mapper.writeValueAsBytes(tree)));
    second.put("bind_host", "127.0.0.2");
    assertEquals(2, load(mapper.writeValueAsBytes(tree)).listeners().size());
  }

  @Test
  void detectsPhysicalProfileAliasesAndNamespaceTableCollisions() throws Exception {
    var tree = mapper.readTree(example());
    var profiles = (com.fasterxml.jackson.databind.node.ArrayNode) tree.path("hbase_profiles");
    profiles.add(
        ((ObjectNode) profiles.get(0)).deepCopy().put("id", "alias").put("quorum", "LOCALHOST."));
    var groups = (com.fasterxml.jackson.databind.node.ArrayNode) tree.path("table_groups");
    groups.add(
        ((ObjectNode) groups.get(0))
            .deepCopy()
            .put("id", "other")
            .put("hbase_profile_id", "alias"));
    assertThrows(IllegalArgumentException.class, () -> load(mapper.writeValueAsBytes(tree)));
  }

  private RuntimeConfiguration load(byte[] bytes) {
    return loader.load(new ByteArrayInputStream(bytes), Map.of());
  }

  private static ObjectNode listenerTls(JsonNode tree) {
    return ((ObjectNode) tree.path("listeners").path(0)).putObject("tls");
  }

  private void rejects(Consumer<JsonNode> change) throws Exception {
    var tree = mapper.readTree(example());
    change.accept(tree);
    assertThrows(IllegalArgumentException.class, () -> load(mapper.writeValueAsBytes(tree)));
  }
}
