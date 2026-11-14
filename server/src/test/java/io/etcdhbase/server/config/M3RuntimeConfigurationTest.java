package io.etcdhbase.server.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class M3RuntimeConfigurationTest {
  @Test
  void checkedInDevelopmentExamplesAgreeOnTableGroupAndPinConfiguration() throws Exception {
    Path examples = Path.of("..", "config", "examples", "m3");
    try (var input = Files.newInputStream(examples.resolve("activation.json"));
         var serverInput = Files.newInputStream(examples.resolve("server.json"));
         var initialInput = Files.newInputStream(examples.resolve("initialize.json"))) {
      var activation = new M3RuntimeConfigurationLoader().load(input);
      var server = new RuntimeConfigurationLoader().load(serverInput);
      var initial = new io.etcdhbase.admin.config.InitializationConfigurationLoader().load(initialInput);
      var runtimePlan = io.etcdhbase.admin.TableGroupPlanFactory.create(server.initialization(), "local", null);
      var initialPlan = io.etcdhbase.admin.TableGroupPlanFactory.create(initial, "local", null);
      assertEquals(initialPlan.fingerprint(), runtimePlan.fingerprint());
      assertEquals(2, activation.pinConfiguration().shardCount());
      assertEquals(16, activation.pinConfiguration().slotsPerShard());
      assertEquals(60_000, activation.pinConfiguration().leaseMillis());
      assertEquals(1_572_864, activation.maxEtcdRequestBytes());
    }
  }

  @Test
  void m4ExampleUsesExplicitLeaseTimingInputs() throws Exception {
    Path example = Path.of("..", "config", "examples", "m4", "activation.json");
    try (var input = Files.newInputStream(example)) {
      var activation = new M3RuntimeConfigurationLoader().load(input);
      assertTrue(activation.m4LeaseApiEnabled());
      assertEquals(5, activation.leaseMinTtlSeconds());
      assertEquals(1_000, activation.leaseClockUncertaintyMs());
      assertFalse(activation.readCacheEnabled());
    }
  }

  @Test
  void acceptsAnExplicitUnsignedStableMemberIdentityAndPinConfiguration() {
    var configuration = load(valid());
    assertFalse(configuration.readCacheEnabled());
    assertEquals(Long.MIN_VALUE, configuration.memberIdBits());
    assertEquals(2, configuration.pinConfiguration().shardCount());
    assertEquals(16, configuration.pinConfiguration().slotsPerShard());
    assertEquals(60_000, configuration.pinConfiguration().leaseMillis());
  }

  @Test
  void cacheDefaultsOffAndCannotBeEnabledWithoutIdentityLifecycleProof() {
    assertFalse(load(valid()).readCacheEnabled());
    assertFalse(load(valid()).m4LeaseApiEnabled());
    assertFalse(load(valid().replace("\"pin_lease_ms\":60000", "\"pin_lease_ms\":60000,\"read_cache_enabled\":false"))
        .readCacheEnabled());
    var enabled = assertThrows(M3RuntimeConfiguration.CacheActivationUnavailable.class, () -> load(valid().replace(
        "\"pin_lease_ms\":60000", "\"pin_lease_ms\":60000,\"read_cache_enabled\":true")));
    assertTrue(enabled.getMessage().contains("identity lifecycle"));
    assertThrows(IllegalArgumentException.class, () -> load(valid().replace(
        "\"pin_lease_ms\":60000", "\"pin_lease_ms\":60000,\"read_cache_enabled\":null")));
    assertThrows(IllegalArgumentException.class, () -> load(valid().replace(
        "\"pin_lease_ms\":60000", "\"pin_lease_ms\":60000,\"read_cache_enabled\":\"true\"")));
    assertThrows(IllegalArgumentException.class, () -> load(valid().replace(
        "\"pin_lease_ms\":60000", "\"pin_lease_ms\":60000,\"m4_lease_api_enabled\":null")));
    assertThrows(IllegalArgumentException.class, () -> load(valid().replace(
        "\"pin_lease_ms\":60000", "\"pin_lease_ms\":60000,\"m4_lease_api_enabled\":\"false\"")));
  }

  @Test
  void m4ActivationRequiresExplicitMinimumTtlAndClockBound() {
    String enabled = valid().replace("\"pin_lease_ms\":60000",
        "\"pin_lease_ms\":60000,\"m4_lease_api_enabled\":true");
    assertThrows(IllegalArgumentException.class, () -> load(enabled));
    assertThrows(IllegalArgumentException.class, () -> load(enabled.replace(
        "\"m4_lease_api_enabled\":true", "\"m4_lease_api_enabled\":true,\"lease_min_ttl_seconds\":5")));
    assertThrows(IllegalArgumentException.class, () -> load(valid().replace(
        "\"pin_lease_ms\":60000", "\"pin_lease_ms\":60000,\"m4_lease_api_enabled\":true,"
            + "\"lease_min_ttl_seconds\":5,\"lease_clock_uncertainty_ms\":0")));
  }

  @Test
  void rejectsInvalidIdentityLeaseAndMemoryWithoutSupplyingDefaults() {
    for (String invalid : new String[] {
        valid().replace("9223372036854775808", "0"),
        valid().replace("9223372036854775808", "18446744073709551616"),
        valid().replace("\"heartbeat_ms\":1000", "\"heartbeat_ms\":4000"),
        valid().replace("\"memory_bytes\":536870912", "\"memory_bytes\":1"),
        valid().replace("\"pin_shards\":2", "\"pin_shards\":0"),
        valid().replace(",\"startup_timeout_ms\":30000", "")}) {
      assertThrows(IllegalArgumentException.class, () -> load(invalid));
    }
  }

  private static M3RuntimeConfiguration load(String json) {
    return new M3RuntimeConfigurationLoader()
        .load(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
  }

  private static String valid() {
    return "{\"member_id\":\"9223372036854775808\",\"owner_lease_ms\":6000,"
        + "\"heartbeat_ms\":1000,\"startup_timeout_ms\":30000,\"shutdown_timeout_ms\":30000,"
        + "\"memory_bytes\":536870912,\"max_etcd_request_bytes\":1572864,"
        + "\"pin_shards\":2,\"pin_slots_per_shard\":16,\"pin_lease_ms\":60000}";
  }
}
