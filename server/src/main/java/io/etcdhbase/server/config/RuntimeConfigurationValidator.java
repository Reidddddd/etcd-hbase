package io.etcdhbase.server.config;

import io.etcdhbase.admin.config.EndpointHost;
import io.etcdhbase.admin.config.InitializationConfiguration.AuthenticationMode;
import io.etcdhbase.admin.config.InitializationConfigurationValidator;
import io.etcdhbase.admin.config.StrictConfigurationJson;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Checks service bindings without DNS, credentials, network access, or table creation. */
public final class RuntimeConfigurationValidator {
  private RuntimeConfigurationValidator() {}

  public static void validate(RuntimeConfiguration configuration, long effectiveBytes) {
    InitializationConfigurationValidator.validate(configuration.initialization());
    validateBudgets(configuration.resources(), effectiveBytes);
    if (configuration.authentication() != null) {
      M3AuthenticationConfiguration.validateAgainst(configuration.authentication(), configuration);
    }
    var groups =
        configuration.tableGroups().stream()
            .collect(Collectors.toMap(group -> group.id(), Function.identity()));
    var profiles =
        configuration.hbaseProfiles().stream()
            .collect(Collectors.toMap(profile -> profile.id(), Function.identity()));
    Set<String> identifiers = new HashSet<>();
    Map<Integer, List<EndpointHost>> sockets = new HashMap<>();
    require(
        !configuration.listeners().isEmpty() && configuration.listeners().size() <= 64,
        "service requires between one and 64 listeners");
    for (var listener : configuration.listeners()) {
      require(
          listener.id().matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
              && identifiers.add(listener.id()),
          "listener IDs must be bounded and distinct");
      var group = groups.get(listener.tableGroupId());
      require(group != null, "listener references an unknown table group");
      var host = ListenerAddress.parse(listener.bindHost());
      require(listener.port() >= 0 && listener.port() <= 65535, "invalid listener port");
      require(listener.port() != 0 || host.isLoopback(), "ephemeral ports require loopback");
      registerSocket(listener, host, sockets);
      if (profiles.get(group.hbaseProfileId()).authenticationMode()
          == AuthenticationMode.DEV_NO_AUTH) {
        require(host.isLoopback(), "development listeners require loopback");
      }
    }
  }

  private static void registerSocket(
      RuntimeConfiguration.Listener listener,
      EndpointHost host,
      Map<Integer, List<EndpointHost>> sockets) {
    if (listener.port() == 0) return;
    var samePort = sockets.computeIfAbsent(listener.port(), ignored -> new ArrayList<>());
    boolean overlapping =
        samePort.stream().anyMatch(previous -> ListenerAddress.overlaps(previous, host));
    require(!overlapping, "listener socket bindings overlap");
    samePort.add(host);
  }

  private static void validateBudgets(RuntimeConfiguration.ResourceBudgets budget, long bytes) {
    require(
        budget.maxConfigBytes() >= bytes
            && budget.maxConfigBytes() <= StrictConfigurationJson.MAX_CONFIG_BYTES,
        "effective configuration exceeds its byte budget");
    require(
        budget.maxConcurrentRequests() >= 1 && budget.maxConcurrentRequests() <= 100_000,
        "invalid concurrent request budget");
    require(
        budget.requestDeadlineMs() >= 1 && budget.requestDeadlineMs() <= 600_000,
        "invalid request deadline budget");
    require(
        budget.maxRpcBytes() >= 1
            && budget.maxRpcBytes() <= budget.maxRequestBytes()
            && budget.maxRequestBytes() <= 67_108_864,
        "invalid RPC or request byte budget");
    require(
        budget.maxScannersPerTable() >= 1 && budget.maxScannersPerTable() <= 1024,
        "invalid scanner budget");
    require(budget.maxQueuedRequests() == 0, "this skeleton requires zero queued requests");
  }

  private static void require(boolean condition, String message) {
    if (!condition) throw new IllegalArgumentException(message);
  }
}
