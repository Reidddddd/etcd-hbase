package io.etcdhbase.server.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.etcdhbase.core.commit.MutationForwardRequest;
import io.etcdhbase.core.commit.SnapshotPinConfiguration;
import io.etcdhbase.core.lease.LeaseCostAdmission;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Explicit activation settings; member identity and budgets are never invented at startup.
 *
 * <p>When M4 Lease support is enabled, the configured clock uncertainty is an operator-provided
 * absolute UTC error bound for the runtime lifetime. The runtime detects wall-clock discontinuity
 * against its local monotonic clock, but deployment must establish the initial synchronization
 * bound before startup and after restart.
 *
 * @param leaseClockUncertaintyMs absolute wall-clock uncertainty in milliseconds; zero disables
 *     M4 Lease activation
 */
public record M3RuntimeConfiguration(
    String memberId,
    int ownerLeaseMs,
    int heartbeatMs,
    int startupTimeoutMs,
    int shutdownTimeoutMs,
    long memoryBytes,
    int maxEtcdRequestBytes,
    int pinShards,
    int pinSlotsPerShard,
    int pinLeaseMs,
    boolean readCacheEnabled,
    boolean m4LeaseApiEnabled,
    long leaseMinTtlSeconds,
    long leaseClockUncertaintyMs,
    int leaseClockMaxDriftPpm,
    int leaseMaximumBindings,
    long leaseMaximumKeyBytes,
    long leaseMaximumRevokeCostBytes,
    @JsonProperty(required = false) String ownerInstanceId,
    @JsonProperty(required = false) OwnerForwardingSettings ownerForwarding,
    @JsonProperty(value = "activation_profile", required = false) ActivationProfile activationProfile) {
  private static final long MIN_MEMORY = 256L << 20;
  private static final long MAX_MEMORY = 1L << 40;

  public static final class CacheActivationUnavailable extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;
    public CacheActivationUnavailable() {
      super("read cache requires verified identity lifecycle and coverage proofs");
    }
  }

  public enum ActivationProfile { PRODUCTION, PUBLIC_TEST }

  public static final class LeaseActivationUnavailable extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;
    public LeaseActivationUnavailable() {
      super("production Lease requires authenticated identity, authorization and recovery capabilities");
    }
  }

  public record OwnerPeer(String instanceId, String host, int port, String peerIdentity) {
    public OwnerPeer {
      if (instanceId == null || !instanceId.matches("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
          || host == null || host.isBlank() || host.length() > 253
          || port < 1 || port > 65_535 || peerIdentity == null
          || !peerIdentity.matches("[A-Za-z0-9][A-Za-z0-9.-]{0,252}")) {
        throw new IllegalArgumentException("invalid authenticated owner peer");
      }
    }
  }

  public record OwnerForwardingSettings(String bindHost, int port, String certificateFile,
      String privateKeyFile, String trustedCaFile, List<OwnerPeer> peers) {
    public OwnerForwardingSettings {
      Objects.requireNonNull(peers, "peers");
      if (bindHost == null || bindHost.isBlank() || bindHost.length() > 253
          || port < 1 || port > 65_535 || certificateFile == null || certificateFile.isBlank()
          || privateKeyFile == null || privateKeyFile.isBlank()
          || trustedCaFile == null || trustedCaFile.isBlank() || peers.isEmpty() || peers.size() > 64) {
        throw new IllegalArgumentException("invalid internal owner forwarding settings");
      }
      peers = List.copyOf(peers);
      var instances = new HashSet<String>();
      var identities = new HashSet<String>();
      for (OwnerPeer peer : peers) {
        Objects.requireNonNull(peer, "peer");
        if (!instances.add(peer.instanceId())
            || !identities.add(peer.peerIdentity().toLowerCase(Locale.ROOT))) {
          throw new IllegalArgumentException("owner peer identities must be unique");
        }
      }
    }
  }

  /** Explicit clock-rate bound required for Lease activation. */
  public M3RuntimeConfiguration(String memberId, int ownerLeaseMs, int heartbeatMs,
      int startupTimeoutMs, int shutdownTimeoutMs, long memoryBytes, int maxEtcdRequestBytes,
      int pinShards, int pinSlotsPerShard, int pinLeaseMs, boolean readCacheEnabled,
      boolean m4LeaseApiEnabled, long leaseMinTtlSeconds, long leaseClockUncertaintyMs,
      int leaseClockMaxDriftPpm, int leaseMaximumBindings, long leaseMaximumKeyBytes,
      long leaseMaximumRevokeCostBytes) {
    this(memberId, ownerLeaseMs, heartbeatMs, startupTimeoutMs, shutdownTimeoutMs, memoryBytes,
        maxEtcdRequestBytes, pinShards, pinSlotsPerShard, pinLeaseMs, readCacheEnabled,
        m4LeaseApiEnabled, leaseMinTtlSeconds, leaseClockUncertaintyMs, leaseClockMaxDriftPpm,
        leaseMaximumBindings, leaseMaximumKeyBytes, leaseMaximumRevokeCostBytes, null, null,
        ActivationProfile.PRODUCTION);
  }

  /** Preserve the full constructor while keeping its activation profile fail-closed. */
  public M3RuntimeConfiguration(String memberId, int ownerLeaseMs, int heartbeatMs,
      int startupTimeoutMs, int shutdownTimeoutMs, long memoryBytes, int maxEtcdRequestBytes,
      int pinShards, int pinSlotsPerShard, int pinLeaseMs, boolean readCacheEnabled,
      boolean m4LeaseApiEnabled, long leaseMinTtlSeconds, long leaseClockUncertaintyMs,
      int leaseClockMaxDriftPpm, int leaseMaximumBindings, long leaseMaximumKeyBytes,
      long leaseMaximumRevokeCostBytes, String ownerInstanceId,
      OwnerForwardingSettings ownerForwarding) {
    this(memberId, ownerLeaseMs, heartbeatMs, startupTimeoutMs, shutdownTimeoutMs, memoryBytes,
        maxEtcdRequestBytes, pinShards, pinSlotsPerShard, pinLeaseMs, readCacheEnabled,
        m4LeaseApiEnabled, leaseMinTtlSeconds, leaseClockUncertaintyMs, leaseClockMaxDriftPpm,
        leaseMaximumBindings, leaseMaximumKeyBytes, leaseMaximumRevokeCostBytes, ownerInstanceId,
        ownerForwarding, ActivationProfile.PRODUCTION);
  }

  /** Existing callers retain the disabled-cache behavior. */
  public M3RuntimeConfiguration(String memberId, int ownerLeaseMs, int heartbeatMs,
      int startupTimeoutMs, int shutdownTimeoutMs, long memoryBytes, int maxEtcdRequestBytes,
      int pinShards, int pinSlotsPerShard, int pinLeaseMs) {
    this(memberId, ownerLeaseMs, heartbeatMs, startupTimeoutMs, shutdownTimeoutMs, memoryBytes,
        maxEtcdRequestBytes, pinShards, pinSlotsPerShard, pinLeaseMs, false, false, 1, 1_000,
        0,
        LeaseCostAdmission.DEFAULT_LIMITS.maximumBindings(),
        LeaseCostAdmission.DEFAULT_LIMITS.maximumKeyBytes(),
        LeaseCostAdmission.DEFAULT_LIMITS.maximumRevokeCostBytes(), null, null,
        ActivationProfile.PRODUCTION);
  }

  /** Preserve callers that explicitly set the disabled-cache state. */
  public M3RuntimeConfiguration(String memberId, int ownerLeaseMs, int heartbeatMs,
      int startupTimeoutMs, int shutdownTimeoutMs, long memoryBytes, int maxEtcdRequestBytes,
      int pinShards, int pinSlotsPerShard, int pinLeaseMs, boolean readCacheEnabled) {
    this(memberId, ownerLeaseMs, heartbeatMs, startupTimeoutMs, shutdownTimeoutMs, memoryBytes,
        maxEtcdRequestBytes, pinShards, pinSlotsPerShard, pinLeaseMs, readCacheEnabled,
        false, 1, 1_000, 0, LeaseCostAdmission.DEFAULT_LIMITS.maximumBindings(),
        LeaseCostAdmission.DEFAULT_LIMITS.maximumKeyBytes(),
        LeaseCostAdmission.DEFAULT_LIMITS.maximumRevokeCostBytes(), null, null,
        ActivationProfile.PRODUCTION);
  }

  /** Preserve callers that predate the explicit J41 Lease revocation limits. */
  public M3RuntimeConfiguration(String memberId, int ownerLeaseMs, int heartbeatMs,
      int startupTimeoutMs, int shutdownTimeoutMs, long memoryBytes, int maxEtcdRequestBytes,
      int pinShards, int pinSlotsPerShard, int pinLeaseMs, boolean readCacheEnabled,
      boolean m4LeaseApiEnabled, long leaseMinTtlSeconds, long leaseClockUncertaintyMs) {
    this(memberId, ownerLeaseMs, heartbeatMs, startupTimeoutMs, shutdownTimeoutMs, memoryBytes,
        maxEtcdRequestBytes, pinShards, pinSlotsPerShard, pinLeaseMs, readCacheEnabled,
        m4LeaseApiEnabled, leaseMinTtlSeconds, leaseClockUncertaintyMs, 0,
        LeaseCostAdmission.DEFAULT_LIMITS.maximumBindings(),
        LeaseCostAdmission.DEFAULT_LIMITS.maximumKeyBytes(),
        LeaseCostAdmission.DEFAULT_LIMITS.maximumRevokeCostBytes(), null, null,
        ActivationProfile.PRODUCTION);
  }

  public M3RuntimeConfiguration {
    if (readCacheEnabled) {
      throw new CacheActivationUnavailable();
    }
    if (activationProfile == null) activationProfile = ActivationProfile.PRODUCTION;
    if (memberId == null || !memberId.matches("[1-9][0-9]{0,19}")) {
      throw new IllegalArgumentException("member ID must be an explicit nonzero uint64");
    }
    try {
      if (Long.parseUnsignedLong(memberId) == 0) throw new NumberFormatException();
    } catch (NumberFormatException invalid) {
      throw new IllegalArgumentException("member ID must be an explicit nonzero uint64");
    }
    if (ownerLeaseMs < 1_000 || ownerLeaseMs > 600_000 || heartbeatMs < 100
        || heartbeatMs > ownerLeaseMs / 3 || startupTimeoutMs < 100 || startupTimeoutMs > 600_000
        || shutdownTimeoutMs < 1_000 || shutdownTimeoutMs > 600_000
        || memoryBytes < MIN_MEMORY || memoryBytes > MAX_MEMORY
        || maxEtcdRequestBytes < 1 || maxEtcdRequestBytes > MutationForwardRequest.MAX_OPERATION_BYTES
        || leaseMinTtlSeconds < 1 || leaseMinTtlSeconds > 9_000_000_000L
        || leaseClockUncertaintyMs < 0 || leaseClockUncertaintyMs > 60_000
        || leaseClockMaxDriftPpm < 0 || leaseClockMaxDriftPpm > 100_000
        || m4LeaseApiEnabled && (leaseClockUncertaintyMs == 0 || leaseClockMaxDriftPpm == 0)) {
      throw new IllegalArgumentException("invalid M3 runtime limits");
    }
    long minimumLeaseMemory = M3MemoryReservations.minimumLeaseMemory(1, maxEtcdRequestBytes);
    if (m4LeaseApiEnabled && memoryBytes < minimumLeaseMemory) {
      throw new IllegalArgumentException("M4 memory_bytes must be at least " + minimumLeaseMemory
          + " for one group recovery account and one Lease request/owner pair");
    }
    new LeaseCostAdmission.Limits(leaseMaximumBindings, leaseMaximumKeyBytes,
        leaseMaximumRevokeCostBytes);
    new SnapshotPinConfiguration(pinShards, pinSlotsPerShard, Duration.ofMillis(pinLeaseMs));
    if (ownerForwarding == null) {
      if (ownerInstanceId != null) {
        throw new IllegalArgumentException("owner_instance_id requires owner_forwarding settings");
      }
    } else {
      OwnerPeer localPeer = ownerForwarding.peers().stream()
          .filter(peer -> peer.instanceId().equals(ownerInstanceId)).findFirst().orElse(null);
      if (ownerInstanceId == null || ownerInstanceId.isBlank() || ownerInstanceId.length() > 128
          || localPeer == null || localPeer.port() != ownerForwarding.port()) {
        throw new IllegalArgumentException("local owner instance must match its forwarding listener");
      }
    }
  }

  /** Preserve the raw bits of etcd's uint64, including values above Long.MAX_VALUE. */
  public long memberIdBits() { return Long.parseUnsignedLong(memberId); }

  public SnapshotPinConfiguration pinConfiguration() {
    return new SnapshotPinConfiguration(pinShards, pinSlotsPerShard, Duration.ofMillis(pinLeaseMs));
  }

  public LeaseCostAdmission.Limits leaseCostLimits() {
    return new LeaseCostAdmission.Limits(leaseMaximumBindings, leaseMaximumKeyBytes,
        leaseMaximumRevokeCostBytes);
  }
}
