package io.etcdhbase.hbase.probe;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testing.TestingHBaseCluster;
import org.apache.hadoop.hbase.testing.TestingHBaseClusterOption;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

/** Disposable SIMPLE or Kerberos HBase server, isolated from the application's client runtime. */
public final class HBaseMiniClusterMain {
  private HBaseMiniClusterMain() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 3 || args.length > 5) {
      throw new IllegalArgumentException(
          "expected configuration, readiness, failure files, optional simple/kerberos profile"
              + " and region-server count");
    }
    int regionServers = validateRegionServerCount(args.length == 5 ? Integer.parseInt(args[4]) : 1);

    Configuration configuration = HBaseConfiguration.create();
    configuration.setInt("hbase.master.info.port", -1);
    configuration.setInt("hbase.regionserver.info.port", -1);
    configuration.set("hbase.security.authentication", "simple");
    configuration.setBoolean("hbase.security.authorization", false);
    configuration.set("hadoop.security.authentication", "simple");
    configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
    configuration.set("hbase.zookeeper.property.clientPortAddress", "127.0.0.1");
    configuration.set("hbase.master.hostname", "127.0.0.1");
    configuration.set("hbase.master.ipc.address", "127.0.0.1");
    configuration.set("hbase.regionserver.hostname", "127.0.0.1");
    configuration.set("hbase.regionserver.ipc.address", "127.0.0.1");
    configuration.set(
        "hbase.client.registry.impl", "org.apache.hadoop.hbase.client.ZKConnectionRegistry");
    TemporaryKerberosSecurity security = null;
    if (args.length >= 4) {
      if (!args[3].equals("kerberos") && !args[3].equals("simple")) {
        throw new IllegalArgumentException("unknown temporary HBase security profile");
      }
      if (args[3].equals("kerberos")) {
        security =
            TemporaryKerberosSecurity.start(
                configuration, Path.of(args[0]).toAbsolutePath().getParent().resolve("security"));
      }
    }
    TestingHBaseClusterOption option =
        TestingHBaseClusterOption.builder()
            .conf(configuration)
            .numMasters(1)
            .numRegionServers(regionServers)
            .numDataNodes(0)
            .numZkServers(1)
            .useExternalDfs("file:///")
            .createRootDir(true)
            .createWALDir(true)
            .build();

    TestingHBaseCluster cluster = TestingHBaseCluster.create(option);
    System.err.println("m0-hbase-cluster phase=starting");
    System.err.flush();
    Path configurationFile = Path.of(args[0]);
    Path readinessFile = Path.of(args[1]);
    Path failureFile = Path.of(args[2]);
    try {
      cluster.start();
      System.err.println("m0-hbase-cluster phase=started");
      System.err.flush();
      try (var output = Files.newOutputStream(configurationFile)) {
        cluster.getConf().writeXml(output);
      }
      String readinessRecord =
          String.join(
              "\t",
              "M0_HBASE_CLUSTER_READY",
              configurationFile.toString(),
              VersionInfo.getVersion(),
              Runtime.version().toString());
      Files.writeString(readinessFile, readinessRecord, StandardCharsets.UTF_8);
      System.out.println(readinessRecord);
      System.out.flush();
      System.err.println("m0-hbase-cluster phase=ready");
      System.err.flush();
    } catch (Exception | Error startupFailure) {
      try {
        Files.writeString(
            failureFile,
            startupFailure.getClass().getName() + ": " + startupFailure.getMessage(),
            StandardCharsets.UTF_8);
      } catch (java.io.IOException markerFailure) {
        startupFailure.addSuppressed(markerFailure);
      }
      throw startupFailure;
    }

    try (var input =
        new java.io.BufferedReader(
            new java.io.InputStreamReader(System.in, StandardCharsets.US_ASCII))) {
      String command;
      while ((command = input.readLine()) != null && !"STOP".equals(command)) {
        if (!command.matches("SCANNERS [a-f0-9]{32}")) throw new IllegalStateException("unknown fixture control command");
        int scanners = 0;
        for (var address : cluster.getRegionServerAddresses()) {
          var online = cluster.getOnlineRegionsInterface(address).orElseThrow();
          if (!(online instanceof HRegionServer server)) throw new IllegalStateException("fixture scanner counters are unavailable");
          scanners += server.getRSRpcServices().getScannersCount();
        }
        Path response = readinessFile.resolveSibling("scanner-count.txt");
        Path pending = response.resolveSibling("scanner-count.pending");
        Files.writeString(pending, command.substring("SCANNERS ".length()) + "\t" + scanners, StandardCharsets.US_ASCII);
        Files.move(pending, response, java.nio.file.StandardCopyOption.REPLACE_EXISTING, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
      }
      if (command == null) throw new IllegalStateException("fixture control channel ended before stop");
    } finally {
      try {
        cluster.stop();
      } finally {
        if (security != null) {
          security.close();
        }
      }
    }
  }

  static int validateRegionServerCount(int count) {
    if (count < 1 || count > 4) {
      throw new IllegalArgumentException("temporary cluster requires 1..4 region servers");
    }
    return count;
  }
}
