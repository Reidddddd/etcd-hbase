package io.etcdhbase.hbase.probe;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testing.TestingHBaseCluster;
import org.apache.hadoop.hbase.testing.TestingHBaseClusterOption;
import org.apache.hadoop.hbase.util.VersionInfo;

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
      publishReadiness(readinessFile, readinessRecord);
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
        respondToControl(cluster, readinessFile, command);
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

  private static void respondToControl(TestingHBaseCluster cluster, Path readinessFile, String command)
      throws Exception {
    if (command.matches("SCANNERS [a-f0-9]{32}")) {
      int scanners = 0;
      for (var address : cluster.getRegionServerAddresses()) {
        var online = cluster.getOnlineRegionsInterface(address).orElseThrow();
        if (!(online instanceof HRegionServer server)) throw new IllegalStateException("fixture scanner counters are unavailable");
        scanners += server.getRSRpcServices().getScannersCount();
      }
      publishControl(readinessFile.resolveSibling("scanner-count.txt"), command.substring(9) + "\t" + scanners);
    } else if (command.matches("STOREFILES [a-f0-9]{32} [A-Za-z0-9_-]{1,512} [a-f0-9]{2,510}")) {
      publishStoreFiles(cluster, readinessFile, command.split(" "));
    } else {
      throw new IllegalStateException("unknown fixture control command");
    }
  }

  private static void publishStoreFiles(TestingHBaseCluster cluster, Path readinessFile, String[] fields)
      throws Exception {
    var table = org.apache.hadoop.hbase.TableName.valueOf(java.util.Base64.getUrlDecoder().decode(fields[2]));
    byte[] family = java.util.HexFormat.of().parseHex(fields[3]);
    var result = new StringBuilder(fields[1]).append('\n');
    for (var address : cluster.getRegionServerAddresses()) {
      var online = cluster.getOnlineRegionsInterface(address).orElseThrow();
      if (!(online instanceof HRegionServer server)) throw new IllegalStateException("fixture stores are unavailable");
      for (var region : server.getRegions(table)) {
        var store = region.getStore(family);
        if (store == null) throw new IllegalStateException("fixture family is missing");
        for (var file : store.getStorefiles()) {
          String path = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
              file.getPath().toString().getBytes(StandardCharsets.UTF_8));
          result.append(path).append('\t').append(file.getFileInfo().getSize()).append('\n');
        }
      }
    }
    publishControl(readinessFile.resolveSibling("store-files.txt"), result.toString());
  }

  private static void publishControl(Path response, String content) throws java.io.IOException {
    Path pending = response.resolveSibling(response.getFileName() + ".pending");
    Files.writeString(pending, content, StandardCharsets.US_ASCII);
    Files.move(pending, response, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
  }

  static void publishReadiness(Path readinessFile, String record) throws java.io.IOException {
    Path pending = Files.createTempFile(readinessFile.getParent(), "readiness-", ".pending");
    try {
      Files.writeString(pending, record, StandardCharsets.UTF_8);
      Files.move(pending, readinessFile, StandardCopyOption.ATOMIC_MOVE);
    } finally {
      Files.deleteIfExists(pending);
    }
  }

  static int validateRegionServerCount(int count) {
    if (count < 1 || count > 4) {
      throw new IllegalArgumentException("temporary cluster requires 1..4 region servers");
    }
    return count;
  }
}
