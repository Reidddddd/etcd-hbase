package io.etcdhbase.server;

import io.etcdhbase.server.config.M3RuntimeConfigurationLoader;
import io.etcdhbase.server.config.M3RuntimeConfiguration.ActivationProfile;
import io.etcdhbase.server.config.RuntimeConfigurationLoader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

/** Independent authenticated M3 runtime controlled by its integration-test parent. */
public final class M5AuthenticateGatewayRuntimeProcess {
  private M5AuthenticateGatewayRuntimeProcess() {}

  public static void main(String[] arguments) throws Exception {
    if (arguments.length != 4 && (arguments.length != 5 || !arguments[4].equals("PUBLIC_TEST"))) {
      throw new IllegalArgumentException("expected runtime, activation, HBase site, Kerberos config, and optional test profile");
    }
    Path runtimeFile = Path.of(arguments[0]);
    Path activationFile = Path.of(arguments[1]);
    Path hbaseSiteFile = Path.of(arguments[2]);
    System.setProperty("java.security.krb5.conf", Path.of(arguments[3]).toString());

    var hbaseConfiguration = HBaseConfiguration.create();
    hbaseConfiguration.addResource(new org.apache.hadoop.fs.Path(hbaseSiteFile.toUri()));
    UserGroupInformation.setConfiguration(hbaseConfiguration);

    var runtimeLoader = new RuntimeConfigurationLoader();
    var activationLoader = arguments.length == 5
        ? new M3RuntimeConfigurationLoader(ActivationProfile.PUBLIC_TEST)
        : new M3RuntimeConfigurationLoader();
    io.etcdhbase.server.config.RuntimeConfiguration runtimeConfiguration;
    io.etcdhbase.server.config.M3RuntimeConfiguration activation;
    try (var input = Files.newInputStream(runtimeFile)) {
      runtimeConfiguration = runtimeLoader.load(input);
    }
    try (var input = Files.newInputStream(activationFile)) {
      activation = activationLoader.load(input);
    }
    try (var server = RuntimeServer.startM3(runtimeConfiguration, activation);
         var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
      if (!server.dataReady()) throw new IllegalStateException("M5 runtime did not recover before readiness");
      System.out.println("M5_GATEWAY_READY\t" + server.boundListeners().getFirst().port());
      System.out.flush();
      if (!"STOP".equals(input.readLine())) {
        throw new IllegalStateException("M5 runtime process received no graceful-stop command");
      }
      server.close();
      System.out.println("M5_GATEWAY_STOPPED");
      System.out.flush();
    }
  }
}
